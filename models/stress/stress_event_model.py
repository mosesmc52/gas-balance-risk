import pymc as pm


def fit_stress_event_model(df: pd.DataFrame, event_level: int = 2):
    d = df.copy().sort_values("date")
    d["event"] = (d["op_stress"] >= event_level).astype(int)

    # Predict event at t using features at t-1
    d["event_y"] = d["event"]
    d["op_stress_lag1"] = d["op_stress"].shift(1)
    d["stress_days_7d_lag1"] = d["stress_days_7d"].shift(1)
    d["hdd_5d_lag1"] = d["hdd_5d"].shift(1)
    d["storage_z_lag1"] = d["storage_z"].shift(1)

    feat_cols = [
        "op_stress_lag1",
        "stress_days_7d_lag1",
        "hdd_5d_lag1",
        "storage_z_lag1",
    ]
    d = d.dropna(subset=["event_y"] + feat_cols).copy()

    def z(s):
        return (s - s.mean()) / (s.std() if s.std() else 1.0)

    X_op = d["op_stress_lag1"].to_numpy()
    X_persist = z(d["stress_days_7d_lag1"]).to_numpy()
    X_hdd = z(d["hdd_5d_lag1"]).to_numpy()
    X_storage = z(d["storage_z_lag1"]).to_numpy()

    y = d["event_y"].to_numpy()

    with pm.Model() as m:
        a = pm.Normal("a", 0.0, 1.0)
        b_op = pm.Normal("b_op", 0.0, 1.0)
        b_persist = pm.Normal("b_persist", 0.0, 1.0)
        b_hdd = pm.Normal("b_hdd", 0.0, 1.0)
        b_storage = pm.Normal("b_storage", 0.0, 1.0)

        logit_p = (
            a
            + b_op * X_op
            + b_persist * X_persist
            + b_hdd * X_hdd
            + b_storage * X_storage
        )
        p = pm.Deterministic("p", pm.math.sigmoid(logit_p))

        pm.Bernoulli("y", p=p, observed=y)

        idata = pm.sample(tune=1500, draws=1500, target_accept=0.9, chains=4)

    return m, idata, d
