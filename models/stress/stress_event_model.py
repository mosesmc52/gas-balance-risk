from __future__ import annotations

import pandas as pd
import pymc as pm
from models.features.standardize import zscore


def fit_stress_event_model(df: pd.DataFrame) -> tuple:
    """
    Binary model: predict P(stress_event_t = 1) using features at t-1.

    Requires df columns:
      - date
      - stress_event (0/1)
      - hdd_median (or hdd_mean)
      - working_gas_bcf (optional)
      - notice_active_count (optional)
      - all_qty_avail_median (optional)
    """
    d = df.copy().sort_values("date")

    if "stress_event" not in d.columns:
        raise KeyError(
            "Missing 'stress_event' in model frame. "
            "Build it from notices (critical_active) or include op_stress."
        )

    # target
    d["y"] = d["stress_event"].astype(int)

    # predictors (choose what exists)
    predictors = []

    if "hdd_median" in d.columns:
        d["hdd_lag1"] = d["hdd_median"].shift(1)
        predictors.append(("hdd_lag1", True))  # zscore
    elif "hdd_mean" in d.columns:
        d["hdd_lag1"] = d["hdd_mean"].shift(1)
        predictors.append(("hdd_lag1", True))

    if "working_gas_bcf" in d.columns:
        d["storage_lag1"] = d["working_gas_bcf"].shift(1)
        predictors.append(("storage_lag1", True))

    if "all_qty_avail_median" in d.columns:
        d["cap_avail_lag1"] = d["all_qty_avail_median"].shift(1)
        predictors.append(("cap_avail_lag1", True))

    if "notice_active_count" in d.columns:
        d["notices_lag1"] = d["notice_active_count"].shift(1)
        predictors.append(("notices_lag1", True))

    if not predictors:
        raise ValueError(
            "No usable predictors found in df. "
            "Expected one of: hdd_median/hdd_mean, working_gas_bcf, all_qty_avail_median, notice_active_count."
        )

    feature_cols = [name for name, _ in predictors]
    d = d.dropna(subset=["y"] + feature_cols).copy()

    y = d["y"].to_numpy(dtype=int)

    Xs = {}
    scalers = {}

    for name, do_z in predictors:
        if do_z:
            z, mu, sd = zscore(d[name])
            Xs[name] = z
            scalers[name] = {"mean": mu, "std": sd}
        else:
            Xs[name] = d[name].to_numpy(dtype=float)

    with pm.Model() as model:
        a = pm.Normal("a", 0.0, 1.5)

        betas = {}
        for name in feature_cols:
            betas[name] = pm.Normal(f"b_{name}", 0.0, 1.0)

        logit_p = a
        for name in feature_cols:
            logit_p = logit_p + betas[name] * Xs[name]

        p = pm.Deterministic("p", pm.math.sigmoid(logit_p))
        pm.Bernoulli("y_obs", p=p, observed=y)

        idata = pm.sample(
            draws=1500, tune=1500, chains=4, target_accept=0.9, progressbar=True
        )

    return model, idata, scalers
