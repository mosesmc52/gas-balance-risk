from __future__ import annotations

import numpy as np
import pandas as pd
import pymc as pm


def _zscore_series(s: pd.Series) -> tuple[np.ndarray, float, float]:
    s = pd.to_numeric(s, errors="coerce")
    mu = float(np.nanmean(s))
    sd = float(np.nanstd(s, ddof=0))
    if sd == 0 or np.isnan(sd):
        return (np.zeros(len(s), dtype=float), mu, sd if not np.isnan(sd) else 1.0)
    return ((s - mu) / sd).to_numpy(dtype=float), mu, sd


def fit_vol_risk_model(df: pd.DataFrame):
    """
    Target: next-day abs log return of Henry Hub proxy (abs(hh_ret)).

    We model y_abs[t] from features at t-1 to avoid lookahead.

    Requires df columns (from build_model_frame):
      - date
      - hh_ret (log return)
      - stress_event (0/1) or critical_active (0/1)
      - hdd_median or hdd_mean
      - working_gas_bcf
      - optional all_qty_avail_median, notice_active_count
    """
    d = df.copy().sort_values("date").reset_index(drop=True)

    # --- Target: realized absolute return proxy ---
    if "hh_ret" not in d.columns:
        raise KeyError(
            "fit_vol_risk_model requires df['hh_ret']. Build it in build_model_frame()."
        )

    d["y_abs"] = pd.to_numeric(d["hh_ret"], errors="coerce").abs()
    d["y"] = d["y_abs"]  # observed

    # --- Operational stress feature ---
    if "stress_event" in d.columns:
        d["op_stress"] = (
            pd.to_numeric(d["stress_event"], errors="coerce").fillna(0).astype(int)
        )
    elif "critical_active" in d.columns:
        d["op_stress"] = (
            pd.to_numeric(d["critical_active"], errors="coerce").fillna(0).astype(int)
        )
    else:
        d["op_stress"] = 0

    # Rolling stress persistence: count stress days in last 7 days
    d["stress_days_7d"] = d["op_stress"].rolling(7, min_periods=1).sum()

    # --- Weather feature ---
    hdd_col = (
        "hdd_median"
        if "hdd_median" in d.columns
        else ("hdd_mean" if "hdd_mean" in d.columns else None)
    )
    if hdd_col is None:
        raise KeyError("fit_vol_risk_model requires hdd_median or hdd_mean in df.")

    d["hdd_5d"] = (
        pd.to_numeric(d[hdd_col], errors="coerce").rolling(5, min_periods=1).mean()
    )

    # --- Storage feature ---
    if "working_gas_bcf" not in d.columns:
        raise KeyError(
            "fit_vol_risk_model requires working_gas_bcf in df (weekly ff-fill to daily)."
        )

    # storage tightness proxy: negative z = tight if below mean (depends on interpretation)
    # keep as z so model sees relative level
    storage_z, storage_mu, storage_sd = _zscore_series(d["working_gas_bcf"])
    d["storage_z"] = storage_z

    # --- Lag all predictors by 1 day (avoid lookahead) ---
    d["op_stress_lag1"] = d["op_stress"].shift(1)
    d["stress_days_7d_lag1"] = d["stress_days_7d"].shift(1)
    d["hdd_5d_lag1"] = d["hdd_5d"].shift(1)
    d["storage_z_lag1"] = pd.Series(d["storage_z"]).shift(1)

    feat_cols = [
        "op_stress_lag1",
        "stress_days_7d_lag1",
        "hdd_5d_lag1",
        "storage_z_lag1",
    ]
    d = d.dropna(subset=["y"] + feat_cols).copy()

    # --- Build design vectors (standardize continuous features) ---
    X_op = d["op_stress_lag1"].to_numpy(dtype=float)

    X_persist, mu_persist, sd_persist = _zscore_series(d["stress_days_7d_lag1"])
    X_hdd, mu_hdd, sd_hdd = _zscore_series(d["hdd_5d_lag1"])
    X_storage, mu_storage, sd_storage = _zscore_series(d["storage_z_lag1"])

    y = d["y"].to_numpy(dtype=float)

    scalers = {
        "persist": {"mean": mu_persist, "std": sd_persist},
        "hdd": {"mean": mu_hdd, "std": sd_hdd},
        "storage": {"mean": mu_storage, "std": sd_storage},
        # op is binary; typically no z-scoring
    }

    with pm.Model() as m:
        a = pm.Normal("a", 0.0, 0.5)

        b_op = pm.Normal("b_op", 0.0, 0.5)
        b_persist = pm.Normal("b_persist", 0.0, 0.5)
        b_hdd = pm.Normal("b_hdd", 0.0, 0.5)
        b_storage = pm.Normal("b_storage", 0.0, 0.5)

        mu = (
            a
            + b_op * X_op
            + b_persist * X_persist
            + b_hdd * X_hdd
            + b_storage * X_storage
        )

        # Heavy-tail likelihood (robust)
        nu = pm.Exponential("nu", 1 / 10) + 2.0
        sigma = pm.HalfNormal("sigma", 0.5)

        pm.StudentT("y_obs", nu=nu, mu=mu, sigma=sigma, observed=y)

        idata = pm.sample(
            tune=1000, draws=1000, target_accept=0.9, chains=4, progressbar=True
        )

    return m, idata, scalers
