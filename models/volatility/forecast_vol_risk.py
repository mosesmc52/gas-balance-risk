from __future__ import annotations

from typing import Dict, Optional, Tuple

import numpy as np


def _z(x: float, mean: float, std: float) -> float:
    if std == 0 or np.isnan(std):
        return 0.0
    return float((x - mean) / std)


def forecast_vol_risk(
    m,
    idata,
    *,
    x: Dict[str, float],
    scalers: Optional[Dict[str, Dict[str, float]]] = None,
    threshold: float = 0.02,
    intercept_name: str = "a",
    sigma_name: str = "sigma",
    nu_name: str = "nu",
    coef_prefix: str = "b_",
    min_nu: float = 2.1,
) -> Tuple[np.ndarray, float]:
    """
    One-step forecast from a fitted StudentT volatility-risk model.

    Returns
    -------
    y_samp: np.ndarray
        Posterior predictive samples of y (one per posterior draw).
    prob_exceed: float
        Monte Carlo estimate of P(y > threshold).
    """
    post = idata.posterior
    vars_ = set(post.data_vars)

    # Flatten draws across chains
    a = post[intercept_name].values.reshape(-1).astype(float)
    sigma = post[sigma_name].values.reshape(-1).astype(float)
    nu = post[nu_name].values.reshape(-1).astype(float)

    # Safety: avoid invalid t df
    nu = np.maximum(nu, float(min_nu))

    mu = a.copy()

    for feat_name, feat_val_raw in x.items():
        coef_name = f"{coef_prefix}{feat_name}"
        if coef_name not in vars_:
            continue

        b = post[coef_name].values.reshape(-1).astype(float)
        feat_val = float(feat_val_raw)

        if scalers is not None and feat_name in scalers:
            mean = float(scalers[feat_name].get("mean", 0.0))
            std = float(scalers[feat_name].get("std", 1.0))
            feat_val = _z(feat_val, mean, std)

        mu = mu + b * feat_val

    # Sample predictive y per posterior draw
    t = np.random.standard_t(df=nu, size=mu.shape[0])
    y_samp = mu + sigma * t

    prob_exceed = float(np.mean(y_samp > threshold))
    return y_samp, prob_exceed
