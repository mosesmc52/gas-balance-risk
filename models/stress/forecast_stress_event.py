from __future__ import annotations

from typing import Dict, Optional, Tuple

import numpy as np


def _z(x: float, mean: float, std: float) -> float:
    if std == 0 or np.isnan(std):
        return 0.0
    return float((x - mean) / std)


def forecast_stress_event_prob(
    m,
    idata,
    *,
    x: Dict[str, float],
    scalers: Optional[Dict[str, Dict[str, float]]] = None,
    prob_threshold: float = 0.30,
    intercept_name: str = "a",
    coef_prefix: str = "b_",
) -> Tuple[np.ndarray, float]:
    """
    Forecast stress-event probability for ONE point from a Bernoulli/logistic model.

    Returns
    -------
    p_samp: np.ndarray
        Posterior samples of p(event=1) (one per posterior draw).
    prob_alert: float
        P(p > prob_threshold)
    """
    post = idata.posterior

    a = post[intercept_name].values.reshape(-1).astype(float)
    logit_p = a.copy()

    for feat_name, feat_val_raw in x.items():
        coef_name = f"{coef_prefix}{feat_name}"
        if coef_name not in post:
            continue

        b = post[coef_name].values.reshape(-1).astype(float)
        feat_val = float(feat_val_raw)

        if scalers is not None and feat_name in scalers:
            mean = float(scalers[feat_name].get("mean", 0.0))
            std = float(scalers[feat_name].get("std", 1.0))
            feat_val = _z(feat_val, mean, std)

        logit_p = logit_p + b * feat_val

    p_samp = 1.0 / (1.0 + np.exp(-logit_p))
    prob_alert = float(np.mean(p_samp > prob_threshold))
    return p_samp, prob_alert
