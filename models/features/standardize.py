import numpy as np
import pandas as pd


def zscore(
    s: pd.Series,
    mean_: float | None = None,
    std_: float | None = None,
    eps: float = 1e-8,
) -> tuple[np.ndarray, float, float]:
    """
    Z-score a pandas Series.

    Returns:
      z: np.ndarray
      mean_: float
      std_: float

    If mean_ / std_ are provided, use them (for inference).
    """
    x = s.to_numpy(dtype=float)

    if mean_ is None:
        mean_ = float(np.nanmean(x))
    if std_ is None:
        std_ = float(np.nanstd(x))

    std_ = std_ if std_ > eps else 1.0
    z = (x - mean_) / std_

    return z, mean_, std_
