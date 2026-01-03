from __future__ import annotations

import pandas as pd


def derive_forecast_inputs(df: pd.DataFrame) -> dict[str, float]:
    """
    Derive x_* inputs for the volatility-risk forecast
    from the latest available model frame.
    """
    d = df.sort_values("date").copy()

    if len(d) < 2:
        raise ValueError("Not enough data to derive forecast inputs")

    x_op = d["stress_event"].rolling(3).mean().iloc[-1]

    x_persist = d["hh_ret"].abs().rolling(3).mean().iloc[-1]

    x_hdd = d["hdd_median"].iloc[-1]

    x_storage = (
        d["working_gas_bcf"].iloc[-1]
        - d["working_gas_bcf"].rolling(365 * 3).mean().iloc[-1]
    )

    return {
        "op": float(x_op),
        "persist": float(x_persist),
        "hdd": float(x_hdd),
        "storage": float(x_storage),
    }
