import numpy as np


def forecast_vol_risk(
    m, idata, x_op, x_persist, x_hdd, x_storage, threshold: float = 0.02
):
    """
    Returns:
      - posterior predictive samples for y
      - P(y > threshold)
    """

    with m:
        # Set new predictors by building mu manually for a single forecast point
        a = idata.posterior["a"].values.reshape(-1)
        b_op = idata.posterior["b_op"].values.reshape(-1)
        b_persist = idata.posterior["b_persist"].values.reshape(-1)
        b_hdd = idata.posterior["b_hdd"].values.reshape(-1)
        b_storage = idata.posterior["b_storage"].values.reshape(-1)
        sigma = idata.posterior["sigma"].values.reshape(-1)
        nu = idata.posterior["nu"].values.reshape(-1)

        mu = (
            a
            + b_op * x_op
            + b_persist * x_persist
            + b_hdd * x_hdd
            + b_storage * x_storage
        )

        # Sample predictive y from StudentT per draw
        y_samp = np.random.standard_t(df=nu, size=mu.shape[0]) * sigma + mu

    prob_exceed = float(np.mean(y_samp > threshold))
    return y_samp, prob_exceed
