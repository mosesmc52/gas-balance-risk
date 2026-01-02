import numpy as np
import pandas as pd


def compute_storage_z(daily_storage: pd.Series, window: int = 260) -> pd.Series:
    """
    Simple rolling z-score (1y ~ 260 business days, or ~365 calendar).
    For weekly storage forward-filled daily, this is fine for MVP.
    """
    mu = daily_storage.rolling(window, min_periods=20).mean()
    sd = daily_storage.rolling(window, min_periods=20).std()
    return (daily_storage - mu) / (sd.replace(0, np.nan))


def build_model_frame(
    stress_daily_csv: str,
    noaa_region_daily_csv: str,
    eia_storage_csv: str,
    price_daily_csv: str | None = None,
) -> pd.DataFrame:
    stress = pd.read_csv(stress_daily_csv)
    noaa = pd.read_csv(noaa_region_daily_csv)
    eia = pd.read_csv(eia_storage_csv)

    for df in (stress, noaa, eia):
        df["date"] = pd.to_datetime(df["date"])

    # Join stress + weather
    df = stress.merge(
        noaa[["pipeline", "date", "hdd_median", "tavg_f_median", "n_stations_used"]],
        on=["pipeline", "date"],
        how="left",
    )

    # Storage weekly -> daily forward-fill
    eia = eia.sort_values("date").set_index("date")
    eia_daily = eia.resample("D").ffill().reset_index()
    eia_daily.rename(columns={"wk_gas": "storage"}, inplace=True)

    df = df.merge(eia_daily[["date", "storage"]], on="date", how="left")
    df["storage"] = df["storage"].ffill()

    # Simple storage regime
    df["storage_z"] = compute_storage_z(df["storage"], window=365)

    # HDD persistence
    df = df.sort_values(["pipeline", "date"])
    df["hdd_5d"] = df.groupby("pipeline")["hdd_median"].transform(
        lambda s: s.rolling(5, min_periods=1).sum()
    )

    # Optional: price-based target
    if price_daily_csv:
        px = pd.read_csv(price_daily_csv)
        px["date"] = pd.to_datetime(px["date"])
        px = px.sort_values("date")
        px["logp"] = np.log(px["hh_spot"].replace(0, np.nan))
        px["y_ret"] = px["logp"].diff()
        px["y_abs"] = px["y_ret"].abs()

        df = df.merge(px[["date", "y_ret", "y_abs"]], on="date", how="left")

        # Lagged y for persistence (only if you have it)
        df["y_abs_lag1"] = df["y_abs"].shift(1)
        df["y_ret_lag1"] = df["y_ret"].shift(1)

    return df
