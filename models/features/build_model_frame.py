from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from data.mongo import (
    load_capacity_df,
    load_henry_hub_daily,
    load_noaa_region_daily,
    load_notices_df,
    load_storage_weekly,
)


@dataclass(frozen=True)
class ModelFrameConfig:
    pipeline: str
    capacity_collection: str
    notices_collection: str
    noaa_region_id: str | None = None
    start: str = "2023-01-01"
    end: str | None = None


def _ensure_daily_index(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).sort_values(date_col)
    df = df.drop_duplicates(subset=[date_col], keep="last")
    return df


def _build_stress_from_notices(notices: pd.DataFrame) -> pd.DataFrame:
    """
    Convert notice records into a daily stress signal.

    Strategy (simple + robust):
    - Create a daily active-notice count time series
    - Create a daily active-critical flag
    - Create stress_event = 1 if any critical notice active that day
      (you can expand later to include maintenance types, etc.)
    """
    if notices.empty:
        return pd.DataFrame(
            columns=["date", "notice_active_count", "critical_active", "stress_event"]
        )

    n = notices.copy()

    # Expect these columns after load_notices_df parsing:
    # posted_dt, effective_dt, end_dt, critical
    n["effective_dt"] = pd.to_datetime(n.get("effective_dt"), utc=True, errors="coerce")
    n["end_dt"] = pd.to_datetime(n.get("end_dt"), utc=True, errors="coerce")
    n["posted_dt"] = pd.to_datetime(n.get("posted_dt"), utc=True, errors="coerce")

    # Fill missing effective_dt with posted_dt (common)
    n["effective_dt"] = n["effective_dt"].fillna(n["posted_dt"])

    # Open-ended notices: set end_dt = effective_dt (same-day) as a conservative default
    # You can instead set to "today" during live scoring; for training use a cap.
    n["end_dt"] = n["end_dt"].fillna(n["effective_dt"])

    # Make date-only bounds for daily expansion
    start_date = n["effective_dt"].dt.floor("D")
    end_date = n["end_dt"].dt.floor("D")

    # Expand to daily rows (vectorized-ish using explode)
    n = n.assign(
        date_range=[pd.date_range(s, e, freq="D") for s, e in zip(start_date, end_date)]
    )
    daily = n.explode("date_range").rename(columns={"date_range": "date"})

    # Aggregate daily features
    daily["date"] = pd.to_datetime(daily["date"]).dt.tz_localize(None)
    daily["critical"] = daily.get("critical", False).fillna(False).astype(bool)

    out = (
        daily.groupby("date", as_index=False)
        .agg(
            notice_active_count=("notice_id", "nunique"),
            critical_active=("critical", "max"),
        )
        .sort_values("date")
    )
    out["stress_event"] = (out["critical_active"]).astype(int)
    return out


def _weekly_to_daily_ffill(
    weekly: pd.DataFrame, date_col: str, value_cols: list[str]
) -> pd.DataFrame:
    """
    Convert weekly series to daily by forward-filling.
    """
    if weekly.empty:
        return weekly

    w = weekly.copy()
    w[date_col] = pd.to_datetime(w[date_col], errors="coerce")
    w = w.dropna(subset=[date_col]).sort_values(date_col)

    w = w.set_index(date_col)[value_cols].sort_index()
    # Daily reindex to full range
    daily_index = pd.date_range(w.index.min(), w.index.max(), freq="D")
    w_daily = w.reindex(daily_index).ffill()
    w_daily.index.name = "date"
    w_daily = w_daily.reset_index()
    return w_daily


def build_model_frame(
    cfg: ModelFrameConfig,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
) -> pd.DataFrame:
    """
    Build a single daily training frame for a given pipeline.

    Returns a DataFrame with daily rows and merged drivers + stress target.
    """
    # --- Load drivers ---
    weather = load_noaa_region_daily(
        pipeline=cfg.pipeline,
        start=cfg.start,
        end=cfg.end,
        region_id=cfg.noaa_region_id,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )
    weather = _ensure_daily_index(weather, "date")

    hh = load_henry_hub_daily(
        start=cfg.start, end=cfg.end, mongo_uri=mongo_uri, mongo_db=mongo_db
    )
    hh = _ensure_daily_index(hh, "date")

    storage_w = load_storage_weekly(
        start=cfg.start,
        end=cfg.end,
        region="lower48",
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )
    # storage loader returns 'date' and 'working_gas_bcf'
    storage_d = _weekly_to_daily_ffill(storage_w, "date", ["working_gas_bcf"])

    # --- Load operational signals ---
    notices = load_notices_df(
        collection=cfg.notices_collection,
        start=cfg.start,
        end=cfg.end,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )
    stress_daily = _build_stress_from_notices(notices)

    # Capacity is usually “intraday snapshot style”; for the first model frame,
    # you often don't need it unless you're deriving constraints vs normal.
    # Still load it here so you can add features later.
    cap = load_capacity_df(
        collection=cfg.capacity_collection,
        start=cfg.start,
        end=cfg.end,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )
    # Example capacity feature: daily median All_Qty_Avail
    cap_feat = pd.DataFrame(columns=["date", "all_qty_avail_median"])
    if (
        not cap.empty
        and "Post_Date_dt" in cap.columns
        and "All_Qty_Avail" in cap.columns
    ):
        tmp = cap.copy()
        tmp["date"] = pd.to_datetime(tmp["Post_Date_dt"], errors="coerce")
        tmp["All_Qty_Avail"] = pd.to_numeric(tmp["All_Qty_Avail"], errors="coerce")
        cap_feat = (
            tmp.dropna(subset=["date"])
            .groupby(tmp["date"].dt.floor("D"), as_index=False)
            .agg(all_qty_avail_median=("All_Qty_Avail", "median"))
            .rename(columns={"date": "date"})
            .sort_values("date")
        )

    # --- Merge everything on daily date ---
    # Establish the daily calendar from weather (preferred) else HH else stress
    base = weather[
        [
            "date",
            "pipeline",
            "region_id",
            "hdd_mean",
            "hdd_median",
            "n_stations_used",
            "source",
        ]
    ].copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce")

    df = base.merge(hh[["date", "henry_hub_usd_per_mmbtu"]], on="date", how="left")
    if not storage_d.empty:
        df = df.merge(storage_d[["date", "working_gas_bcf"]], on="date", how="left")
    df = df.merge(stress_daily, on="date", how="left")
    df = df.merge(cap_feat, on="date", how="left")

    # Fill stress columns for days with no notices
    for col in ["notice_active_count", "critical_active", "stress_event"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    df["critical_active"] = df.get("critical_active", 0).astype(int)
    df["stress_event"] = df.get("stress_event", 0).astype(int)

    # Optional: basic derived features for model stability
    # Return (log) for Henry Hub if present
    if "henry_hub_usd_per_mmbtu" in df.columns:
        x = df["henry_hub_usd_per_mmbtu"].astype(float)
        df["hh_log"] = np.log(x.replace(0, np.nan))
        df["hh_ret"] = df["hh_log"].diff()

    # Clean final ordering
    df = df.sort_values("date").reset_index(drop=True)

    return df
