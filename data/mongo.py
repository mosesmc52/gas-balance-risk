from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Sequence, Tuple

import pandas as pd
from pymongo import MongoClient


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db: str


def get_mongo_config(
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
) -> MongoConfig:
    """
    Read Mongo config from args or env vars.

    Env vars expected:
      - MONGO_URI
      - MONGO_DB
    """
    uri = mongo_uri or os.getenv("MONGO_URI")
    db = mongo_db or os.getenv("MONGO_DB", "gas_model")
    if not uri:
        raise ValueError("Missing Mongo URI. Provide mongo_uri=... or set MONGO_URI.")
    return MongoConfig(uri=uri, db=db)


def load_mongo_df(
    *,
    collection: str,
    query: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, int]] = None,
    sort: Optional[Sequence[Tuple[str, int]]] = None,
    limit: Optional[int] = None,
    date_col: str = "date",
    parse_dates: bool = True,
    drop_mongo_id: bool = True,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
) -> pd.DataFrame:
    """
    Load documents from MongoDB into a DataFrame.

    Parameters
    ----------
    collection:
        Mongo collection name (e.g., "eia_hh_spot_daily")
    query:
        Mongo query dict (default: {})
    projection:
        Mongo projection dict (e.g., {"_id": 0, "date": 1, "value": 1})
        If None, returns all fields.
    sort:
        Optional list of (field, direction), e.g. [("date", 1)]
    limit:
        Optional maximum docs to return.
    date_col:
        Column to parse as dates if parse_dates=True.
    parse_dates:
        If True, convert date_col to pandas datetime.
    drop_mongo_id:
        If True, drop "_id" if present.

    Returns
    -------
    pd.DataFrame
    """
    cfg = get_mongo_config(mongo_uri=mongo_uri, mongo_db=mongo_db)
    client = MongoClient(cfg.uri)
    try:
        col = client[cfg.db][collection]

        q = query or {}
        cursor = col.find(q, projection)

        if sort:
            cursor = cursor.sort(list(sort))
        if limit:
            cursor = cursor.limit(int(limit))

        docs = list(cursor)
        if not docs:
            return pd.DataFrame()

        df = pd.DataFrame(docs)

        if drop_mongo_id and "_id" in df.columns:
            df = df.drop(columns=["_id"])

        if parse_dates and date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        return df

    finally:
        client.close()


def load_henry_hub_daily(
    *,
    start: str | None = None,
    end: str | None = None,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
) -> pd.DataFrame:
    """
    Convenience loader for Henry Hub spot prices stored by your script.

    Collection: eia_hh_spot_daily
    Fields: date, value, units, series
    """
    query: Dict[str, Any] = {}
    if start or end:
        query["date"] = {}
        if start:
            query["date"]["$gte"] = start
        if end:
            query["date"]["$lte"] = end

    df = load_mongo_df(
        collection="eia_hh_spot_daily",
        query=query,
        projection={"_id": 0, "date": 1, "value": 1, "units": 1, "series": 1},
        sort=[("date", 1)],
        date_col="date",
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )

    if not df.empty:
        df = df.rename(columns={"value": "henry_hub_usd_per_mmbtu"})
    return df


def load_storage_weekly(
    *,
    start: str | None = None,
    end: str | None = None,
    region: str | None = None,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
) -> pd.DataFrame:
    """
    Convenience loader for EIA working gas in storage (weekly).

    Collection: eia_storage_weekly
    Fields: date, value, units, series, region
    """
    query: Dict[str, Any] = {}
    if start or end:
        query["date"] = {}
        if start:
            query["date"]["$gte"] = start
        if end:
            query["date"]["$lte"] = end
    if region:
        query["region"] = region

    df = load_mongo_df(
        collection="eia_storage_weekly",
        query=query,
        projection={
            "_id": 0,
            "date": 1,
            "value": 1,
            "units": 1,
            "series": 1,
            "region": 1,
        },
        sort=[("date", 1)],
        date_col="date",
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )

    if not df.empty:
        df = df.rename(columns={"value": "working_gas_bcf"})
    return df


def load_noaa_region_daily(
    *,
    pipeline: str,
    start: str | None = None,
    end: str | None = None,
    region_id: str | None = None,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
) -> pd.DataFrame:
    """
    Load daily NOAA regional HDD data for a given pipeline.

    Collection: noaa_region_daily

    Fields expected:
      - date
      - pipeline
      - region_id
      - hdd_mean
      - hdd_median
      - n_stations_used
      - source

    Parameters
    ----------
    pipeline:
        Pipeline identifier (e.g. "algonquin")
    start, end:
        Optional date filters (YYYY-MM-DD)
    region_id:
        Optional region identifier (if you store multiple regions per pipeline)

    Returns
    -------
    pd.DataFrame sorted by date
    """
    query: dict[str, Any] = {"pipeline": pipeline}

    if start or end:
        query["date"] = {}
        if start:
            query["date"]["$gte"] = start
        if end:
            query["date"]["$lte"] = end

    if region_id:
        query["region_id"] = region_id

    df = load_mongo_df(
        collection="noaa_region_daily",
        query=query,
        projection={
            "_id": 0,
            "date": 1,
            "pipeline": 1,
            "region_id": 1,
            "hdd_mean": 1,
            "hdd_median": 1,
            "n_stations_used": 1,
            "source": 1,
        },
        sort=[("date", 1)],
        date_col="date",
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )

    return df


def load_capacity_df(
    *,
    collection: str,
    start: str | None = None,
    end: str | None = None,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Load capacity rows from a pipeline-specific capacity collection.

    Assumes collection already corresponds to a single pipeline
    (e.g. 'algonquin_capacity').

    Filters by Post_Date if provided.
    """
    query: dict[str, Any] = {}

    if start or end:
        query["Post_Date"] = {}
        if start:
            query["Post_Date"]["$gte"] = start
        if end:
            query["Post_Date"]["$lte"] = end

    df = load_mongo_df(
        collection=collection,
        query=query,
        projection={
            "_id": 0,
            "Loc_Name": 1,
            "Post_Date": 1,
            "TSP": 1,
            "All_Qty_Avail": 1,
            "Cap_Type_Desc": 1,
            "Cycle_Desc": 1,
            "Eff_Gas_Day": 1,
            "Eff_Time": 1,
            "Flow_Ind_Desc": 1,
            "IT": 1,
            "Loc": 1,
            "Loc_Purp_Desc": 1,
            "Loc_QTI_Desc": 1,
            "Loc_Zn": 1,
            "Meas_Basis_Desc": 1,
            "Operating_Capacity": 1,
            "Operationally_Available_Capacity": 1,
            "Post_Time": 1,
            "TSP_Name": 1,
            "Total_Design_Capacity": 1,
            "Total_Scheduled_Quantity": 1,
            "_meta": 1,
            "downloaded_at_utc": 1,
            "source_url": 1,
        },
        sort=[("Post_Date", 1), ("Post_Time", 1)],
        limit=limit,
        parse_dates=False,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )

    if df.empty:
        return df

    # Parse date-like fields explicitly
    df["Post_Date_dt"] = pd.to_datetime(df.get("Post_Date"), errors="coerce")
    df["Eff_Gas_Day_dt"] = pd.to_datetime(df.get("Eff_Gas_Day"), errors="coerce")
    df["downloaded_at_utc"] = pd.to_datetime(
        df.get("downloaded_at_utc"), errors="coerce"
    )

    return df


def _to_utc_dt(s: str) -> datetime:
    """
    Convert 'YYYY-MM-DD' or ISO string to timezone-aware UTC datetime.
    """
    ts = pd.to_datetime(s, utc=True, errors="raise")
    return ts.to_pydatetime()


def load_notices_df(
    *,
    collection: str,
    start: str | None = None,
    end: str | None = None,
    mongo_uri: str | None = None,
    mongo_db: str | None = None,
    limit: int | None = None,
    only_active: bool = False,
) -> pd.DataFrame:
    """
    Load notices from a pipeline-specific notices collection.

    posted_dt is stored as BSON Date (ISODate), so start/end must be datetimes.
    """
    query: dict[str, Any] = {}

    if start or end:
        query["posted_dt"] = {}
        if start:
            query["posted_dt"]["$gte"] = _to_utc_dt(start)
        if end:
            # inclusive end-of-day behavior:
            end_dt = pd.to_datetime(end, utc=True).to_pydatetime()
            # Move to end of that day (23:59:59.999) in UTC
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
            query["posted_dt"]["$lte"] = end_dt

    if only_active:
        now_utc = datetime.now(timezone.utc)
        query["$or"] = [{"end_dt": None}, {"end_dt": {"$gte": now_utc}}]

    df = load_mongo_df(
        collection=collection,
        query=query,
        projection={
            "_id": 0,
            "notice_id": 1,
            "posted_dt": 1,
            "tsp": 1,
            "_meta": 1,
            "body": 1,
            "critical": 1,
            "effective_dt": 1,
            "end_dt": 1,
            "kind": 1,
            "name": 1,
            "prior_id": 1,
            "response": 1,
            "status": 1,
            "subject": 1,
            "type": 1,
            "url": 1,
        },
        sort=[("posted_dt", 1)],
        limit=limit,
        parse_dates=False,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )

    if df.empty:
        return df

    # These are already datetimes when stored as ISODate, but parse defensively.
    df["posted_dt"] = pd.to_datetime(df.get("posted_dt"), utc=True, errors="coerce")
    df["effective_dt"] = pd.to_datetime(
        df.get("effective_dt"), utc=True, errors="coerce"
    )
    df["end_dt"] = pd.to_datetime(df.get("end_dt"), utc=True, errors="coerce")

    return df
