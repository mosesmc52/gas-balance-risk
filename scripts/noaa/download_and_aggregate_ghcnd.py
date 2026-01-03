#!/usr/bin/env python3
"""
Download NOAA GHCND daily station CSVs from:
  https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/

Filter stations from an input CSV where pipeline == ..., download station files,
then compute a region-level daily aggregation and save.

Optionally upsert station-day and region-day results into MongoDB.

Expected station CSV columns (minimum):
  - ghcnd_station_id
  - pipeline
Optional:
  - station_name
  - state
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Optional dependency
try:
    from pymongo import MongoClient, UpdateOne
except Exception:  # pragma: no cover
    MongoClient = None
    UpdateOne = None

load_dotenv()

NOAA_GHCND_ACCESS_BASE = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
)


# -----------------------------
# Data structures ("items")
# -----------------------------


@dataclass(frozen=True)
class StationMetaItem:
    pipeline: str
    ghcnd_station_id: str
    station_name: Optional[str] = None
    state: Optional[str] = None


# -----------------------------
# Helpers
# -----------------------------


def c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def compute_hdd_from_tavg_c(tavg_c: float, base_f: float = 65.0) -> float:
    """HDD = max(0, baseF - TavgF)."""
    tavg_f = c_to_f(tavg_c)
    return max(0.0, base_f - tavg_f)


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def download_station_csv(station_id: str, out_dir: str, timeout: int = 60) -> str:
    safe_mkdir(out_dir)
    url = f"{NOAA_GHCND_ACCESS_BASE}{station_id}.csv"
    out_path = os.path.join(out_dir, f"{station_id}.csv")

    # simple cache
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return out_path

    r = requests.get(url, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(
            f"Failed download {station_id}: HTTP {r.status_code} url={url}"
        )

    with open(out_path, "wb") as f:
        f.write(r.content)

    return out_path


def read_and_normalize_station_file(
    station_id: str,
    filepath: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> pd.DataFrame:
    """
    Normalize station data to:
      pipeline-independent fields: station_id, date, tavg_c, tmin_c, tmax_c, hdd
    Temperatures are tenths of °C in the NOAA access files.
    """
    df = pd.read_csv(filepath)

    if "DATE" not in df.columns:
        raise ValueError(f"{station_id}: missing DATE column in {filepath}")

    cols = ["DATE"]
    for c in ("TAVG", "TMIN", "TMAX"):
        if c in df.columns:
            cols.append(c)
    df = df[cols].copy()
    df.rename(columns={"DATE": "date"}, inplace=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date)]

    # Convert tenths °C -> °C
    for col in ("TAVG", "TMIN", "TMAX"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 10.0

    # Prefer TAVG; fallback to mean(TMIN, TMAX)
    df["tavg_c"] = df["TAVG"] if "TAVG" in df.columns else pd.NA
    df["tmin_c"] = df["TMIN"] if "TMIN" in df.columns else pd.NA
    df["tmax_c"] = df["TMAX"] if "TMAX" in df.columns else pd.NA

    mask_fill = df["tavg_c"].isna() & df["tmin_c"].notna() & df["tmax_c"].notna()
    df.loc[mask_fill, "tavg_c"] = (
        df.loc[mask_fill, "tmin_c"] + df.loc[mask_fill, "tmax_c"]
    ) / 2.0

    def _hdd(x):
        if pd.isna(x):
            return pd.NA
        return compute_hdd_from_tavg_c(float(x), base_f=65.0)

    df["hdd"] = df["tavg_c"].apply(_hdd)

    df["ghcnd_station_id"] = station_id
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    return df[["ghcnd_station_id", "date", "tavg_c", "tmin_c", "tmax_c", "hdd"]].copy()


def aggregate_region_daily(
    df_all: pd.DataFrame, pipeline: str, region_id: str
) -> pd.DataFrame:
    """
    Aggregate across stations by day.
    Outputs both median and mean. Median is recommended for robustness.
    """
    df = df_all.copy()
    for col in ("tavg_c", "hdd"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df_valid = df.dropna(subset=["tavg_c"]).copy()

    g = df_valid.groupby("date", as_index=False)
    agg = g.agg(
        n_stations_used=("ghcnd_station_id", "nunique"),
        tavg_c_median=("tavg_c", "median"),
        tavg_c_mean=("tavg_c", "mean"),
        hdd_median=("hdd", "median"),
        hdd_mean=("hdd", "mean"),
    ).sort_values("date")

    agg["tavg_f_median"] = agg["tavg_c_median"].apply(
        lambda x: c_to_f(float(x)) if pd.notna(x) else pd.NA
    )
    agg["tavg_f_mean"] = agg["tavg_c_mean"].apply(
        lambda x: c_to_f(float(x)) if pd.notna(x) else pd.NA
    )

    agg.insert(0, "pipeline", pipeline)
    agg.insert(1, "region_id", region_id)

    return agg[
        [
            "pipeline",
            "region_id",
            "date",
            "n_stations_used",
            "tavg_c_median",
            "tavg_f_median",
            "hdd_median",
            "tavg_c_mean",
            "tavg_f_mean",
            "hdd_mean",
        ]
    ].copy()


def load_station_meta(csv_path: str, pipeline_filter: str) -> List[StationMetaItem]:
    df = pd.read_csv(csv_path)

    required = {"pipeline", "ghcnd_station_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"stations csv missing columns: {sorted(missing)}")

    df["pipeline"] = df["pipeline"].astype(str).str.strip().str.lower()
    df["ghcnd_station_id"] = df["ghcnd_station_id"].astype(str).str.strip()

    df = df[df["pipeline"] == pipeline_filter.strip().lower()].copy()
    if df.empty:
        raise ValueError(f"No stations found for pipeline == {pipeline_filter!r}")

    items: List[StationMetaItem] = []
    for _, r in df.iterrows():
        items.append(
            StationMetaItem(
                pipeline=str(r["pipeline"]),
                ghcnd_station_id=str(r["ghcnd_station_id"]),
                station_name=(
                    str(r["station_name"])
                    if "station_name" in df.columns and pd.notna(r.get("station_name"))
                    else None
                ),
                state=(
                    str(r["state"])
                    if "state" in df.columns and pd.notna(r.get("state"))
                    else None
                ),
            )
        )

    # De-duplicate station ids
    seen = set()
    out = []
    for it in items:
        if it.ghcnd_station_id not in seen:
            out.append(it)
            seen.add(it.ghcnd_station_id)
    return out


# -----------------------------
# MongoDB sink
# -----------------------------


def mongo_upsert_weather(
    *,
    mongo_uri: str,
    mongo_db: str,
    pipeline: str,
    stations_meta: List[StationMetaItem],
    df_station_norm: pd.DataFrame,
    df_region_daily: pd.DataFrame,
    station_collection: str,
    region_collection: str,
    batch_size: int = 2000,
) -> Dict[str, Any]:
    """
    Upsert station-day and region-day documents.

    Station unique key: (pipeline, ghcnd_station_id, date)
    Region unique key:  (pipeline, region_id, date)
    """
    if MongoClient is None or UpdateOne is None:
        raise RuntimeError(
            "pymongo is not installed. Install with: pip install pymongo"
        )

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    col_station = db[station_collection]
    col_region = db[region_collection]

    # Ensure indexes (safe to call repeatedly)
    col_station.create_index(
        [("pipeline", 1), ("ghcnd_station_id", 1), ("date", 1)], unique=True
    )
    col_region.create_index(
        [("pipeline", 1), ("region_id", 1), ("date", 1)], unique=True
    )

    # Station meta map
    meta_by_id = {s.ghcnd_station_id: asdict(s) for s in stations_meta}

    # Station documents
    ops = []
    station_upserts = 0
    for rec in df_station_norm.to_dict(orient="records"):
        sid = rec["ghcnd_station_id"]
        doc = {
            "pipeline": pipeline,
            "ghcnd_station_id": sid,
            "date": rec["date"],
            "tavg_c": None if pd.isna(rec.get("tavg_c")) else float(rec["tavg_c"]),
            "tmin_c": None if pd.isna(rec.get("tmin_c")) else float(rec["tmin_c"]),
            "tmax_c": None if pd.isna(rec.get("tmax_c")) else float(rec["tmax_c"]),
            "hdd": None if pd.isna(rec.get("hdd")) else float(rec["hdd"]),
            "station_meta": meta_by_id.get(sid, {}),
            "updated_at_utc": datetime.now(timezone.utc),
            "source": {
                "provider": "NOAA",
                "dataset": "GHCND",
                "access_base": NOAA_GHCND_ACCESS_BASE,
            },
        }

        ops.append(
            UpdateOne(
                {"pipeline": pipeline, "ghcnd_station_id": sid, "date": rec["date"]},
                {"$set": doc},
                upsert=True,
            )
        )

        if len(ops) >= batch_size:
            res = col_station.bulk_write(ops, ordered=False)
            station_upserts += (res.upserted_count or 0) + (res.modified_count or 0)
            ops = []

    if ops:
        res = col_station.bulk_write(ops, ordered=False)
        station_upserts += (res.upserted_count or 0) + (res.modified_count or 0)

    # Region documents
    ops = []
    region_upserts = 0
    for rec in df_region_daily.to_dict(orient="records"):
        doc = {
            "pipeline": rec["pipeline"],
            "region_id": rec["region_id"],
            "date": rec["date"],
            "n_stations_used": int(rec["n_stations_used"]),
            "tavg_c_median": (
                None
                if pd.isna(rec.get("tavg_c_median"))
                else float(rec["tavg_c_median"])
            ),
            "tavg_f_median": (
                None
                if pd.isna(rec.get("tavg_f_median"))
                else float(rec["tavg_f_median"])
            ),
            "hdd_median": (
                None if pd.isna(rec.get("hdd_median")) else float(rec["hdd_median"])
            ),
            "tavg_c_mean": (
                None if pd.isna(rec.get("tavg_c_mean")) else float(rec["tavg_c_mean"])
            ),
            "tavg_f_mean": (
                None if pd.isna(rec.get("tavg_f_mean")) else float(rec["tavg_f_mean"])
            ),
            "hdd_mean": (
                None if pd.isna(rec.get("hdd_mean")) else float(rec["hdd_mean"])
            ),
            "updated_at_utc": datetime.now(timezone.utc),
            "source": {
                "provider": "NOAA",
                "dataset": "GHCND",
                "access_base": NOAA_GHCND_ACCESS_BASE,
            },
        }

        ops.append(
            UpdateOne(
                {
                    "pipeline": doc["pipeline"],
                    "region_id": doc["region_id"],
                    "date": doc["date"],
                },
                {"$set": doc},
                upsert=True,
            )
        )

        if len(ops) >= batch_size:
            res = col_region.bulk_write(ops, ordered=False)
            region_upserts += (res.upserted_count or 0) + (res.modified_count or 0)
            ops = []

    if ops:
        res = col_region.bulk_write(ops, ordered=False)
        region_upserts += (res.upserted_count or 0) + (res.modified_count or 0)

    client.close()

    return {
        "mongo_db": mongo_db,
        "station_collection": station_collection,
        "region_collection": region_collection,
        "station_upserts_or_updates": station_upserts,
        "region_upserts_or_updates": region_upserts,
    }


# -----------------------------
# Main
# -----------------------------


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--stations-csv",
        default="scripts/noaa/pipeline_airport_ghcnd_mapping.csv",
        required=False,
        help="Path to stations CSV",
    )
    p.add_argument(
        "--pipeline",
        default="algonquin",
        help="Filter pipeline name (default: algonquin)",
    )
    p.add_argument("--out-dir", default="data/noaa", help="Output directory")
    p.add_argument("--start", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument(
        "--days_ago", type=int, default=0, help="The number of days ago to start"
    )
    p.add_argument("--end", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")

    # Mongo options
    p.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", None),
        help="MongoDB URI (optional). If set, upserts to Mongo.",
    )
    p.add_argument(
        "--mongo-db", default=os.getenv("MONGO_DB", None), help="MongoDB database name."
    ),
    p.add_argument(
        "--mongo-station-collection",
        default="noaa_station_daily",
        help="Collection for station-day docs",
    )
    p.add_argument(
        "--mongo-region-collection",
        default="noaa_region_daily",
        help="Collection for region-day docs",
    )

    args = p.parse_args()

    pipeline = args.pipeline.strip().lower()
    region_id = pipeline  # simple default

    # 1) Load stations
    stations = load_station_meta(args.stations_csv, pipeline_filter=pipeline)

    # 2) Download + normalize each station file
    station_dir = os.path.join(args.out_dir, "stations", pipeline)
    agg_dir = os.path.join(args.out_dir, "regional")
    safe_mkdir(station_dir)
    safe_mkdir(agg_dir)

    frames = []

    if args.start:
        start_date = args.start
    elif args.days_ago > 0:
        start_date = datetime.now() - timedelta(days=args.days_ago)

    for st in stations:
        fp = download_station_csv(
            st.ghcnd_station_id, out_dir=station_dir, timeout=args.timeout
        )
        df_st = read_and_normalize_station_file(
            station_id=st.ghcnd_station_id,
            filepath=fp,
            start_date=start_date,
            end_date=args.end,
        )
        frames.append(df_st)

    df_all = pd.concat(frames, ignore_index=True)

    # 3) Aggregate daily region series
    df_region = aggregate_region_daily(df_all, pipeline=pipeline, region_id=region_id)

    # 4) Save outputs (CSV)
    station_norm_path = os.path.join(agg_dir, f"{pipeline}_stations_normalized.csv")
    region_path = os.path.join(agg_dir, f"{pipeline}_region_daily.csv")
    meta_path = os.path.join(agg_dir, f"{pipeline}_region_daily.meta.json")

    df_all.to_csv(station_norm_path, index=False)
    df_region.to_csv(region_path, index=False)

    meta = {
        "pipeline": pipeline,
        "region_id": region_id,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stations_count": len(stations),
        "stations": [asdict(s) for s in stations],
        "source_base_url": NOAA_GHCND_ACCESS_BASE,
        "station_files_dir": station_dir,
        "outputs": {
            "stations_normalized_csv": station_norm_path,
            "region_daily_csv": region_path,
        },
        "date_filter": {"start_date": start_date, "end_date": args.end},
        "aggregation": {
            "tavg": "median and mean across stations",
            "hdd": "computed from regional tavg using base 65F",
            "temp_units": "GHCND tenths of C converted to C; also saved F",
        },
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"Saved station-normalized: {station_norm_path}")
    print(f"Saved region daily:      {region_path}")
    print(f"Saved metadata:          {meta_path}")

    # 5) Optional Mongo write
    if args.mongo_uri:
        result = mongo_upsert_weather(
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            pipeline=pipeline,
            stations_meta=stations,
            df_station_norm=df_all,
            df_region_daily=df_region,
            station_collection=args.mongo_station_collection,
            region_collection=args.mongo_region_collection,
        )
        print("Mongo upsert result:", result)


if __name__ == "__main__":
    main()
