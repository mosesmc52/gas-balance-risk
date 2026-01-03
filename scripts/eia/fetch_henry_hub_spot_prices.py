#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from eia_ng import EIAClient

try:
    from pymongo import MongoClient, UpdateOne
except Exception:  # pragma: no cover
    MongoClient = None
    UpdateOne = None

load_dotenv()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch Henry Hub natural gas spot prices (daily) via eia-ng-client; save to CSV and/or Mongo."
    )
    p.add_argument(
        "--start", default=None, required=False, help="Start date (YYYY-MM-DD)"
    )
    p.add_argument("--end", default=None, help="Optional end date (YYYY-MM-DD)")
    p.add_argument(
        "--days_ago", type=int, default=30, help="The number of days ago to start"
    )
    p.add_argument(
        "--out",
        default="data/eia/henry_hub_spot_prices_daily.csv",
        help="Output CSV path (saved when --write-csv=1).",
    )
    p.add_argument(
        "--write-csv",
        type=int,
        default=0,
        help="1=write CSV (default), 0=skip CSV.",
    )

    # Mongo options
    p.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", None),
        help="MongoDB URI (optional). If set, upserts to Mongo.",
    )
    p.add_argument(
        "--mongo-db", default=os.getenv("MONGO_DB", None), help="MongoDB database name."
    )
    p.add_argument(
        "--mongo-collection",
        default="eia_hh_spot_daily",
        help="Mongo collection for Henry Hub spot.",
    )
    p.add_argument(
        "--mongo-batch-size",
        type=int,
        default=2000,
        help="Bulk upsert batch size.",
    )
    return p.parse_args()


def _rows_to_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Expected row shape (from your example):
      period, value, units, series, series-description, duoarea, process, product, ...
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).copy()

    # Normalize fields
    df.rename(
        columns={
            "period": "date",
            "series-description": "series_description",
            "area-name": "area_name",
            "process-name": "process_name",
            "product-name": "product_name",
        },
        inplace=True,
    )

    if "date" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Unexpected schema. Columns: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # value is a string in your response; convert to float
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # keep a canonical column name for modeling
    df["henry_hub_usd_per_mmbtu"] = df["value"]

    # Canonical series id (your response has series='RNGWHHD')
    if "series" not in df.columns:
        df["series"] = "RNGWHHD"

    # Keep a clean set of columns (you can expand later)
    keep = [
        "date",
        "henry_hub_usd_per_mmbtu",
        "units",
        "series",
        "series_description",
        "duoarea",
        "area_name",
        "product",
        "product_name",
        "process",
        "process_name",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[keep].copy()
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df


def mongo_upsert_hh_spot(
    *,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    df: pd.DataFrame,
    batch_size: int = 2000,
) -> dict:
    if MongoClient is None or UpdateOne is None:
        raise RuntimeError(
            "pymongo is not installed. Install with: pip install pymongo"
        )

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    col = db[mongo_collection]

    # Unique key: (series, date)
    col.create_index([("series", 1), ("date", 1)], unique=True)

    ops = []
    n_ops = 0

    now = datetime.utcnow()

    for rec in df.to_dict(orient="records"):
        doc = {
            "series": rec.get("series") or "RNGWHHD",
            "date": rec["date"],
            "value": (
                None
                if pd.isna(rec.get("henry_hub_usd_per_mmbtu"))
                else float(rec["henry_hub_usd_per_mmbtu"])
            ),
            "units": rec.get("units"),
            "series_description": rec.get("series_description"),
            "duoarea": rec.get("duoarea"),
            "area_name": rec.get("area_name"),
            "product": rec.get("product"),
            "product_name": rec.get("product_name"),
            "process": rec.get("process"),
            "process_name": rec.get("process_name"),
            "updated_at_utc": now,
            "source": {"provider": "EIA", "endpoint": "natural_gas.spot_prices"},
        }

        ops.append(
            UpdateOne(
                {"series": doc["series"], "date": doc["date"]},
                {"$set": doc},
                upsert=True,
            )
        )

        if len(ops) >= batch_size:
            res = col.bulk_write(ops, ordered=False)
            n_ops += (res.upserted_count or 0) + (res.modified_count or 0)
            ops = []

    if ops:
        res = col.bulk_write(ops, ordered=False)
        n_ops += (res.upserted_count or 0) + (res.modified_count or 0)

    client.close()
    return {
        "mongo_db": mongo_db,
        "collection": mongo_collection,
        "upserts_or_updates": n_ops,
    }


def main() -> None:
    args = _parse_args()

    if not os.getenv("EIA_API_KEY"):
        raise SystemExit(
            "Missing EIA_API_KEY env var. Set it first, e.g. export EIA_API_KEY='...'"
        )

    client = EIAClient()
    if args.start:
        start_date = args.start
    elif args.days_ago > 0:
        start_date = datetime.now() - timedelta(days=args.days_ago)

    if args.end:
        rows = client.natural_gas.spot_prices(start=start_date, end=args.end)
    else:
        rows = client.natural_gas.spot_prices(start=start_date)

    df = _rows_to_df(rows)
    if df.empty:
        raise SystemExit("No rows returned for Henry Hub spot prices.")

    # CSV
    if args.write_csv == 1:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        print(f"Wrote {len(df):,} rows to CSV: {out_path}")

    # Mongo
    if args.mongo_uri:
        result = mongo_upsert_hh_spot(
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_collection=args.mongo_collection,
            df=df,
            batch_size=args.mongo_batch_size,
        )
        print("Mongo upsert:", result)


if __name__ == "__main__":
    main()
