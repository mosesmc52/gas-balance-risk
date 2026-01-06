from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from scripts.ops.email.SES import AmazonSES

load_dotenv()

# =========================
# CONFIG (EDIT THIS)
# =========================
PIPELINE_COLLECTIONS: Dict[str, Dict[str, str]] = {
    "algonquin": {
        "notices": "ebb_algonquin_notices",
        "capacity": "ebb_algonquin_capacity",
    },
    # "transco": {"notices": "ebb_transco_notices", "capacity": "ebb_transco_capacity"},
}

DEFAULT_MONGO_URI = os.getenv("MONGO_URI", None)
DEFAULT_DB_NAME = "energy_gas_risk"

# =========================
# Date parsing helpers
# =========================
DATE_FORMATS = ("%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y/%m/%d")


def _parse_date(s: str) -> date:
    return pd.Timestamp(s).date()


def _try_parse_date(v: Any) -> Optional[date]:
    """
    Attempts to parse:
    - datetime -> date
    - pandas Timestamp -> date
    - strings in DATE_FORMATS
    """
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, pd.Timestamp):
        return v.date()

    s = str(v).strip()
    if not s:
        return None

    # Try pandas first (handles many variants)
    try:
        return pd.Timestamp(s).date()
    except Exception:
        pass

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    return None


def _parse_capacity_post_date(v: Any) -> Optional[date]:
    # capacity Post_Date is often MM-DD-YYYY or MM/DD/YYYY
    return _try_parse_date(v)


def _to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def _df_to_text(df: pd.DataFrame, title: str) -> str:
    if df is None or df.empty:
        return f"=== {title} ===\n(no rows)\n"
    return f"=== {title} ===\n{df.to_string(index=False)}\n"


def _escape_html(s: str) -> str:
    # Minimal HTML escaping for email safety
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _df_to_html_table(df: pd.DataFrame, title: str, *, min_width_px: int = 1100) -> str:
    """
    Email-friendly HTML table:
    - inline styles (better compatibility across clients)
    - explicit thead/tbody
    - zebra striping
    - light borders + padding
    - numeric columns right-aligned
    """
    if df is None or df.empty:
        return f"<h3 style='margin:16px 0 6px 0;'>{_escape_html(title)}</h3><p>(no rows)</p>"

    # Determine alignment per column (right align numbers, left otherwise)
    is_num = {c: pd.api.types.is_numeric_dtype(df[c]) for c in df.columns}

    # Header cells
    ths = []
    for c in df.columns:
        ths.append(
            "<th "
            "style="
            "'text-align:left; padding:8px 10px; border-bottom:2px solid #d0d0d0; "
            "background:#f3f4f6; font-weight:700; white-space:nowrap;'"
            f">{_escape_html(c)}</th>"
        )
    thead = "<tr>" + "".join(ths) + "</tr>"

    # Body rows
    body_rows = []
    for i, row in enumerate(df.itertuples(index=False, name=None)):
        bg = "#ffffff" if (i % 2 == 0) else "#fafafa"
        tds = []
        for j, val in enumerate(row):
            col = df.columns[j]
            align = "right" if is_num.get(col, False) else "left"
            # Keep None/NaN readable
            cell = "" if pd.isna(val) else _escape_html(val)
            tds.append(
                "<td "
                "style="
                f"'text-align:{align}; padding:7px 10px; border-bottom:1px solid #e6e6e6; "
                "vertical-align:top; white-space:nowrap;'"
                f">{cell}</td>"
            )
        body_rows.append(f"<tr style='background:{bg};'>" + "".join(tds) + "</tr>")

    table = (
        f"<h3 style='margin:16px 0 6px 0;'>{_escape_html(title)}</h3>"
        f"<div style='width:100%; overflow-x:auto;'>"
        f"<table style='width:100%; min-width:{int(min_width_px)}px; border-collapse:collapse; "
        "font-family:Arial, sans-serif; font-size:12px;'>"
        f"<thead>{thead}</thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
    )
    return table


def build_email_payload(
    df_ingest: pd.DataFrame,
    df_fresh: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
    subject_prefix: str = "Gas Risk Daily Report",
) -> Tuple[str, str, str]:
    subject = f"{subject_prefix} — {start_date.isoformat()} to {end_date.isoformat()}"

    # TEXT
    parts_txt = []
    parts_txt.append(
        f"Gas Risk Daily Report\nWindow: {start_date.isoformat()} to {end_date.isoformat()}\n"
    )
    parts_txt.append(_df_to_text(df_ingest, "Pipeline Ingestion Report (daily)"))
    parts_txt.append(_df_to_text(df_fresh, "Data Freshness Report (latest dates)"))
    content_text = "\n".join(parts_txt).strip() + "\n"

    # HTML
    parts_html = []
    parts_html.append(
        f"""
        <html>
          <body style="font-family: Arial, sans-serif; font-size: 14px; color:#111;">
            <h2 style="margin:0 0 8px 0;">Gas Risk Daily Report</h2>
            <p style="margin:0 0 12px 0;"><b>Window:</b> {start_date.isoformat()} to {end_date.isoformat()}</p>
        """.strip()
    )

    parts_html.append(_df_to_html_table(df_ingest, "Pipeline Ingestion Report (daily)"))
    parts_html.append(
        _df_to_html_table(df_fresh, "Data Freshness Report (latest dates)")
    )

    parts_html.append("</body></html>")
    content_html = "\n".join(parts_html)

    return subject, content_text, content_html


# =========================
# Core pipeline ingestion report
# =========================
def run_pipeline_ingestion_report(
    db,
    start_date: date,
    end_date: date,
    pipeline_collections: Dict[str, Dict[str, str]],
    notices_date_field: str = "posted_dt",
    capacity_date_field: str = "Post_Date",
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for pipeline, colls in pipeline_collections.items():
        # -------- Notices --------
        notices_coll = db[colls["notices"]]

        # posted_dt is ISODate -> query server-side
        start_dt = pd.Timestamp(start_date).to_pydatetime()
        end_dt_excl = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).to_pydatetime()

        q = {notices_date_field: {"$gte": start_dt, "$lt": end_dt_excl}}
        for doc in notices_coll.find(q, projection={notices_date_field: 1}):
            dt = doc.get(notices_date_field)
            day = _try_parse_date(dt)
            if day is None:
                continue
            rows.append(
                {
                    "day": day,
                    "pipeline": pipeline,
                    "kind": "notices",
                    "count": 1,
                    "scheduled_qty": 0.0,
                    "design_cap": 0.0,
                }
            )

        # -------- Capacity --------
        capacity_coll = db[colls["capacity"]]

        # Capacity Post_Date is string-like; filter client-side robustly
        for doc in capacity_coll.find(
            {},
            projection={
                capacity_date_field: 1,
                "Total_Scheduled_Quantity": 1,
                "Total_Design_Capacity": 1,
            },
        ):
            day = _parse_capacity_post_date(doc.get(capacity_date_field))
            if day is None or not (start_date <= day <= end_date):
                continue

            tsq = _to_float(doc.get("Total_Scheduled_Quantity"))
            tdc = _to_float(doc.get("Total_Design_Capacity"))

            rows.append(
                {
                    "day": day,
                    "pipeline": pipeline,
                    "kind": "capacity",
                    "count": 1,
                    "scheduled_qty": tsq,
                    "design_cap": tdc,
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    g = (
        df.groupby(["day", "pipeline", "kind"], as_index=False)
        .agg(
            n_rows=("count", "sum"),
            total_scheduled_qty=("scheduled_qty", "sum"),
            total_design_cap=("design_cap", "sum"),
            n_with_sched_qty=("scheduled_qty", lambda s: int((pd.Series(s) > 0).sum())),
        )
        .sort_values(["day", "pipeline", "kind"])
    )

    g["utilization_proxy"] = g.apply(
        lambda r: (
            (r["total_scheduled_qty"] / r["total_design_cap"])
            if r["total_design_cap"] > 0
            else None
        ),
        axis=1,
    )

    notices = g[g["kind"] == "notices"].rename(columns={"n_rows": "notices_added"})
    cap = g[g["kind"] == "capacity"].rename(columns={"n_rows": "capacity_rows_added"})

    report = (
        pd.merge(
            notices[["day", "pipeline", "notices_added"]],
            cap[
                [
                    "day",
                    "pipeline",
                    "capacity_rows_added",
                    "n_with_sched_qty",
                    "total_scheduled_qty",
                    "utilization_proxy",
                ]
            ],
            on=["day", "pipeline"],
            how="outer",
        )
        .fillna(
            {
                "notices_added": 0,
                "capacity_rows_added": 0,
                "n_with_sched_qty": 0,
                "total_scheduled_qty": 0.0,
            }
        )
        .sort_values(["day", "pipeline"])
        .reset_index(drop=True)
    )

    return report


# =========================
# Data freshness report
# =========================
def get_latest_date(
    db,
    collection_name: str,
    date_field: str,
    *,
    projection_extra: Optional[Dict[str, int]] = None,
    limit_scan: int = 5000,
) -> Optional[date]:
    """
    Robustly finds the latest date for a collection.

    Strategy:
    - Try to sort server-side by date_field descending (works if field is ISODate or consistently sortable)
    - If that fails or yields unparseable, scan up to limit_scan docs and take max parsable date.
    """
    proj = {date_field: 1}
    if projection_extra:
        proj.update(projection_extra)

    coll = db[collection_name]

    # Attempt server-side sort (fast path)
    try:
        doc = coll.find_one({}, projection=proj, sort=[(date_field, -1)])
        if doc:
            d = _try_parse_date(doc.get(date_field))
            if d is not None:
                return d
    except Exception:
        pass

    # Fallback: scan
    latest: Optional[date] = None
    try:
        cur = coll.find({}, projection=proj).limit(int(limit_scan))
        for doc in cur:
            d = _try_parse_date(doc.get(date_field))
            if d is None:
                continue
            if latest is None or d > latest:
                latest = d
    except Exception:
        return None

    return latest


def run_freshness_report(
    db,
    *,
    eia_storage_coll: str,
    eia_storage_date_field: str,
    henry_hub_coll: str,
    henry_hub_date_field: str,
    noaa_region_coll: str,
    noaa_region_date_field: str,
) -> pd.DataFrame:
    items: List[Tuple[str, str, str, Optional[date]]] = []

    items.append(
        (
            "EIA Storage",
            eia_storage_coll,
            eia_storage_date_field,
            get_latest_date(db, eia_storage_coll, eia_storage_date_field),
        )
    )

    items.append(
        (
            "Henry Hub Spot Price",
            henry_hub_coll,
            henry_hub_date_field,
            get_latest_date(db, henry_hub_coll, henry_hub_date_field),
        )
    )

    items.append(
        (
            "NOAA Regional (daily)",
            noaa_region_coll,
            noaa_region_date_field,
            get_latest_date(db, noaa_region_coll, noaa_region_date_field),
        )
    )

    return pd.DataFrame(
        items, columns=["dataset", "collection", "date_field", "latest_date"]
    )


# =========================
# CLI entrypoint
# =========================
def main():
    parser = argparse.ArgumentParser(
        description="Daily pipeline ingestion + data freshness report (optional SES email)"
    )

    # Date window for ingestion counts
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")

    parser.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)

    # Notices / capacity date fields (in case you change schema)
    parser.add_argument("--notices-date-field", default="posted_dt")
    parser.add_argument("--capacity-date-field", default="Post_Date")

    # Freshness config (explicit)
    parser.add_argument("--eia-storage-coll", default="eia_storage_weekly")
    parser.add_argument("--eia-storage-date-field", default="period")
    parser.add_argument("--henry-hub-coll", default="eia_henry_hub_spot")
    parser.add_argument("--henry-hub-date-field", default="period")
    parser.add_argument("--noaa-region-coll", default="noaa_region_daily")
    parser.add_argument("--noaa-region-date-field", default="date")

    # Email options (SES)
    parser.add_argument(
        "--email-to",
        default=os.getenv("TO_ADDRESSES"),
        help="If set, email the report to this address (requires SES env/args).",
    )
    parser.add_argument(
        "--email-format",
        choices=["text", "html", "both"],
        default="html",
        help="Email body format to send via SES.",
    )
    parser.add_argument(
        "--email-subject-prefix",
        default="Gas Risk Daily Report",
        help="Subject prefix for emailed report.",
    )

    # Prefer env vars, allow override via flags
    parser.add_argument(
        "--ses-region",
        default=os.getenv("AWS_SES_REGION_NAME"),
    )
    parser.add_argument(
        "--ses-access-key",
        default=os.getenv("AWS_SES_ACCESS_KEY_ID"),
    )
    parser.add_argument(
        "--ses-secret-key",
        default=os.getenv("AWS_SES_SECRET_ACCESS_KEY"),
    )
    parser.add_argument("--ses-from", default=os.getenv("FROM_ADDRESS"))

    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client[args.db_name]

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)

    # 1) Pipeline ingestion report
    df_ingest = run_pipeline_ingestion_report(
        db,
        start_date=start,
        end_date=end,
        pipeline_collections=PIPELINE_COLLECTIONS,
        notices_date_field=args.notices_date_field,
        capacity_date_field=args.capacity_date_field,
    )

    # 2) Freshness report
    df_fresh = run_freshness_report(
        db,
        eia_storage_coll=args.eia_storage_coll,
        eia_storage_date_field=args.eia_storage_date_field,
        henry_hub_coll=args.henry_hub_coll,
        henry_hub_date_field=args.henry_hub_date_field,
        noaa_region_coll=args.noaa_region_coll,
        noaa_region_date_field=args.noaa_region_date_field,
    )

    # Output to stdout
    print("\n=== Pipeline Ingestion Report (daily) ===")
    if df_ingest.empty:
        print("No pipeline ingestion data found for the selected date range.")
    else:
        print(df_ingest.to_string(index=False))

    print("\n=== Data Freshness Report (latest dates) ===")
    print(df_fresh.to_string(index=False))

    # Optional: Email via SES
    if args.email_to:

        missing = []
        if not args.ses_region:
            missing.append("ses-region (AWS_REGION / SES_REGION)")
        if not args.ses_access_key:
            missing.append("ses-access-key (AWS_ACCESS_KEY_ID / SES_ACCESS_KEY)")
        if not args.ses_secret_key:
            missing.append("ses-secret-key (AWS_SECRET_ACCESS_KEY / SES_SECRET_KEY)")
        if not args.ses_from:
            missing.append("ses-from (SES_FROM / SES_FROM_ADDRESS)")

        if missing:
            raise SystemExit(
                "Email requested but SES configuration is missing: "
                + ", ".join(missing)
            )

        ses = AmazonSES(
            region=args.ses_region,
            access_key=args.ses_access_key,
            secret_key=args.ses_secret_key,
            from_address=args.ses_from,
        )

        subject, content_text, content_html = build_email_payload(
            df_ingest,
            df_fresh,
            start_date=start,
            end_date=end,
            subject_prefix=args.email_subject_prefix,
        )

        to_addresses = args.email_to.split(",")
        for to_address in to_addresses:
            # SES does not support multi-part alt body via this class; we send two emails.
            # ses.send_text_email(to_address, subject, content_text)
            ses.send_html_email(to_address, subject, content_html)
            print(f"\n[Email] Sent message to {args.email_to}")


if __name__ == "__main__":
    main()
