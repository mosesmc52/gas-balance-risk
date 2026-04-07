#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Gas Risk Daily Job
# ============================================================

JOB_START_TS="$(date -u)"

# ------------------------------------------------------------
# Reporting window configuration
# ------------------------------------------------------------

REPORT_LOOKBACK_DAYS="${REPORT_LOOKBACK_DAYS:-3}"

END_DATE="$(date -u +%Y-%m-%d)"
START_DATE="$(date -u -d "${REPORT_LOOKBACK_DAYS} days ago" +%Y-%m-%d)"

echo "================================================"
echo "Gas Risk Daily Job started at ${JOB_START_TS}"
echo "================================================"

# ------------------------------------------------------------
# Environment sanity checks
# ------------------------------------------------------------
: "${MONGO_URI:?Missing MONGO_URI}"
: "${MONGO_DB:?Missing MONGO_DB}"
: "${EIA_API_KEY:?Missing EIA_API_KEY}"

export PYTHONUNBUFFERED=1



# ============================================================
# Helper functions (safe, minimal abstraction)
# ============================================================

step() {
  echo
  echo "------------------------------------------------"
  echo "$1"
  echo "------------------------------------------------"
}

substep() {
  echo
  echo "  → $1"
}

run_cmd() {
  echo "    $ $*"
  "$@"
}

# ============================================================
# STEP 1 — Scrapy EBB ingestion (pipelines)
# ============================================================

step "[STEP 1] Scrapy: EBB pipeline ingestion"

cd /app/scrapy

# ------------------------------
# PIPELINE: Algonquin
# ------------------------------
substep "Pipeline: Algonquin — Capacity"
run_cmd scrapy crawl algonquin_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: Algonquin — Notices"
run_cmd scrapy crawl algonquin_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: Transco
# ------------------------------
substep "Pipeline: Transco — Capacity"
run_cmd scrapy crawl transco_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: Transco — Notices"
run_cmd scrapy crawl transco_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: Tennessee
# ------------------------------
substep "Pipeline: Tennessee — Capacity"
run_cmd scrapy crawl tenn_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: Tennessee — Notices"
run_cmd scrapy crawl tenn_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: Texas Eastern
# ------------------------------
substep "Pipeline: Texas Eastern — Capacity"
run_cmd scrapy crawl tetco_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: Texas Eastern — Notices"
run_cmd scrapy crawl tetco_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: Columbia Gas Transmission
# ------------------------------
substep "Pipeline: Columbia Gas Transmission — Capacity"
run_cmd scrapy crawl tco_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: Columbia Gas Transmission — Notices"
run_cmd scrapy crawl tco_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: El Paso
# ------------------------------
substep "Pipeline: El Paso — Capacity"
run_cmd scrapy crawl elpaso_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: El Paso — Notices"
run_cmd scrapy crawl elpaso_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: Northern Natural
# ------------------------------
substep "Pipeline: Northern Natural — Capacity"
run_cmd scrapy crawl nngp_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: Northern Natural — Notices"
run_cmd scrapy crawl nngp_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: ANR
# ------------------------------
substep "Pipeline: ANR — Capacity"
run_cmd scrapy crawl anr_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: ANR — Notices"
run_cmd scrapy crawl anr_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: Louisiana
# ------------------------------
substep "Pipeline: Louisiana — Capacity"
run_cmd scrapy crawl louisiana_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: Louisiana — Notices"
run_cmd scrapy crawl louisiana_notices -a days_ago=3 -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: SoCalGas
# ------------------------------
substep "Pipeline: SoCalGas — Capacity"
run_cmd scrapy crawl so_cal_capacity -a days_ago=3 -s LOG_LEVEL=INFO

substep "Pipeline: SoCalGas — Notices"
run_cmd scrapy crawl so_cal_notices -a days_ago=3 -s LOG_LEVEL=INFO


echo "[STEP 1] Scrapy ingestion completed"

# ============================================================
# STEP 2 — EIA Henry Hub daily prices
# ============================================================

step "[STEP 2] EIA: Henry Hub spot prices"

cd /app

run_cmd python scripts/eia/fetch_henry_hub_spot_prices.py \
  --days_ago 60 \
  --mongo-uri "$MONGO_URI" \
  --mongo-db "$MONGO_DB"

echo "[STEP 2] Henry Hub ingestion completed"

# ============================================================
# STEP 3 — EIA weekly working gas storage
# ============================================================

step "[STEP 3] EIA: Working gas storage (weekly)"

run_cmd python scripts/eia/fetch_working_gas_storage_weekly.py \
  --days_ago 60 \
  --region lower48 \
  --mongo-uri "$MONGO_URI" \
  --mongo-db "$MONGO_DB"

echo "[STEP 3] Storage ingestion completed"

# ============================================================
# STEP 4 — NOAA GHCND daily station aggregation
# ============================================================

step "[STEP 4] NOAA: GHCND daily station data"

run_cmd python scripts/noaa/download_and_aggregate_ghcnd.py \
  --days_ago 60 \
  --mongo-uri "$MONGO_URI" \
  --mongo-db "$MONGO_DB"

echo "[STEP 4] NOAA ingestion completed"

# ============================================================
# STEP 5 — Daily ingestion report
# ============================================================

step "[STEP 5] Ops: Daily ingestion report"

run_cmd python scripts/ops/ingest/daily_ingestion_report.py \
  --start-date "${START_DATE}" \
  --end-date "${END_DATE}"

echo "[STEP 5] Daily ingestion report completed"

# ============================================================
# STEP 6 — NOAA GHCND daily station aggregation
# ============================================================
echo "[STEP 6] Backup Mongodb to Digital Ocean Spaces"

run_cmd python scripts/ops/backup/mongo_backup.py --retention-days 7

echo "[STEP 6] Backup Mongodb completed"
# ============================================================
# Job end
# ============================================================

JOB_END_TS="$(date -u)"

echo
echo "================================================"
echo "Gas Risk Daily Job finished at ${JOB_END_TS}"
echo "================================================"
