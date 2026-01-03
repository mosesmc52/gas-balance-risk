#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Gas Risk Daily Job
# ============================================================

JOB_START_TS="$(date -u)"

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
run_cmd scrapy crawl algonquin_capacity -s LOG_LEVEL=INFO

substep "Pipeline: Algonquin — Notices"
run_cmd scrapy crawl algonquin_notices -s LOG_LEVEL=INFO

# ------------------------------
# PIPELINE: <ADD NEW PIPELINE HERE>
# ------------------------------
# substep "Pipeline: XYZ — Capacity"
# run_cmd scrapy crawl xyz_capacity -s LOG_LEVEL=INFO
#
# substep "Pipeline: XYZ — Notices"
# run_cmd scrapy crawl xyz_notices -s LOG_LEVEL=INFO

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
# Job end
# ============================================================

JOB_END_TS="$(date -u)"

echo
echo "================================================"
echo "Gas Risk Daily Job finished at ${JOB_END_TS}"
echo "================================================"
