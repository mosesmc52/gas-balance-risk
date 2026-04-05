#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="${LOG_DIR:-/var/log}"
LOG_FILE="${LOG_FILE:-${LOG_DIR}/job.log}"
mkdir -p "$LOG_DIR"

# Send stdout/stderr to both console and log file
exec > >(tee -a "$LOG_FILE") 2>&1

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
HOST_NAME="$(hostname)"
JOB_NAME="${JOB_NAME:-gas-risk-daily}"
EXIT_CODE=0
LOG_SPACES_KEY=""

# ============================================================
# Gas Risk Daily Job
# ============================================================

JOB_START_TS="$(date -u)"

# ------------------------------------------------------------
# Helper functions: logging / cleanup / upload
# ------------------------------------------------------------

on_error() {
  EXIT_CODE=$?
  echo
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "Job failed"
  echo "  job_name:   ${JOB_NAME}"
  echo "  run_id:     ${RUN_ID}"
  echo "  host_name:  ${HOST_NAME}"
  echo "  exit_code:  ${EXIT_CODE}"
  echo "  failed_at:  $(date -u)"
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
}

upload_log_to_spaces() {
  if [[ -z "${SPACES_KEY:-}" || -z "${SPACES_SECRET:-}" || -z "${SPACES_BUCKET:-}" || -z "${SPACES_REGION:-}" || -z "${SPACES_ENDPOINT:-}" ]]; then
    echo "[log-upload] Spaces env vars not fully configured; skipping upload."
    return 0
  fi

  LOG_SPACES_KEY="logs/${JOB_NAME}/$(date -u +%Y/%m/%d)/${RUN_ID}-${HOST_NAME}.log"

  echo "[log-upload] Installing awscli if needed..."
  if ! command -v aws >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
      apt-get update -y >/dev/null 2>&1 || true
      apt-get install -y awscli >/dev/null 2>&1 || true
    fi
  fi

  if ! command -v aws >/dev/null 2>&1; then
    echo "[log-upload] aws CLI unavailable; skipping upload."
    return 0
  fi

  export AWS_ACCESS_KEY_ID="${SPACES_KEY}"
  export AWS_SECRET_ACCESS_KEY="${SPACES_SECRET}"
  export AWS_DEFAULT_REGION="${SPACES_REGION}"

  echo "[log-upload] Uploading ${LOG_FILE} to s3://${SPACES_BUCKET}/${LOG_SPACES_KEY}"
  aws --endpoint-url "${SPACES_ENDPOINT}" s3 cp "${LOG_FILE}" "s3://${SPACES_BUCKET}/${LOG_SPACES_KEY}" || true
  echo "[log-upload] Uploaded key: ${LOG_SPACES_KEY}"
}

write_status_file() {
  mkdir -p /opt/job
  printf '%s\n' "${EXIT_CODE}" > /opt/job/exit_code

  cat > /opt/job/run_status.json <<EOF
{
  "job_name": "${JOB_NAME}",
  "run_id": "${RUN_ID}",
  "host_name": "${HOST_NAME}",
  "started_at": "${JOB_START_TS}",
  "finished_at": "$(date -u)",
  "exit_code": ${EXIT_CODE},
  "log_file": "${LOG_FILE}",
  "spaces_key": "${LOG_SPACES_KEY}"
}
EOF
}

cleanup() {
  EXIT_CODE=$?
  write_status_file
  upload_log_to_spaces

  JOB_END_TS="$(date -u)"

  echo
  echo "================================================"
  echo "Gas Risk Daily Job finished at ${JOB_END_TS}"
  echo "Exit code: ${EXIT_CODE}"
  echo "Log file: ${LOG_FILE}"
  if [[ -n "${LOG_SPACES_KEY}" ]]; then
    echo "Spaces log key: ${LOG_SPACES_KEY}"
  fi
  echo "================================================"

  exit "${EXIT_CODE}"
}

trap on_error ERR
trap cleanup EXIT

# ------------------------------------------------------------
# Reporting window configuration
# ------------------------------------------------------------

REPORT_LOOKBACK_DAYS="${REPORT_LOOKBACK_DAYS:-3}"

END_DATE="$(date -u +%Y-%m-%d)"
START_DATE="$(date -u -d "${REPORT_LOOKBACK_DAYS} days ago" +%Y-%m-%d)"

echo "================================================"
echo "Gas Risk Daily Job started at ${JOB_START_TS}"
echo "Job name: ${JOB_NAME}"
echo "Run ID: ${RUN_ID}"
echo "Host: ${HOST_NAME}"
echo "Log file: ${LOG_FILE}"
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
run_cmd scrapy crawl algonquin_notices -a cutoff_days=3 -s LOG_LEVEL=INFO

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
# STEP 6 — Backup MongoDB to DigitalOcean Spaces
# ============================================================

step "[STEP 6] Backup MongoDB to DigitalOcean Spaces"

run_cmd python scripts/ops/backup/mongo_backup.py --retention-days 7

echo "[STEP 6] Backup MongoDB completed"
