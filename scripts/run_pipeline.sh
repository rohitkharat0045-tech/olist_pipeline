#!/usr/bin/env bash

# ============================================================
# OLIST DATA ENGINEERING PIPELINE
# Day 5 - End-to-End Pipeline Automation
# ============================================================

set -Eeuo pipefail


# ------------------------------------------------------------
# 1. Project paths
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"


# ------------------------------------------------------------
# 2. Create required directories
# ------------------------------------------------------------

mkdir -p logs
mkdir -p archive
mkdir -p processed
mkdir -p reports


# ------------------------------------------------------------
# 3. Create run ID
# ------------------------------------------------------------

RUN_ID="$(date '+%Y%m%d_%H%M%S')"

LOG_FILE="$PROJECT_ROOT/logs/pipeline_${RUN_ID}.log"

ARCHIVE_FILE="$PROJECT_ROOT/archive/run_${RUN_ID}.tar.gz"


# ------------------------------------------------------------
# 4. Save normal terminal output
# ------------------------------------------------------------

exec 3>&1
exec 4>&2


# ------------------------------------------------------------
# 5. Redirect pipeline output to terminal + log
# ------------------------------------------------------------

exec > >(tee -a "$LOG_FILE") 2>&1


# ------------------------------------------------------------
# 6. Error handler
# ------------------------------------------------------------

handle_error() {

    exit_code=$?

    echo
    echo "============================================================"
    echo "PIPELINE FAILED"
    echo "============================================================"
    echo "Run ID      : $RUN_ID"
    echo "Exit Code   : $exit_code"
    echo "Failed Line : $1"
    echo "Log File    : $LOG_FILE"
    echo "============================================================"

    exit "$exit_code"
}


trap 'handle_error $LINENO' ERR


# ------------------------------------------------------------
# 7. Pipeline header
# ------------------------------------------------------------

echo
echo "============================================================"
echo "OLIST DATA ENGINEERING PIPELINE"
echo "============================================================"
echo "Run ID      : $RUN_ID"
echo "Start Time  : $(date '+%Y-%m-%d %H:%M:%S')"
echo "Project     : $PROJECT_ROOT"
echo "Log File    : $LOG_FILE"
echo "============================================================"


# ------------------------------------------------------------
# 8. Validate config.env
# ------------------------------------------------------------

echo
echo "[VALIDATION] Checking config.env..."

if [[ ! -f "$PROJECT_ROOT/config.env" ]]; then

    echo "ERROR: config.env not found."

    exit 1

fi

echo "config.env found."


# ------------------------------------------------------------
# 9. Load environment variables
# ------------------------------------------------------------

echo
echo "[CONFIG] Loading environment variables..."

set -a

source "$PROJECT_ROOT/config.env"

set +a

echo "Environment variables loaded."


# ------------------------------------------------------------
# 10. Validate virtual environment
# ------------------------------------------------------------

echo
echo "[VALIDATION] Checking Python virtual environment..."

if [[ ! -f "$PROJECT_ROOT/venv/bin/activate" ]]; then

    echo "ERROR: Python virtual environment not found."

    exit 1

fi

echo "Virtual environment found."


# ------------------------------------------------------------
# 11. Activate virtual environment
# ------------------------------------------------------------

source "$PROJECT_ROOT/venv/bin/activate"

echo "Virtual environment activated."


# ------------------------------------------------------------
# 12. Validate raw data
# ------------------------------------------------------------

echo
echo "[VALIDATION] Checking raw datasets..."

if ! compgen -G "$PROJECT_ROOT/raw/*.csv" > /dev/null; then

    echo "ERROR: No CSV files found inside raw/"

    exit 1

fi


RAW_COUNT=$(find "$PROJECT_ROOT/raw" -maxdepth 1 -type f -name "*.csv" | wc -l)


echo "Raw CSV files found: $RAW_COUNT"


# ------------------------------------------------------------
# STAGE 1 - RAW DATA PROFILING
# ------------------------------------------------------------

echo
echo "============================================================"
echo "STAGE 1 - RAW DATA PROFILING"
echo "============================================================"

bash "$PROJECT_ROOT/scripts/profile_raw_data.sh"

echo
echo "STAGE 1 COMPLETED"


# ------------------------------------------------------------
# STAGE 2 - PYSPARK ETL
# ------------------------------------------------------------

echo
echo "============================================================"
echo "STAGE 2 - PYSPARK ETL"
echo "============================================================"

python -m src.transform.spark_pipeline

echo
echo "STAGE 2 COMPLETED"


# ------------------------------------------------------------
# Validate master dataset
# ------------------------------------------------------------

echo
echo "[VALIDATION] Checking master_orders..."

if [[ ! -d "$PROJECT_ROOT/processed/master_orders" ]]; then

    echo "ERROR: master_orders directory was not created."

    exit 1

fi


if ! compgen -G \
    "$PROJECT_ROOT/processed/master_orders/*.parquet" \
    > /dev/null; then

    echo "ERROR: No Parquet files found in master_orders."

    exit 1

fi


echo "master_orders validation passed."


# ------------------------------------------------------------
# STAGE 3 - SQL ANALYTICS
# ------------------------------------------------------------

echo
echo "============================================================"
echo "STAGE 3 - SPARK SQL ANALYTICS"
echo "============================================================"

python "$PROJECT_ROOT/src/analysis/run_sql_reports.py"

echo
echo "STAGE 3 COMPLETED"


# ------------------------------------------------------------
# Validate reports
# ------------------------------------------------------------

echo
echo "[VALIDATION] Checking analytics reports..."


REPORTS=(

    "order_status_report"

    "monthly_revenue_report"

    "state_performance_report"

    "running_revenue_report"

    "delivery_analysis_report"

)


for report in "${REPORTS[@]}"; do

    REPORT_PATH="$PROJECT_ROOT/reports/$report"

    if [[ ! -d "$REPORT_PATH" ]]; then

        echo "ERROR: Missing report: $report"

        exit 1

    fi


    if [[ ! -f "$REPORT_PATH/_SUCCESS" ]]; then

        echo "ERROR: Spark success marker missing: $report"

        exit 1

    fi


    echo "Validated: $report"

done


# ------------------------------------------------------------
# Pipeline complete
# ------------------------------------------------------------

echo
echo "============================================================"
echo "PIPELINE PROCESSING COMPLETED"
echo "============================================================"

echo "Run ID   : $RUN_ID"

echo "End Time : $(date '+%Y-%m-%d %H:%M:%S')"

echo "Status   : SUCCESS"

echo "============================================================"


# ------------------------------------------------------------
# Stop logging before creating archive
# ------------------------------------------------------------

exec 1>&3
exec 2>&4


# ------------------------------------------------------------
# Create archive
# ------------------------------------------------------------

echo
echo "[ARCHIVE] Creating pipeline archive..."


tar \
    -czf "$ARCHIVE_FILE" \
    -C "$PROJECT_ROOT" \
    reports \
    processed \
    "logs/$(basename "$LOG_FILE")"


echo "Archive created:"
echo "$ARCHIVE_FILE"


# ------------------------------------------------------------
# Final message
# ------------------------------------------------------------

echo
echo "============================================================"
echo "OLIST PIPELINE COMPLETED SUCCESSFULLY"
echo "============================================================"
echo "Run ID  : $RUN_ID"
echo "Log     : $LOG_FILE"
echo "Archive : $ARCHIVE_FILE"
echo "============================================================"
