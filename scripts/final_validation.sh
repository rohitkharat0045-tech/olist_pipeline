#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# OLIST DATA ENGINEERING PROJECT - FINAL VALIDATION
# ============================================================

# ------------------------------------------------------------
# Project paths
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# ------------------------------------------------------------
# Counters
# ------------------------------------------------------------

PASSED=0
FAILED=0

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

pass_check() {
    echo "[PASS] $1"
    PASSED=$((PASSED + 1))
}

fail_check() {
    echo "[FAIL] $1"
    FAILED=$((FAILED + 1))
}

section() {
    echo
    echo "============================================================"
    echo "$1"
    echo "============================================================"
}

# ============================================================
# HEADER
# ============================================================

echo
echo "============================================================"
echo "OLIST DATA ENGINEERING PROJECT"
echo "FINAL VALIDATION"
echo "============================================================"
echo "Project : $PROJECT_ROOT"
echo "Time    : $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# ============================================================
# 1. CONFIGURATION
# ============================================================

section "[1] CONFIGURATION"

if [[ -f "config.env" ]]; then
    pass_check "config.env exists"
else
    fail_check "config.env missing"
fi

# ============================================================
# 2. RAW DATASETS
# ============================================================

section "[2] RAW DATASETS"

if [[ -d "raw" ]]; then

    RAW_COUNT=$(
        find raw \
            -maxdepth 1 \
            -type f \
            -name "*.csv" \
            | wc -l
    )

    if [[ "$RAW_COUNT" -eq 9 ]]; then
        pass_check "9 Olist raw CSV files found"
    else
        fail_check "Expected 9 CSV files, found $RAW_COUNT"
    fi

else
    fail_check "raw directory missing"
fi

# ============================================================
# 3. PYTHON SOURCE FILES
# ============================================================

section "[3] PYTHON SOURCE"

PYTHON_FILES=(
    "src/ingestion/profiler.py"
    "src/transform/spark_pipeline.py"
    "src/analysis/run_sql_reports.py"
    "src/validation/validate_master_orders.py"
    "src/utils/logger.py"
)

for file in "${PYTHON_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        pass_check "$file"
    else
        fail_check "$file missing"
    fi
done

# ============================================================
# 4. SQL ANALYTICS
# ============================================================

section "[4] SQL ANALYTICS"

SQL_FILES=(
    "sql/views.sql"
    "sql/01_order_status.sql"
    "sql/02_monthly_revenue.sql"
    "sql/03_state_performance.sql"
    "sql/04_running_revenue.sql"
    "sql/05_delivery_analysis.sql"
)

for file in "${SQL_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        pass_check "$file"
    else
        fail_check "$file missing"
    fi
done

# ============================================================
# 5. AUTOMATION SCRIPTS
# ============================================================

section "[5] AUTOMATION"

SCRIPTS=(
    "scripts/profile_raw_data.sh"
    "scripts/run_pipeline.sh"
    "scripts/archive_outputs.sh"
)

for file in "${SCRIPTS[@]}"; do
    if [[ -f "$file" ]]; then
        pass_check "$file"
    else
        fail_check "$file missing"
    fi
done

# ============================================================
# 6. AIRFLOW
# ============================================================

section "[6] AIRFLOW"

if [[ -f "dags/olist_pipeline_dag.py" ]]; then
    pass_check "Airflow DAG exists"
else
    fail_check "Airflow DAG missing"
fi

# ============================================================
# 7. PROCESSED MASTER DATASET
# ============================================================

section "[7] PROCESSED DATA"

MASTER_ORDERS_DIR="processed/master_orders"

if [[ -d "$MASTER_ORDERS_DIR" ]]; then

    if [[ -f "$MASTER_ORDERS_DIR/_SUCCESS" ]]; then
        pass_check "master_orders Spark success marker found"
    else
        fail_check "master_orders/_SUCCESS missing"
    fi

    PARQUET_COUNT=$(
        find "$MASTER_ORDERS_DIR" \
            -maxdepth 1 \
            -type f \
            -name "*.parquet" \
            | wc -l
    )

    if [[ "$PARQUET_COUNT" -gt 0 ]]; then
        pass_check "$PARQUET_COUNT Parquet files found"
    else
        fail_check "No master_orders Parquet files found"
    fi

else
    fail_check "processed/master_orders directory missing"
fi

# ============================================================
# 8. ANALYTICAL REPORTS
# ============================================================

section "[8] ANALYTICAL REPORTS"

REPORTS=(
    "order_status_report"
    "monthly_revenue_report"
    "state_performance_report"
    "running_revenue_report"
    "delivery_analysis_report"
)

if [[ -d "reports" ]]; then

    for report in "${REPORTS[@]}"; do

        if [[ -f "reports/$report/_SUCCESS" ]]; then
            pass_check "$report"
        else
            fail_check "$report missing or incomplete"
        fi

    done

else
    fail_check "reports directory missing"
fi

# ============================================================
# FINAL RESULT
# ============================================================

echo
echo "============================================================"
echo "FINAL VALIDATION RESULT"
echo "============================================================"
echo "Passed : $PASSED"
echo "Failed : $FAILED"
echo "============================================================"

if [[ "$FAILED" -eq 0 ]]; then
    echo
    echo "PROJECT STATUS: HEALTHY"
    echo "All required project components passed validation."
    echo "============================================================"
    exit 0
else
    echo
    echo "PROJECT STATUS: VALIDATION FAILED"
    echo "$FAILED validation check(s) need attention."
    echo "============================================================"
    exit 1
fi