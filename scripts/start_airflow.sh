#!/bin/bash

set -e

# ------------------------------------------------------------
# OLIST PIPELINE - START APACHE AIRFLOW
# ------------------------------------------------------------

# Resolve project directory (works in bash and sh)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AIRFLOW_HOME="$PROJECT_DIR/airflow"

echo "============================================================"
echo "OLIST PIPELINE - AIRFLOW STARTUP"
echo "============================================================"
echo "Project Directory : $PROJECT_DIR"
echo "Airflow Home      : $AIRFLOW_HOME"
echo "============================================================"

# Move to project directory
cd "$PROJECT_DIR"

# Set Airflow home
export AIRFLOW_HOME="$AIRFLOW_HOME"

echo
echo "[CHECK] Checking Python environment..."
echo "Python:"
which python
python --version

echo
echo "[CHECK] Checking Apache Airflow..."
if ! command -v airflow >/dev/null 2>&1; then
    echo
    echo "[ERROR] Apache Airflow is not installed in the active environment."
    echo
    echo "Current Python:"
    which python
    echo
    echo "Install Airflow inside your virtual environment first."
    exit 1
fi

echo "Airflow executable:"
which airflow

echo "Airflow version:"
airflow version

echo
echo "[CONFIG] AIRFLOW_HOME=$AIRFLOW_HOME"

# Create Airflow directory if required
mkdir -p "$AIRFLOW_HOME"

echo
echo "============================================================"
echo "STARTING APACHE AIRFLOW"
echo "============================================================"
echo
echo "Airflow UI will normally be available at:"
echo "http://localhost:8080"
echo
echo "Press Ctrl+C to stop Airflow."
echo "============================================================"
echo

airflow standalone
