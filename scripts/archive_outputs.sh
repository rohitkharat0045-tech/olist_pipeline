#!/bin/bash

set -e

# ------------------------------------------------------------
# OLIST PIPELINE - ARCHIVE OUTPUTS
# ------------------------------------------------------------

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCHIVE_DIR="$PROJECT_DIR/archive"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
ARCHIVE_FILE="$ARCHIVE_DIR/run_${TIMESTAMP}.tar.gz"

echo "============================================================"
echo "OLIST PIPELINE OUTPUT ARCHIVER"
echo "============================================================"
echo "Project Directory : $PROJECT_DIR"
echo "Archive Directory : $ARCHIVE_DIR"
echo "Archive File      : $ARCHIVE_FILE"
echo "============================================================"

# Create archive directory if it does not exist
mkdir -p "$ARCHIVE_DIR"

# Move to project root
cd "$PROJECT_DIR"

echo
echo "[CHECK] Looking for output directories..."

DIRS_TO_ARCHIVE=()

if [ -d "processed" ]; then
    DIRS_TO_ARCHIVE+=("processed")
    echo "Found: processed/"
fi

if [ -d "reports" ]; then
    DIRS_TO_ARCHIVE+=("reports")
    echo "Found: reports/"
fi

if [ -d "logs" ]; then
    DIRS_TO_ARCHIVE+=("logs")
    echo "Found: logs/"
fi

# Stop if nothing is available to archive
if [ ${#DIRS_TO_ARCHIVE[@]} -eq 0 ]; then
    echo
    echo "[ERROR] No output directories found."
    echo "Expected one or more of:"
    echo "  processed/"
    echo "  reports/"
    echo "  logs/"
    exit 1
fi

echo
echo "[ARCHIVE] Creating compressed archive..."

tar -czf "$ARCHIVE_FILE" "${DIRS_TO_ARCHIVE[@]}"

echo
echo "============================================================"
echo "ARCHIVE CREATED SUCCESSFULLY"
echo "============================================================"
echo "Archive: $ARCHIVE_FILE"
echo

ls -lh "$ARCHIVE_FILE"

echo
echo "[VERIFY] Archive contents:"
tar -tzf "$ARCHIVE_FILE" | head -20

echo
echo "Done."
