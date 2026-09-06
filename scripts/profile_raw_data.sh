#!/bin/bash

# ============================================
# profile_raw_data.sh
# Purpose:
#   Profile all raw Olist CSV files before
#   Python/PySpark processing.
# ============================================

set -uo pipefail

RAW_DIR="./raw"
LOG_DIR="./logs"

REPORT="$LOG_DIR/raw_profile_$(date +%Y%m%d_%H%M%S).txt"

mkdir -p "$LOG_DIR"

# --------------------------------------------
# Report Header
# --------------------------------------------

echo "==========================================" | tee "$REPORT"
echo "       OLIST RAW DATA PROFILE REPORT      " | tee -a "$REPORT"
echo "Generated: $(date)" | tee -a "$REPORT"
echo "==========================================" | tee -a "$REPORT"

# --------------------------------------------
# Check RAW directory
# --------------------------------------------

if [ ! -d "$RAW_DIR" ]; then
    echo "ERROR: Raw directory not found: $RAW_DIR" | tee -a "$REPORT"
    exit 1
fi

# --------------------------------------------
# Check CSV files
# --------------------------------------------

csv_files=("$RAW_DIR"/*.csv)

if [ ! -e "${csv_files[0]}" ]; then
    echo "ERROR: No CSV files found in $RAW_DIR" | tee -a "$REPORT"
    exit 1
fi

# --------------------------------------------
# Process every CSV file
# --------------------------------------------

file_count=0

for file in "${csv_files[@]}"; do

    file_count=$((file_count + 1))

    filename=$(basename "$file")

    echo "" | tee -a "$REPORT"
    echo "File: $filename" | tee -a "$REPORT"
    echo "------------------------------------------" | tee -a "$REPORT"

    # Total lines
    total_lines=$(wc -l < "$file")

    if [ "$total_lines" -gt 0 ]; then
        data_rows=$((total_lines - 1))
    else
        data_rows=0
    fi

    echo "Rows: $data_rows" | tee -a "$REPORT"

    # Number of columns
    header=$(head -n 1 "$file")

    col_count=$(echo "$header" | awk -F',' '{print NF}')

    echo "Columns: $col_count" | tee -a "$REPORT"

    # File size
    size=$(du -sh "$file" | cut -f1)

    echo "File Size: $size" | tee -a "$REPORT"

    # Header
    echo "Header:" | tee -a "$REPORT"
    echo "$header" | tee -a "$REPORT"

    # Empty-field check
    empty_count=$(awk -F',' '
        NR > 1 {
            for (i = 1; i <= NF; i++) {
                if ($i == "") count++
            }
        }
        END {
            print count + 0
        }
    ' "$file")

    echo "Empty field occurrences: $empty_count" | tee -a "$REPORT"

    echo "Status: Processed successfully" | tee -a "$REPORT"

done

# --------------------------------------------
# Final Summary
# --------------------------------------------

echo "" | tee -a "$REPORT"
echo "==========================================" | tee -a "$REPORT"
echo "Profiling complete." | tee -a "$REPORT"
echo "CSV files processed: $file_count" | tee -a "$REPORT"
echo "Report saved at: $REPORT" | tee -a "$REPORT"
echo "==========================================" | tee -a "$REPORT"
