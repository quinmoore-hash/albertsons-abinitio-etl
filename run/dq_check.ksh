#!/bin/ksh
#############################################################################
# dq_check.ksh
# Post-load data-quality gate for the nightly batch.
# Validates row counts and reject percentage against sandbox thresholds.
# Usage: dq_check.ksh <BUSINESS_DATE:YYYYMMDD>
#############################################################################

set -e
: ${PROJECT_DIR:?"PROJECT_DIR not set"}
BUSINESS_DATE=${1:?"usage: dq_check.ksh <YYYYMMDD>"}

. $PROJECT_DIR/sand/sandbox.pset

SUMMARY=$PROJECT_DIR/data/out/daily_sales_summary_${BUSINESS_DATE}.dat
REJECT=$PROJECT_DIR/data/out/reject_${BUSINESS_DATE}.dat

rows=$(m_wc -l $SUMMARY 2>/dev/null | awk '{print $1}')
rej=$(m_wc -l $REJECT 2>/dev/null | awk '{print $1}')
rows=${rows:-0}; rej=${rej:-0}

echo "[DQ] business_date=$BUSINESS_DATE summary_rows=$rows reject_rows=$rej"

if [ "$rows" -lt "$DQ_MIN_ROWCOUNT" ]; then
  echo "[DQ][FAIL] summary rows $rows below minimum $DQ_MIN_ROWCOUNT" >&2
  exit 2
fi

total=$((rows + rej))
if [ "$total" -gt 0 ]; then
  pct=$(echo "scale=2; $rej * 100 / $total" | bc)
  over=$(echo "$pct > $DQ_MAX_REJECT_PCT" | bc)
  if [ "$over" -eq 1 ]; then
    echo "[DQ][FAIL] reject pct ${pct}% exceeds max ${DQ_MAX_REJECT_PCT}%" >&2
    exit 3
  fi
fi

echo "[DQ][PASS] business_date=$BUSINESS_DATE"
