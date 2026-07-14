#!/bin/ksh
#############################################################################
# inventory_snapshot.ksh
# Deployed script for graph mp/inventory_snapshot.mp
# Usage: inventory_snapshot.ksh <BUSINESS_DATE:YYYYMMDD>
#############################################################################

set -e
set -o pipefail

: ${AB_HOME:?"AB_HOME not set - source config/abinitio.env first"}
: ${PROJECT_DIR:?"PROJECT_DIR not set - source sand/project.pset first"}

export PATH=$AB_HOME/bin:$PATH

BUSINESS_DATE=${1:?"usage: inventory_snapshot.ksh <YYYYMMDD>"}
export BUSINESS_DATE

. $PROJECT_DIR/sand/sandbox.pset
. $PROJECT_DIR/dbc/edw.dbc.env

AI_SERIAL=$PROJECT_DIR/data/serial
INV_INPUT_FILE=$PROJECT_DIR/data/in/inventory_${BUSINESS_DATE}.dat
INV_OUT_FILE=$PROJECT_DIR/data/out/inventory_value_${BUSINESS_DATE}.dat

echo "[$(date '+%Y-%m-%d %H:%M:%S')] START inventory_snapshot BUSINESS_DATE=$BUSINESS_DATE"

# 1. Filter active stores / non-negative on-hand (FILTER_BY_EXPRESSION)
m_dump $PROJECT_DIR/dml/inventory.dml $INV_INPUT_FILE \
  | m_filter -select 'in.on_hand_qty >= 0 && in.store_status == "OPEN"' \
  > $AI_SERIAL/inv_active_${BUSINESS_DATE}.dat

# 2. Sort + ROLLUP to store x department value
m_sort -key "store_id department" $AI_SERIAL/inv_active_${BUSINESS_DATE}.dat \
  > $AI_SERIAL/inv_sorted_${BUSINESS_DATE}.dat

m_rollup \
  -key "store_id department" \
  -transform $PROJECT_DIR/xfr/inventory_rollup.xfr \
  $AI_SERIAL/inv_sorted_${BUSINESS_DATE}.dat \
  > $INV_OUT_FILE

# 3. Load target table EDW.F_INVENTORY_VALUE
m_db load $PROJECT_DIR/dbc/edw.dbc \
  -table EDW.F_INVENTORY_VALUE \
  -input $INV_OUT_FILE \
  -record-format $PROJECT_DIR/dml/inventory_value.dml \
  -mode truncate

echo "[$(date '+%Y-%m-%d %H:%M:%S')] END   inventory_snapshot BUSINESS_DATE=$BUSINESS_DATE"
