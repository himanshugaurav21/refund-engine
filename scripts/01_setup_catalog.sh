#!/usr/bin/env bash
# =============================================================================
# Phase 1: Catalog & Schema Setup
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE in config.env}"
: "${DATABRICKS_WAREHOUSE_ID:?Set DATABRICKS_WAREHOUSE_ID in config.env}"
: "${REFUND_CATALOG:=refund_decisioning}"

P="--profile ${DATABRICKS_PROFILE}"

echo "=== Phase 1: Catalog & Schema Setup ==="
echo "  Profile:   ${DATABRICKS_PROFILE}"
echo "  Warehouse: ${DATABRICKS_WAREHOUSE_ID}"
echo "  Catalog:   ${REFUND_CATALOG}"
echo ""

run_sql() {
    local sql="$1"
    echo "  SQL: ${sql}"
    databricks api post /api/2.0/sql/statements ${P} --json "{
        \"warehouse_id\": \"${DATABRICKS_WAREHOUSE_ID}\",
        \"statement\": \"${sql}\",
        \"wait_timeout\": \"30s\"
    }" | python3 -c "
import sys, json
d = json.load(sys.stdin)
status = d.get('status', {}).get('state', 'UNKNOWN')
if status == 'FAILED':
    err = d.get('status', {}).get('error', {}).get('message', 'Unknown')
    print(f'  FAILED: {err}')
    sys.exit(1)
else:
    print(f'  OK ({status})')
"
}

# Create catalog
run_sql "CREATE CATALOG IF NOT EXISTS ${REFUND_CATALOG}"

# Create schemas
for schema in refund_bronze refund_silver refund_gold refund_serving; do
    run_sql "CREATE SCHEMA IF NOT EXISTS ${REFUND_CATALOG}.${schema}"
done

# Create Volume for CSV source files
run_sql "CREATE VOLUME IF NOT EXISTS ${REFUND_CATALOG}.refund_bronze.refund_source_files"

echo ""
echo "=== Phase 1 Complete ==="
echo "  Catalog: ${REFUND_CATALOG}"
echo "  Schemas: refund_bronze, refund_silver, refund_gold, refund_serving"
echo "  Volume:  ${REFUND_CATALOG}.refund_bronze.refund_source_files"
