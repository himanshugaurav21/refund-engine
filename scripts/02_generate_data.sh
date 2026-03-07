#!/usr/bin/env bash
# =============================================================================
# Phase 2: Mock Data Generation
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
source "${ROOT_DIR}/config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE in config.env}"
: "${REFUND_CATALOG:=refund_decisioning}"

echo "=== Phase 2: Data Generation ==="
echo "  Profile: ${DATABRICKS_PROFILE}"
echo "  Catalog: ${REFUND_CATALOG}"
echo ""

export DATABRICKS_PROFILE
export REFUND_CATALOG
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_PROFILE}"

cd "${ROOT_DIR}"
uv run --with polars --with numpy --with "databricks-connect>=16.4,<17.0" \
    generate_data.py

echo ""
echo "=== Phase 2 Complete ==="

# Upload CSVs to Volume via Files API
echo ""
echo "--- Uploading CSVs to Volume ---"
CSV_DIR="/tmp/refund_engine_csvs"
HOST="${DATABRICKS_HOST:-}"

if [ -z "$HOST" ]; then
    echo "  ERROR: DATABRICKS_HOST not set. Source config.env first."
    exit 1
fi

TOKEN=$(databricks auth token --profile "${DATABRICKS_PROFILE}" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token') or d.get('token_value') or d.get('token',''))")

if [ -z "$TOKEN" ]; then
    echo "  ERROR: Could not obtain auth token. Check your DATABRICKS_PROFILE."
    exit 1
fi

for table in refund_requests order_history customer_profiles household_mappings \
             store_locations product_catalog delivery_events refund_policies \
             historical_decisions; do
    echo "  Uploading ${table}.csv..."
    curl -s -X PUT "${HOST}/api/2.0/fs/files/Volumes/${REFUND_CATALOG}/refund_bronze/refund_source_files/${table}.csv" \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/octet-stream" \
        --data-binary "@${CSV_DIR}/${table}.csv" > /dev/null
done

echo ""
echo "=== CSV Upload Complete ==="
