#!/usr/bin/env bash
# =============================================================================
# Phase 3: Lakeflow Declarative Pipeline Deployment
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
source "${ROOT_DIR}/config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE in config.env}"
: "${DATABRICKS_USER:?Set DATABRICKS_USER in config.env}"
: "${REFUND_CATALOG:=refund_decisioning}"
: "${PIPELINE_NAME:=refund-engine-pipeline}"
: "${PIPELINE_TARGET_SCHEMA:=refund_gold}"

P="--profile ${DATABRICKS_PROFILE}"
NOTEBOOK_PATH="/Users/${DATABRICKS_USER}/refund-engine/pipeline_notebook"
AGENT_NOTEBOOK_PATH="/Users/${DATABRICKS_USER}/refund-engine/refund_agent_notebook"

echo "=== Phase 3: Pipeline Deployment ==="
echo "  Notebook: ${NOTEBOOK_PATH}"
echo "  Pipeline: ${PIPELINE_NAME}"
echo ""

# Ensure workspace directory exists
echo "--- Creating workspace directory ---"
databricks workspace mkdirs "/Users/${DATABRICKS_USER}/refund-engine" ${P}

# Upload the pipeline notebook
echo "--- Uploading pipeline notebook ---"
databricks workspace import "${NOTEBOOK_PATH}" --file "${ROOT_DIR}/pipeline_notebook.py" \
    --format SOURCE --language PYTHON --overwrite ${P}
echo "  Pipeline notebook uploaded to ${NOTEBOOK_PATH}"

# Upload the agent notebook
echo ""
echo "--- Uploading agent notebook ---"
databricks workspace import "${AGENT_NOTEBOOK_PATH}" --file "${ROOT_DIR}/refund_agent_notebook.py" \
    --format SOURCE --language PYTHON --overwrite ${P}
echo "  Agent notebook uploaded to ${AGENT_NOTEBOOK_PATH}"

# Create the pipeline
echo ""
echo "--- Creating DLT pipeline ---"
PIPELINE_JSON=$(cat <<EOF
{
    "name": "${PIPELINE_NAME}",
    "catalog": "${REFUND_CATALOG}",
    "target": "${PIPELINE_TARGET_SCHEMA}",
    "serverless": true,
    "continuous": false,
    "channel": "CURRENT",
    "libraries": [
        {"notebook": {"path": "${NOTEBOOK_PATH}"}}
    ],
    "configuration": {
        "pipelines.enableTrackHistory": "true"
    }
}
EOF
)

RESULT=$(databricks api post /api/2.0/pipelines ${P} --json "${PIPELINE_JSON}" 2>&1)
PIPELINE_ID=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pipeline_id',''))" 2>/dev/null || echo "")

if [ -z "$PIPELINE_ID" ]; then
    echo "  Pipeline may already exist. Checking..."
    PIPELINE_ID=$(databricks pipelines list ${P} -o json 2>/dev/null | \
        python3 -c "import sys,json; pipelines=json.load(sys.stdin); print(next((p['pipeline_id'] for p in pipelines if p.get('name')=='${PIPELINE_NAME}'), ''))" 2>/dev/null || echo "")
fi

if [ -n "$PIPELINE_ID" ]; then
    echo "  Pipeline ID: ${PIPELINE_ID}"
    echo ""
    echo "--- Starting pipeline (full refresh) ---"
    databricks pipelines start-update "${PIPELINE_ID}" --full-refresh ${P} || true
    echo "  Pipeline update triggered. Monitor in the Databricks UI."
else
    echo "  ERROR: Could not create or find pipeline. Create manually in the UI."
fi

echo ""
echo "=== Phase 3 Complete ==="
echo "  Pipeline ID: ${PIPELINE_ID:-UNKNOWN}"
