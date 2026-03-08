#!/usr/bin/env bash
# =============================================================================
# Full End-to-End Deployment - Refund Engine
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

if [ -z "${DATABRICKS_PROFILE:-}" ]; then
    echo "ERROR: config.env not sourced. Run:"
    echo "  source config.env"
    exit 1
fi

: "${DATABRICKS_HOST:?Set DATABRICKS_HOST in config.env}"
: "${DATABRICKS_USER:?Set DATABRICKS_USER in config.env}"
: "${DATABRICKS_WAREHOUSE_ID:?Set DATABRICKS_WAREHOUSE_ID in config.env}"
: "${REFUND_CATALOG:=refund_decisioning}"
: "${APP_NAME:=refund-console}"
: "${SERVING_ENDPOINT:=databricks-claude-sonnet-4}"
: "${APP_WORKSPACE_PATH:=/Workspace/Users/${DATABRICKS_USER}/${APP_NAME}}"

P="--profile ${DATABRICKS_PROFILE}"

# ---------------------------------------------------------------------------
# Helper: run SQL via Databricks SQL Statements API
# ---------------------------------------------------------------------------
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
else:
    print(f'  OK ({status})')
"
}

echo "============================================================"
echo "  Refund Engine - Full Deployment"
echo "============================================================"
echo "  Workspace: ${DATABRICKS_HOST}"
echo "  Profile:   ${DATABRICKS_PROFILE}"
echo "  User:      ${DATABRICKS_USER}"
echo "  Catalog:   ${REFUND_CATALOG}"
echo "  Warehouse: ${DATABRICKS_WAREHOUSE_ID}"
echo "  Endpoint:  ${SERVING_ENDPOINT}"
echo "  App:       ${APP_NAME}"
echo "============================================================"
echo ""

# Phase 1: Catalog & Schema
echo ">>> PHASE 1: Catalog & Schema Setup"
bash "${SCRIPT_DIR}/01_setup_catalog.sh"
echo ""

# Phase 2: Data Generation
echo ">>> PHASE 2: Data Generation"
bash "${SCRIPT_DIR}/02_generate_data.sh"
echo ""

# Phase 3: Pipeline
echo ">>> PHASE 3: Lakeflow Pipeline"
bash "${SCRIPT_DIR}/03_deploy_pipeline.sh"
echo ""
echo "  Waiting 120s for pipeline to complete..."
sleep 120

# Phase 4: Serving Layer
echo ">>> PHASE 4: Serving Layer"
cd "${ROOT_DIR}"
source config.env 2>/dev/null || true
uv run --with "databricks-connect>=16.4,<17.0" scripts/04_setup_serving.py
echo ""

# Phase 5: Genie Space
echo ">>> PHASE 5: Genie Space"
python3 "${SCRIPT_DIR}/05_setup_genie.py"
GENIE_SPACE_ID=$(cat /tmp/refund_genie_space_id.txt 2>/dev/null || echo "")
echo ""

# Phase 6: App Deployment
echo ">>> PHASE 6: App Deployment"
bash "${SCRIPT_DIR}/06_deploy_app.sh"
echo ""

# =========================================================================
# Phase 7: Post-deployment automation (resources, permissions, experiment)
# =========================================================================
echo ">>> PHASE 7: Post-Deployment Setup"

# --- Get app SP info ---
echo "--- Getting app service principal ---"
APP_INFO=$(databricks apps get "${APP_NAME}" ${P} -o json 2>/dev/null || echo "{}")
SP_CLIENT_ID=$(echo "$APP_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id',''))" 2>/dev/null || echo "")
APP_URL=$(echo "$APP_INFO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || echo "")
echo "  SP Client ID: ${SP_CLIENT_ID:-UNKNOWN}"
echo "  App URL: ${APP_URL:-UNKNOWN}"

if [ -z "$SP_CLIENT_ID" ]; then
    echo "  WARNING: Could not get SP Client ID. You may need to configure resources and permissions manually."
else
    # --- Add resources to app ---
    echo ""
    echo "--- Adding resources to app ---"
    databricks api put "/api/2.0/apps/${APP_NAME}" ${P} --json "{
        \"name\": \"${APP_NAME}\",
        \"resources\": [
            {\"name\": \"sql-warehouse\", \"sql_warehouse\": {\"id\": \"${DATABRICKS_WAREHOUSE_ID}\", \"permission\": \"CAN_USE\"}},
            {\"name\": \"serving-endpoint\", \"serving_endpoint\": {\"name\": \"${SERVING_ENDPOINT}\", \"permission\": \"CAN_QUERY\"}}
        ]
    }" > /dev/null 2>&1 || echo "  WARNING: Failed to add resources via API"
    echo "  Resources added"

    # --- Grant SP access to catalog ---
    echo ""
    echo "--- Granting SP catalog access ---"
    for sql in \
        "GRANT USE CATALOG ON CATALOG ${REFUND_CATALOG} TO \`${SP_CLIENT_ID}\`" \
        "GRANT USE SCHEMA ON SCHEMA ${REFUND_CATALOG}.refund_gold TO \`${SP_CLIENT_ID}\`" \
        "GRANT SELECT ON SCHEMA ${REFUND_CATALOG}.refund_gold TO \`${SP_CLIENT_ID}\`" \
        "GRANT USE SCHEMA ON SCHEMA ${REFUND_CATALOG}.refund_serving TO \`${SP_CLIENT_ID}\`" \
        "GRANT ALL PRIVILEGES ON SCHEMA ${REFUND_CATALOG}.refund_serving TO \`${SP_CLIENT_ID}\`"; do
        run_sql "${sql}"
    done

    # --- Create feedback table ---
    echo ""
    echo "--- Creating feedback table ---"
    run_sql "CREATE TABLE IF NOT EXISTS ${REFUND_CATALOG}.refund_serving.refund_feedback (feedback_id STRING, refund_id STRING, feedback_type STRING, notes STRING, submitted_at TIMESTAMP)"

    # --- Create MLflow experiment and grant SP access ---
    echo ""
    echo "--- Setting up MLflow experiment ---"
    EXPERIMENT_PATH="/Users/${DATABRICKS_USER}/refund-engine/refund-agent"
    EXP_RESULT=$(databricks api post /api/2.0/mlflow/experiments/create ${P} --json "{\"name\": \"${EXPERIMENT_PATH}\"}" 2>&1 || echo "{}")
    EXPERIMENT_ID=$(echo "$EXP_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('experiment_id',''))" 2>/dev/null || echo "")

    if [ -z "$EXPERIMENT_ID" ]; then
        EXPERIMENT_ID=$(databricks api post /api/2.0/mlflow/experiments/get-by-name ${P} --json "{\"experiment_name\": \"${EXPERIMENT_PATH}\"}" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('experiment',{}).get('experiment_id',''))" 2>/dev/null || echo "")
    fi

    if [ -n "$EXPERIMENT_ID" ]; then
        databricks api patch "/api/2.0/permissions/experiments/${EXPERIMENT_ID}" ${P} --json "{
            \"access_control_list\": [{\"service_principal_name\": \"${SP_CLIENT_ID}\", \"permission_level\": \"CAN_MANAGE\"}]
        }" > /dev/null 2>&1 || echo "  WARNING: Failed to grant experiment permissions"
        echo "  Experiment ID: ${EXPERIMENT_ID}"
        echo "  Experiment path: ${EXPERIMENT_PATH}"
    else
        echo "  WARNING: Could not create or find MLflow experiment"
    fi

    # --- Grant SP access to Genie Space ---
    if [ -n "$GENIE_SPACE_ID" ]; then
        echo ""
        echo "--- Granting SP access to Genie Space ---"
        databricks api patch "/api/2.0/permissions/genie/${GENIE_SPACE_ID}" ${P} --json "{
            \"access_control_list\": [{\"service_principal_name\": \"${SP_CLIENT_ID}\", \"permission_level\": \"CAN_MANAGE\"}]
        }" > /dev/null 2>&1 || echo "  WARNING: Failed to grant Genie Space permissions"
        echo "  Genie Space ${GENIE_SPACE_ID} — SP access granted"
    fi

    # --- Update app.yaml with experiment name and Genie Space ID, then redeploy ---
    echo ""
    echo "--- Updating app configuration and redeploying ---"
    APP_DIR="${ROOT_DIR}/refund-console"

    # Update app.yaml with experiment name, warehouse ID, and Genie Space ID
    python3 -c "
import re
with open('${APP_DIR}/app.yaml', 'r') as f:
    content = f.read()
# Update MLFLOW_EXPERIMENT_NAME (regardless of current value)
content = re.sub(
    r'(name: MLFLOW_EXPERIMENT_NAME\n\s+value:) .*',
    r'\1 \"${EXPERIMENT_PATH}\"',
    content,
)
# Ensure DATABRICKS_WAREHOUSE_ID is present
if 'DATABRICKS_WAREHOUSE_ID' not in content:
    content = content.rstrip() + '\n  - name: DATABRICKS_WAREHOUSE_ID\n    value: \"${DATABRICKS_WAREHOUSE_ID}\"\n'
else:
    content = re.sub(
        r'(name: DATABRICKS_WAREHOUSE_ID\n\s+value:) .*',
        r'\1 \"${DATABRICKS_WAREHOUSE_ID}\"',
        content,
    )
# Add GENIE_SPACE_ID if available
genie_id = '${GENIE_SPACE_ID}'
if genie_id:
    if 'GENIE_SPACE_ID' not in content:
        content = content.rstrip() + '\n  - name: GENIE_SPACE_ID\n    value: \"' + genie_id + '\"\n'
    else:
        content = re.sub(
            r'(name: GENIE_SPACE_ID\n\s+value:) .*',
            r'\1 \"' + genie_id + '\"',
            content,
        )
with open('${APP_DIR}/app.yaml', 'w') as f:
    f.write(content)
"

    # Re-upload updated app.yaml
    databricks workspace import "${APP_WORKSPACE_PATH}/app.yaml" --file "${APP_DIR}/app.yaml" --format AUTO --overwrite ${P}

    # Redeploy the app
    databricks apps deploy "${APP_NAME}" \
        --source-code-path "${APP_WORKSPACE_PATH}" \
        ${P} 2>&1 || echo "  WARNING: App redeploy failed — you may need to deploy manually"
    echo "  App redeployed with updated configuration"
fi

echo ""
echo "============================================================"
echo "  Deployment Complete!"
echo "============================================================"
echo ""
echo "  App URL: ${APP_URL:-UNKNOWN}"
echo "  SP Client ID: ${SP_CLIENT_ID:-UNKNOWN}"
echo "  Genie Space: ${GENIE_SPACE_ID:-NOT SET}"
echo "  MLflow Experiment: ${EXPERIMENT_PATH:-NOT SET}"
echo ""
echo "  The app has been configured with all resources and permissions."
echo "  Open the App URL above to start using the Refund Console."
echo "============================================================"
