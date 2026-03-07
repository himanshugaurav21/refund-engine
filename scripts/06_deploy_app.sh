#!/usr/bin/env bash
# =============================================================================
# Phase 7: Databricks App Deployment
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
source "${ROOT_DIR}/config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE in config.env}"
: "${DATABRICKS_USER:?Set DATABRICKS_USER in config.env}"
: "${APP_NAME:=refund-console}"
: "${APP_WORKSPACE_PATH:=/Workspace/Users/${DATABRICKS_USER}/${APP_NAME}}"

P="--profile ${DATABRICKS_PROFILE}"
APP_DIR="${ROOT_DIR}/refund-console"

echo "=== Phase 7: App Deployment ==="
echo "  App name:       ${APP_NAME}"
echo "  Workspace path: ${APP_WORKSPACE_PATH}"
echo ""

# Step 1: Build frontend
echo "--- Building frontend ---"
cd "${APP_DIR}/frontend"
npm install
npm run build
echo "  Frontend built to frontend/dist/"

# Step 2: Create the Databricks App
echo ""
echo "--- Creating Databricks App ---"
databricks apps create "${APP_NAME}" --description "Refund Console - Abuse Decisioning & Triage" ${P} 2>&1 || \
    echo "  App may already exist (this is OK)"

# Step 3: Upload files to workspace (no sync - avoids node_modules)
echo ""
echo "--- Uploading app files to workspace ---"

# Ensure workspace directories exist
databricks workspace mkdirs "${APP_WORKSPACE_PATH}" ${P}
databricks workspace mkdirs "${APP_WORKSPACE_PATH}/server" ${P}
databricks workspace mkdirs "${APP_WORKSPACE_PATH}/server/routes" ${P}
databricks workspace mkdirs "${APP_WORKSPACE_PATH}/frontend/dist/assets" ${P}

# Upload backend files
for f in app.py app.yaml requirements.txt; do
    databricks workspace import "${APP_WORKSPACE_PATH}/$f" --file "${APP_DIR}/$f" --format AUTO --overwrite ${P}
done

for f in __init__.py config.py warehouse.py llm.py agent.py; do
    [ -f "${APP_DIR}/server/$f" ] && \
    databricks workspace import "${APP_WORKSPACE_PATH}/server/$f" --file "${APP_DIR}/server/$f" --format AUTO --overwrite ${P}
done

for f in __init__.py dashboard.py cases.py actions.py agent.py feedback.py genie.py; do
    [ -f "${APP_DIR}/server/routes/$f" ] && \
    databricks workspace import "${APP_WORKSPACE_PATH}/server/routes/$f" --file "${APP_DIR}/server/routes/$f" --format AUTO --overwrite ${P}
done

# Upload frontend dist
for f in $(find "${APP_DIR}/frontend/dist" -type f 2>/dev/null); do
    rel="${f#${APP_DIR}/}"
    target_dir=$(dirname "${APP_WORKSPACE_PATH}/${rel}")
    databricks workspace mkdirs "${target_dir}" ${P} 2>/dev/null || true
    databricks workspace import "${APP_WORKSPACE_PATH}/${rel}" --file "$f" --format AUTO --overwrite ${P}
done

echo "  Files uploaded to ${APP_WORKSPACE_PATH}"

# Step 4: Deploy
echo ""
echo "--- Deploying app ---"
databricks apps deploy "${APP_NAME}" \
    --source-code-path "${APP_WORKSPACE_PATH}" \
    ${P}

# Step 5: Get app URL
echo ""
echo "--- Getting app URL ---"
APP_URL=$(databricks apps get "${APP_NAME}" ${P} -o json 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('url', 'UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")

echo ""
echo "=== Phase 7 Complete ==="
echo "  App URL: ${APP_URL}"
echo ""
echo "  IMPORTANT: After first deployment, add resources in the Databricks UI:"
echo "    1. Go to Compute > Apps > ${APP_NAME} > Edit"
echo "    2. Add 'SQL Warehouse' resource -> Select your warehouse -> Permission: 'Can use'"
echo "    3. Add 'Model serving endpoint' resource -> Select '${SERVING_ENDPOINT:-databricks-claude-sonnet-4}' -> Permission: 'Can query'"
echo "    4. Redeploy to pick up the new environment variables"
echo ""
echo "  Grant the app's service principal access to the catalog:"
echo "    GRANT USE CATALOG ON CATALOG ${REFUND_CATALOG:-refund_decisioning} TO \`<sp-client-id>\`;"
echo "    GRANT USE SCHEMA ON SCHEMA ${REFUND_CATALOG:-refund_decisioning}.refund_gold TO \`<sp-client-id>\`;"
echo "    GRANT SELECT ON SCHEMA ${REFUND_CATALOG:-refund_decisioning}.refund_gold TO \`<sp-client-id>\`;"
echo "    GRANT USE SCHEMA ON SCHEMA ${REFUND_CATALOG:-refund_decisioning}.refund_serving TO \`<sp-client-id>\`;"
echo "    GRANT SELECT ON SCHEMA ${REFUND_CATALOG:-refund_decisioning}.refund_serving TO \`<sp-client-id>\`;"
echo "    GRANT MODIFY ON SCHEMA ${REFUND_CATALOG:-refund_decisioning}.refund_serving TO \`<sp-client-id>\`;"
