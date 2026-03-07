#!/usr/bin/env python3
"""
Phase 6: Genie Space Setup
============================
Creates the 'Refund Abuse Intelligence' Genie Space.

Run:
  source config.env
  python3 scripts/05_setup_genie.py
"""

import json
import os
import subprocess
import sys

PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("REFUND_CATALOG", "refund_decisioning")

if not WAREHOUSE_ID:
    print("ERROR: DATABRICKS_WAREHOUSE_ID not set. Source config.env first.")
    sys.exit(1)


def get_token():
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", PROFILE],
        capture_output=True, text=True,
    )
    data = json.loads(result.stdout)
    return data.get("access_token") or data.get("token_value") or data["token"]


def get_host():
    result = subprocess.run(
        ["databricks", "auth", "profiles", "--output", "json"],
        capture_output=True, text=True,
    )
    profiles = json.loads(result.stdout)
    for p in profiles.get("profiles", []):
        if p.get("name") == PROFILE:
            return p.get("host", "")
    return os.environ.get("DATABRICKS_HOST", "")


def api_call(method, path, data=None):
    import urllib.request
    token = get_token()
    host = get_host().rstrip("/")
    url = f"{host}{path}"

    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        try:
            return json.loads(error_body)
        except json.JSONDecodeError:
            return {"error": error_body, "status_code": e.code}


print("=== Phase 6: Genie Space Setup ===")
print(f"  Profile:   {PROFILE}")
print(f"  Warehouse: {WAREHOUSE_ID}")
print(f"  Catalog:   {CATALOG}")
print()

serialized_space = {
    "version": 2,
    "data_sources": {
        "tables": [
            {
                "identifier": f"{CATALOG}.refund_gold.refund_case_decisioning",
                "description": [
                    "Per-refund-request risk assessment with composite risk score (0-1), "
                    "recommended action (APPROVE/REJECT/ESCALATE), and scoring breakdown."
                ],
            },
            {
                "identifier": f"{CATALOG}.refund_gold.refund_customer_360",
                "description": [
                    "Per-customer abuse profile with refund rate, risk tier (LOW/MEDIUM/HIGH/CRITICAL), "
                    "household signals, and refund frequency over 30d and 90d windows."
                ],
            },
            {
                "identifier": f"{CATALOG}.refund_gold.refund_segment_summary",
                "description": [
                    "Aggregated refund metrics by reason_code, channel, customer tier, "
                    "and risk tier. Includes approval, escalation, and rejection rates."
                ],
            },
        ]
    },
    "instructions": {
        "text_instructions": [
            {
                "id": "a0000000000000000000000000000001",
                "content": [
                    f"approval_rate is the fraction of cases with recommended_action = 'APPROVE' "
                    f"out of total cases. Use refund_case_decisioning or refund_segment_summary.",

                    f"abuse_rate is the percentage of customers with risk_tier in ('HIGH', 'CRITICAL') "
                    f"in the refund_customer_360 table.",

                    f"escalation_rate is the fraction of cases with recommended_action = 'ESCALATE' "
                    f"out of total cases.",

                    f"refund_leakage is the total dollar amount of refunds for cases where "
                    f"recommended_action = 'REJECT' but the historical action was 'approved'. "
                    f"This represents money lost to abuse.",

                    f"household_coordination means multiple members of the same household "
                    f"filed refunds within 48 hours. The coordinated_timing_flag in "
                    f"refund_customer_360 indicates this pattern.",
                ],
            }
        ],
        "example_question_sqls": [
            {
                "id": "b0000000000000000000000000000001",
                "question": [
                    "Show me the top 20 customers by abuse risk with their refund patterns"
                ],
                "sql": [
                    f'SELECT customer_id, first_name, last_name, tier, risk_tier, '
                    f'refund_rate, refunds_90d, total_refund_amount_90d, '
                    f'coordinated_timing_flag '
                    f'FROM {CATALOG}.refund_gold.refund_customer_360 '
                    f'WHERE risk_tier IN ("HIGH", "CRITICAL") '
                    f'ORDER BY refund_rate DESC '
                    f'LIMIT 20'
                ],
            },
            {
                "id": "b0000000000000000000000000000002",
                "question": [
                    "What is the refund leakage by product category?"
                ],
                "sql": [
                    f'SELECT product_category, '
                    f'COUNT(*) as total_cases, '
                    f'SUM(CASE WHEN recommended_action = "REJECT" THEN amount ELSE 0 END) as potential_leakage, '
                    f'ROUND(AVG(risk_score), 3) as avg_risk_score '
                    f'FROM {CATALOG}.refund_gold.refund_case_decisioning '
                    f'GROUP BY product_category '
                    f'ORDER BY potential_leakage DESC'
                ],
            },
            {
                "id": "b0000000000000000000000000000003",
                "question": [
                    "Show escalation rates by channel and reason code"
                ],
                "sql": [
                    f'SELECT channel, reason_code, total_cases, '
                    f'escalation_rate, avg_risk_score '
                    f'FROM {CATALOG}.refund_gold.refund_segment_summary '
                    f'WHERE escalation_rate > 0.1 '
                    f'ORDER BY escalation_rate DESC'
                ],
            },
        ],
    },
}

print("--- Creating Genie Space ---")
create_payload = {
    "title": "Refund Abuse Intelligence",
    "description": "Conversational BI for refund abuse analytics and decisioning.",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(serialized_space),
}

result = api_call("POST", "/api/2.0/genie/spaces", create_payload)

if "space_id" in result:
    space_id = result["space_id"]
    print(f"  Created space: {space_id}")
elif "error_code" in result:
    print(f"  Error: {result.get('message', result.get('error_code'))}")
    print("  Attempting to find existing space...")
    spaces_result = api_call("GET", "/api/2.0/genie/spaces")
    space_id = None
    for space in spaces_result.get("spaces", []):
        if space.get("title") == "Refund Abuse Intelligence":
            space_id = space["space_id"]
            print(f"  Found existing space: {space_id}")
            update_result = api_call(
                "PATCH",
                f"/api/2.0/genie/spaces/{space_id}",
                {"serialized_space": json.dumps(serialized_space)},
            )
            print(f"  Updated with full configuration")
            break

    if not space_id:
        print("  ERROR: Could not create or find Genie Space")
        sys.exit(1)
else:
    space_id = result.get("space_id", "UNKNOWN")

host = get_host().rstrip("/")

# Write space ID for deploy_all.sh to consume
with open("/tmp/refund_genie_space_id.txt", "w") as f:
    f.write(space_id)

print()
print("=== Phase 6 Complete ===")
print(f"  Space ID: {space_id}")
print(f"  URL: {host}/genie/rooms/{space_id}")
print()
print("  Tables: 3 (refund_customer_360, refund_case_decisioning, refund_segment_summary)")
print("  Text instructions: 5")
print("  Certified SQL examples: 3")
