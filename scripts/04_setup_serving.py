#!/usr/bin/env python3
"""
Phase 5: Serving Layer Setup
==============================
Creates the refund_live_cases serving table with liquid clustering.

Run:
  source config.env
  uv run --with "databricks-connect>=16.4,<17.0" scripts/04_setup_serving.py
"""

import os

CATALOG = os.environ.get("REFUND_CATALOG", "refund_decisioning")
PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")

os.environ["DATABRICKS_CONFIG_PROFILE"] = PROFILE

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()

print(f"=== Phase 5: Serving Layer Setup ===")
print(f"  Catalog: {CATALOG}")
print(f"  Profile: {PROFILE}")
print()

# Create refund_live_cases with liquid clustering
print("--- Creating refund_live_cases ---")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.refund_serving.refund_live_cases (
        refund_id STRING,
        workflow_state STRING,
        abuse_risk_tier STRING,
        order_id STRING,
        customer_id BIGINT,
        first_name STRING,
        last_name STRING,
        tier STRING,
        reason_code STRING,
        amount DOUBLE,
        channel STRING,
        request_date DATE,
        item_condition STRING,
        order_amount DOUBLE,
        order_date DATE,
        payment_method STRING,
        product_name STRING,
        product_category STRING,
        product_price DOUBLE,
        is_high_value BOOLEAN,
        days_since_order INT,
        within_return_window BOOLEAN,
        delivery_confirmed BOOLEAN,
        delivery_issues STRING,
        refund_to_order_ratio DOUBLE,
        lifetime_orders INT,
        lifetime_refunds INT,
        cust_refunds_90d INT,
        cust_refund_amt_90d DOUBLE,
        refund_frequency STRING,
        household_id STRING,
        coordinated_timing_flag BOOLEAN,
        risk_score DOUBLE,
        recommended_action STRING,
        recommendation_reasons_json STRING,
        csr_action STRING,
        csr_reason STRING,
        csr_acted_at TIMESTAMP,
        ai_explanation STRING
    )
    CLUSTER BY (workflow_state, abuse_risk_tier)
""")

# Populate from gold layer
print("--- Populating from gold layer ---")
spark.sql(f"TRUNCATE TABLE {CATALOG}.refund_serving.refund_live_cases")
spark.sql(f"""
    INSERT INTO {CATALOG}.refund_serving.refund_live_cases
    SELECT
        c.refund_id,
        CASE
            WHEN c.recommended_action = 'APPROVE' AND c.risk_score < 0.2 THEN 'auto_approved'
            WHEN c.recommended_action = 'REJECT' THEN 'pending_review'
            WHEN c.recommended_action = 'ESCALATE' THEN 'pending_review'
            ELSE 'pending_review'
        END as workflow_state,
        CASE
            WHEN c.risk_score >= 0.7 THEN 'CRITICAL'
            WHEN c.risk_score >= 0.4 THEN 'HIGH'
            WHEN c.risk_score >= 0.2 THEN 'MEDIUM'
            ELSE 'LOW'
        END as abuse_risk_tier,
        c.order_id,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.tier,
        c.reason_code,
        c.amount,
        c.channel,
        c.request_date,
        c.item_condition,
        c.order_amount,
        c.order_date,
        c.payment_method,
        c.product_name,
        c.product_category,
        c.product_price,
        c.is_high_value,
        c.days_since_order,
        c.within_return_window,
        c.delivery_confirmed,
        c.delivery_issues,
        c.refund_to_order_ratio,
        c.lifetime_orders,
        c.lifetime_refunds,
        c.cust_refunds_90d,
        c.cust_refund_amt_90d,
        c.refund_frequency,
        c.household_id,
        c.coordinated_timing_flag,
        c.risk_score,
        c.recommended_action,
        c.recommendation_reasons_json,
        NULL as csr_action,
        NULL as csr_reason,
        NULL as csr_acted_at,
        NULL as ai_explanation
    FROM {CATALOG}.refund_gold.refund_case_decisioning c
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.refund_serving.refund_live_cases").collect()[0]["cnt"]
print(f"  {count:,} live cases created")

# Show distribution
print("\n--- Workflow state distribution ---")
dist = spark.sql(f"""
    SELECT workflow_state, abuse_risk_tier, COUNT(*) as cnt
    FROM {CATALOG}.refund_serving.refund_live_cases
    GROUP BY workflow_state, abuse_risk_tier
    ORDER BY workflow_state, abuse_risk_tier
""").collect()
for row in dist:
    print(f"  {row['workflow_state']:20s} | {row['abuse_risk_tier']:10s} | {row['cnt']:,}")

spark.stop()

print(f"\n=== Phase 5 Complete ===")
print(f"  Table: {CATALOG}.refund_serving.refund_live_cases")
