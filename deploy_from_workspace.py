# Databricks notebook source
# MAGIC %md
# MAGIC # Refund Abuse Decisioning Engine — Workspace Deployment
# MAGIC
# MAGIC **Deploy the entire Refund Engine directly from a Databricks workspace.**
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Clone this repo via **Git Folders**: Workspace > Git Folders > Add Git Folder
# MAGIC 2. A Serverless SQL Warehouse (get its ID from SQL Warehouses page)
# MAGIC 3. A Foundation Model endpoint (e.g., `databricks-claude-sonnet-4`)
# MAGIC
# MAGIC ## Phases
# MAGIC | Phase | Description |
# MAGIC |-------|-------------|
# MAGIC | 1 | Catalog & Schema Setup |
# MAGIC | 2 | Mock Data Generation |
# MAGIC | 3 | DLT Pipeline Deployment |
# MAGIC | 4 | Serving Layer (liquid clustering) |
# MAGIC | 5 | Genie Space |
# MAGIC | 6 | Databricks App |
# MAGIC | 7 | Post-Deployment (permissions, MLflow, feedback table) |
# MAGIC
# MAGIC Run all cells top-to-bottom, or run individual phases as needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set your workspace-specific values below. Only `WAREHOUSE_ID` typically needs changing.

# COMMAND ----------

# Widgets for configuration — change these for your workspace
dbutils.widgets.text("catalog", "refund_decisioning", "Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")
dbutils.widgets.text("serving_endpoint", "databricks-claude-sonnet-4", "Foundation Model Endpoint")
dbutils.widgets.text("app_name", "refund-console", "App Name")

# COMMAND ----------

import os, json, time, requests

CATALOG = dbutils.widgets.get("catalog")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
SERVING_ENDPOINT = dbutils.widgets.get("serving_endpoint")
APP_NAME = dbutils.widgets.get("app_name")

# Auto-detect current user and host
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
HOST = spark.conf.get("spark.databricks.workspaceUrl", "")
if HOST and not HOST.startswith("https://"):
    HOST = f"https://{HOST}"

# Derive paths
GIT_FOLDER_ROOT = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else "."
PIPELINE_NAME = "refund-engine-pipeline"
APP_WORKSPACE_PATH = f"/Workspace/Users/{CURRENT_USER}/{APP_NAME}"

if not WAREHOUSE_ID:
    raise ValueError("⚠️ WAREHOUSE_ID is required. Set it in the widget above.")

print(f"""
╔══════════════════════════════════════════════════════════════╗
║  Refund Engine — Workspace Deployment                       ║
╠══════════════════════════════════════════════════════════════╣
║  Workspace:  {HOST:<46s} ║
║  User:       {CURRENT_USER:<46s} ║
║  Catalog:    {CATALOG:<46s} ║
║  Warehouse:  {WAREHOUSE_ID:<46s} ║
║  Endpoint:   {SERVING_ENDPOINT:<46s} ║
║  App:        {APP_NAME:<46s} ║
╚══════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# Helper: Databricks API call using notebook context token
def api_call(method, path, data=None):
    """Make authenticated API call using notebook context."""
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    url = f"{HOST}{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.request(method, url, headers=headers, json=data, timeout=60)
    try:
        return resp.json()
    except Exception:
        return {"status_code": resp.status_code, "text": resp.text}

def run_sql_api(sql):
    """Execute SQL via SQL Statements API (for DDL that spark.sql can't do)."""
    result = api_call("POST", "/api/2.0/sql/statements", {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "30s"
    })
    status = result.get("status", {}).get("state", "UNKNOWN")
    if status == "FAILED":
        err = result.get("status", {}).get("error", {}).get("message", "Unknown error")
        print(f"  ❌ FAILED: {err}")
    else:
        print(f"  ✅ {status}")
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Catalog & Schema Setup

# COMMAND ----------

print("═══ PHASE 1: Catalog & Schema Setup ═══\n")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"  ✅ Catalog: {CATALOG}")

for schema in ["refund_bronze", "refund_silver", "refund_gold", "refund_serving"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"  ✅ Schema: {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.refund_bronze.refund_source_files")
print(f"  ✅ Volume: {CATALOG}.refund_bronze.refund_source_files")

print("\n═══ Phase 1 Complete ═══")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Mock Data Generation
# MAGIC
# MAGIC Generates synthetic refund data with realistic abuse patterns (8-12% abuse rate).
# MAGIC Uses PySpark directly — no external dependencies needed.

# COMMAND ----------

print("═══ PHASE 2: Data Generation ═══\n")

import random
from datetime import date, timedelta
from pyspark.sql import Row
from pyspark.sql.types import *

random.seed(42)

# ── Helper functions ──
def random_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 1, 31)

# ── 1. Customer Profiles (20,000) ──
print("  Generating customer_profiles (20,000)...")
first_names = ["John","Jane","Michael","Emily","David","Sarah","James","Emma","Robert","Olivia",
               "William","Sophia","Richard","Isabella","Charles","Mia","Thomas","Charlotte","Daniel","Amelia"]
last_names = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
              "Wilson","Anderson","Taylor","Thomas","Moore","Jackson","Martin","Lee","Thompson","White"]
tiers = ["Basic","Silver","Gold","Premium","Platinum"]
tier_weights = [0.35, 0.25, 0.20, 0.12, 0.08]

customers = []
for i in range(1, 20001):
    lt_orders = random.randint(1, 200)
    lt_refunds = int(lt_orders * random.uniform(0.02, 0.4))
    customers.append(Row(
        customer_id=100000 + i,
        first_name=random.choice(first_names),
        last_name=random.choice(last_names),
        email=f"customer{100000+i}@example.com",
        tier=random.choices(tiers, weights=tier_weights, k=1)[0],
        lifetime_orders=lt_orders,
        lifetime_refunds=lt_refunds,
        lifetime_spend=round(lt_orders * random.uniform(25, 500), 2),
        created_date=random_date(date(2018, 1, 1), date(2024, 6, 1)),
    ))

df_customers = spark.createDataFrame(customers)
df_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.customer_profiles")
print(f"    ✅ {df_customers.count()} rows")

# ── 2. Product Catalog (2,000) ──
print("  Generating product_catalog (2,000)...")
categories = ["Electronics","Clothing","Home & Kitchen","Sports","Toys","Books","Jewelry","Health","Automotive","Garden"]
products = []
for i in range(1, 2001):
    cat = random.choice(categories)
    base_price = {"Electronics": (100, 2000), "Jewelry": (50, 5000), "Clothing": (15, 300),
                  "Home & Kitchen": (10, 500), "Sports": (20, 800), "Toys": (5, 200),
                  "Books": (5, 60), "Health": (10, 150), "Automotive": (20, 1000), "Garden": (10, 400)}
    lo, hi = base_price.get(cat, (10, 500))
    price = round(random.uniform(lo, hi), 2)
    products.append(Row(
        product_id=f"PROD-{i:04d}", product_name=f"{cat} Item #{i}",
        category=cat, price=price,
        refund_rate_baseline=round(random.uniform(0.01, 0.15), 3),
        is_high_value=price > 500,
    ))
df_products = spark.createDataFrame(products)
df_products.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.product_catalog")
print(f"    ✅ {df_products.count()} rows")

# ── 3. Store Locations (200) ──
print("  Generating store_locations (200)...")
regions = ["Northeast","Southeast","Midwest","Southwest","West"]
states = {"Northeast": ["NY","NJ","PA","CT","MA"], "Southeast": ["FL","GA","NC","SC","VA"],
          "Midwest": ["IL","OH","MI","IN","WI"], "Southwest": ["TX","AZ","NM","OK","CO"],
          "West": ["CA","WA","OR","NV","UT"]}
stores = []
for i in range(1, 201):
    region = random.choice(regions)
    state = random.choice(states[region])
    stores.append(Row(store_id=f"STORE-{i:03d}", store_name=f"Store #{i}",
                      region=region, city=f"City-{i}", state=state))
df_stores = spark.createDataFrame(stores)
df_stores.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.store_locations")
print(f"    ✅ {df_stores.count()} rows")

# ── 4. Order History (200,000) ──
print("  Generating order_history (200,000)...")
payment_methods = ["credit_card","debit_card","paypal","gift_card","apple_pay"]
delivery_statuses = ["delivered","in_transit","returned","failed"]
cust_ids = [c.customer_id for c in customers]
orders = []
for i in range(1, 200001):
    orders.append(Row(
        order_id=f"ORD-{i:07d}", customer_id=random.choice(cust_ids),
        order_date=random_date(START_DATE, END_DATE),
        total_amount=round(random.uniform(10, 3000), 2),
        delivery_status=random.choices(delivery_statuses, weights=[0.85, 0.05, 0.05, 0.05], k=1)[0],
        payment_method=random.choice(payment_methods),
        store_id=f"STORE-{random.randint(1,200):03d}",
    ))
df_orders = spark.createDataFrame(orders)
df_orders.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.order_history")
print(f"    ✅ {df_orders.count()} rows")

# ── 5. Refund Requests (50,000) ──
print("  Generating refund_requests (50,000)...")
reason_codes = ["ITEM_NOT_RECEIVED","ITEM_DAMAGED","WRONG_ITEM","CHANGED_MIND","UNAUTHORIZED_PURCHASE","DEFECTIVE"]
channels = ["online","in_store","phone","chat"]
item_conditions = ["unopened","opened","damaged","missing"]
order_ids = [o.order_id for o in orders]
refund_requests = []
for i in range(1, 50001):
    oid = random.choice(order_ids[:100000])
    amt = round(random.uniform(5, 5000), 2)
    refund_requests.append(Row(
        refund_id=f"REF-{i:07d}", order_id=oid, customer_id=random.choice(cust_ids),
        reason_code=random.choice(reason_codes), amount=amt,
        channel=random.choice(channels), request_date=random_date(START_DATE, END_DATE),
        item_condition=random.choices(item_conditions, weights=[0.3, 0.35, 0.25, 0.1], k=1)[0],
        product_id=f"PROD-{random.randint(1,2000):04d}",
    ))
df_refunds = spark.createDataFrame(refund_requests)
df_refunds.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.refund_requests")
print(f"    ✅ {df_refunds.count()} rows")

# ── 6. Household Mappings (8,000) ──
print("  Generating household_mappings (8,000)...")
relationships = ["spouse","parent","child","sibling","roommate"]
households = []
hh_id = 0
used_customers = set()
for i in range(4000):
    hh_id += 1
    members = random.sample(cust_ids, random.randint(2, 4))
    for cid in members:
        if cid not in used_customers:
            used_customers.add(cid)
            households.append(Row(
                household_id=f"HH-{hh_id:05d}", customer_id=cid,
                relationship=random.choice(relationships), shared_address_flag=True,
            ))
            if len(households) >= 8000:
                break
    if len(households) >= 8000:
        break
df_households = spark.createDataFrame(households)
df_households.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.household_mappings")
print(f"    ✅ {df_households.count()} rows")

# ── 7. Delivery Events (180,000) ──
print("  Generating delivery_events (180,000)...")
carriers = ["UPS","FedEx","USPS","DHL","Amazon Logistics"]
delivery_issues_list = ["none","delayed","damaged_in_transit","wrong_address","weather_delay"]
deliveries = []
for i in range(1, 180001):
    oid = random.choice(order_ids[:180000])
    confirmed = random.random() < 0.9
    deliveries.append(Row(
        delivery_id=f"DEL-{i:07d}", order_id=oid,
        carrier=random.choice(carriers),
        status="delivered" if confirmed else random.choice(["in_transit","failed"]),
        delivery_date=random_date(START_DATE, END_DATE),
        delivery_issues=random.choices(delivery_issues_list, weights=[0.7, 0.1, 0.08, 0.07, 0.05], k=1)[0],
        photo_proof=confirmed and random.random() < 0.8,
    ))
df_deliveries = spark.createDataFrame(deliveries)
df_deliveries.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.delivery_events")
print(f"    ✅ {df_deliveries.count()} rows")

# ── 8. Refund Policies (50) ──
print("  Generating refund_policies (50)...")
policies = []
for i, cat in enumerate(categories * 5):
    policies.append(Row(
        policy_id=f"POL-{i+1:03d}", category=cat,
        max_refund_window_days=random.choice([14, 30, 45, 60, 90]),
        auto_approve_threshold=round(random.uniform(15, 50), 2),
        requires_return=random.random() < 0.7,
    ))
df_policies = spark.createDataFrame(policies[:50])
df_policies.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.refund_policies")
print(f"    ✅ {df_policies.count()} rows")

# ── 9. Historical Decisions (45,000) ──
print("  Generating historical_decisions (45,000)...")
actions = ["approved","rejected","escalated"]
action_weights = [0.65, 0.20, 0.15]
decisions = []
refund_ids = [r.refund_id for r in refund_requests]
for i in range(1, 45001):
    action = random.choices(actions, weights=action_weights, k=1)[0]
    decisions.append(Row(
        decision_id=f"DEC-{i:07d}", refund_id=refund_ids[i-1] if i <= len(refund_ids) else f"REF-{i:07d}",
        action=action, reason=f"Auto-{action}" if random.random() < 0.6 else "Manual review",
        override_flag=random.random() < 0.08,
    ))
df_decisions = spark.createDataFrame(decisions)
df_decisions.write.mode("overwrite").saveAsTable(f"{CATALOG}.refund_bronze.historical_decisions")
print(f"    ✅ {df_decisions.count()} rows")

print("\n═══ Phase 2 Complete ═══")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: DLT Pipeline Deployment
# MAGIC
# MAGIC Uploads the pipeline notebook and creates/starts the Lakeflow DLT pipeline.

# COMMAND ----------

print("═══ PHASE 3: DLT Pipeline Deployment ═══\n")

# The pipeline_notebook.py is already in the Git Folder.
# We need to get its workspace path for the DLT pipeline config.

# Detect current notebook path to infer Git Folder root
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
git_folder_root = "/".join(notebook_path.split("/")[:-1])
pipeline_notebook_path = f"{git_folder_root}/pipeline_notebook"

print(f"  Git Folder root: {git_folder_root}")
print(f"  Pipeline notebook: {pipeline_notebook_path}")

# Create the DLT pipeline
pipeline_config = {
    "name": PIPELINE_NAME,
    "catalog": CATALOG,
    "target": "refund_gold",
    "serverless": True,
    "continuous": False,
    "channel": "CURRENT",
    "libraries": [{"notebook": {"path": pipeline_notebook_path}}],
    "configuration": {"pipelines.enableTrackHistory": "true"},
}

result = api_call("POST", "/api/2.0/pipelines", pipeline_config)
pipeline_id = result.get("pipeline_id", "")

if not pipeline_id:
    print("  Pipeline may already exist. Searching...")
    list_result = api_call("GET", "/api/2.0/pipelines")
    for p in list_result.get("statuses", []):
        if p.get("name") == PIPELINE_NAME:
            pipeline_id = p["pipeline_id"]
            print(f"  Found existing pipeline: {pipeline_id}")
            # Update the pipeline to point to current notebook
            api_call("PUT", f"/api/2.0/pipelines/{pipeline_id}", pipeline_config)
            print(f"  Updated pipeline configuration")
            break

if pipeline_id:
    print(f"  Pipeline ID: {pipeline_id}")
    print("  Starting pipeline (full refresh)...")
    api_call("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", {"full_refresh": True})
    print("  ✅ Pipeline update triggered")
    print(f"  Monitor at: {HOST}/#joblist/pipelines/{pipeline_id}")
else:
    print("  ❌ Could not create or find pipeline. Create manually in the UI.")

print("\n═══ Phase 3 Complete ═══")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏳ Wait for Pipeline
# MAGIC
# MAGIC The DLT pipeline needs to finish before proceeding. This cell polls until complete.
# MAGIC You can also monitor progress in the Databricks UI.

# COMMAND ----------

if pipeline_id:
    print(f"Waiting for pipeline {pipeline_id} to complete...")
    print(f"Monitor at: {HOST}/#joblist/pipelines/{pipeline_id}\n")

    for attempt in range(60):  # Wait up to 10 minutes
        result = api_call("GET", f"/api/2.0/pipelines/{pipeline_id}")
        state = result.get("state", "UNKNOWN")
        latest = result.get("latest_updates", [{}])
        update_state = latest[0].get("state", "UNKNOWN") if latest else "UNKNOWN"

        if update_state in ("COMPLETED", "FAILED", "CANCELED"):
            if update_state == "COMPLETED":
                print(f"  ✅ Pipeline completed successfully!")
            else:
                print(f"  ❌ Pipeline {update_state}")
            break

        if attempt % 6 == 0:  # Print every 60 seconds
            print(f"  Pipeline state: {state} | Update: {update_state} ({attempt * 10}s elapsed)")
        time.sleep(10)
    else:
        print("  ⚠️ Timeout waiting for pipeline. Check the UI and continue manually.")
else:
    print("No pipeline ID — skipping wait.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Serving Layer

# COMMAND ----------

print("═══ PHASE 4: Serving Layer Setup ═══\n")

# Create refund_live_cases with liquid clustering
print("  Creating refund_live_cases table...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.refund_serving.refund_live_cases (
        refund_id STRING, workflow_state STRING, abuse_risk_tier STRING,
        order_id STRING, customer_id BIGINT, first_name STRING, last_name STRING,
        tier STRING, reason_code STRING, amount DOUBLE, channel STRING,
        request_date DATE, item_condition STRING, order_amount DOUBLE,
        order_date DATE, payment_method STRING, product_name STRING,
        product_category STRING, product_price DOUBLE, is_high_value BOOLEAN,
        days_since_order INT, within_return_window BOOLEAN, delivery_confirmed BOOLEAN,
        delivery_issues STRING, refund_to_order_ratio DOUBLE,
        lifetime_orders INT, lifetime_refunds INT, cust_refunds_90d INT,
        cust_refund_amt_90d DOUBLE, refund_frequency STRING,
        household_id STRING, coordinated_timing_flag BOOLEAN,
        risk_score DOUBLE, recommended_action STRING, recommendation_reasons_json STRING,
        csr_action STRING, csr_reason STRING, csr_acted_at TIMESTAMP, ai_explanation STRING
    )
    CLUSTER BY (workflow_state, abuse_risk_tier)
""")

# Populate from gold layer
print("  Populating from gold layer...")
spark.sql(f"TRUNCATE TABLE {CATALOG}.refund_serving.refund_live_cases")
spark.sql(f"""
    INSERT INTO {CATALOG}.refund_serving.refund_live_cases
    SELECT
        c.refund_id,
        CASE
            WHEN c.recommended_action = 'APPROVE' AND c.risk_score < 0.2 THEN 'auto_approved'
            ELSE 'pending_review'
        END as workflow_state,
        CASE
            WHEN c.risk_score >= 0.7 THEN 'CRITICAL'
            WHEN c.risk_score >= 0.4 THEN 'HIGH'
            WHEN c.risk_score >= 0.2 THEN 'MEDIUM'
            ELSE 'LOW'
        END as abuse_risk_tier,
        c.order_id, c.customer_id, c.first_name, c.last_name, c.tier,
        c.reason_code, c.amount, c.channel, c.request_date, c.item_condition,
        c.order_amount, c.order_date, c.payment_method,
        c.product_name, c.product_category, c.product_price, c.is_high_value,
        c.days_since_order, c.within_return_window, c.delivery_confirmed,
        c.delivery_issues, c.refund_to_order_ratio,
        c.lifetime_orders, c.lifetime_refunds, c.cust_refunds_90d,
        c.cust_refund_amt_90d, c.refund_frequency,
        c.household_id, c.coordinated_timing_flag,
        c.risk_score, c.recommended_action, c.recommendation_reasons_json,
        NULL, NULL, NULL, NULL
    FROM {CATALOG}.refund_gold.refund_case_decisioning c
""")

count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.refund_serving.refund_live_cases").collect()[0][0]
print(f"  ✅ {count:,} live cases created")

# Show distribution
dist = spark.sql(f"""
    SELECT workflow_state, abuse_risk_tier, COUNT(*) as cnt
    FROM {CATALOG}.refund_serving.refund_live_cases
    GROUP BY 1, 2 ORDER BY 1, 2
""").collect()
for row in dist:
    print(f"    {row['workflow_state']:20s} | {row['abuse_risk_tier']:10s} | {row['cnt']:,}")

# Create feedback table
print("\n  Creating refund_feedback table...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.refund_serving.refund_feedback (
        feedback_id STRING, refund_id STRING, feedback_type STRING,
        notes STRING, submitted_at TIMESTAMP
    )
""")
print("  ✅ Feedback table ready")

print("\n═══ Phase 4 Complete ═══")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5: Genie Space

# COMMAND ----------

print("═══ PHASE 5: Genie Space Setup ═══\n")

serialized_space = {
    "version": 2,
    "data_sources": {
        "tables": [
            {"identifier": f"{CATALOG}.refund_gold.refund_case_decisioning",
             "description": ["Per-refund risk assessment with composite risk score (0-1), recommended action, and scoring breakdown."]},
            {"identifier": f"{CATALOG}.refund_gold.refund_customer_360",
             "description": ["Per-customer abuse profile with refund rate, risk tier, household signals, and refund frequency."]},
            {"identifier": f"{CATALOG}.refund_gold.refund_segment_summary",
             "description": ["Aggregated refund metrics by reason_code, channel, customer tier, and risk tier."]},
        ]
    },
    "instructions": {
        "text_instructions": [{
            "id": "a0000000000000000000000000000001",
            "content": [
                "approval_rate = fraction of cases with recommended_action = 'APPROVE' out of total cases.",
                "abuse_rate = percentage of customers with risk_tier in ('HIGH', 'CRITICAL').",
                "escalation_rate = fraction of cases with recommended_action = 'ESCALATE'.",
                "refund_leakage = total dollar amount where recommended_action = 'REJECT' but historical action was 'approved'.",
                "household_coordination = multiple household members filed refunds within 48 hours.",
            ],
        }],
        "example_question_sqls": [
            {"id": "b0000000000000000000000000000001",
             "question": ["Show me the top 20 customers by abuse risk"],
             "sql": [f"SELECT customer_id, first_name, last_name, tier, risk_tier, refund_rate, refunds_90d FROM {CATALOG}.refund_gold.refund_customer_360 WHERE risk_tier IN ('HIGH', 'CRITICAL') ORDER BY refund_rate DESC LIMIT 20"]},
            {"id": "b0000000000000000000000000000002",
             "question": ["What is the refund leakage by product category?"],
             "sql": [f"SELECT product_category, COUNT(*) as total_cases, SUM(CASE WHEN recommended_action = 'REJECT' THEN amount ELSE 0 END) as potential_leakage FROM {CATALOG}.refund_gold.refund_case_decisioning GROUP BY product_category ORDER BY potential_leakage DESC"]},
        ],
    },
}

result = api_call("POST", "/api/2.0/genie/spaces", {
    "title": "Refund Abuse Intelligence",
    "description": "Conversational BI for refund abuse analytics.",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(serialized_space),
})

genie_space_id = result.get("space_id", "")
if not genie_space_id:
    print("  Space may already exist. Searching...")
    spaces = api_call("GET", "/api/2.0/genie/spaces")
    for s in spaces.get("spaces", []):
        if s.get("title") == "Refund Abuse Intelligence":
            genie_space_id = s["space_id"]
            api_call("PATCH", f"/api/2.0/genie/spaces/{genie_space_id}",
                     {"serialized_space": json.dumps(serialized_space)})
            print(f"  Updated existing space: {genie_space_id}")
            break

if genie_space_id:
    print(f"  ✅ Genie Space ID: {genie_space_id}")
    print(f"  URL: {HOST}/genie/rooms/{genie_space_id}")
else:
    print("  ❌ Could not create or find Genie Space")

print("\n═══ Phase 5 Complete ═══")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6: Databricks App Deployment
# MAGIC
# MAGIC Copies the app code from the Git Folder to a workspace path and deploys it.

# COMMAND ----------

print("═══ PHASE 6: App Deployment ═══\n")

# Step 1: Create the app
print("--- Creating Databricks App ---")
result = api_call("POST", "/api/2.0/apps", {
    "name": APP_NAME,
    "description": "Refund Console - Abuse Decisioning & Triage"
})
if "name" in result:
    print(f"  ✅ App created: {APP_NAME}")
elif "ALREADY_EXISTS" in str(result):
    print(f"  App already exists (OK)")
else:
    print(f"  Result: {result}")

# Step 2: Copy app files from Git Folder to workspace path
print("\n--- Copying app files to workspace ---")

# We need to use the Workspace API to copy files
# The app source is in the Git Folder at ./refund-console/
git_app_dir = f"{git_folder_root}/refund-console"

# Create workspace directories
for d in ["", "/server", "/server/routes", "/frontend/dist/assets"]:
    api_call("POST", "/api/2.0/workspace/mkdirs", {"path": f"{APP_WORKSPACE_PATH}{d}"})

# Import files from Git Folder to workspace path using %sh
print("  Copying files...")

# COMMAND ----------

# Use shell to copy files from Git Folder to workspace path via databricks CLI
# The Git Folder is mounted at /Workspace/... so we can access files directly

import base64

def upload_file_to_workspace(local_path, workspace_path):
    """Upload a file to Databricks workspace using the Import API."""
    try:
        with open(local_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")

        result = api_call("POST", "/api/2.0/workspace/import", {
            "path": workspace_path,
            "content": content,
            "format": "AUTO",
            "overwrite": True,
            "language": "PYTHON" if workspace_path.endswith(".py") else None,
        })
        return "error_code" not in result
    except Exception as e:
        print(f"    ⚠️ Failed to upload {workspace_path}: {e}")
        return False

# Map of source files to upload
# We need to find the actual filesystem path of the Git Folder
import subprocess

# Get the repo root on DBFS/local
repo_root_result = subprocess.run(
    ["find", "/Workspace", "-maxdepth", 5, "-name", "deploy_from_workspace.py", "-path", "*/refund-engine/*"],
    capture_output=True, text=True, timeout=10
)
repo_paths = repo_root_result.stdout.strip().split("\n")
repo_root = ""
for p in repo_paths:
    if p and "refund-engine" in p:
        repo_root = "/".join(p.split("/")[:-1])
        break

if not repo_root:
    # Fallback: use notebook path
    repo_root = f"/Workspace{git_folder_root}"

print(f"  Repo root: {repo_root}")
app_source = f"{repo_root}/refund-console"

# Backend files
backend_files = [
    "app.py", "app.yaml", "requirements.txt",
    "server/__init__.py", "server/config.py", "server/warehouse.py",
    "server/llm.py", "server/agent.py",
    "server/routes/__init__.py", "server/routes/dashboard.py",
    "server/routes/cases.py", "server/routes/actions.py",
    "server/routes/agent.py", "server/routes/feedback.py",
    "server/routes/genie.py",
]

uploaded = 0
for f in backend_files:
    src = f"{app_source}/{f}"
    dst = f"{APP_WORKSPACE_PATH}/{f}"
    if os.path.exists(src):
        if upload_file_to_workspace(src, dst):
            uploaded += 1
    else:
        print(f"    ⚠️ Not found: {src}")

print(f"  ✅ Uploaded {uploaded} backend files")

# Frontend dist files
frontend_dist = f"{app_source}/frontend/dist"
if os.path.exists(frontend_dist):
    frontend_count = 0
    for root, dirs, files in os.walk(frontend_dist):
        for fname in files:
            src = os.path.join(root, fname)
            rel = os.path.relpath(src, app_source)
            dst = f"{APP_WORKSPACE_PATH}/{rel}"
            # Ensure parent dir exists
            parent = "/".join(dst.split("/")[:-1])
            api_call("POST", "/api/2.0/workspace/mkdirs", {"path": parent})
            if upload_file_to_workspace(src, dst):
                frontend_count += 1
    print(f"  ✅ Uploaded {frontend_count} frontend files")
else:
    print("  ⚠️ Frontend dist not found. Build the frontend first:")
    print("    cd refund-console/frontend && npm install && npm run build")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update app.yaml with Genie Space ID and MLflow Experiment

# COMMAND ----------

# Update app.yaml with runtime config
print("--- Updating app.yaml ---")

import re as _re

app_yaml_path = f"{app_source}/app.yaml"
if os.path.exists(app_yaml_path):
    with open(app_yaml_path, "r") as f:
        app_yaml = f.read()

    experiment_path = f"/Users/{CURRENT_USER}/refund-engine/refund-agent"

    # Update MLFLOW_EXPERIMENT_NAME (regardless of current value)
    app_yaml = _re.sub(
        r'(name: MLFLOW_EXPERIMENT_NAME\n\s+value:) .*',
        rf'\1 "{experiment_path}"',
        app_yaml,
    )

    # Ensure DATABRICKS_WAREHOUSE_ID is present
    if "DATABRICKS_WAREHOUSE_ID" not in app_yaml:
        app_yaml = app_yaml.rstrip() + f'\n  - name: DATABRICKS_WAREHOUSE_ID\n    value: "{WAREHOUSE_ID}"\n'
    else:
        app_yaml = _re.sub(
            r'(name: DATABRICKS_WAREHOUSE_ID\n\s+value:) .*',
            rf'\1 "{WAREHOUSE_ID}"',
            app_yaml,
        )

    # Add GENIE_SPACE_ID if available
    if genie_space_id:
        if "GENIE_SPACE_ID" not in app_yaml:
            app_yaml = app_yaml.rstrip() + f'\n  - name: GENIE_SPACE_ID\n    value: "{genie_space_id}"\n'
        else:
            app_yaml = _re.sub(
                r'(name: GENIE_SPACE_ID\n\s+value:) .*',
                rf'\1 "{genie_space_id}"',
                app_yaml,
            )

    # Write updated yaml and upload
    updated_yaml_path = "/tmp/app.yaml"
    with open(updated_yaml_path, "w") as f:
        f.write(app_yaml)

    upload_file_to_workspace(updated_yaml_path, f"{APP_WORKSPACE_PATH}/app.yaml")
    print(f"  ✅ app.yaml updated with warehouse ID, MLflow experiment, and Genie Space ID")

# COMMAND ----------

# Deploy the app
print("--- Deploying app ---")
deploy_result = api_call("POST", f"/api/2.0/apps/{APP_NAME}/deployments", {
    "source_code_path": APP_WORKSPACE_PATH,
})
deployment_id = deploy_result.get("deployment_id", "")
if deployment_id:
    print(f"  ✅ Deployment triggered: {deployment_id}")
else:
    print(f"  Deployment result: {deploy_result}")

print("\n═══ Phase 6 Complete ═══")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7: Post-Deployment (Permissions & Resources)

# COMMAND ----------

print("═══ PHASE 7: Post-Deployment Setup ═══\n")

# Get app SP info
app_info = api_call("GET", f"/api/2.0/apps/{APP_NAME}")
sp_client_id = app_info.get("service_principal_client_id", "")
app_url = app_info.get("url", "")
print(f"  SP Client ID: {sp_client_id or 'UNKNOWN'}")
print(f"  App URL: {app_url or 'UNKNOWN'}")

if sp_client_id:
    # Add resources to app
    print("\n--- Adding resources ---")
    api_call("PUT", f"/api/2.0/apps/{APP_NAME}", {
        "name": APP_NAME,
        "resources": [
            {"name": "sql-warehouse", "sql_warehouse": {"id": WAREHOUSE_ID, "permission": "CAN_USE"}},
            {"name": "serving-endpoint", "serving_endpoint": {"name": SERVING_ENDPOINT, "permission": "CAN_QUERY"}},
        ]
    })
    print("  ✅ SQL Warehouse + Serving Endpoint resources added")

    # Grant SP catalog access
    print("\n--- Granting catalog access ---")
    grants = [
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp_client_id}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.refund_gold TO `{sp_client_id}`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.refund_gold TO `{sp_client_id}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.refund_serving TO `{sp_client_id}`",
        f"GRANT ALL PRIVILEGES ON SCHEMA {CATALOG}.refund_serving TO `{sp_client_id}`",
    ]
    for sql in grants:
        spark.sql(sql)
    print("  ✅ Catalog grants applied")

    # MLflow experiment
    print("\n--- Setting up MLflow experiment ---")
    experiment_path = f"/Users/{CURRENT_USER}/refund-engine/refund-agent"
    exp_result = api_call("POST", "/api/2.0/mlflow/experiments/create", {"name": experiment_path})
    experiment_id = exp_result.get("experiment_id", "")
    if not experiment_id:
        exp_result = api_call("POST", "/api/2.0/mlflow/experiments/get-by-name",
                              {"experiment_name": experiment_path})
        experiment_id = exp_result.get("experiment", {}).get("experiment_id", "")

    if experiment_id:
        api_call("PATCH", f"/api/2.0/permissions/experiments/{experiment_id}", {
            "access_control_list": [{"service_principal_name": sp_client_id, "permission_level": "CAN_MANAGE"}]
        })
        print(f"  ✅ Experiment: {experiment_path} (ID: {experiment_id})")

    # Genie Space permissions
    if genie_space_id:
        print("\n--- Genie Space permissions ---")
        api_call("PATCH", f"/api/2.0/permissions/genie/{genie_space_id}", {
            "access_control_list": [{"service_principal_name": sp_client_id, "permission_level": "CAN_MANAGE"}]
        })
        print(f"  ✅ SP granted access to Genie Space")

    # Redeploy app to pick up resource env vars
    print("\n--- Redeploying app ---")
    api_call("POST", f"/api/2.0/apps/{APP_NAME}/deployments", {
        "source_code_path": APP_WORKSPACE_PATH,
    })
    print("  ✅ App redeployed with resources")

else:
    print("  ⚠️ Could not get SP Client ID. Configure permissions manually.")

print("\n═══ Phase 7 Complete ═══")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Summary

# COMMAND ----------

print(f"""
╔══════════════════════════════════════════════════════════════════╗
║                    DEPLOYMENT COMPLETE                          ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  App URL:          {app_url or 'Check Databricks UI':<42s} ║
║  Catalog:          {CATALOG:<42s} ║
║  SP Client ID:     {sp_client_id or 'UNKNOWN':<42s} ║
║  Pipeline ID:      {pipeline_id or 'UNKNOWN':<42s} ║
║  Genie Space ID:   {genie_space_id or 'UNKNOWN':<42s} ║
║  MLflow Experiment: {experiment_path:<41s} ║
║                                                                  ║
║  Next Steps:                                                     ║
║  1. Open the App URL and verify the dashboard loads              ║
║  2. Navigate to Cases and review a high-risk case                ║
║  3. Click "Run AI Analysis" to test the 4-step agent             ║
║  4. Try the Genie tab with "Show top abuse customers"            ║
║  5. Submit test feedback on the Feedback tab                     ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
""")
