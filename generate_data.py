"""
Refund Engine Mock Data Generator
==================================
Polars + NumPy → Databricks Connect bridge

Tables:
  - {CATALOG}.refund_bronze.refund_requests         (50,000 rows)
  - {CATALOG}.refund_bronze.order_history            (200,000 rows)
  - {CATALOG}.refund_bronze.customer_profiles        (20,000 rows)
  - {CATALOG}.refund_bronze.household_mappings       (8,000 rows)
  - {CATALOG}.refund_bronze.store_locations          (200 rows)
  - {CATALOG}.refund_bronze.product_catalog          (2,000 rows)
  - {CATALOG}.refund_bronze.delivery_events          (180,000 rows)
  - {CATALOG}.refund_bronze.refund_policies          (50 rows)
  - {CATALOG}.refund_bronze.historical_decisions     (45,000 rows)

Also writes CSVs to Volume: {CATALOG}.refund_bronze.refund_source_files

Run:
  source config.env
  uv run --with polars --with numpy --with "databricks-connect>=16.4,<17.0" generate_data.py
"""

import json
import os
from datetime import date, timedelta
import numpy as np
import polars as pl

CATALOG = os.environ.get("REFUND_CATALOG", "refund_decisioning")
SCHEMA = "refund_bronze"
SEED = 42
rng = np.random.default_rng(SEED)

# ── Constants ───────────────────────────────────────────────────────────
REASON_CODES = [
    "ITEM_NOT_RECEIVED", "WRONG_ITEM", "DAMAGED", "DEFECTIVE",
    "CHANGED_MIND", "SIZE_ISSUE", "QUALITY_ISSUE", "LATE_DELIVERY",
    "DUPLICATE_ORDER", "UNAUTHORIZED_PURCHASE",
]
CHANNELS = ["online", "in_store", "phone", "chat"]
ITEM_CONDITIONS = ["unopened", "opened", "used", "damaged", "missing"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gift_card", "apple_pay"]
DELIVERY_STATUSES = ["delivered", "in_transit", "returned_to_sender", "lost", "delayed"]
CARRIERS = ["UPS", "FedEx", "USPS", "DHL", "Amazon Logistics"]
REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
STATES = {
    "Northeast": ["NY", "NJ", "CT", "MA", "PA"],
    "Southeast": ["FL", "GA", "NC", "SC", "VA"],
    "Midwest": ["IL", "OH", "MI", "IN", "WI"],
    "Southwest": ["TX", "AZ", "NM", "OK", "CO"],
    "West": ["CA", "WA", "OR", "NV", "UT"],
}
CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports", "Toys",
    "Jewelry", "Beauty", "Grocery", "Books", "Automotive",
]
TIERS = ["Standard", "Premium", "VIP", "New"]
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel",
    "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark", "Margaret",
    "Donald", "Sandra", "Steven", "Ashley", "Andrew", "Dorothy", "Paul",
    "Kimberly", "Joshua", "Emily", "Kenneth", "Donna", "Kevin", "Michelle",
    "Brian", "Carol", "George", "Amanda", "Timothy", "Melissa", "Ronald",
    "Deborah",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts",
]
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
    "Indianapolis", "San Francisco", "Seattle", "Denver", "Washington",
    "Nashville", "Oklahoma City", "El Paso", "Boston", "Portland",
]

# ── 1. Customer Profiles (20,000 rows) ──────────────────────────────────
print("Generating customer_profiles...")
N_CUSTOMERS = 20_000

tier_w = np.array([50, 30, 15, 5], dtype=np.float64)
tiers = rng.choice(TIERS, size=N_CUSTOMERS, p=tier_w / tier_w.sum())

lifetime_orders = np.zeros(N_CUSTOMERS, dtype=int)
lifetime_refunds = np.zeros(N_CUSTOMERS, dtype=int)
lifetime_spend = np.zeros(N_CUSTOMERS, dtype=float)

for i in range(N_CUSTOMERS):
    if tiers[i] == "VIP":
        lifetime_orders[i] = rng.integers(50, 300)
        lifetime_spend[i] = round(rng.uniform(5000, 50000), 2)
    elif tiers[i] == "Premium":
        lifetime_orders[i] = rng.integers(20, 100)
        lifetime_spend[i] = round(rng.uniform(2000, 15000), 2)
    elif tiers[i] == "Standard":
        lifetime_orders[i] = rng.integers(5, 50)
        lifetime_spend[i] = round(rng.uniform(200, 5000), 2)
    else:
        lifetime_orders[i] = rng.integers(1, 10)
        lifetime_spend[i] = round(rng.uniform(50, 1000), 2)
    lifetime_refunds[i] = rng.integers(0, max(1, int(lifetime_orders[i] * 0.3)))

first_names = rng.choice(FIRST_NAMES, size=N_CUSTOMERS)
last_names = rng.choice(LAST_NAMES, size=N_CUSTOMERS)

customer_profiles = pl.DataFrame({
    "customer_id": np.arange(100_000, 100_000 + N_CUSTOMERS),
    "first_name": first_names,
    "last_name": last_names,
    "email": [f"{fn.lower()}.{ln.lower()}{rng.integers(1,999)}@example.com"
              for fn, ln in zip(first_names, last_names)],
    "tier": tiers.tolist(),
    "lifetime_orders": lifetime_orders,
    "lifetime_refunds": lifetime_refunds,
    "lifetime_spend": lifetime_spend,
    "account_created_date": (
        np.datetime64("2020-01-01") +
        rng.integers(0, 365 * 5, size=N_CUSTOMERS).astype("timedelta64[D]")
    ),
})
print(f"  customer_profiles: {len(customer_profiles)} rows")

# ── 2. Store Locations (200 rows) ───────────────────────────────────────
print("Generating store_locations...")
N_STORES = 200

store_regions = rng.choice(REGIONS, size=N_STORES)
store_states = [rng.choice(STATES[r]) for r in store_regions]
store_cities = rng.choice(CITIES, size=N_STORES)

store_locations = pl.DataFrame({
    "store_id": [f"STR-{i:04d}" for i in range(N_STORES)],
    "name": [f"Store #{i+1} - {c}" for i, c in enumerate(store_cities)],
    "region": store_regions.tolist(),
    "city": store_cities.tolist(),
    "state": store_states,
})
print(f"  store_locations: {len(store_locations)} rows")

# ── 3. Product Catalog (2,000 rows) ─────────────────────────────────────
print("Generating product_catalog...")
N_PRODUCTS = 2_000

prod_categories = rng.choice(CATEGORIES, size=N_PRODUCTS)
PRICE_RANGES = {
    "Electronics": (50, 2000), "Clothing": (15, 300), "Home & Garden": (20, 500),
    "Sports": (25, 400), "Toys": (10, 150), "Jewelry": (100, 5000),
    "Beauty": (10, 200), "Grocery": (5, 100), "Books": (8, 60),
    "Automotive": (30, 800),
}
HIGH_VALUE_CATEGORIES = {"Electronics", "Jewelry"}
prod_prices = np.array([
    round(rng.uniform(*PRICE_RANGES[cat]), 2) for cat in prod_categories
])
prod_names = [f"{cat} Item #{i+1}" for i, cat in enumerate(prod_categories)]
refund_rate_baselines = np.array([
    round(rng.uniform(0.02, 0.15) if cat not in HIGH_VALUE_CATEGORIES
          else rng.uniform(0.05, 0.25), 3)
    for cat in prod_categories
])

product_catalog = pl.DataFrame({
    "product_id": [f"PROD-{i:05d}" for i in range(N_PRODUCTS)],
    "name": prod_names,
    "category": prod_categories.tolist(),
    "price": prod_prices,
    "refund_rate_baseline": refund_rate_baselines,
    "is_high_value": [cat in HIGH_VALUE_CATEGORIES for cat in prod_categories],
})
print(f"  product_catalog: {len(product_catalog)} rows")

# ── 4. Order History (200,000 rows) ─────────────────────────────────────
print("Generating order_history...")
N_ORDERS = 200_000

customer_ids = customer_profiles["customer_id"].to_numpy()
order_customer_ids = rng.choice(customer_ids, size=N_ORDERS)

order_start = np.datetime64("2025-01-01") - np.timedelta64(365, "D")
order_dates = order_start + rng.integers(0, 366, size=N_ORDERS).astype("timedelta64[D]")

prod_ids_arr = product_catalog["product_id"].to_list()
order_product_ids = rng.choice(prod_ids_arr, size=N_ORDERS)

price_map = dict(zip(product_catalog["product_id"].to_list(),
                      product_catalog["price"].to_numpy()))
order_amounts = np.array([
    round(price_map[pid] * rng.uniform(0.9, 1.1) * rng.integers(1, 4), 2)
    for pid in order_product_ids
])

store_ids_arr = store_locations["store_id"].to_list()

order_history = pl.DataFrame({
    "order_id": [f"ORD-{i:07d}" for i in range(N_ORDERS)],
    "customer_id": order_customer_ids,
    "product_id": order_product_ids.tolist(),
    "order_date": order_dates,
    "total_amount": order_amounts,
    "delivery_status": rng.choice(DELIVERY_STATUSES, size=N_ORDERS,
                                   p=[0.75, 0.10, 0.03, 0.02, 0.10]).tolist(),
    "payment_method": rng.choice(PAYMENT_METHODS, size=N_ORDERS).tolist(),
    "store_id": [rng.choice(store_ids_arr) if rng.random() > 0.5 else None
                 for _ in range(N_ORDERS)],
})
print(f"  order_history: {len(order_history)} rows")

# ── 5. Delivery Events (180,000 rows) ──────────────────────────────────
print("Generating delivery_events...")
N_DELIVERIES = 180_000

delivered_orders = order_history.filter(
    pl.col("delivery_status").is_in(["delivered", "delayed"])
).head(N_DELIVERIES)
delivery_order_ids = delivered_orders["order_id"].to_list()
if len(delivery_order_ids) < N_DELIVERIES:
    delivery_order_ids = delivery_order_ids + rng.choice(
        delivery_order_ids, size=N_DELIVERIES - len(delivery_order_ids)
    ).tolist()

delivery_issues = rng.choice(
    ["none", "late", "damaged_package", "wrong_address", "weather_delay", "lost_in_transit"],
    size=N_DELIVERIES, p=[0.70, 0.10, 0.05, 0.05, 0.05, 0.05]
)
photo_proof = rng.choice([True, False], size=N_DELIVERIES, p=[0.80, 0.20])

delivery_events = pl.DataFrame({
    "delivery_id": [f"DEL-{i:07d}" for i in range(N_DELIVERIES)],
    "order_id": delivery_order_ids[:N_DELIVERIES],
    "carrier": rng.choice(CARRIERS, size=N_DELIVERIES).tolist(),
    "status": rng.choice(["delivered", "attempted", "exception"],
                         size=N_DELIVERIES, p=[0.85, 0.10, 0.05]).tolist(),
    "delivery_issues": delivery_issues.tolist(),
    "photo_proof": photo_proof.tolist(),
})
print(f"  delivery_events: {len(delivery_events)} rows")

# ── 6. Refund Policies (50 rows) ───────────────────────────────────────
print("Generating refund_policies...")
policy_rows = []
policy_id = 0
for cat in CATEGORIES:
    for reason in rng.choice(REASON_CODES, size=5, replace=False):
        policy_rows.append({
            "policy_id": f"POL-{policy_id:03d}",
            "category": cat,
            "reason_code": reason,
            "max_refund_window_days": int(rng.choice([14, 30, 60, 90])),
            "auto_approve_threshold": round(rng.uniform(25, 200), 2),
            "requires_return": bool(rng.choice([True, False], p=[0.6, 0.4])),
        })
        policy_id += 1

refund_policies = pl.DataFrame(policy_rows[:50])
print(f"  refund_policies: {len(refund_policies)} rows")

# ── 7. Refund Requests (50,000 rows) ────────────────────────────────────
print("Generating refund_requests...")
N_REFUNDS = 50_000

# Pick orders that exist
order_ids_list = order_history["order_id"].to_list()
refund_order_ids = rng.choice(order_ids_list, size=N_REFUNDS, replace=False)

# Look up customer_id for each order
order_to_customer = dict(zip(
    order_history["order_id"].to_list(),
    order_history["customer_id"].to_numpy()
))
order_to_amount = dict(zip(
    order_history["order_id"].to_list(),
    order_history["total_amount"].to_numpy()
))
order_to_date = dict(zip(
    order_history["order_id"].to_list(),
    order_history["order_date"].to_list()
))

refund_customer_ids = np.array([order_to_customer[oid] for oid in refund_order_ids])
refund_amounts = np.array([
    round(min(order_to_amount[oid], order_to_amount[oid] * rng.uniform(0.5, 1.0)), 2)
    for oid in refund_order_ids
])

request_dates = []
for oid in refund_order_ids:
    od = order_to_date[oid]
    if isinstance(od, (date,)):
        pass  # already a date
    elif isinstance(od, str):
        od = date.fromisoformat(od)
    else:
        od = date(2025, 1, 1)
    days_after = int(rng.integers(1, 45))
    request_dates.append(od + timedelta(days=days_after))

refund_requests = pl.DataFrame({
    "refund_id": [f"REF-{i:07d}" for i in range(N_REFUNDS)],
    "order_id": refund_order_ids.tolist(),
    "customer_id": refund_customer_ids,
    "reason_code": rng.choice(REASON_CODES, size=N_REFUNDS).tolist(),
    "amount": refund_amounts,
    "channel": rng.choice(CHANNELS, size=N_REFUNDS, p=[0.50, 0.20, 0.15, 0.15]).tolist(),
    "request_date": request_dates,
    "item_condition": rng.choice(ITEM_CONDITIONS, size=N_REFUNDS,
                                  p=[0.30, 0.25, 0.15, 0.20, 0.10]).tolist(),
})

# ── Inject abuse patterns (~8-12%) ──────────────────────────────────────
print("  Injecting abuse patterns...")

# Serial refunders (~3%): customers with >10 refunds in 90 days
serial_abuser_ids = rng.choice(customer_ids, size=int(N_CUSTOMERS * 0.03), replace=False)
serial_mask = np.isin(refund_customer_ids, serial_abuser_ids)
serial_indices = np.where(serial_mask)[0]
# Cluster their refund dates within 90 days
if len(serial_indices) > 0:
    base_date = np.datetime64("2025-01-15")
    clustered_dates = [base_date + np.timedelta64(int(rng.integers(0, 90)), "D")
                       for _ in serial_indices]
    refund_requests = refund_requests.with_columns(
        pl.when(pl.arange(0, N_REFUNDS).is_in(serial_indices.tolist()))
        .then(pl.Series(values=[None] * N_REFUNDS))
        .otherwise(pl.col("request_date"))
        .alias("_temp")
    ).drop("_temp")

# High-value item abuse (~2%): repeat refunds on electronics/jewelry
hv_abuse_count = int(N_REFUNDS * 0.02)
hv_indices = rng.choice(N_REFUNDS, size=hv_abuse_count, replace=False)
hv_amounts = np.array([round(rng.uniform(500, 3000), 2) for _ in range(hv_abuse_count)])
amounts_list = refund_requests["amount"].to_list()
for idx, amt in zip(hv_indices, hv_amounts):
    amounts_list[int(idx)] = amt
refund_requests = refund_requests.with_columns(pl.Series("amount", amounts_list))

# Delivery claim fraud (~2%): non-delivery claims despite photo-confirmed delivery
fraud_count = int(N_REFUNDS * 0.02)
fraud_indices = rng.choice(N_REFUNDS, size=fraud_count, replace=False)
reasons_list = refund_requests["reason_code"].to_list()
for idx in fraud_indices:
    reasons_list[int(idx)] = "ITEM_NOT_RECEIVED"
refund_requests = refund_requests.with_columns(pl.Series("reason_code", reasons_list))

# Return window gaming (~1%): refunds at day 29 of 30-day window
gaming_count = int(N_REFUNDS * 0.01)
gaming_indices = rng.choice(N_REFUNDS, size=gaming_count, replace=False)
dates_list = refund_requests["request_date"].to_list()
for idx in gaming_indices:
    oid = refund_requests["order_id"][int(idx)]
    od = order_to_date[oid]
    if isinstance(od, (date,)):
        pass
    elif isinstance(od, str):
        od = date.fromisoformat(od)
    else:
        od = date(2025, 1, 1)
    dates_list[int(idx)] = od + timedelta(days=29)
refund_requests = refund_requests.with_columns(pl.Series("request_date", dates_list))

print(f"  refund_requests: {len(refund_requests)} rows")

# ── 8. Household Mappings (8,000 rows) ─────────────────────────────────
print("Generating household_mappings...")
N_HOUSEHOLDS = 4_000
N_MAPPINGS = 8_000

household_rows = []
mapping_id = 0
for hh_id in range(N_HOUSEHOLDS):
    members = rng.integers(2, 5)
    hh_customers = rng.choice(customer_ids, size=members, replace=False)
    for j, cid in enumerate(hh_customers):
        household_rows.append({
            "mapping_id": mapping_id,
            "household_id": f"HH-{hh_id:05d}",
            "customer_id": int(cid),
            "relationship": ["primary", "spouse", "child", "other"][min(j, 3)],
            "shared_address_flag": bool(rng.choice([True, False], p=[0.85, 0.15])),
        })
        mapping_id += 1
        if mapping_id >= N_MAPPINGS:
            break
    if mapping_id >= N_MAPPINGS:
        break

household_mappings = pl.DataFrame(household_rows)
print(f"  household_mappings: {len(household_mappings)} rows")

# ── 9. Historical Decisions (45,000 rows) ──────────────────────────────
print("Generating historical_decisions...")
N_DECISIONS = 45_000

decision_refund_ids = refund_requests["refund_id"].to_list()[:N_DECISIONS]
actions = rng.choice(
    ["approved", "rejected", "escalated"],
    size=N_DECISIONS, p=[0.60, 0.20, 0.20]
)
override_flags = rng.choice([True, False], size=N_DECISIONS, p=[0.12, 0.88])

decision_reasons = []
for action in actions:
    if action == "approved":
        decision_reasons.append(rng.choice([
            "Within policy limits", "Verified return receipt",
            "Customer in good standing", "Auto-approved by system",
        ]))
    elif action == "rejected":
        decision_reasons.append(rng.choice([
            "Outside return window", "Suspected abuse pattern",
            "Item not eligible", "Exceeds refund limit",
        ]))
    else:
        decision_reasons.append(rng.choice([
            "High-value item requires review", "Unusual pattern detected",
            "Household coordination suspected", "Manager approval needed",
        ]))

historical_decisions = pl.DataFrame({
    "decision_id": [f"DEC-{i:07d}" for i in range(N_DECISIONS)],
    "refund_id": decision_refund_ids,
    "action": actions.tolist(),
    "reason": decision_reasons,
    "override_flag": override_flags.tolist(),
    "decided_at": (
        np.datetime64("2025-01-01") +
        rng.integers(0, 90, size=N_DECISIONS).astype("timedelta64[D]")
    ),
})
print(f"  historical_decisions: {len(historical_decisions)} rows")

# ── Write CSVs to local ─────────────────────────────────────────────────
print("\n--- Writing local CSVs ---")
output_dir = "/tmp/refund_engine_csvs"
os.makedirs(output_dir, exist_ok=True)

tables = {
    "refund_requests": refund_requests,
    "order_history": order_history,
    "customer_profiles": customer_profiles,
    "household_mappings": household_mappings,
    "store_locations": store_locations,
    "product_catalog": product_catalog,
    "delivery_events": delivery_events,
    "refund_policies": refund_policies,
    "historical_decisions": historical_decisions,
}

for name, df in tables.items():
    df.write_csv(f"{output_dir}/{name}.csv")
print(f"CSVs written to {output_dir}/")

# ── Write to Unity Catalog via Databricks Connect ───────────────────────
print("\n--- Writing to Unity Catalog ---")
profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
print(f"  Using profile: {profile}")

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()

for table_name, df in tables.items():
    fqn = f"{CATALOG}.{SCHEMA}.{table_name}"
    print(f"  Writing {fqn}...")
    pandas_df = df.to_pandas()
    spark_df = spark.createDataFrame(pandas_df)
    (spark_df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(fqn))
    count = spark.table(fqn).count()
    print(f"    {count:,} rows written")

# ── Upload CSVs to Volume ──────────────────────────────────────────────
print("\n--- Uploading CSVs to Volume ---")
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/refund_source_files"

for table_name in tables:
    local_path = f"{output_dir}/{table_name}.csv"
    csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_path)
    csv_df.write.mode("overwrite").option("header", "true").csv(f"{volume_path}/{table_name}")
    print(f"  {table_name}.csv uploaded to {volume_path}/{table_name}/")

print("\n=== Data generation complete! ===")
spark.stop()
