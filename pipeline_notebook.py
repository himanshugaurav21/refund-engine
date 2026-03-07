# Databricks notebook source
# MAGIC %md
# MAGIC # Refund Engine - Lakeflow Declarative Pipeline
# MAGIC Bronze -> Silver -> Gold medallion architecture for refund abuse decisioning.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Ingestion from Volume (9 tables)

# COMMAND ----------

VOLUME_BASE = "/Volumes/refund_decisioning/refund_bronze/refund_source_files"

# COMMAND ----------

@dlt.table(
    name="refund_bronze_requests",
    comment="Raw refund requests ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_refund_id", "refund_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0")
def refund_bronze_requests():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/refund_requests.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_orders",
    comment="Raw order history ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
def refund_bronze_orders():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/order_history.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_customers",
    comment="Raw customer profiles ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def refund_bronze_customers():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/customer_profiles.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_households",
    comment="Raw household mappings ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_household_id", "household_id IS NOT NULL")
def refund_bronze_households():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/household_mappings.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_stores",
    comment="Raw store locations ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
def refund_bronze_stores():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/store_locations.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_products",
    comment="Raw product catalog ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def refund_bronze_products():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/product_catalog.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_deliveries",
    comment="Raw delivery events ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_delivery_id", "delivery_id IS NOT NULL")
def refund_bronze_deliveries():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/delivery_events.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_policies",
    comment="Raw refund policies ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
def refund_bronze_policies():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/refund_policies.csv")
    )

# COMMAND ----------

@dlt.table(
    name="refund_bronze_decisions",
    comment="Raw historical decisions ingested from CSV",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_decision_id", "decision_id IS NOT NULL")
def refund_bronze_decisions():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_BASE}/historical_decisions.csv")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned & Enriched (3 tables)

# COMMAND ----------

@dlt.table(
    name="refund_silver_enriched_requests",
    comment="Refund requests enriched with order, customer, product, delivery, and policy data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_refund_id", "refund_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def refund_silver_enriched_requests():
    requests = dlt.read("refund_bronze_requests")
    orders = dlt.read("refund_bronze_orders")
    customers = dlt.read("refund_bronze_customers")
    products = dlt.read("refund_bronze_products")
    deliveries = dlt.read("refund_bronze_deliveries")
    policies = dlt.read("refund_bronze_policies")

    # Get latest delivery per order
    latest_delivery = (
        deliveries
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.desc("delivery_id"))
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            F.col("order_id").alias("del_order_id"),
            F.col("status").alias("delivery_status_actual"),
            F.col("delivery_issues"),
            F.col("photo_proof"),
        )
    )

    enriched = (
        requests
        .join(orders.select(
            "order_id",
            F.col("total_amount").alias("order_amount"),
            F.col("order_date"),
            F.col("delivery_status").alias("order_delivery_status"),
            "payment_method",
            "store_id",
            F.col("product_id").alias("order_product_id"),
        ), on="order_id", how="left")
        .join(customers.select(
            "customer_id",
            F.col("first_name"),
            F.col("last_name"),
            "tier",
            "lifetime_orders",
            "lifetime_refunds",
            "lifetime_spend",
        ), on="customer_id", how="left")
        .join(products.select(
            F.col("product_id"),
            F.col("name").alias("product_name"),
            F.col("category").alias("product_category"),
            F.col("price").alias("product_price"),
            "refund_rate_baseline",
            "is_high_value",
        ), F.col("order_product_id") == F.col("product_id"), how="left")
        .join(latest_delivery, F.col("order_id") == F.col("del_order_id"), how="left")
        .join(policies.select(
            F.col("category").alias("policy_category"),
            F.col("reason_code").alias("policy_reason_code"),
            "max_refund_window_days",
            "auto_approve_threshold",
            "requires_return",
        ), (F.col("product_category") == F.col("policy_category")) &
           (F.col("reason_code") == F.col("policy_reason_code")), how="left")
        .withColumn("days_since_order",
                    F.datediff(F.col("request_date"), F.col("order_date")))
        .withColumn("within_return_window",
                    F.col("days_since_order") <= F.coalesce(F.col("max_refund_window_days"), F.lit(30)))
        .withColumn("refund_to_order_ratio",
                    F.when(F.col("order_amount") > 0,
                           F.round(F.col("amount") / F.col("order_amount"), 3))
                    .otherwise(F.lit(1.0)))
        .withColumn("delivery_confirmed",
                    (F.col("delivery_status_actual") == "delivered") &
                    (F.col("photo_proof") == True))
        .drop("del_order_id", "order_product_id", "product_id",
               "policy_category", "policy_reason_code")
    )
    return enriched

# COMMAND ----------

@dlt.table(
    name="refund_silver_customer_refund_history",
    comment="Per-customer refund aggregations over 30d and 90d windows",
    table_properties={"quality": "silver"}
)
def refund_silver_customer_refund_history():
    requests = dlt.read("refund_bronze_requests")

    return (
        requests
        .withColumn("request_date", F.col("request_date").cast("date"))
        .groupBy("customer_id")
        .agg(
            F.count("refund_id").alias("total_refunds"),
            F.sum("amount").alias("total_refund_amount"),
            F.sum(F.when(
                F.col("request_date") >= F.date_sub(F.current_date(), 30), 1
            ).otherwise(0)).alias("refunds_30d"),
            F.sum(F.when(
                F.col("request_date") >= F.date_sub(F.current_date(), 90), 1
            ).otherwise(0)).alias("refunds_90d"),
            F.sum(F.when(
                F.col("request_date") >= F.date_sub(F.current_date(), 90),
                F.col("amount")
            ).otherwise(0)).alias("total_refund_amount_90d"),
            F.avg("amount").alias("avg_refund_amount"),
            F.max("request_date").alias("last_refund_date"),
        )
        .withColumn("refund_frequency",
                    F.when(F.col("refunds_90d") > 10, "very_high")
                    .when(F.col("refunds_90d") > 5, "high")
                    .when(F.col("refunds_90d") > 2, "medium")
                    .otherwise("low"))
    )

# COMMAND ----------

@dlt.table(
    name="refund_silver_household_activity",
    comment="Per-household refund activity with coordination detection",
    table_properties={"quality": "silver"}
)
def refund_silver_household_activity():
    requests = dlt.read("refund_bronze_requests")
    households = dlt.read("refund_bronze_households")

    # Join requests with household membership
    hh_requests = (
        requests
        .join(households.select("customer_id", "household_id", "shared_address_flag"),
              on="customer_id", how="inner")
    )

    # Household-level aggregations
    hh_agg = (
        hh_requests
        .withColumn("request_date", F.col("request_date").cast("date"))
        .groupBy("household_id")
        .agg(
            F.countDistinct("customer_id").alias("household_members_with_refunds"),
            F.count("refund_id").alias("household_total_refunds"),
            F.sum(F.when(
                F.col("request_date") >= F.date_sub(F.current_date(), 30), 1
            ).otherwise(0)).alias("household_refund_count_30d"),
            F.sum("amount").alias("household_total_refund_amount"),
            F.collect_set("reason_code").alias("household_reason_codes"),
        )
    )

    # Coordination detection: multiple members filing similar refunds within 48 hours
    w = Window.partitionBy("household_id").orderBy("request_date")
    coord = (
        hh_requests
        .withColumn("prev_date", F.lag("request_date").over(w))
        .withColumn("prev_customer", F.lag("customer_id").over(w))
        .withColumn("hours_between",
                    (F.unix_timestamp(F.col("request_date")) -
                     F.unix_timestamp(F.col("prev_date"))) / 3600)
        .withColumn("coord_flag",
                    (F.col("hours_between").isNotNull()) &
                    (F.col("hours_between") <= 48) &
                    (F.col("customer_id") != F.col("prev_customer")))
        .groupBy("household_id")
        .agg(
            F.sum(F.when(F.col("coord_flag"), 1).otherwise(0))
            .alias("coordinated_refund_count")
        )
        .withColumn("coordinated_timing_flag", F.col("coordinated_refund_count") > 0)
    )

    return (
        hh_agg
        .join(coord, on="household_id", how="left")
        .withColumn("household_reason_codes",
                    F.array_join(F.col("household_reason_codes"), ", "))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Tables (3 tables)

# COMMAND ----------

@dlt.table(
    name="refund_customer_360",
    comment="Customer abuse profile with refund rate, risk tier, and household signals",
    table_properties={"quality": "gold"}
)
def refund_customer_360():
    customers = dlt.read("refund_bronze_customers")
    refund_hist = dlt.read("refund_silver_customer_refund_history")
    households = dlt.read("refund_bronze_households")
    hh_activity = dlt.read("refund_silver_household_activity")

    # Get household info per customer
    cust_hh = (
        households
        .select("customer_id", "household_id", "shared_address_flag")
        .join(hh_activity, on="household_id", how="left")
    )

    result = (
        customers
        .join(refund_hist, on="customer_id", how="left")
        .join(cust_hh, on="customer_id", how="left")
        .withColumn("refund_rate",
                    F.when(F.col("lifetime_orders") > 0,
                           F.round(F.coalesce(F.col("total_refunds"), F.lit(0)) /
                                   F.col("lifetime_orders"), 3))
                    .otherwise(F.lit(0.0)))
        .withColumn("risk_tier",
                    F.when(
                        (F.col("refund_rate") > 0.4) |
                        (F.col("refunds_90d") > 10) |
                        (F.col("coordinated_timing_flag") == True),
                        "CRITICAL"
                    ).when(
                        (F.col("refund_rate") > 0.25) |
                        (F.col("refunds_90d") > 5),
                        "HIGH"
                    ).when(
                        (F.col("refund_rate") > 0.15) |
                        (F.col("refunds_90d") > 2),
                        "MEDIUM"
                    ).otherwise("LOW"))
        .select(
            "customer_id", "first_name", "last_name", "email", "tier",
            "lifetime_orders", "lifetime_refunds", "lifetime_spend",
            "account_created_date",
            "total_refunds", "total_refund_amount", "avg_refund_amount",
            "refunds_30d", "refunds_90d", "total_refund_amount_90d",
            "refund_frequency", "last_refund_date",
            "refund_rate", "risk_tier",
            "household_id", "shared_address_flag",
            "household_refund_count_30d", "coordinated_timing_flag",
            "household_total_refund_amount",
        )
    )
    return result

# COMMAND ----------

@dlt.table(
    name="refund_case_decisioning",
    comment="Per-request risk score and recommended action",
    table_properties={"quality": "gold"}
)
def refund_case_decisioning():
    enriched = dlt.read("refund_silver_enriched_requests")
    cust_hist = dlt.read("refund_silver_customer_refund_history")
    hh = dlt.read("refund_bronze_households")
    hh_activity = dlt.read("refund_silver_household_activity")

    # Get household signals per customer
    cust_hh = (
        hh.select("customer_id", "household_id")
        .join(hh_activity.select(
            "household_id",
            "household_refund_count_30d",
            "coordinated_timing_flag",
        ), on="household_id", how="left")
    )

    scored = (
        enriched
        .join(cust_hist.select(
            "customer_id",
            F.col("refunds_90d").alias("cust_refunds_90d"),
            F.col("total_refund_amount_90d").alias("cust_refund_amt_90d"),
            "refund_frequency",
        ), on="customer_id", how="left")
        .join(cust_hh, on="customer_id", how="left")
        # Component scores (0.0 to 1.0 each)
        .withColumn("refund_rate_score",
                    F.least(F.lit(1.0),
                            F.when(F.col("lifetime_orders") > 0,
                                   F.coalesce(F.col("lifetime_refunds"), F.lit(0)) /
                                   F.col("lifetime_orders") * 2.5)
                            .otherwise(F.lit(0.0))))
        .withColumn("frequency_score",
                    F.least(F.lit(1.0),
                            F.coalesce(F.col("cust_refunds_90d"), F.lit(0)) / 15.0))
        .withColumn("amount_score",
                    F.least(F.lit(1.0),
                            F.when(F.col("amount") > 500, 0.8)
                            .when(F.col("amount") > 200, 0.5)
                            .when(F.col("amount") > 100, 0.3)
                            .otherwise(0.1)))
        .withColumn("household_score",
                    F.when(F.col("coordinated_timing_flag") == True, 0.9)
                    .when(F.coalesce(F.col("household_refund_count_30d"), F.lit(0)) > 3, 0.6)
                    .otherwise(0.1))
        .withColumn("delivery_score",
                    F.when(
                        (F.col("reason_code") == "ITEM_NOT_RECEIVED") &
                        (F.col("delivery_confirmed") == True), 1.0
                    ).when(F.col("reason_code") == "ITEM_NOT_RECEIVED", 0.5)
                    .otherwise(0.1))
        .withColumn("policy_score",
                    F.when(F.col("within_return_window") == False, 0.8)
                    .when(F.col("days_since_order") > 25, 0.5)
                    .otherwise(0.1))
        # Composite weighted risk score
        .withColumn("risk_score", F.round(
            F.col("refund_rate_score") * 0.25 +
            F.col("frequency_score") * 0.20 +
            F.col("amount_score") * 0.15 +
            F.col("household_score") * 0.15 +
            F.col("delivery_score") * 0.15 +
            F.col("policy_score") * 0.10,
            3
        ))
        # Recommended action
        .withColumn("recommended_action",
                    F.when(F.col("risk_score") >= 0.7, "REJECT")
                    .when(F.col("risk_score") >= 0.4, "ESCALATE")
                    .otherwise("APPROVE"))
        # Recommendation reasons as JSON
        .withColumn("recommendation_reasons_json", F.to_json(F.struct(
            F.col("refund_rate_score"),
            F.col("frequency_score"),
            F.col("amount_score"),
            F.col("household_score"),
            F.col("delivery_score"),
            F.col("policy_score"),
            F.col("within_return_window"),
            F.col("delivery_confirmed"),
            F.col("days_since_order"),
        )))
        .select(
            "refund_id", "order_id", "customer_id",
            "first_name", "last_name", "tier",
            "reason_code", "amount", "channel", "request_date",
            "item_condition",
            "order_amount", "order_date", "payment_method",
            "product_name", "product_category", "product_price",
            "is_high_value",
            "days_since_order", "within_return_window",
            "delivery_confirmed", "delivery_issues",
            "refund_to_order_ratio",
            "lifetime_orders", "lifetime_refunds",
            "cust_refunds_90d", "cust_refund_amt_90d", "refund_frequency",
            "household_id", "coordinated_timing_flag",
            "risk_score", "recommended_action", "recommendation_reasons_json",
        )
    )
    return scored

# COMMAND ----------

@dlt.table(
    name="refund_segment_summary",
    comment="Aggregates by reason_code, channel, tier, risk for segment analysis",
    table_properties={"quality": "gold"}
)
def refund_segment_summary():
    cases = dlt.read("refund_case_decisioning")

    return (
        cases
        .groupBy("reason_code", "channel", "tier",
                 F.when(F.col("risk_score") >= 0.7, "CRITICAL")
                 .when(F.col("risk_score") >= 0.4, "HIGH")
                 .when(F.col("risk_score") >= 0.2, "MEDIUM")
                 .otherwise("LOW").alias("risk_tier"))
        .agg(
            F.count("refund_id").alias("total_cases"),
            F.sum(F.when(F.col("recommended_action") == "APPROVE", 1).otherwise(0)).alias("approved_count"),
            F.sum(F.when(F.col("recommended_action") == "ESCALATE", 1).otherwise(0)).alias("escalated_count"),
            F.sum(F.when(F.col("recommended_action") == "REJECT", 1).otherwise(0)).alias("rejected_count"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.sum("amount").alias("total_refund_amount"),
            F.avg("amount").alias("avg_refund_amount"),
        )
        .withColumn("approval_rate",
                    F.round(F.col("approved_count") / F.col("total_cases"), 3))
        .withColumn("escalation_rate",
                    F.round(F.col("escalated_count") / F.col("total_cases"), 3))
    )
