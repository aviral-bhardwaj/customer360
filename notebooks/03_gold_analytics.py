# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Pipeline - Analytics Tables
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook implements the **Gold Layer** of the Medallion Architecture.
# MAGIC
# MAGIC **Purpose**: Create business-level aggregated tables optimized for analytics and reporting.
# MAGIC
# MAGIC **Characteristics**:
# MAGIC - Customer-centric aggregations
# MAGIC - Pre-computed KPIs and metrics
# MAGIC - Denormalized for query performance
# MAGIC - Ready for dashboards and ML
# MAGIC
# MAGIC ## Input
# MAGIC - Silver Delta tables (from `02_silver_transformation`)
# MAGIC
# MAGIC ## Output
# MAGIC - Gold Delta tables in `gold` schema (e.g., `customer360_demo.gold.customer_360`):
# MAGIC   - `gold.customer_360` - Unified customer profile
# MAGIC   - `gold.customer_segments` - RFM segmentation
# MAGIC   - `gold.customer_clv` - Customer lifetime value
# MAGIC   - `gold.campaign_performance` - Campaign analytics
# MAGIC   - `gold.daily_metrics` - Daily KPIs
# MAGIC
# MAGIC ---
# MAGIC **Schedule**: Runs after Silver transformation completes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
from typing import Dict, Tuple
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def save_gold_table(df: DataFrame, table_name: str) -> Tuple[str, int]:
    """
    Save DataFrame as a Gold Delta table.

    Args:
        df: Aggregated DataFrame
        table_name: Logical table name (without prefix)

    Returns:
        Tuple of (full_table_name, row_count)
    """
    full_table_name = get_full_table_name(table_name, layer="gold")

    df.write \
        .format("delta") \
        .mode(WRITE_MODE) \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)

    row_count = df.count()
    return full_table_name, row_count


def read_silver_table(table_name: str) -> DataFrame:
    """Read a Silver table by name."""
    return spark.table(get_full_table_name(table_name, layer="silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Table 1: Customer 360 Profile
# MAGIC
# MAGIC **Business Purpose**: Unified view of each customer aggregating all touchpoints.
# MAGIC
# MAGIC **Grain**: One row per `customer_id`

# COMMAND ----------

def build_gold_customer_360() -> DataFrame:
    """
    Build the unified Customer 360 profile table.

    Aggregates data from:
    - Customers (demographics)
    - Transactions (purchase behavior)
    - Loyalty (program engagement)
    - Service Interactions (support history)

    Returns:
        Customer 360 DataFrame
    """
    print("🏗️  Building: gold_customer_360")

    # Load Silver tables
    try:
        customers = read_silver_table("customers")
        has_customers = True
    except:
        print("   ⚠️ silver_customers not found, using customer_insights")
        has_customers = False

    transactions = read_silver_table("transactions")
    loyalty = read_silver_table("loyalty")
    service = read_silver_table("service_interactions")

    # If no customers table, derive from customer_insights
    if not has_customers:
        insights = read_silver_table("customer_insights")
        customers = insights \
            .select("customer_id", "customer_name", "gender", "age", "age_group",
                    "credit_score", "credit_tier", "monthly_income", "income_bracket",
                    "country", "state", "city") \
            .dropDuplicates(["customer_id"])

    # =========================================================================
    # Aggregate Transaction Metrics
    # =========================================================================
    txn_metrics = transactions \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_spend"),
            F.avg("amount").alias("avg_transaction_value"),
            F.max("amount").alias("max_transaction_value"),
            F.min("amount").alias("min_transaction_value"),
            F.max("transaction_date").alias("last_transaction_date"),
            F.min("transaction_date").alias("first_transaction_date"),
            F.countDistinct("transaction_month").alias("active_months"),
            F.countDistinct("channel").alias("channels_used"),
            F.sum(F.when(F.col("channel") == "online", 1).otherwise(0)).alias("online_transactions"),
            F.sum(F.when(F.col("channel") == "instore", 1).otherwise(0)).alias("instore_transactions"),
            F.sum(F.when(F.col("channel") == "online", F.col("amount")).otherwise(0)).alias("online_spend"),
            F.sum(F.when(F.col("channel") == "instore", F.col("amount")).otherwise(0)).alias("instore_spend"),
            F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_transactions")
        ) \
        .withColumn("days_since_last_transaction",
            F.datediff(F.current_date(), F.col("last_transaction_date"))) \
        .withColumn("customer_tenure_days",
            F.datediff(F.current_date(), F.col("first_transaction_date"))) \
        .withColumn("online_spend_pct",
            F.when(F.col("total_spend") > 0,
                   F.round(F.col("online_spend") / F.col("total_spend") * 100, 2))
            .otherwise(0))

    # =========================================================================
    # Aggregate Loyalty Metrics
    # =========================================================================
    loyalty_metrics = loyalty \
        .groupBy("customer_id") \
        .agg(
            F.max("total_points").alias("loyalty_points"),
            F.max("tier_level").alias("loyalty_tier"),
            F.max("tier_rank").alias("loyalty_tier_rank"),
            F.max("tenure_days").alias("loyalty_tenure_days"),
            F.count("*").alias("loyalty_accounts")
        )

    # =========================================================================
    # Aggregate Service Interaction Metrics
    # =========================================================================
    service_metrics = service \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("total_support_tickets"),
            F.sum(F.when(F.col("is_resolved"), 1).otherwise(0)).alias("resolved_tickets"),
            F.sum(F.when(F.col("is_escalated"), 1).otherwise(0)).alias("escalated_tickets"),
            F.sum(F.when(F.col("is_pending"), 1).otherwise(0)).alias("pending_tickets"),
            F.max("interaction_date").alias("last_support_date"),
            F.countDistinct("issue_type").alias("unique_issue_types")
        ) \
        .withColumn("support_resolution_rate",
            F.when(F.col("total_support_tickets") > 0,
                   F.round(F.col("resolved_tickets") / F.col("total_support_tickets") * 100, 2))
            .otherwise(None)) \
        .withColumn("days_since_last_support",
            F.datediff(F.current_date(), F.col("last_support_date")))

    # =========================================================================
    # Join All Metrics
    # =========================================================================
    customer_360 = customers \
        .join(txn_metrics, "customer_id", "left") \
        .join(loyalty_metrics, "customer_id", "left") \
        .join(service_metrics, "customer_id", "left")

    # =========================================================================
    # Add Derived Fields
    # =========================================================================
    customer_360 = customer_360 \
        .withColumn("is_active",
            F.when(F.col("days_since_last_transaction") <= 90, True).otherwise(False)) \
        .withColumn("preferred_channel",
            F.when(F.col("online_transactions") > F.col("instore_transactions"), "Online")
             .when(F.col("instore_transactions") > F.col("online_transactions"), "In-Store")
             .when((F.col("online_transactions") > 0) & (F.col("instore_transactions") > 0), "Omnichannel")
             .otherwise("Unknown")) \
        .withColumn("engagement_level",
            F.when(F.col("total_transactions") >= 10, "High")
             .when(F.col("total_transactions") >= 5, "Medium")
             .when(F.col("total_transactions") >= 1, "Low")
             .otherwise("None")) \
        .withColumn("has_support_issues",
            F.col("total_support_tickets") > 0) \
        .withColumn("_gold_timestamp", F.current_timestamp())

    # Fill nulls for numeric metrics
    customer_360 = customer_360.fillna({
        "total_transactions": 0,
        "total_spend": 0.0,
        "avg_transaction_value": 0.0,
        "loyalty_points": 0,
        "total_support_tickets": 0,
        "online_transactions": 0,
        "instore_transactions": 0
    })

    return customer_360

# COMMAND ----------

# Setup gold schema (creates if not exists)
setup_layer_schema("gold")

# COMMAND ----------

# Build and save Customer 360
customer_360_df = build_gold_customer_360()
c360_table, c360_rows = save_gold_table(customer_360_df, "customer_360")
print(f"   ✅ Saved: {c360_table} ({c360_rows:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Table 2: Customer Segments (RFM Analysis)
# MAGIC
# MAGIC **Business Purpose**: Segment customers based on Recency, Frequency, Monetary value.
# MAGIC
# MAGIC **Grain**: One row per `customer_id`

# COMMAND ----------

def build_gold_customer_segments(customer_360: DataFrame) -> DataFrame:
    """
    Build RFM-based customer segmentation table.

    RFM Scoring (1-5 scale, 5 is best):
    - Recency: Days since last purchase (lower = better)
    - Frequency: Number of transactions (higher = better)
    - Monetary: Total spend (higher = better)

    Returns:
        Customer segments DataFrame
    """
    print("🏗️  Building: gold_customer_segments")

    # Calculate RFM base metrics
    rfm_df = customer_360 \
        .withColumn("recency_days",
            F.coalesce(F.col("days_since_last_transaction"), F.lit(999))) \
        .withColumn("frequency",
            F.coalesce(F.col("total_transactions"), F.lit(0))) \
        .withColumn("monetary",
            F.coalesce(F.col("total_spend"), F.lit(0.0)))

    # =========================================================================
    # Calculate RFM Scores (1-5)
    # =========================================================================
    # Recency: Lower days = higher score
    rfm_df = rfm_df \
        .withColumn("r_score",
            F.when(F.col("recency_days") <= 30, 5)
             .when(F.col("recency_days") <= 60, 4)
             .when(F.col("recency_days") <= 90, 3)
             .when(F.col("recency_days") <= 180, 2)
             .otherwise(1))

    # Frequency: Higher count = higher score
    rfm_df = rfm_df \
        .withColumn("f_score",
            F.when(F.col("frequency") >= 10, 5)
             .when(F.col("frequency") >= 5, 4)
             .when(F.col("frequency") >= 3, 3)
             .when(F.col("frequency") >= 1, 2)
             .otherwise(1))

    # Monetary: Higher spend = higher score
    rfm_df = rfm_df \
        .withColumn("m_score",
            F.when(F.col("monetary") >= 1000, 5)
             .when(F.col("monetary") >= 500, 4)
             .when(F.col("monetary") >= 200, 3)
             .when(F.col("monetary") >= 50, 2)
             .otherwise(1))

    # =========================================================================
    # Composite Scores
    # =========================================================================
    rfm_df = rfm_df \
        .withColumn("rfm_score",
            F.col("r_score") + F.col("f_score") + F.col("m_score")) \
        .withColumn("rfm_string",
            F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score")))

    # =========================================================================
    # Assign Customer Segments
    # =========================================================================
    segments_df = rfm_df \
        .withColumn("customer_segment",
            F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
                   "Champions")
             .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 4),
                   "Loyal Customers")
             .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2),
                   "New Customers")
             .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3),
                   "Potential Loyalists")
             .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3),
                   "At Risk")
             .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
                   "Can't Lose Them")
             .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2),
                   "Hibernating")
             .when((F.col("r_score") <= 2) & (F.col("m_score") <= 2),
                   "Lost")
             .otherwise("Needs Attention")
        )

    # =========================================================================
    # Assign Churn Risk
    # =========================================================================
    segments_df = segments_df \
        .withColumn("churn_risk",
            F.when(F.col("customer_segment").isin(["Hibernating", "Lost"]), "Critical")
             .when(F.col("customer_segment").isin(["At Risk", "Can't Lose Them"]), "High")
             .when(F.col("customer_segment") == "Needs Attention", "Medium")
             .otherwise("Low")) \
        .withColumn("churn_risk_score",
            F.when(F.col("churn_risk") == "Critical", 4)
             .when(F.col("churn_risk") == "High", 3)
             .when(F.col("churn_risk") == "Medium", 2)
             .otherwise(1)) \
        .withColumn("_gold_timestamp", F.current_timestamp())

    # Select final columns
    output_df = segments_df.select(
        "customer_id",
        "recency_days",
        "frequency",
        "monetary",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "rfm_string",
        "customer_segment",
        "churn_risk",
        "churn_risk_score",
        "_gold_timestamp"
    )

    return output_df

# COMMAND ----------

# Build and save Customer Segments
segments_df = build_gold_customer_segments(customer_360_df)
seg_table, seg_rows = save_gold_table(segments_df, "customer_segments")
print(f"   ✅ Saved: {seg_table} ({seg_rows:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Table 3: Customer Lifetime Value (CLV)
# MAGIC
# MAGIC **Business Purpose**: Estimate historical and predicted customer lifetime value.
# MAGIC
# MAGIC **Grain**: One row per `customer_id`

# COMMAND ----------

def build_gold_customer_clv(customer_360: DataFrame) -> DataFrame:
    """
    Build Customer Lifetime Value estimation table.

    CLV Calculation:
    - Historical CLV: Total spend to date
    - Predicted CLV: Based on average value × predicted frequency × time horizon

    Returns:
        Customer CLV DataFrame
    """
    print("🏗️  Building: gold_customer_clv")

    clv_df = customer_360 \
        .withColumn("historical_clv",
            F.coalesce(F.col("total_spend"), F.lit(0.0))) \
        .withColumn("avg_purchase_value",
            F.coalesce(F.col("avg_transaction_value"), F.lit(0.0))) \
        .withColumn("purchase_frequency_annual",
            F.when((F.col("customer_tenure_days").isNotNull()) & (F.col("customer_tenure_days") > 0),
                   (F.col("total_transactions") / F.col("customer_tenure_days")) * 365)
            .otherwise(0)) \
        .withColumn("predicted_annual_value",
            F.col("avg_purchase_value") * F.col("purchase_frequency_annual")) \
        .withColumn("predicted_clv_1yr",
            F.col("predicted_annual_value")) \
        .withColumn("predicted_clv_3yr",
            F.col("predicted_annual_value") * 3) \
        .withColumn("predicted_clv_5yr",
            F.col("predicted_annual_value") * 5) \
        .withColumn("clv_tier",
            F.when(F.col("historical_clv") >= 1000, "Platinum")
             .when(F.col("historical_clv") >= 500, "Gold")
             .when(F.col("historical_clv") >= 200, "Silver")
             .otherwise("Bronze")) \
        .withColumn("clv_percentile",
            F.percent_rank().over(Window.orderBy("historical_clv"))) \
        .withColumn("is_high_value",
            F.col("clv_percentile") >= 0.8) \
        .withColumn("_gold_timestamp", F.current_timestamp())

    output_df = clv_df.select(
        "customer_id",
        "historical_clv",
        "avg_purchase_value",
        "purchase_frequency_annual",
        "predicted_annual_value",
        "predicted_clv_1yr",
        "predicted_clv_3yr",
        "predicted_clv_5yr",
        "clv_tier",
        "clv_percentile",
        "is_high_value",
        "_gold_timestamp"
    )

    return output_df

# COMMAND ----------

# Build and save Customer CLV
clv_df = build_gold_customer_clv(customer_360_df)
clv_table, clv_rows = save_gold_table(clv_df, "customer_clv")
print(f"   ✅ Saved: {clv_table} ({clv_rows:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Table 4: Campaign Performance
# MAGIC
# MAGIC **Business Purpose**: Analyze marketing campaign effectiveness.
# MAGIC
# MAGIC **Grain**: One row per `campaign_source`

# COMMAND ----------

def build_gold_campaign_performance() -> DataFrame:
    """
    Build campaign performance analytics table.

    Metrics calculated:
    - Conversion rate
    - Revenue per campaign
    - Return rate
    - Customer acquisition

    Returns:
        Campaign performance DataFrame
    """
    print("🏗️  Building: gold_campaign_performance")

    try:
        insights = read_silver_table("customer_insights")
    except Exception as e:
        print(f"   ⚠️ silver_customer_insights not available: {e}")
        return None

    campaign_metrics = insights \
        .groupBy("campaign_source") \
        .agg(
            F.count("*").alias("total_sessions"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("order_confirmed") == True, 1).otherwise(0)).alias("conversions"),
            F.sum(F.when(F.col("order_confirmed") == True, F.col("order_value")).otherwise(0)).alias("total_revenue"),
            F.avg(F.when(F.col("order_confirmed") == True, F.col("order_value")).otherwise(None)).alias("avg_order_value"),
            F.sum(F.when(F.col("order_confirmed") == True, F.col("total_profit")).otherwise(0)).alias("total_profit"),
            F.sum(F.when(F.col("order_returned") == True, 1).otherwise(0)).alias("returns"),
            F.avg("age").alias("avg_customer_age"),
            F.avg("monthly_income").alias("avg_customer_income")
        ) \
        .withColumn("conversion_rate",
            F.round(F.col("conversions") / F.col("total_sessions") * 100, 2)) \
        .withColumn("return_rate",
            F.when(F.col("conversions") > 0,
                   F.round(F.col("returns") / F.col("conversions") * 100, 2))
            .otherwise(0)) \
        .withColumn("revenue_per_session",
            F.round(F.col("total_revenue") / F.col("total_sessions"), 2)) \
        .withColumn("profit_margin_pct",
            F.when(F.col("total_revenue") > 0,
                   F.round(F.col("total_profit") / F.col("total_revenue") * 100, 2))
            .otherwise(0)) \
        .withColumn("campaign_rank",
            F.dense_rank().over(Window.orderBy(F.desc("total_revenue")))) \
        .withColumn("_gold_timestamp", F.current_timestamp()) \
        .orderBy(F.desc("total_revenue"))

    return campaign_metrics

# COMMAND ----------

# Build and save Campaign Performance
campaign_df = build_gold_campaign_performance()
if campaign_df is not None:
    camp_table, camp_rows = save_gold_table(campaign_df, "campaign_performance")
    print(f"   ✅ Saved: {camp_table} ({camp_rows:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Table 5: Daily Metrics
# MAGIC
# MAGIC **Business Purpose**: Daily aggregated KPIs for trend analysis.
# MAGIC
# MAGIC **Grain**: One row per `transaction_date`

# COMMAND ----------

def build_gold_daily_metrics() -> DataFrame:
    """
    Build daily aggregated metrics table for trend analysis.

    Metrics:
    - Transaction volume
    - Revenue
    - Customer counts
    - Channel breakdown

    Returns:
        Daily metrics DataFrame
    """
    print("🏗️  Building: gold_daily_metrics")

    transactions = read_silver_table("transactions")

    daily_metrics = transactions \
        .groupBy("transaction_date") \
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_transaction_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("channel") == "online", 1).otherwise(0)).alias("online_transactions"),
            F.sum(F.when(F.col("channel") == "instore", 1).otherwise(0)).alias("instore_transactions"),
            F.sum(F.when(F.col("channel") == "online", F.col("amount")).otherwise(0)).alias("online_revenue"),
            F.sum(F.when(F.col("channel") == "instore", F.col("amount")).otherwise(0)).alias("instore_revenue"),
            F.sum(F.when(F.col("status") == "Completed", 1).otherwise(0)).alias("completed_orders"),
            F.sum(F.when(F.col("status") == "Failed", 1).otherwise(0)).alias("failed_orders")
        ) \
        .withColumn("online_revenue_pct",
            F.when(F.col("total_revenue") > 0,
                   F.round(F.col("online_revenue") / F.col("total_revenue") * 100, 2))
            .otherwise(0)) \
        .withColumn("completion_rate",
            F.when(F.col("total_transactions") > 0,
                   F.round(F.col("completed_orders") / F.col("total_transactions") * 100, 2))
            .otherwise(0)) \
        .withColumn("day_of_week", F.dayofweek("transaction_date")) \
        .withColumn("is_weekend",
            F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False)) \
        .withColumn("_gold_timestamp", F.current_timestamp()) \
        .orderBy("transaction_date")

    return daily_metrics

# COMMAND ----------

# Build and save Daily Metrics
daily_df = build_gold_daily_metrics()
daily_table, daily_rows = save_gold_table(daily_df, "daily_metrics")
print(f"   ✅ Saved: {daily_table} ({daily_rows:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Pipeline Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("🥇 GOLD LAYER COMPLETE")
print("=" * 80)
print(f"Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

print("📋 GOLD TABLES CREATED:")
print("-" * 80)
print(f"  • {c360_table} ({c360_rows:,} rows) - Unified customer profile")
print(f"  • {seg_table} ({seg_rows:,} rows) - RFM segmentation")
print(f"  • {clv_table} ({clv_rows:,} rows) - Customer lifetime value")
if campaign_df is not None:
    print(f"  • {camp_table} ({camp_rows:,} rows) - Campaign performance")
print(f"  • {daily_table} ({daily_rows:,} rows) - Daily metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Analytics Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer 360 Sample

# COMMAND ----------

print("📊 Top Customers by Total Spend:")
spark.table(get_full_table_name("customer_360", "gold")) \
    .select("customer_id", "customer_name", "total_transactions",
            F.round("total_spend", 2).alias("total_spend"),
            "loyalty_tier", "preferred_channel", "is_active") \
    .orderBy(F.desc("total_spend")) \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segment Distribution

# COMMAND ----------

print("📊 Customer Segment Distribution:")
spark.table(get_full_table_name("customer_segments", "gold")) \
    .groupBy("customer_segment", "churn_risk") \
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.avg("monetary"), 2).alias("avg_spend")
    ) \
    .orderBy(F.desc("customer_count")) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Campaign Performance

# COMMAND ----------

if campaign_df is not None:
    print("📊 Campaign Performance Ranking:")
    spark.table(get_full_table_name("campaign_performance", "gold")) \
        .select("campaign_rank", "campaign_source", "total_sessions",
                "conversions", "conversion_rate",
                F.round("total_revenue", 2).alias("total_revenue")) \
        .orderBy("campaign_rank") \
        .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC The Gold layer is now ready for:
# MAGIC - **Dashboards**: Connect Databricks SQL or BI tools
# MAGIC - **ML Models**: Use for churn prediction, CLV forecasting
# MAGIC - **Ad-hoc Analysis**: Query directly in notebooks
# MAGIC
# MAGIC ### Table Summary
# MAGIC
# MAGIC Tables created in `customer360_demo.gold` schema:
# MAGIC
# MAGIC | Table | Grain | Use Case |
# MAGIC |-------|-------|----------|
# MAGIC | `gold.customer_360` | customer_id | Single customer view, support tools |
# MAGIC | `gold.customer_segments` | customer_id | Marketing targeting, personalization |
# MAGIC | `gold.customer_clv` | customer_id | Investment prioritization, retention |
# MAGIC | `gold.campaign_performance` | campaign_source | Marketing ROI, budget allocation |
# MAGIC | `gold.daily_metrics` | transaction_date | Trend analysis, forecasting |
