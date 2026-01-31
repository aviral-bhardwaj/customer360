# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Pipeline - Data Transformation & Cleansing
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook implements the **Silver Layer** of the Medallion Architecture.
# MAGIC
# MAGIC **Purpose**: Transform raw Bronze data into cleaned, conformed, and enriched tables.
# MAGIC
# MAGIC **Characteristics**:
# MAGIC - Schema standardization (snake_case naming)
# MAGIC - Data type casting and validation
# MAGIC - Null handling and default values
# MAGIC - Deduplication
# MAGIC - Business logic enrichments
# MAGIC - Cross-source data unification
# MAGIC
# MAGIC ## Input
# MAGIC - Bronze Delta tables (from `01_bronze_ingestion`)
# MAGIC
# MAGIC ## Output
# MAGIC - Silver Delta tables in `silver` schema (e.g., `customer360_demo.silver.customers`)
# MAGIC
# MAGIC ---
# MAGIC **Schedule**: Runs after Bronze ingestion completes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType,
    TimestampType, DateType, BooleanType
)
from typing import Dict, Tuple
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Functions

# COMMAND ----------

def apply_data_quality_checks(df: DataFrame, table_name: str) -> Tuple[DataFrame, Dict]:
    """
    Apply standard data quality checks and return metrics.

    Checks:
    - Null value detection
    - Duplicate detection and removal
    - Row count validation

    Args:
        df: Input DataFrame
        table_name: Name for logging

    Returns:
        Tuple of (cleaned DataFrame, quality metrics dict)
    """
    metrics = {
        "table_name": table_name,
        "original_row_count": df.count(),
        "null_counts": {},
        "duplicate_count": 0
    }

    # Count nulls per column (excluding metadata columns)
    for col in df.columns:
        if not col.startswith("_"):
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                metrics["null_counts"][col] = null_count

    # Detect duplicates
    data_cols = [c for c in df.columns if not c.startswith("_")]
    total_rows = df.count()
    distinct_rows = df.select(data_cols).distinct().count()
    metrics["duplicate_count"] = total_rows - distinct_rows

    # Remove exact duplicates
    cleaned_df = df.dropDuplicates(data_cols)
    metrics["cleaned_row_count"] = cleaned_df.count()
    metrics["rows_removed"] = metrics["original_row_count"] - metrics["cleaned_row_count"]

    return cleaned_df, metrics


def print_quality_report(metrics: Dict) -> None:
    """Print data quality metrics."""
    print(f"   Original rows: {metrics['original_row_count']:,}")
    if metrics['duplicate_count'] > 0:
        print(f"   Duplicates removed: {metrics['duplicate_count']:,}")
    if metrics['null_counts']:
        print(f"   Columns with nulls: {list(metrics['null_counts'].keys())}")
    print(f"   Final rows: {metrics['cleaned_row_count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Transformation Functions

# COMMAND ----------

def transform_silver_customers(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze customers to Silver layer.

    Transformations:
    - Standardize column names to snake_case
    - Validate email format
    - Extract email domain
    - Add quality flags
    """
    silver_df = bronze_df \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("Name", "customer_name") \
        .withColumnRenamed("Email", "email") \
        .withColumnRenamed("Address", "full_address") \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("email_domain",
            F.when(F.col("email").contains("@"),
                   F.split(F.col("email"), "@").getItem(1))
            .otherwise(None)
        ) \
        .withColumn("is_valid_email",
            F.col("email").rlike("^[\\w.-]+@[\\w.-]+\\.\\w+$")
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp()) \
        .drop("_source_file", "_ingestion_timestamp", "_ingestion_date")

    return silver_df

# COMMAND ----------

def transform_silver_transactions(
    online_bronze_df: DataFrame,
    instore_bronze_df: DataFrame
) -> DataFrame:
    """
    Unify and transform online and in-store transactions into Silver layer.

    Transformations:
    - Combine both channels into single table
    - Standardize column names
    - Add channel indicator
    - Parse timestamps and extract date components
    - Cast data types
    """
    # Transform online transactions
    online_silver = online_bronze_df \
        .withColumnRenamed("OrderID", "transaction_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("ProductID", "product_id") \
        .withColumnRenamed("DateTime", "transaction_datetime") \
        .withColumnRenamed("PaymentMethod", "payment_method") \
        .withColumnRenamed("Amount", "amount") \
        .withColumnRenamed("Status", "status") \
        .withColumn("channel", F.lit("online")) \
        .withColumn("store_id", F.lit(None).cast(IntegerType())) \
        .select(
            "transaction_id", "customer_id", "product_id", "store_id",
            "transaction_datetime", "amount", "payment_method", "status", "channel"
        )

    # Transform in-store transactions
    instore_silver = instore_bronze_df \
        .withColumnRenamed("TransactionID", "transaction_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("StoreID", "store_id") \
        .withColumnRenamed("DateTime", "transaction_datetime") \
        .withColumnRenamed("Amount", "amount") \
        .withColumnRenamed("PaymentMethod", "payment_method") \
        .withColumn("channel", F.lit("instore")) \
        .withColumn("product_id", F.lit(None).cast(IntegerType())) \
        .withColumn("status", F.lit("Completed")) \
        .select(
            "transaction_id", "customer_id", "product_id", "store_id",
            "transaction_datetime", "amount", "payment_method", "status", "channel"
        )

    # Union both channels
    unified_df = online_silver.union(instore_silver)

    # Apply transformations
    silver_df = unified_df \
        .withColumn("transaction_id", F.col("transaction_id").cast(IntegerType())) \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("amount", F.col("amount").cast(DoubleType())) \
        .withColumn("transaction_datetime", F.to_timestamp("transaction_datetime")) \
        .withColumn("transaction_date", F.to_date("transaction_datetime")) \
        .withColumn("transaction_year", F.year("transaction_datetime")) \
        .withColumn("transaction_month", F.month("transaction_datetime")) \
        .withColumn("transaction_quarter", F.quarter("transaction_datetime")) \
        .withColumn("transaction_day_of_week", F.dayofweek("transaction_datetime")) \
        .withColumn("transaction_hour", F.hour("transaction_datetime")) \
        .withColumn("is_weekend",
            F.when(F.col("transaction_day_of_week").isin([1, 7]), True)
            .otherwise(False)
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp())

    return silver_df

# COMMAND ----------

def transform_silver_loyalty(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze loyalty accounts to Silver layer.

    Transformations:
    - Standardize column names
    - Add tier ranking for sorting
    - Calculate tenure days
    - Cast data types
    """
    silver_df = bronze_df \
        .withColumnRenamed("LoyaltyID", "loyalty_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("PointsEarned", "total_points") \
        .withColumnRenamed("TierLevel", "tier_level") \
        .withColumnRenamed("JoinDate", "join_date") \
        .withColumn("loyalty_id", F.col("loyalty_id").cast(IntegerType())) \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("total_points", F.col("total_points").cast(IntegerType())) \
        .withColumn("join_date", F.to_date("join_date")) \
        .withColumn("tenure_days",
            F.datediff(F.current_date(), F.col("join_date"))
        ) \
        .withColumn("tenure_months",
            F.floor(F.col("tenure_days") / 30)
        ) \
        .withColumn("tier_rank",
            F.when(F.col("tier_level") == "Platinum", 4)
             .when(F.col("tier_level") == "Gold", 3)
             .when(F.col("tier_level") == "Silver", 2)
             .when(F.col("tier_level") == "Bronze", 1)
             .otherwise(0)
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp()) \
        .drop("_source_file", "_ingestion_timestamp", "_ingestion_date")

    return silver_df

# COMMAND ----------

def transform_silver_service_interactions(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze service interactions to Silver layer.

    Transformations:
    - Standardize column names
    - Add resolution flags
    - Parse timestamps
    - Add derived metrics
    """
    silver_df = bronze_df \
        .withColumnRenamed("InteractionID", "interaction_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("DateTime", "interaction_datetime") \
        .withColumnRenamed("AgentID", "agent_id") \
        .withColumnRenamed("IssueType", "issue_type") \
        .withColumnRenamed("ResolutionStatus", "resolution_status") \
        .withColumn("interaction_id", F.col("interaction_id").cast(IntegerType())) \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("agent_id", F.col("agent_id").cast(IntegerType())) \
        .withColumn("interaction_datetime", F.to_timestamp("interaction_datetime")) \
        .withColumn("interaction_date", F.to_date("interaction_datetime")) \
        .withColumn("interaction_hour", F.hour("interaction_datetime")) \
        .withColumn("is_resolved",
            F.col("resolution_status") == "Resolved"
        ) \
        .withColumn("is_escalated",
            F.col("resolution_status") == "Escalated"
        ) \
        .withColumn("is_pending",
            F.col("resolution_status") == "Pending"
        ) \
        .withColumn("issue_category",
            F.when(F.col("issue_type").isin(["Technical Issue", "Billing"]), "Technical")
             .when(F.col("issue_type") == "Complaint", "Complaint")
             .when(F.col("issue_type") == "Product Inquiry", "Inquiry")
             .otherwise("Other")
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp()) \
        .drop("_source_file", "_ingestion_timestamp", "_ingestion_date")

    return silver_df

# COMMAND ----------

def transform_silver_customer_insights(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze customer insights to Silver layer.

    Transformations:
    - Standardize column names
    - Add demographic segments
    - Calculate order metrics
    - Parse timestamps
    """
    # Note: CampaignSchema has trailing space in source
    silver_df = bronze_df \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("FullName", "customer_name") \
        .withColumnRenamed("Gender", "gender") \
        .withColumnRenamed("Age", "age") \
        .withColumnRenamed("CreditScore", "credit_score") \
        .withColumnRenamed("MonthlyIncome", "monthly_income") \
        .withColumnRenamed("Country", "country") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("Category", "product_category") \
        .withColumnRenamed("Product", "product_name") \
        .withColumnRenamed("Cost", "product_cost") \
        .withColumnRenamed("Price", "product_price") \
        .withColumnRenamed("Quantity", "quantity") \
        .withColumnRenamed("PaymentMethod", "payment_method") \
        .withColumnRenamed("SessionStart", "session_start") \
        .withColumnRenamed("SessionEnd", "session_end") \
        .withColumnRenamed("CartAdditionTime", "cart_addition_time") \
        .withColumnRenamed("OrderConfirmation", "order_confirmed") \
        .withColumnRenamed("OrderConfirmationTime", "order_confirmation_time") \
        .withColumnRenamed("OrderReturn", "order_returned") \
        .withColumnRenamed("ReturnReason", "return_reason")

    # Handle the column with trailing space
    if "CampaignSchema " in bronze_df.columns:
        silver_df = silver_df.withColumnRenamed("CampaignSchema ", "campaign_source")
    elif "CampaignSchema" in bronze_df.columns:
        silver_df = silver_df.withColumnRenamed("CampaignSchema", "campaign_source")

    # Apply type casting and derivations
    silver_df = silver_df \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("age", F.col("age").cast(IntegerType())) \
        .withColumn("credit_score", F.col("credit_score").cast(IntegerType())) \
        .withColumn("monthly_income", F.col("monthly_income").cast(DoubleType())) \
        .withColumn("product_cost", F.col("product_cost").cast(DoubleType())) \
        .withColumn("product_price", F.col("product_price").cast(DoubleType())) \
        .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
        .withColumn("session_start", F.to_timestamp("session_start")) \
        .withColumn("session_end", F.to_timestamp("session_end")) \
        .withColumn("order_value",
            F.col("product_price") * F.col("quantity")
        ) \
        .withColumn("profit_margin",
            F.col("product_price") - F.col("product_cost")
        ) \
        .withColumn("total_profit",
            F.col("profit_margin") * F.col("quantity")
        ) \
        .withColumn("order_confirmed",
            F.when(F.col("order_confirmed") == "True", True)
             .when(F.col("order_confirmed") == "False", False)
             .otherwise(F.col("order_confirmed").cast(BooleanType()))
        ) \
        .withColumn("order_returned",
            F.when(F.col("order_returned") == "True", True)
             .when(F.col("order_returned") == "False", False)
             .otherwise(F.col("order_returned").cast(BooleanType()))
        ) \
        .withColumn("age_group",
            F.when(F.col("age") < 25, "18-24")
             .when(F.col("age") < 35, "25-34")
             .when(F.col("age") < 45, "35-44")
             .when(F.col("age") < 55, "45-54")
             .when(F.col("age") < 65, "55-64")
             .otherwise("65+")
        ) \
        .withColumn("income_bracket",
            F.when(F.col("monthly_income") < 3000, "Low (<3K)")
             .when(F.col("monthly_income") < 5000, "Medium (3K-5K)")
             .when(F.col("monthly_income") < 7500, "High (5K-7.5K)")
             .otherwise("Premium (7.5K+)")
        ) \
        .withColumn("credit_tier",
            F.when(F.col("credit_score") >= 750, "Excellent")
             .when(F.col("credit_score") >= 700, "Good")
             .when(F.col("credit_score") >= 650, "Fair")
             .otherwise("Poor")
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp()) \
        .drop("_source_file", "_ingestion_timestamp", "_ingestion_date")

    return silver_df

# COMMAND ----------

def transform_silver_products(bronze_df: DataFrame) -> DataFrame:
    """Transform Bronze products to Silver layer."""
    silver_df = bronze_df \
        .withColumnRenamed("ProductID", "product_id") \
        .withColumnRenamed("Name", "product_name") \
        .withColumnRenamed("Category", "category") \
        .withColumnRenamed("Price", "price") \
        .withColumn("product_id", F.col("product_id").cast(IntegerType())) \
        .withColumn("price", F.col("price").cast(DoubleType())) \
        .withColumn("price_tier",
            F.when(F.col("price") < 100, "Budget")
             .when(F.col("price") < 250, "Mid-Range")
             .when(F.col("price") < 400, "Premium")
             .otherwise("Luxury")
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp()) \
        .drop("_source_file", "_ingestion_timestamp", "_ingestion_date")

    return silver_df

# COMMAND ----------

def transform_silver_stores(bronze_df: DataFrame) -> DataFrame:
    """Transform Bronze stores to Silver layer."""
    silver_df = bronze_df \
        .withColumnRenamed("StoreID", "store_id") \
        .withColumnRenamed("Location", "location") \
        .withColumnRenamed("Manager", "manager_name") \
        .withColumnRenamed("OpenHours", "open_hours") \
        .withColumn("store_id", F.col("store_id").cast(IntegerType())) \
        .withColumn("_silver_timestamp", F.current_timestamp()) \
        .drop("_source_file", "_ingestion_timestamp", "_ingestion_date")

    return silver_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Silver Tables

# COMMAND ----------

def save_silver_table(df: DataFrame, table_name: str) -> Tuple[str, int]:
    """
    Save DataFrame as a Silver Delta table.

    Args:
        df: Transformed DataFrame
        table_name: Logical table name (without prefix)

    Returns:
        Tuple of (full_table_name, row_count)
    """
    full_table_name = get_full_table_name(table_name, layer="silver")

    df.write \
        .format("delta") \
        .mode(WRITE_MODE) \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)

    row_count = df.count()
    return full_table_name, row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Silver Transformations

# COMMAND ----------

def run_silver_transformations() -> Dict[str, Dict]:
    """
    Run the complete Silver transformation pipeline.

    Returns:
        Dictionary with transformation results
    """
    print("\n" + "=" * 80)
    print("🥈 SILVER LAYER TRANSFORMATION")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Setup silver schema (creates if not exists)
    setup_layer_schema("silver")

    results = {}

    # =========================================================================
    # Transform Customers
    # =========================================================================
    print("📊 Processing: Customers")
    try:
        bronze_df = spark.table(get_full_table_name("customers", "bronze"))
        cleaned_df, metrics = apply_data_quality_checks(bronze_df, "customers")
        silver_df = transform_silver_customers(cleaned_df)
        table_name, row_count = save_silver_table(silver_df, "customers")
        print_quality_report(metrics)
        print(f"   ✅ Saved: {table_name}\n")
        results["customers"] = {"status": "SUCCESS", "table": table_name, "rows": row_count}
    except Exception as e:
        print(f"   ❌ Error: {e}\n")
        results["customers"] = {"status": "ERROR", "error": str(e)}

    # =========================================================================
    # Transform Unified Transactions
    # =========================================================================
    print("📊 Processing: Unified Transactions (Online + In-Store)")
    try:
        online_df = spark.table(get_full_table_name("online_transactions", "bronze"))
        instore_df = spark.table(get_full_table_name("instore_transactions", "bronze"))
        silver_df = transform_silver_transactions(online_df, instore_df)
        table_name, row_count = save_silver_table(silver_df, "transactions")
        print(f"   Online transactions: {online_df.count():,}")
        print(f"   In-store transactions: {instore_df.count():,}")
        print(f"   Combined total: {row_count:,}")
        print(f"   ✅ Saved: {table_name}\n")
        results["transactions"] = {"status": "SUCCESS", "table": table_name, "rows": row_count}
    except Exception as e:
        print(f"   ❌ Error: {e}\n")
        results["transactions"] = {"status": "ERROR", "error": str(e)}

    # =========================================================================
    # Transform Loyalty
    # =========================================================================
    print("📊 Processing: Loyalty Accounts")
    try:
        bronze_df = spark.table(get_full_table_name("loyalty_accounts", "bronze"))
        cleaned_df, metrics = apply_data_quality_checks(bronze_df, "loyalty")
        silver_df = transform_silver_loyalty(cleaned_df)
        table_name, row_count = save_silver_table(silver_df, "loyalty")
        print_quality_report(metrics)
        print(f"   ✅ Saved: {table_name}\n")
        results["loyalty"] = {"status": "SUCCESS", "table": table_name, "rows": row_count}
    except Exception as e:
        print(f"   ❌ Error: {e}\n")
        results["loyalty"] = {"status": "ERROR", "error": str(e)}

    # =========================================================================
    # Transform Service Interactions
    # =========================================================================
    print("📊 Processing: Service Interactions")
    try:
        bronze_df = spark.table(get_full_table_name("service_interactions", "bronze"))
        cleaned_df, metrics = apply_data_quality_checks(bronze_df, "service_interactions")
        silver_df = transform_silver_service_interactions(cleaned_df)
        table_name, row_count = save_silver_table(silver_df, "service_interactions")
        print_quality_report(metrics)
        print(f"   ✅ Saved: {table_name}\n")
        results["service_interactions"] = {"status": "SUCCESS", "table": table_name, "rows": row_count}
    except Exception as e:
        print(f"   ❌ Error: {e}\n")
        results["service_interactions"] = {"status": "ERROR", "error": str(e)}

    # =========================================================================
    # Transform Customer Insights
    # =========================================================================
    print("📊 Processing: Customer Insights")
    try:
        bronze_df = spark.table(get_full_table_name("customer_insights", "bronze"))
        cleaned_df, metrics = apply_data_quality_checks(bronze_df, "customer_insights")
        silver_df = transform_silver_customer_insights(cleaned_df)
        table_name, row_count = save_silver_table(silver_df, "customer_insights")
        print_quality_report(metrics)
        print(f"   ✅ Saved: {table_name}\n")
        results["customer_insights"] = {"status": "SUCCESS", "table": table_name, "rows": row_count}
    except Exception as e:
        print(f"   ❌ Error: {e}\n")
        results["customer_insights"] = {"status": "ERROR", "error": str(e)}

    # =========================================================================
    # Transform Products
    # =========================================================================
    print("📊 Processing: Products")
    try:
        bronze_df = spark.table(get_full_table_name("products", "bronze"))
        cleaned_df, metrics = apply_data_quality_checks(bronze_df, "products")
        silver_df = transform_silver_products(cleaned_df)
        table_name, row_count = save_silver_table(silver_df, "products")
        print_quality_report(metrics)
        print(f"   ✅ Saved: {table_name}\n")
        results["products"] = {"status": "SUCCESS", "table": table_name, "rows": row_count}
    except Exception as e:
        print(f"   ❌ Error: {e}\n")
        results["products"] = {"status": "ERROR", "error": str(e)}

    # =========================================================================
    # Transform Stores
    # =========================================================================
    print("📊 Processing: Stores")
    try:
        bronze_df = spark.table(get_full_table_name("stores", "bronze"))
        cleaned_df, metrics = apply_data_quality_checks(bronze_df, "stores")
        silver_df = transform_silver_stores(cleaned_df)
        table_name, row_count = save_silver_table(silver_df, "stores")
        print_quality_report(metrics)
        print(f"   ✅ Saved: {table_name}\n")
        results["stores"] = {"status": "SUCCESS", "table": table_name, "rows": row_count}
    except Exception as e:
        print(f"   ❌ Error: {e}\n")
        results["stores"] = {"status": "ERROR", "error": str(e)}

    # =========================================================================
    # Summary
    # =========================================================================
    success_count = sum(1 for r in results.values() if r["status"] == "SUCCESS")
    error_count = sum(1 for r in results.values() if r["status"] == "ERROR")

    print("=" * 80)
    print("📊 SILVER TRANSFORMATION SUMMARY")
    print("=" * 80)
    print(f"Total Tables:   {len(results)}")
    print(f"Successful:     {success_count}")
    print(f"Errors:         {error_count}")
    print(f"End Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    return results

# COMMAND ----------

# Execute Silver transformations
silver_results = run_silver_transformations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables

# COMMAND ----------

print("\n📋 SILVER TABLES CREATED:")
print("-" * 80)

for table_name, info in silver_results.items():
    if info["status"] == "SUCCESS":
        print(f"  • {info['table']} ({info['rows']:,} rows)")

# Show sample data
print("\n" + "=" * 80)
print("📄 SAMPLE: silver_transactions")
print("=" * 80)

spark.table(get_full_table_name("transactions", "silver")) \
    .select("transaction_id", "customer_id", "amount", "channel",
            "transaction_date", "is_weekend") \
    .show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC Silver layer transformation is complete. Tables created in `customer360_demo.silver` schema:
# MAGIC
# MAGIC | Table | Grain | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | `silver.customers` | customer_id | Standardized customer profiles |
# MAGIC | `silver.transactions` | transaction_id | Unified online + in-store transactions |
# MAGIC | `silver.loyalty` | loyalty_id | Enriched loyalty data with tier ranks |
# MAGIC | `silver.service_interactions` | interaction_id | Support tickets with resolution flags |
# MAGIC | `silver.customer_insights` | customer_id + session | Demographics and purchase behavior |
# MAGIC | `silver.products` | product_id | Product catalog with price tiers |
# MAGIC | `silver.stores` | store_id | Store locations |
# MAGIC
# MAGIC **Next Step**: Run `03_gold_analytics` to create analytics-ready aggregated tables.
