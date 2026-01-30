# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 Analytics Demo - Medallion Architecture Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook demonstrates a complete **Customer 360** analytics solution using the **Medallion Architecture** (Bronze → Silver → Gold).
# MAGIC
# MAGIC ### What is Customer 360?
# MAGIC Customer 360 is a unified view of customer data aggregated from multiple sources - transactions, loyalty programs,
# MAGIC support interactions, demographics, and behavioral data - enabling:
# MAGIC - **Single Customer View**: One consolidated profile per customer
# MAGIC - **Customer Segmentation**: RFM analysis, churn prediction, lifetime value
# MAGIC - **Personalization**: Next-best-offer, campaign optimization
# MAGIC
# MAGIC ### Medallion Architecture Layers
# MAGIC | Layer | Purpose | Data Quality |
# MAGIC |-------|---------|--------------|
# MAGIC | **Bronze** | Raw data ingestion, append-only | As-is from source |
# MAGIC | **Silver** | Cleaned, conformed, enriched | Validated & standardized |
# MAGIC | **Gold** | Business-level aggregates | Analytics-ready |
# MAGIC
# MAGIC ---
# MAGIC **Author**: Customer Analytics Team
# MAGIC **Last Updated**: 2025

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Demo Use Cases
# MAGIC
# MAGIC This demo showcases **5 key Customer 360 analytics use cases**:
# MAGIC
# MAGIC ### Use Case 1: Unified Customer Profile (Single View)
# MAGIC **Business Question**: "Who is this customer across all our touchpoints?"
# MAGIC - **Tables Used**: Customers, LoyaltyAccounts, OnlineTransactions, InStoreTransactions, CustomerServiceInteractions
# MAGIC - **KPIs**: Total spend, loyalty tier, support history, preferred channels
# MAGIC - **Visualization**: Customer profile card with 360-degree view
# MAGIC
# MAGIC ### Use Case 2: RFM Customer Segmentation
# MAGIC **Business Question**: "Which customers are most valuable? Who is at risk of churning?"
# MAGIC - **Tables Used**: Customers, OnlineTransactions, InStoreTransactions
# MAGIC - **KPIs**: Recency (days since last purchase), Frequency (# transactions), Monetary (total spend)
# MAGIC - **Visualization**: Segment distribution chart, customer value matrix
# MAGIC
# MAGIC ### Use Case 3: Customer Lifetime Value (CLV) Estimation
# MAGIC **Business Question**: "What is each customer worth to us over their lifetime?"
# MAGIC - **Tables Used**: Customers, All Transactions, LoyaltyAccounts
# MAGIC - **KPIs**: Historical CLV, predicted CLV, customer acquisition cost ratio
# MAGIC - **Visualization**: CLV distribution, top customers ranking
# MAGIC
# MAGIC ### Use Case 4: Campaign Performance & Attribution
# MAGIC **Business Question**: "Which campaigns drive the most valuable customers?"
# MAGIC - **Tables Used**: Customer360Insights (contains campaign data)
# MAGIC - **KPIs**: Conversion rate by campaign, revenue per campaign, ROI
# MAGIC - **Visualization**: Campaign performance dashboard, attribution funnel
# MAGIC
# MAGIC ### Use Case 5: Churn Risk & Support Impact Analysis
# MAGIC **Business Question**: "Which customers are likely to churn? How does support quality impact retention?"
# MAGIC - **Tables Used**: CustomerServiceInteractions, Transactions, LoyaltyAccounts
# MAGIC - **KPIs**: Churn risk score, support ticket frequency, resolution rate
# MAGIC - **Visualization**: Churn risk heatmap, support impact correlation

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configuration
# MAGIC Modify these variables for your environment.

# COMMAND ----------

# =============================================================================
# CONFIGURATION - MODIFY THESE FOR YOUR ENVIRONMENT
# =============================================================================

# Base path where raw CSV files are stored
# For Databricks: Use DBFS paths like "/dbfs/mnt/raw/" or Unity Catalog volumes
BASE_RAW_PATH = "/Workspace/Repos/customer360/data"  # Adjust for your environment

# Alternative paths for different environments
# BASE_RAW_PATH = "/dbfs/mnt/datalake/raw/customer360"  # DBFS mount
# BASE_RAW_PATH = "/Volumes/catalog/schema/raw_data"    # Unity Catalog volume

# Catalog and schema configuration (Unity Catalog)
CATALOG_NAME = "customer360_demo"
SCHEMA_NAME = "analytics"

# Legacy database name (if not using Unity Catalog)
DATABASE_NAME = "customer360_db"

# Delta table storage location (for non-managed tables)
DELTA_BASE_PATH = "/dbfs/mnt/delta/customer360"

# Use Unity Catalog (set to False for legacy Hive metastore)
USE_UNITY_CATALOG = True

# Data source subdirectories
DATA_SOURCES = {
    "core_360": "360",                        # Core customer 360 tables
    "insights": "",                           # Customer360Insights.csv (root data folder)
    "shopping_behavior": "shopping_behaviour" # Shopping behavior data
}

# CSV file patterns for auto-discovery
CSV_PATTERNS = {
    "customers": ["Customers*.csv", "customers*.csv"],
    "products": ["Products*.csv", "products*.csv"],
    "stores": ["Stores*.csv", "stores*.csv"],
    "agents": ["Agents*.csv", "agents*.csv"],
    "online_transactions": ["OnlineTransactions*.csv", "online_transactions*.csv"],
    "instore_transactions": ["InStoreTransactions*.csv", "instore_transactions*.csv"],
    "loyalty_accounts": ["LoyaltyAccounts*.csv", "loyalty_accounts*.csv"],
    "loyalty_transactions": ["LoyaltyTransactions*.csv", "loyalty_transactions*.csv"],
    "service_interactions": ["CustomerServiceInteractions*.csv", "service_interactions*.csv"],
    "customer_insights": ["Customer360Insights*.csv"],
    "shopping_behavior": ["customer_transactions*.csv"]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports and Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, BooleanType
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import os

# Initialize Spark session (already available in Databricks as 'spark')
# This is here for local testing - in Databricks, use the existing 'spark' variable
try:
    spark
except NameError:
    spark = SparkSession.builder \
        .appName("Customer360_Medallion_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Configuration loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔍 STEP 1: Schema Discovery
# MAGIC Automatically scan CSV files, infer schemas, and display summary statistics.

# COMMAND ----------

def discover_csv_schemas(base_path: str, recursive: bool = True) -> Dict[str, Dict]:
    """
    Scan all CSV files in a directory, infer schemas, and return comprehensive metadata.

    Args:
        base_path: Root directory to scan for CSV files
        recursive: Whether to scan subdirectories

    Returns:
        Dictionary with file metadata including schema, row count, sample values
    """
    discovered_files = {}

    # Get list of CSV files
    # In Databricks, use dbutils.fs.ls() for DBFS paths
    # For local/repo paths, we'll use a different approach

    try:
        # Try using dbutils (Databricks environment)
        csv_files = []
        def list_files_recursive(path):
            try:
                items = dbutils.fs.ls(path)
                for item in items:
                    if item.isDir() and recursive:
                        list_files_recursive(item.path)
                    elif item.path.lower().endswith('.csv'):
                        csv_files.append(item.path)
            except Exception as e:
                print(f"Warning: Could not list {path}: {e}")

        list_files_recursive(base_path)

    except NameError:
        # Fallback for non-Databricks environment (local testing)
        import glob
        pattern = f"{base_path}/**/*.csv" if recursive else f"{base_path}/*.csv"
        csv_files = glob.glob(pattern, recursive=recursive)

    print(f"Found {len(csv_files)} CSV files")
    print("=" * 80)

    for file_path in csv_files:
        try:
            # Read CSV with schema inference
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("samplingRatio", "1.0") \
                .csv(file_path)

            # Get file name
            file_name = os.path.basename(file_path)

            # Collect schema information
            schema_info = []
            for field in df.schema.fields:
                schema_info.append({
                    "column_name": field.name,
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                })

            # Get row count
            row_count = df.count()

            # Get sample values (first row)
            sample_row = df.limit(1).collect()
            sample_values = {}
            if sample_row:
                for field in df.schema.fields:
                    sample_values[field.name] = str(sample_row[0][field.name])[:50]  # Truncate long values

            discovered_files[file_name] = {
                "file_path": file_path,
                "row_count": row_count,
                "column_count": len(df.columns),
                "columns": df.columns,
                "schema": schema_info,
                "sample_values": sample_values,
                "dataframe": df
            }

            print(f"\n📄 {file_name}")
            print(f"   Path: {file_path}")
            print(f"   Rows: {row_count:,} | Columns: {len(df.columns)}")
            print(f"   Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")

        except Exception as e:
            print(f"⚠️ Error processing {file_path}: {e}")

    return discovered_files


def print_schema_summary_table(discovered_files: Dict[str, Dict]) -> None:
    """
    Print a formatted summary table of all discovered CSV schemas.
    """
    print("\n" + "=" * 100)
    print("📊 SCHEMA DISCOVERY SUMMARY")
    print("=" * 100)

    # Create summary data
    summary_data = []
    for file_name, info in discovered_files.items():
        # Format column info
        col_summary = []
        for col_info in info["schema"][:5]:  # First 5 columns
            col_summary.append(f"{col_info['column_name']}({col_info['data_type'][:10]})")
        col_str = ", ".join(col_summary)
        if len(info["schema"]) > 5:
            col_str += f" +{len(info['schema'])-5} more"

        summary_data.append({
            "file_name": file_name,
            "rows": info["row_count"],
            "cols": info["column_count"],
            "columns_preview": col_str
        })

    # Create DataFrame for nice display
    summary_df = spark.createDataFrame(summary_data)
    summary_df.show(truncate=False)

    # Print detailed schema for each file
    print("\n" + "=" * 100)
    print("📋 DETAILED SCHEMAS")
    print("=" * 100)

    for file_name, info in discovered_files.items():
        print(f"\n{'─' * 50}")
        print(f"📄 {file_name} ({info['row_count']:,} rows)")
        print(f"{'─' * 50}")
        print(f"{'Column Name':<30} {'Data Type':<20} {'Sample Value':<40}")
        print(f"{'-' * 30} {'-' * 20} {'-' * 40}")

        for col_info in info["schema"]:
            sample = info["sample_values"].get(col_info["column_name"], "N/A")[:40]
            print(f"{col_info['column_name']:<30} {col_info['data_type']:<20} {sample:<40}")

# COMMAND ----------

# Execute schema discovery
print("🔍 Starting CSV Schema Discovery...")
print(f"📁 Scanning: {BASE_RAW_PATH}")

discovered_schemas = discover_csv_schemas(BASE_RAW_PATH)
print_schema_summary_table(discovered_schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥉 STEP 2: Bronze Layer - Raw Data Ingestion
# MAGIC Load raw CSV data into Delta tables with minimal transformation (append-only, schema preserved).

# COMMAND ----------

def setup_database() -> None:
    """
    Create the database/schema for the medallion layers.
    """
    if USE_UNITY_CATALOG:
        # Unity Catalog approach
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
        spark.sql(f"USE CATALOG {CATALOG_NAME}")
        spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
        print(f"✅ Using Unity Catalog: {CATALOG_NAME}.{SCHEMA_NAME}")
    else:
        # Legacy Hive metastore approach
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        spark.sql(f"USE {DATABASE_NAME}")
        print(f"✅ Using database: {DATABASE_NAME}")


def ingest_bronze_table(
    df: DataFrame,
    table_name: str,
    source_file: str,
    mode: str = "overwrite"
) -> DataFrame:
    """
    Ingest a DataFrame into a Bronze Delta table.

    Bronze layer characteristics:
    - Raw data preserved as-is
    - Add metadata columns for lineage
    - Schema evolution enabled
    - Append-only for production (overwrite for demo)

    Args:
        df: Source DataFrame
        table_name: Target table name (will be prefixed with 'bronze_')
        source_file: Original source file path for lineage
        mode: Write mode ('overwrite' for demo, 'append' for production)

    Returns:
        The written DataFrame
    """
    bronze_table_name = f"bronze_{table_name}"

    # Add metadata columns for data lineage
    bronze_df = df \
        .withColumn("_source_file", F.lit(source_file)) \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_ingestion_date", F.current_date())

    # Write to Delta table
    if USE_UNITY_CATALOG:
        full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{bronze_table_name}"
    else:
        full_table_name = f"{DATABASE_NAME}.{bronze_table_name}"

    bronze_df.write \
        .format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)

    row_count = bronze_df.count()
    print(f"✅ Bronze table created: {full_table_name} ({row_count:,} rows)")

    return bronze_df


def ingest_all_bronze_tables(discovered_files: Dict[str, Dict]) -> Dict[str, DataFrame]:
    """
    Ingest all discovered CSV files into Bronze Delta tables.

    Returns:
        Dictionary mapping table names to their Bronze DataFrames
    """
    print("\n" + "=" * 80)
    print("🥉 BRONZE LAYER INGESTION")
    print("=" * 80)

    # Setup database
    setup_database()

    # Mapping from file names to logical table names
    file_to_table_mapping = {
        "Customers.csv": "customers",
        "Products.csv": "products",
        "Stores.csv": "stores",
        "Agents.csv": "agents",
        "OnlineTransactions.csv": "online_transactions",
        "InStoreTransactions.csv": "instore_transactions",
        "LoyaltyAccounts.csv": "loyalty_accounts",
        "LoyaltyTransactions.csv": "loyalty_transactions",
        "CustomerServiceInteractions.csv": "service_interactions",
        "Customer360Insights.csv": "customer_insights",
        "customer_transactions_final.csv": "shopping_behavior"
    }

    bronze_tables = {}

    for file_name, info in discovered_files.items():
        # Get logical table name
        table_name = file_to_table_mapping.get(file_name)

        if table_name is None:
            # Auto-generate table name from file name
            table_name = file_name.replace(".csv", "").lower().replace(" ", "_")
            print(f"ℹ️ Auto-mapping: {file_name} → {table_name}")

        # Ingest to Bronze
        bronze_df = ingest_bronze_table(
            df=info["dataframe"],
            table_name=table_name,
            source_file=info["file_path"]
        )

        bronze_tables[table_name] = bronze_df

    print(f"\n✅ Bronze layer complete: {len(bronze_tables)} tables ingested")
    return bronze_tables

# COMMAND ----------

# Execute Bronze layer ingestion
bronze_tables = ingest_all_bronze_tables(discovered_schemas)

# Display Bronze tables
print("\n📋 Bronze Tables Created:")
for table_name in bronze_tables.keys():
    print(f"   • bronze_{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥈 STEP 3: Silver Layer - Cleaned & Conformed Data
# MAGIC Apply data quality checks, standardize formats, and create conformed customer-centric tables.

# COMMAND ----------

def apply_data_quality_checks(df: DataFrame, table_name: str) -> Tuple[DataFrame, Dict]:
    """
    Apply standard data quality checks and return cleaned DataFrame with quality metrics.

    Checks applied:
    - Null value detection and handling
    - Duplicate detection
    - Data type validation
    - Value range validation
    """
    quality_metrics = {
        "table_name": table_name,
        "original_row_count": df.count(),
        "null_counts": {},
        "duplicate_count": 0
    }

    # Count nulls per column
    null_counts = {}
    for col in df.columns:
        if not col.startswith("_"):  # Skip metadata columns
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                null_counts[col] = null_count
    quality_metrics["null_counts"] = null_counts

    # Detect duplicates (based on all columns except metadata)
    data_cols = [c for c in df.columns if not c.startswith("_")]
    total_rows = df.count()
    distinct_rows = df.select(data_cols).distinct().count()
    quality_metrics["duplicate_count"] = total_rows - distinct_rows

    # Clean: Remove complete duplicates
    cleaned_df = df.dropDuplicates(data_cols)
    quality_metrics["cleaned_row_count"] = cleaned_df.count()

    return cleaned_df, quality_metrics


def transform_to_silver_customers(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze customers to Silver layer with standardization.

    Transformations:
    - Standardize column names to snake_case
    - Parse and validate email format
    - Extract address components
    - Add data quality flags
    """
    silver_df = bronze_df \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("Name", "customer_name") \
        .withColumnRenamed("Email", "email") \
        .withColumnRenamed("Address", "full_address") \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("email_domain", F.split(F.col("email"), "@").getItem(1)) \
        .withColumn("is_valid_email", F.col("email").rlike("^[\\w.-]+@[\\w.-]+\\.\\w+$")) \
        .withColumn("_silver_timestamp", F.current_timestamp())

    return silver_df


def transform_to_silver_transactions(
    online_df: DataFrame,
    instore_df: DataFrame
) -> DataFrame:
    """
    Combine and transform online and in-store transactions into unified Silver transactions.

    Transformations:
    - Unify schema across channels
    - Standardize date/time formats
    - Add channel indicator
    - Calculate derived fields
    """
    # Transform online transactions
    online_silver = online_df \
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
    instore_silver = instore_df \
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

    # Union all transactions
    unified_transactions = online_silver.union(instore_silver)

    # Add derived columns
    silver_df = unified_transactions \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("amount", F.col("amount").cast(DoubleType())) \
        .withColumn("transaction_datetime", F.to_timestamp("transaction_datetime")) \
        .withColumn("transaction_date", F.to_date("transaction_datetime")) \
        .withColumn("transaction_year", F.year("transaction_datetime")) \
        .withColumn("transaction_month", F.month("transaction_datetime")) \
        .withColumn("transaction_day_of_week", F.dayofweek("transaction_datetime")) \
        .withColumn("is_weekend", F.when(F.col("transaction_day_of_week").isin([1, 7]), True).otherwise(False)) \
        .withColumn("_silver_timestamp", F.current_timestamp())

    return silver_df


def transform_to_silver_loyalty(
    accounts_df: DataFrame,
    transactions_df: DataFrame
) -> DataFrame:
    """
    Transform loyalty data into Silver layer with enrichments.
    """
    # Clean loyalty accounts
    accounts_silver = accounts_df \
        .withColumnRenamed("LoyaltyID", "loyalty_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("PointsEarned", "total_points") \
        .withColumnRenamed("TierLevel", "tier_level") \
        .withColumnRenamed("JoinDate", "join_date") \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("total_points", F.col("total_points").cast(IntegerType())) \
        .withColumn("join_date", F.to_date("join_date")) \
        .withColumn("tenure_days", F.datediff(F.current_date(), F.col("join_date"))) \
        .withColumn("tier_rank",
            F.when(F.col("tier_level") == "Platinum", 4)
             .when(F.col("tier_level") == "Gold", 3)
             .when(F.col("tier_level") == "Silver", 2)
             .otherwise(1)
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp())

    return accounts_silver


def transform_to_silver_service_interactions(bronze_df: DataFrame) -> DataFrame:
    """
    Transform customer service interactions to Silver layer.
    """
    silver_df = bronze_df \
        .withColumnRenamed("InteractionID", "interaction_id") \
        .withColumnRenamed("CustomerID", "customer_id") \
        .withColumnRenamed("DateTime", "interaction_datetime") \
        .withColumnRenamed("AgentID", "agent_id") \
        .withColumnRenamed("IssueType", "issue_type") \
        .withColumnRenamed("ResolutionStatus", "resolution_status") \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("interaction_datetime", F.to_timestamp("interaction_datetime")) \
        .withColumn("interaction_date", F.to_date("interaction_datetime")) \
        .withColumn("is_resolved", F.col("resolution_status") == "Resolved") \
        .withColumn("is_escalated", F.col("resolution_status") == "Escalated") \
        .withColumn("_silver_timestamp", F.current_timestamp())

    return silver_df


def transform_to_silver_customer_insights(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Customer360Insights to Silver layer with standardization.
    """
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
        .withColumnRenamed("CampaignSchema ", "campaign_source")  # Note: has trailing space in source
        .withColumnRenamed("PaymentMethod", "payment_method") \
        .withColumnRenamed("OrderConfirmation", "order_confirmed") \
        .withColumnRenamed("OrderReturn", "order_returned") \
        .withColumnRenamed("ReturnReason", "return_reason") \
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType())) \
        .withColumn("age", F.col("age").cast(IntegerType())) \
        .withColumn("credit_score", F.col("credit_score").cast(IntegerType())) \
        .withColumn("monthly_income", F.col("monthly_income").cast(DoubleType())) \
        .withColumn("product_cost", F.col("product_cost").cast(DoubleType())) \
        .withColumn("product_price", F.col("product_price").cast(DoubleType())) \
        .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
        .withColumn("order_value", F.col("product_price") * F.col("quantity")) \
        .withColumn("profit_margin", F.col("product_price") - F.col("product_cost")) \
        .withColumn("order_confirmed", F.col("order_confirmed") == "True") \
        .withColumn("order_returned", F.col("order_returned") == "True") \
        .withColumn("age_group",
            F.when(F.col("age") < 25, "18-24")
             .when(F.col("age") < 35, "25-34")
             .when(F.col("age") < 45, "35-44")
             .when(F.col("age") < 55, "45-54")
             .when(F.col("age") < 65, "55-64")
             .otherwise("65+")
        ) \
        .withColumn("income_bracket",
            F.when(F.col("monthly_income") < 3000, "Low")
             .when(F.col("monthly_income") < 5000, "Medium")
             .when(F.col("monthly_income") < 7500, "High")
             .otherwise("Premium")
        ) \
        .withColumn("_silver_timestamp", F.current_timestamp())

    return silver_df


def create_all_silver_tables(bronze_tables: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Create all Silver layer tables from Bronze tables.
    """
    print("\n" + "=" * 80)
    print("🥈 SILVER LAYER TRANSFORMATION")
    print("=" * 80)

    silver_tables = {}

    # Silver Customers
    if "customers" in bronze_tables:
        print("\n📊 Transforming: Customers")
        cleaned_df, metrics = apply_data_quality_checks(bronze_tables["customers"], "customers")
        silver_customers = transform_to_silver_customers(cleaned_df)
        silver_tables["customers"] = silver_customers
        save_silver_table(silver_customers, "customers")
        print(f"   Quality: {metrics['original_row_count']} rows, {metrics['duplicate_count']} duplicates removed")

    # Silver Unified Transactions
    if "online_transactions" in bronze_tables and "instore_transactions" in bronze_tables:
        print("\n📊 Transforming: Unified Transactions")
        silver_transactions = transform_to_silver_transactions(
            bronze_tables["online_transactions"],
            bronze_tables["instore_transactions"]
        )
        silver_tables["transactions"] = silver_transactions
        save_silver_table(silver_transactions, "transactions")
        print(f"   Combined online + in-store: {silver_transactions.count():,} total transactions")

    # Silver Loyalty
    if "loyalty_accounts" in bronze_tables:
        print("\n📊 Transforming: Loyalty Accounts")
        cleaned_df, metrics = apply_data_quality_checks(bronze_tables["loyalty_accounts"], "loyalty_accounts")
        silver_loyalty = transform_to_silver_loyalty(cleaned_df, bronze_tables.get("loyalty_transactions"))
        silver_tables["loyalty"] = silver_loyalty
        save_silver_table(silver_loyalty, "loyalty")

    # Silver Service Interactions
    if "service_interactions" in bronze_tables:
        print("\n📊 Transforming: Service Interactions")
        cleaned_df, metrics = apply_data_quality_checks(bronze_tables["service_interactions"], "service_interactions")
        silver_service = transform_to_silver_service_interactions(cleaned_df)
        silver_tables["service_interactions"] = silver_service
        save_silver_table(silver_service, "service_interactions")

    # Silver Customer Insights
    if "customer_insights" in bronze_tables:
        print("\n📊 Transforming: Customer Insights")
        cleaned_df, metrics = apply_data_quality_checks(bronze_tables["customer_insights"], "customer_insights")
        silver_insights = transform_to_silver_customer_insights(cleaned_df)
        silver_tables["customer_insights"] = silver_insights
        save_silver_table(silver_insights, "customer_insights")

    print(f"\n✅ Silver layer complete: {len(silver_tables)} tables created")
    return silver_tables


def save_silver_table(df: DataFrame, table_name: str) -> None:
    """Save DataFrame as a Silver Delta table."""
    silver_table_name = f"silver_{table_name}"

    if USE_UNITY_CATALOG:
        full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{silver_table_name}"
    else:
        full_table_name = f"{DATABASE_NAME}.{silver_table_name}"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)

    print(f"   ✅ Saved: {full_table_name} ({df.count():,} rows)")

# COMMAND ----------

# Execute Silver layer transformations
silver_tables = create_all_silver_tables(bronze_tables)

# Display Silver tables
print("\n📋 Silver Tables Created:")
for table_name in silver_tables.keys():
    print(f"   • silver_{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥇 STEP 4: Gold Layer - Analytics-Ready Tables
# MAGIC Create business-level aggregates optimized for Customer 360 analytics.

# COMMAND ----------

def build_gold_customer_360(silver_tables: Dict[str, DataFrame]) -> DataFrame:
    """
    Build the comprehensive Customer 360 profile table.

    This is the central Gold table that provides a unified view of each customer
    aggregating data from all touchpoints.

    Grain: One row per customer
    """
    print("\n📊 Building: Gold Customer 360 Profile")

    # Start with customer base
    customers = silver_tables.get("customers")
    if customers is None:
        print("   ⚠️ No customer base table found, using customer_insights")
        # Fall back to customer insights for customer base
        customers = silver_tables["customer_insights"] \
            .select("customer_id", "customer_name", "gender", "age", "age_group",
                    "credit_score", "monthly_income", "income_bracket",
                    "country", "state", "city") \
            .dropDuplicates(["customer_id"])

    # Aggregate transaction metrics
    if "transactions" in silver_tables:
        txn_metrics = silver_tables["transactions"] \
            .groupBy("customer_id") \
            .agg(
                F.count("*").alias("total_transactions"),
                F.sum("amount").alias("total_spend"),
                F.avg("amount").alias("avg_transaction_value"),
                F.max("transaction_date").alias("last_transaction_date"),
                F.min("transaction_date").alias("first_transaction_date"),
                F.countDistinct("channel").alias("channels_used"),
                F.sum(F.when(F.col("channel") == "online", 1).otherwise(0)).alias("online_transactions"),
                F.sum(F.when(F.col("channel") == "instore", 1).otherwise(0)).alias("instore_transactions")
            ) \
            .withColumn("days_since_last_transaction",
                F.datediff(F.current_date(), F.col("last_transaction_date"))) \
            .withColumn("customer_tenure_days",
                F.datediff(F.current_date(), F.col("first_transaction_date")))
    else:
        txn_metrics = None

    # Aggregate loyalty metrics
    if "loyalty" in silver_tables:
        loyalty_metrics = silver_tables["loyalty"] \
            .groupBy("customer_id") \
            .agg(
                F.max("total_points").alias("loyalty_points"),
                F.max("tier_level").alias("loyalty_tier"),
                F.max("tier_rank").alias("loyalty_tier_rank"),
                F.max("tenure_days").alias("loyalty_tenure_days")
            )
    else:
        loyalty_metrics = None

    # Aggregate service interaction metrics
    if "service_interactions" in silver_tables:
        service_metrics = silver_tables["service_interactions"] \
            .groupBy("customer_id") \
            .agg(
                F.count("*").alias("total_support_tickets"),
                F.sum(F.when(F.col("is_resolved"), 1).otherwise(0)).alias("resolved_tickets"),
                F.sum(F.when(F.col("is_escalated"), 1).otherwise(0)).alias("escalated_tickets"),
                F.max("interaction_date").alias("last_support_date")
            ) \
            .withColumn("support_resolution_rate",
                F.col("resolved_tickets") / F.col("total_support_tickets"))
    else:
        service_metrics = None

    # Build Customer 360 profile
    customer_360 = customers

    if txn_metrics is not None:
        customer_360 = customer_360.join(txn_metrics, "customer_id", "left")

    if loyalty_metrics is not None:
        customer_360 = customer_360.join(loyalty_metrics, "customer_id", "left")

    if service_metrics is not None:
        customer_360 = customer_360.join(service_metrics, "customer_id", "left")

    # Add calculated fields
    customer_360 = customer_360 \
        .withColumn("is_active",
            F.when(F.col("days_since_last_transaction") <= 90, True).otherwise(False)) \
        .withColumn("preferred_channel",
            F.when(F.col("online_transactions") > F.col("instore_transactions"), "Online")
             .when(F.col("instore_transactions") > F.col("online_transactions"), "In-Store")
             .otherwise("Omnichannel")) \
        .withColumn("_gold_timestamp", F.current_timestamp())

    # Fill nulls for metrics
    customer_360 = customer_360.fillna({
        "total_transactions": 0,
        "total_spend": 0.0,
        "avg_transaction_value": 0.0,
        "loyalty_points": 0,
        "total_support_tickets": 0
    })

    return customer_360


def build_gold_customer_segments(customer_360: DataFrame) -> DataFrame:
    """
    Build RFM-based customer segmentation table.

    RFM Segmentation:
    - Recency: Days since last purchase (lower is better)
    - Frequency: Number of transactions (higher is better)
    - Monetary: Total spend (higher is better)

    Grain: One row per customer with segment assignment
    """
    print("\n📊 Building: Gold Customer Segments (RFM Analysis)")

    # Calculate RFM scores (1-5 scale, 5 is best)
    # Using ntile for quintile-based scoring
    window_spec = Window.orderBy(F.col("metric"))

    # Recency score (lower days = higher score, so we reverse)
    rfm_df = customer_360 \
        .withColumn("recency_days", F.coalesce(F.col("days_since_last_transaction"), F.lit(999))) \
        .withColumn("frequency", F.coalesce(F.col("total_transactions"), F.lit(0))) \
        .withColumn("monetary", F.coalesce(F.col("total_spend"), F.lit(0.0)))

    # Calculate percentile-based scores
    rfm_df = rfm_df \
        .withColumn("r_score",
            F.when(F.col("recency_days") <= 30, 5)
             .when(F.col("recency_days") <= 60, 4)
             .when(F.col("recency_days") <= 90, 3)
             .when(F.col("recency_days") <= 180, 2)
             .otherwise(1)) \
        .withColumn("f_score",
            F.when(F.col("frequency") >= 10, 5)
             .when(F.col("frequency") >= 5, 4)
             .when(F.col("frequency") >= 3, 3)
             .when(F.col("frequency") >= 1, 2)
             .otherwise(1)) \
        .withColumn("m_score",
            F.when(F.col("monetary") >= 1000, 5)
             .when(F.col("monetary") >= 500, 4)
             .when(F.col("monetary") >= 200, 3)
             .when(F.col("monetary") >= 50, 2)
             .otherwise(1))

    # Calculate composite RFM score
    rfm_df = rfm_df \
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score")) \
        .withColumn("rfm_string",
            F.concat(F.col("r_score"), F.col("f_score"), F.col("m_score")))

    # Assign customer segments based on RFM
    segments_df = rfm_df \
        .withColumn("customer_segment",
            F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Champions")
             .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 4), "Loyal Customers")
             .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "New Customers")
             .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Potential Loyalists")
             .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3), "At Risk")
             .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Can't Lose Them")
             .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "Hibernating")
             .when((F.col("r_score") <= 2) & (F.col("m_score") <= 2), "Lost")
             .otherwise("Needs Attention")
        ) \
        .withColumn("churn_risk",
            F.when(F.col("customer_segment").isin(["At Risk", "Can't Lose Them"]), "High")
             .when(F.col("customer_segment").isin(["Hibernating", "Lost"]), "Critical")
             .when(F.col("customer_segment").isin(["Needs Attention"]), "Medium")
             .otherwise("Low")
        ) \
        .withColumn("_gold_timestamp", F.current_timestamp())

    # Select relevant columns
    segments_output = segments_df.select(
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
        "_gold_timestamp"
    )

    return segments_output


def build_gold_customer_clv(customer_360: DataFrame, silver_tables: Dict[str, DataFrame]) -> DataFrame:
    """
    Build Customer Lifetime Value (CLV) estimation table.

    CLV Calculation approach:
    - Historical CLV: Sum of all past transactions
    - Predicted CLV: Based on average transaction value * predicted future transactions

    Grain: One row per customer
    """
    print("\n📊 Building: Gold Customer CLV")

    clv_df = customer_360 \
        .withColumn("historical_clv", F.coalesce(F.col("total_spend"), F.lit(0.0))) \
        .withColumn("avg_purchase_value", F.coalesce(F.col("avg_transaction_value"), F.lit(0.0))) \
        .withColumn("purchase_frequency_annual",
            F.when(F.col("customer_tenure_days") > 0,
                (F.col("total_transactions") / F.col("customer_tenure_days")) * 365
            ).otherwise(0)
        ) \
        .withColumn("predicted_annual_value",
            F.col("avg_purchase_value") * F.col("purchase_frequency_annual")
        ) \
        .withColumn("predicted_clv_3yr",
            F.col("predicted_annual_value") * 3
        ) \
        .withColumn("clv_tier",
            F.when(F.col("historical_clv") >= 1000, "Platinum")
             .when(F.col("historical_clv") >= 500, "Gold")
             .when(F.col("historical_clv") >= 200, "Silver")
             .otherwise("Bronze")
        ) \
        .withColumn("_gold_timestamp", F.current_timestamp())

    clv_output = clv_df.select(
        "customer_id",
        "historical_clv",
        "avg_purchase_value",
        "purchase_frequency_annual",
        "predicted_annual_value",
        "predicted_clv_3yr",
        "clv_tier",
        "_gold_timestamp"
    )

    return clv_output


def build_gold_campaign_performance(silver_tables: Dict[str, DataFrame]) -> DataFrame:
    """
    Build campaign performance analytics table.

    Grain: One row per campaign source
    """
    print("\n📊 Building: Gold Campaign Performance")

    if "customer_insights" not in silver_tables:
        print("   ⚠️ Customer insights table not available")
        return None

    insights = silver_tables["customer_insights"]

    campaign_metrics = insights \
        .groupBy("campaign_source") \
        .agg(
            F.count("*").alias("total_sessions"),
            F.sum(F.when(F.col("order_confirmed"), 1).otherwise(0)).alias("conversions"),
            F.sum("order_value").alias("total_revenue"),
            F.avg("order_value").alias("avg_order_value"),
            F.sum(F.when(F.col("order_returned"), 1).otherwise(0)).alias("returns"),
            F.countDistinct("customer_id").alias("unique_customers")
        ) \
        .withColumn("conversion_rate",
            F.col("conversions") / F.col("total_sessions") * 100) \
        .withColumn("return_rate",
            F.col("returns") / F.col("conversions") * 100) \
        .withColumn("revenue_per_session",
            F.col("total_revenue") / F.col("total_sessions")) \
        .withColumn("_gold_timestamp", F.current_timestamp())

    return campaign_metrics


def build_gold_daily_metrics(silver_tables: Dict[str, DataFrame]) -> DataFrame:
    """
    Build daily aggregated metrics for trend analysis.

    Grain: One row per date
    """
    print("\n📊 Building: Gold Daily Metrics")

    if "transactions" not in silver_tables:
        print("   ⚠️ Transactions table not available")
        return None

    daily_metrics = silver_tables["transactions"] \
        .groupBy("transaction_date") \
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_transaction_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("channel") == "online", F.col("amount")).otherwise(0)).alias("online_revenue"),
            F.sum(F.when(F.col("channel") == "instore", F.col("amount")).otherwise(0)).alias("instore_revenue")
        ) \
        .withColumn("online_revenue_pct",
            F.col("online_revenue") / F.col("total_revenue") * 100) \
        .orderBy("transaction_date") \
        .withColumn("_gold_timestamp", F.current_timestamp())

    return daily_metrics


def save_gold_table(df: DataFrame, table_name: str) -> None:
    """Save DataFrame as a Gold Delta table."""
    gold_table_name = f"gold_{table_name}"

    if USE_UNITY_CATALOG:
        full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{gold_table_name}"
    else:
        full_table_name = f"{DATABASE_NAME}.{gold_table_name}"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)

    print(f"   ✅ Saved: {full_table_name} ({df.count():,} rows)")


def create_all_gold_tables(silver_tables: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Create all Gold layer tables from Silver tables.
    """
    print("\n" + "=" * 80)
    print("🥇 GOLD LAYER - ANALYTICS TABLES")
    print("=" * 80)

    gold_tables = {}

    # Build Customer 360 Profile
    customer_360 = build_gold_customer_360(silver_tables)
    gold_tables["customer_360"] = customer_360
    save_gold_table(customer_360, "customer_360")

    # Build Customer Segments (RFM)
    customer_segments = build_gold_customer_segments(customer_360)
    gold_tables["customer_segments"] = customer_segments
    save_gold_table(customer_segments, "customer_segments")

    # Build Customer CLV
    customer_clv = build_gold_customer_clv(customer_360, silver_tables)
    gold_tables["customer_clv"] = customer_clv
    save_gold_table(customer_clv, "customer_clv")

    # Build Campaign Performance
    campaign_performance = build_gold_campaign_performance(silver_tables)
    if campaign_performance is not None:
        gold_tables["campaign_performance"] = campaign_performance
        save_gold_table(campaign_performance, "campaign_performance")

    # Build Daily Metrics
    daily_metrics = build_gold_daily_metrics(silver_tables)
    if daily_metrics is not None:
        gold_tables["daily_metrics"] = daily_metrics
        save_gold_table(daily_metrics, "daily_metrics")

    print(f"\n✅ Gold layer complete: {len(gold_tables)} tables created")
    return gold_tables

# COMMAND ----------

# Execute Gold layer creation
gold_tables = create_all_gold_tables(silver_tables)

# Display Gold tables
print("\n📋 Gold Tables Created:")
for table_name in gold_tables.keys():
    print(f"   • gold_{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📊 STEP 5: Demo Visualizations & Analytics
# MAGIC Sample queries and visualizations for client demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 1: Customer 360 Profile View
# MAGIC Show a complete customer profile combining all data sources.

# COMMAND ----------

def display_customer_360_sample(gold_tables: Dict[str, DataFrame], num_customers: int = 5) -> None:
    """
    Display sample Customer 360 profiles for demonstration.
    """
    print("\n" + "=" * 80)
    print("👤 CUSTOMER 360 PROFILE SAMPLES")
    print("=" * 80)

    customer_360 = gold_tables["customer_360"]

    # Show top customers by spend
    print("\n📊 Top Customers by Total Spend:")
    customer_360.select(
        "customer_id", "customer_name", "total_transactions",
        "total_spend", "loyalty_tier", "preferred_channel", "is_active"
    ).orderBy(F.desc("total_spend")).limit(num_customers).show(truncate=False)


# Execute demo
display_customer_360_sample(gold_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2: Customer Segmentation Distribution
# MAGIC Show RFM segment distribution for strategic planning.

# COMMAND ----------

def display_segment_distribution(gold_tables: Dict[str, DataFrame]) -> None:
    """
    Display customer segment distribution for demonstration.
    """
    print("\n" + "=" * 80)
    print("📊 CUSTOMER SEGMENT DISTRIBUTION")
    print("=" * 80)

    segments = gold_tables["customer_segments"]

    # Segment distribution
    print("\n📈 Customers by Segment:")
    segment_dist = segments.groupBy("customer_segment") \
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("monetary").alias("avg_spend"),
            F.avg("frequency").alias("avg_transactions")
        ) \
        .orderBy(F.desc("customer_count"))
    segment_dist.show(truncate=False)

    # Churn risk distribution
    print("\n⚠️ Churn Risk Distribution:")
    segments.groupBy("churn_risk") \
        .agg(F.count("*").alias("customer_count")) \
        .orderBy("churn_risk").show()


# Execute demo
display_segment_distribution(gold_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 3: Campaign Performance Analytics
# MAGIC Show which marketing campaigns drive the best results.

# COMMAND ----------

def display_campaign_performance(gold_tables: Dict[str, DataFrame]) -> None:
    """
    Display campaign performance metrics for demonstration.
    """
    print("\n" + "=" * 80)
    print("📣 CAMPAIGN PERFORMANCE ANALYTICS")
    print("=" * 80)

    if "campaign_performance" not in gold_tables:
        print("Campaign data not available")
        return

    campaigns = gold_tables["campaign_performance"]

    print("\n📊 Campaign Performance Summary:")
    campaigns.select(
        "campaign_source", "total_sessions", "conversions",
        F.round("conversion_rate", 2).alias("conversion_rate_%"),
        F.round("total_revenue", 2).alias("total_revenue"),
        F.round("avg_order_value", 2).alias("avg_order_value")
    ).orderBy(F.desc("total_revenue")).show(truncate=False)


# Execute demo
display_campaign_performance(gold_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 4: CLV Tier Analysis
# MAGIC Show customer value distribution and high-value customer identification.

# COMMAND ----------

def display_clv_analysis(gold_tables: Dict[str, DataFrame]) -> None:
    """
    Display CLV analysis for demonstration.
    """
    print("\n" + "=" * 80)
    print("💰 CUSTOMER LIFETIME VALUE ANALYSIS")
    print("=" * 80)

    clv = gold_tables["customer_clv"]

    # CLV tier distribution
    print("\n📊 CLV Tier Distribution:")
    clv.groupBy("clv_tier") \
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("historical_clv").alias("avg_historical_clv"),
            F.avg("predicted_clv_3yr").alias("avg_predicted_3yr_clv")
        ) \
        .orderBy(F.desc("avg_historical_clv")).show(truncate=False)

    # Top 10 by predicted CLV
    print("\n🏆 Top 10 Customers by Predicted 3-Year CLV:")
    clv.select(
        "customer_id",
        F.round("historical_clv", 2).alias("historical_clv"),
        F.round("predicted_clv_3yr", 2).alias("predicted_3yr_clv"),
        "clv_tier"
    ).orderBy(F.desc("predicted_clv_3yr")).limit(10).show()


# Execute demo
display_clv_analysis(gold_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC # 📋 Pipeline Summary
# MAGIC Complete overview of the Medallion architecture implementation.

# COMMAND ----------

def print_pipeline_summary() -> None:
    """
    Print a comprehensive summary of the pipeline.
    """
    print("\n" + "=" * 100)
    print("📋 CUSTOMER 360 MEDALLION PIPELINE - SUMMARY")
    print("=" * 100)

    print("""
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                               MEDALLION ARCHITECTURE OVERVIEW                                     │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                   │
│   ┌──────────────┐      ┌──────────────┐      ┌──────────────────────────────────────────────┐  │
│   │   CSV FILES  │ ───► │    BRONZE    │ ───► │                   SILVER                     │  │
│   │  (Raw Data)  │      │  (Ingestion) │      │          (Cleaned & Conformed)               │  │
│   └──────────────┘      └──────────────┘      └──────────────────────────────────────────────┘  │
│                                                              │                                    │
│                                                              ▼                                    │
│                                           ┌──────────────────────────────────────────────────┐  │
│                                           │                    GOLD                           │  │
│                                           │            (Analytics-Ready)                      │  │
│                                           │                                                   │  │
│                                           │  ┌─────────────┐  ┌─────────────┐  ┌───────────┐ │  │
│                                           │  │ Customer360 │  │  Segments   │  │    CLV    │ │  │
│                                           │  └─────────────┘  └─────────────┘  └───────────┘ │  │
│                                           │  ┌─────────────┐  ┌─────────────┐                │  │
│                                           │  │  Campaign   │  │   Daily     │                │  │
│                                           │  │ Performance │  │  Metrics    │                │  │
│                                           │  └─────────────┘  └─────────────┘                │  │
│                                           └──────────────────────────────────────────────────┘  │
│                                                                                                   │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘

BRONZE LAYER (Raw Ingestion)
├── bronze_customers           │ Customer master data
├── bronze_products            │ Product catalog
├── bronze_stores              │ Store locations
├── bronze_agents              │ Support agents
├── bronze_online_transactions │ E-commerce transactions
├── bronze_instore_transactions│ In-store purchases
├── bronze_loyalty_accounts    │ Loyalty program membership
├── bronze_loyalty_transactions│ Points activity
├── bronze_service_interactions│ Support tickets
├── bronze_customer_insights   │ Comprehensive customer data
└── bronze_shopping_behavior   │ Shopping behavior analytics

SILVER LAYER (Cleaned & Conformed)
├── silver_customers           │ Standardized customer profiles (Grain: customer_id)
├── silver_transactions        │ Unified transactions - online + in-store (Grain: transaction_id)
├── silver_loyalty             │ Enriched loyalty data (Grain: loyalty_id)
├── silver_service_interactions│ Cleaned support tickets (Grain: interaction_id)
└── silver_customer_insights   │ Standardized insights (Grain: session/customer_id)

GOLD LAYER (Analytics-Ready)
├── gold_customer_360          │ Unified customer profile (Grain: customer_id)
├── gold_customer_segments     │ RFM segmentation (Grain: customer_id)
├── gold_customer_clv          │ Customer Lifetime Value (Grain: customer_id)
├── gold_campaign_performance  │ Campaign analytics (Grain: campaign_source)
└── gold_daily_metrics         │ Daily aggregates (Grain: transaction_date)

    """)

    print("\n✅ Pipeline execution complete!")
    print("   - All layers processed successfully")
    print("   - Delta tables created with full data lineage")
    print("   - Ready for dashboards and ML workloads")


# Print summary
print_pipeline_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 Next Steps for Production
# MAGIC
# MAGIC ## Recommended Enhancements:
# MAGIC
# MAGIC 1. **Incremental Processing**
# MAGIC    - Use Delta Lake MERGE for incremental updates
# MAGIC    - Implement change data capture (CDC) patterns
# MAGIC    - Add watermarking for streaming ingestion
# MAGIC
# MAGIC 2. **Data Quality Framework**
# MAGIC    - Integrate Great Expectations or Delta Live Tables expectations
# MAGIC    - Add data quality dashboards
# MAGIC    - Implement alerting for quality issues
# MAGIC
# MAGIC 3. **Orchestration**
# MAGIC    - Schedule with Databricks Workflows
# MAGIC    - Add dependency management
# MAGIC    - Implement retry logic
# MAGIC
# MAGIC 4. **ML Integration**
# MAGIC    - Churn prediction model
# MAGIC    - Customer propensity scoring
# MAGIC    - Next-best-offer recommendations
# MAGIC
# MAGIC 5. **Visualization**
# MAGIC    - Connect to Databricks SQL dashboards
# MAGIC    - Build executive KPI views
# MAGIC    - Create self-service analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **End of Customer 360 Medallion Pipeline Demo**
# MAGIC
# MAGIC For questions or customization, contact the Customer Analytics Team.
