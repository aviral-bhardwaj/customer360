# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 Pipeline - Shared Configuration
# MAGIC
# MAGIC This module contains shared configuration variables and utility functions
# MAGIC used across all Medallion layers (Bronze, Silver, Gold).
# MAGIC
# MAGIC **Usage**: Run this notebook first, or use `%run ./config` in other notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Variables
# MAGIC Modify these for your environment.

# COMMAND ----------

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

# Base path where raw CSV files are stored
# Adjust based on your Databricks environment
BASE_RAW_PATH = "/Workspace/Repos/customer360/data"

# Alternative paths for different environments:
# BASE_RAW_PATH = "/dbfs/mnt/datalake/raw/customer360"   # DBFS mount
# BASE_RAW_PATH = "/Volumes/catalog/schema/raw_data"     # Unity Catalog volume

# =============================================================================
# CATALOG & SCHEMA CONFIGURATION
# =============================================================================

# Unity Catalog settings
CATALOG_NAME = "customer360_demo"
SCHEMA_NAME = "analytics"

# Legacy database name (if not using Unity Catalog)
DATABASE_NAME = "customer360_db"

# Use Unity Catalog (set to False for legacy Hive metastore)
USE_UNITY_CATALOG = True

# Delta table storage location (for external/unmanaged tables)
DELTA_BASE_PATH = "/dbfs/mnt/delta/customer360"

# =============================================================================
# DATA SOURCE PATHS
# =============================================================================

# Subdirectories within BASE_RAW_PATH
DATA_PATHS = {
    "core_360": f"{BASE_RAW_PATH}/360",
    "insights": BASE_RAW_PATH,
    "shopping_behavior": f"{BASE_RAW_PATH}/shopping_behaviour"
}

# =============================================================================
# FILE TO TABLE MAPPING
# =============================================================================

# Maps source CSV files to logical table names
FILE_TO_TABLE_MAPPING = {
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

# =============================================================================
# TABLE NAMING CONVENTIONS
# =============================================================================

# Prefixes for each layer
BRONZE_PREFIX = "bronze_"
SILVER_PREFIX = "silver_"
GOLD_PREFIX = "gold_"

# =============================================================================
# PROCESSING CONFIGURATION
# =============================================================================

# Write mode for initial load vs incremental
INITIAL_LOAD_MODE = "overwrite"
INCREMENTAL_MODE = "append"

# Current processing mode (change for production incremental loads)
WRITE_MODE = INITIAL_LOAD_MODE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def get_full_table_name(table_name: str, layer: str = None) -> str:
    """
    Get the fully qualified table name based on catalog/schema settings.

    Args:
        table_name: Base table name (without layer prefix)
        layer: Optional layer prefix ('bronze', 'silver', 'gold')

    Returns:
        Fully qualified table name
    """
    # Add layer prefix if specified
    if layer:
        prefix_map = {
            "bronze": BRONZE_PREFIX,
            "silver": SILVER_PREFIX,
            "gold": GOLD_PREFIX
        }
        table_name = f"{prefix_map.get(layer, '')}{table_name}"

    if USE_UNITY_CATALOG:
        return f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
    else:
        return f"{DATABASE_NAME}.{table_name}"


def setup_database():
    """
    Create the database/schema for the medallion layers.
    Must be called before writing any tables.
    """
    if USE_UNITY_CATALOG:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
        spark.sql(f"USE CATALOG {CATALOG_NAME}")
        spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
        print(f"✅ Using Unity Catalog: {CATALOG_NAME}.{SCHEMA_NAME}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        spark.sql(f"USE {DATABASE_NAME}")
        print(f"✅ Using database: {DATABASE_NAME}")


def print_config():
    """Print current configuration for verification."""
    print("=" * 60)
    print("CUSTOMER 360 PIPELINE CONFIGURATION")
    print("=" * 60)
    print(f"Base Raw Path:     {BASE_RAW_PATH}")
    print(f"Unity Catalog:     {USE_UNITY_CATALOG}")
    if USE_UNITY_CATALOG:
        print(f"Catalog:           {CATALOG_NAME}")
        print(f"Schema:            {SCHEMA_NAME}")
    else:
        print(f"Database:          {DATABASE_NAME}")
    print(f"Write Mode:        {WRITE_MODE}")
    print("=" * 60)

# COMMAND ----------

# Print configuration on load
print_config()
