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

# # Drop all tables in the schema, then drop all volumes, then drop the schema itself

# catalog = "customer360_demo"
# schema = "default"

# # Drop all tables in the schema
# tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
# tables = [row.tableName for row in tables_df.collect()]
# for table in tables:
#     spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")

# # Drop all volumes in the schema
# volumes_df = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}")
# volumes = [row.volume_name for row in volumes_df.collect()]
# for volume in volumes:
#     spark.sql(f"DROP VOLUME IF EXISTS {catalog}.{schema}.{volume}")

# # Drop the schema
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")

# COMMAND ----------

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

# Base path where raw CSV files are stored
# Adjust based on your Databricks environment
BASE_RAW_PATH = "/Volumes/mydatabricks/customer360/data_360/data/"


# Alternative paths for different environments:
# BASE_RAW_PATH = "/dbfs/mnt/datalake/raw/customer360"   # DBFS mount
# BASE_RAW_PATH = "/Volumes/catalog/schema/raw_data"     # Unity Catalog volume

# =============================================================================
# CATALOG & SCHEMA CONFIGURATION
# =============================================================================

# Unity Catalog settings
CATALOG_NAME = "customer360_demo"

# Separate schemas for each Medallion layer
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Legacy database names (if not using Unity Catalog)
BRONZE_DATABASE = "customer360_bronze"
SILVER_DATABASE = "customer360_silver"
GOLD_DATABASE = "customer360_gold"

# Use Unity Catalog (set to False for legacy Hive metastore)
USE_UNITY_CATALOG = True

# Delta table storage location (for external/unmanaged tables)
DELTA_BASE_PATH = "/Volumes/mydatabricks/customer360/data_360/delta/"

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

def get_schema_for_layer(layer: str) -> str:
    """
    Get the schema/database name for a specific layer.

    Args:
        layer: 'bronze', 'silver', or 'gold'

    Returns:
        Schema name for Unity Catalog or database name for Hive
    """
    if USE_UNITY_CATALOG:
        schema_map = {
            "bronze": BRONZE_SCHEMA,
            "silver": SILVER_SCHEMA,
            "gold": GOLD_SCHEMA
        }
    else:
        schema_map = {
            "bronze": BRONZE_DATABASE,
            "silver": SILVER_DATABASE,
            "gold": GOLD_DATABASE
        }
    return schema_map.get(layer, BRONZE_SCHEMA)


def get_full_table_name(table_name: str, layer: str) -> str:
    """
    Get the fully qualified table name based on catalog/schema settings.

    Args:
        table_name: Base table name (e.g., 'customers', 'transactions')
        layer: Layer name ('bronze', 'silver', 'gold')

    Returns:
        Fully qualified table name (e.g., 'customer360_demo.silver.customers')
    """
    schema = get_schema_for_layer(layer)

    if USE_UNITY_CATALOG:
        return f"{CATALOG_NAME}.{schema}.{table_name}"
    else:
        return f"{schema}.{table_name}"


def setup_layer_schema(layer: str) -> None:
    """
    Create the schema/database for a specific layer if it doesn't exist.

    Args:
        layer: 'bronze', 'silver', or 'gold'
    """
    schema = get_schema_for_layer(layer)

    if USE_UNITY_CATALOG:
        # Check if catalog exists, if not inform user to create it manually
        try:
            catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
            if CATALOG_NAME not in catalogs:
                print(f"⚠️  Catalog '{CATALOG_NAME}' does not exist.")
                print(f"   Please create it manually in Databricks UI or run:")
                print(f"   CREATE CATALOG {CATALOG_NAME}")
                raise Exception(f"Catalog '{CATALOG_NAME}' not found. Please create it first.")
        except Exception as e:
            if "SHOW CATALOGS" in str(e):
                # If SHOW CATALOGS fails, try to use the catalog directly
                pass
            elif "not found" in str(e).lower():
                raise e

        # Use the catalog
        spark.sql(f"USE CATALOG {CATALOG_NAME}")

        # Create schema if not exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}")
        spark.sql(f"USE SCHEMA {schema}")
        print(f"✅ Using Unity Catalog: {CATALOG_NAME}.{schema}")
    else:
        # Legacy Hive metastore
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        spark.sql(f"USE {schema}")
        print(f"✅ Using database: {schema}")


def print_config():
    """Print current configuration for verification."""
    print("=" * 60)
    print("CUSTOMER 360 PIPELINE CONFIGURATION")
    print("=" * 60)
    print(f"Base Raw Path:     {BASE_RAW_PATH}")
    print(f"Unity Catalog:     {USE_UNITY_CATALOG}")
    if USE_UNITY_CATALOG:
        print(f"Catalog:           {CATALOG_NAME}")
        print(f"Bronze Schema:     {BRONZE_SCHEMA}")
        print(f"Silver Schema:     {SILVER_SCHEMA}")
        print(f"Gold Schema:       {GOLD_SCHEMA}")
    else:
        print(f"Bronze Database:   {BRONZE_DATABASE}")
        print(f"Silver Database:   {SILVER_DATABASE}")
        print(f"Gold Database:     {GOLD_DATABASE}")
    print(f"Write Mode:        {WRITE_MODE}")
    print("=" * 60)

# COMMAND ----------

# Print configuration on load
print_config()
