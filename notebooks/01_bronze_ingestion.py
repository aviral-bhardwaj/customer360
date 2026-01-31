# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Pipeline - Raw Data Ingestion
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook implements the **Bronze Layer** of the Medallion Architecture.
# MAGIC
# MAGIC **Purpose**: Ingest raw CSV files into Delta tables with minimal transformation.
# MAGIC
# MAGIC **Characteristics**:
# MAGIC - Data preserved exactly as received from source
# MAGIC - Add metadata columns for lineage tracking
# MAGIC - Schema evolution enabled for flexibility
# MAGIC - Append-only for production (overwrite for initial load)
# MAGIC
# MAGIC ## Input
# MAGIC - Raw CSV files from configured data paths
# MAGIC
# MAGIC ## Output
# MAGIC - Bronze Delta tables with `bronze_` prefix
# MAGIC
# MAGIC ---
# MAGIC **Schedule**: Daily (or triggered on new file arrival)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import Dict, List, Tuple
from datetime import datetime
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Discovery
# MAGIC Scan and discover CSV files with their schemas.

# COMMAND ----------

def discover_csv_files(base_path: str, recursive: bool = True) -> List[str]:
    """
    Discover all CSV files in the given path.

    Args:
        base_path: Root directory to scan
        recursive: Whether to scan subdirectories

    Returns:
        List of CSV file paths
    """
    csv_files = []

    try:
        # Try using dbutils (Databricks environment)
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
        # Fallback for local testing
        import glob
        pattern = f"{base_path}/**/*.csv" if recursive else f"{base_path}/*.csv"
        csv_files = glob.glob(pattern, recursive=recursive)

    return csv_files


def discover_csv_schemas(base_path: str) -> Dict[str, Dict]:
    """
    Scan all CSV files and infer their schemas.

    Args:
        base_path: Root directory to scan

    Returns:
        Dictionary with file metadata including schema, row count, sample values
    """
    print(f"🔍 Scanning for CSV files in: {base_path}")
    print("=" * 80)

    csv_files = discover_csv_files(base_path)
    print(f"Found {len(csv_files)} CSV files\n")

    discovered_files = {}

    for file_path in csv_files:
        try:
            # Read CSV with schema inference
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("samplingRatio", "1.0") \
                .csv(file_path)

            file_name = os.path.basename(file_path)
            row_count = df.count()

            # Collect schema information
            schema_info = []
            for field in df.schema.fields:
                schema_info.append({
                    "column_name": field.name,
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                })

            # Get sample values
            sample_row = df.limit(1).collect()
            sample_values = {}
            if sample_row:
                for field in df.schema.fields:
                    val = sample_row[0][field.name]
                    sample_values[field.name] = str(val)[:50] if val else "NULL"

            discovered_files[file_name] = {
                "file_path": file_path,
                "row_count": row_count,
                "column_count": len(df.columns),
                "columns": df.columns,
                "schema": schema_info,
                "sample_values": sample_values,
                "dataframe": df
            }

            print(f"📄 {file_name}")
            print(f"   Rows: {row_count:,} | Columns: {len(df.columns)}")
            print(f"   Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
            print()

        except Exception as e:
            print(f"⚠️ Error processing {file_path}: {e}\n")

    return discovered_files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Summary Report

# COMMAND ----------

def print_schema_summary(discovered_files: Dict[str, Dict]) -> None:
    """Print a formatted summary of discovered schemas."""

    print("\n" + "=" * 100)
    print("📊 SCHEMA DISCOVERY SUMMARY")
    print("=" * 100)

    # Summary table
    summary_data = []
    for file_name, info in discovered_files.items():
        col_preview = ", ".join(info["columns"][:4])
        if len(info["columns"]) > 4:
            col_preview += f" +{len(info['columns'])-4} more"

        summary_data.append((
            file_name,
            info["row_count"],
            info["column_count"],
            col_preview
        ))

    summary_df = spark.createDataFrame(
        summary_data,
        ["File Name", "Rows", "Columns", "Column Preview"]
    )
    summary_df.show(truncate=False)

    # Detailed schemas
    print("\n📋 DETAILED SCHEMAS")
    print("=" * 100)

    for file_name, info in discovered_files.items():
        print(f"\n{'─' * 60}")
        print(f"📄 {file_name} ({info['row_count']:,} rows)")
        print(f"{'─' * 60}")
        print(f"{'Column Name':<30} {'Data Type':<20} {'Sample':<40}")
        print(f"{'-'*30} {'-'*20} {'-'*40}")

        for col_info in info["schema"]:
            sample = info["sample_values"].get(col_info["column_name"], "N/A")[:40]
            print(f"{col_info['column_name']:<30} {col_info['data_type']:<20} {sample:<40}")

# COMMAND ----------

# Discover all CSV schemas
discovered_schemas = discover_csv_schemas(BASE_RAW_PATH)
print_schema_summary(discovered_schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion Functions

# COMMAND ----------

def add_metadata_columns(df: DataFrame, source_file: str) -> DataFrame:
    """
    Add standard metadata columns for data lineage tracking.

    Args:
        df: Source DataFrame
        source_file: Original source file path

    Returns:
        DataFrame with metadata columns added
    """
    return df \
        .withColumn("_source_file", F.lit(source_file)) \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_ingestion_date", F.current_date())


def ingest_to_bronze(
    df: DataFrame,
    table_name: str,
    source_file: str,
    mode: str = None
) -> Tuple[str, int]:
    """
    Ingest a DataFrame into a Bronze Delta table.

    Args:
        df: Source DataFrame
        table_name: Logical table name (without prefix)
        source_file: Original source file for lineage
        mode: Write mode (defaults to config WRITE_MODE)

    Returns:
        Tuple of (full_table_name, row_count)
    """
    mode = mode or WRITE_MODE

    # Add metadata columns
    bronze_df = add_metadata_columns(df, source_file)

    # Get full table name with bronze prefix
    full_table_name = get_full_table_name(table_name, layer="bronze")

    # Write to Delta
    bronze_df.write \
        .format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)

    row_count = bronze_df.count()
    return full_table_name, row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Bronze Ingestion

# COMMAND ----------

def run_bronze_ingestion(discovered_files: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Run the complete Bronze ingestion pipeline.

    Args:
        discovered_files: Dictionary from schema discovery

    Returns:
        Dictionary with ingestion results
    """
    print("\n" + "=" * 80)
    print("🥉 BRONZE LAYER INGESTION")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Write Mode: {WRITE_MODE}")
    print()

    # Setup database/catalog
    setup_database()

    ingestion_results = {}
    success_count = 0
    error_count = 0

    for file_name, info in discovered_files.items():
        try:
            # Get logical table name from mapping, or auto-generate
            table_name = FILE_TO_TABLE_MAPPING.get(file_name)
            if table_name is None:
                table_name = file_name.replace(".csv", "").lower().replace(" ", "_").replace("-", "_")
                print(f"ℹ️  Auto-mapping: {file_name} → {table_name}")

            # Ingest to Bronze
            full_table_name, row_count = ingest_to_bronze(
                df=info["dataframe"],
                table_name=table_name,
                source_file=info["file_path"]
            )

            ingestion_results[table_name] = {
                "status": "SUCCESS",
                "source_file": file_name,
                "full_table_name": full_table_name,
                "row_count": row_count
            }

            print(f"✅ {full_table_name}")
            print(f"   Source: {file_name} | Rows: {row_count:,}")
            success_count += 1

        except Exception as e:
            ingestion_results[file_name] = {
                "status": "ERROR",
                "error": str(e)
            }
            print(f"❌ Error ingesting {file_name}: {e}")
            error_count += 1

        print()

    # Summary
    print("=" * 80)
    print("📊 BRONZE INGESTION SUMMARY")
    print("=" * 80)
    print(f"Total Files:    {len(discovered_files)}")
    print(f"Successful:     {success_count}")
    print(f"Errors:         {error_count}")
    print(f"End Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    return ingestion_results

# COMMAND ----------

# DBTITLE 1,Override setup_database to handle catalog creation
# Workaround: Override setup_database() to handle catalog creation with storage location
def setup_database():
    """
    Create the database/schema for the medallion layers.
    Modified to handle Unity Catalog storage location requirement.
    """
    if USE_UNITY_CATALOG:
        # Try to use existing catalog first, or create with managed location
        try:
            spark.sql(f"USE CATALOG {CATALOG_NAME}")
            print(f"✅ Using existing catalog: {CATALOG_NAME}")
        except:
            # Catalog doesn't exist, try to create it with managed location
            try:
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME} MANAGED LOCATION '{DELTA_BASE_PATH}catalogs/{CATALOG_NAME}'")
                print(f"✅ Created catalog: {CATALOG_NAME}")
            except Exception as e:
                print(f"⚠️ Could not create catalog. Please create '{CATALOG_NAME}' manually via UI or use an existing catalog.")
                print(f"Error: {e}")
                # Fallback to 'main' catalog if available
                try:
                    spark.sql("USE CATALOG main")
                    print(f"✅ Falling back to 'main' catalog")
                    # Update global CATALOG_NAME
                    globals()['CATALOG_NAME'] = 'main'
                except:
                    raise Exception("Cannot create or access any catalog. Please create a catalog manually.")
        
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
        spark.sql(f"USE CATALOG {CATALOG_NAME}")
        spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
        print(f"✅ Using Unity Catalog: {CATALOG_NAME}.{SCHEMA_NAME}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        spark.sql(f"USE {DATABASE_NAME}")
        print(f"✅ Using database: {DATABASE_NAME}")

# COMMAND ----------

# Execute Bronze ingestion
bronze_results = run_bronze_ingestion(discovered_schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

def verify_bronze_tables(results: Dict[str, Dict]) -> None:
    """Display verification of created Bronze tables."""

    print("\n📋 BRONZE TABLES CREATED:")
    print("-" * 80)

    for table_name, info in results.items():
        if info["status"] == "SUCCESS":
            print(f"  • {info['full_table_name']}")
            print(f"    Source: {info['source_file']} | Rows: {info['row_count']:,}")

    # Show sample from first table
    print("\n" + "=" * 80)
    print("📄 SAMPLE DATA (first Bronze table)")
    print("=" * 80)

    for table_name, info in results.items():
        if info["status"] == "SUCCESS":
            df = spark.table(info['full_table_name'])
            df.select(df.columns[:6]).show(5, truncate=False)
            break

# COMMAND ----------

verify_bronze_tables(bronze_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC Bronze layer ingestion is complete. The following tables are now available:
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `bronze_customers` | Customer master data |
# MAGIC | `bronze_products` | Product catalog |
# MAGIC | `bronze_stores` | Store locations |
# MAGIC | `bronze_agents` | Support agents |
# MAGIC | `bronze_online_transactions` | E-commerce transactions |
# MAGIC | `bronze_instore_transactions` | In-store purchases |
# MAGIC | `bronze_loyalty_accounts` | Loyalty membership |
# MAGIC | `bronze_loyalty_transactions` | Points activity |
# MAGIC | `bronze_service_interactions` | Support tickets |
# MAGIC | `bronze_customer_insights` | Comprehensive analytics |
# MAGIC | `bronze_shopping_behavior` | Shopping patterns |
# MAGIC
# MAGIC **Next Step**: Run `02_silver_transformation` to create cleaned and conformed tables.
