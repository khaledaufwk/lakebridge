# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader - Dimension Tables
# MAGIC
# MAGIC Loads dimension tables from TimescaleDB using incremental watermark-based extraction.
# MAGIC
# MAGIC **Tables Loaded:** 26 dimension tables
# MAGIC - Organization, Project, Worker, Crew, Trade, Company
# MAGIC - Floor (with geometry), Zone (with geometry)
# MAGIC - Device, DeviceModel, Workshift, WorkshiftDetails
# MAGIC - Activity, LocationGroup, Department, Title
# MAGIC - ObservationSource, ObservationType, ShiftType, ReportType
# MAGIC - Manager, Location, WorkerRole, Task, Shift
# MAGIC
# MAGIC **Watermark Strategy:**
# MAGIC - Most tables use `WatermarkUTC` column
# MAGIC - Reference tables (DeviceModel, Department, etc.) use full load
# MAGIC
# MAGIC **Schedule:** Daily at 2 AM

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

import sys
import os

# Add source directory to path
sys.path.append("/Workspace/migration_project/pipelines/timescaledb/src")

from timescaledb_loader import (
    TimescaleDBLoader,
    TimescaleDBCredentials,
    LoadStatus,
    WatermarkType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "raw_timescaledb"
SECRET_SCOPE = "wakecap-timescale"
REGISTRY_PATH = "/Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables.yml"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Loader

# COMMAND ----------

# Get notebook context for tracking
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
except Exception:
    notebook_path = "bronze_loader_dimensions"
    run_id = "manual_run"

print(f"Notebook: {notebook_path}")
print(f"Run ID: {run_id}")

# COMMAND ----------

# Initialize credentials from Databricks secrets
credentials = TimescaleDBCredentials.from_databricks_secrets(SECRET_SCOPE)

print(f"Connected to: {credentials.host}:{credentials.port}/{credentials.database}")

# COMMAND ----------

# Initialize the loader
loader = TimescaleDBLoader(
    spark=spark,
    credentials=credentials,
    target_catalog=TARGET_CATALOG,
    target_schema=TARGET_SCHEMA,
    pipeline_id=notebook_path,
    pipeline_run_id=str(run_id)
)

print(f"Loader initialized for {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Dimension Tables

# COMMAND ----------

# Load all dimension tables from registry
print("Starting dimension tables load...")
print("=" * 60)

results = loader.load_all_tables(
    registry_path=REGISTRY_PATH,
    category_filter="dimensions"
)

print("=" * 60)
print(f"Completed loading {len(results)} dimension tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

# Create summary DataFrame with explicit schema to handle empty results or all-null columns
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Define explicit schema to avoid [CANNOT_DETERMINE_TYPE] error when error column is all NULL
summary_schema = StructType([
    StructField("table", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("previous_watermark", StringType(), True),
    StructField("new_watermark", StringType(), True),
    StructField("error", StringType(), True)
])

if results:
    summary_data = [
        Row(
            table=r.table_config.source_table,
            status=r.status.value,
            rows_loaded=r.rows_loaded,
            duration_seconds=round(r.duration_seconds, 2) if r.duration_seconds else 0.0,
            previous_watermark=str(r.previous_watermark) if r.previous_watermark else "N/A",
            new_watermark=str(r.new_watermark) if r.new_watermark else "N/A",
            error=r.error_message[:100] if r.error_message else None
        )
        for r in results
    ]
    summary_df = spark.createDataFrame(summary_data, schema=summary_schema)
    display(summary_df.orderBy("table"))
else:
    print("No tables were loaded - results list is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics

# COMMAND ----------

# Calculate statistics
success_count = sum(1 for r in results if r.status == LoadStatus.SUCCESS)
failed_count = sum(1 for r in results if r.status == LoadStatus.FAILED)
skipped_count = sum(1 for r in results if r.status == LoadStatus.SKIPPED)
total_rows = sum(r.rows_loaded for r in results)
total_duration = sum(r.duration_seconds or 0 for r in results)

print(f"""
Load Statistics:
================
Success:  {success_count}
Failed:   {failed_count}
Skipped:  {skipped_count}
-----------------
Total:    {len(results)}

Total Rows Loaded: {total_rows:,}
Total Duration:    {total_duration:.2f} seconds
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failure Handling

# COMMAND ----------

# Check for failures and raise exception if any
failures = [r for r in results if r.status == LoadStatus.FAILED]

if failures:
    print("FAILED TABLES:")
    print("-" * 60)
    for f in failures:
        print(f"  - {f.table_config.source_table}: {f.error_message}")
    print("-" * 60)

    # Raise exception to fail the job
    raise Exception(f"{len(failures)} dimension tables failed to load. See details above.")
else:
    print("All dimension tables loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data in Target Tables

# COMMAND ----------

# Verify a sample of loaded tables
sample_tables = ["Worker", "Project", "Organization", "Crew", "Device"]

print("Verifying sample tables:")
print("-" * 60)

for table_name in sample_tables:
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.public_{table_name.lower()}"
    try:
        count = spark.table(target_table).count()
        print(f"  {table_name}: {count:,} rows")
    except Exception as e:
        print(f"  {table_name}: ERROR - {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit with Status

# COMMAND ----------

# Return success status
dbutils.notebook.exit(f"SUCCESS: Loaded {success_count} tables, {total_rows:,} rows")
