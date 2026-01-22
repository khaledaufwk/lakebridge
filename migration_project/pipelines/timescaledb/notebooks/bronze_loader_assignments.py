# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader - Assignment Tables
# MAGIC
# MAGIC Loads assignment/bridge tables from TimescaleDB using incremental watermark-based extraction.
# MAGIC
# MAGIC **Tables Loaded:** 11 assignment tables
# MAGIC - CrewAssignments, TradeAssignments, WorkshiftAssignments
# MAGIC - DeviceAssignments, ManagerAssignments, ManagerAssignmentSnapshots
# MAGIC - LocationGroupAssignments, WorkerLocationAssignments
# MAGIC - ProjectAssignments, WorkerRoleAssignments, LocationAssignments
# MAGIC
# MAGIC **Watermark Strategy:** All tables use `WatermarkUTC` column for incremental loading
# MAGIC
# MAGIC **Schedule:** Hourly

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
    notebook_path = "bronze_loader_assignments"
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
# MAGIC ## Load Assignment Tables

# COMMAND ----------

# Load all assignment tables from registry
print("Starting assignment tables load...")
print("=" * 60)

results = loader.load_all_tables(
    registry_path=REGISTRY_PATH,
    category_filter="assignments"
)

print("=" * 60)
print(f"Completed loading {len(results)} assignment tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

# Create summary DataFrame
from pyspark.sql import Row

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

summary_df = spark.createDataFrame(summary_data)
display(summary_df.orderBy("table"))

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
    raise Exception(f"{len(failures)} assignment tables failed to load. See details above.")
else:
    print("All assignment tables loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data in Target Tables

# COMMAND ----------

# Verify all assignment tables
assignment_tables = [
    "CrewAssignments",
    "TradeAssignments",
    "WorkshiftAssignments",
    "DeviceAssignments",
    "ManagerAssignments",
    "ProjectAssignments"
]

print("Verifying assignment tables:")
print("-" * 60)

for table_name in assignment_tables:
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
