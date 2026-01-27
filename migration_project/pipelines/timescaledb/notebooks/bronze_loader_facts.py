# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader - Fact Tables (DEPRECATED)
# MAGIC
# MAGIC **⚠️ DEPRECATED:** Use `bronze_loader_optimized` instead for all table loading.
# MAGIC This notebook uses the old v1 loader. The main job now uses bronze_loader_optimized with v2 config.
# MAGIC
# MAGIC Loads fact tables (TimescaleDB hypertables) from TimescaleDB using incremental watermark-based extraction.
# MAGIC
# MAGIC **Tables Loaded:** 9 fact tables (ordered by size)
# MAGIC 1. FactProgress (small)
# MAGIC 2. FactWorkersTasks (small)
# MAGIC 3. FactWeatherObservations (medium)
# MAGIC 4. FactWorkersShifts (medium)
# MAGIC 5. FactWorkersShiftsCombined (medium)
# MAGIC 6. FactReportedAttendance (large)
# MAGIC 7. FactWorkersContacts (large)
# MAGIC 8. FactWorkersHistory (very large)
# MAGIC 9. FactObservations (very large - loaded last)
# MAGIC
# MAGIC **Watermark Strategy:** All tables use `WatermarkUTC` column
# MAGIC **Partitioning:** Fact tables are partitioned by date columns for optimal query performance
# MAGIC
# MAGIC **Schedule:**
# MAGIC - Real-time (5 min): FactObservations
# MAGIC - Hourly: All other fact tables

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

import sys
import os
from datetime import datetime

# Add source directory to path
sys.path.append("/Workspace/migration_project/pipelines/timescaledb/src")

from timescaledb_loader import (
    TimescaleDBLoader,
    TimescaleDBCredentials,
    LoadStatus,
    WatermarkType,
    TableConfig
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

# Fact tables load order (smallest to largest)
FACT_TABLES_ORDER = [
    "FactProgress",
    "FactWorkersTasks",
    "FactWeatherObservations",
    "FactWorkersShifts",
    "FactWorkersShiftsCombined",
    "FactReportedAttendance",
    "FactWorkersContacts",
    "FactWorkersHistory",
    "FactObservations"  # Largest table - loaded last
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters

# COMMAND ----------

# Create widgets for parameterized runs
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.multiselect("tables", "ALL", ["ALL"] + FACT_TABLES_ORDER, "Tables to Load")
dbutils.widgets.text("batch_size", "100000", "Batch Size")

# Get widget values
load_mode = dbutils.widgets.get("load_mode")
selected_tables = dbutils.widgets.get("tables")
batch_size = int(dbutils.widgets.get("batch_size"))

force_full_load = (load_mode == "full")
table_filter = None if "ALL" in selected_tables else selected_tables.split(",")

print(f"Load Mode: {load_mode}")
print(f"Tables: {selected_tables}")
print(f"Batch Size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Loader

# COMMAND ----------

# Get notebook context for tracking
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
except Exception:
    notebook_path = "bronze_loader_facts"
    run_id = "manual_run"

print(f"Notebook: {notebook_path}")
print(f"Run ID: {run_id}")
print(f"Start Time: {datetime.now()}")

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
# MAGIC ## Load Fact Tables

# COMMAND ----------

# Load fact tables in the specified order
print("Starting fact tables load...")
print("=" * 60)
print(f"Load Order: {FACT_TABLES_ORDER}")
print(f"Force Full Load: {force_full_load}")
print("=" * 60)

results = loader.load_all_tables(
    registry_path=REGISTRY_PATH,
    category_filter="facts",
    table_filter=table_filter if table_filter else FACT_TABLES_ORDER,
    force_full_load=force_full_load
)

print("=" * 60)
print(f"Completed loading {len(results)} fact tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

# Create detailed summary DataFrame with explicit schema to handle empty results or all-null columns
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Define explicit schema to avoid [CANNOT_DETERMINE_TYPE] error when error column is all NULL
summary_schema = StructType([
    StructField("table", StringType(), True),
    StructField("category", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("rows_per_second", DoubleType(), True),
    StructField("partition_column", StringType(), True),
    StructField("previous_watermark", StringType(), True),
    StructField("new_watermark", StringType(), True),
    StructField("error", StringType(), True)
])

if results:
    summary_data = [
        Row(
            table=r.table_config.source_table,
            category=r.table_config.category,
            status=r.status.value,
            rows_loaded=r.rows_loaded,
            duration_seconds=round(r.duration_seconds, 2) if r.duration_seconds else 0.0,
            rows_per_second=round(r.rows_loaded / r.duration_seconds, 0) if r.duration_seconds and r.duration_seconds > 0 else 0,
            partition_column=r.table_config.partition_column or "N/A",
            previous_watermark=str(r.previous_watermark)[:25] if r.previous_watermark else "N/A",
            new_watermark=str(r.new_watermark)[:25] if r.new_watermark else "N/A",
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
# MAGIC ## Performance Statistics

# COMMAND ----------

# Calculate detailed statistics
success_count = sum(1 for r in results if r.status == LoadStatus.SUCCESS)
failed_count = sum(1 for r in results if r.status == LoadStatus.FAILED)
skipped_count = sum(1 for r in results if r.status == LoadStatus.SKIPPED)
total_rows = sum(r.rows_loaded for r in results)
total_duration = sum(r.duration_seconds or 0 for r in results)
avg_rows_per_sec = total_rows / total_duration if total_duration > 0 else 0

# Find largest and slowest
largest_load = max(results, key=lambda r: r.rows_loaded) if results else None
slowest_load = max(results, key=lambda r: r.duration_seconds or 0) if results else None

print(f"""
=== FACT TABLES LOAD STATISTICS ===

Status Summary:
  Success:  {success_count}
  Failed:   {failed_count}
  Skipped:  {skipped_count}
  -----------------
  Total:    {len(results)}

Performance:
  Total Rows Loaded:     {total_rows:,}
  Total Duration:        {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)
  Avg Throughput:        {avg_rows_per_sec:,.0f} rows/second

Largest Load:
  Table: {largest_load.table_config.source_table if largest_load else 'N/A'}
  Rows:  {largest_load.rows_loaded:,} if largest_load else 0

Slowest Load:
  Table:    {slowest_load.table_config.source_table if slowest_load else 'N/A'}
  Duration: {slowest_load.duration_seconds:.2f}s if slowest_load and slowest_load.duration_seconds else 0
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
        print(f"  - {f.table_config.source_table}")
        print(f"    Error: {f.error_message}")
        print()
    print("-" * 60)

    # Raise exception to fail the job
    raise Exception(f"{len(failures)} fact tables failed to load. See details above.")
else:
    print("All fact tables loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Partitioning

# COMMAND ----------

# Verify partitioning on fact tables
partitioned_tables = {
    "FactWorkersHistory": "LocalDate",
    "FactWorkersShifts": "ShiftLocalDate",
    "FactWorkersShiftsCombined": "ShiftLocalDate",
    "FactReportedAttendance": "ShiftLocalDate",
    "FactObservations": "ObservationTime",
    "FactProgress": "ProgressDate",
    "FactWorkersContacts": "LocalDate"
}

print("Verifying table partitions:")
print("-" * 60)

for table_name, expected_partition in partitioned_tables.items():
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.public_{table_name.lower()}"
    try:
        # Get table details
        detail_df = spark.sql(f"DESCRIBE DETAIL {target_table}")
        partitions = detail_df.select("partitionColumns").first()

        if partitions and partitions[0]:
            print(f"  {table_name}: Partitioned by {partitions[0]}")
        else:
            print(f"  {table_name}: Not partitioned (expected: {expected_partition})")
    except Exception as e:
        print(f"  {table_name}: ERROR - {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Run basic data quality checks on loaded fact tables
print("Data Quality Summary:")
print("-" * 60)

for r in results:
    if r.status == LoadStatus.SUCCESS and r.rows_loaded > 0:
        table_name = r.table_config.source_table
        target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.public_{table_name.lower()}"

        try:
            df = spark.table(target_table)

            # Count nulls in primary key
            pk_col = r.table_config.primary_key_columns[0]
            null_pk_count = df.filter(f"{pk_col} IS NULL").count()

            # Check metadata columns
            has_loaded_at = "_loaded_at" in df.columns
            has_source_system = "_source_system" in df.columns

            status = "OK" if null_pk_count == 0 and has_loaded_at else "WARN"
            print(f"  {table_name}: {status}")
            print(f"    - Rows: {df.count():,}")
            print(f"    - Null PKs: {null_pk_count}")
            print(f"    - Has metadata: {has_loaded_at and has_source_system}")

        except Exception as e:
            print(f"  {table_name}: ERROR - {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit with Status

# COMMAND ----------

# Final summary
end_time = datetime.now()
print(f"End Time: {end_time}")

# Return success status
exit_message = f"SUCCESS: Loaded {success_count} fact tables, {total_rows:,} rows in {total_duration:.1f}s"
print(f"\n{exit_message}")

dbutils.notebook.exit(exit_message)
