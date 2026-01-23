# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader - Observations Database
# MAGIC
# MAGIC Loads observation data from TimescaleDB wakecap_observation database.
# MAGIC
# MAGIC **Source:** TimescaleDB (wakecap_observation database)
# MAGIC **Target:** wakecap_prod.raw (Bronze layer)
# MAGIC **Prefix:** observation_
# MAGIC
# MAGIC Tables:
# MAGIC - Observation (~1.4M rows) - main incidents with Location geometry
# MAGIC - TrackerSteps (~5.6M rows) - tracker step logs
# MAGIC - ObservationTracker (~595K rows) - tracker metadata
# MAGIC - ObservationUpdate (~405K rows) - updates/comments
# MAGIC - ObservationUpdateAttachment (~3.2K rows) - attachments
# MAGIC - Settings (~13 rows) - project settings

# COMMAND ----------

# MAGIC %pip install /Volumes/wakecap_prod/migration/libs/timescaledb_loader-2.0.0-py3-none-any.whl --quiet

# COMMAND ----------

import os
from datetime import datetime

from timescaledb_loader import (
    TimescaleDBLoaderV2,
    TimescaleDBCredentials,
    LoadStatus,
    WatermarkType,
    TableConfigV2
)
import yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "raw"
TABLE_PREFIX = "observation_"
SECRET_SCOPE = "wakecap-observation"
REGISTRY_PATH = "/Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables_observation.yml"

# Ensure target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Table prefix: {TABLE_PREFIX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters

# COMMAND ----------

# Create widgets for parameterized runs
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("category", "ALL", ["ALL", "dimensions", "facts"], "Category")
dbutils.widgets.text("batch_size", "100000", "Batch Size")
dbutils.widgets.text("fetch_size", "50000", "JDBC Fetch Size")
dbutils.widgets.text("max_tables", "0", "Max Tables (0=all)")

# Get widget values
load_mode = dbutils.widgets.get("load_mode")
category = dbutils.widgets.get("category")
batch_size = int(dbutils.widgets.get("batch_size"))
fetch_size = int(dbutils.widgets.get("fetch_size"))
max_tables = int(dbutils.widgets.get("max_tables"))

force_full_load = (load_mode == "full")
category_filter = None if category == "ALL" else category

print(f"Load Mode: {load_mode}")
print(f"Category: {category}")
print(f"Batch Size: {batch_size:,}")
print(f"Fetch Size: {fetch_size:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Loader

# COMMAND ----------

# Get notebook context for tracking
# NOTE: currentRunId() is not accessible in Python, use timestamp instead
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
except Exception:
    notebook_path = "bronze_loader_observation"

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Notebook: {notebook_path}")
print(f"Run ID: {run_id}")
print(f"Start Time: {datetime.now()}")

# COMMAND ----------

# Initialize credentials from Databricks secrets
# Pass dbutils explicitly for USER_ISOLATION cluster compatibility
credentials = TimescaleDBCredentials.from_databricks_secrets(SECRET_SCOPE, dbutils=dbutils)
print(f"Connected to: {credentials.host}:{credentials.port}/{credentials.database}")

# COMMAND ----------

# Initialize the optimized loader
loader = TimescaleDBLoaderV2(
    spark=spark,
    credentials=credentials,
    target_catalog=TARGET_CATALOG,
    target_schema=TARGET_SCHEMA,
    pipeline_id=notebook_path,
    pipeline_run_id=str(run_id),
    table_prefix=TABLE_PREFIX,
    max_retries=3,
    retry_delay=10
)

print(f"Loader initialized for {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Table prefix: {TABLE_PREFIX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Table Registry

# COMMAND ----------

# Load and display registry info
with open(REGISTRY_PATH, 'r') as f:
    registry = yaml.safe_load(f)

defaults = registry.get("defaults", {})
tables_config = registry.get("tables", [])
excluded_tables = registry.get("excluded_tables", [])

print(f"Registry Version: {registry.get('registry_version', 'unknown')}")
print(f"Source Database: {registry.get('source_database', 'unknown')}")
print(f"Total tables: {len(tables_config)}")
print(f"Excluded tables: {len(excluded_tables)}")

# Show tables to load
print("\nTables to Load:")
for t in tables_config:
    comment = t.get('comment', '')[:50] if t.get('comment') else ''
    print(f"  - {t['source_table']} ({t.get('category', 'unknown')}) {comment}")

# Show excluded tables
print("\nExcluded Tables:")
for t in excluded_tables:
    print(f"  - {t['source_table']}: {t.get('reason', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Tables

# COMMAND ----------

# Load tables
print("=" * 70)
print("Starting observation table load...")
print("=" * 70)

results = loader.load_all_tables(
    registry_path=REGISTRY_PATH,
    category_filter=category_filter,
    force_full_load=force_full_load
)

# Apply max_tables limit if specified
if max_tables > 0:
    results = results[:max_tables]

print("=" * 70)
print(f"Completed loading {len(results)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

# Create detailed summary with explicit schema to handle empty results or all-null columns
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

# Define explicit schema to avoid [CANNOT_DETERMINE_TYPE] error
summary_schema = StructType([
    StructField("table", StringType(), True),
    StructField("category", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("retries", IntegerType(), True),
    StructField("watermark_expr", StringType(), True),
    StructField("has_geometry", StringType(), True),
    StructField("previous_wm", StringType(), True),
    StructField("new_wm", StringType(), True),
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
            retries=r.retry_count,
            watermark_expr="GREATEST" if r.table_config.watermark_expression else "Single",
            has_geometry="Yes" if r.table_config.has_geometry else "No",
            previous_wm=str(r.previous_watermark)[:20] if r.previous_watermark else "N/A",
            new_wm=str(r.new_watermark)[:20] if r.new_watermark else "N/A",
            error=r.error_message[:80] if r.error_message else None
        )
        for r in results
    ]
    summary_df = spark.createDataFrame(summary_data, schema=summary_schema)
    display(summary_df.orderBy("category", "table"))
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
total_retries = sum(r.retry_count for r in results)
tables_with_greatest = sum(1 for r in results if r.table_config.watermark_expression)
tables_with_geometry = sum(1 for r in results if r.table_config.has_geometry)

print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║              OBSERVATION LOAD STATISTICS                              ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                        ║
║  Status Summary:                                                       ║
║    ✓ Success:  {success_count:>4}                                                    ║
║    ✗ Failed:   {failed_count:>4}                                                    ║
║    ○ Skipped:  {skipped_count:>4}                                                    ║
║    ─────────────────                                                   ║
║    Total:      {len(results):>4}                                                    ║
║                                                                        ║
║  Performance:                                                          ║
║    Total Rows Loaded:     {total_rows:>15,}                            ║
║    Total Duration:        {total_duration:>12.2f} sec ({total_duration/60:.1f} min)       ║
║    Avg Throughput:        {total_rows/total_duration if total_duration > 0 else 0:>12,.0f} rows/sec           ║
║    Total Retries:         {total_retries:>12}                                   ║
║                                                                        ║
║  Optimizations Applied:                                                ║
║    Tables using GREATEST:  {tables_with_greatest:>4}                                   ║
║    Tables with geometry:   {tables_with_geometry:>4}                                   ║
║                                                                        ║
╚══════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failure Analysis

# COMMAND ----------

# Check for failures
failures = [r for r in results if r.status == LoadStatus.FAILED]

if failures:
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║                      FAILED TABLES                                    ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    for f in failures:
        print(f"║")
        print(f"║  Table: {f.table_config.source_table}")
        print(f"║  Category: {f.table_config.category}")
        print(f"║  Retries: {f.retry_count}")
        print(f"║  Error: {f.error_message[:60]}...")
        print(f"║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print(f"\n⚠️  WARNING: {len(failures)} tables failed to load")
else:
    print("✓ All tables loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Sample Tables

# COMMAND ----------

# Verify sample tables
sample_tables = [
    "observation_observation",
    "observation_observationtracker",
    "observation_observationupdate",
    "observation_trackersteps",
    "observation_settings"
]

print("Verifying sample tables:")
print("-" * 60)

for table_name in sample_tables:
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"
    try:
        count = spark.table(target_table).count()
        print(f"  ✓ {table_name}: {count:,} rows")
    except Exception as e:
        print(f"  ✗ {table_name}: {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark Summary

# COMMAND ----------

# Show watermark status for observation tables
print("Watermark Table Summary (Observation):")
print("-" * 60)

try:
    wm_df = spark.sql(f"""
        SELECT
            source_table,
            watermark_column,
            CASE WHEN watermark_expression IS NOT NULL THEN 'GREATEST' ELSE 'Single' END as wm_type,
            last_load_status,
            last_load_row_count,
            last_watermark_timestamp,
            last_load_end_time
        FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE source_system = 'timescaledb'
          AND source_table IN ('Observation', 'ObservationTracker', 'ObservationUpdate',
                               'ObservationUpdateAttachment', 'TrackerSteps', 'Settings')
        ORDER BY last_load_end_time DESC
    """)
    display(wm_df)
except Exception as e:
    print(f"Could not query watermark table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit

# COMMAND ----------

# Final summary
end_time = datetime.now()
print(f"End Time: {end_time}")

# Exit message
if failed_count > 0:
    exit_message = f"PARTIAL: Loaded {success_count}/{len(results)} observation tables ({total_rows:,} rows). {failed_count} failed."
else:
    exit_message = f"SUCCESS: Loaded {success_count} observation tables ({total_rows:,} rows) in {total_duration:.1f}s"

print(f"\n{exit_message}")
dbutils.notebook.exit(exit_message)
