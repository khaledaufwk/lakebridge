# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Loader - DeviceLocation Tables (Optimized)
# MAGIC
# MAGIC Dedicated loader for the large DeviceLocation hypertables with optimized settings.
# MAGIC
# MAGIC **Tables:**
# MAGIC - DeviceLocation (82M rows, ~50M rows/month)
# MAGIC - DeviceLocationSummary (848K rows, ~400K rows/month)
# MAGIC
# MAGIC **Optimizations:**
# MAGIC - Correct primary keys for MERGE operations
# MAGIC - GeneratedAt watermark (UpdatedAt was NULL)
# MAGIC - Larger batch sizes for hypertables
# MAGIC - Z-ORDER optimization after load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("run_optimize", "yes", ["yes", "no"], "Run OPTIMIZE after load?")
dbutils.widgets.text("max_rows_per_batch", "500000", "Max Rows Per Batch")

load_mode = dbutils.widgets.get("load_mode")
run_optimize = dbutils.widgets.get("run_optimize") == "yes"
max_rows_per_batch = int(dbutils.widgets.get("max_rows_per_batch"))

print(f"Load Mode: {load_mode}")
print(f"Run Optimize: {run_optimize}")
print(f"Max Rows Per Batch: {max_rows_per_batch:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Loader

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

# Get credentials from secrets (pass dbutils explicitly for USER_ISOLATION clusters)
credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)

# Initialize loader with optimized settings
# Note: currentRunId() is not accessible in Python notebooks, so we use None
loader = TimescaleDBLoaderV2(
    spark=spark,
    credentials=credentials,
    target_catalog="wakecap_prod",
    target_schema="raw",
    pipeline_id=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
    pipeline_run_id=None,
    table_prefix="timescale_",
    max_retries=3,
    retry_delay=10  # Longer delay for large tables
)

print("Loader initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Table Configurations
# MAGIC
# MAGIC Using the optimized configurations with correct PKs and watermarks.

# COMMAND ----------

# DeviceLocation - Large hypertable
devicelocation_config = TableConfigV2(
    source_schema="public",
    source_table="DeviceLocation",
    primary_key_columns=["DeviceId", "ProjectId", "ActiveSequance", "InactiveSequance", "GeneratedAt"],
    watermark_column="GeneratedAt",
    watermark_type=WatermarkType.TIMESTAMP,
    watermark_expression=None,  # No GREATEST needed - GeneratedAt is definitive
    category="facts",
    is_full_load=(load_mode == "full"),
    fetch_size=100000,
    batch_size=max_rows_per_batch,
    is_hypertable=True,
    has_geometry=True,
    geometry_columns=["Point"],
    comment="Large hypertable - ~50M rows/month, partitioned by GeneratedAt"
)

# DeviceLocationSummary - Daily aggregated hypertable
devicelocationsummary_config = TableConfigV2(
    source_schema="public",
    source_table="DeviceLocationSummary",
    primary_key_columns=["Day", "DeviceId", "ProjectId"],
    watermark_column="GeneratedAt",
    watermark_type=WatermarkType.TIMESTAMP,
    watermark_expression=None,
    category="facts",
    is_full_load=(load_mode == "full"),
    fetch_size=50000,
    batch_size=200000,
    is_hypertable=True,
    has_geometry=True,
    geometry_columns=["Point", "ConfidenceArea"],
    comment="Daily summary hypertable - ~400K rows/month"
)

tables = [devicelocation_config, devicelocationsummary_config]

print("Table configurations:")
for t in tables:
    print(f"  - {t.source_table}")
    print(f"    PK: {t.primary_key_columns}")
    print(f"    Watermark: {t.watermark_column}")
    print(f"    Batch size: {t.batch_size:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Tables

# COMMAND ----------

from datetime import datetime

results = []
total_start = datetime.now()

for table_config in tables:
    print(f"\n{'='*60}")
    print(f"Loading: {table_config.source_table}")
    print(f"{'='*60}")

    result = loader.load_table(
        table_config,
        force_full_load=(load_mode == "full")
    )
    results.append(result)

    print(f"\nResult:")
    print(f"  Status: {result.status.value}")
    print(f"  Rows loaded: {result.rows_loaded:,}")
    if result.duration_seconds:
        print(f"  Duration: {result.duration_seconds:.1f} seconds")
    if result.previous_watermark:
        print(f"  Previous watermark: {result.previous_watermark}")
    if result.new_watermark:
        print(f"  New watermark: {result.new_watermark}")
    if result.error_message:
        print(f"  Error: {result.error_message}")

total_end = datetime.now()
total_duration = (total_end - total_start).total_seconds()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("LOAD SUMMARY")
print("=" * 60)

success_count = sum(1 for r in results if r.status == LoadStatus.SUCCESS)
failed_count = sum(1 for r in results if r.status == LoadStatus.FAILED)
skipped_count = sum(1 for r in results if r.status == LoadStatus.SKIPPED)
total_rows = sum(r.rows_loaded for r in results)

print(f"\nTables processed: {len(results)}")
print(f"  Success: {success_count}")
print(f"  Failed: {failed_count}")
print(f"  Skipped: {skipped_count}")
print(f"\nTotal rows loaded: {total_rows:,}")
print(f"Total duration: {total_duration:.1f} seconds")

if total_rows > 0 and total_duration > 0:
    print(f"Throughput: {total_rows / total_duration:,.0f} rows/second")

# Show details table
print("\n" + "-" * 60)
print(f"{'Table':<30} {'Status':<10} {'Rows':<15} {'Duration':<10}")
print("-" * 60)
for r in results:
    duration_str = f"{r.duration_seconds:.1f}s" if r.duration_seconds else "N/A"
    print(f"{r.table_config.source_table:<30} {r.status.value:<10} {r.rows_loaded:<15,} {duration_str:<10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Optimization (Optional)
# MAGIC
# MAGIC Apply Z-ORDER clustering and table properties for query performance.

# COMMAND ----------

if run_optimize and success_count > 0:
    print("\nRunning OPTIMIZE with Z-ORDER...")

    optimization_configs = [
        {
            "table": "wakecap_prod.raw.timescale_devicelocation",
            "zorder_columns": ["ProjectId", "GeneratedAt"],
            "target_file_size": "128mb"
        },
        {
            "table": "wakecap_prod.raw.timescale_devicelocationsummary",
            "zorder_columns": ["ProjectId", "Day"],
            "target_file_size": None
        }
    ]

    for config in optimization_configs:
        table_name = config["table"]
        zorder_cols = ", ".join(config["zorder_columns"])

        print(f"\n  Optimizing {table_name}...")

        # Set table properties
        properties = [
            "'delta.autoOptimize.optimizeWrite' = 'true'",
            "'delta.autoOptimize.autoCompact' = 'true'"
        ]
        if config["target_file_size"]:
            properties.append(f"'delta.targetFileSize' = '{config['target_file_size']}'")

        props_sql = ", ".join(properties)
        try:
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props_sql})")
            print(f"    ✓ Properties set")
        except Exception as e:
            print(f"    ✗ Error setting properties: {e}")

        # Run OPTIMIZE with Z-ORDER
        try:
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
            print(f"    ✓ Z-ORDER complete")
        except Exception as e:
            print(f"    ✗ Error during OPTIMIZE: {e}")

    print("\nOptimization complete!")
else:
    if not run_optimize:
        print("\nOptimization skipped (run_optimize=no)")
    else:
        print("\nOptimization skipped (no successful loads)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print("\nVerification - Target table stats:")
print("=" * 60)

for table_config in tables:
    table_name = f"wakecap_prod.raw.timescale_{table_config.source_table.lower()}"

    try:
        # Get row count
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']

        # Get table details
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]

        print(f"\n{table_name}:")
        print(f"  Row count: {count:,}")
        print(f"  Size: {detail['sizeInBytes'] / (1024**3):.2f} GB")
        print(f"  Num files: {detail['numFiles']}")
    except Exception as e:
        print(f"\n{table_name}: Error - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Watermarks

# COMMAND ----------

print("\nWatermark state after load:")
print("=" * 60)

for table_config in tables:
    df = spark.sql(f"""
        SELECT
            source_table,
            watermark_column,
            last_watermark_timestamp,
            last_load_status,
            last_load_row_count,
            last_load_end_time
        FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE source_system = 'timescaledb'
          AND source_schema = 'public'
          AND source_table = '{table_config.source_table}'
    """)

    print(f"\n{table_config.source_table}:")
    df.show(truncate=False)
