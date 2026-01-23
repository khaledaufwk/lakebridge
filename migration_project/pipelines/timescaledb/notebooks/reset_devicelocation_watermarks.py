# Databricks notebook source
# MAGIC %md
# MAGIC # Reset DeviceLocation Watermarks
# MAGIC
# MAGIC This notebook resets the watermark entries for DeviceLocation and DeviceLocationSummary
# MAGIC to enable fresh incremental loading with the correct configuration:
# MAGIC
# MAGIC **Changes Applied:**
# MAGIC - DeviceLocation: Watermark changed from UpdatedAt (all NULL) to GeneratedAt
# MAGIC - DeviceLocationSummary: Watermark changed from UpdatedAt to GeneratedAt
# MAGIC - Primary keys updated to match actual source table PKs
# MAGIC
# MAGIC **Run this BEFORE running the bronze loader for these tables.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Widget to control whether to drop existing tables - MUST BE AT TOP
dbutils.widgets.dropdown("drop_existing_tables", "no", ["yes", "no"], "Drop existing tables?")
drop_tables = dbutils.widgets.get("drop_existing_tables") == "yes"

print(f"Drop existing tables: {drop_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

WATERMARK_TABLE = "wakecap_prod.migration._timescaledb_watermarks"
CATALOG = "wakecap_prod"
SCHEMA = "raw"

tables_to_reset = [
    {
        "source_table": "DeviceLocation",
        "new_watermark_column": "GeneratedAt",
        "new_watermark_expression": None,  # No GREATEST needed
        "target_table": "timescale_devicelocation"
    },
    {
        "source_table": "DeviceLocationSummary",
        "new_watermark_column": "GeneratedAt",
        "new_watermark_expression": None,
        "target_table": "timescale_devicelocationsummary"
    }
]

print(f"Tables to reset: {[t['source_table'] for t in tables_to_reset]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Current Watermark State

# COMMAND ----------

print("Current watermark entries:")
print("=" * 80)

for table_info in tables_to_reset:
    source_table = table_info["source_table"]

    df = spark.sql(f"""
        SELECT
            source_table,
            watermark_column,
            watermark_expression,
            last_watermark_value,
            last_watermark_timestamp,
            last_load_status,
            last_load_row_count,
            last_load_end_time
        FROM {WATERMARK_TABLE}
        WHERE source_system = 'timescaledb'
          AND source_schema = 'public'
          AND source_table = '{source_table}'
    """)

    if df.count() > 0:
        print(f"\n{source_table}:")
        df.show(truncate=False)
    else:
        print(f"\n{source_table}: No existing watermark entry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Watermarks
# MAGIC
# MAGIC Delete existing watermark entries so the loader treats these as new tables
# MAGIC and performs a full load with the correct configuration.

# COMMAND ----------

for table_info in tables_to_reset:
    source_table = table_info["source_table"]
    new_wm_col = table_info["new_watermark_column"]

    print(f"\nResetting watermark for {source_table}...")

    # Delete existing watermark entry
    spark.sql(f"""
        DELETE FROM {WATERMARK_TABLE}
        WHERE source_system = 'timescaledb'
          AND source_schema = 'public'
          AND source_table = '{source_table}'
    """)

    print(f"  Deleted existing watermark entry")
    print(f"  New watermark column will be: {new_wm_col}")

print("\n" + "=" * 80)
print("Watermark reset complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Reset

# COMMAND ----------

print("Verification - watermark entries after reset:")
print("=" * 80)

for table_info in tables_to_reset:
    source_table = table_info["source_table"]

    count = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {WATERMARK_TABLE}
        WHERE source_system = 'timescaledb'
          AND source_schema = 'public'
          AND source_table = '{source_table}'
    """).collect()[0]['cnt']

    if count == 0:
        print(f"✓ {source_table}: Watermark cleared (will do full load)")
    else:
        print(f"✗ {source_table}: Watermark still exists!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Target Tables
# MAGIC
# MAGIC Optionally drop and recreate target tables to ensure clean state with correct PKs.

# COMMAND ----------

if drop_tables:
    print("Dropping existing target tables...")
    for table_info in tables_to_reset:
        target_table = f"{CATALOG}.{SCHEMA}.{table_info['target_table']}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")
            print(f"  Dropped: {target_table}")
        except Exception as e:
            print(f"  Error dropping {target_table}: {e}")
else:
    print("Keeping existing tables (data will be merged with correct PKs)")
    print("Note: If tables have incorrect data from wrong PKs, consider setting drop_existing_tables=yes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC After running this notebook:
# MAGIC
# MAGIC 1. Run the bronze loader job with these specific tables:
# MAGIC    ```python
# MAGIC    loader.load_all_tables(
# MAGIC        registry_path="config/timescaledb_tables_v2.yml",
# MAGIC        table_filter=["DeviceLocation", "DeviceLocationSummary"],
# MAGIC        force_full_load=True
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 2. After loading, run the optimization notebook:
# MAGIC    `/Workspace/migration_project/pipelines/timescaledb/notebooks/optimize_devicelocation`
# MAGIC
# MAGIC 3. Verify the data loaded correctly with proper row counts
