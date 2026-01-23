# Databricks notebook source
# MAGIC %md
# MAGIC # DeviceLocation Table Optimization
# MAGIC
# MAGIC This notebook applies Delta Lake optimizations to DeviceLocation and DeviceLocationSummary tables.
# MAGIC
# MAGIC **Optimizations Applied:**
# MAGIC - Z-ORDER clustering for query performance
# MAGIC - Auto-optimize and auto-compact settings
# MAGIC - Target file size for large tables
# MAGIC
# MAGIC **Run after:** Initial data load or periodically for maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

catalog = "wakecap_prod"
schema = "raw"

tables_config = [
    {
        "table": "timescale_devicelocation",
        "zorder_columns": ["ProjectId", "GeneratedAt"],
        "target_file_size": "128mb",
        "description": "Large hypertable - 82M rows, ~50M rows/month"
    },
    {
        "table": "timescale_devicelocationsummary",
        "zorder_columns": ["ProjectId", "Day"],
        "target_file_size": None,  # Use default
        "description": "Daily summary hypertable - 848K rows"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Table Properties

# COMMAND ----------

for config in tables_config:
    table_name = f"{catalog}.{schema}.{config['table']}"
    print(f"\n{'='*60}")
    print(f"Optimizing: {table_name}")
    print(f"Description: {config['description']}")
    print(f"{'='*60}")

    # Check if table exists
    try:
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
        print(f"Current row count: {row_count:,}")
    except Exception as e:
        print(f"Table not found or error: {e}")
        continue

    # Set table properties
    properties = [
        "'delta.autoOptimize.optimizeWrite' = 'true'",
        "'delta.autoOptimize.autoCompact' = 'true'"
    ]

    if config['target_file_size']:
        properties.append(f"'delta.targetFileSize' = '{config['target_file_size']}'")

    props_sql = ", ".join(properties)
    alter_sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props_sql})"

    print(f"\nApplying properties...")
    print(f"SQL: {alter_sql}")
    spark.sql(alter_sql)
    print("Properties applied successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run OPTIMIZE with Z-ORDER
# MAGIC
# MAGIC This operation reorganizes data files for optimal query performance.
# MAGIC Z-ORDER colocates related data in the same files for faster filtering.

# COMMAND ----------

for config in tables_config:
    table_name = f"{catalog}.{schema}.{config['table']}"
    zorder_cols = ", ".join(config['zorder_columns'])

    print(f"\n{'='*60}")
    print(f"Z-ORDERing: {table_name}")
    print(f"Columns: {zorder_cols}")
    print(f"{'='*60}")

    optimize_sql = f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})"
    print(f"SQL: {optimize_sql}")

    try:
        result = spark.sql(optimize_sql)
        metrics = result.collect()
        if metrics:
            print(f"Optimization metrics: {metrics}")
        print("Z-ORDER completed successfully.")
    except Exception as e:
        print(f"Error during optimization: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Optimization

# COMMAND ----------

for config in tables_config:
    table_name = f"{catalog}.{schema}.{config['table']}"

    print(f"\n{'='*60}")
    print(f"Verification: {table_name}")
    print(f"{'='*60}")

    # Get table details
    detail_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
    detail = detail_df.collect()[0]

    print(f"Location: {detail['location']}")
    print(f"Num files: {detail['numFiles']}")
    print(f"Size (bytes): {detail['sizeInBytes']:,}")
    print(f"Size (GB): {detail['sizeInBytes'] / (1024**3):.2f}")

    # Get table properties
    props_df = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
    print("\nTable Properties:")
    for row in props_df.collect():
        if 'delta.' in row['key']:
            print(f"  {row['key']}: {row['value']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Optimizations applied:
# MAGIC
# MAGIC | Table | Z-ORDER Columns | Auto-Optimize | Target File Size |
# MAGIC |-------|-----------------|---------------|------------------|
# MAGIC | timescale_devicelocation | ProjectId, GeneratedAt | Enabled | 128MB |
# MAGIC | timescale_devicelocationsummary | ProjectId, Day | Enabled | Default |
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Faster queries filtering by ProjectId
# MAGIC - Improved time-range queries on GeneratedAt/Day
# MAGIC - Automatic file compaction on writes
# MAGIC - Optimized write performance
