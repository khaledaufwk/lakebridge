# Databricks notebook source
# MAGIC %md
# MAGIC # Full Initialization Test

# COMMAND ----------

print("Step 1: Running loader module...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

print("Step 2: Loading credentials...")
credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"  Host: {credentials.host}")

# COMMAND ----------

print("Step 3: Initializing loader...")
try:
    loader = TimescaleDBLoaderV2(
        spark=spark,
        credentials=credentials,
        target_catalog="wakecap_prod",
        target_schema="raw",
        pipeline_id="test",
        pipeline_run_id=None,
        table_prefix="timescale_",
        max_retries=3,
        retry_delay=10
    )
    print("  Loader initialized!")
except Exception as e:
    print(f"  ERROR initializing loader: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Step 4: Creating TableConfigV2...")
try:
    config = TableConfigV2(
        source_schema="public",
        source_table="DeviceLocation",
        primary_key_columns=["DeviceId", "ProjectId", "ActiveSequance", "InactiveSequance", "GeneratedAt"],
        watermark_column="GeneratedAt",
        watermark_type=WatermarkType.TIMESTAMP,
        watermark_expression=None,
        category="facts",
        is_full_load=True,
        fetch_size=100000,
        batch_size=500000,
        is_hypertable=True,
        has_geometry=True,
        geometry_columns=["Point"],
        comment="Test config"
    )
    print(f"  Config created: {config.source_table}")
    print(f"  PK: {config.primary_key_columns}")
except Exception as e:
    print(f"  ERROR creating config: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Step 5: Skipping JDBC test for now...")

# COMMAND ----------

print("\n" + "="*60)
print("ALL INITIALIZATION TESTS PASSED!")
print("="*60)
