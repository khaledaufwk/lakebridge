# Databricks notebook source
print("Testing load_table method...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"Credentials loaded: {credentials.host}")

# COMMAND ----------

loader = TimescaleDBLoaderV2(
    spark=spark,
    credentials=credentials,
    target_catalog="wakecap_prod",
    target_schema="raw",
    pipeline_id="test_load_table",
    pipeline_run_id=None,
    table_prefix="test_",  # Use test prefix to not affect real tables
    max_retries=1,
    retry_delay=5
)
print("Loader initialized!")

# COMMAND ----------

# Create a minimal config for testing - use DeviceLocationSummary since it's smaller
print("Creating test config for DeviceLocationSummary...")
config = TableConfigV2(
    source_schema="public",
    source_table="DeviceLocationSummary",
    primary_key_columns=["Day", "DeviceId", "ProjectId"],
    watermark_column="GeneratedAt",
    watermark_type=WatermarkType.TIMESTAMP,
    watermark_expression=None,
    category="facts",
    is_full_load=True,
    fetch_size=1000,
    batch_size=5000,  # Small batch for testing
    is_hypertable=True,
    has_geometry=True,
    geometry_columns=["Point", "ConfidenceArea"],
    comment="Test load"
)
print(f"Config: {config.source_table}")

# COMMAND ----------

print("Calling load_table...")
try:
    result = loader.load_table(config, force_full_load=True)
    print(f"Status: {result.status}")
    print(f"Rows: {result.rows_loaded}")
    if result.error_message:
        print(f"Error: {result.error_message}")
except Exception as e:
    print(f"EXCEPTION: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

print("Done!")
