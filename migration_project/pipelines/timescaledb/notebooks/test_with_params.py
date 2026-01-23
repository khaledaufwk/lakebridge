# Databricks notebook source
print("Testing notebook with parameters...")

# COMMAND ----------

# Create widgets just like the main notebook
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("run_optimize", "yes", ["yes", "no"], "Run OPTIMIZE after load?")
dbutils.widgets.text("max_rows_per_batch", "500000", "Max Rows Per Batch")

# COMMAND ----------

# Read parameters
load_mode = dbutils.widgets.get("load_mode")
run_optimize = dbutils.widgets.get("run_optimize") == "yes"
max_rows_per_batch = int(dbutils.widgets.get("max_rows_per_batch"))

print(f"Load Mode: {load_mode}")
print(f"Run Optimize: {run_optimize}")
print(f"Max Rows Per Batch: {max_rows_per_batch:,}")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

print("Loading credentials...")
credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"  Credentials loaded: host={credentials.host}")

# COMMAND ----------

print("Initializing loader...")
loader = TimescaleDBLoaderV2(
    spark=spark,
    credentials=credentials,
    target_catalog="wakecap_prod",
    target_schema="raw",
    pipeline_id="test_params",
    pipeline_run_id=None,
    table_prefix="timescale_",
    max_retries=3,
    retry_delay=10
)
print("  Loader initialized!")

# COMMAND ----------

print("Creating config...")
config = TableConfigV2(
    source_schema="public",
    source_table="DeviceLocation",
    primary_key_columns=["DeviceId", "ProjectId", "ActiveSequance", "InactiveSequance", "GeneratedAt"],
    watermark_column="GeneratedAt",
    watermark_type=WatermarkType.TIMESTAMP,
    watermark_expression=None,
    category="facts",
    is_full_load=(load_mode == "full"),
    fetch_size=100000,
    batch_size=max_rows_per_batch,
    is_hypertable=True,
    has_geometry=True,
    geometry_columns=["Point"],
    comment="Test"
)
print(f"  Config created: {config.source_table}")
print(f"  is_full_load: {config.is_full_load}")

# COMMAND ----------

print("\n" + "="*60)
print("ALL TESTS PASSED!")
print("="*60)
