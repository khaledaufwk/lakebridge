# Databricks notebook source
print("Testing exact same initialization as main notebook...")

# COMMAND ----------

# Same widgets as main notebook
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

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

# Same exact initialization as main notebook
print("Getting credentials...")
credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"  Credentials loaded!")

# COMMAND ----------

print("Getting notebook context...")
pipeline_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"  Pipeline ID: {pipeline_id}")

run_id_defined = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
print(f"  Run ID defined: {run_id_defined}")

if run_id_defined:
    pipeline_run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
else:
    pipeline_run_id = None
print(f"  Pipeline Run ID: {pipeline_run_id}")

# COMMAND ----------

print("Initializing loader...")
loader = TimescaleDBLoaderV2(
    spark=spark,
    credentials=credentials,
    target_catalog="wakecap_prod",
    target_schema="raw",
    pipeline_id=pipeline_id,
    pipeline_run_id=pipeline_run_id,
    table_prefix="timescale_",
    max_retries=3,
    retry_delay=10
)
print("Loader initialized!")

# COMMAND ----------

print("Creating DeviceLocation config...")
devicelocation_config = TableConfigV2(
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
    comment="Large hypertable"
)
print(f"  DeviceLocation config created!")

# COMMAND ----------

print("Creating DeviceLocationSummary config...")
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
    comment="Daily summary"
)
print(f"  DeviceLocationSummary config created!")

# COMMAND ----------

tables = [devicelocation_config, devicelocationsummary_config]
print(f"Created {len(tables)} table configs")

# COMMAND ----------

print("\n" + "="*60)
print("ALL INITIALIZATION TESTS PASSED!")
print("="*60)
