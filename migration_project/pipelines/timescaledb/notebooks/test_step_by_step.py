# Databricks notebook source
print("Step 1: Widgets...")
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("run_optimize", "yes", ["yes", "no"], "Run OPTIMIZE after load?")
dbutils.widgets.text("max_rows_per_batch", "500000", "Max Rows Per Batch")

load_mode = dbutils.widgets.get("load_mode")
run_optimize = dbutils.widgets.get("run_optimize") == "yes"
max_rows_per_batch = int(dbutils.widgets.get("max_rows_per_batch"))
print(f"  load_mode = {load_mode}")
print(f"  run_optimize = {run_optimize}")
print(f"  max_rows_per_batch = {max_rows_per_batch:,}")

# COMMAND ----------

print("Step 2: Run loader module...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

print("Step 3: Credentials...")
credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"  Loaded")

# COMMAND ----------

print("Step 4: Notebook path...")
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    print(f"  Path: {notebook_path}")
except Exception as e:
    print(f"  ERROR: {e}")
    notebook_path = "unknown"

# COMMAND ----------

print("Step 5: Run ID...")
try:
    run_id_defined = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
    print(f"  isDefined: {run_id_defined}")
    if run_id_defined:
        run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
        print(f"  Run ID: {run_id}")
    else:
        run_id = None
        print(f"  Run ID: None")
except Exception as e:
    print(f"  ERROR: {e}")
    run_id = None

# COMMAND ----------

print("Step 6: Initialize loader...")
try:
    loader = TimescaleDBLoaderV2(
        spark=spark,
        credentials=credentials,
        target_catalog="wakecap_prod",
        target_schema="raw",
        pipeline_id=notebook_path,
        pipeline_run_id=run_id,
        table_prefix="timescale_",
        max_retries=3,
        retry_delay=10
    )
    print("  Loader initialized!")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Step 7: Create config...")
try:
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
    print(f"  Config created!")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# COMMAND ----------

print("Step 8: Create second config...")
try:
    config2 = TableConfigV2(
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
        comment="Test"
    )
    print(f"  Config2 created!")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Step 9: Create tables list...")
tables = [config, config2]
print(f"  Tables: {len(tables)}")

for t in tables:
    print(f"    - {t.source_table}")
    print(f"      PK: {t.primary_key_columns}")

# COMMAND ----------

print("\nALL STEPS PASSED!")
