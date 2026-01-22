# Next Steps - Phase 1 Bronze Layer Implementation

**Date:** 2026-01-21
**Status:** Code Complete - Awaiting Deployment
**Source:** TimescaleDB (wakecap_app database)
**Tables:** 81 tables (generic incremental load with PK-based upsert)

---

## Prerequisites

Before proceeding, ensure you have:
- [ ] Databricks CLI installed and configured
- [ ] Access to Databricks workspace with Unity Catalog
- [ ] TimescaleDB connection details (host, port, database, user, password)
- [ ] Network connectivity from Databricks to TimescaleDB

---

## Step 1: Configure Databricks Secret Scope

Create the secret scope and add TimescaleDB credentials.

```bash
# Create secret scope (one-time)
databricks secrets create-scope --scope wakecap-timescale

# Add credentials (you'll be prompted for values)
databricks secrets put --scope wakecap-timescale --key timescaledb-host
databricks secrets put --scope wakecap-timescale --key timescaledb-port
databricks secrets put --scope wakecap-timescale --key timescaledb-database
databricks secrets put --scope wakecap-timescale --key timescaledb-user
databricks secrets put --scope wakecap-timescale --key timescaledb-password
```

**Verify secrets are created:**
```bash
databricks secrets list --scope wakecap-timescale
```

---

## Step 2: Create Target Schema

Run in Databricks SQL or notebook:

```sql
-- Create the bronze layer schema under source/timescaledb
CREATE SCHEMA IF NOT EXISTS wakecap_prod.source_timescaledb
COMMENT 'Bronze layer - raw data from TimescaleDB';

-- Create the migration schema for watermark tracking
CREATE SCHEMA IF NOT EXISTS wakecap_prod.migration
COMMENT 'Migration tracking and metadata';
```

---

## Step 3: Deploy Watermark Table

Execute the DDL to create the watermark tracking table:

```sql
-- Run the contents of:
-- migration_project/pipelines/timescaledb/ddl/create_watermark_table.sql
```

Or run directly:

```sql
CREATE TABLE IF NOT EXISTS wakecap_prod.migration._timescaledb_watermarks (
    source_system STRING NOT NULL,
    source_schema STRING NOT NULL,
    source_table STRING NOT NULL,
    watermark_column STRING NOT NULL,
    watermark_type STRING NOT NULL,
    last_watermark_value STRING,
    last_watermark_timestamp TIMESTAMP,
    last_watermark_bigint BIGINT,
    last_load_start_time TIMESTAMP,
    last_load_end_time TIMESTAMP,
    last_load_status STRING,
    last_load_row_count BIGINT,
    last_error_message STRING,
    pipeline_id STRING,
    pipeline_run_id STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP,
    created_by STRING DEFAULT current_user()
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

---

## Step 4: Upload Files to Databricks Workspace

Upload the pipeline files to Databricks workspace:

```bash
# Create workspace directories
databricks workspace mkdirs /Workspace/migration_project/pipelines/timescaledb/notebooks
databricks workspace mkdirs /Workspace/migration_project/pipelines/timescaledb/src
databricks workspace mkdirs /Workspace/migration_project/pipelines/timescaledb/config

# Upload the bronze loader notebook
databricks workspace import \
  migration_project/pipelines/timescaledb/notebooks/bronze_loader.py \
  /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader \
  --language PYTHON --overwrite

# Upload source files
databricks workspace import \
  migration_project/pipelines/timescaledb/src/timescaledb_loader.py \
  /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader.py \
  --language PYTHON --overwrite

# Upload config (as file, not notebook)
databricks workspace import \
  migration_project/pipelines/timescaledb/config/timescaledb_tables.yml \
  /Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables.yml \
  --format AUTO --overwrite

# Upload DLT pipeline
databricks workspace import \
  migration_project/pipelines/timescaledb/dlt_timescaledb_bronze.py \
  /Workspace/migration_project/pipelines/timescaledb/dlt_timescaledb_bronze \
  --language PYTHON --overwrite
```

---

## Step 5: Test Connectivity

Run a quick test in a Databricks notebook:

```python
# Test TimescaleDB connectivity
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

# Get credentials
host = dbutils.secrets.get("wakecap-timescale", "timescaledb-host")
port = dbutils.secrets.get("wakecap-timescale", "timescaledb-port")
database = dbutils.secrets.get("wakecap-timescale", "timescaledb-database")
user = dbutils.secrets.get("wakecap-timescale", "timescaledb-user")
password = dbutils.secrets.get("wakecap-timescale", "timescaledb-password")

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}?sslmode=require"

# Test connection with simple query
test_df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", "SELECT 1 as test")
    .option("user", user)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

test_df.show()
print("Connection successful!")
```

---

## Step 6: Run Initial Load - All Tables

Run the generic bronze loader for all 81 tables:

```python
# In Databricks, run:
dbutils.notebook.run(
    "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader",
    timeout_seconds=14400,  # 4 hours for initial load
    arguments={
        "load_mode": "incremental",
        "tables": "ALL",
        "fetch_size": "10000",
        "batch_size": "100000"
    }
)
```

Or run interactively in the notebook UI.

**Expected output:** 81 tables loaded to `wakecap_prod.source_timescaledb` with PK-based upsert

---

## Step 7: Run Load for Specific Tables (Optional)

To load specific tables only:

```python
dbutils.notebook.run(
    "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader",
    timeout_seconds=3600,
    arguments={
        "load_mode": "incremental",
        "tables": "People,Zone,Equipment,Crew",
        "fetch_size": "10000"
    }
)
```

---

## Step 8: Run Load for High-Volume Tables

For high-volume tables (OBS, DeviceLocation), use larger fetch size:

```python
dbutils.notebook.run(
    "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader",
    timeout_seconds=7200,
    arguments={
        "load_mode": "incremental",
        "tables": "OBS,OBSCurrent,DeviceLocation",
        "fetch_size": "50000",
        "batch_size": "100000"
    }
)
```

**Expected output:** High-volume tables loaded (OBS and DeviceLocation may take longest)

---

## Step 9: Validate Load Results

Run validation queries:

```sql
-- Check all tables loaded successfully
SELECT
    source_table,
    last_load_status,
    last_load_row_count,
    last_watermark_timestamp,
    last_load_end_time
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb'
ORDER BY last_load_end_time DESC;

-- Verify total tables loaded
SELECT COUNT(DISTINCT source_table) as tables_loaded
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb' AND last_load_status = 'success';
-- Expected: 81

-- Check for any failures
SELECT source_table, last_error_message
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE last_load_status = 'failed';
```

---

## Step 10: Create DLT Pipeline (Optional)

If using DLT instead of notebooks:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

w = WorkspaceClient()

pipeline = w.pipelines.create(
    name="WakeCapDW_Bronze_TimescaleDB",
    catalog="wakecap_prod",
    target="source_timescaledb",
    development=True,
    serverless=True,
    continuous=False,
    libraries=[
        PipelineLibrary(
            notebook=NotebookLibrary(
                path="/Workspace/migration_project/pipelines/timescaledb/dlt_timescaledb_bronze"
            )
        )
    ],
    configuration={
        "spark.databricks.delta.schema.autoMerge.enabled": "true"
    }
)

print(f"Pipeline created: {pipeline.pipeline_id}")
```

---

## Step 11: Configure Scheduled Job

Create Databricks Workflow for scheduled loading:

### Hourly Job (All Tables)
```python
w.jobs.create(
    name="WakeCapDW_Bronze_TimescaleDB_Hourly",
    tasks=[{
        "task_key": "load_all_tables",
        "notebook_task": {
            "notebook_path": "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader",
            "base_parameters": {
                "tables": "ALL",
                "load_mode": "incremental"
            }
        },
        "new_cluster": {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 2
        }
    }],
    schedule={
        "quartz_cron_expression": "0 0 * * * ?",
        "timezone_id": "UTC"
    }
)
```

---

## Verification Checklist

After completing all steps, verify:

- [ ] Secret scope `wakecap-timescale` exists with all 5 keys
- [ ] Schema `wakecap_prod.source_timescaledb` exists
- [ ] Watermark table `wakecap_prod.migration._timescaledb_watermarks` exists
- [ ] All 81 tables show `last_load_status = 'success'` in watermark table
- [ ] Bronze tables exist: `wakecap_prod.source_timescaledb.public_*`
- [ ] Metadata columns present: `_loaded_at`, `_source_system`, `_source_table`
- [ ] Scheduled job is running on time

---

## Troubleshooting

### Connection Timeout
- Check network connectivity from Databricks to TimescaleDB
- Verify firewall rules allow Databricks IPs
- Increase `statement_timeout` in JDBC options

### Authentication Failure
- Verify secrets are correctly set in scope
- Check TimescaleDB user has SELECT permissions

### Out of Memory
- Reduce `fetch_size` for large tables
- Increase cluster memory
- Load tables in smaller batches

### Slow Performance
- Increase `fetch_size` for better throughput (up to 100000)
- Use larger cluster for high-volume tables
- Run during off-peak hours

---

## Contact

For issues or questions, contact the Data Engineering team.
