# Plan: Phase 1 - Bronze Layer Loading from TimescaleDB

## Task Description

Implement the complete Bronze Layer loading infrastructure for the WakeCapDW migration. This phase focuses on incrementally loading all 48 tables from TimescaleDB into Databricks Delta tables using the watermark-based extraction pattern. The implementation will leverage the existing parametrized `TimescaleDBLoader` class and table registry.

## Objective

When this plan is complete:
1. All 48 TimescaleDB tables will be loaded into the Bronze layer (`wakecap_prod.raw_timescaledb`)
2. Incremental loading will be operational using watermark-based extraction
3. The watermark tracking table will maintain state for all loaded tables
4. DLT pipelines will be configured and deployable to Databricks
5. Load scheduling will be established (real-time, hourly, daily)

---

## Source System Analysis

- **Database**: TimescaleDB (PostgreSQL-based time-series database)
- **Connection**: JDBC with PostgreSQL driver
- **Object Count**: 48 tables total
  - 26 Dimension tables
  - 11 Assignment tables
  - 9 Fact tables (TimescaleDB hypertables)
  - 2 Other/Staging tables
- **Complexity Indicators**: Geometry columns (2 tables), Hypertables (9 tables), Custom column mappings
- **Estimated Data Volume**: FactObservations and FactWorkersHistory are very large (partitioned)

## Target Architecture

- **Catalog**: `wakecap_prod`
- **Schema**: `raw_timescaledb` (Bronze layer)
- **Pipeline Type**: DLT (Delta Live Tables) + Batch Loader
- **Medallion Layer**: Bronze (raw data, metadata columns added)
- **State Management**: `wakecap_prod.migration._timescaledb_watermarks`

---

## Relevant Files

Use these files to complete the task:

### Existing Files (Read/Modify)

- **`migration_project/pipelines/timescaledb/config/timescaledb_tables.yml`**
  - Table registry with all 48 tables, watermark columns, and configurations
  - Source of truth for table metadata

- **`migration_project/pipelines/timescaledb/src/timescaledb_loader.py`**
  - Parametrized `TimescaleDBLoader` class for incremental loading
  - `WatermarkManager` for tracking load state
  - `TableConfig` dataclass for table configurations

- **`migration_project/pipelines/timescaledb/dlt_timescaledb_raw.py`**
  - DLT pipeline definition for raw table loading
  - Factory functions for creating DLT tables

- **`migration_project/pipelines/timescaledb/ddl/create_watermark_table.sql`**
  - DDL for watermark tracking table and helper views

- **`migration_project/credentials_template.yml`**
  - Template for TimescaleDB connection credentials

### New Files to Create

- **`migration_project/pipelines/timescaledb/notebooks/bronze_loader_dimensions.py`**
  - Databricks notebook for loading dimension tables

- **`migration_project/pipelines/timescaledb/notebooks/bronze_loader_assignments.py`**
  - Databricks notebook for loading assignment tables

- **`migration_project/pipelines/timescaledb/notebooks/bronze_loader_facts.py`**
  - Databricks notebook for loading fact tables (hypertables)

- **`migration_project/pipelines/timescaledb/config/load_schedule.yml`**
  - Load scheduling configuration (real-time, hourly, daily)

- **`migration_project/pipelines/timescaledb/tests/test_timescaledb_loader.py`**
  - Unit tests for the loader components

---

## Table Inventory

### Dimension Tables (26 tables)

| # | Table | Primary Key | Watermark Column | Type | Full Load |
|---|-------|-------------|------------------|------|-----------|
| 1 | Organization | OrganizationID | modified_at | timestamp | No |
| 2 | Project | ProjectID | modified_at | timestamp | No |
| 3 | Worker | WorkerID | WatermarkUTC | timestamp | No |
| 4 | Crew | CrewID | WatermarkUTC | timestamp | No |
| 5 | Trade | TradeID | WatermarkUTC | timestamp | No |
| 6 | Company | CompanyID | WatermarkUTC | timestamp | No |
| 7 | Floor | FloorID | WatermarkUTC | timestamp | No |
| 8 | Zone | ZoneID | WatermarkUTC | timestamp | No |
| 9 | Device | DeviceID | WatermarkUTC | timestamp | No |
| 10 | DeviceModel | DeviceModelID | modified_at | timestamp | **Yes** |
| 11 | Workshift | WorkshiftID | WatermarkUTC | timestamp | No |
| 12 | WorkshiftDetails | WorkshiftDetailsID | WatermarkUTC | timestamp | No |
| 13 | Activity | ActivityID | WatermarkUTC | timestamp | No |
| 14 | LocationGroup | LocationGroupID | WatermarkUTC | timestamp | No |
| 15 | Department | DepartmentID | modified_at | timestamp | **Yes** |
| 16 | Title | TitleID | modified_at | timestamp | **Yes** |
| 17 | ObservationSource | ObservationSourceID | modified_at | timestamp | **Yes** |
| 18 | ObservationType | ObservationTypeID | modified_at | timestamp | **Yes** |
| 19 | ShiftType | ShiftTypeID | modified_at | timestamp | **Yes** |
| 20 | ReportType | ReportTypeID | modified_at | timestamp | **Yes** |
| 21 | Manager | ManagerID | WatermarkUTC | timestamp | No |
| 22 | Location | LocationID | WatermarkUTC | timestamp | No |
| 23 | WorkerRole | WorkerRoleID | modified_at | timestamp | **Yes** |
| 24 | Task | TaskID | WatermarkUTC | timestamp | No |
| 25 | Shift | ShiftID | WatermarkUTC | timestamp | No |

**Special Handling:**
- `Floor` and `Zone` have geometry columns: `ST_AsText(Geometry)` conversion required

### Assignment Tables (11 tables)

| # | Table | Primary Key | Watermark Column | Type |
|---|-------|-------------|------------------|------|
| 1 | CrewAssignments | CrewAssignmentID | WatermarkUTC | timestamp |
| 2 | TradeAssignments | TradeAssignmentID | WatermarkUTC | timestamp |
| 3 | WorkshiftAssignments | WorkshiftAssignmentID | WatermarkUTC | timestamp |
| 4 | DeviceAssignments | DeviceAssignmentID | WatermarkUTC | timestamp |
| 5 | ManagerAssignments | ManagerAssignmentID | WatermarkUTC | timestamp |
| 6 | ManagerAssignmentSnapshots | ManagerAssignmentSnapshotID | WatermarkUTC | timestamp |
| 7 | LocationGroupAssignments | LocationGroupAssignmentID | WatermarkUTC | timestamp |
| 8 | WorkerLocationAssignments | WorkerLocationAssignmentID | WatermarkUTC | timestamp |
| 9 | ProjectAssignments | ProjectAssignmentID | WatermarkUTC | timestamp |
| 10 | WorkerRoleAssignments | WorkerRoleAssignmentID | WatermarkUTC | timestamp |
| 11 | LocationAssignments | LocationAssignmentID | WatermarkUTC | timestamp |

### Fact Tables (9 tables - TimescaleDB Hypertables)

| # | Table | Primary Key | Watermark | Partition Column | Fetch Size |
|---|-------|-------------|-----------|------------------|------------|
| 1 | FactWorkersHistory | WorkerHistoryID | WatermarkUTC | LocalDate | 10000 |
| 2 | FactWorkersShifts | WorkerShiftID | WatermarkUTC | ShiftLocalDate | 10000 |
| 3 | FactWorkersShiftsCombined | WorkerShiftCombinedID | WatermarkUTC | ShiftLocalDate | 10000 |
| 4 | FactReportedAttendance | ReportedAttendanceID | WatermarkUTC | ShiftLocalDate | 10000 |
| 5 | FactObservations | ObservationID | WatermarkUTC | ObservationTime | **50000** |
| 6 | FactProgress | ProgressID | WatermarkUTC | ProgressDate | 10000 |
| 7 | FactWeatherObservations | WeatherObservationID | WatermarkUTC | - | 10000 |
| 8 | FactWorkersContacts | WorkerContactID | WatermarkUTC | LocalDate | 10000 |
| 9 | FactWorkersTasks | WorkerTaskID | WatermarkUTC | - | 10000 |

### Other Tables (2 tables)

| # | Table | Schema | Primary Key | Watermark | Full Load |
|---|-------|--------|-------------|-----------|-----------|
| 1 | ContactTracingRule | public | ContactTracingRuleID | WatermarkUTC | No |
| 2 | SyncState | stg | SyncStateID | modified_at | **Yes** |
| 3 | ImpactedAreaLog | stg | ImpactedAreaLogID | created_at | **Yes** |

---

## Implementation Phases

### Phase 1.1: Infrastructure Setup

**Priority:** CRITICAL
**Effort:** Low

1. Configure TimescaleDB credentials in Databricks secret scope
2. Create target schema `wakecap_prod.raw_timescaledb`
3. Deploy watermark tracking table `wakecap_prod.migration._timescaledb_watermarks`
4. Verify network connectivity from Databricks to TimescaleDB

### Phase 1.2: Reference Data Loading (Full Load Tables)

**Priority:** HIGH
**Effort:** Low

Load small reference tables that use full-load strategy (8 tables):
- DeviceModel, Department, Title, ObservationSource
- ObservationType, ShiftType, ReportType, WorkerRole

### Phase 1.3: Dimension Tables Loading

**Priority:** HIGH
**Effort:** Medium

Load dimension tables with incremental loading (18 tables):
- Organization, Project, Worker, Crew, Trade, Company
- Floor (with geometry), Zone (with geometry)
- Device, Workshift, WorkshiftDetails, Activity
- LocationGroup, Manager, Location, Task, Shift

### Phase 1.4: Assignment Tables Loading

**Priority:** HIGH
**Effort:** Medium

Load all bridge/relationship tables (11 tables)

### Phase 1.5: Fact Tables Loading

**Priority:** HIGH
**Effort:** High

Load large fact tables with partitioning (9 tables):
- Start with smaller facts: FactProgress, FactWorkersTasks, FactWeatherObservations
- Then larger facts: FactWorkersShifts, FactReportedAttendance
- Finally very large facts: FactWorkersHistory, FactObservations, FactWorkersContacts

### Phase 1.6: Scheduling & Monitoring

**Priority:** MEDIUM
**Effort:** Low

Configure load scheduling:
- Real-time (every 5 min): FactObservations
- Hourly: FactWorkersHistory, assignment tables
- Daily: Dimension tables, reference data

---

## Step by Step Tasks

### 1. Configure Databricks Secret Scope

Create secret scope and add TimescaleDB credentials.

```bash
# Create secret scope
databricks secrets create-scope --scope wakecap-timescale

# Add credentials
databricks secrets put --scope wakecap-timescale --key timescaledb-host
databricks secrets put --scope wakecap-timescale --key timescaledb-port
databricks secrets put --scope wakecap-timescale --key timescaledb-database
databricks secrets put --scope wakecap-timescale --key timescaledb-user
databricks secrets put --scope wakecap-timescale --key timescaledb-password
```

### 2. Create Target Schema and Watermark Table

Deploy the infrastructure DDL.

```sql
-- Create target schema
CREATE SCHEMA IF NOT EXISTS wakecap_prod.raw_timescaledb
COMMENT 'Bronze layer - raw data from TimescaleDB';

-- Create watermark tracking table
-- Execute: migration_project/pipelines/timescaledb/ddl/create_watermark_table.sql
```

### 3. Test Single Table Load (Validation)

Test the loader with a small reference table to verify connectivity.

```python
# In Databricks notebook
from timescaledb_loader import TimescaleDBLoader, TimescaleDBCredentials, TableConfig

# Initialize with secrets
creds = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale")
loader = TimescaleDBLoader(
    spark=spark,
    credentials=creds,
    target_catalog="wakecap_prod",
    target_schema="raw_timescaledb"
)

# Test with ShiftType (smallest table)
test_config = TableConfig(
    source_schema="public",
    source_table="ShiftType",
    primary_key_columns=["ShiftTypeID"],
    watermark_column="modified_at",
    watermark_type=WatermarkType.TIMESTAMP,
    is_full_load=True
)

result = loader.load_table(test_config)
print(f"Status: {result.status}, Rows: {result.rows_loaded}")
```

### 4. Create Dimension Loader Notebook

Create a reusable notebook for loading dimension tables.

**File:** `migration_project/pipelines/timescaledb/notebooks/bronze_loader_dimensions.py`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader - Dimension Tables
# MAGIC
# MAGIC Loads dimension tables from TimescaleDB using incremental watermark-based extraction.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

import sys
sys.path.append("/Workspace/migration_project/pipelines/timescaledb/src")

from timescaledb_loader import (
    TimescaleDBLoader,
    TimescaleDBCredentials,
    LoadStatus
)

# COMMAND ----------

# Initialize loader from secrets
credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale")

loader = TimescaleDBLoader(
    spark=spark,
    credentials=credentials,
    target_catalog="wakecap_prod",
    target_schema="raw_timescaledb",
    pipeline_id=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
    pipeline_run_id=dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
)

# COMMAND ----------

# Load all dimension tables
registry_path = "/Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables.yml"

results = loader.load_all_tables(
    registry_path=registry_path,
    category_filter="dimensions"
)

# COMMAND ----------

# Display results summary
from pyspark.sql import Row

summary_data = [
    Row(
        table=r.table_config.source_table,
        status=r.status.value,
        rows=r.rows_loaded,
        duration_sec=r.duration_seconds or 0,
        watermark=r.new_watermark or "N/A"
    )
    for r in results
]

display(spark.createDataFrame(summary_data))

# COMMAND ----------

# Check for failures
failures = [r for r in results if r.status == LoadStatus.FAILED]
if failures:
    for f in failures:
        print(f"FAILED: {f.table_config.source_table} - {f.error_message}")
    raise Exception(f"{len(failures)} tables failed to load")
```

### 5. Create Assignment Loader Notebook

**File:** `migration_project/pipelines/timescaledb/notebooks/bronze_loader_assignments.py`

Similar to dimension loader but with `category_filter="assignments"`.

### 6. Create Fact Loader Notebook

**File:** `migration_project/pipelines/timescaledb/notebooks/bronze_loader_facts.py`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader - Fact Tables
# MAGIC
# MAGIC Loads fact tables (TimescaleDB hypertables) with optimized fetch sizes and partitioning.

# COMMAND ----------

# Similar initialization as dimensions...

# COMMAND ----------

# Load fact tables in priority order
fact_tables_order = [
    "FactProgress",
    "FactWorkersTasks",
    "FactWeatherObservations",
    "FactWorkersShifts",
    "FactWorkersShiftsCombined",
    "FactReportedAttendance",
    "FactWorkersContacts",
    "FactWorkersHistory",
    "FactObservations"  # Largest table - load last
]

results = loader.load_all_tables(
    registry_path=registry_path,
    category_filter="facts",
    table_filter=fact_tables_order
)
```

### 7. Create Load Schedule Configuration

**File:** `migration_project/pipelines/timescaledb/config/load_schedule.yml`

```yaml
# Load Schedule Configuration
# Defines when each table category should be refreshed

schedules:
  realtime:
    cron: "*/5 * * * *"  # Every 5 minutes
    tables:
      - FactObservations

  hourly:
    cron: "0 * * * *"  # Every hour
    tables:
      - FactWorkersHistory
      - FactWorkersShifts
      - FactWorkersShiftsCombined
      - FactReportedAttendance
      - FactWorkersContacts
      - FactWorkersTasks
      - FactProgress
      - FactWeatherObservations
    categories:
      - assignments

  daily:
    cron: "0 2 * * *"  # 2 AM daily
    categories:
      - dimensions
    tables:
      - ContactTracingRule
      - SyncState
      - ImpactedAreaLog
```

### 8. Create DLT Pipeline Definition

Update the DLT pipeline to use the parametrized loader pattern.

**File:** `migration_project/pipelines/timescaledb/dlt_timescaledb_bronze.py`

```python
# Databricks notebook source
import dlt
from pyspark.sql import functions as F
import yaml

# COMMAND ----------

def load_credentials():
    """Load credentials from Databricks secrets."""
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    return {
        "host": dbutils.secrets.get("wakecap-timescale", "timescaledb-host"),
        "port": dbutils.secrets.get("wakecap-timescale", "timescaledb-port"),
        "database": dbutils.secrets.get("wakecap-timescale", "timescaledb-database"),
        "user": dbutils.secrets.get("wakecap-timescale", "timescaledb-user"),
        "password": dbutils.secrets.get("wakecap-timescale", "timescaledb-password")
    }

def get_jdbc_url(creds):
    return f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['database']}?sslmode=require"

def read_table(schema, table, fetch_size=10000):
    """Parametrized function to read from TimescaleDB."""
    creds = load_credentials()
    query = f'SELECT * FROM "{schema}"."{table}"'

    return (spark.read
        .format("jdbc")
        .option("url", get_jdbc_url(creds))
        .option("query", query)
        .option("user", creds["user"])
        .option("password", creds["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", fetch_size)
        .load()
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("timescaledb"))
        .withColumn("_source_table", F.lit(f"{schema}.{table}"))
    )

# COMMAND ----------

def create_bronze_table(table_name, source_schema, source_table, primary_keys, comment, fetch_size=10000, partition_col=None):
    """Factory function to create DLT bronze tables."""

    @dlt.table(
        name=f"bronze_{table_name.lower()}",
        comment=comment,
        table_properties={
            "quality": "bronze",
            "source_system": "timescaledb",
            "source_table": f"{source_schema}.{source_table}",
            "primary_keys": ",".join(primary_keys),
            "pipelines.reset.allowed": "true"
        },
        partition_cols=[partition_col] if partition_col else None
    )
    def load():
        return read_table(source_schema, source_table, fetch_size)

    return load

# COMMAND ----------

# Load table registry
with open("/Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables.yml") as f:
    registry = yaml.safe_load(f)

# Create DLT tables dynamically from registry
for table in registry["tables"]:
    if table.get("enabled", True):
        create_bronze_table(
            table_name=table["source_table"],
            source_schema=table["source_schema"],
            source_table=table["source_table"],
            primary_keys=table["primary_key_columns"],
            comment=table.get("comment", f"Raw data from {table['source_table']}"),
            fetch_size=table.get("fetch_size", 10000),
            partition_col=table.get("partition_column")
        )
```

### 9. Deploy and Test Pipeline

```python
# Create DLT pipeline via Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

w = WorkspaceClient()

pipeline = w.pipelines.create(
    name="WakeCapDW_Bronze_TimescaleDB",
    catalog="wakecap_prod",
    target="raw_timescaledb",
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

### 10. Validate Load Results

```sql
-- Check watermark table for load status
SELECT
    source_table,
    last_load_status,
    last_load_row_count,
    last_watermark_timestamp,
    last_load_end_time
FROM wakecap_prod.migration._timescaledb_watermarks
ORDER BY last_load_end_time DESC;

-- Check row counts in bronze tables
SELECT 'bronze_worker' as table_name, COUNT(*) as row_count FROM wakecap_prod.raw_timescaledb.bronze_worker
UNION ALL
SELECT 'bronze_project', COUNT(*) FROM wakecap_prod.raw_timescaledb.bronze_project
UNION ALL
SELECT 'bronze_factobservations', COUNT(*) FROM wakecap_prod.raw_timescaledb.bronze_factobservations;
```

---

## Testing Strategy

### Unit Tests

Create unit tests for the loader components.

**File:** `migration_project/pipelines/timescaledb/tests/test_timescaledb_loader.py`

```python
import pytest
from timescaledb_loader import TableConfig, WatermarkType, LoadStatus

def test_table_config_from_dict():
    """Test TableConfig creation from dictionary."""
    data = {
        "source_schema": "public",
        "source_table": "Worker",
        "primary_key_columns": ["WorkerID"],
        "watermark_column": "WatermarkUTC",
        "watermark_type": "timestamp"
    }

    config = TableConfig.from_dict(data)

    assert config.source_schema == "public"
    assert config.source_table == "Worker"
    assert config.watermark_type == WatermarkType.TIMESTAMP
    assert config.target_table_name == "public_worker"

def test_table_config_full_source_name():
    """Test full source name property."""
    config = TableConfig(
        source_schema="public",
        source_table="Worker",
        primary_key_columns=["WorkerID"],
        watermark_column="WatermarkUTC"
    )

    assert config.full_source_name == "public.Worker"
```

### Integration Tests

Run end-to-end load tests in Databricks.

```python
# Integration test notebook
def test_full_load_cycle():
    """Test full load cycle for a single table."""
    # 1. Clear existing data
    spark.sql("DELETE FROM wakecap_prod.migration._timescaledb_watermarks WHERE source_table = 'ShiftType'")
    spark.sql("DROP TABLE IF EXISTS wakecap_prod.raw_timescaledb.public_shifttype")

    # 2. Run initial load
    result1 = loader.load_table(shifttype_config, force_full_load=True)
    assert result1.status == LoadStatus.SUCCESS
    assert result1.rows_loaded > 0

    # 3. Run incremental load (should skip if no new data)
    result2 = loader.load_table(shifttype_config)
    assert result2.status in [LoadStatus.SUCCESS, LoadStatus.SKIPPED]

    # 4. Verify watermark was updated
    wm = loader.watermark_manager.get_watermark("public", "ShiftType")
    assert wm is not None
```

---

## Acceptance Criteria

1. **All 48 tables loaded**: Every table in `timescaledb_tables.yml` has data in Bronze layer
2. **Watermark tracking operational**: `_timescaledb_watermarks` table contains entries for all tables with `last_load_status = 'success'`
3. **Incremental loading works**: Running the loader twice only loads new records (not full reload)
4. **Geometry handling correct**: Floor and Zone tables have Geometry column as WKT STRING
5. **Fact tables partitioned**: Large fact tables are partitioned by their respective date columns
6. **Metadata columns present**: All Bronze tables have `_loaded_at`, `_source_system`, `_source_table` columns
7. **No load failures**: All tables load successfully without errors
8. **Row counts match**: Bronze table counts match source TimescaleDB counts

---

## Validation Commands

Execute these commands to validate the task is complete:

```bash
# 1. Verify secret scope exists
databricks secrets list-scopes | grep wakecap-timescale

# 2. Verify schema exists
databricks sql execute --sql "SHOW SCHEMAS IN wakecap_prod LIKE 'raw_timescaledb'"

# 3. Verify watermark table exists
databricks sql execute --sql "DESCRIBE wakecap_prod.migration._timescaledb_watermarks"
```

```sql
-- 4. Verify all tables loaded successfully
SELECT COUNT(*) as success_count
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE last_load_status = 'success';
-- Expected: 48

-- 5. Verify no failed loads
SELECT source_table, last_error_message
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE last_load_status = 'failed';
-- Expected: 0 rows

-- 6. Verify total row counts
SELECT
    _source_table,
    COUNT(*) as rows
FROM wakecap_prod.raw_timescaledb.bronze_worker
GROUP BY _source_table;

-- 7. Verify metadata columns exist
DESCRIBE wakecap_prod.raw_timescaledb.bronze_worker;
-- Should include: _loaded_at, _source_system, _source_table, _pipeline_id, _pipeline_run_id
```

```python
# 8. Run loader validation script
from timescaledb_loader import TimescaleDBLoader

# Load and validate all tables
results = loader.load_all_tables("config/timescaledb_tables.yml")

# Assert all succeeded
failed = [r for r in results if r.status.value == "failed"]
assert len(failed) == 0, f"Failed tables: {[r.table_config.source_table for r in failed]}"

print(f"Successfully validated {len(results)} tables")
```

---

## Notes

### Required Databricks Secrets

```bash
# Create scope (one-time)
databricks secrets create-scope --scope wakecap-timescale

# Add secrets
databricks secrets put --scope wakecap-timescale --key timescaledb-host
databricks secrets put --scope wakecap-timescale --key timescaledb-port
databricks secrets put --scope wakecap-timescale --key timescaledb-database
databricks secrets put --scope wakecap-timescale --key timescaledb-user
databricks secrets put --scope wakecap-timescale --key timescaledb-password
```

### Required Cluster Libraries

- PostgreSQL JDBC Driver: `org.postgresql:postgresql:42.6.0` (Maven coordinates)

### Network Requirements

- Databricks cluster must have network access to TimescaleDB host
- Ensure TimescaleDB allows connections from Databricks IP ranges
- SSL mode `require` is configured by default

### Performance Considerations

- **FactObservations**: Use `fetch_size=50000` for optimal throughput
- **Large tables**: Consider running during off-peak hours
- **Parallelism**: Tables are loaded sequentially to avoid JDBC connection exhaustion
- **Initial load**: First run will take longer; subsequent incremental loads are faster

### Rollback Procedure

If a load fails and needs to be rerun:

```sql
-- Reset watermark for a specific table
DELETE FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_table = 'FailedTableName';

-- Drop and recreate the bronze table
DROP TABLE IF EXISTS wakecap_prod.raw_timescaledb.bronze_failedtablename;
```

Then rerun the loader with `force_full_load=True`.

---

*Plan created: 2026-01-20*
*Last updated: 2026-01-20*
