# WakeCapDW Migration Plan

**Created:** 2026-01-18
**Last Updated:** 2026-01-22

---

## Overview

This plan outlines the work required to complete the migration of WakeCapDW to Databricks Unity Catalog. The infrastructure and transpilation phases are complete. The remaining work focuses on:

1. Loading Bronze Layer from TimescaleDB (incremental) - **IMPLEMENTED**
2. Implementing Silver Layer transformations
3. Converting stored procedures
4. Converting functions
5. Implementing Gold Layer views
6. Validating the migration

---

## Phase 1: Bronze Layer - TimescaleDB Incremental Loading

### Status: IMPLEMENTED

**Job Name:** `WakeCapDW_Bronze_TimescaleDB_Raw`
**Loader:** `TimescaleDBLoaderV2` (pipelines/timescaledb/src/timescaledb_loader_v2.py)

### Architecture Overview

```
┌─────────────────────┐      ┌───────────────────────────────┐      ┌─────────────────────┐
│    TimescaleDB      │      │   TimescaleDBLoaderV2         │      │   Bronze Layer      │
│    wakecap_app      │ ───► │   (Watermark-based)           │ ───► │   (Delta Tables)    │
│    (PostgreSQL)     │ JDBC │   - GREATEST expressions      │      │   - 81 tables       │
│                     │      │   - Geometry ST_AsText        │      │   - timescale_*     │
│                     │      │   - Retry logic (3x)          │      │   - MERGE upserts   │
└─────────────────────┘      └───────────────────────────────┘      └─────────────────────┘
                                        │
                                        ▼
                              ┌───────────────────────────────┐
                              │  Watermark Tracking           │
                              │  _timescaledb_watermarks      │
                              │  - Per-table state            │
                              │  - Load status & row counts   │
                              └───────────────────────────────┘
```

### 1.1 Technical Implementation Details

#### Job Configuration

| Parameter | Value |
|-----------|-------|
| Job Name | `WakeCapDW_Bronze_TimescaleDB_Raw` |
| Schedule | Daily at 2:00 AM UTC (`0 0 2 * * ?`) |
| Cluster | Standard_DS3_v2, 2 workers |
| Notebook | `/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_optimized` |

#### Loader Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_catalog` | `wakecap_prod` | Unity Catalog catalog |
| `target_schema` | `raw` | Schema for bronze tables |
| `table_prefix` | `timescale_` | Prefix for all target tables |
| `fetch_size` | 50,000 | JDBC fetch size |
| `batch_size` | 100,000 | Batch processing size |
| `max_retries` | 3 | Retry attempts on failure |
| `retry_delay` | 5 seconds | Delay between retries |

#### JDBC Connection

```python
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}?sslmode=require"

# Connection options
.option("driver", "org.postgresql.Driver")
.option("fetchsize", 50000)
.option("sessionInitStatement", "SET statement_timeout = '60min'")
```

### 1.2 Secret Management

**Secret Scope:** `wakecap-timescale`

| Secret Key | Description |
|------------|-------------|
| `timescaledb-host` | TimescaleDB host address |
| `timescaledb-port` | Port (default 5432) |
| `timescaledb-database` | Database name (wakecap_app) |
| `timescaledb-user` | Database username |
| `timescaledb-password` | Database password |

### 1.3 Watermark-Based Incremental Loading

#### Watermark Table Schema

**Table:** `wakecap_prod.migration._timescaledb_watermarks`

```sql
CREATE TABLE _timescaledb_watermarks (
    source_system STRING NOT NULL,           -- 'timescaledb'
    source_schema STRING NOT NULL,           -- 'public'
    source_table STRING NOT NULL,            -- e.g., 'Activity'
    watermark_column STRING NOT NULL,        -- e.g., 'UpdatedAt'
    watermark_type STRING NOT NULL,          -- 'timestamp', 'bigint', 'date'
    watermark_expression STRING,             -- GREATEST expression if multiple columns
    last_watermark_value STRING,             -- String representation
    last_watermark_timestamp TIMESTAMP,      -- For timestamp types
    last_watermark_bigint BIGINT,            -- For bigint/integer types
    last_load_start_time TIMESTAMP,
    last_load_end_time TIMESTAMP,
    last_load_status STRING,                 -- 'success', 'failed', 'skipped'
    last_load_row_count BIGINT,
    last_error_message STRING,
    pipeline_id STRING,
    pipeline_run_id STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    created_by STRING
) USING DELTA
```

#### Incremental Query Pattern

```python
# Standard watermark query
query = f"""
    SELECT * FROM "{schema}"."{table}"
    WHERE "{watermark_column}" > '{last_watermark}'
    ORDER BY "{watermark_column}"
"""

# GREATEST expression for tables with multiple timestamp columns
query = f"""
    SELECT * FROM "{schema}"."{table}"
    WHERE GREATEST("CreatedAt", "UpdatedAt") > '{last_watermark}'
    ORDER BY GREATEST("CreatedAt", "UpdatedAt")
"""
```

### 1.4 Geometry Column Handling

Tables with geometry columns (PostGIS) use `ST_AsText` conversion:

| Table | Has Geometry | Handling |
|-------|--------------|----------|
| Blueprint | Yes | `ST_AsText("Geometry") AS "GeometryWKT"` |
| DeviceLocation | Yes | `ST_AsText` conversion |
| DeviceLocationSummary | Yes | `ST_AsText` conversion |
| NovadeWorkPermit | Yes | `ST_AsText` conversion |
| Space | Yes | `ST_AsText` conversion |
| SpaceHistory | Yes | `ST_AsText` conversion |
| Zone | Yes | `ST_AsText` conversion |
| ZoneHistory | Yes | `ST_AsText` conversion |
| ZoneViolationLog | Yes | `ST_AsText` conversion |

### 1.5 Table Registry

**Registry File:** `pipelines/timescaledb/config/timescaledb_tables_v2.yml`

#### Statistics

| Category | Count | Description |
|----------|-------|-------------|
| **Total Tables** | 81 | All tables in registry |
| **Dimensions** | ~25 | Reference/lookup tables |
| **Assignments** | ~15 | Association tables |
| **Facts** | ~20 | Transactional data |
| **History** | ~10 | Audit/history tables |
| **With Geometry** | 9 | Tables with PostGIS columns |

#### Sample Table Configurations

```yaml
# Simple dimension table
- source_table: Company
  primary_key_columns: [Id]
  watermark_column: UpdatedAt
  category: dimensions

# Table with geometry
- source_table: Zone
  primary_key_columns: [Id]
  watermark_column: UpdatedAt
  has_geometry: true
  geometry_handling: ST_AsText
  category: dimensions

# Fact table with different watermark
- source_table: EquipmentTelemetry
  primary_key_columns: [Id]
  watermark_column: CreatedAt  # Uses CreatedAt instead of UpdatedAt
  category: facts
```

### 1.6 Delta Write Strategy

#### MERGE Operation for Upserts

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, target_table)
merge_condition = " AND ".join([
    f"target.{pk} = source.{pk}"
    for pk in primary_key_columns
])

(
    delta_table.alias("target")
    .merge(df.alias("source"), merge_condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

#### Table Properties

```sql
ALTER TABLE {target_table} SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'bronze',
    'source_system' = 'timescaledb',
    'source_table' = '{source_table}'
)
```

### 1.7 Metadata Columns

Every bronze table includes:

| Column | Value | Description |
|--------|-------|-------------|
| `_loaded_at` | `current_timestamp()` | When the row was loaded |
| `_source_system` | `'timescaledb'` | Source system identifier |
| `_source_schema` | `'public'` | Source schema |
| `_source_table` | Table name | Source table name |
| `_pipeline_id` | Notebook path | Pipeline identifier |
| `_pipeline_run_id` | Run ID | Unique run identifier |

### 1.8 Notebook Parameters

The bronze loader notebook accepts these parameters:

| Widget | Options | Default | Description |
|--------|---------|---------|-------------|
| `load_mode` | incremental, full | incremental | Load mode |
| `category` | ALL, dimensions, assignments, facts, history | ALL | Category filter |
| `batch_size` | numeric | 100000 | Batch processing size |
| `fetch_size` | numeric | 50000 | JDBC fetch size |
| `max_tables` | numeric | 0 (all) | Limit tables to load |

### 1.9 Running the Bronze Job

#### Via Databricks UI

1. Navigate to Workflows > Jobs
2. Find `WakeCapDW_Bronze_TimescaleDB_Raw`
3. Click "Run Now"
4. Set parameters:
   - `load_mode`: "incremental" (or "full" for initial load)
   - `category`: "ALL"

#### Via CLI

```bash
# Run incremental load
databricks jobs run-now --job-id <job-id> \
    --notebook-params '{"load_mode": "incremental", "category": "ALL"}'

# Run full load (initial or reset)
databricks jobs run-now --job-id <job-id> \
    --notebook-params '{"load_mode": "full", "category": "ALL"}'

# Load specific category
databricks jobs run-now --job-id <job-id> \
    --notebook-params '{"load_mode": "incremental", "category": "dimensions"}'
```

#### Via Python Script

```bash
python run_bronze_all.py
# or
python monitor_pipeline.py --job-name "WakeCapDW_Bronze_TimescaleDB_Raw"
```

### 1.10 Monitoring & Verification

#### Check Watermarks

```sql
SELECT
    source_table,
    watermark_column,
    CASE WHEN watermark_expression IS NOT NULL THEN 'GREATEST' ELSE 'Single' END as wm_type,
    last_load_status,
    last_load_row_count,
    last_watermark_timestamp,
    last_load_end_time
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb'
ORDER BY last_load_end_time DESC;
```

#### Verify Table Counts

```sql
-- Sample verification
SELECT 'timescale_activity' as table_name, COUNT(*) as rows FROM wakecap_prod.raw.timescale_activity
UNION ALL
SELECT 'timescale_company', COUNT(*) FROM wakecap_prod.raw.timescale_company
UNION ALL
SELECT 'timescale_people', COUNT(*) FROM wakecap_prod.raw.timescale_people
UNION ALL
SELECT 'timescale_zone', COUNT(*) FROM wakecap_prod.raw.timescale_zone;

-- Total table count
SELECT COUNT(DISTINCT source_table) as loaded_tables
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE last_load_status = 'success';
-- Expected: 81
```

#### Check for Failures

```sql
SELECT source_table, last_error_message, last_load_end_time
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE last_load_status = 'failed'
ORDER BY last_load_end_time DESC;
```

### 1.11 Error Handling & Recovery

#### Automatic Retry

- Failed loads automatically retry up to 3 times
- 5-second delay between retries
- Errors logged to watermark table

#### Manual Recovery

```python
# Re-run failed tables with full load
loader.load_table(table_config, force_full_load=True)

# Or via notebook parameter
# Set load_mode = "full" and run specific category
```

---

## Phase 2: Stored Procedure Conversion

### 2.1 Conversion Strategy

Stored procedures will be converted to one of:
- **DLT Tables/Views:** For ETL transformations that produce tables
- **Python Notebooks:** For complex logic with multiple operations
- **SQL Notebooks:** For simpler set-based operations
- **Databricks Workflows:** For orchestration of multiple steps

### 2.2 High Priority Conversions (5 Procedures)

#### 2.2.1 stg.spCalculateFactWorkersShifts (1639 lines)

**Current Logic:** Calculates worker shift data from observations
**Patterns:** CURSOR, TEMP_TABLE
**Target:** DLT Pipeline + Python Notebook

**Conversion Plan:**
1. Extract CURSOR logic into set-based operations
2. Convert temp tables to temporary views or CTEs
3. Implement as streaming DLT table for incremental updates
4. Add data quality constraints

**Estimated Effort:** High

---

#### 2.2.2 stg.spDeltaSyncFactWorkersHistory (1561 lines)

**Current Logic:** Incremental sync of worker history data
**Patterns:** TEMP_TABLE, SPATIAL
**Target:** DLT Streaming Table + Python UDFs for spatial

**Conversion Plan:**
1. Implement Change Data Capture (CDC) pattern using DLT
2. Convert spatial operations to H3 or custom Python UDFs
3. Use APPLY CHANGES INTO for merge operations

**Estimated Effort:** High

---

#### 2.2.3 stg.spDeltaSyncFactObservations (1165 lines)

**Current Logic:** Syncs observation data incrementally
**Patterns:** TEMP_TABLE, MERGE
**Target:** DLT Streaming Table with APPLY CHANGES

**Conversion Plan:**
1. Convert MERGE to DLT APPLY CHANGES INTO
2. Implement watermarking for incremental processing
3. Convert temp tables to streaming intermediate tables

**Estimated Effort:** High

---

#### 2.2.4 stg.spCalculateFactWorkersContacts_ByRule (951 lines)

**Current Logic:** Calculates worker contacts based on rules
**Patterns:** CURSOR, DYNAMIC_SQL
**Target:** Python Notebook with parameterized SQL

**Conversion Plan:**
1. Analyze dynamic SQL patterns
2. Create parameterized SQL templates
3. Convert cursors to DataFrame operations
4. Implement as scheduled notebook job

**Estimated Effort:** High

---

#### 2.2.5 mrg.spMergeOldData (903 lines)

**Current Logic:** Merges historical data
**Patterns:** CURSOR, TEMP_TABLE, SPATIAL
**Target:** Python Notebook

**Conversion Plan:**
1. This may be a one-time migration procedure
2. If ongoing, convert to DLT merge patterns
3. Handle spatial data with H3 library

**Estimated Effort:** Medium-High

---

### 2.3 Medium Priority Conversions (Staging Procedures)

| Procedure | Target | Effort |
|-----------|--------|--------|
| stg.spStageWorkers | DLT Silver Table | Medium |
| stg.spStageProjects | DLT Silver Table | Medium |
| stg.spStageCrews | DLT Silver Table | Medium |
| stg.spStageDevices | DLT Silver Table | Medium |
| stg.spStageFact* | DLT Silver/Gold Tables | Medium-High |
| stg.spDeltaSync* | DLT Streaming Tables | High |
| stg.spCalculate* | Python Notebooks | High |

### 2.4 Lower Priority Conversions (Admin Procedures)

| Procedure | Recommendation |
|-----------|---------------|
| dbo.spRebuildIndex* | Not needed - Delta Lake handles optimization |
| dbo.spUpdateStatistics* | Not needed - Delta Lake auto-optimizes |
| dbo.spMaintenance* | Replace with OPTIMIZE and VACUUM commands |
| dbo.spCleanup* | Convert to scheduled cleanup jobs |

---

## Phase 3: Function Conversion

### 3.1 Spatial Functions (HIGH Priority)

Spatial functions require special handling. Options:

**Option A: H3 Library (Recommended)**
- Install h3-databricks library
- Convert geography points to H3 hexagonal indexes
- Enables efficient spatial joins and aggregations

**Option B: Custom Python UDFs**
- Implement using Shapely library
- Register as Spark UDFs

| Function | Conversion Approach |
|----------|---------------------|
| fnGeometry2SVG | Python UDF with Shapely |
| fnGeoPointShiftScale | Python UDF |
| fnFixGeographyOrder | Python UDF |

### 3.2 Time/Date Functions (MEDIUM Priority)

| Function | Databricks Equivalent |
|----------|----------------------|
| fnAtTimeZone | `from_utc_timestamp()` or `to_utc_timestamp()` |
| fnCalcTimeCategory | SQL CASE expression or Python UDF |

### 3.3 String/Pattern Functions (MEDIUM Priority)

| Function | Databricks Equivalent |
|----------|----------------------|
| fnExtractPattern | `regexp_extract()` |
| fnStripNonNumerics | `regexp_replace(col, '[^0-9]', '')` |

### 3.4 Security Predicate Functions (HIGH Priority)

These functions implement row-level security. Conversion approach:

| Function | Conversion |
|----------|------------|
| fn_OrganizationPredicate | Unity Catalog Row Filter |
| fn_ProjectPredicate | Unity Catalog Row Filter |
| fn_UserPredicate | Unity Catalog Row Filter |

---

## Phase 4: Silver Layer Implementation

### 4.1 Data Quality Rules

Add data quality expectations to Silver layer tables:

```python
@dlt.expect_or_drop("valid_worker_id", "worker_id IS NOT NULL")
@dlt.expect_or_drop("valid_dates", "start_date <= end_date")
@dlt.expect("non_negative_value", "value >= 0")
```

### 4.2 Silver Tables to Create

| Table | Source | Transformations |
|-------|--------|-----------------|
| silver_Worker | timescale_people | Dedupe, clean names, validate IDs |
| silver_Project | timescale_* | Validate status, clean names |
| silver_Crew | timescale_crew | Validate references |
| silver_Device | timescale_* | Validate model references |
| silver_Organization | timescale_company | Hierarchy validation |
| silver_FactObservations | timescale_* | Date validation, deduplication |
| silver_FactWorkersShifts | timescale_workshift* | Time calculations, validation |

---

## Phase 5: Gold Layer Implementation

### 5.1 Business Views

Implement remaining views:

| View | Priority | Dependencies |
|------|----------|--------------|
| gold_vwFactWorkersHistory | HIGH | silver_FactWorkersHistory |
| gold_vwFactWorkersContacts | HIGH | silver_FactWorkersContacts |
| gold_vwFactWorkersTasks | MEDIUM | silver_FactWorkersTasks |
| gold_vwFactObservations | HIGH | silver_FactObservations |
| gold_vwFactProgress | MEDIUM | silver_FactProgress |
| gold_vwLocation | MEDIUM | silver_Location |
| gold_vwManager | MEDIUM | silver_Manager |

---

## Phase 6: Testing and Reconciliation

### 6.1 Row Count Validation

```python
# Compare TimescaleDB source to Databricks target
source_count = spark.read.jdbc(timescale_jdbc_url, table).count()
target_count = spark.table(f"wakecap_prod.raw.timescale_{table.lower()}").count()
assert source_count == target_count, f"Row count mismatch: {table}"
```

### 6.2 Data Type Validation

Verify data type mappings (PostgreSQL/TimescaleDB → Databricks):
- `text` / `varchar` → `STRING`
- `timestamp` / `timestamptz` → `TIMESTAMP`
- `integer` → `INT`
- `bigint` → `BIGINT`
- `numeric(p,s)` → `DECIMAL(p,s)`
- `boolean` → `BOOLEAN`
- `jsonb` → `STRING` (JSON stored as string)
- `geometry` / `geography` → `STRING` (WKT via ST_AsText)

### 6.3 Business Logic Validation

Compare key aggregations:
- Total workers by organization
- Total observations by date range
- Total shifts by project
- Sum of hours worked

---

## Phase 7: Production Deployment

### 7.1 Pre-Production Checklist

- [x] Bronze layer tables populated (81 tables)
- [x] Incremental loading working
- [x] Watermark tracking operational
- [ ] Silver layer transformations complete
- [ ] Gold layer views working
- [ ] Critical stored procedures converted
- [ ] Row counts validated
- [ ] Business logic validated
- [ ] Performance acceptable
- [ ] Security (RLS) implemented

### 7.2 Production Schedule

| Job | Schedule | Description |
|-----|----------|-------------|
| WakeCapDW_Bronze_TimescaleDB_Raw | Daily 2:00 AM UTC | Incremental bronze load |
| DLT Pipeline | Every 4 hours | Silver/Gold transformations |
| Validation Job | Weekly | Data reconciliation |

---

## Appendix A: File Inventory

### Bronze Layer Files

| File | Purpose |
|------|---------|
| `pipelines/timescaledb/notebooks/bronze_loader_optimized.py` | Main loader notebook |
| `pipelines/timescaledb/notebooks/bronze_loader_dimensions.py` | Dimension-only loader |
| `pipelines/timescaledb/notebooks/bronze_loader_facts.py` | Fact-only loader |
| `pipelines/timescaledb/notebooks/bronze_loader_assignments.py` | Assignment-only loader |
| `pipelines/timescaledb/src/timescaledb_loader_v2.py` | Loader module |
| `pipelines/timescaledb/config/timescaledb_tables_v2.yml` | Table registry (81 tables) |

### Production Scripts

| Script | Purpose |
|--------|---------|
| `run_bronze_all.py` | Run all bronze tables |
| `run_bronze_raw.py` | Run raw bronze |
| `monitor_pipeline.py` | Monitor pipeline execution |
| `check_watermarks.py` | Verify watermarks |

---

## Appendix B: Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Large table timeout | Medium | High | 60-min statement timeout, incremental loading |
| Geometry conversion errors | Low | Medium | ST_AsText fallback, validation |
| Network interruption | Medium | Medium | Automatic retry (3x with 5s delay) |
| Data loss during migration | Low | Critical | Full backup, watermark tracking |
| Performance degradation | Low | High | Performance testing before cutover |

---

*This plan should be reviewed and updated as the migration progresses.*
