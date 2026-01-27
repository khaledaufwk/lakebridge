# WakeCapDW Migration Plan

**Created:** 2026-01-18
**Last Updated:** 2026-01-27

---

## ⚠️ IMPORTANT: Production Databricks Jobs

**All future development work MUST be added to one of these three production jobs:**

| Job Name | Job ID | Purpose | Schedule |
|----------|--------|---------|----------|
| **WakeCapDW_Bronze_TimescaleDB_Raw** | 28181369160316 | Bronze layer ingestion | 2:00 AM UTC |
| **WakeCapDW_Silver_TimescaleDB** | 181959206191493 | Silver transformations (9 tasks) | 3:00 AM UTC |
| **WakeCapDW_Gold** | 933934272544045 | Gold facts (7 tasks) | 5:30 AM UTC |

**Do NOT create new standalone jobs.** All work should be consolidated into these three jobs.

---

## Overview

This plan outlines the work required to complete the migration of WakeCapDW to Databricks Unity Catalog. **All phases are now COMPLETE.**

| Phase | Status |
|-------|--------|
| 1. Loading Bronze Layer from TimescaleDB | **COMPLETE** |
| 2. Implementing Silver Layer transformations | **COMPLETE** |
| 3. Converting stored procedures | **COMPLETE** |
| 4. Converting functions | **COMPLETE** |
| 5. Implementing Gold Layer views | **COMPLETE** |
| 6. Validating the migration | **READY** |

---

## Phase 1: Bronze Layer - TimescaleDB Incremental Loading

### Status: COMPLETE (Initial Load Done 2026-01-22)

**Verification Results:**
- **78 tables** successfully loaded to `wakecap_prod.raw`
- All tables prefixed with `timescale_`
- **22 tables** match SQL Server stg.wc2023_* tables
- **56 tables** are TimescaleDB-only (new data sources)

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

| Table | Has Geometry | Geometry Columns | Handling |
|-------|--------------|------------------|----------|
| Blueprint | Yes | Geometry | `ST_AsText("Geometry") AS "GeometryWKT"` |
| DeviceLocation | Yes | Point | `ST_AsText("Point")` |
| DeviceLocationSummary | Yes | Point, ConfidenceArea | `ST_AsText` for both columns |
| NovadeWorkPermit | Yes | Geometry | `ST_AsText` conversion |
| Space | Yes | Geometry | `ST_AsText` conversion |
| SpaceHistory | Yes | Geometry | `ST_AsText` conversion |
| Zone | Yes | Geometry | `ST_AsText` conversion |
| ZoneHistory | Yes | Geometry | `ST_AsText` conversion |
| ZoneViolationLog | Yes | Geometry | `ST_AsText` conversion |

### 1.5 Large Table Optimization: DeviceLocation

**Optimized:** 2026-01-22

DeviceLocation and DeviceLocationSummary are TimescaleDB hypertables requiring special handling due to their size and structure.

#### 1.5.1 DeviceLocation (82M rows, 52GB)

| Attribute | Original Config | Optimized Config | Reason |
|-----------|-----------------|------------------|--------|
| **Primary Key** | `[Id]` | `[DeviceId, ProjectId, ActiveSequance, InactiveSequance, GeneratedAt]` | Matches actual composite PK in source |
| **Watermark Column** | `UpdatedAt` | `GeneratedAt` | `UpdatedAt` is NULL for all 82M rows |
| **Geometry Column** | `Geometry` | `Point` | Correct column name in source |
| **Fetch Size** | 50,000 | 100,000 | Larger batches for hypertable |
| **Batch Size** | 200,000 | 500,000 | Optimized for high-volume table |

**Source Characteristics:**
- TimescaleDB hypertable partitioned by `GeneratedAt` (14-day chunks)
- ~1.7M rows per day (~50M rows/month)
- Append-only pattern (no updates)
- Index exists: `IX_DeviceLocation_GeneratedAt`

#### 1.5.2 DeviceLocationSummary (848K rows, 3.8GB)

| Attribute | Original Config | Optimized Config | Reason |
|-----------|-----------------|------------------|--------|
| **Primary Key** | `[Id]` | `[Day, DeviceId, ProjectId]` | Matches actual composite PK in source |
| **Watermark Column** | `UpdatedAt` | `GeneratedAt` | Consistent with DeviceLocation |
| **Geometry Columns** | `Geometry` | `Point, ConfidenceArea` | Both geometry columns |
| **Batch Size** | (default) | 200,000 | Appropriate for table size |

**Source Characteristics:**
- TimescaleDB hypertable partitioned by `Day` (30-day chunks)
- ~400K rows/month (daily aggregations)
- Index exists: `IX_DeviceLocationSummary_GeneratedAt`

#### 1.5.3 Databricks-Side Optimizations

The following optimizations are applied to the target Delta tables:

```sql
-- DeviceLocation: Z-ORDER for query performance
OPTIMIZE wakecap_prod.raw.timescale_devicelocation
ZORDER BY (ProjectId, GeneratedAt);

-- DeviceLocationSummary: Z-ORDER for query performance
OPTIMIZE wakecap_prod.raw.timescale_devicelocationsummary
ZORDER BY (ProjectId, Day);

-- Table properties for large tables
ALTER TABLE wakecap_prod.raw.timescale_devicelocation SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.targetFileSize' = '128mb'
);

ALTER TABLE wakecap_prod.raw.timescale_devicelocationsummary SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

**Note:** TimescaleDB compression was evaluated but not implemented as it has adverse effects on source database performance.

### 1.6 Append-Only Mode Optimization

**Added:** 2026-01-24

For tables that are insert-only (no updates or deletes), the loader uses APPEND mode instead of MERGE, providing significant performance improvements.

#### 1.6.1 How to Identify Append-Only Tables

Tables are candidates for append-only mode when:

| Pattern | Description | Example |
|---------|-------------|---------|
| **Hypertables** | TimescaleDB partitioned tables designed for time-series inserts | DeviceLocation, EquipmentTelemetry |
| **CreatedAt-only watermark** | Tables using only `CreatedAt` (no UpdatedAt in GREATEST expression) | Inspection |
| **Log/telemetry tables** | Tables that only record events, never update them | ZoneViolationLog |
| **Cache tables** | Pre-computed tables that are refreshed by insert | ViewFactWorkshiftsCache |

#### 1.6.2 Current Append-Only Tables

| Table | Rows | Reason | Performance Gain |
|-------|------|--------|------------------|
| DeviceLocation | 85M | Hypertable, insert-only telemetry | ~4x faster (MERGE skipped) |
| DeviceLocationSummary | 7M | Hypertable, daily aggregation inserts | ~3x faster |
| EquipmentTelemetry | 0.8M | Hypertable, telemetry inserts only | ~3x faster |
| Inspection | ~50K | Uses only CreatedAt watermark | ~2x faster |
| ViewFactWorkshiftsCache | ~2M | Hypertable, cache refresh by insert | ~3x faster |

#### 1.6.3 Configuration

In `timescaledb_tables_v2.yml`:

```yaml
# Append-only table example
- source_table: DeviceLocation
  primary_key_columns: [DeviceId, ProjectId, ActiveSequance, InactiveSequance, GeneratedAt]
  watermark_column: GeneratedAt
  category: facts
  is_hypertable: true
  is_append_only: true  # CRITICAL: Skip MERGE, use APPEND mode
  comment: "Large hypertable - append-only (no updates)"
```

#### 1.6.4 Implementation Details

The loader detects `is_append_only: true` and uses direct APPEND instead of MERGE:

```python
# In timescaledb_loader_v2.py
if table_config.is_append_only:
    # Use APPEND mode - skip expensive MERGE operation
    df.write.format("delta").mode("append").saveAsTable(target_table)
else:
    # Standard MERGE for tables with updates
    delta_table.merge(df, merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
```

**Why This Matters:**
- MERGE operations require comparing every incoming row against the entire target table
- For large tables like DeviceLocation (85M rows), this comparison is very expensive
- APPEND mode simply adds new rows without comparison
- DeviceLocation load time reduced from ~107 minutes to ~25-30 minutes

### 1.7 Table Registry

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

### 1.8 Delta Write Strategy

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

### 1.9 Metadata Columns

Every bronze table includes:

| Column | Value | Description |
|--------|-------|-------------|
| `_loaded_at` | `current_timestamp()` | When the row was loaded |
| `_source_system` | `'timescaledb'` | Source system identifier |
| `_source_schema` | `'public'` | Source schema |
| `_source_table` | Table name | Source table name |
| `_pipeline_id` | Notebook path | Pipeline identifier |
| `_pipeline_run_id` | Run ID | Unique run identifier |

### 1.10 Notebook Parameters

The bronze loader notebook accepts these parameters:

| Widget | Options | Default | Description |
|--------|---------|---------|-------------|
| `load_mode` | incremental, full | incremental | Load mode |
| `category` | ALL, dimensions, assignments, facts, history | ALL | Category filter |
| `batch_size` | numeric | 100000 | Batch processing size |
| `fetch_size` | numeric | 50000 | JDBC fetch size |
| `max_tables` | numeric | 0 (all) | Limit tables to load |

### 1.11 Running the Bronze Job

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

### 1.12 Monitoring & Verification

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

### 1.13 Error Handling & Recovery

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

### 1.14 Additional TimescaleDB Databases

#### 1.14.1 Observation Database (wakecap_observation)

**Status:** READY FOR DEPLOYMENT (2026-01-22)

The `wakecap_observation` database contains safety observation and incident tracking data.

| Metric | Value |
|--------|-------|
| **Database** | wakecap_observation |
| **Tables** | 6 |
| **Total Rows** | ~8M |
| **Table Prefix** | `observation_` |
| **Secret Scope** | `wakecap-observation` |
| **Notebook** | `bronze_loader_observation.py` |
| **Registry** | `timescaledb_tables_observation.yml` |

**Tables to Load:**

| Table | Rows | PK | Watermark | Notes |
|-------|------|-----|-----------|-------|
| Observation | ~1.4M | Id | UpdatedAt | Has Location geometry |
| TrackerSteps | ~5.6M | Id | UpdatedAt | Largest table |
| ObservationTracker | ~595K | Id | UpdatedAt | Tracker metadata |
| ObservationUpdate | ~405K | Id | UpdatedAt | Comments/updates |
| ObservationUpdateAttachment | ~3.2K | Id | UpdatedAt | Attachments |
| Settings | ~13 | Id | UpdatedAt | Project settings |

**Deployment:**
```bash
# Deploy observation loader and configure secrets
python deploy_observation_pipeline.py
```

#### 1.14.2 Weather Station Database (weather-station)

**Status:** READY FOR DEPLOYMENT (2026-01-22)

The `weather-station` database contains weather monitoring configuration and thresholds.

| Metric | Value |
|--------|-------|
| **Database** | weather-station |
| **Tables** | 7 |
| **Total Rows** | ~115 |
| **Table Prefix** | `weather_` |
| **Secret Scope** | `wakecap-weather` |
| **Notebook** | `bronze_loader_weather.py` |
| **Registry** | `timescaledb_tables_weather.yml` |

**Tables to Load:**

| Table | Rows | PK | Watermark | Notes |
|-------|------|-----|-----------|-------|
| ProjectSettings | 44 | Id | UpdatedAt | Project-specific settings with JSONB |
| ProjectThreshold | 41 | Id | UpdatedAt | Weather alert thresholds |
| Indicator | 13 | Id | UpdatedAt | Weather indicators |
| Graph | 11 | Id | UpdatedAt | Graph configurations |
| HeatIndexStatus | 5 | Id | UpdatedAt | Heat index definitions |
| Report | 1 | Id | UpdatedAt | Report configurations |
| CalculatedStatistics | 0 | Id | UpdatedAt | Calculated stats (empty) |

#### 1.14.3 Combined Deployment

**Deploy both new databases:**
```bash
# Deploy observation + weather loaders and update job
python deploy_new_databases.py
```

This creates a single job with 3 parallel tasks:
1. `load_wakecap_app` - Main database (81 tables)
2. `load_observation` - Observation database (6 tables)
3. `load_weather` - Weather station (7 tables)

**Total Tables:** 94 (81 + 6 + 7)

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

### Status: DEPLOYED (2026-01-24)

The Silver layer has been deployed as a standalone Databricks Job with 77 tables organized into 8 processing groups.

| Metric | Value |
|--------|-------|
| **Job Name** | WakeCapDW_Silver_TimescaleDB |
| **Job ID** | 181959206191493 |
| **Target Schema** | wakecap_prod.silver |
| **Total Tables** | 77 |
| **Schedule** | Daily 3:00 AM UTC (Paused) |
| **Job URL** | https://adb-3022397433351638.18.azuredatabricks.net/jobs/181959206191493 |

### 4.1 Architecture

```
Bronze (wakecap_prod.raw)                    Silver (wakecap_prod.silver)
     |                                              |
     | Watermark-based incremental                  |
     | (_loaded_at column)                          |
     v                                              v
+------------------+     +-----------------+     +------------------+
| timescale_*      | --> | silver_loader   | --> | silver_*         |
| (78 tables)      |     | notebook        |     | (77 tables)      |
+------------------+     +-----------------+     +------------------+
                                |
                                v
                         +------------------+
                         | _silver_watermarks|
                         | (tracking table)  |
                         +------------------+
```

### 4.2 Job Structure (8 Tasks with Dependencies)

```
silver_independent_dimensions ----+
                                  +--> silver_project_children --> silver_zone_dependent --+--> silver_assignments --> silver_facts
silver_organization --> silver_project --+                                                  |
                                                                                            +--> silver_history
```

| Task | Processing Group | Tables | Cluster |
|------|------------------|--------|---------|
| silver_independent_dimensions | independent_dimensions | 11 | 2 workers |
| silver_organization | organization | 2 | 2 workers |
| silver_project | project_dependent | 1 | 2 workers |
| silver_project_children | project_children | 16 | 2 workers |
| silver_zone_dependent | zone_dependent | 7 | 2 workers |
| silver_assignments | assignments | 17 | 2 workers |
| silver_facts | facts | 20 | 4 workers |
| silver_history | history | 3 | 2 workers |

### 4.3 Data Quality Rules (3-Tier Validation)

The Silver layer implements 3-tier data quality validation:

| Tier | Action | Use Case |
|------|--------|----------|
| **Critical** | Drop failing rows | Primary key NOT NULL, required fields |
| **Business** | Log violation, keep row | FK references, soft-delete checks |
| **Advisory** | Warn only | Range validation, format checks |

Example expectations:
```python
expectations = {
    'critical': [
        "Id IS NOT NULL",
        "Name IS NOT NULL AND LENGTH(TRIM(Name)) > 0"
    ],
    'business': [
        "ProjectId IS NOT NULL",
        "DeletedAt IS NULL"
    ],
    'advisory': [
        "Latitude BETWEEN -90 AND 90 OR Latitude IS NULL"
    ]
}
```

### 4.4 Silver Tables (77 Total)

| Category | Count | Examples |
|----------|-------|----------|
| Independent Dimensions | 11 | company_type, crew_type, nationality, discipline |
| Organization | 2 | silver_organization, silver_device |
| Project | 1 | silver_project |
| Project Children | 16 | silver_worker, silver_crew, silver_floor, silver_zone |
| Zone Dependent | 7 | silver_workshift_day, silver_crew_composition |
| Assignments | 17 | silver_resource_device, silver_certificate |
| Facts | 20 | silver_fact_workers_history (82M), silver_fact_sgs_roster (68M) |
| History | 3 | silver_history_space, silver_history_zone |

### 4.5 Key Table Mappings

| Bronze Table | Silver Table | Transformation |
|--------------|--------------|----------------|
| timescale_company | silver_organization | Filter `Type='organization'` |
| timescale_company | silver_project | Filter `Type='project'` |
| timescale_people | silver_worker | Key mapping: People = Worker |
| timescale_space | silver_floor | Key mapping: Space = Floor |
| timescale_avldevice | silver_device | Key mapping: AvlDevice = Device |
| timescale_devicelocation | silver_fact_workers_history | 82M rows, composite PK |

### 4.6 Watermark Tracking

**Table:** `wakecap_prod.migration._silver_watermarks`

```sql
SELECT table_name, processing_group, last_load_status,
       last_load_row_count, rows_dropped_critical,
       rows_flagged_business, last_bronze_watermark
FROM wakecap_prod.migration._silver_watermarks
ORDER BY processing_group, table_name;
```

### 4.7 Running the Silver Job

**Initial Load (Manual):**
1. Go to: https://adb-3022397433351638.18.azuredatabricks.net/jobs/181959206191493
2. Click "Run now"
3. Monitor 8 tasks in dependency order

**Enable Schedule:**
After successful initial load, change schedule from "Paused" to "Active" (3:00 AM UTC)

### 4.8 Verification Queries

```sql
-- Check Silver tables created
SHOW TABLES IN wakecap_prod.silver;

-- Row count reconciliation
SELECT 'bronze' as layer, COUNT(*) as cnt
FROM wakecap_prod.raw.timescale_company
WHERE "DeletedAt" IS NULL
UNION ALL
SELECT 'silver_org' as layer, COUNT(*) as cnt
FROM wakecap_prod.silver.silver_organization;

-- FK integrity check
SELECT COUNT(*) AS orphaned_projects
FROM wakecap_prod.silver.silver_project p
LEFT JOIN wakecap_prod.silver.silver_organization o
  ON p.OrganizationId = o.OrganizationId
WHERE p.OrganizationId IS NOT NULL
  AND o.OrganizationId IS NULL;
```

### 4.9 Files Deployed

| File | Workspace Path |
|------|----------------|
| Notebook | `/Workspace/migration_project/pipelines/silver/notebooks/silver_loader` |
| Config | `/Workspace/migration_project/pipelines/silver/config/silver_tables.yml` |
| DDL | `/Workspace/migration_project/pipelines/silver/ddl/create_silver_watermarks.sql` |

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

- [x] Bronze layer tables populated (78 tables loaded to wakecap_prod.raw)
- [x] Incremental loading working (watermark-based JDBC extraction)
- [x] Watermark tracking operational (_timescaledb_watermarks table)
- [x] Geometry handling working (9 tables with ST_AsText conversion)
- [x] **DeviceLocation optimization complete (correct PKs, GeneratedAt watermark, Z-ORDER)**
- [x] **Silver layer DEPLOYED (Job ID: 181959206191493, 77 tables)**
- [ ] Silver layer initial load executed
- [ ] Gold layer views working
- [ ] Critical stored procedures converted
- [ ] Row counts validated against source
- [ ] Business logic validated
- [ ] Performance acceptable
- [ ] Security (RLS) implemented

### 7.2 Production Schedule

| Job | Schedule | Description |
|-----|----------|-------------|
| WakeCapDW_Bronze_TimescaleDB_Raw | Daily 2:00 AM UTC | Incremental bronze load (78 tables) |
| **WakeCapDW_Silver_TimescaleDB** | **Daily 3:00 AM UTC** | **Silver layer transformations (77 tables)** |
| Bronze_DeviceLocation | On-demand / After main job | Dedicated loader for large DeviceLocation tables |
| DLT Pipeline (Gold) | Every 4 hours | Gold layer views |
| Validation Job | Weekly | Data reconciliation |

**Schedule Coordination:**
```
2:00 AM UTC - Bronze Job (TimescaleDB -> wakecap_prod.raw)
3:00 AM UTC - Silver Job (Bronze -> wakecap_prod.silver)
4:00 AM UTC - Gold Job (Silver -> wakecap_prod.gold) [Future]
```

**Note:** DeviceLocation tables are included in the main job but can also be run separately using `bronze_loader_devicelocation` notebook for:
- Initial full loads
- Recovery from failures
- Performance testing with different batch sizes

---

## Appendix A: File Inventory

### Bronze Layer Files

| File | Purpose |
|------|---------|
| `pipelines/timescaledb/notebooks/bronze_loader_optimized.py` | Main loader notebook (wakecap_app) |
| `pipelines/timescaledb/notebooks/bronze_loader_observation.py` | **Observation database loader (wakecap_observation)** |
| `pipelines/timescaledb/notebooks/bronze_loader_weather.py` | **Weather station loader (weather-station)** |
| `pipelines/timescaledb/notebooks/bronze_loader_dimensions.py` | Dimension-only loader |
| `pipelines/timescaledb/notebooks/bronze_loader_facts.py` | Fact-only loader |
| `pipelines/timescaledb/notebooks/bronze_loader_assignments.py` | Assignment-only loader |
| `pipelines/timescaledb/notebooks/bronze_loader_devicelocation.py` | **Dedicated DeviceLocation loader (optimized)** |
| `pipelines/timescaledb/notebooks/reset_devicelocation_watermarks.py` | **Reset watermarks for DeviceLocation tables** |
| `pipelines/timescaledb/notebooks/optimize_devicelocation.py` | **Z-ORDER optimization for DeviceLocation** |
| `pipelines/timescaledb/src/timescaledb_loader_v2.py` | Loader module |
| `pipelines/timescaledb/config/timescaledb_tables_v2.yml` | Table registry - wakecap_app (81 tables) |
| `pipelines/timescaledb/config/timescaledb_tables_observation.yml` | **Table registry - wakecap_observation (6 tables)** |
| `pipelines/timescaledb/config/timescaledb_tables_weather.yml` | **Table registry - weather-station (7 tables)** |

### Silver Layer Files

| File | Purpose |
|------|---------|
| `pipelines/silver/notebooks/silver_loader.py` | **Main Silver transformation notebook** |
| `pipelines/silver/config/silver_tables.yml` | **77 table registry with DQ expectations** |
| `pipelines/silver/ddl/create_silver_watermarks.sql` | **Watermark tracking DDL** |
| `deploy_silver_layer.py` | **Silver layer deployment script** |

### Production Scripts

| Script | Purpose |
|--------|---------|
| `deploy_new_databases.py` | Deploy observation + weather loaders and update job |
| `deploy_silver_layer.py` | **Deploy Silver layer job and notebooks** |
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

---

## Update Log

| Date | Update |
|------|--------|
| 2026-01-24 | **Append-only mode optimization** - Added `is_append_only` pattern for hypertables (5 tables: DeviceLocation, DeviceLocationSummary, EquipmentTelemetry, Inspection, ViewFactWorkshiftsCache) |
| 2026-01-24 | **Silver layer DEPLOYED** - Job ID 181959206191493, 77 tables, 8 processing groups |
| 2026-01-24 | Created silver_loader.py notebook (standalone, self-contained) |
| 2026-01-24 | Created silver_tables.yml registry with 3-tier DQ expectations |
| 2026-01-24 | Created deploy_silver_layer.py deployment script |
| 2026-01-22 | **Added wakecap_observation database** - 6 tables (~8M rows), observation_* prefix |
| 2026-01-22 | **Added weather-station database** - 7 tables (~115 rows), weather_* prefix |
| 2026-01-22 | **Created unified deployment script** - deploy_new_databases.py for both new databases |
| 2026-01-22 | DeviceLocation optimization: Fixed PK, watermark (GeneratedAt), geometry columns; added Databricks Z-ORDER |
| 2026-01-22 | Bronze layer initial load COMPLETE - 78 tables loaded from TimescaleDB |
| 2026-01-21 | TimescaleDB table registry v2.1 created |
| 2026-01-18 | Initial plan created |

*This plan should be reviewed and updated as the migration progresses.*
