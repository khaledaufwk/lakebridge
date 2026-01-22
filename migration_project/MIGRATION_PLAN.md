# WakeCapDW Migration Plan - Remaining Objects

**Created:** 2026-01-18
**Last Updated:** 2026-01-20

---

## Overview

This plan outlines the work required to complete the migration of WakeCapDW to Databricks Unity Catalog. The infrastructure and transpilation phases are complete. The remaining work focuses on:

1. Loading Bronze Layer from TimescaleDB (incremental)
2. Implementing Silver Layer transformations
3. Converting stored procedures
4. Converting functions
5. Implementing Gold Layer views
6. Validating the migration

---

## Phase 1: Bronze Layer Loading from TimescaleDB (Incremental)

### Architecture Overview

**Data Source:** TimescaleDB (PostgreSQL-based time-series database)
**Loading Strategy:** Incremental loading using watermark columns (new records only)
**Target:** Bronze Layer in Databricks Lakehouse

```
┌─────────────────────┐      ┌───────────────────────────┐      ┌─────────────────────┐
│    TimescaleDB      │      │   Incremental Loader      │      │   Bronze Layer      │
│  (Source Database)  │ ───► │  (Watermark-based)        │ ───► │  (Delta Tables)     │
│                     │      │  - Track last loaded ID   │      │  - Append-only      │
│                     │      │  - Only fetch new records │      │  - Raw data         │
└─────────────────────┘      └───────────────────────────┘      └─────────────────────┘
```

### 1.1 TimescaleDB Connection Configuration

**Priority:** CRITICAL
**Effort:** Low

Configure TimescaleDB credentials in `migration_project/credentials_template.yml`:

```yaml
timescaledb:
  host: YOUR_TIMESCALE_HOST_HERE
  port: 5432
  database: YOUR_DATABASE_NAME_HERE
  user: YOUR_TIMESCALE_USERNAME_HERE
  password: YOUR_TIMESCALE_PASSWORD_HERE
  sslmode: require
  incremental:
    enabled: true
    watermark_column: created_at
    watermark_type: timestamp
    initial_watermark: null
```

### 1.2 Incremental Loading Strategy

**Method:** Watermark-based incremental extraction

**How it works:**
1. Track the last successfully loaded watermark value (timestamp or ID)
2. Query TimescaleDB for records WHERE `watermark_column` > `last_watermark`
3. Append new records to Raw Zone Delta tables
4. Update watermark state after successful load

**Watermark Storage:** Delta table `wakecap_prod.migration._watermarks`

```sql
CREATE TABLE IF NOT EXISTS wakecap_prod.migration._watermarks (
    table_name STRING,
    watermark_column STRING,
    last_watermark_value STRING,
    watermark_type STRING,
    last_load_timestamp TIMESTAMP,
    records_loaded BIGINT
);
```

### 1.3 Incremental Load Implementation

**DLT Pattern for Incremental Reads:**

```python
import dlt
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
import yaml

def load_credentials():
    """Load TimescaleDB credentials from YAML file."""
    with open("/Workspace/migration_project/credentials_template.yml") as f:
        return yaml.safe_load(f)

def get_timescale_jdbc_url(config):
    """Build JDBC URL for TimescaleDB."""
    ts = config['timescaledb']
    return f"jdbc:postgresql://{ts['host']}:{ts['port']}/{ts['database']}?sslmode={ts['sslmode']}"

def get_last_watermark(spark, table_name):
    """Get last loaded watermark value for a table."""
    try:
        result = spark.sql(f"""
            SELECT last_watermark_value, watermark_type
            FROM wakecap_prod.migration._watermarks
            WHERE table_name = '{table_name}'
        """).first()
        return result if result else None
    except:
        return None

def update_watermark(spark, table_name, new_watermark, watermark_column, watermark_type, records_loaded):
    """Update watermark after successful load."""
    spark.sql(f"""
        MERGE INTO wakecap_prod.migration._watermarks AS target
        USING (SELECT '{table_name}' as table_name) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            last_watermark_value = '{new_watermark}',
            last_load_timestamp = current_timestamp(),
            records_loaded = {records_loaded}
        WHEN NOT MATCHED THEN INSERT
            (table_name, watermark_column, last_watermark_value, watermark_type, last_load_timestamp, records_loaded)
        VALUES ('{table_name}', '{watermark_column}', '{new_watermark}', '{watermark_type}', current_timestamp(), {records_loaded})
    """)

@dlt.table(
    name="raw_timescale_observations",
    comment="Raw observations loaded incrementally from TimescaleDB"
)
def raw_timescale_observations():
    """Load new observations from TimescaleDB using watermark-based incremental strategy."""
    config = load_credentials()
    jdbc_url = get_timescale_jdbc_url(config)
    ts = config['timescaledb']

    # Get last watermark
    last_watermark = get_last_watermark(spark, "observations")

    # Build incremental query
    if last_watermark and ts['incremental']['enabled']:
        watermark_col = ts['incremental']['watermark_column']
        if ts['incremental']['watermark_type'] == 'timestamp':
            query = f"(SELECT * FROM observations WHERE {watermark_col} > '{last_watermark.last_watermark_value}'::timestamp) AS t"
        else:
            query = f"(SELECT * FROM observations WHERE {watermark_col} > {last_watermark.last_watermark_value}) AS t"
    else:
        query = "observations"

    # Read from TimescaleDB
    df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", ts['user'])
        .option("password", ts['password'])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return df.withColumn("_loaded_at", current_timestamp())
```

### 1.4 Tables for Incremental Loading from TimescaleDB

| Table | Watermark Column | Type | Priority |
|-------|------------------|------|----------|
| observations | created_at | timestamp | HIGH |
| worker_history | modified_at | timestamp | HIGH |
| device_readings | reading_time | timestamp | HIGH |
| location_events | event_time | timestamp | MEDIUM |
| shift_records | shift_date | timestamp | MEDIUM |

### 1.5 Incremental Load Schedule

| Load Type | Frequency | Tables |
|-----------|-----------|--------|
| Real-time | Every 5 min | observations, device_readings |
| Hourly | Every hour | worker_history, location_events |
| Daily | Once per day | shift_records, reference data |

### 1.6 Test Single Table Ingestion

**Priority:** HIGH
**Effort:** Low

**Target Tables:** Start with small dimension tables
1. `shift_types` (smallest dimension)
2. `device_models`
3. `observation_types`

**Steps:**
1. Configure TimescaleDB connection in credentials file
2. Run DLT pipeline with single table
3. Verify data in `wakecap_prod.migration.bronze_shift_types`
4. Check row counts match source

### 1.7 Dimension Tables Ingestion

**Priority:** HIGH
**Effort:** Medium

**Order of ingestion (dependency-based):**

| Batch | Tables | Est. Rows |
|-------|--------|-----------|
| 1 | Company, Organization | Low |
| 2 | Project, Department, Trade, WorkerRole | Low |
| 3 | Crew, Worker, Device, DeviceModel | Medium |
| 4 | Location, Manager, Shift, ShiftType | Low |
| 5 | Task, Workshift, ObservationType, ReportType | Low |

### 1.8 Assignment Tables Ingestion

**Priority:** HIGH
**Effort:** Medium

**Tables:**
- CrewAssignments
- DeviceAssignments
- LocationAssignments
- ManagerAssignments
- ProjectAssignments
- TradeAssignments
- WorkerRoleAssignments
- WorkshiftAssignments

### 1.9 Fact Tables Ingestion

**Priority:** HIGH
**Effort:** High (large data volumes)

**Order of ingestion:**
| Batch | Table | Expected Size |
|-------|-------|---------------|
| 1 | FactProgress | Medium |
| 2 | FactReportedAttendance | High |
| 3 | FactWorkersTasks | Medium |
| 4 | FactWorkersContacts | High |
| 5 | FactWorkersShifts | High |
| 6 | FactWorkersHistory | Very High |
| 7 | FactObservations | Very High |
| 8 | FactWeatherObservations | Medium |

**Considerations:**
- FactObservations and FactWorkersHistory may require incremental loading
- Consider partitioning by date for large fact tables
- Monitor cluster performance during ingestion

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

**Steps:**
1. Analyze current predicate logic
2. Implement as Unity Catalog row-level security
3. Apply filters to relevant tables/views

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
| silver_Worker | bronze_Worker | Dedupe, clean names, validate IDs |
| silver_Project | bronze_Project | Validate status, clean names |
| silver_Crew | bronze_Crew | Validate references |
| silver_Device | bronze_Device | Validate model references |
| silver_Organization | bronze_Organization | Hierarchy validation |
| silver_FactObservations | bronze_FactObservations | Date validation, deduplication |
| silver_FactWorkersShifts | bronze_FactWorkersShifts | Time calculations, validation |

---

## Phase 5: Gold Layer Implementation

### 5.1 Business Views

Implement remaining 24 views not yet configured:

| View | Priority | Dependencies |
|------|----------|--------------|
| gold_vwFactWorkersHistory | HIGH | silver_FactWorkersHistory |
| gold_vwFactWorkersContacts | HIGH | silver_FactWorkersContacts |
| gold_vwFactWorkersTasks | MEDIUM | silver_FactWorkersTasks |
| gold_vwFactObservations | HIGH | silver_FactObservations |
| gold_vwFactProgress | MEDIUM | silver_FactProgress |
| gold_vwFactWeatherObservations | LOW | silver_FactWeatherObservations |
| gold_vwLocation | MEDIUM | silver_Location |
| gold_vwLocationAssignment | MEDIUM | silver_LocationAssignment |
| gold_vwManager | MEDIUM | silver_Manager |
| gold_vwManagerAssignment | MEDIUM | silver_ManagerAssignment |
| gold_vwDepartment | LOW | silver_Department |
| gold_vwTrade | LOW | silver_Trade |
| gold_vwTradeAssignment | LOW | silver_TradeAssignment |
| gold_vwWorkerRole | LOW | silver_WorkerRole |
| gold_vwWorkerRoleAssignment | LOW | silver_WorkerRoleAssignment |
| gold_vwTask | MEDIUM | silver_Task |
| gold_vwCompany | LOW | silver_Company |
| gold_vwDeviceModel | LOW | silver_DeviceModel |
| gold_vwObservationType | LOW | silver_ObservationType |
| gold_vwReportType | LOW | silver_ReportType |
| gold_vwDeviceAssignment | MEDIUM | silver_DeviceAssignment |
| gold_vwDeviceAssignment_Continuous | MEDIUM | silver_DeviceAssignment |
| gold_vwProjectAssignment | MEDIUM | silver_ProjectAssignment |
| gold_vwWorkshiftAssignments | MEDIUM | silver_WorkshiftAssignments |

---

## Phase 6: Testing and Reconciliation

### 6.1 Row Count Validation

```python
# For each table - compare TimescaleDB source to Databricks target
source_count = spark.read.jdbc(timescale_jdbc_url, table).count()
target_count = spark.table(f"wakecap_prod.migration.{table}").count()
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
- `geometry` / `geography` → Custom handling (H3 or GeoJSON STRING)

### 6.3 Business Logic Validation

Compare key aggregations:
- Total workers by organization
- Total observations by date range
- Total shifts by project
- Sum of hours worked

### 6.4 Performance Testing

- Query performance comparison
- Dashboard load times
- Concurrent user testing

---

## Phase 7: Production Deployment

### 7.1 Pre-Production Checklist

- [ ] All bronze tables populated
- [ ] Silver layer transformations complete
- [ ] Gold layer views working
- [ ] Critical stored procedures converted
- [ ] Row counts validated
- [ ] Business logic validated
- [ ] Performance acceptable
- [ ] Security (RLS) implemented

### 7.2 Cutover Steps

1. Disable development mode in pipeline
2. Set up production scheduling
3. Configure alerting and monitoring
4. Update downstream applications to use Databricks as data source
5. Run parallel validation for 1-2 weeks
6. Transition to production incremental loading from TimescaleDB

---

## Appendix A: Object Inventory

### Tables Not Yet in Pipeline (112 remaining)

The following tables need to be added to the DLT pipeline configuration:

```
AlertConfig, AlertHistory, AuditLog,
BatchJob, BatchJobHistory,
Calendar, CalendarException,
Configuration, ConfigurationHistory,
Currency, CurrencyRate,
DataQualityRule, DataQualityResult,
EmailTemplate, EmailLog,
FeatureFlag, FeatureFlagHistory,
Gateway, GatewayConfig, GatewayLog,
Holiday, HolidayCalendar,
Integration, IntegrationConfig, IntegrationLog,
JobSchedule, JobScheduleHistory,
KeyValue, KeyValueHistory,
Language, LanguageTranslation,
Metric, MetricHistory,
Notification, NotificationConfig, NotificationLog,
Permission, PermissionGroup, PermissionRole,
QueueItem, QueueItemHistory,
Report, ReportConfig, ReportSchedule,
Setting, SettingHistory,
SystemLog, SystemMetric,
Tenant, TenantConfig,
User, UserPreference, UserSession,
Version, VersionHistory,
Webhook, WebhookConfig, WebhookLog,
Zone, ZoneConfig, ZoneAssignment
... and more
```

### Stored Procedures Full List (70)

See `migration_project/source_sql/stored_procedures/` for complete list.

### Functions Full List (23)

See `migration_project/source_sql/functions/` for complete list.

---

## Appendix B: Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Large table ingestion timeout | Medium | High | Implement incremental loading |
| Spatial function accuracy | Medium | Medium | Validate against source |
| Performance degradation | Low | High | Performance testing before cutover |
| Data loss during migration | Low | Critical | Full backup, reconciliation |
| Security gap (RLS) | Medium | High | Implement UC row filters |

---

## Appendix C: Resource Requirements

### Databricks Cluster Sizing

- **Development:** Standard_DS3_v2 (4 cores, 14GB RAM)
- **Production:** Standard_DS4_v2 or larger for fact table processing
- **Autoscaling:** 2-8 workers for variable workloads

### Estimated Costs

- Development cluster: ~$X/hour
- Production cluster: ~$X/hour
- Storage: ~$X/month for Delta tables

---

*This plan should be reviewed and updated as the migration progresses.*
