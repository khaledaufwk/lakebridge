# WakeCapDW Migration - Deployment Guide

This guide provides step-by-step instructions for deploying the WakeCapDW migration to production.

---

## ⚠️ IMPORTANT: Production Databricks Jobs

**All future development work MUST be added to one of these three production jobs:**

| Job Name | Job ID | Purpose | Schedule | Workspace Path |
|----------|--------|---------|----------|----------------|
| **WakeCapDW_Bronze_TimescaleDB_Raw** | 28181369160316 | Bronze layer ingestion from TimescaleDB | 2:00 AM UTC | `/Workspace/migration_project/pipelines/timescaledb/notebooks/` |
| **WakeCapDW_Silver_TimescaleDB** | 181959206191493 | Silver layer transformations (9 tasks) | 3:00 AM UTC | `/Workspace/migration_project/pipelines/silver/notebooks/` |
| **WakeCapDW_Gold** | 933934272544045 | Gold layer facts and aggregations (7 tasks) | 5:30 AM UTC | `/Workspace/migration_project/pipelines/gold/notebooks/` |

### Job URLs
- **Bronze:** https://adb-3022397433351638.18.azuredatabricks.net/#job/28181369160316
- **Silver:** https://adb-3022397433351638.18.azuredatabricks.net/#job/181959206191493
- **Gold:** https://adb-3022397433351638.18.azuredatabricks.net/#job/933934272544045

### Adding New Work

When adding new notebooks or transformations:

1. **Determine the appropriate layer:**
   - **Bronze** - Raw data ingestion from source systems
   - **Silver** - Data cleansing, deduplication, standardization
   - **Gold** - Business logic, aggregations, facts, dimensions for reporting

2. **Create your notebook** in the appropriate local directory:
   - Bronze: `migration_project/pipelines/timescaledb/notebooks/`
   - Silver: `migration_project/pipelines/silver/notebooks/`
   - Gold: `migration_project/pipelines/gold/notebooks/`

3. **Upload to Databricks** using the corresponding workspace path

4. **Add as a task** to the appropriate job with correct dependencies

5. **Do NOT create new standalone jobs** - all work should be consolidated into these three jobs

### Pipeline Execution Order

```
WakeCapDW_Bronze_TimescaleDB_Raw (2:00 AM UTC)
         │
         ▼
WakeCapDW_Silver_TimescaleDB (3:00 AM UTC)
         │
         ▼
WakeCapDW_Gold (5:30 AM UTC)
```

---

## Prerequisites

### Azure Resources
- TimescaleDB instance with wakecap_app database
- Databricks workspace with Unity Catalog enabled
- Azure Key Vault for secrets management

### Access Requirements
- Databricks workspace admin access
- TimescaleDB read access (PostgreSQL)
- Azure Key Vault access

### Tools
- Databricks CLI (`databricks`)
- Python 3.9+ with `databricks-sdk` package

---

## Phase 1: Bronze Layer - TimescaleDB Ingestion

**Status: COMPLETE (Initial Load Done 2026-01-22)**

| Metric | Value |
|--------|-------|
| Tables Loaded | 78 |
| Target Schema | wakecap_prod.raw |
| Table Prefix | timescale_* |
| Load Method | JDBC + Watermark-based incremental |
| Geometry Tables | 9 (with ST_AsText conversion) |

The migration uses **Databricks notebooks with JDBC** to read directly from TimescaleDB, using watermark-based incremental extraction.

### Architecture

```
┌─────────────────┐     JDBC      ┌─────────────────┐
│   TimescaleDB   │ ────────────> │   Databricks    │
│   wakecap_app   │  (PostgreSQL) │   Delta Lake    │
└─────────────────┘               └─────────────────┘
                                        │
                                        ▼
                              ┌─────────────────────┐
                              │  wakecap_prod.raw   │
                              │  (81 Delta Tables)  │
                              │  timescale_*        │
                              └─────────────────────┘
```

### 1.1 Configure TimescaleDB Secrets

Create the secret scope and store TimescaleDB credentials:

```bash
# Create secret scope (if not exists)
databricks secrets create-scope wakecap-timescale

# Store TimescaleDB credentials
databricks secrets put-secret wakecap-timescale host --string-value "your-timescale-host.timescaledb.io"
databricks secrets put-secret wakecap-timescale port --string-value "5432"
databricks secrets put-secret wakecap-timescale database --string-value "wakecap_app"
databricks secrets put-secret wakecap-timescale username --string-value "your-username"
databricks secrets put-secret wakecap-timescale password --string-value "your-password"
```

Or via Azure Key Vault:

```bash
# Link to Azure Key Vault
databricks secrets create-scope wakecap-timescale --scope-backend-type AZURE_KEYVAULT \
    --resource-id /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault}

# Add secrets to Key Vault
az keyvault secret set --vault-name wakecap24 --name timescale-host --value "your-host"
az keyvault secret set --vault-name wakecap24 --name timescale-password --value "your-password"
```

### 1.2 Deploy Bronze Loader Notebooks

**Option A: Python Script**
```bash
cd migration_project
python deploy_timescaledb_bronze.py
```

**Option B: Databricks CLI**
```bash
# Create workspace folders
databricks workspace mkdirs /Workspace/migration_project/pipelines/timescaledb/notebooks
databricks workspace mkdirs /Workspace/migration_project/pipelines/timescaledb/src
databricks workspace mkdirs /Workspace/migration_project/pipelines/timescaledb/config

# Upload notebooks
databricks workspace import pipelines/timescaledb/notebooks/bronze_loader_optimized.py \
    /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_optimized \
    --format SOURCE --language PYTHON --overwrite

databricks workspace import pipelines/timescaledb/notebooks/bronze_loader_dimensions.py \
    /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_dimensions \
    --format SOURCE --language PYTHON --overwrite

databricks workspace import pipelines/timescaledb/notebooks/bronze_loader_facts.py \
    /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_facts \
    --format SOURCE --language PYTHON --overwrite

databricks workspace import pipelines/timescaledb/notebooks/bronze_loader_assignments.py \
    /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_assignments \
    --format SOURCE --language PYTHON --overwrite

# Upload source modules
databricks workspace import pipelines/timescaledb/src/timescaledb_loader_v2.py \
    /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2 \
    --format SOURCE --language PYTHON --overwrite

# Upload config
databricks workspace import pipelines/timescaledb/config/timescaledb_tables_v2.yml \
    /Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables_v2.yml \
    --format AUTO --overwrite
```

### 1.3 Create Bronze Job

Create the Databricks job for bronze layer loading:

```bash
databricks jobs create --json '{
  "name": "WakeCapDW_Bronze_TimescaleDB_Raw",
  "tasks": [
    {
      "task_key": "bronze_load_all",
      "notebook_task": {
        "notebook_path": "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_optimized",
        "base_parameters": {
          "load_mode": "incremental",
          "category": "ALL"
        }
      },
      "job_cluster_key": "bronze_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "bronze_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.preview.enabled": "true"
        }
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}'
```

### 1.4 Run Initial Full Load

1. Open Databricks > Workflows > Jobs
2. Find "WakeCapDW_Bronze_TimescaleDB_Raw"
3. Click "Run Now" with parameters:
   - `load_mode`: "full" (for initial load)
   - `category`: "ALL"
4. Monitor job execution in the Runs tab

Or via CLI:
```bash
databricks jobs run-now --job-id <job-id> --notebook-params '{"load_mode": "full", "category": "ALL"}'
```

### 1.5 Verify Data in Delta Tables

```sql
-- In Databricks SQL
-- Check sample bronze tables
SELECT COUNT(*) FROM wakecap_prod.raw.timescale_activity;
SELECT COUNT(*) FROM wakecap_prod.raw.timescale_company;
SELECT COUNT(*) FROM wakecap_prod.raw.timescale_people;
SELECT COUNT(*) FROM wakecap_prod.raw.timescale_zone;

-- Check watermarks
SELECT
    source_table,
    last_load_status,
    last_load_row_count,
    last_watermark_timestamp,
    last_load_end_time
FROM wakecap_prod.migration._timescaledb_watermarks
ORDER BY last_load_end_time DESC
LIMIT 20;

-- Verify table count
SELECT COUNT(DISTINCT source_table) as table_count
FROM wakecap_prod.migration._timescaledb_watermarks;
-- Expected: 81 tables
```

### 1.6 Monitor Incremental Loads

Use the monitoring script:
```bash
python monitor_pipeline.py --job-name "WakeCapDW_Bronze_TimescaleDB_Raw"
```

Or check watermarks:
```bash
python check_watermarks.py
```

### 1.7 DeviceLocation Optimization (Large Tables)

**Updated: 2026-01-24**

DeviceLocation and DeviceLocationSummary are large TimescaleDB hypertables requiring special handling.

| Table | Rows | Size | Special Handling |
|-------|------|------|------------------|
| DeviceLocation | 85M | 52GB | Composite PK, GeneratedAt watermark, **Append-only** |
| DeviceLocationSummary | 7M | 3.8GB | Composite PK, GeneratedAt watermark, **Append-only** |
| EquipmentTelemetry | 0.8M | - | Hypertable, CreatedAt watermark, **Append-only** |
| Inspection | ~50K | - | CreatedAt-only watermark, **Append-only** |
| ViewFactWorkshiftsCache | ~2M | - | Hypertable, WatermarkUTC, **Append-only** |

#### Append-Only Mode Optimization

Tables marked as `is_append_only: true` use direct APPEND instead of MERGE operations, providing ~3-4x faster load times.

**How to identify append-only candidates:**
- Hypertables (TimescaleDB partitioned tables)
- Tables using only `CreatedAt` watermark (no UpdatedAt in GREATEST expression)
- Log/telemetry tables that never update existing rows
- Cache tables refreshed by insert

**Key Configuration Changes:**

| Table | Old Config | New Config |
|-------|------------|------------|
| DeviceLocation PK | `[Id]` | `[DeviceId, ProjectId, ActiveSequance, InactiveSequance, GeneratedAt]` |
| DeviceLocation Watermark | `UpdatedAt` (NULL!) | `GeneratedAt` |
| DeviceLocation Geometry | `Geometry` | `Point` |
| DeviceLocationSummary PK | `[Id]` | `[Day, DeviceId, ProjectId]` |
| DeviceLocationSummary Geometry | `Geometry` | `Point, ConfidenceArea` |

**To Reset and Reload DeviceLocation Tables:**

**Step 1: Reset Watermarks (via SQL Warehouse)**
```bash
cd migration_project
python run_with_serverless.py
```

This will:
- Delete existing watermark entries for DeviceLocation tables
- Drop existing target tables
- Use Serverless SQL Warehouse (auto-starts if stopped)

**Step 2: Run Full Load (via Databricks UI)**

1. Open notebook: `/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation`
2. Attach to cluster with Unity Catalog access
3. Set parameters:
   - `load_mode` = `full`
   - `run_optimize` = `yes`
4. Run All cells

**Databricks-side Optimizations Applied:**
```sql
-- Z-ORDER for query performance
OPTIMIZE wakecap_prod.raw.timescale_devicelocation ZORDER BY (ProjectId, GeneratedAt);
OPTIMIZE wakecap_prod.raw.timescale_devicelocationsummary ZORDER BY (ProjectId, Day);

-- Table properties
ALTER TABLE wakecap_prod.raw.timescale_devicelocation SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.targetFileSize' = '128mb'
);
```

**Dedicated Notebooks:**
| Notebook | Purpose |
|----------|---------|
| `reset_devicelocation_watermarks` | Reset watermarks for fresh load |
| `bronze_loader_devicelocation` | Optimized loader with correct PKs |
| `optimize_devicelocation` | Z-ORDER optimization |

### 1.8 Loaded Tables Summary

**78 tables loaded to wakecap_prod.raw:**

| Category | Count | Examples |
|----------|-------|----------|
| Dimensions | 35 | Company, People, Zone, Trade, Department |
| Assignments | 18 | WorkshiftResourceAssignment, LocationGroupZone |
| Facts | 20 | ResourceZone, ResourceHours, EquipmentTelemetry |
| History | 3 | SpaceHistory, ZoneHistory, ZoneViolationLog |

**22 tables match SQL Server stg.wc2023_* tables** - these are the core dimension and fact tables that were in the original SQL Server staging layer.

**56 tables are TimescaleDB-only** - these include equipment telemetry, certificates, training, permits, and other data not previously in the SQL Server staging.

### 1.9 Tables NOT Yet Migrated

The following SQL Server `stg.wc2023_*` tables are **NOT** in the Bronze layer:

| Table | Notes |
|-------|-------|
| observation_* (7 tables) | Observation dimension tables - may need separate load |
| organization | Organization hierarchy |
| Project | Project dimension |
| weather_station_sensor | Weather data |
| *_full tables (8) | Full-refresh staging variants - may not need migration |
| asset_location* | Asset tracking data |
| node | Graph/hierarchy data |

**Recommendation:** Review whether these tables should be loaded from:
1. SQL Server directly (using JDBC)
2. TimescaleDB (if data exists there)
3. Skipped (if they are staging variants or deprecated)

---

## Phase 2: Silver Layer - Data Transformation

**Status: DEPLOYED (2026-01-24)**

| Metric | Value |
|--------|-------|
| Tables Deployed | 77 |
| Target Schema | wakecap_prod.silver |
| Job Name | WakeCapDW_Silver_TimescaleDB |
| Job ID | 181959206191493 |
| Load Method | Watermark-based incremental from Bronze `_loaded_at` |
| Schedule | Daily 3:00 AM UTC (Paused initially) |

The Silver layer uses a **standalone notebook pattern** (not DLT) with watermark-based incremental loading from the Bronze layer.

### Architecture

```
┌─────────────────────┐     Watermark      ┌─────────────────────┐
│  Bronze Layer       │ ────────────────>  │  Silver Layer       │
│  wakecap_prod.raw   │  (_loaded_at)      │  wakecap_prod.silver│
│  (78 timescale_*)   │                    │  (77 silver_*)      │
└─────────────────────┘                    └─────────────────────┘
                                                   │
                                                   ▼
                                          ┌─────────────────────┐
                                          │ _silver_watermarks  │
                                          │ (tracking table)    │
                                          └─────────────────────┘
```

### 2.1 Deploy Silver Layer

**Option A: Python Script (Recommended)**
```bash
cd migration_project
python deploy_silver_layer.py
```

This will:
1. Create Silver schema (wakecap_prod.silver)
2. Create watermark tracking table
3. Upload notebook and config files
4. Create Databricks Job with 8 tasks

**Option B: Manual Deployment**
```bash
# Create workspace directories
databricks workspace mkdirs /Workspace/migration_project/pipelines/silver/notebooks
databricks workspace mkdirs /Workspace/migration_project/pipelines/silver/config

# Upload notebook
databricks workspace import pipelines/silver/notebooks/silver_loader.py \
    /Workspace/migration_project/pipelines/silver/notebooks/silver_loader \
    --format SOURCE --language PYTHON --overwrite

# Upload config
databricks workspace import pipelines/silver/config/silver_tables.yml \
    /Workspace/migration_project/pipelines/silver/config/silver_tables.yml \
    --format AUTO --overwrite
```

### 2.2 Job Structure

The Silver job has 8 tasks with dependencies:

```
Task Dependencies:
──────────────────────────────────────────────────────────────────
silver_independent_dimensions ─┐
                               ├──> silver_project_children ──> silver_zone_dependent ─┬──> silver_assignments ──> silver_facts
silver_organization ──> silver_project ─┘                                               └──> silver_history
```

| Task | Processing Group | Tables | Workers |
|------|------------------|--------|---------|
| silver_independent_dimensions | independent_dimensions | 11 | 2 |
| silver_organization | organization | 2 | 2 |
| silver_project | project_dependent | 1 | 2 |
| silver_project_children | project_children | 16 | 2 |
| silver_zone_dependent | zone_dependent | 7 | 2 |
| silver_assignments | assignments | 17 | 2 |
| silver_facts | facts | 20 | 4 |
| silver_history | history | 3 | 2 |

### 2.3 Run Initial Load

1. Open Databricks > Workflows > Jobs
2. Find "WakeCapDW_Silver_TimescaleDB" (Job ID: 181959206191493)
3. Click "Run Now"
4. Monitor task execution in the Runs tab

**Job URL:** https://adb-3022397433351638.18.azuredatabricks.net/jobs/181959206191493

Or via CLI:
```bash
databricks jobs run-now --job-id 181959206191493
```

### 2.4 Verify Silver Tables

```sql
-- Check Silver tables created
SHOW TABLES IN wakecap_prod.silver;

-- Check watermarks
SELECT table_name, processing_group, last_load_status,
       last_load_row_count, rows_dropped_critical,
       rows_flagged_business, last_bronze_watermark
FROM wakecap_prod.migration._silver_watermarks
ORDER BY processing_group, table_name;

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

### 2.5 Enable Schedule

After successful initial load:
1. Go to job settings
2. Change schedule from "Paused" to "Active"
3. Schedule: Daily 3:00 AM UTC (1 hour after Bronze job)

### 2.6 Data Quality Validation

The Silver layer implements 3-tier data quality:

| Tier | Action | Example |
|------|--------|---------|
| Critical | Drop rows | `Id IS NOT NULL` |
| Business | Log, keep rows | `DeletedAt IS NULL` |
| Advisory | Warn only | `Latitude BETWEEN -90 AND 90` |

Check DQ metrics in watermark table:
```sql
SELECT table_name,
       rows_input,
       rows_dropped_critical,
       rows_flagged_business,
       rows_warned_advisory
FROM wakecap_prod.migration._silver_watermarks
WHERE rows_dropped_critical > 0 OR rows_flagged_business > 0;
```

### 2.7 Silver Layer Files

| Category | File | Workspace Path |
|----------|------|----------------|
| Notebook | silver_loader.py | /Workspace/migration_project/pipelines/silver/notebooks/silver_loader |
| Config | silver_tables.yml | /Workspace/migration_project/pipelines/silver/config/silver_tables.yml |
| DDL | create_silver_watermarks.sql | /Workspace/migration_project/pipelines/silver/ddl/create_silver_watermarks.sql |
| Deploy | deploy_silver_layer.py | Local script |

---

## Phase 4: Unity Catalog Setup

### 2.1 Create Service Principal (if not using Managed Identity)

```bash
# Create service principal
az ad sp create-for-rbac \
    --name "wakecap-databricks-sp" \
    --role "Storage Blob Data Contributor" \
    --scopes "/subscriptions/{sub-id}/resourceGroups/wakecap-data-rg/providers/Microsoft.Storage/storageAccounts/wakecapadls"
```

Save the output:
- `appId` → Application ID
- `password` → Client Secret
- `tenant` → Directory ID

### 2.2 Configure Databricks Secret Scope

```bash
# Create secret scope
databricks secrets create-scope wakecap-secrets

# Store credentials
databricks secrets put-secret wakecap-secrets sp-application-id --string-value "your-app-id"
databricks secrets put-secret wakecap-secrets sp-directory-id --string-value "your-tenant-id"
databricks secrets put-secret wakecap-secrets sp-client-secret --string-value "your-secret"
```

### 2.3 Run Unity Catalog Setup Notebook

1. Import `databricks/setup_unity_catalog.py` to workspace
2. Run the notebook with parameters:
   - `storage_account`: wakecapadls
   - `container`: raw
   - `catalog_name`: wakecap_prod
3. Verify external location access

---

## Phase 3: Gold Layer - Business Logic

**Status: DEPLOYED (2026-01-27)**

| Metric | Value |
|--------|-------|
| Tables Deployed | 7 |
| Target Schema | wakecap_prod.gold |
| Job Name | WakeCapDW_Gold |
| Job ID | 933934272544045 |
| Load Method | Watermark-based incremental from Silver |
| Schedule | Daily 5:30 AM UTC |

The Gold layer contains business logic, facts, and aggregations for reporting.

### Architecture

```
┌─────────────────────┐                    ┌─────────────────────┐
│  Silver Layer       │ ────────────────>  │  Gold Layer         │
│  wakecap_prod.silver│                    │  wakecap_prod.gold  │
│  (77 silver_*)      │                    │  (7 gold_*)         │
└─────────────────────┘                    └─────────────────────┘
```

### 3.1 Gold Job Structure

The Gold job has 7 tasks:

```
Task Dependencies:
──────────────────────────────────────────────────────────────────
gold_fact_workers_history ──> gold_fact_workers_shifts

Independent (run in parallel):
  - gold_fact_weather_observations
  - gold_fact_reported_attendance
  - gold_fact_progress
  - gold_worker_location_assignments
  - gold_manager_assignment_snapshots
```

| Task | Description | Depends On |
|------|-------------|------------|
| gold_fact_workers_history | Worker shift history aggregation | (none) |
| gold_fact_workers_shifts | Derived shift calculations | gold_fact_workers_history |
| gold_fact_weather_observations | Weather data by project | (none) |
| gold_fact_reported_attendance | Attendance reporting | (none) |
| gold_fact_progress | Progress tracking | (none) |
| gold_worker_location_assignments | Worker location assignments | (none) |
| gold_manager_assignment_snapshots | Manager hierarchy snapshots | (none) |

### 3.2 Deploy Gold Layer

Gold notebooks are located at:
- **Local:** `migration_project/pipelines/gold/notebooks/`
- **Databricks:** `/Workspace/migration_project/pipelines/gold/notebooks/`

To add a new gold notebook:

```python
# 1. Create notebook locally
# migration_project/pipelines/gold/notebooks/gold_new_fact.py

# 2. Upload to Databricks
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
import base64

w = WorkspaceClient()
content = Path('migration_project/pipelines/gold/notebooks/gold_new_fact.py').read_text()
w.workspace.import_(
    path='/Workspace/migration_project/pipelines/gold/notebooks/gold_new_fact',
    content=base64.b64encode(content.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)

# 3. Add task to WakeCapDW_Gold job (Job ID: 933934272544045)
```

### 3.3 Run Gold Job

**Job URL:** https://adb-3022397433351638.18.azuredatabricks.net/#job/933934272544045

Via CLI:
```bash
databricks jobs run-now --job-id 933934272544045
```

### 3.4 Verify Gold Tables

```sql
-- Check Gold tables
SHOW TABLES IN wakecap_prod.gold;

-- Row counts
SELECT 'gold_fact_workers_history' as table_name, COUNT(*) as cnt FROM wakecap_prod.gold.gold_fact_workers_history
UNION ALL
SELECT 'gold_manager_assignment_snapshots', COUNT(*) FROM wakecap_prod.gold.gold_manager_assignment_snapshots;
```

---

## Phase 6: Validation

### 4.1 Run Validation Notebook

1. Open `/Workspace/WakeCapDW/notebooks/validation_reconciliation`
2. Set parameters:
   - `validation_level`: "full" (or "detailed" for sample comparison)
   - `sample_size`: 1000
3. Run all cells
4. Review results

### 4.2 Expected Validation Results

| Check | Expected |
|-------|----------|
| Row counts | 99.9%+ match |
| PK completeness | 0 null PKs |
| Aggregations | 99%+ match |
| Sample records | 99%+ found |

### 4.3 Troubleshooting Common Issues

**Issue: Row count mismatch**
- Check if incremental extraction is missing data
- Verify watermark values in `_timescaledb_watermarks` table
- Re-run full extract if needed: `load_mode=full`

**Issue: NULL primary keys**
- Review source data quality
- Check DQ expectations in Silver layer
- Verify transformation logic

**Issue: Aggregation mismatch**
- Check for data type precision differences
- Review NULL handling in calculations
- Compare specific records

**Issue: Geometry column errors**
- Tables with geometry use `ST_AsText` conversion
- Check `has_geometry: true` in table registry
- Verify PostGIS extension is available in TimescaleDB

---

## Phase 7: Production Deployment

### 5.1 Switch to Production Mode

1. Update pipeline config: `"development": false`
2. Update pipeline: `databricks pipelines update --json @dlt_pipeline_config.json`
3. Create production trigger/schedule

### 5.2 Schedule Incremental Updates

**Bronze Layer (TimescaleDB):**
1. Job "WakeCapDW_Bronze_TimescaleDB_Raw" is scheduled daily at 2:00 AM UTC
2. Uses watermark-based incremental extraction
3. Parameters: `load_mode=incremental`, `category=ALL`

**DLT Pipeline:**
- Set to "Continuous" mode for streaming, OR
- Set trigger schedule for batch (e.g., every 4 hours)

### 5.3 Apply Row-Level Security

```sql
-- Run row_filters.sql
%run /Workspace/WakeCapDW/security/row_filters

-- Apply filters to tables
ALTER TABLE wakecap_prod.gold.gold_worker_daily_summary
SET ROW FILTER security.organization_filter ON (OrganizationID);
```

### 5.4 Enable Monitoring

1. Configure DLT pipeline alerts (email/Slack)
2. Set up monitoring dashboard for:
   - Bronze job status and row counts
   - Watermark progression
   - Data freshness metrics
3. Use `monitor_pipeline.py` for CLI monitoring

---

## File Inventory

### Production Scripts

| Category | Script | Purpose |
|----------|--------|---------|
| Setup | `setup_migration.py` | Initial setup with secrets and JDBC |
| Setup | `deploy_timescaledb_bronze.py` | TimescaleDB bronze deployment |
| Setup | `run_setup.py` | Quick setup runner |
| Pipeline | `start_pipeline.py` | Start DLT pipelines |
| Monitor | `monitor_pipeline.py` | Pipeline monitoring |
| Monitor | `monitor_optimized_job.py` | Job monitoring |
| Monitor | `check_watermarks.py` | Watermark verification |
| Jobs | `run_bronze_all.py` | Run all bronze tables |
| Jobs | `run_bronze_raw.py` | Run raw bronze tables |
| Jobs | `deploy_optimized_pipeline.py` | Deploy optimized pipeline |
| Jobs | `run_optimized_job.py` | Run optimized job |
| Analysis | `analyze_sql.py` | SQL complexity analysis |
| Generation | `generate_dlt_pipeline.py` | Generate DLT notebooks |
| Deployment | `deploy_to_databricks.py` | Deploy to workspace |

### Pipeline Files

| Category | Path | Purpose |
|----------|------|---------|
| DLT | `pipelines/dlt/bronze_all_tables.py` | Bronze tables from Delta |
| DLT | `pipelines/dlt/streaming_*.py` | CDC streaming tables |
| DLT | `pipelines/dlt/silver_*.py` | Silver layer with DQ |
| DLT | `pipelines/dlt/gold_views.py` | Business views |
| Notebooks | `pipelines/notebooks/*.py` | Complex calculations |
| UDFs | `pipelines/udfs/*.py` | Python UDFs |
| Security | `pipelines/security/row_filters.sql` | Row-level security |
| TimescaleDB | `pipelines/timescaledb/dlt_timescaledb_bronze.py` | Bronze DLT definitions |
| TimescaleDB | `pipelines/timescaledb/notebooks/*.py` | Modular loaders |
| TimescaleDB | `pipelines/timescaledb/src/*.py` | Loader modules |
| TimescaleDB | `pipelines/timescaledb/config/*.yml` | Table registries (81 tables) |
| Main | `pipelines/wakecap_migration_pipeline.py` | Main migration pipeline |

### Databricks Config

| Category | File | Purpose |
|----------|------|---------|
| Config | `databricks/config/wakecapdw_tables_complete.json` | Full table configuration |
| Jobs | `databricks/jobs/*.json` | Job definitions |
| Notebooks | `databricks/notebooks/*.py` | Deployment notebooks |
| Scripts | `databricks/scripts/*.py` | Deployment scripts |
| Pipeline | `databricks/dlt_pipeline_config.json` | DLT pipeline config |

---

## Data Flow Summary

```
┌───────────────────────────────────────────────────────────────────────────┐
│                             DATA FLOW                                      │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  TimescaleDB (wakecap_app)                                                │
│       │                                                                    │
│       │ JDBC (PostgreSQL driver)                                          │
│       │ Watermark-based incremental                                       │
│       ▼                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │  BRONZE LAYER (wakecap_prod.raw)                                    │ │
│  │  - 78 tables: timescale_*                                           │ │
│  │  - Job: WakeCapDW_Bronze_TimescaleDB_Raw                            │ │
│  │  - Schedule: Daily 2:00 AM UTC                                      │ │
│  │  - Watermarks: _timescaledb_watermarks                              │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│       │                                                                    │
│       │ Standalone Notebook (watermark on _loaded_at)                     │
│       ▼                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │  SILVER LAYER (wakecap_prod.silver)                      DEPLOYED   │ │
│  │  - 77 tables: silver_*                                              │ │
│  │  - Job: WakeCapDW_Silver_TimescaleDB (ID: 181959206191493)          │ │
│  │  - Schedule: Daily 3:00 AM UTC                                      │ │
│  │  - 8 tasks with dependency chain                                    │ │
│  │  - 3-tier DQ validation (critical/business/advisory)                │ │
│  │  - Watermarks: _silver_watermarks                                   │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│       │                                                                    │
│       │ DLT transformations                                               │
│       ▼                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │  GOLD LAYER (wakecap_prod.gold)                          DEPLOYED   │ │
│  │  - 7 tables: gold_*                                                 │ │
│  │  - Job: WakeCapDW_Gold (ID: 933934272544045)                        │ │
│  │  - Schedule: Daily 5:30 AM UTC                                      │ │
│  │  - Business facts and aggregations                                  │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
└───────────────────────────────────────────────────────────────────────────┘
```

### Schedule Coordination

| Job | Job ID | Schedule | Purpose |
|-----|--------|----------|---------|
| WakeCapDW_Bronze_TimescaleDB_Raw | 28181369160316 | 2:00 AM UTC | Bronze layer from TimescaleDB |
| WakeCapDW_Silver_TimescaleDB | 181959206191493 | 3:00 AM UTC | Silver layer transformations |
| WakeCapDW_Gold | 933934272544045 | 5:30 AM UTC | Gold layer facts |

**⚠️ All future work must be added to one of these three jobs. Do not create new standalone jobs.**

---

## Support

For issues with this migration:
1. Check DLT pipeline event logs
2. Check bronze job logs and watermark table
3. Review MIGRATION_STATUS.md for conversion details
4. Compare source data with converted Delta tables

---

## Update Log

| Date | Update |
|------|--------|
| 2026-01-27 | **Gold layer DEPLOYED** - Job ID 933934272544045 with 7 tasks |
| 2026-01-27 | **IMPORTANT:** Added Production Jobs section - all work must go to Bronze/Silver/Gold jobs |
| 2026-01-27 | Consolidated gold notebooks into single WakeCapDW_Gold job |
| 2026-01-27 | Added silver_manager_assignments task to Silver job (9 tasks total) |
| 2026-01-27 | Deleted temporary SP_Migration_All_Layers jobs |
| 2026-01-24 | **Silver layer DEPLOYED** - Added Phase 2 with Job ID 181959206191493 |
| 2026-01-24 | Added Silver job structure (8 tasks, 77 tables) |
| 2026-01-24 | Updated data flow diagram with Silver layer details |
| 2026-01-24 | Added append-only mode documentation for 5 large/hypertable tables |
| 2026-01-22 | Added DeviceLocation optimization section (1.7) - correct PKs, GeneratedAt watermark |
| 2026-01-22 | Phase 1 (Bronze Layer) marked COMPLETE - 78 tables loaded |
| 2026-01-22 | Updated TimescaleDB ingestion documentation |

*Last updated: 2026-01-27*
