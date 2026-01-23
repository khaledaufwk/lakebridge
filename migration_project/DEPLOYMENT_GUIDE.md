# WakeCapDW Migration - Deployment Guide

This guide provides step-by-step instructions for deploying the WakeCapDW migration to production.

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

**Updated: 2026-01-22**

DeviceLocation and DeviceLocationSummary are large TimescaleDB hypertables requiring special handling.

| Table | Rows | Size | Special Handling |
|-------|------|------|------------------|
| DeviceLocation | 82M | 52GB | Composite PK, GeneratedAt watermark |
| DeviceLocationSummary | 848K | 3.8GB | Composite PK, GeneratedAt watermark |

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

## Phase 2: Unity Catalog Setup

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

## Phase 3: DLT Pipeline Deployment

### 3.1 Upload Notebooks to Workspace

**Option A: Python Script**
```bash
cd migration_project
python databricks/deploy_to_databricks.py
```

**Option B: Databricks CLI**
```bash
# Create folders
databricks workspace mkdirs /Workspace/WakeCapDW/dlt
databricks workspace mkdirs /Workspace/WakeCapDW/notebooks
databricks workspace mkdirs /Workspace/WakeCapDW/udfs

# Upload DLT notebooks
databricks workspace import pipelines/dlt/bronze_all_tables.py /Workspace/WakeCapDW/dlt/bronze_all_tables --format SOURCE --language PYTHON --overwrite
databricks workspace import pipelines/dlt/streaming_dimensions.py /Workspace/WakeCapDW/dlt/streaming_dimensions --format SOURCE --language PYTHON --overwrite
databricks workspace import pipelines/dlt/streaming_facts.py /Workspace/WakeCapDW/dlt/streaming_facts --format SOURCE --language PYTHON --overwrite
databricks workspace import pipelines/dlt/batch_calculations.py /Workspace/WakeCapDW/dlt/batch_calculations --format SOURCE --language PYTHON --overwrite
databricks workspace import pipelines/dlt/silver_dimensions.py /Workspace/WakeCapDW/dlt/silver_dimensions --format SOURCE --language PYTHON --overwrite
databricks workspace import pipelines/dlt/silver_facts.py /Workspace/WakeCapDW/dlt/silver_facts --format SOURCE --language PYTHON --overwrite
databricks workspace import pipelines/dlt/gold_views.py /Workspace/WakeCapDW/dlt/gold_views --format SOURCE --language PYTHON --overwrite
```

### 3.2 Create DLT Pipeline

```bash
# Create pipeline
databricks pipelines create --json @databricks/dlt_pipeline_config.json
```

Or via UI:
1. Open Databricks workspace
2. Navigate to Workflows > Delta Live Tables
3. Click "Create Pipeline"
4. Configure:
   - Name: WakeCapDW_Migration_Pipeline
   - Target: wakecap_prod.migration
   - Source: Add all 7 DLT notebooks
   - Cluster: Use cluster_config.json settings
5. Save

### 3.3 Install Required Libraries

The DLT pipeline cluster needs h3 and shapely. These are specified in `cluster_config.json`.

If using interactive cluster for testing:
```bash
# Install on running cluster
databricks libraries install --cluster-id <cluster-id> --pypi-package h3==3.7.6
databricks libraries install --cluster-id <cluster-id> --pypi-package shapely==2.0.2
```

### 3.4 Start Pipeline (Development Mode)

```bash
# Start pipeline
databricks pipelines start <pipeline-id>
```

Or via UI:
1. Open pipeline in Databricks
2. Click "Start" (development mode)
3. Monitor execution in pipeline graph view

---

## Phase 4: Validation

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

## Phase 5: Production Deployment

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
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA FLOW                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  TimescaleDB (wakecap_app)                                          │
│       │                                                              │
│       │ JDBC (PostgreSQL driver)                                    │
│       │ Watermark-based incremental                                 │
│       ▼                                                              │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  BRONZE LAYER (wakecap_prod.raw)                            │   │
│  │  - 81 tables: timescale_*                                   │   │
│  │  - Job: WakeCapDW_Bronze_TimescaleDB_Raw                    │   │
│  │  - Watermarks: _timescaledb_watermarks                      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│       │ DLT Pipeline (streaming/batch)                              │
│       ▼                                                              │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  SILVER LAYER (wakecap_prod.silver)                         │   │
│  │  - Data quality expectations                                │   │
│  │  - Deduplication                                            │   │
│  │  - Type standardization                                     │   │
│  └─────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│       │ DLT transformations                                         │
│       ▼                                                              │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  GOLD LAYER (wakecap_prod.gold)                             │   │
│  │  - Business views                                           │   │
│  │  - Aggregations                                             │   │
│  │  - Row-level security                                       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

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
| 2026-01-22 | Added DeviceLocation optimization section (1.7) - correct PKs, GeneratedAt watermark |
| 2026-01-22 | Phase 1 (Bronze Layer) marked COMPLETE - 78 tables loaded |
| 2026-01-22 | Updated TimescaleDB ingestion documentation |

*Last updated: 2026-01-22*
