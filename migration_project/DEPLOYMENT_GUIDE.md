# WakeCapDW Migration - Deployment Guide

This guide provides step-by-step instructions for deploying the WakeCapDW migration to production.

## Prerequisites

### Azure Resources
- Azure SQL Server with WakeCapDW_20251215 database
- Databricks workspace with Unity Catalog enabled
- Azure Key Vault for secrets management

### Access Requirements
- Databricks workspace admin access
- SQL Server read access
- Azure Key Vault access

### Tools
- Databricks CLI (`databricks`)
- Python 3.9+ with `databricks-sdk` package

---

## Phase 1: Databricks Direct Ingestion (Replaces ADF)

The migration uses **Databricks notebooks with JDBC** to read directly from SQL Server, eliminating the need for Azure Data Factory.

### Architecture

```
┌─────────────────┐     JDBC      ┌─────────────────┐
│  SQL Server     │ ────────────> │   Databricks    │
│  WakeCapDW      │               │   Delta Lake    │
└─────────────────┘               └─────────────────┘
                                        │
                                        ▼
                              ┌─────────────────────┐
                              │  wakecap_prod.raw   │
                              │  (Delta Tables)     │
                              └─────────────────────┘
```

### 1.1 Configure Azure Key Vault Secrets

Create the following secrets in your Key Vault scope (`akv-wakecap24`):

```bash
# Using Databricks CLI
databricks secrets create-scope akv-wakecap24 --scope-backend-type AZURE_KEYVAULT \
    --resource-id /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault}

# Or using Azure CLI to add secrets to Key Vault
az keyvault secret set --vault-name wakecap24 \
    --name sqlserver-wakecap-password \
    --value "your-sql-server-password"
```

### 1.2 Deploy Ingestion Notebooks

**Option A: Python Script**
```bash
cd migration_project/databricks

pip install databricks-sdk

python deploy_ingestion_pipeline.py \
    --host https://adb-3022397433351638.18.azuredatabricks.net \
    --token <your-pat-token> \
    --job-type orchestrator
```

**Option B: Databricks CLI**
```bash
# Create workspace folders
databricks workspace mkdirs /Workspace/WakeCapDW/notebooks

# Upload notebooks
databricks workspace import notebooks/sqlserver_incremental_load.py \
    /Workspace/WakeCapDW/notebooks/sqlserver_incremental_load \
    --format SOURCE --language PYTHON --overwrite

databricks workspace import notebooks/wakecapdw_orchestrator.py \
    /Workspace/WakeCapDW/notebooks/wakecapdw_orchestrator \
    --format SOURCE --language PYTHON --overwrite

# Create job
databricks jobs create --json @jobs/wakecapdw_migration_job.json
```

### 1.3 Run Initial Full Load

1. Open Databricks > Workflows > Jobs
2. Find "WakeCapDW_Migration_Incremental"
3. Click "Run Now" with parameters:
   - For first run, tables will auto-detect as initial load
4. Monitor job execution in the Runs tab

### 1.4 Verify Data in Delta Tables

```sql
-- In Databricks SQL
SELECT COUNT(*) FROM wakecap_prod.raw.wakecapdw_dbo_worker;
SELECT COUNT(*) FROM wakecap_prod.raw.wakecapdw_dbo_project;
SELECT COUNT(*) FROM wakecap_prod.raw.wakecapdw_dbo_factworkershistory;

-- Check watermarks
SHOW TBLPROPERTIES wakecap_prod.raw.wakecapdw_dbo_worker;
```

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
- Verify watermark values in SyncState table
- Re-run full extract if needed

**Issue: NULL primary keys**
- Review source data quality
- Check DQ expectations in Silver layer
- Verify transformation logic

**Issue: Aggregation mismatch**
- Check for data type precision differences
- Review NULL handling in calculations
- Compare specific records

---

## Phase 5: Production Deployment

### 5.1 Switch to Production Mode

1. Update pipeline config: `"development": false`
2. Update pipeline: `databricks pipelines update --json @dlt_pipeline_config.json`
3. Create production trigger/schedule

### 5.2 Schedule Incremental Updates

**ADF Incremental Pipeline:**
1. Create trigger for WakeCapDW_IncrementalExtract
2. Schedule: Daily at 2:00 AM UTC
3. Parameters: Each fact table with incrementalColumn

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
2. Set up Azure Monitor for ADF pipelines
3. Create dashboard for data freshness metrics

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
| DLT | `pipelines/dlt/bronze_all_tables.py` | 142 Bronze tables |
| DLT | `pipelines/dlt/streaming_*.py` | CDC streaming tables |
| DLT | `pipelines/dlt/silver_*.py` | Silver layer with DQ |
| DLT | `pipelines/dlt/gold_views.py` | Business views |
| Notebooks | `pipelines/notebooks/*.py` | Complex calculations |
| UDFs | `pipelines/udfs/*.py` | Python UDFs |
| Security | `pipelines/security/row_filters.sql` | Row-level security |
| TimescaleDB | `pipelines/timescaledb/dlt_timescaledb_bronze.py` | Bronze DLT definitions |
| TimescaleDB | `pipelines/timescaledb/notebooks/*.py` | Modular loaders |
| TimescaleDB | `pipelines/timescaledb/src/*.py` | Loader modules |
| TimescaleDB | `pipelines/timescaledb/config/*.yml` | Table registries |
| Main | `pipelines/wakecap_migration_pipeline.py` | Main migration pipeline |

### Databricks Config

| Category | File | Purpose |
|----------|------|---------|
| Config | `databricks/config/wakecapdw_tables_complete.json` | Full table configuration |
| Jobs | `databricks/jobs/*.json` | Job definitions |
| Notebooks | `databricks/notebooks/*.py` | Deployment notebooks |
| Scripts | `databricks/scripts/*.py` | Deployment scripts |
| Pipeline | `databricks/dlt_pipeline_config.json` | DLT pipeline config |

### ADF (Legacy)

| Category | File | Purpose |
|----------|------|---------|
| ADF | `adf/arm_template.json` | ARM deployment template |
| ADF | `adf/deploy_adf.ps1` | PowerShell deployment script |
| ADF | `adf/pipeline_config.json` | Table configuration |

---

## Support

For issues with this migration:
1. Check DLT pipeline event logs
2. Review MIGRATION_STATUS.md for conversion details
3. Compare source SQL with converted Python/DLT code

---

*Last updated: 2026-01-22*
