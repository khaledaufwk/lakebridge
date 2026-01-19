# WakeCapDW Migration - Deployment Guide

This guide provides step-by-step instructions for deploying the WakeCapDW migration to production.

## Prerequisites

### Azure Resources
- Azure SQL Server with WakeCapDW_20251215 database
- Azure Data Factory (or create new)
- ADLS Gen2 storage account with `raw` container
- Databricks workspace with Unity Catalog enabled

### Access Requirements
- Azure subscription contributor access
- Databricks workspace admin access
- SQL Server read access
- ADLS Storage Blob Data Contributor role

### Tools
- Azure CLI (`az`) or Azure PowerShell
- Databricks CLI (`databricks`)
- Python 3.9+ (optional, for SDK deployment)

---

## Phase 1: ADF Pipeline Deployment

### 1.1 Configure Credentials

Create a `credentials.env` file (DO NOT commit to git):

```bash
# SQL Server Connection
SQL_CONNECTION_STRING="Server=tcp:your-server.database.windows.net,1433;Initial Catalog=WakeCapDW_20251215;..."

# ADLS Gen2
ADLS_ACCOUNT_NAME="wakecapadls"
ADLS_ACCOUNT_KEY="your-account-key"

# Resource Group
RESOURCE_GROUP="wakecap-data-rg"
ADF_NAME="wakecap-adf"
```

### 1.2 Deploy ADF Pipelines

**Option A: PowerShell**
```powershell
cd migration_project/adf

# Load credentials
. ./credentials.env

# Deploy
./deploy_adf.ps1 `
    -ResourceGroupName $RESOURCE_GROUP `
    -FactoryName $ADF_NAME `
    -SqlConnectionString $SQL_CONNECTION_STRING `
    -AdlsAccountName $ADLS_ACCOUNT_NAME `
    -AdlsAccountKey $ADLS_ACCOUNT_KEY
```

**Option B: Azure CLI**
```bash
# Deploy ARM template
az deployment group create \
    --resource-group wakecap-data-rg \
    --template-file arm_template.json \
    --parameters factoryName=wakecap-adf \
    --parameters sqlServerConnectionString="$SQL_CONNECTION_STRING" \
    --parameters adlsAccountName=wakecapadls \
    --parameters adlsAccountKey="$ADLS_ACCOUNT_KEY"
```

### 1.3 Run Initial Data Extract

1. Open Azure Portal > Data Factory > wakecap-adf
2. Click "Author & Monitor"
3. Navigate to Pipelines > WakeCapDW_FullExtract
4. Click "Add Trigger" > "Trigger Now"
5. Monitor pipeline execution (~30-60 min for full load)

### 1.4 Verify Data in ADLS

```bash
# List extracted data
az storage fs directory list \
    --account-name wakecapadls \
    --file-system raw \
    --path wakecap \
    --auth-mode login
```

Expected structure:
```
wakecap/
├── dimensions/
│   ├── Organization/
│   ├── Project/
│   ├── Worker/
│   └── ...
├── assignments/
│   ├── CrewAssignments/
│   └── ...
├── facts/
│   ├── FactWorkersHistory/
│   ├── FactWorkersShifts/
│   └── ...
└── other/
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

| Category | File | Purpose |
|----------|------|---------|
| ADF | `adf/arm_template.json` | ARM deployment template |
| ADF | `adf/deploy_adf.ps1` | PowerShell deployment script |
| ADF | `adf/pipeline_config.json` | Table configuration |
| Databricks | `databricks/setup_unity_catalog.py` | UC setup notebook |
| Databricks | `databricks/cluster_config.json` | Cluster configuration |
| Databricks | `databricks/dlt_pipeline_config.json` | DLT pipeline config |
| Databricks | `databricks/deploy_to_databricks.py` | Deployment script |
| DLT | `pipelines/dlt/bronze_all_tables.py` | 142 Bronze tables |
| DLT | `pipelines/dlt/streaming_*.py` | CDC streaming tables |
| DLT | `pipelines/dlt/silver_*.py` | Silver layer with DQ |
| DLT | `pipelines/dlt/gold_views.py` | Business views |
| Notebooks | `pipelines/notebooks/*.py` | Complex calculations |
| UDFs | `pipelines/udfs/*.py` | Python UDFs |
| Security | `pipelines/security/row_filters.sql` | Row-level security |

---

## Support

For issues with this migration:
1. Check DLT pipeline event logs
2. Review MIGRATION_STATUS.md for conversion details
3. Compare source SQL with converted Python/DLT code

---

*Last updated: 2026-01-19*
