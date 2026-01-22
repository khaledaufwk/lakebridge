# Databricks Deployment Report

**Deployment Time:** 2026-01-18T19:05:51.406370
**Deployed By:** khaledauf@wakecap.com

## Databricks Connection
- **Host:** https://adb-3022397433351638.18.azuredatabricks.net
- **Catalog:** wakecap_prod
- **Schema:** migration

## Deployed Artifacts

### DLT Notebook
- **Workspace Path:** /Workspace/Shared/migrations/wakecap/wakecap_migration_pipeline
- **Status:** Uploaded

### DLT Pipeline
- **Pipeline Name:** WakeCapDW_Migration_Pipeline
- **Pipeline ID:** bc59257f-73aa-42ed-9fc6-01a6c14dbb0a
- **Development Mode:** True

## Pipeline Configuration
- **Tables:** 30
- **Views:** 10
- **Photon Enabled:** True

## Next Steps

1. **View Pipeline in Databricks:**
   - Navigate to: Workflows > Delta Live Tables
   - Find: WakeCapDW_Migration_Pipeline

2. **Configure Data Source Secrets:**
   ```bash
   databricks secrets create-scope --scope wakecap
   databricks secrets put --scope wakecap --key sqlserver_jdbc_url --string-value "jdbc:sqlserver://wakecap24.database.windows.net:1433;database=WakeCapDW_20251215"
   databricks secrets put --scope wakecap --key sqlserver_user --string-value "your_user"
   databricks secrets put --scope wakecap --key sqlserver_password --string-value "your_password"
   ```

3. **Start Pipeline (Development Mode):**
   ```bash
   databricks pipelines start bc59257f-73aa-42ed-9fc6-01a6c14dbb0a
   ```

4. **Monitor Pipeline:**
   - Check pipeline status in Databricks UI
   - Review data quality metrics
   - Validate row counts

## Links
- [DLT Pipeline](https://adb-3022397433351638.18.azuredatabricks.net/pipelines/bc59257f-73aa-42ed-9fc6-01a6c14dbb0a)
- [Notebook](https://adb-3022397433351638.18.azuredatabricks.net#workspace/Workspace/Shared/migrations/wakecap/wakecap_migration_pipeline)
