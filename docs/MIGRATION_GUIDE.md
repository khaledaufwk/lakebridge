# End-to-End Migration Guide: SQL Server to Databricks DLT

This guide walks through a complete migration from Microsoft SQL Server to Databricks Delta Live Tables (DLT) using Lakebridge's AI-powered workflow.

---

## ⚠️ WakeCapDW Production Jobs

For the WakeCapDW migration specifically, **all work must be added to these three production jobs:**

| Job Name | Job ID | Purpose | Schedule |
|----------|--------|---------|----------|
| **WakeCapDW_Bronze_TimescaleDB_Raw** | 28181369160316 | Bronze layer ingestion | 2:00 AM UTC |
| **WakeCapDW_Silver_TimescaleDB** | 181959206191493 | Silver transformations | 3:00 AM UTC |
| **WakeCapDW_Gold** | 933934272544045 | Gold facts | 5:30 AM UTC |

See `migration_project/DEPLOYMENT_GUIDE.md` for WakeCapDW-specific details.

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Server database access credentials
- ODBC Driver 18 for SQL Server installed
- Python 3.10+ environment

## Installation

```bash
pip install databricks-labs-lakebridge[agent]
```

## Phase 1: Setup & Configuration

### 1.1 Configure Database Credentials

Create credentials file at `~/.databricks/labs/lakebridge/.credentials.yml`:

```yaml
secret_vault_type: local

mssql:
  database: WakeCapDW
  driver: ODBC Driver 18 for SQL Server
  server: myserver.database.windows.net
  port: 1433
  user: migration_user
  password: your_secure_password
  auth_type: sql_authentication
  encrypt: true
  trustServerCertificate: false
  loginTimeout: 30
```

### 1.2 Configure Databricks Connection

```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="your-personal-access-token"

# Or use Databricks CLI authentication
databricks auth login --host http://adb-3022397433351638.18.azuredatabricks.net/
```

## Phase 2: Assessment

### 2.1 Extract SQL from Source Database

First, extract stored procedures, views, and functions from your MSSQL database:

```bash
# Create directory for SQL files
mkdir -p ./source_sql/

# Connect to MSSQL and extract objects (using Python)
python -c "
from databricks.labs.lakebridge.connections.database_manager import MSSQLConnector
from sqlalchemy import text
import os

connector = MSSQLConnector({
    'database': 'YOUR_DATABASE',
    'driver': 'ODBC Driver 18 for SQL Server',
    'server': 'your-server.database.windows.net',
    'port': 1433,
    'user': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD',
    'auth_type': 'sql_authentication',
    'encrypt': True,
    'trustServerCertificate': False,
    'loginTimeout': 30
})

with connector.engine.connect() as conn:
    # Extract stored procedures
    result = conn.execute(text('''
        SELECT name, OBJECT_DEFINITION(object_id) as definition
        FROM sys.procedures WHERE OBJECT_DEFINITION(object_id) IS NOT NULL
    '''))
    for row in result:
        with open(f'./source_sql/{row.name}.sql', 'w') as f:
            f.write(row.definition or '')
    print('Stored procedures extracted')
"
```

### 2.2 Analyze SQL Code

```bash
databricks labs lakebridge analyze \
  --source-tech tsql \
  --source-directory ./source_sql/ \
  --report-file ./assessment/analysis_report.json
```

## Phase 3: Transpilation

### 3.1 Transpile SQL to Databricks Dialect

```bash
databricks labs lakebridge transpile \
  --source-dialect tsql \
  --input-source ./source_sql/ \
  --output-folder ./transpiled/ \
  --catalog-name migration_catalog \
  --schema-name staging \
  --error-file-path ./transpiled/errors.log
```

### 3.2 Review Transpilation Results

Check `./transpiled/` for:
- Successfully converted SQL files
- Error log for any failures
- Warnings for manual review

## Phase 4: DLT Generation

### 4.1 Generate DLT Pipeline

```bash
databricks labs lakebridge dlt-generate \
  --input-dir ./transpiled/ \
  --output-file ./pipelines/main_pipeline.py \
  --catalog migration_catalog \
  --schema production \
  --notebook-name "WakeCapDW Migration Pipeline" \
  --description "Migrated from SQL Server WakeCapDW database"
```

### 4.2 Review Generated Pipeline

The generated notebook includes:
- Bronze layer: Raw data ingestion
- Silver layer: Cleaned and validated data
- Gold layer: Business aggregates
- Data quality expectations

## Phase 5: Deployment

### 5.1 Deploy to Databricks

```bash
databricks labs lakebridge dlt-deploy \
  --notebook-path ./pipelines/main_pipeline.py \
  --workspace-path /Workspace/Shared/migrations/wakecap \
  --pipeline-name "WakeCapDW Production" \
  --target-catalog migration_catalog \
  --target-schema production \
  --serverless
```

### 5.2 Run Initial Pipeline

```bash
databricks labs lakebridge dlt-run \
  --pipeline-id <pipeline-id> \
  --full-refresh \
  --wait
```

## Phase 6: AI-Powered Workflow (Optional)

For complex migrations, use the AI agent workflow:

### 6.1 Create Migration Plan

```bash
databricks labs lakebridge agent-plan \
  --prompt "Migrate WakeCapDW database to Databricks DLT with:
    - Medallion architecture (Bronze/Silver/Gold)
    - CDC support for Orders and Inventory tables
    - Daily aggregation for Sales reporting
    - Data quality checks on customer PII fields" \
  --working-dir ./migration_project
```

This creates a detailed plan in `specs/wakecap-migration.md`.

### 6.2 Implement the Plan

```bash
databricks labs lakebridge agent-build \
  --plan-path specs/wakecap-migration.md \
  --working-dir ./migration_project
```

### 6.3 Review Implementation

```bash
databricks labs lakebridge agent-review \
  --prompt "WakeCapDW migration to DLT" \
  --plan-path specs/wakecap-migration.md \
  --working-dir ./migration_project
```

Review report saved to `app_review/review_<timestamp>.md`.

### 6.4 Fix Issues (if needed)

If review shows FAIL verdict:

```bash
databricks labs lakebridge agent-fix \
  --prompt "WakeCapDW migration" \
  --plan-path specs/wakecap-migration.md \
  --review-path app_review/review_2025.md \
  --working-dir ./migration_project
```

### 6.5 Or Run Complete Workflow

```bash
databricks labs lakebridge agent-migrate \
  --prompt "Migrate WakeCapDW to Databricks DLT" \
  --working-dir ./migration_project
```

## Phase 7: Reconciliation

### 7.1 Configure Reconciliation

```bash
databricks labs lakebridge configure-secrets
```

### 7.2 Run Data Reconciliation

```bash
databricks labs lakebridge reconcile \
  --source-type mssql \
  --target-catalog migration_catalog \
  --target-schema production \
  --tables customers,orders,products
```

## Using Slash Commands (IDE Integration)

In Cursor or Claude IDE, use these commands:

```
# Create a detailed migration plan
/plan Migrate WakeCapDW stored procedures to Databricks DLT pipelines

# Implement the plan
/build specs/wakecap-stored-proc-migration.md

# Review the implementation
/review "WakeCapDW stored procedure migration" specs/wakecap-stored-proc-migration.md

# Fix any issues
/fix "WakeCapDW migration" specs/wakecap-stored-proc-migration.md app_review/review.md

# Ask questions about the codebase
/question How are the customer dimension tables currently loaded?
```

## Project Structure After Migration

```
migration_project/
├── specs/                          # Implementation plans
│   └── wakecap-migration.md
├── app_review/                     # Code review reports
│   └── review_2025-01-15.md
├── app_fix_reports/                # Fix reports
│   └── fix_2025-01-15.md
├── source_sql/                     # Original SQL files
│   ├── stored_procedures/
│   ├── views/
│   └── tables/
├── transpiled/                     # Transpiled Databricks SQL
│   ├── stored_procedures/
│   ├── views/
│   └── tables/
├── pipelines/                      # DLT pipeline notebooks
│   ├── main_pipeline.py
│   └── cdc_pipeline.py
└── assessment/                     # Assessment reports
    ├── profiler_report.html
    └── analysis/
```

## Troubleshooting

### Common Issues

1. **ODBC Connection Failed**
   ```
   Error: [ODBC Driver 18 for SQL Server]Login timeout expired
   ```
   Solution: Check firewall rules and server connectivity.

2. **Unity Catalog Access Denied**
   ```
   Error: User does not have permission on catalog
   ```
   Solution: Grant appropriate Unity Catalog permissions.

3. **Pipeline Validation Failed**
   ```
   Error: Table dependency not found
   ```
   Solution: Check table creation order in DLT notebook.

### Debug Mode

Run with verbose logging:

```bash
databricks labs lakebridge agent-migrate \
  --prompt "..." \
  --working-dir ./project \
  --verbose
```

Check logs in `.lakebridge_agent_logs/`.

## Best Practices

1. **Start Small**: Migrate a subset of tables first
2. **Use Development Mode**: Test pipelines with `--development` flag
3. **Validate Incrementally**: Run reconciliation after each phase
4. **Version Control**: Keep all migration artifacts in git
5. **Document Changes**: Use agent review reports for documentation
6. **Monitor Performance**: Compare execution times with source system

## Support

- Documentation: https://databrickslabs.github.io/lakebridge/
- Issues: https://github.com/databrickslabs/lakebridge/issues
