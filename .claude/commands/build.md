---
description: Build the codebase based on the plan
argument-hint: [path-to-plan]
---

# Build

Follow the `Workflow` to implement the `PATH_TO_PLAN` then `Report` the completed work.

## Instructions

- IMPORTANT: Implement the plan top to bottom, in order. Do not skip any steps. Do not stop in between steps. Complete every step in the plan before stopping.
  - Make your best guess judgement based on the plan, everything will be detailed there.
  - If you have not run any validation commands throughout your implementation, DO NOT STOP until you have validated the work.
  - Your implementation should end with executing the validation commands to validate the work, if there are issues, fix them before stopping.

## Variables

PATH_TO_PLAN: $ARGUMENTS

## Migration-Specific Build Knowledge

When building SQL Server to Databricks migrations, use these proven patterns:

### Credentials Configuration

Create `~/.databricks/labs/lakebridge/.credentials.yml`:

```yaml
secret_vault_type: local

mssql:
  database: <database_name>
  driver: ODBC Driver 18 for SQL Server
  server: <server>.database.windows.net
  port: 1433
  user: <username>
  password: <password>
  auth_type: sql_authentication
  encrypt: true
  trustServerCertificate: false
  loginTimeout: 30

databricks:
  host: https://<workspace>.azuredatabricks.net
  token: <personal_access_token>
  catalog: <catalog_name>
  schema: <schema_name>
```

### SQL Object Extraction Pattern

```python
from databricks.labs.lakebridge.connections.database_manager import MSSQLConnector
from sqlalchemy import text

config = {...}  # Load from credentials
connector = MSSQLConnector(config)
conn = connector._engine.connect()

# Extract stored procedures
result = conn.execute(text('''
    SELECT s.name AS schema_name, p.name AS proc_name,
           OBJECT_DEFINITION(p.object_id) AS definition
    FROM sys.procedures p
    JOIN sys.schemas s ON p.schema_id = s.schema_id
    WHERE OBJECT_DEFINITION(p.object_id) IS NOT NULL
'''))
```

### Transpilation with SQLGlot

```python
from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
import asyncio

async def transpile_sql():
    engine = SqlglotEngine()
    result = await engine.transpile(
        source_dialect='tsql',
        target_dialect='databricks',
        source_code=sql_content,
        file_path=input_path
    )
    return result.transpiled_code, result.errors

code, errors = asyncio.run(transpile_sql())
```

### DLT Pipeline Pattern

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

SECRET_SCOPE = "migration_secrets"

def read_sql_server_table(table_name, schema_name="dbo"):
    return (
        spark.read.format("jdbc")
        .option("url", dbutils.secrets.get(SECRET_SCOPE, "sqlserver_jdbc_url"))
        .option("dbtable", f"[{schema_name}].[{table_name}]")
        .option("user", dbutils.secrets.get(SECRET_SCOPE, "sqlserver_user"))
        .option("password", dbutils.secrets.get(SECRET_SCOPE, "sqlserver_password"))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

@dlt.table(name="bronze_worker", table_properties={"quality": "bronze"})
def bronze_worker():
    return read_sql_server_table("Worker", "dbo")

@dlt.table(name="silver_worker")
@dlt.expect_or_drop("valid_id", "WorkerID IS NOT NULL")
def silver_worker():
    return (dlt.read("bronze_worker")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp()))
```

### Databricks Deployment Pattern

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.pipelines import NotebookLibrary
import base64
from pathlib import Path

w = WorkspaceClient(host=host, token=token)

# 1. Create secret scope
try:
    w.secrets.create_scope(scope="migration_secrets")
except Exception as e:
    if "already exists" not in str(e):
        raise

# 2. Create target schema (CRITICAL!)
try:
    w.schemas.create(name="migration", catalog_name="my_catalog")
except Exception as e:
    if "already exists" not in str(e):
        raise

# 3. Create workspace folder
workspace_path = "/Workspace/Shared/migrations/pipeline"
try:
    w.workspace.mkdirs(str(Path(workspace_path).parent))
except:
    pass

# 4. Upload notebook
content = Path('pipeline.py').read_text()
w.workspace.import_(
    path=workspace_path,
    content=base64.b64encode(content.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)

# 5. Create DLT pipeline
result = w.pipelines.create(
    name="Migration_Pipeline",
    catalog="my_catalog",
    target="migration",
    development=True,
    libraries=[NotebookLibrary(path=workspace_path)]
)
```

### Common Issues and Fixes

| Issue | Symptom | Fix |
|-------|---------|-----|
| Missing schema | Pipeline fails | Create schema before pipeline |
| Missing JDBC driver | No driver error | Add mssql-jdbc JAR to cluster |
| Firewall blocking | Timeout | Add Databricks IPs to firewall |
| Secret not found | Auth error | Create scope and add secrets |
| Permissions denied | Access error | Grant catalog access |

### Monitoring Pipeline Execution

```python
import time

pipeline_id = "your-pipeline-id"
update_id = "your-update-id"

# Poll for completion
while True:
    update = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = update.update.state.value
    print(f"State: {state}")
    
    if state in ['COMPLETED', 'FAILED', 'CANCELED']:
        break
    time.sleep(30)
```

## Workflow

- If no `PATH_TO_PLAN` is provided, STOP immediately and ask the user to provide it.
- Read the plan at `PATH_TO_PLAN`. Ultrathink about the plan and IMPLEMENT it into the codebase.
  - Implement the entire plan top to bottom before stopping.
  - For migrations, follow this order:
    1. Verify credentials configuration
    2. Test connectivity to both source and target
    3. Extract SQL objects from source
    4. Analyze SQL complexity
    5. Run transpilation
    6. Generate DLT pipeline
    7. Create schema in Unity Catalog (if needed)
    8. Configure secrets in Databricks
    9. Upload notebook to workspace
    10. Create/update DLT pipeline
    11. Start pipeline and monitor
    12. Validate results

## Report

- Summarize the work you've just done in a concise bullet point list.
- Report the files and total lines changed with `git diff --stat`
- For migrations, include:
  - Objects extracted: X procedures, Y views, Z tables
  - Transpilation success rate: X%
  - Pipeline status: COMPLETED/RUNNING/FAILED
  - Tables created: list of bronze/silver/gold tables
  - Pipeline URL: link to Databricks pipeline
