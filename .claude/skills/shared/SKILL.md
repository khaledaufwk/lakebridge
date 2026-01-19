---
name: lakebridge-shared
description: Shared utilities for Lakebridge skills - credentials, Databricks client, SQL Server client, and transpiler
---

# Lakebridge Shared Utilities

## Purpose

This module provides shared functionality used across all Lakebridge skills for SQL Server to Databricks migrations. It is not invoked directly but imported by other skills.

## Scripts

### credentials.py - Credential Management

```python
from shared.scripts import CredentialsManager

creds = CredentialsManager().load()

# SQL Server credentials
print(creds.sqlserver.jdbc_url)
print(creds.sqlserver.connection_string)

# Databricks credentials
print(creds.databricks.workspace_url)
print(creds.databricks.catalog)
```

### databricks_client.py - Databricks Operations

```python
from shared.scripts import DatabricksClient

client = DatabricksClient(host=host, token=token)

# Secret management
client.ensure_secret_scope("migration_secrets")
client.set_migration_secrets(scope, jdbc_url, user, password)

# Schema management
client.ensure_schema("catalog", "schema")

# Notebook management
client.upload_notebook(content, "/Workspace/Shared/pipeline")
client.verify_notebook(workspace_path)

# Pipeline management
pipeline_id = client.create_pipeline(
    name="Migration",
    notebook_path=path,
    catalog="cat",
    schema="sch",
    serverless=True  # CRITICAL
)
update_id = client.start_pipeline(pipeline_id)
status = client.wait_for_pipeline(pipeline_id, update_id)
```

### sqlserver_client.py - SQL Server Operations

```python
from shared.scripts import SQLServerClient

client = SQLServerClient(server, database, user, password)

# Test connection
if client.test_connection():
    # Extract objects
    result = client.extract_all()
    print(result.summary())
    # {"tables": 50, "views": 10, "procedures": 30, "functions": 5}

    # Analyze complexity
    for proc in result.procedures:
        if proc.requires_manual_conversion:
            print(f"{proc.full_name}: {proc.complexity_indicators}")
```

### transpiler.py - SQL Conversion

```python
from shared.scripts import SQLTranspiler

transpiler = SQLTranspiler()

# Map data types
db_type = transpiler.map_type("NVARCHAR(MAX)")  # "STRING"

# Transpile a view
result = transpiler.transpile_view(sql_code, "MyView")
if result.success:
    print(result.transpiled)  # DLT table code

# Generate DLT tables
bronze = transpiler.generate_bronze_table("Worker", "dbo")
silver = transpiler.generate_silver_table("Worker", "WorkerID")

# Generate complete notebook
notebook = transpiler.generate_dlt_notebook(
    tables=[{"name": "Worker", "pk_column": "WorkerID"}],
    pipeline_name="Migration"
)
```

## Dependencies

These scripts require:
- `pyyaml` - For credential file parsing
- `databricks-sdk` - For Databricks operations
- `sqlalchemy` - For SQL Server connections
- `pyodbc` - For ODBC driver
- `sqlglot` (optional) - For advanced SQL transpilation
