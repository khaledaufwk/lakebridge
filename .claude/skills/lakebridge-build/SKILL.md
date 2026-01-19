---
name: lakebridge-build
description: Implement plans by following step-by-step workflows, converting SQL objects, and deploying to Databricks
---

# Lakebridge Build

## Purpose

Implement an existing plan by following its step-by-step tasks. This skill reads a plan file and executes all implementation steps in order, validating the work upon completion.

## Usage

Invoke this skill to implement a plan:
- "Use the lakebridge-build skill with specs/add-oauth-authentication.md"
- "Use the lakebridge-build skill with specs/wakecap-migration.md"

## Variables

PATH_TO_PLAN: $ARGUMENTS

## Instructions

- **IMPORTANT**: Implement the plan top to bottom, in order. Do not skip any steps. Do not stop in between steps. Complete every step in the plan before stopping.
- Make your best guess judgement based on the plan, everything will be detailed there.
- If you have not run any validation commands throughout your implementation, DO NOT STOP until you have validated the work.
- Your implementation should end with executing the validation commands to validate the work, if there are issues, fix them before stopping.

## SQL Object Conversion Workflow

When converting stored procedures, views, and functions from SQL Server to Databricks:

### Conversion Steps

1. **Read Source SQL** - Read the source SQL file from `migration_project/source_sql/`
2. **Analyze Patterns** - Identify complexity patterns (CURSOR, TEMP_TABLE, DYNAMIC_SQL, SPATIAL)
3. **Determine Target** - Choose output format based on patterns:
   - Simple MERGE -> DLT streaming table
   - CTE-based transforms -> DLT batch table
   - CURSOR/DYNAMIC_SQL -> Python notebook
   - Functions -> SQL UDF or Python UDF
4. **Generate Code** - Write Databricks-compatible code
5. **Write Output** - Save to appropriate location under `migration_project/pipelines/`
6. **Update Status** - Mark as converted in `migration_project/MIGRATION_STATUS.md`

### Output Directory Structure

```
migration_project/pipelines/
├── dlt/
│   ├── streaming_tables.py      # DeltaSync procedures
│   ├── batch_calculations.py    # Calculate procedures
│   ├── assignment_tables.py     # Assignment procedures
│   └── gold_views.py            # Transpiled views
├── notebooks/
│   ├── calc_fact_workers_shifts.py     # Complex CURSOR procedures
│   ├── calc_worker_contacts.py
│   └── merge_old_data.py
├── udfs/
│   ├── simple_udfs.sql          # String/time functions
│   ├── spatial_udfs.py          # Geography functions
│   └── hierarchy_udfs.py        # Recursive CTE functions
└── security/
    └── row_filters.sql          # Security predicates
```

### T-SQL to Spark/Databricks Conversion Patterns

| T-SQL Pattern | Databricks Equivalent |
|---------------|----------------------|
| `CURSOR` loop | DataFrame with window functions |
| `#TempTable` | `createOrReplaceTempView()` or CTE |
| `MERGE INTO` | DLT `APPLY CHANGES INTO` or Spark `MERGE INTO` |
| `DYNAMIC SQL` | Parameterized f-string queries |
| `EXEC sp_name` | Function call or notebook `%run` |
| `@@ROWCOUNT` | `df.count()` or action result |
| `BEGIN TRAN` | Delta Lake ACID (automatic) |
| `geography::Point` | H3 index or (lat, lon) tuple |
| `STDistance()` | Haversine UDF or H3 `h3_distance` |

### Stored Procedure Conversion Template

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Converted from: {original_procedure_name}
# MAGIC Original: {line_count} lines T-SQL
# MAGIC Patterns: {patterns_found}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.table(
    name="{target_table_name}",
    comment="Converted from {original_procedure_name}"
)
def {function_name}():
    # Step 1: Load source data
    source_df = spark.read.parquet(f"{ADLS_PATH}/{source_path}")

    # Step 2: Apply transformations (replaces cursor logic)
    window_spec = Window.partitionBy("key_column").orderBy("sort_column")

    result_df = (
        source_df
        .withColumn("row_num", row_number().over(window_spec))
        .withColumn("prev_value", lag("value_column").over(window_spec))
    )

    return result_df
```

### Function Conversion Template (Python UDF)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def fn_function_name(param1: str, param2: int) -> str:
    """Converted from: dbo.fnFunctionName"""
    result = ...
    return result

spark.udf.register("fn_function_name", fn_function_name)
```

## Migration Build Knowledge

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

databricks:
  host: https://<workspace>.azuredatabricks.net
  token: <personal_access_token>
  catalog: <catalog_name>
  schema: <schema_name>
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
        .withColumn("worker_ingested_at", current_timestamp()))
```

### Databricks Deployment Pattern

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary
import base64

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

# 3. Upload notebook
content = Path('pipeline.py').read_text()
w.workspace.import_(
    path=workspace_path,
    content=base64.b64encode(content.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)

# 4. Create DLT pipeline - ALWAYS use serverless
result = w.pipelines.create(
    name="Migration_Pipeline",
    catalog="my_catalog",
    target="migration",
    development=True,
    serverless=True,  # CRITICAL: Avoids VM quota issues
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=workspace_path))]
)
```

### Common Issues and Fixes

| Issue | Symptom | Fix |
|-------|---------|-----|
| Missing schema | Pipeline fails | Create schema before pipeline |
| Missing JDBC driver | No driver error | Add mssql-jdbc JAR to cluster |
| Firewall blocking | Timeout | Add Databricks IPs to firewall |
| **NO_TABLES_IN_PIPELINE** | No tables found | Check notebook format, @dlt.table decorators |
| **WAITING_FOR_RESOURCES** | Stuck waiting | Use `serverless=True` |
| **AMBIGUOUS_REFERENCE** | Column ambiguous | Use unique column names |

### Avoiding Column Ambiguity in Joins

```python
# WRONG: Both tables have _ingested_at
@dlt.table(name="silver_worker")
def silver_worker():
    return dlt.read("bronze_worker").withColumn("_ingested_at", current_timestamp())

# CORRECT: Use unique column names
@dlt.table(name="silver_worker")
def silver_worker():
    return dlt.read("bronze_worker").withColumn("worker_ingested_at", current_timestamp())
```

## Workflow

- If no `PATH_TO_PLAN` is provided, STOP immediately and ask the user to provide it.
- Read the plan at `PATH_TO_PLAN` and implement it into the codebase.
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

After completing the build, provide:

```
Build Complete

Plan: {PATH_TO_PLAN}

Summary:
- <bullet point of work done>
- <bullet point of work done>

Files Changed:
{git diff --stat output}

Validation Results:
- <command>: PASS/FAIL
- <command>: PASS/FAIL

For Migrations:
- Objects extracted: X procedures, Y views, Z tables
- Transpilation success rate: X%
- Pipeline status: COMPLETED/RUNNING/FAILED
- Tables created: bronze_*, silver_*, gold_*
- Pipeline URL: <link>
```

## Examples

### Example 1: Feature Build
```
User: "Use the lakebridge-build skill with specs/add-oauth.md"

1. Reads plan from specs/add-oauth.md
2. Creates OAuth provider config
3. Implements token storage
4. Adds login/logout endpoints
5. Runs tests: npm test -> PASS
6. Reports completion
```

### Example 2: Migration Build
```
User: "Use the lakebridge-build skill with specs/wakecap-migration.md"

1. Reads migration plan
2. Verifies credentials
3. Extracts 50 tables, 30 procedures
4. Transpiles with SQLGlot
5. Generates DLT pipeline (bronze/silver/gold)
6. Creates schema in Unity Catalog
7. Uploads notebook
8. Creates serverless pipeline
9. Monitors to COMPLETED
10. Reports success with row counts
```
