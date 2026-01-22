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

## Claude-Driven SQL Object Conversion

When converting stored procedures, views, and functions from SQL Server to Databricks, Claude acts as the primary developer. Follow these patterns:

### Conversion Workflow

1. **Read Source SQL** - Read the source SQL file from `migration_project/source_sql/`
2. **Analyze Patterns** - Identify complexity patterns (CURSOR, TEMP_TABLE, DYNAMIC_SQL, SPATIAL)
3. **Determine Target** - Choose output format based on patterns:
   - Simple MERGE → DLT streaming table
   - CTE-based transforms → DLT batch table
   - CURSOR/DYNAMIC_SQL → Python notebook
   - Functions → SQL UDF or Python UDF
4. **Generate Code** - Write Databricks-compatible code
5. **Write Output** - Save to appropriate location under `migration_project/pipelines/`
6. **Update Status** - Mark as converted in `migration_project/MIGRATION_STATUS.md`

### Output Directory Structure

```
migration_project/pipelines/
├── dlt/
│   ├── streaming_tables.py      # DeltaSync procedures → Auto Loader + APPLY CHANGES
│   ├── batch_calculations.py    # Calculate procedures → DLT batch tables
│   ├── assignment_tables.py     # Assignment procedures → DLT tables
│   └── gold_views.py            # Transpiled views → Gold layer
├── notebooks/
│   ├── calc_fact_workers_shifts.py     # Complex CURSOR procedures
│   ├── calc_worker_contacts.py
│   └── merge_old_data.py
├── udfs/
│   ├── simple_udfs.sql          # String/time functions → SQL UDFs
│   ├── spatial_udfs.py          # Geography functions → Python UDFs
│   └── hierarchy_udfs.py        # Recursive CTE functions → Python UDFs
└── security/
    └── row_filters.sql          # Security predicates → Unity Catalog filters
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
# MAGIC Converted by: Claude Code

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Configuration
CATALOG = "wakecap_prod"
SCHEMA = "migration"
ADLS_PATH = "abfss://raw@<storage>.dfs.core.windows.net/wakecap"

# COMMAND ----------

# ORIGINAL T-SQL LOGIC (for reference):
# {commented_original_sql}

# COMMAND ----------

@dlt.table(
    name="{target_table_name}",
    comment="Converted from {original_procedure_name}"
)
def {function_name}():
    # Step 1: Load source data (replaces temp table creation)
    source_df = spark.read.parquet(f"{ADLS_PATH}/{source_path}")

    # Step 2: Apply transformations (replaces cursor logic)
    window_spec = Window.partitionBy("key_column").orderBy("sort_column")

    result_df = (
        source_df
        .withColumn("row_num", row_number().over(window_spec))
        .withColumn("prev_value", lag("value_column").over(window_spec))
        # ... additional transformations
    )

    return result_df
```

### Function Conversion Template (Python UDF)

```python
# migration_project/pipelines/udfs/{category}_udfs.py

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, IntegerType

# Original: dbo.fnFunctionName
# Parameters: @param1 NVARCHAR(MAX), @param2 INT
# Returns: NVARCHAR(MAX)

@udf(returnType=StringType())
def fn_function_name(param1: str, param2: int) -> str:
    """
    Converted from: dbo.fnFunctionName
    Original logic: {description}
    """
    # Implementation
    result = ...
    return result

# Register for SQL use
spark.udf.register("fn_function_name", fn_function_name)
```

### View Conversion Template (DLT Gold Layer)

```python
@dlt.table(
    name="gold_{view_name}",
    comment="Converted from dbo.{view_name}"
)
def gold_{view_name}():
    """
    Gold layer view - business logic from dbo.{view_name}
    """
    return spark.sql("""
        -- Transpiled SQL from SQLGlot
        SELECT ...
        FROM LIVE.silver_table1 t1
        JOIN LIVE.silver_table2 t2 ON t1.id = t2.id
        WHERE ...
    """)
```

---

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
| **NO_TABLES_IN_PIPELINE** | No tables found | Check notebook format, @dlt.table decorators |
| **WAITING_FOR_RESOURCES** | Stuck waiting | Use `serverless=True` in pipeline create |
| **AMBIGUOUS_REFERENCE** | Column ambiguous | Use unique column names or explicit aliases |
| **QuotaExhausted** | VM quota error | Use serverless compute |

### Pipeline Creation - ALWAYS Use Serverless

```python
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary

# CRITICAL: Always set serverless=True to avoid Azure VM quota issues
result = w.pipelines.create(
    name="Migration_Pipeline",
    catalog="my_catalog",
    target="migration",
    development=True,
    serverless=True,  # REQUIRED to avoid QuotaExhausted errors
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=workspace_path))]
)
```

### DLT Notebook Format (CRITICAL)

The notebook MUST follow this exact format for DLT to recognize tables:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Title

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, StringType

# COMMAND ----------

@dlt.table(name="bronze_table", comment="Description")
def bronze_table():
    # Return a DataFrame
    return spark.createDataFrame(data, schema)

# COMMAND ----------

@dlt.table(name="silver_table")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
def silver_table():
    return dlt.read("bronze_table").withColumn("ingested_at", current_timestamp())
```

### Avoiding Column Ambiguity in Joins

When joining tables, use unique column names to avoid AMBIGUOUS_REFERENCE errors:

```python
# WRONG: Both tables have _ingested_at
@dlt.table(name="silver_worker")
def silver_worker():
    return dlt.read("bronze_worker").withColumn("_ingested_at", current_timestamp())

@dlt.table(name="silver_project")
def silver_project():
    return dlt.read("bronze_project").withColumn("_ingested_at", current_timestamp())

# This will FAIL:
def gold_summary():
    return workers.join(projects, "id").select("_ingested_at")  # Ambiguous!

# CORRECT: Use unique column names
@dlt.table(name="silver_worker")
def silver_worker():
    return dlt.read("bronze_worker").withColumn("worker_ingested_at", current_timestamp())

@dlt.table(name="silver_project")
def silver_project():
    return dlt.read("bronze_project").withColumn("project_ingested_at", current_timestamp())

# This works:
def gold_summary():
    return workers.join(projects, "id").select(col("worker_ingested_at").alias("ingested_at"))
```

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

### Claude-Driven Conversion Workflow

When the plan involves converting stored procedures, views, or functions:

1. **Read Source Files**
   - Read each SQL file from `migration_project/source_sql/{stored_procedures|views|functions}/`
   - Identify the complexity patterns (CURSOR, TEMP_TABLE, DYNAMIC_SQL, SPATIAL, MERGE)

2. **Determine Target Format**
   | Source Pattern | Target Format | Output Location |
   |----------------|---------------|-----------------|
   | spDeltaSync* | DLT streaming table | `pipelines/dlt/streaming_tables.py` |
   | spCalculate* (simple) | DLT batch table | `pipelines/dlt/batch_calculations.py` |
   | spCalculate* (CURSOR) | Python notebook | `pipelines/notebooks/` |
   | Views | DLT Gold table | `pipelines/dlt/gold_views.py` |
   | fn* (simple) | SQL UDF | `pipelines/udfs/simple_udfs.sql` |
   | fn* (spatial) | Python UDF | `pipelines/udfs/spatial_udfs.py` |
   | fn_*Predicate | Row filter | `pipelines/security/row_filters.sql` |

3. **Generate Databricks Code**
   - Use the conversion templates from this file
   - Replace T-SQL patterns with Spark/Databricks equivalents
   - Include original SQL as comments for reference
   - Add proper DLT decorators and expectations

4. **Write Output Files**
   - Create directory structure if needed
   - Write converted code to appropriate location
   - Group related conversions in single files where logical

5. **Update Migration Status**
   - Mark each converted object in `migration_project/MIGRATION_STATUS.md`
   - Note any manual review needed
   - Track conversion statistics

6. **Validate Conversions**
   - Ensure generated code is syntactically valid Python
   - Verify DLT table definitions are complete
   - Check for missing dependencies or imports

## Report

- Summarize the work you've just done in a concise bullet point list.
- Report the files and total lines changed with `git diff --stat`
- For migrations, include:
  - Objects extracted: X procedures, Y views, Z tables
  - Transpilation success rate: X%
  - Pipeline status: COMPLETED/RUNNING/FAILED
  - Tables created: list of bronze/silver/gold tables
  - Pipeline URL: link to Databricks pipeline
