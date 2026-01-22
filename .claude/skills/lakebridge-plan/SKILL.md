---
name: lakebridge-plan
description: Create detailed engineering implementation plans based on user requirements and save them to the specs directory
---

# Lakebridge Plan

## Purpose

Create a detailed implementation plan based on user requirements. Analyze the request, think through the implementation approach, and save a comprehensive specification document that can be used as a blueprint for actual development work.

## Usage

Invoke this skill to create implementation plans:
- "Use the lakebridge-plan skill to plan a user authentication system"
- "Use the lakebridge-plan skill to plan migrating SQL Server to Databricks"

## Variables

USER_PROMPT: $ARGUMENTS
PLAN_OUTPUT_DIRECTORY: `specs/`

## Instructions

- **IMPORTANT**: If no `USER_PROMPT` is provided, stop and ask the user to provide it.
- Carefully analyze the user's requirements provided in the USER_PROMPT variable
- Determine the task type (chore|feature|refactor|fix|enhancement|migration) and complexity (simple|medium|complex)
- Think deeply about the best approach to implement the requested functionality or solve the problem
- Explore the codebase to understand existing patterns and architecture
- Follow the Plan Format below to create a comprehensive implementation plan
- Include all required sections and conditional sections based on task type and complexity
- Generate a descriptive, kebab-case filename based on the main topic of the plan
- Save the complete implementation plan to `PLAN_OUTPUT_DIRECTORY/<descriptive-name>.md`
- Ensure the plan is detailed enough that another developer could follow it to implement the solution
- Include code examples or pseudo-code where appropriate to clarify complex concepts
- Consider edge cases, error handling, and scalability concerns

## Migration-Specific Knowledge

When planning SQL Server to Databricks migrations, incorporate these learnings:

### Phase 1: Assessment
- Extract SQL objects using `sys.procedures`, `sys.views`, `sys.objects` queries
- Analyze complexity indicators: CURSOR, TEMP_TABLE, DYNAMIC_SQL, SPATIAL, MERGE, PIVOT
- Stored procedures with cursors require manual conversion to set-based operations
- User-defined functions need conversion to Python UDFs or Databricks SQL functions
- Spatial/geography functions require H3 library or custom UDFs

### Phase 2: Transpilation
- SQLGlot handles tables and views well but struggles with complex T-SQL procedures
- Key transformations:
  - `[column]` -> `` `column` ``
  - `nvarchar(MAX)` -> `STRING`
  - `datetime` -> `TIMESTAMP`
  - `GETDATE()` -> `CURRENT_TIMESTAMP()`
  - `ISNULL()` -> `COALESCE()`
  - `IDENTITY(1,1)` -> `GENERATED ALWAYS AS IDENTITY`

### Phase 3: DLT Pipeline Design
- Use medallion architecture: Bronze (raw) -> Silver (cleaned) -> Gold (aggregated)
- Bronze layer: Raw JDBC ingestion from source
- Silver layer: Data quality expectations, soft-delete filtering, standardization
- Gold layer: Business views and aggregates

### Phase 4: Databricks Deployment Prerequisites
- **Secret Scope**: Create scope for SQL Server credentials
- **Target Schema**: Must exist before pipeline runs (CREATE SCHEMA IF NOT EXISTS)
- **JDBC Driver**: `mssql-jdbc-12.4.2.jre11.jar` must be added to cluster libraries
- **Network Access**: Databricks IPs must be allowed through SQL Server firewall
- **Unity Catalog**: Catalog must exist and user needs appropriate permissions
- **Serverless Compute**: Use `serverless=True` to avoid VM quota issues

### Common Deployment Issues
1. Missing target schema -> Pipeline fails silently
2. Missing JDBC driver -> Connection errors
3. Firewall blocking -> Timeout errors
4. Secret scope not found -> Authentication errors
5. Catalog permissions -> Access denied errors
6. **NO_TABLES_IN_PIPELINE** -> Notebook format issue, missing @dlt.table decorators
7. **WAITING_FOR_RESOURCES** -> Azure VM quota exhausted, use serverless
8. **AMBIGUOUS_REFERENCE** -> Column name collision in joins, use explicit aliases

### DLT Notebook Format Requirements
Notebooks must follow this exact format:
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Title

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

@dlt.table(name="table_name", comment="description")
def table_name():
    return spark.createDataFrame(...)
```

### Pipeline Creation Best Practices
```python
# Always use serverless to avoid quota issues
result = w.pipelines.create(
    name="Migration_Pipeline",
    catalog="catalog_name",
    target="schema_name",
    development=True,
    serverless=True,  # CRITICAL: Avoids VM quota issues
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=workspace_path))]
)
```

### Join Column Naming Convention
When creating silver/gold layers with joins, avoid ambiguous columns:
```python
# Silver layer: Use unique column names
.withColumn("worker_ingested_at", current_timestamp())  # Not "_ingested_at"

# Gold layer: Explicitly select columns
.select(
    col("WorkerID"),
    col("worker_ingested_at").alias("ingested_at")
)
```

## Bronze Layer Implementation - Standalone Notebook Pattern (RECOMMENDED)

**IMPORTANT**: All bronze layer loading MUST use this standalone notebook pattern. This is the proven production pattern for JDBC-based incremental loading.

### Why Standalone Notebooks Over DLT
- **Self-contained**: No external module dependencies, easier to debug
- **Flexible watermarking**: GREATEST() expressions for comprehensive change tracking
- **Better error handling**: Per-table retry logic and detailed status tracking
- **Job workflow support**: Can be orchestrated as multi-task Databricks Jobs
- **Geometry support**: Built-in handling for PostGIS/spatial columns

### Notebook Structure Template
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader - [Source System]
# MAGIC
# MAGIC Self-contained loader for [Source] bronze layer migration.
# MAGIC All code is embedded - no external module dependencies.
# MAGIC
# MAGIC **Source:** [Source Database]
# MAGIC **Target:** [catalog].[schema]

# COMMAND ----------

import time
import logging
from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeLoader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "raw"
TABLE_PREFIX = "timescale_"  # or "sqlserver_", "postgres_", etc.
SECRET_SCOPE = "wakecap-timescale"

# Ensure schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.migration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("category", "ALL", ["ALL", "dimensions", "assignments", "facts", "history"], "Category")
dbutils.widgets.text("max_tables", "0", "Max Tables (0=all)")
dbutils.widgets.text("table_list", "", "Specific Tables (comma-separated)")

load_mode = dbutils.widgets.get("load_mode")
category_filter = dbutils.widgets.get("category")
max_tables = int(dbutils.widgets.get("max_tables"))
table_list_str = dbutils.widgets.get("table_list").strip()

force_full_load = (load_mode == "full")
if category_filter == "ALL":
    category_filter = None

# Parse table list if provided
specific_tables = None
if table_list_str:
    specific_tables = [t.strip() for t in table_list_str.split(",") if t.strip()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enums and Data Classes

# COMMAND ----------

class WatermarkType(Enum):
    TIMESTAMP = "timestamp"
    BIGINT = "bigint"

class LoadStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class TableConfig:
    source_schema: str
    source_table: str
    primary_key_columns: List[str]
    watermark_column: str
    watermark_expression: Optional[str] = None  # e.g., GREATEST(CreatedAt, UpdatedAt, DeletedAt)
    category: str = "unknown"
    is_full_load: bool = False
    fetch_size: int = 50000
    has_geometry: bool = False
    geometry_columns: Optional[List[str]] = None
    enabled: bool = True

@dataclass
class LoadResult:
    table_name: str
    status: LoadStatus
    rows_loaded: int = 0
    duration_seconds: float = 0
    error_message: Optional[str] = None
```

### Table Registry Pattern (Embedded in Notebook)
```python
# Embedded table registry with GREATEST watermarks for comprehensive change tracking
TABLE_REGISTRY = [
    # DIMENSIONS - smaller lookup tables
    {"source_table": "Activity", "primary_key_columns": ["Id"], "watermark_column": "UpdatedAt",
     "watermark_expression": "GREATEST(COALESCE(\"CreatedAt\", '1900-01-01'), COALESCE(\"UpdatedAt\", '1900-01-01'))",
     "category": "dimensions"},
    {"source_table": "Company", "primary_key_columns": ["Id"], "watermark_column": "UpdatedAt",
     "watermark_expression": "GREATEST(COALESCE(\"CreatedAt\", '1900-01-01'), COALESCE(\"UpdatedAt\", '1900-01-01'))",
     "category": "dimensions"},
    # Small reference tables - full load each time
    {"source_table": "CompanyType", "primary_key_columns": ["Id"], "watermark_column": "UpdatedAt",
     "category": "dimensions", "is_full_load": True},

    # GEOMETRY TABLES - require special handling
    {"source_table": "Zone", "primary_key_columns": ["Id"], "watermark_column": "UpdatedAt",
     "category": "dimensions", "has_geometry": True, "geometry_columns": ["Coordinates"]},

    # FACTS - high-volume tables with larger fetch sizes
    {"source_table": "DeviceLocation", "primary_key_columns": ["Id"], "watermark_column": "UpdatedAt",
     "category": "facts", "fetch_size": 50000},
    {"source_table": "EquipmentTelemetry", "primary_key_columns": ["Id"], "watermark_column": "CreatedAt",
     "category": "facts", "fetch_size": 100000},
]

# Tables to exclude (complex JSON/binary that need special handling)
EXCLUDED_TABLES = ["AuditTrail", "MobileSyncRejectedActions", "SGSIntegrationLog"]
```

### Watermark Manager Pattern
```python
WATERMARK_TABLE = f"{TARGET_CATALOG}.migration._timescaledb_watermarks"

# Create watermark table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        source_system STRING,
        source_schema STRING,
        source_table STRING,
        watermark_column STRING,
        last_watermark_timestamp TIMESTAMP,
        last_load_status STRING,
        last_load_row_count BIGINT,
        last_error_message STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
""")

def get_watermark(source_table: str):
    """Get last watermark for a table."""
    try:
        result = spark.sql(f"""
            SELECT last_watermark_timestamp
            FROM {WATERMARK_TABLE}
            WHERE source_system = 'timescaledb'
              AND source_table = '{source_table}'
              AND last_load_status = 'success'
        """).collect()
        return result[0].last_watermark_timestamp if result else None
    except:
        return None

def update_watermark(source_table: str, watermark_value, status: str, row_count: int, error_msg: str = None):
    """Update watermark using MERGE for upsert."""
    ts_sql = f"TIMESTAMP'{watermark_value}'" if watermark_value else "NULL"
    err_sql = f"'{error_msg[:500]}'" if error_msg else "NULL"

    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING (SELECT 'timescaledb' as source_system, 'public' as source_schema, '{source_table}' as source_table) AS source
        ON target.source_system = source.source_system AND target.source_table = source.source_table
        WHEN MATCHED THEN UPDATE SET
            target.last_watermark_timestamp = {ts_sql},
            target.last_load_status = '{status}',
            target.last_load_row_count = {row_count},
            target.last_error_message = {err_sql},
            target.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            source_system, source_schema, source_table, watermark_column,
            last_watermark_timestamp, last_load_status, last_load_row_count, last_error_message, updated_at
        ) VALUES (
            'timescaledb', 'public', '{source_table}', 'UpdatedAt',
            {ts_sql}, '{status}', {row_count}, {err_sql}, current_timestamp()
        )
    """)
```

### Query Builder with Geometry Support
```python
def build_query(config: dict, last_watermark, all_columns: List[str] = None) -> str:
    """Build SQL query - handles geometry columns via CTE to avoid JDBC serialization issues."""
    source_table = config["source_table"]
    watermark_col = config.get("watermark_column", "UpdatedAt")
    watermark_expr = config.get("watermark_expression", f'"{watermark_col}"')
    has_geometry = config.get("has_geometry", False)
    geometry_cols = config.get("geometry_columns", [])
    is_full_load = config.get("is_full_load", False)

    base_table = f'"public"."{source_table}"'

    if has_geometry and geometry_cols and all_columns:
        # For geometry tables: use CTE to convert geometry to WKT text
        non_geom_cols = [c for c in all_columns if c not in geometry_cols]
        inner_cols = ", ".join([f'"{c}"' for c in non_geom_cols])
        geom_converts = ", ".join([f'ST_AsText("{col}")::text AS "{col}"' for col in geometry_cols])
        inner_select = f'{inner_cols}, {geom_converts}'

        where_clause = ""
        if last_watermark is not None and not is_full_load:
            where_clause = f' WHERE {watermark_expr} > \'{last_watermark}\''

        query = f"""
        WITH converted AS (
            SELECT {inner_select}
            FROM {base_table}{where_clause}
        )
        SELECT * FROM converted ORDER BY "{watermark_col}"
        """
    else:
        # Normal query for non-geometry tables
        if last_watermark is None or is_full_load:
            query = f'SELECT * FROM {base_table}'
        else:
            query = f"SELECT * FROM {base_table} WHERE {watermark_expr} > '{last_watermark}'"
        query += f' ORDER BY "{watermark_col}"'

    return query
```

### Load Function with Delta MERGE
```python
def load_table(config: dict, force_full: bool = False) -> LoadResult:
    """Load a single table with JDBC read and Delta MERGE upsert."""
    source_table = config["source_table"]
    fetch_size = config.get("fetch_size", 50000)
    pk_columns = config.get("primary_key_columns", ["Id"])
    watermark_col = config.get("watermark_column", "UpdatedAt")
    is_full_load = config.get("is_full_load", False) or force_full

    start_time = time.time()
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_PREFIX}{source_table.lower()}"

    try:
        last_watermark = None if is_full_load else get_watermark(source_table)
        query = build_query(config, last_watermark)

        df = (spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")  # or com.microsoft.sqlserver.jdbc.SQLServerDriver
            .option("fetchsize", fetch_size)
            .load()
        )

        # Add metadata columns
        df = (df
            .withColumn("_loaded_at", F.current_timestamp())
            .withColumn("_source_system", F.lit("timescaledb"))
            .withColumn("_source_table", F.lit(source_table))
        )

        row_count = df.count()
        if row_count == 0 and last_watermark:
            return LoadResult(source_table, LoadStatus.SKIPPED, 0, time.time() - start_time)

        # Get new watermark value
        new_watermark = None
        if watermark_col in df.columns:
            max_row = df.agg(F.max(watermark_col).alias("max_wm")).collect()[0]
            new_watermark = max_row.max_wm

        table_exists = spark.catalog.tableExists(target_table)

        if not table_exists or is_full_load:
            # Full load - overwrite
            df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        else:
            # Incremental - MERGE upsert on primary key
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forName(spark, target_table)
            merge_cond = " AND ".join([f"target.`{pk}` = source.`{pk}`" for pk in pk_columns])

            (delta_table.alias("target")
                .merge(df.alias("source"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        update_watermark(source_table, new_watermark, "success", row_count)
        return LoadResult(source_table, LoadStatus.SUCCESS, row_count, time.time() - start_time)

    except Exception as e:
        update_watermark(source_table, None, "failed", 0, str(e))
        return LoadResult(source_table, LoadStatus.FAILED, 0, time.time() - start_time, str(e))
```

### Databricks Job Workflow Pattern
Create multi-task jobs for orchestrated loading with dependencies:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, TaskDependency
from databricks.sdk.service.compute import ClusterSpec

w = WorkspaceClient()

job = w.jobs.create(
    name="WakeCapDW_Bronze_TimescaleDB_Loader",
    tasks=[
        # Task 1: Load Dimensions (independent)
        Task(
            task_key="load_dimensions",
            description="Load dimension tables from TimescaleDB",
            notebook_task=NotebookTask(
                notebook_path="/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_standalone",
                base_parameters={
                    "load_mode": "incremental",
                    "category": "dimensions"
                }
            ),
            new_cluster=ClusterSpec(
                spark_version="14.3.x-scala2.12",
                num_workers=2,
                node_type_id="Standard_DS3_v2",
                spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"}
            ),
            timeout_seconds=7200,
        ),
        # Task 2: Load Assignments (depends on dimensions)
        Task(
            task_key="load_assignments",
            description="Load assignment tables from TimescaleDB",
            depends_on=[TaskDependency(task_key="load_dimensions")],
            notebook_task=NotebookTask(
                notebook_path="/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_standalone",
                base_parameters={
                    "load_mode": "incremental",
                    "category": "assignments"
                }
            ),
            new_cluster=ClusterSpec(
                spark_version="14.3.x-scala2.12",
                num_workers=2,
                node_type_id="Standard_DS3_v2",
                spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"}
            ),
            timeout_seconds=7200,
        ),
        # Task 3: Load Facts (depends on assignments, larger cluster)
        Task(
            task_key="load_facts",
            description="Load fact tables (hypertables) from TimescaleDB",
            depends_on=[TaskDependency(task_key="load_assignments")],
            notebook_task=NotebookTask(
                notebook_path="/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_standalone",
                base_parameters={
                    "load_mode": "incremental",
                    "category": "facts",
                    "batch_size": "100000"
                }
            ),
            new_cluster=ClusterSpec(
                spark_version="14.3.x-scala2.12",
                num_workers=4,  # More workers for fact tables
                node_type_id="Standard_DS4_v2",  # Larger nodes
                spark_conf={"spark.databricks.delta.schema.autoMerge.enabled": "true"}
            ),
            timeout_seconds=14400,  # 4 hours for facts
        ),
    ],
    max_concurrent_runs=1,
    timeout_seconds=28800,  # 8 hours total
)
print(f"Job created: {job.job_id}")
```

### Table Category Guidelines
| Category | Description | Typical fetch_size | Cluster Size |
|----------|-------------|-------------------|--------------|
| dimensions | Lookup/reference tables | 50000 | 2 workers, DS3_v2 |
| assignments | Junction/mapping tables | 50000 | 2 workers, DS3_v2 |
| facts | Transactional/time-series | 100000 | 4 workers, DS4_v2 |
| history | Historical/audit tables | 50000 | 2 workers, DS3_v2 |

### Watermark Expression Patterns
```python
# Simple - single timestamp column
"watermark_column": "UpdatedAt"

# GREATEST - capture any modification (create, update, soft-delete)
"watermark_expression": "GREATEST(COALESCE(\"CreatedAt\", '1900-01-01'), COALESCE(\"UpdatedAt\", '1900-01-01'), COALESCE(\"DeletedAt\", '1900-01-01'))"

# For tables with only CreatedAt (append-only)
"watermark_column": "CreatedAt"
```

### Bronze Layer Validation Queries
```sql
-- Check all tables loaded successfully
SELECT source_table, last_load_status, last_load_row_count,
       last_watermark_timestamp, updated_at
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb'
ORDER BY updated_at DESC;

-- Verify total tables loaded
SELECT COUNT(DISTINCT source_table) as tables_loaded
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb' AND last_load_status = 'success';

-- Check for any failures
SELECT source_table, last_error_message
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE last_load_status = 'failed';
```

### Troubleshooting
| Issue | Cause | Solution |
|-------|-------|----------|
| Connection Timeout | Firewall/network | Allow Databricks IPs, check SSL settings |
| Authentication Failure | Bad credentials | Verify secrets in scope |
| Out of Memory | Large table | Reduce `fetch_size`, increase cluster memory |
| Slow Performance | Small fetch size | Increase `fetch_size` up to 100000 |
| Geometry Serialization Error | PostGIS type not supported | Use CTE with ST_AsText() conversion |
| AMBIGUOUS_REFERENCE | Column name collision | Use explicit aliases in queries |

## Workflow

1. **Analyze Requirements** - Parse the USER_PROMPT to understand the core problem and desired outcome
2. **Explore Codebase** - Understand existing patterns, architecture, and relevant files
3. **Design Solution** - Develop technical approach including architecture decisions and implementation strategy
4. **Document Plan** - Structure a comprehensive markdown document with problem statement, implementation steps, and testing approach
5. **Generate Filename** - Create a descriptive kebab-case filename based on the plan's main topic
6. **Save & Report** - Write the plan to `PLAN_OUTPUT_DIRECTORY/<filename>.md` and provide a summary

## Plan Format

Follow this format when creating implementation plans:

```md
# Plan: <task name>

## Task Description
<describe the task in detail based on the prompt>

## Objective
<clearly state what will be accomplished when this plan is complete>

## Problem Statement
<if feature or medium/complex: clearly define the specific problem or opportunity>

## Solution Approach
<if feature or medium/complex: describe the proposed solution approach>

## Source System Analysis (if migration)
- **Database**: <source database name>
- **Server**: <server address>
- **Object Count**: <tables, views, procedures, functions>
- **Complexity Indicators**: <CURSOR, DYNAMIC_SQL, SPATIAL, etc.>
- **Estimated Manual Work**: <list objects requiring manual conversion>

## Target Architecture (if migration)
- **Catalog**: <Unity Catalog name>
- **Schema**: <target schema>
- **Pipeline Type**: <DLT, Workflow, etc.>
- **Medallion Layers**: Bronze, Silver, Gold

## Relevant Files
Use these files to complete the task:
<list files relevant to the task with bullet points>

### New Files
<files to be created>

## Implementation Phases (if medium/complex or migration)

### Phase 1: Foundation/Setup
<describe foundational work>

### Phase 2: Core Implementation
<describe main implementation>

### Phase 3: Integration & Polish/Validation
<describe integration and testing>

## Step by Step Tasks
IMPORTANT: Execute every step in order, top to bottom.

### 1. <First Task Name>
- <specific action>
- <specific action>

### 2. <Second Task Name>
- <specific action>

## Testing Strategy (if feature or medium/complex)
<describe testing approach>

## Acceptance Criteria
<list specific, measurable criteria>

## Validation Commands
Execute these commands to validate the task is complete:
- <command> - <description>

## Notes
<optional additional context>
```

## Report

After creating and saving the implementation plan, provide:

```
Implementation Plan Created

File: specs/<filename>.md
Topic: <brief description>
Key Components:
- <main component 1>
- <main component 2>
- <main component 3>
```

## Examples

### Example 1: Feature Plan
```
User: "Use the lakebridge-plan skill to plan adding OAuth authentication"

Creates: specs/add-oauth-authentication.md
- Task Type: feature
- Complexity: medium
- Phases: Setup, Implementation, Integration
- Key decisions: OAuth provider, token storage, refresh flow
```

### Example 2: Migration Plan
```
User: "Use the lakebridge-plan skill to plan WakeCap database migration"

Creates: specs/wakecap-database-migration.md
- Task Type: migration
- Complexity: complex
- Source: SQL Server with 50 tables, 30 procedures
- Target: Databricks DLT with medallion architecture
- Phases: Assessment, Transpilation, Pipeline, Deployment, Validation
```

## Scripts

This skill includes Python scripts for plan generation:

### planner.py

```python
from scripts.planner import PlanGenerator, PlanTemplate, TaskType, Complexity

generator = PlanGenerator()

# Analyze prompt to determine type and complexity
task_type, complexity = generator.analyze_prompt("Add OAuth authentication")
# (TaskType.FEATURE, Complexity.MEDIUM)

# Create template
template = PlanTemplate(
    task_name="Add OAuth Authentication",
    task_type=task_type,
    complexity=complexity,
    description="Implement OAuth 2.0...",
    objective="Users can log in via OAuth",
    tasks=[
        {"name": "Configure OAuth", "actions": ["Add client ID", "Set callback URL"]},
        {"name": "Implement Flow", "actions": ["Create login endpoint", "Handle callback"]},
    ],
    acceptance_criteria=["Users can log in with Google", "Token refresh works"],
    validation_commands=[{"command": "npm test", "description": "Run tests"}],
)

# Generate markdown and save
markdown = generator.generate(template)
filepath = generator.save(template, "specs/")

# For migrations, use convenience method
migration_template = generator.create_migration_template(
    source_database="WakeCap",
    source_server="server.database.windows.net",
    target_catalog="wakecap_prod",
    target_schema="migration",
    tables=["Worker", "Project", "Site"],
)
```
