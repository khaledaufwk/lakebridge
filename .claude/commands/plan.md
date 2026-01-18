---
description: Creates a concise engineering implementation plan based on user requirements and saves it to specs directory
argument-hint: [user prompt]
---

# Quick Plan

Create a detailed implementation plan based on the user's requirements provided through the `USER_PROMPT` variable. Analyze the request, think through the implementation approach, and save a comprehensive specification document to `PLAN_OUTPUT_DIRECTORY/<name-of-plan>.md` that can be used as a blueprint for actual development work. Follow the `Instructions` and work through the `Workflow` to create the plan.

## Variables

USER_PROMPT: $1
PLAN_OUTPUT_DIRECTORY: `specs/`

## Instructions

- IMPORTANT: If no `USER_PROMPT` is provided, stop and ask the user to provide it.
- Carefully analyze the user's requirements provided in the USER_PROMPT variable
- Determine the task type (chore|feature|refactor|fix|enhancement|migration) and complexity (simple|medium|complex)
- Think deeply (ultrathink) about the best approach to implement the requested functionality or solve the problem
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
  - `[column]` → `` `column` ``
  - `nvarchar(MAX)` → `STRING`
  - `datetime` → `TIMESTAMP`
  - `GETDATE()` → `CURRENT_TIMESTAMP()`
  - `ISNULL()` → `COALESCE()`
  - `IDENTITY(1,1)` → `GENERATED ALWAYS AS IDENTITY`

### Phase 3: DLT Pipeline Design
- Use medallion architecture: Bronze (raw) → Silver (cleaned) → Gold (aggregated)
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
1. Missing target schema → Pipeline fails silently
2. Missing JDBC driver → Connection errors
3. Firewall blocking → Timeout errors
4. Secret scope not found → Authentication errors
5. Catalog permissions → Access denied errors
6. **NO_TABLES_IN_PIPELINE** → Notebook format issue, missing @dlt.table decorators
7. **WAITING_FOR_RESOURCES** → Azure VM quota exhausted, use serverless
8. **AMBIGUOUS_REFERENCE** → Column name collision in joins, use explicit aliases

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

## Workflow

1. Analyze Requirements - THINK HARD and parse the USER_PROMPT to understand the core problem and desired outcome
2. Explore Codebase - Understand existing patterns, architecture, and relevant files
3. Design Solution - Develop technical approach including architecture decisions and implementation strategy
4. Document Plan - Structure a comprehensive markdown document with problem statement, implementation steps, and testing approach
5. Generate Filename - Create a descriptive kebab-case filename based on the plan's main topic
6. Save & Report - Follow the `Report` section to write the plan to `PLAN_OUTPUT_DIRECTORY/<filename>.md` and provide a summary of key components

## Plan Format

Follow this format when creating implementation plans:

```md
# Plan: <task name>

## Task Description
<describe the task in detail based on the prompt>

## Objective
<clearly state what will be accomplished when this plan is complete>

<if task_type is feature or complexity is medium/complex, include these sections:>
## Problem Statement
<clearly define the specific problem or opportunity this task addresses>

## Solution Approach
<describe the proposed solution approach and how it addresses the objective>
</if>

<if task_type is migration, include this section:>
## Source System Analysis
- **Database**: <source database name>
- **Server**: <server address>
- **Object Count**: <tables, views, procedures, functions>
- **Complexity Indicators**: <CURSOR, DYNAMIC_SQL, SPATIAL, etc.>
- **Estimated Manual Work**: <list objects requiring manual conversion>

## Target Architecture
- **Catalog**: <Unity Catalog name>
- **Schema**: <target schema>
- **Pipeline Type**: <DLT, Workflow, etc.>
- **Medallion Layers**: Bronze, Silver, Gold
</if>

## Relevant Files
Use these files to complete the task:

<list files relevant to the task with bullet points explaining why. Include new files to be created under an h3 'New Files' section if needed>

<if complexity is medium/complex, include this section:>
## Implementation Phases
### Phase 1: Foundation
<describe any foundational work needed>

### Phase 2: Core Implementation
<describe the main implementation work>

### Phase 3: Integration & Polish
<describe integration, testing, and final touches>
</if>

<if task_type is migration, include this section:>
## Implementation Phases
### Phase 1: Setup & Configuration
- Configure credentials file at `~/.databricks/labs/lakebridge/.credentials.yml`
- Verify SQL Server connectivity
- Verify Databricks connectivity
- Create target schema in Unity Catalog

### Phase 2: Assessment & Extraction
- Extract SQL objects from source database
- Analyze SQL complexity and identify manual work
- Generate assessment report

### Phase 3: Transpilation
- Run SQLGlot transpilation for tables and views
- Identify objects requiring manual conversion
- Document transpilation errors

### Phase 4: DLT Pipeline Generation
- Generate bronze layer tables (JDBC ingestion)
- Generate silver layer tables (data quality)
- Generate gold layer views (business logic)

### Phase 5: Databricks Deployment
- Create secret scope with SQL Server credentials
- Upload pipeline notebook to workspace
- Create/update DLT pipeline
- Configure cluster libraries (JDBC driver)

### Phase 6: Validation
- Run pipeline in development mode
- Validate data quality expectations
- Compare row counts with source
- Run reconciliation if needed
</if>

## Step by Step Tasks
IMPORTANT: Execute every step in order, top to bottom.

<list step by step tasks as h3 headers with bullet points. Start with foundational changes then move to specific changes. Last step should validate the work>

### 1. <First Task Name>
- <specific action>
- <specific action>

### 2. <Second Task Name>
- <specific action>
- <specific action>

<continue with additional tasks as needed>

<if task_type is feature or complexity is medium/complex, include this section:>
## Testing Strategy
<describe testing approach, including unit tests and edge cases as applicable>
</if>

## Acceptance Criteria
<list specific, measurable criteria that must be met for the task to be considered complete>

## Validation Commands
Execute these commands to validate the task is complete:

<list specific commands to validate the work. Be precise about what to run>
- Example: `uv run python -m py_compile apps/*.py` - Test to ensure the code compiles

<if task_type is migration, include these validation commands:>
- `python test_connections.py` - Verify SQL Server and Databricks connectivity
- `python extract_sql_objects.py` - Extract and count SQL objects
- `python analyze_sql.py` - Generate complexity assessment
- `python transpile_sql.py` - Run transpilation
- `python generate_dlt_pipeline.py` - Generate DLT notebook
- `python deploy_to_databricks.py` - Deploy to workspace
- `python monitor_pipeline.py` - Monitor pipeline execution
</if>

## Notes
<optional additional context, considerations, or dependencies. If new libraries are needed, specify using `uv add`>

<if task_type is migration, include these notes:>
### Required Databricks Secrets
```bash
databricks secrets create-scope --scope <scope_name>
databricks secrets put --scope <scope_name> --key sqlserver_jdbc_url
databricks secrets put --scope <scope_name> --key sqlserver_user
databricks secrets put --scope <scope_name> --key sqlserver_password
```

### Required Cluster Libraries
- SQL Server JDBC Driver: `mssql-jdbc-12.4.2.jre11.jar`

### Network Requirements
- Databricks cluster must have network access to SQL Server
- Add Databricks IPs to SQL Server firewall or enable Azure service access
</if>
```

## Report

After creating and saving the implementation plan, provide a concise report with the following format:

```
✅ Implementation Plan Created

File: PLAN_OUTPUT_DIRECTORY/<filename>.md
Topic: <brief description of what the plan covers>
Key Components:
- <main component 1>
- <main component 2>
- <main component 3>
```
