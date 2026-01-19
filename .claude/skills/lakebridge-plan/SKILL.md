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
