---
name: lakebridge-fix
description: Fix issues identified in code review reports by implementing recommended solutions in priority order
---

# Lakebridge Fix

## Purpose

You are a specialized code fix agent. Your job is to read a code review report, understand the original requirements and plan, and systematically fix all identified issues. You implement the recommended solutions from the review, starting with Blockers and High Risk items, then working down to Medium and Low Risk items.

## Usage

Invoke this skill to fix issues from a review:
- "Use the lakebridge-fix skill with review at app_review/review_2024-01-15.md"
- "Use the lakebridge-fix skill with review at app_review/review_latest.md and plan at specs/migration.md"

## Variables

USER_PROMPT: $1
PLAN_PATH: $2
REVIEW_PATH: $3
FIX_OUTPUT_DIRECTORY: `app_fix_reports/`

## Instructions

- **CRITICAL**: You ARE building and fixing code. Your job is to IMPLEMENT solutions.
- If no `USER_PROMPT` or `REVIEW_PATH` is provided, STOP immediately and ask the user to provide them.
- Read the review report at REVIEW_PATH to understand what issues need to be fixed.
- Read the plan at PLAN_PATH to understand the original implementation intent.
- Prioritize fixes by risk tier: Blockers first, then High Risk, Medium Risk, and finally Low Risk.
- For each issue, implement the recommended solution (prefer the primary solution).
- After fixing each issue, verify the fix works as expected.
- Run validation commands from the original plan.
- Create a fix report documenting what was changed.

## Migration-Specific Fixes

### Missing Schema Fix
```python
from databricks.sdk import WorkspaceClient
import yaml
from pathlib import Path

creds = yaml.safe_load(open(Path.home() / '.databricks/labs/lakebridge/.credentials.yml'))['databricks']
w = WorkspaceClient(host=creds['host'], token=creds['token'])

catalog = "your_catalog"
schema = "your_schema"

# Check if schema exists, create if not
try:
    w.schemas.get(full_name=f"{catalog}.{schema}")
    print(f"Schema {catalog}.{schema} exists")
except Exception:
    w.schemas.create(name=schema, catalog_name=catalog)
    print(f"Created schema {catalog}.{schema}")
```

### Missing Secret Scope Fix
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host=host, token=token)
scope_name = "migration_secrets"

# Create scope (ignore if exists)
try:
    w.secrets.create_scope(scope=scope_name)
    print(f"Created secret scope: {scope_name}")
except Exception as e:
    if "already exists" in str(e):
        print(f"Secret scope {scope_name} already exists")
    else:
        raise

# Add secrets
secrets_to_add = {
    "sqlserver_jdbc_url": "jdbc:sqlserver://server:1433;database=db;encrypt=true",
    "sqlserver_user": "username",
    "sqlserver_password": "password"
}

for key, value in secrets_to_add.items():
    w.secrets.put_secret(scope=scope_name, key=key, string_value=value)
    print(f"Added secret: {key}")
```

### JDBC Driver Fix
Add this library to your Databricks cluster:
- Maven: `com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11`
- Or upload JAR: `mssql-jdbc-12.4.2.jre11.jar`

### Transpilation Error Fixes

**T-SQL to Databricks mappings:**
```python
mappings = {
    # Data types
    "NVARCHAR(MAX)": "STRING",
    "VARCHAR(MAX)": "STRING",
    "DATETIME": "TIMESTAMP",
    "DATETIME2": "TIMESTAMP",
    "BIT": "BOOLEAN",
    "MONEY": "DECIMAL(19,4)",
    "UNIQUEIDENTIFIER": "STRING",

    # Functions
    "GETDATE()": "CURRENT_TIMESTAMP()",
    "ISNULL(": "COALESCE(",
    "LEN(": "LENGTH(",
    "CHARINDEX(": "LOCATE(",
}

for tsql, databricks in mappings.items():
    sql = sql.replace(tsql, databricks)
```

### NO_TABLES_IN_PIPELINE Fix
```python
# 1. Ensure notebook has correct format
notebook_content = '''# Databricks notebook source
import dlt

# COMMAND ----------

@dlt.table(name="my_table")
def my_table():
    return spark.createDataFrame([(1,)], ["id"])
'''

# 2. Re-upload with proper format
import base64
from databricks.sdk.service.workspace import ImportFormat, Language

content_b64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
w.workspace.import_(
    path=workspace_path,
    content=content_b64,
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)

# 3. Verify notebook was uploaded as NOTEBOOK type
status = w.workspace.get_status(workspace_path)
assert status.object_type.name == 'NOTEBOOK', "Upload failed"
```

### WAITING_FOR_RESOURCES / QuotaExhausted Fix
```python
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary

# Delete the old pipeline
w.pipelines.delete(pipeline_id=old_pipeline_id)

# Recreate with serverless=True
result = w.pipelines.create(
    name="Pipeline_Name",
    catalog="catalog",
    target="schema",
    development=True,
    serverless=True,  # THIS IS THE FIX
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=workspace_path))]
)

# Start the new pipeline
update = w.pipelines.start_update(pipeline_id=result.pipeline_id, full_refresh=True)
```

### AMBIGUOUS_REFERENCE Fix
```python
# Before (BROKEN):
@dlt.table(name="silver_worker")
def silver_worker():
    return dlt.read("bronze_worker").withColumn("_ingested_at", current_timestamp())

@dlt.table(name="silver_project")
def silver_project():
    return dlt.read("bronze_project").withColumn("_ingested_at", current_timestamp())

# After (FIXED):
@dlt.table(name="silver_worker")
def silver_worker():
    return dlt.read("bronze_worker").withColumn("worker_ingested_at", current_timestamp())

@dlt.table(name="silver_project")
def silver_project():
    return dlt.read("bronze_project").withColumn("project_ingested_at", current_timestamp())

@dlt.table(name="gold_summary")
def gold_summary():
    workers = dlt.read("silver_worker")
    projects = dlt.read("silver_project")
    return workers.join(projects, "ProjectID").select(
        col("worker_ingested_at").alias("ingested_at")  # Explicit reference
    )
```

### Complete Pipeline Fix Workflow
```python
import yaml
import base64
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary

# 1. Load credentials
creds = yaml.safe_load(open(Path.home() / '.databricks/labs/lakebridge/.credentials.yml'))['databricks']
w = WorkspaceClient(host=creds['host'], token=creds['token'])

# 2. Delete failed pipeline
try:
    w.pipelines.delete(pipeline_id=failed_pipeline_id)
except:
    pass

# 3. Fix and re-upload notebook
fixed_notebook = '''# Databricks notebook source
import dlt
# ... fixed code ...
'''
content_b64 = base64.b64encode(fixed_notebook.encode('utf-8')).decode('utf-8')
w.workspace.import_(
    path=workspace_path,
    content=content_b64,
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)

# 4. Create new pipeline with serverless
result = w.pipelines.create(
    name="Fixed_Pipeline",
    catalog="catalog",
    target="schema",
    development=True,
    serverless=True,
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=workspace_path))]
)

# 5. Start and monitor
update = w.pipelines.start_update(pipeline_id=result.pipeline_id, full_refresh=True)
```

## Workflow

1. **Read the Review Report** - Parse the review at REVIEW_PATH to extract all issues by risk tier

2. **Read the Plan** - Review the plan at PLAN_PATH for original requirements and validation commands

3. **Read the Original Prompt** - Understand the USER_PROMPT to keep the original intent in mind

4. **Fix Blockers** - For each BLOCKER issue:
   - Read the affected file
   - Implement the primary recommended solution
   - If primary fails, try alternative solutions
   - Verify the fix

5. **Fix High Risk Issues** - Same process as Blockers

6. **Fix Medium Risk Issues** - Implement if beneficial

7. **Fix Low Risk Issues** - Implement if time permits

8. **Run Validation** - Execute all validation commands from the plan

9. **Verify Fixes** - Confirm each issue is resolved

10. **Generate Fix Report** - Write to `FIX_OUTPUT_DIRECTORY/fix_<timestamp>.md`

## Report Format

```markdown
# Fix Report

**Generated**: [ISO timestamp]
**Original Work**: [Brief summary from USER_PROMPT]
**Plan Reference**: [PLAN_PATH]
**Review Reference**: [REVIEW_PATH]
**Status**: ALL FIXED | PARTIAL | BLOCKED

---

## Executive Summary

[2-3 sentence overview of what was fixed]

---

## Fixes Applied

### BLOCKERS Fixed

#### Issue #1: [Issue Title from Review]

**Original Problem**: [What was wrong]

**Solution Applied**: [Which recommended solution was used]

**Changes Made**:
- File: `[path/to/file.ext]`
- Lines: `[XX-YY]`

**Code Changed**:
```[language]
// Before
[original code]

// After
[fixed code]
```

**Verification**: [How it was verified]

---

### HIGH RISK Fixed
[Same structure]

### MEDIUM RISK Fixed
[Same structure]

### LOW RISK Fixed
[Same structure]

---

## Skipped Issues

| Issue | Risk Level | Reason Skipped |
| ----- | ---------- | -------------- |
| [Issue] | MEDIUM | [Rationale] |

---

## Validation Results

| Command | Result | Notes |
| ------- | ------ | ----- |
| `[command]` | PASS / FAIL | [Notes] |

---

## Files Changed

| File | Changes | Lines +/- |
| ---- | ------- | --------- |
| `[path]` | [Description] | +X / -Y |

---

## Final Status

**All Blockers Fixed**: [Yes/No]
**All High Risk Fixed**: [Yes/No]
**Validation Passing**: [Yes/No]

**Overall Status**: [ALL FIXED / PARTIAL / BLOCKED]

**Next Steps** (if any):
- [Remaining items]

---

**Report File**: `app_fix_reports/fix_[timestamp].md`
```

## Examples

### Example 1: All Issues Fixed
```
User: "Use the lakebridge-fix skill with review at app_review/review_2024-01-15.md"

Fixed:
- 2 BLOCKERS (missing schema, no serverless)
- 1 HIGH RISK (error handling)
- 2 MEDIUM (code cleanup)

Validation: All commands PASS
Status: ALL FIXED
```

### Example 2: Partial Fix
```
User: "Use the lakebridge-fix skill with review at app_review/review_migration.md"

Fixed:
- 1 BLOCKER (missing schema)
- 1 HIGH RISK (timeout handling)

Skipped:
- 1 BLOCKER (firewall requires Azure admin)

Status: PARTIAL
Next Steps: Contact Azure admin for firewall configuration
```

## Scripts

This skill includes Python scripts for automated fixes:

### fixer.py

```python
from scripts.fixer import IssueFixer

fixer = IssueFixer()
fixer.load_credentials()

# Fix infrastructure issues
fixer.fix_missing_schema("catalog", "schema")
fixer.fix_missing_secret_scope("migration_secrets")
fixer.fix_missing_secrets("migration_secrets")

# Or fix all at once
fixer.fix_all_infrastructure("catalog", "schema", "migration_secrets")

# Fix notebook content
fixed_content = fixer.fix_notebook_format(content)      # Add header, import
fixed_content = fixer.fix_ambiguous_columns(fixed_content)  # Prefix columns
fixed_content = fixer.fix_serverless_flag(fixed_content)    # serverless=True

# Or fix all content at once
fixed_content = fixer.fix_notebook_content(original_content)

# Fix pipeline (delete and recreate with serverless)
action = fixer.fix_pipeline_serverless(
    old_pipeline_id="xxx",
    notebook_content=fixed_content,
    pipeline_name="Migration",
    catalog="catalog",
    schema="schema",
    workspace_path="/Workspace/Shared/migrations/pipeline"
)

# Get fix result
result = fixer.get_result()
filepath = result.save("app_fix_reports/")
print(f"Fixed {result.total_fixed} issues")
```
