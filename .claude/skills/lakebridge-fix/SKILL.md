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

## Stored Procedure Conversion

This skill includes comprehensive capabilities for converting T-SQL stored procedures to Databricks notebooks.

### SP Conversion Workflow

1. **Pattern Detection** - Identify T-SQL patterns requiring conversion
2. **CURSOR Conversion** - Transform to Window functions
3. **MERGE Generation** - Create Delta MERGE operations
4. **Temp Table Conversion** - Convert to Spark temp views
5. **Spatial Function Stubs** - Generate H3/UDF placeholders
6. **Dependency Resolution** - Create missing tables/UDFs
7. **Validation Generation** - Create test notebooks

### SP Conversion Patterns

| T-SQL Pattern | Spark Equivalent |
|--------------|------------------|
| CURSOR iteration | Window functions (lag/lead/row_number) |
| MERGE INTO | DeltaTable.merge() |
| #TempTable | createOrReplaceTempView() |
| ##GlobalTemp | Delta table or cached DataFrame |
| geography::Point | (lat, lon) columns + H3 index |
| STDistance | Haversine UDF |
| EXEC @dynamic | Parameterized f-string queries |

## Scripts

This skill includes Python scripts for automated fixes:

### Core Fixer (fixer.py)

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

### SP Converter (Phase 1)

```python
from scripts.sp_converter import SPConverter

converter = SPConverter()

# Convert a stored procedure to Databricks notebook
result = converter.convert(
    source_sql=tsql_content,
    procedure_name="stg.spCalculateMetrics",
    target_catalog="wakecap_prod",
    target_schema="gold",
    source_tables_mapping={
        "dbo.Worker": "wakecap_prod.silver.silver_worker",
        "dbo.Project": "wakecap_prod.silver.silver_project",
    }
)

if result.success:
    print(f"Converted {result.procedure_name}")
    print(f"Patterns converted: {result.patterns_converted}")
    print(f"Manual review needed: {result.patterns_manual}")

    # Write notebook
    with open(result.target_path, "w") as f:
        f.write(result.notebook_content)
```

### CURSOR to Window Converter (Phase 1)

```python
from scripts.sp_converter import CursorConverter

converter = CursorConverter()

# Convert CURSOR pattern to Window function
cursor_sql = """
DECLARE worker_cursor CURSOR FOR
    SELECT WorkerId, TimestampUTC FROM FactWorkersHistory
    ORDER BY WorkerId, TimestampUTC
...
"""

converted, warnings = converter.convert(
    cursor_sql=cursor_sql,
    context={
        "partition_cols": ["WorkerId"],
        "order_cols": ["TimestampUTC"],
        "timestamp_col": "TimestampUTC",
        "gap_threshold": 300
    }
)

print(converted)  # Spark Window function code
```

### Temp Table Converter (Phase 2)

```python
from scripts.temp_converter import TempTableConverter

converter = TempTableConverter()

# Detect and convert temp tables
temp_tables = converter.detect_temp_tables(source_sql)
for temp in temp_tables:
    print(f"Found {temp['name']}, used {temp['usage_count']} times")

# Convert all temp tables
converted_code = converter.convert_all(
    source_sql,
    cache_threshold=2  # Cache if used more than twice
)
```

### Spatial Function Converter (Phase 2)

```python
from scripts.spatial_converter import SpatialConverter

converter = SpatialConverter()

# Detect spatial functions
spatial_funcs = converter.detect_spatial_functions(source_sql)
for func in spatial_funcs:
    print(f"Found {func['function']} at line {func['line']}")

# Generate conversion stubs
stubs = converter.convert(source_sql, {})
print(stubs)  # H3 setup + Haversine UDF + usage examples
```

### Dependency Resolver (Phase 3)

```python
from scripts.dependency_resolver import DependencyResolver

resolver = DependencyResolver(
    catalog="wakecap_prod",
    dry_run=False
)

# Set existing tables
resolver.set_existing_tables({
    "wakecap_prod.silver.silver_worker",
    "wakecap_prod.silver.silver_project",
})

# Resolve dependencies
dependencies = [
    {"target_object": "dbo.Worker", "dependency_type": "reads"},
    {"target_object": "dbo.Project", "dependency_type": "reads"},
    {"target_object": "dbo.fnCalcTime", "dependency_type": "uses_function"},
]

actions = resolver.resolve_all(dependencies, auto_create=True)

# Generate setup notebook
setup_notebook = resolver.generate_setup_notebook(actions)

# Generate report
report = resolver.generate_resolution_report(actions)
print(report)
```

### Validation Notebook Generator (Phase 2/3)

```python
from scripts.validation_generator import ValidationNotebookGenerator, ValidationConfig

generator = ValidationNotebookGenerator()

config = ValidationConfig(
    procedure_name="stg.spCalculateMetrics",
    source_table="wakecap_prod.silver.silver_fact_workers",
    target_table="wakecap_prod.gold.gold_fact_summary",
    key_columns=["ProjectId", "WorkerId", "ShiftLocalDate"],
    aggregation_columns=["ActiveTime", "InactiveTime", "TotalTime"],
    date_column="ShiftLocalDate",
    sample_size=100,
    tolerance_percent=0.01,
    # Phase 3 enhancements
    categorical_columns=["WorkerType", "ProjectStatus"],
    foreign_key_checks=[
        {"column": "project_id", "ref_table": "silver.dim_project", "ref_column": "project_id"}
    ],
    include_profiling=True,
    include_freshness=True,
)

notebook = generator.generate(config)

# Or generate comprehensive validation with all Phase 3 integration
notebook = generator.generate_comprehensive_validation(
    procedure_name="stg.spCalculateMetrics",
    source_sql=tsql_content,
    target_notebook=converted_notebook,
    source_table="silver.source_table",
    target_table="gold.target_table",
    dependencies=dependency_analysis_result,
    business_logic_comparison=logic_comparison_result,
)
```

### Complete SP Migration Workflow

```python
from scripts.sp_converter import SPConverter
from scripts.dependency_resolver import DependencyResolver
from scripts.validation_generator import ValidationNotebookGenerator

# 1. Convert the stored procedure
converter = SPConverter()
conversion_result = converter.convert(
    source_sql=tsql_content,
    procedure_name="stg.spCalculateMetrics",
    target_catalog="wakecap_prod",
    target_schema="gold"
)

# 2. Resolve dependencies
resolver = DependencyResolver(catalog="wakecap_prod")
actions = resolver.resolve_all(
    conversion_result.get("dependencies", []),
    auto_create=True
)

# 3. Generate validation notebook
generator = ValidationNotebookGenerator()
validation_notebook = generator.generate_from_conversion(
    procedure_name="stg.spCalculateMetrics",
    source_table="silver.fact_workers",
    target_table="gold.gold_fact_summary"
)

# 4. Save outputs
with open("notebooks/calc_metrics.py", "w") as f:
    f.write(conversion_result.notebook_content)

with open("notebooks/validate_calc_metrics.py", "w") as f:
    f.write(validation_notebook)

print(f"Conversion score: {conversion_result.conversion_score:.0%}")
print(f"Dependencies resolved: {len([a for a in actions if a.action.value == 'exists'])}")
```
