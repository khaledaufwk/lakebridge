---
name: lakebridge-review
description: Review completed work by analyzing git diffs and producing risk-tiered validation reports with PASS/FAIL verdict
---

# Lakebridge Review

## Purpose

You are a specialized code review and validation agent. Analyze completed work using git diffs, identify potential issues across four risk tiers (Blockers, High Risk, Medium Risk, Low Risk), and produce comprehensive validation reports. You operate in ANALYSIS AND REPORTING mode - you do NOT build, modify, or fix code.

## Usage

Invoke this skill to review completed work:
- "Use the lakebridge-review skill to review the authentication changes"
- "Use the lakebridge-review skill to review the migration with plan at specs/migration.md"

## Variables

USER_PROMPT: $1
PLAN_PATH: $2
REVIEW_OUTPUT_DIRECTORY: `app_review/`

## Instructions

- **CRITICAL**: You are NOT building anything. Your job is to ANALYZE and REPORT only.
- If no `USER_PROMPT` is provided, STOP immediately and ask the user to provide it.
- Focus on validating work against the USER_PROMPT requirements and the plan at PLAN_PATH.
- Use `git diff` extensively to understand exactly what changed in the codebase.
- Categorize every issue into one of four risk tiers: Blocker, High Risk, Medium Risk, or Low Risk.
- For each issue, provide 1-3 recommended solutions.
- Include exact file paths, line numbers, and offending code snippets for every issue.
- Write all reports to the `REVIEW_OUTPUT_DIRECTORY` with timestamps.
- End every report with a clear PASS or FAIL verdict based on whether blockers exist.

## Risk Tier Definitions

### BLOCKER (Critical - Must Fix Before Merge)
- Security vulnerabilities (exposed secrets, SQL injection, XSS)
- Breaking changes to public APIs without deprecation
- Data loss or corruption risks
- Critical bugs that crash the application
- Missing required migrations or database schema mismatches
- Hardcoded production credentials

**Migration-Specific Blockers:**
- Missing target schema in Unity Catalog
- Secret scope not created or missing required secrets
- JDBC driver not configured in cluster libraries
- SQL Server firewall blocking Databricks access
- Expired or invalid Databricks token

### HIGH RISK (Should Fix Before Merge)
- Performance regressions or inefficient algorithms
- Missing error handling in critical paths
- Race conditions or concurrency issues
- Incomplete feature implementation
- Memory leaks or resource exhaustion risks
- Missing or inadequate logging

### MEDIUM RISK (Fix Soon)
- Code duplication or violation of DRY principle
- Inconsistent naming conventions or code style
- Missing unit tests for new functionality
- Technical debt introduced
- Suboptimal architecture patterns
- Missing input validation on non-critical paths

### LOW RISK (Nice to Have)
- Minor code style inconsistencies
- Opportunities for minor refactoring
- Missing docstring comments
- Non-critical type safety improvements
- Cosmetic improvements to error messages

## Migration-Specific Review Checklist

### Pre-Deployment Checklist

**Credentials & Configuration:**
- [ ] `~/.databricks/labs/lakebridge/.credentials.yml` exists
- [ ] SQL Server credentials are valid
- [ ] Databricks token has not expired
- [ ] Target catalog and schema are specified

**Transpilation:**
- [ ] Tables transpiled successfully (~100%)
- [ ] Views transpiled (check for errors)
- [ ] Functions flagged for manual conversion
- [ ] Data type mappings verified

**DLT Pipeline:**
- [ ] Bronze layer tables defined
- [ ] Silver layer includes data quality expectations
- [ ] Gold layer implements business views
- [ ] Secret scope name is consistent
- [ ] JDBC driver requirement documented

**Databricks Deployment:**
- [ ] Secret scope created with all required secrets
- [ ] Target schema exists in Unity Catalog (CRITICAL!)
- [ ] Notebook uploaded to correct workspace path
- [ ] Pipeline created with correct catalog/schema target
- [ ] `serverless=True` is set

### Common Migration Blockers

| Issue | Detection Method | Resolution |
|-------|-----------------|------------|
| Missing schema | Pipeline fails with "schema not found" | Create schema before pipeline run |
| JDBC driver missing | "No suitable driver" error | Add mssql-jdbc JAR to cluster |
| Network blocked | Connection timeout | Configure firewall rules |
| Secret scope missing | "Scope not found" error | Create scope and add secrets |
| **NO_TABLES_IN_PIPELINE** | "no tables were found" | Check notebook format and @dlt.table decorators |
| **WAITING_FOR_RESOURCES** | Stuck in waiting state | Use `serverless=True` |
| **AMBIGUOUS_REFERENCE** | Column ambiguous error | Use unique column names |

### DLT Notebook Format Checklist

- [ ] First line is `# Databricks notebook source`
- [ ] Uses `# COMMAND ----------` between cells
- [ ] Has `import dlt` statement
- [ ] All tables have `@dlt.table(name="...")` decorator
- [ ] Uploaded with `format=ImportFormat.SOURCE, language=Language.PYTHON`

### Column Naming Checklist (Avoid AMBIGUOUS_REFERENCE)

- [ ] Bronze layer: Raw column names from source
- [ ] Silver layer: Use prefixed names for computed columns (e.g., `worker_ingested_at`)
- [ ] Gold layer: Explicitly reference columns with `col()` and use `.alias()`
- [ ] No duplicate column names across tables being joined

## Workflow

1. **Parse the USER_PROMPT** - Extract the description of work, identify scope, note requirements

2. **Read the Plan** - If `PLAN_PATH` is provided, understand what was supposed to be implemented

3. **Analyze Git Changes**
   - `git status` to see current state
   - `git diff` to see unstaged changes
   - `git diff --staged` to see staged changes
   - `git log -1 --stat` for recent commits
   - Identify all files added, modified, or deleted

4. **Inspect Changed Files**
   - Use Read to examine each modified file
   - Use Grep to search for anti-patterns:
     - Hardcoded credentials or secrets
     - TODO/FIXME comments
     - Commented-out code blocks
     - Missing error handling
     - Debug statements in production code

5. **Categorize Issues by Risk Tier** - Apply the risk tier definitions above

6. **Document Each Issue** - For every issue capture:
   - Description (clear, concise summary)
   - Location (file path, line numbers)
   - Code (exact offending snippet)
   - Solutions (1-3 recommendations)

7. **Generate the Report** - Follow the Report format below

8. **Deliver the Report** - Write to `REVIEW_OUTPUT_DIRECTORY/review_<timestamp>.md`

## Report Format

```markdown
# Code Review Report

**Generated**: [ISO timestamp]
**Reviewed Work**: [Brief summary from USER_PROMPT]
**Plan Reference**: [PLAN_PATH if provided]
**Git Diff Summary**: [X files changed, Y insertions(+), Z deletions(-)]
**Verdict**: FAIL | PASS

---

## Executive Summary

[2-3 sentence overview of the review]

---

## Quick Reference

| #   | Description               | Risk Level | Recommended Solution             |
| --- | ------------------------- | ---------- | -------------------------------- |
| 1   | [Brief issue description] | BLOCKER    | [Primary solution in 5-10 words] |
| 2   | [Brief issue description] | HIGH       | [Primary solution in 5-10 words] |

---

## Issues by Risk Tier

### BLOCKERS (Must Fix Before Merge)

#### Issue #1: [Issue Title]

**Description**: [What's wrong and why it's a blocker]

**Location**:
- File: `[absolute/path/to/file.ext]`
- Lines: `[XX-YY]`

**Offending Code**:
```[language]
[exact code snippet]
```

**Recommended Solutions**:
1. **[Primary Solution]** (Preferred)
   - [Step-by-step explanation]

2. **[Alternative Solution]** (If applicable)
   - [Step-by-step explanation]

---

### HIGH RISK (Should Fix Before Merge)
[Same structure as Blockers]

### MEDIUM RISK (Fix Soon)
[Same structure]

### LOW RISK (Nice to Have)
[Same structure, can be brief]

---

## Plan Compliance Check (if PLAN_PATH provided)

- [ ] Acceptance Criteria 1: [Status and notes]
- [ ] Acceptance Criteria 2: [Status and notes]
- [ ] Validation Commands: [Results]

## Migration-Specific Compliance (if applicable)

### Source Extraction
- [ ] SQL objects extracted: [X procedures, Y views, Z tables]

### Transpilation
- [ ] Tables transpiled: [X/Y success rate]

### DLT Pipeline
- [ ] Bronze layer defined: [X tables]
- [ ] Silver layer with expectations: [X tables]
- [ ] Gold layer views: [X views]

### Databricks Deployment
- [ ] Secret scope: [scope_name] - [EXISTS/MISSING]
- [ ] Target schema: [catalog.schema] - [EXISTS/MISSING]
- [ ] Pipeline status: [COMPLETED/RUNNING/FAILED]

---

## Final Verdict

**Status**: [FAIL / PASS]

**Reasoning**: [FAIL if any blockers exist. PASS if only Medium/Low risk items remain.]

**Next Steps**:
- [Action item 1]
- [Action item 2]

---

**Report File**: `app_review/review_[timestamp].md`
```

## Examples

### Example 1: Clean Review
```
User: "Use the lakebridge-review skill to review the OAuth implementation"

Findings:
- 0 BLOCKERS
- 1 MEDIUM (missing test for edge case)
- 2 LOW (docstring improvements)

Verdict: PASS
```

### Example 2: Failed Review
```
User: "Use the lakebridge-review skill to review the migration"

Findings:
- 2 BLOCKERS (missing schema, no serverless flag)
- 1 HIGH (no error handling on JDBC connection)
- 3 MEDIUM (code duplication)

Verdict: FAIL
Recommended: Run lakebridge-fix skill to address blockers
```

## Stored Procedure Migration Review

This skill includes comprehensive capabilities for reviewing T-SQL to Databricks stored procedure migrations.

### SP Migration Review Workflow

1. **Pattern Detection** - Identify T-SQL patterns (CURSOR, MERGE, temp tables, spatial)
2. **Conversion Validation** - Verify all patterns are converted
3. **Business Logic Comparison** - Compare source and target logic
4. **Dependency Tracking** - Identify and resolve missing dependencies
5. **Data Type Validation** - Verify type mappings
6. **Checklist Generation** - Create procedure-specific review checklists
7. **Report Generation** - Produce comprehensive review reports

### SP Review Checklist

**Pattern Conversion:**
- [ ] CURSOR patterns → Window functions (lag/lead/row_number)
- [ ] MERGE statements → DeltaTable.merge()
- [ ] Temp tables (#temp) → createOrReplaceTempView()
- [ ] Spatial functions → H3/Haversine UDFs

**Business Logic:**
- [ ] All input tables read in target
- [ ] All output tables written
- [ ] Aggregations match
- [ ] Join conditions preserved
- [ ] Filter conditions equivalent

**Dependencies:**
- [ ] All table dependencies resolved
- [ ] Procedure call chain converted
- [ ] UDFs registered for function dependencies
- [ ] Views converted or inlined

**Data Types:**
- [ ] VARCHAR/NVARCHAR → STRING
- [ ] DECIMAL precision preserved
- [ ] DATETIME → TIMESTAMP
- [ ] GEOGRAPHY → STRING (WKT) + H3

## Scripts

This skill includes Python scripts for code analysis:

### Core Analysis (analyzer.py)

```python
from scripts.analyzer import CodeAnalyzer, ReviewReport, RiskTier

analyzer = CodeAnalyzer()

# Analyze code content for issues
issues = analyzer.analyze_content(code, file_path="path/to/file.py")

# Check DLT notebook specifically
notebook_issues = analyzer.check_dlt_notebook(notebook_content)

# Check migration deployment requirements
deployment_issues = analyzer.check_migration_deployment(
    has_schema=True,
    has_secret_scope=True,
    has_secrets=True,
    uses_serverless=False  # Will flag HIGH risk
)

# Create report
all_issues = issues + notebook_issues + deployment_issues
report = analyzer.create_report(
    issues=all_issues,
    summary="Found 2 blockers and 3 medium-risk issues",
    plan_path="specs/migration.md",
    git_diff_stats="5 files changed, 200 insertions(+), 50 deletions(-)"
)

# Save report
filepath = report.save("app_review/")
print(f"Verdict: {report.verdict}")
print(f"Blockers: {len(report.blockers)}")
```

### Business Logic Comparison (Phase 3)

```python
from scripts.analyzer_business_logic import BusinessLogicComparator

comparator = BusinessLogicComparator(table_mapping={
    "dbo.Worker": "wakecap_prod.silver.silver_worker",
    "dbo.Project": "wakecap_prod.silver.silver_project",
})

# Compare source T-SQL with target Spark
result = comparator.compare(
    source_sql=tsql_content,
    target_notebook=notebook_content,
    procedure_name="stg.spCalculateMetrics"
)

print(f"Verdict: {result['verdict']}")
print(f"Issues: {len(result['comparisons'])}")
print(comparator.extractor.compare_logic_report(result))
```

### Dependency Tracking (Phase 3)

```python
from scripts.analyzer_dependencies import DependencyAnalyzer

analyzer = DependencyAnalyzer(
    catalog="wakecap_prod",
    table_mapping={"dbo.Worker": "wakecap_prod.silver.silver_worker"}
)

# Analyze dependencies
result = analyzer.analyze(
    source_sql=tsql_content,
    procedure_name="stg.spMyProcedure",
    existing_tables={"wakecap_prod.silver.silver_worker"}
)

print(f"Readiness: {result['readiness_percentage']:.0f}%")
print(f"Blockers: {result['blockers']}")
print(f"Can convert: {result['can_convert']}")
```

### Data Type Validation (Phase 4)

```python
from scripts.datatype_validator import DataTypeValidator

validator = DataTypeValidator()

# Extract and validate types
result = validator.validate_all_types(
    source_sql=tsql_content,
    target_notebook=notebook_content
)

print(f"Verdict: {result['verdict']}")
print(f"Errors: {result['error_count']}")
print(f"Warnings: {result['warning_count']}")

# Generate report
report = validator.generate_report(result)
print(report)
```

### Checklist Generation (Phase 4)

```python
from scripts.checklist_generator import SPChecklistGenerator, SPChecklistConfig

generator = SPChecklistGenerator()

# Generate from analysis results
checklist = generator.generate_from_analysis(
    procedure_name="stg.spCalculateMetrics",
    source_sql=tsql_content,
    target_notebook=notebook_content,
    conversion_result=conversion_result,
    dependency_analysis=dependency_result,
    business_logic_comparison=logic_result,
)

print(checklist)

# Quick checklists for common patterns
from scripts.checklist_generator import QuickChecklist
print(QuickChecklist.for_cursor_conversion("my_proc"))
print(QuickChecklist.for_merge_conversion("my_proc"))
```

### Enhanced Report Generation (Phase 4)

```python
from scripts.enhanced_report import ReportBuilder

builder = ReportBuilder("stg.spCalculateMetrics")
builder.set_summary("Reviewed stored procedure conversion")
builder.set_source_info("sql/stg.spCalculateMetrics.sql", 250)
builder.set_target_info("notebooks/calc_metrics.py", 180)
builder.set_conversion_score(0.85)
builder.set_complexity_score(6)

# Add pattern status
builder.add_pattern("CURSOR", source_count=2, target_count=2, converted=True)
builder.add_pattern("TEMP_TABLE", source_count=3, target_count=3, converted=True)

# Add dependencies
builder.add_dependency("dbo.Worker", "TABLE_READ", is_resolved=True,
                       databricks_equivalent="silver.silver_worker")

# Build and save
report = builder.build()
report.save("app_review/sp_review_report.md")
print(f"Verdict: {report.verdict}")
```
