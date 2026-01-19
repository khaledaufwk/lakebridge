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

## Scripts

This skill includes Python scripts for code analysis:

### analyzer.py

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
