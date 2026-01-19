# Review Checklists

## General Code Review Checklist

### Security
- [ ] No hardcoded credentials or secrets
- [ ] No SQL injection vulnerabilities
- [ ] No XSS vulnerabilities
- [ ] Input validation on all user-provided data
- [ ] Sensitive data properly encrypted/masked in logs
- [ ] Authentication/authorization checks in place

### Code Quality
- [ ] Code follows project conventions
- [ ] No duplicate code (DRY principle)
- [ ] Functions are single-purpose
- [ ] Meaningful variable and function names
- [ ] No commented-out code blocks
- [ ] No TODO/FIXME in production code

### Error Handling
- [ ] All exceptions properly caught and handled
- [ ] Error messages are informative but not leaky
- [ ] Resources properly cleaned up (try/finally or context managers)
- [ ] Graceful degradation where appropriate

### Performance
- [ ] No N+1 query patterns
- [ ] Efficient algorithms for data size
- [ ] No memory leaks
- [ ] Database queries are indexed
- [ ] Pagination for large datasets

### Testing
- [ ] Unit tests for new functionality
- [ ] Edge cases covered
- [ ] Tests are deterministic (no flaky tests)
- [ ] Mocks used appropriately

### Documentation
- [ ] Public APIs documented
- [ ] Complex logic explained
- [ ] README updated if needed
- [ ] CHANGELOG updated

## Migration-Specific Checklists

### Pre-Deployment Checklist

#### Credentials & Configuration
- [ ] `~/.databricks/labs/lakebridge/.credentials.yml` exists
- [ ] YAML format is valid
- [ ] SQL Server credentials are correct
- [ ] Databricks host URL is correct
- [ ] Databricks token is valid (not expired)
- [ ] Target catalog name is specified
- [ ] Target schema name is specified

#### Source Analysis
- [ ] All tables extracted
- [ ] All views extracted
- [ ] All stored procedures extracted
- [ ] All functions extracted
- [ ] Complexity indicators documented
- [ ] Manual conversion items identified
- [ ] Assessment report generated

#### Transpilation
- [ ] Tables: Expected ~100% success
- [ ] Views: Check complex views for errors
- [ ] Functions: Flagged for UDF conversion
- [ ] Procedures: Flagged for manual conversion
- [ ] Data type mappings verified:
  - [ ] NVARCHAR -> STRING
  - [ ] DATETIME -> TIMESTAMP
  - [ ] BIT -> BOOLEAN
  - [ ] MONEY -> DECIMAL(19,4)
  - [ ] UNIQUEIDENTIFIER -> STRING

### DLT Pipeline Checklist

#### Structure
- [ ] Bronze layer tables defined for all source tables
- [ ] Silver layer tables have data quality expectations
- [ ] Gold layer implements business views/aggregates
- [ ] Proper table naming: bronze_*, silver_*, gold_*

#### Data Quality
- [ ] Primary key NOT NULL expectations
- [ ] Soft delete filtering in silver layer
- [ ] Timestamp columns added for audit
- [ ] Duplicate handling (dropDuplicates)

#### Code Format
- [ ] First line: `# Databricks notebook source`
- [ ] Cell separators: `# COMMAND ----------`
- [ ] Import statement: `import dlt`
- [ ] All tables have `@dlt.table(name="...")` decorator
- [ ] Function names match table names

### Databricks Deployment Checklist

#### Secret Scope
- [ ] Scope created: `{scope_name}`
- [ ] Secret: `sqlserver_jdbc_url`
- [ ] Secret: `sqlserver_user`
- [ ] Secret: `sqlserver_password`

#### Schema
- [ ] Target schema exists in Unity Catalog
- [ ] Schema name matches pipeline target
- [ ] User has CREATE TABLE permissions

#### Notebook Upload
- [ ] Path starts with `/Workspace/`
- [ ] Format: `ImportFormat.SOURCE`
- [ ] Language: `Language.PYTHON`
- [ ] Verified as `ObjectType.NOTEBOOK` after upload

#### Pipeline Configuration
- [ ] `serverless=True` is set
- [ ] `development=True` for testing
- [ ] Correct `catalog` specified
- [ ] Correct `target` (schema) specified
- [ ] `libraries` uses `PipelineLibrary(notebook=NotebookLibrary(path=...))`

### Column Naming Checklist (Avoid AMBIGUOUS_REFERENCE)

- [ ] Bronze layer: Raw column names from source
- [ ] Silver layer: Prefixed names for computed columns
  - Example: `worker_ingested_at` not `_ingested_at`
- [ ] Gold layer joins: Explicit column references
  - Example: `col("worker_ingested_at").alias("ingested_at")`
- [ ] No duplicate column names across joined tables

### Connectivity Checklist

- [ ] SQL Server accessible from local machine
- [ ] Databricks workspace accessible
- [ ] JDBC driver available on cluster
- [ ] Firewall allows Databricks -> SQL Server
- [ ] Network latency acceptable

## Risk Tier Quick Reference

### BLOCKER (Must Fix)
| Issue | Detection |
|-------|-----------|
| Hardcoded credentials | Grep for password, secret, token |
| Missing schema | Pipeline "schema not found" error |
| Missing secret scope | "Scope not found" error |
| JDBC driver missing | "No suitable driver" error |
| Network blocked | Connection timeout |
| NO_TABLES_IN_PIPELINE | Check notebook format |
| Invalid token | 401 Unauthorized |

### HIGH RISK (Should Fix)
| Issue | Detection |
|-------|-----------|
| No error handling | Try/except blocks missing |
| Performance issue | Nested loops, N+1 queries |
| Memory leak | Resources not closed |
| Incomplete feature | Missing acceptance criteria |
| WAITING_FOR_RESOURCES | Stuck state (use serverless) |

### MEDIUM RISK (Fix Soon)
| Issue | Detection |
|-------|-----------|
| Code duplication | Similar blocks in multiple places |
| Missing tests | No test files for new code |
| Inconsistent naming | Mixed conventions |
| Technical debt | Complex logic without comments |
| AMBIGUOUS_REFERENCE | Duplicate column names |

### LOW RISK (Nice to Have)
| Issue | Detection |
|-------|-----------|
| Style inconsistency | Linting warnings |
| Missing docstrings | Functions without documentation |
| Minor refactoring | Could be cleaner |
| Type hints missing | No type annotations |

## Report Template Quick Reference

```markdown
# Code Review Report

**Verdict**: PASS | FAIL

## Quick Reference
| # | Description | Risk | Solution |
|---|-------------|------|----------|
| 1 | Issue desc  | BLOCKER | Fix action |

## Issues by Risk Tier
### BLOCKERS
### HIGH RISK
### MEDIUM RISK
### LOW RISK

## Final Verdict
**Status**: PASS/FAIL
**Reasoning**: [Explain]
```
