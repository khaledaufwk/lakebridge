---
name: lakebridge-question
description: Answer questions about the project structure and documentation without making any code changes
---

# Lakebridge Question

## Purpose

Answer questions by analyzing the project structure and documentation. This skill provides information and answers questions without making any code changes.

## Usage

Invoke this skill to ask questions about the project:
- "Use the lakebridge-question skill to explain how authentication works"
- "Use the lakebridge-question skill to describe the migration architecture"

## Variables

QUESTION: $ARGUMENTS

## Instructions

- **IMPORTANT**: This is a question-answering task only - DO NOT write, edit, or create any files
- **IMPORTANT**: Focus on understanding and explaining existing code and project structure
- **IMPORTANT**: Provide clear, informative answers based on project analysis
- **IMPORTANT**: If the question requires code changes, explain what would need to be done conceptually without implementing

## Analysis Approach

1. Run `git ls-files` to understand the project structure
2. Read README.md for project overview and documentation
3. Connect the question to relevant parts of the project
4. Provide comprehensive answers based on analysis

## Migration FAQ Knowledge Base

### Q: Why does my DLT pipeline show "NO_TABLES_IN_PIPELINE"?
**A:** This error means Databricks couldn't find any `@dlt.table` decorated functions. Common causes:
1. Notebook not uploaded in SOURCE format
2. Import statement `import dlt` missing
3. Decorator syntax error
4. File not properly shared with pipeline

### Q: How do I avoid Azure VM quota issues?
**A:** Use serverless compute by setting `serverless=True` in pipeline creation:
```python
w.pipelines.create(
    name="Pipeline",
    serverless=True,  # Avoids VM quota issues
    ...
)
```

### Q: What causes "AMBIGUOUS_REFERENCE" errors in DLT?
**A:** When joining tables with same column names (like `_ingested_at`), explicitly alias or prefix columns:
```python
# Bad: col("_ingested_at") - ambiguous after join
# Good: col("worker_ingested_at").alias("ingested_at")
```

### Q: What's the correct notebook format for DLT?
**A:** DLT notebooks must have:
- `# Databricks notebook source` as first line
- `# COMMAND ----------` between cells
- `# MAGIC %md` for markdown cells
- Proper `@dlt.table` decorators

### Q: How do I verify my pipeline notebook was uploaded correctly?
**A:** Use workspace API to check:
```python
status = w.workspace.get_status(notebook_path)
print(f"Type: {status.object_type}")  # Should be NOTEBOOK
```

### Q: What are the required secrets for SQL Server migration?
**A:** Three secrets in your scope:
- `sqlserver_jdbc_url`: `jdbc:sqlserver://server:1433;database=db;encrypt=true`
- `sqlserver_user`: SQL Server username
- `sqlserver_password`: SQL Server password

### Q: Why is my pipeline stuck in WAITING_FOR_RESOURCES?
**A:** Usually Azure VM quota exhaustion. Solutions:
1. Use `serverless=True` (recommended)
2. Use smaller VM types in cluster config
3. Request quota increase from Azure

### Q: What is the medallion architecture?
**A:** A data design pattern with three layers:
- **Bronze**: Raw data ingestion, minimal transformation
- **Silver**: Cleaned, validated, deduplicated data
- **Gold**: Business-level aggregates and views

### Q: What T-SQL patterns require manual conversion?
**A:**
- **CURSOR loops**: Convert to DataFrame window functions
- **Dynamic SQL**: Convert to parameterized f-strings
- **Geography functions**: Convert to H3 or Haversine UDFs
- **Recursive CTEs**: Convert to iterative DataFrame operations

### Q: How do I connect to SQL Server from Databricks?
**A:** Use JDBC with secrets:
```python
df = (spark.read.format("jdbc")
    .option("url", dbutils.secrets.get("scope", "sqlserver_jdbc_url"))
    .option("dbtable", "[schema].[table]")
    .option("user", dbutils.secrets.get("scope", "sqlserver_user"))
    .option("password", dbutils.secrets.get("scope", "sqlserver_password"))
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load())
```

## Response Format

- Direct answer to the question
- Supporting evidence from project structure
- References to relevant documentation
- Code examples where applicable
- Conceptual explanations where needed

## Examples

### Example 1: Architecture Question
```
User: "Use the lakebridge-question skill to explain the workflow architecture"

Answer: The project uses a 4-step workflow pattern:
1. Plan - Creates implementation specs
2. Build - Implements the plan
3. Review - Analyzes changes for issues
4. Fix - Addresses identified problems

Each step runs as a subagent with its own session tracking...
```

### Example 2: Troubleshooting Question
```
User: "Use the lakebridge-question skill to explain why my pipeline shows NO_TABLES"

Answer: The NO_TABLES_IN_PIPELINE error occurs when DLT cannot find
@dlt.table decorated functions. Check these common causes:

1. First line must be: # Databricks notebook source
2. Must have: import dlt
3. Each table needs: @dlt.table(name="...")
4. Upload must use: format=ImportFormat.SOURCE, language=Language.PYTHON

To verify your notebook format, read the file and confirm these elements...
```
