---
allowed-tools: Bash(git ls-files:*), Read
description: Answer questions about the project structure and documentation without coding
---

# Question

Answer the user's question by analyzing the project structure and documentation. This prompt is designed to provide information and answer questions without making any code changes.

## Instructions

- **IMPORTANT: This is a question-answering task only - DO NOT write, edit, or create any files**
- **IMPORTANT: Focus on understanding and explaining existing code and project structure**
- **IMPORTANT: Provide clear, informative answers based on project analysis**
- **IMPORTANT: If the question requires code changes, explain what would need to be done conceptually without implementing**

## Execute

- `git ls-files` to understand the project structure

## Read

- README.md for project overview and documentation

## Analysis Approach

- Review the project structure from git ls-files
- Understand the project's purpose from README
- Connect the question to relevant parts of the project
- Provide comprehensive answers based on analysis

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

## Response Format

- Direct answer to the question
- Supporting evidence from project structure
- References to relevant documentation
- Conceptual explanations where applicable

## Question

$ARGUMENTS
