#!/usr/bin/env python3
"""
Generate Delta Live Tables (DLT) pipeline from transpiled SQL files.
"""
import sys
import re
import json
from pathlib import Path
from datetime import datetime


def extract_table_name(sql_content: str) -> tuple[str, str]:
    """Extract schema and table name from CREATE TABLE/VIEW statement."""
    # Match CREATE TABLE/VIEW `schema`.`table` or [schema].[table]
    patterns = [
        r'CREATE\s+(?:TABLE|VIEW)\s+`([^`]+)`\.`([^`]+)`',
        r'CREATE\s+(?:TABLE|VIEW)\s+\[([^\]]+)\]\.\[([^\]]+)\]',
        r'CREATE\s+(?:TABLE|VIEW)\s+(\w+)\.(\w+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, sql_content, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2)
    
    return 'dbo', 'unknown'


def extract_select_statement(sql_content: str) -> str:
    """Extract SELECT statement from a view definition."""
    # Remove CREATE VIEW ... AS
    match = re.search(r'CREATE\s+VIEW[^A]+AS\s+(.+)$', sql_content, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return sql_content


def extract_columns_from_create_table(sql_content: str) -> list[dict]:
    """Extract column definitions from CREATE TABLE statement."""
    columns = []
    
    # Find content between parentheses
    match = re.search(r'CREATE\s+TABLE[^(]+\((.+)\)', sql_content, re.IGNORECASE | re.DOTALL)
    if not match:
        return columns
    
    column_block = match.group(1)
    
    # Split by comma (but not within parentheses)
    depth = 0
    current = ""
    for char in column_block:
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
        elif char == ',' and depth == 0:
            if current.strip():
                columns.append(current.strip())
            current = ""
            continue
        current += char
    
    if current.strip():
        columns.append(current.strip())
    
    # Parse each column
    parsed_columns = []
    for col_def in columns:
        # Skip constraints
        if col_def.upper().startswith(('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT', '/*')):
            continue
        
        # Match column name and type
        match = re.match(r'`?(\w+)`?\s+(\w+)', col_def)
        if match:
            parsed_columns.append({
                'name': match.group(1),
                'type': match.group(2),
                'definition': col_def
            })
    
    return parsed_columns


def generate_dlt_notebook(tables: list, views: list, catalog: str, schema: str) -> str:
    """Generate a DLT notebook in Python format."""
    
    notebook = []
    
    # Header cell
    notebook.append('''# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration Pipeline
# MAGIC 
# MAGIC This Delta Live Tables pipeline migrates data from SQL Server (WakeCapDW) to Databricks.
# MAGIC 
# MAGIC **Source:** Azure SQL Server - WakeCapDW_20251215
# MAGIC **Target:** Databricks Unity Catalog
# MAGIC **Generated:** ''' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '''
# MAGIC 
# MAGIC ## Architecture
# MAGIC - **Bronze Layer:** Raw ingestion from SQL Server
# MAGIC - **Silver Layer:** Cleaned and validated data
# MAGIC - **Gold Layer:** Business aggregates and views
''')
    
    # Imports cell
    notebook.append('''# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
CATALOG = "''' + catalog + '''"
SCHEMA = "''' + schema + '''"
''')
    
    # Bronze layer - tables
    notebook.append('''# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Tables
# MAGIC These tables contain raw data ingested from SQL Server.
''')
    
    # Generate table definitions
    for table_info in tables[:30]:  # Limit to 30 tables for initial pipeline
        schema_name = table_info['schema']
        table_name = table_info['table']
        columns = table_info.get('columns', [])
        
        # Create table comment
        col_summary = ', '.join([c['name'] for c in columns[:5]])
        if len(columns) > 5:
            col_summary += f", ... (+{len(columns)-5} more)"
        
        notebook.append(f'''# COMMAND ----------

@dlt.table(
    name="bronze_{schema_name}_{table_name}",
    comment="Raw data from SQL Server {schema_name}.{table_name}",
    table_properties={{
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "{schema_name}.{table_name}"
    }}
)
def bronze_{schema_name}_{table_name}():
    """
    Bronze table: {schema_name}.{table_name}
    Columns: {col_summary}
    """
    # TODO: Configure SQL Server connection using Databricks secrets
    # Example using JDBC:
    # return (
    #     spark.read
    #     .format("jdbc")
    #     .option("url", dbutils.secrets.get("wakecap", "sqlserver_jdbc_url"))
    #     .option("dbtable", "{schema_name}.{table_name}")
    #     .option("user", dbutils.secrets.get("wakecap", "sqlserver_user"))
    #     .option("password", dbutils.secrets.get("wakecap", "sqlserver_password"))
    #     .load()
    # )
    
    # Placeholder - returns empty DataFrame with schema
    return spark.createDataFrame([], "{table_name} STRUCT<placeholder: STRING>")
''')
    
    # Silver layer - key tables
    notebook.append('''# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned Data
# MAGIC These tables contain validated and cleaned data.
''')
    
    key_tables = ['Worker', 'Project', 'Crew', 'Device', 'Organization']
    for table_info in tables:
        if table_info['table'] in key_tables:
            schema_name = table_info['schema']
            table_name = table_info['table']
            
            notebook.append(f'''# COMMAND ----------

@dlt.table(
    name="silver_{table_name.lower()}",
    comment="Cleaned {table_name} data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "{table_name}ID IS NOT NULL")
def silver_{table_name.lower()}():
    """
    Silver table for {table_name}
    Applies data quality rules and transformations
    """
    return (
        dlt.read("bronze_{schema_name}_{table_name}")
        .filter(col("DeletedAt").isNull())  # Exclude soft-deleted records
        .withColumn("_ingested_at", current_timestamp())
    )
''')
    
    # Gold layer - views/aggregates
    notebook.append('''# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Views & Aggregates
# MAGIC These views provide business-ready data for reporting and analytics.
''')
    
    for view_info in views[:10]:  # Limit to 10 views
        schema_name = view_info['schema']
        view_name = view_info['view']
        
        notebook.append(f'''# COMMAND ----------

@dlt.view(
    name="gold_{view_name.lower()}",
    comment="Business view: {view_name}"
)
def gold_{view_name.lower()}():
    """
    Gold view: {schema_name}.{view_name}
    Business-ready aggregated data
    """
    # Original SQL from SQL Server has been transpiled
    # TODO: Implement view logic using silver layer tables
    return spark.sql("""
        SELECT * 
        FROM LIVE.silver_worker
        LIMIT 100
    """)
''')
    
    # Utility functions
    notebook.append('''# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration
# MAGIC 
# MAGIC To run this pipeline:
# MAGIC 1. Configure SQL Server secrets in Databricks
# MAGIC 2. Update bronze table JDBC connections
# MAGIC 3. Create DLT pipeline with this notebook
# MAGIC 4. Set target catalog and schema
# MAGIC 
# MAGIC ### Required Secrets
# MAGIC ```
# MAGIC databricks secrets create-scope --scope wakecap
# MAGIC databricks secrets put --scope wakecap --key sqlserver_jdbc_url
# MAGIC databricks secrets put --scope wakecap --key sqlserver_user
# MAGIC databricks secrets put --scope wakecap --key sqlserver_password
# MAGIC ```
''')
    
    return '\n'.join(notebook)


def parse_transpiled_files(transpiled_dir: Path) -> tuple[list, list]:
    """Parse transpiled SQL files and extract table/view information."""
    tables = []
    views = []
    
    # Parse tables
    tables_dir = transpiled_dir / "tables"
    if tables_dir.exists():
        for sql_file in tables_dir.glob("*.sql"):
            try:
                content = sql_file.read_text(encoding='utf-8', errors='ignore')
                
                # Check if transpiled successfully (Errors: 0)
                if "Errors: 0" not in content[:500]:
                    continue
                
                schema, table = extract_table_name(content)
                columns = extract_columns_from_create_table(content)
                
                tables.append({
                    'file': sql_file.name,
                    'schema': schema,
                    'table': table,
                    'columns': columns,
                    'content': content
                })
            except Exception as e:
                print(f"  Warning: Could not parse {sql_file.name}: {e}")
    
    # Parse views
    views_dir = transpiled_dir / "views"
    if views_dir.exists():
        for sql_file in views_dir.glob("*.sql"):
            try:
                content = sql_file.read_text(encoding='utf-8', errors='ignore')
                
                # Check if transpiled successfully
                if "Errors: 0" not in content[:500]:
                    continue
                
                schema, view = extract_table_name(content)
                
                views.append({
                    'file': sql_file.name,
                    'schema': schema,
                    'view': view,
                    'content': content
                })
            except Exception as e:
                print(f"  Warning: Could not parse {sql_file.name}: {e}")
    
    return tables, views


def main():
    print("=" * 60)
    print("DLT PIPELINE GENERATION")
    print("=" * 60)
    
    base_dir = Path(__file__).parent
    transpiled_dir = base_dir / "transpiled"
    pipelines_dir = base_dir / "pipelines"
    
    # Ensure output directory exists
    pipelines_dir.mkdir(exist_ok=True)
    
    # Configuration
    catalog = "wakecap_prod"
    schema = "migration"
    
    print(f"Parsing transpiled SQL files from: {transpiled_dir}")
    
    # Parse transpiled files
    tables, views = parse_transpiled_files(transpiled_dir)
    
    print(f"\nSuccessfully parsed:")
    print(f"  - Tables: {len(tables)}")
    print(f"  - Views: {len(views)}")
    
    if not tables and not views:
        print("\n[WARN] No successfully transpiled files found!")
        print("       Generating template pipeline anyway...")
        tables = [{'schema': 'dbo', 'table': 'Worker', 'columns': []}]
        views = [{'schema': 'dbo', 'view': 'vwWorker'}]
    
    # Generate DLT notebook
    print(f"\nGenerating DLT pipeline notebook...")
    notebook_content = generate_dlt_notebook(tables, views, catalog, schema)
    
    # Write notebook
    notebook_path = pipelines_dir / "wakecap_migration_pipeline.py"
    with open(notebook_path, 'w', encoding='utf-8') as f:
        f.write(notebook_content)
    
    print(f"[OK] DLT notebook generated: {notebook_path}")
    
    # Generate pipeline configuration JSON
    pipeline_config = {
        "name": "WakeCapDW_Migration_Pipeline",
        "catalog": catalog,
        "schema": schema,
        "target": f"{catalog}.{schema}",
        "development": True,
        "continuous": False,
        "channel": "PREVIEW",
        "photon": True,
        "tables": [
            {
                "name": f"bronze_{t['schema']}_{t['table']}",
                "source_schema": t['schema'],
                "source_table": t['table']
            }
            for t in tables[:30]
        ],
        "views": [
            {
                "name": f"gold_{v['view'].lower()}",
                "source_schema": v['schema'],
                "source_view": v['view']
            }
            for v in views[:10]
        ]
    }
    
    config_path = pipelines_dir / "pipeline_config.json"
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(pipeline_config, f, indent=2)
    
    print(f"[OK] Pipeline config generated: {config_path}")
    
    # Generate summary
    summary = f"""# DLT Pipeline Generation Summary

**Generated:** {datetime.now().isoformat()}
**Target Catalog:** {catalog}
**Target Schema:** {schema}

## Pipeline Contents

### Bronze Layer (Raw Ingestion)
- {len(tables)} tables from SQL Server

### Silver Layer (Cleaned Data)  
- Key dimension tables with data quality checks

### Gold Layer (Business Views)
- {len(views)} business views

## Files Generated

1. `wakecap_migration_pipeline.py` - Main DLT notebook
2. `pipeline_config.json` - Pipeline configuration

## Next Steps

1. **Upload to Databricks:**
   ```
   databricks workspace import pipelines/wakecap_migration_pipeline.py /Workspace/Shared/migrations/wakecap/
   ```

2. **Create DLT Pipeline:**
   - Go to Databricks Workflows > Delta Live Tables
   - Create new pipeline
   - Select the uploaded notebook
   - Configure target: `{catalog}.{schema}`
   - Enable development mode for testing

3. **Configure Secrets:**
   ```bash
   databricks secrets create-scope --scope wakecap
   databricks secrets put --scope wakecap --key sqlserver_jdbc_url
   databricks secrets put --scope wakecap --key sqlserver_user  
   databricks secrets put --scope wakecap --key sqlserver_password
   ```

4. **Run Pipeline:**
   - Start in development mode
   - Validate data quality
   - Switch to production mode
"""
    
    summary_path = pipelines_dir / "DLT_GENERATION_SUMMARY.md"
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(summary)
    
    print(f"[OK] Summary generated: {summary_path}")
    
    print("\n" + "=" * 60)
    print("DLT PIPELINE GENERATION COMPLETE")
    print("=" * 60)
    print(f"Output directory: {pipelines_dir}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
