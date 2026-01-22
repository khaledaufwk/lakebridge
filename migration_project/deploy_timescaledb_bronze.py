#!/usr/bin/env python3
"""
TimescaleDB Bronze Layer Deployment Script
==========================================

Deploys the Phase 1 Bronze Layer pipeline for loading 81 tables from TimescaleDB
into Databricks (wakecap_prod.source_timescaledb).

Usage:
    python deploy_timescaledb_bronze.py

Prerequisites:
    1. Fill in credentials_template.yml with TimescaleDB and Databricks credentials
    2. Run this script

This script will:
    1. Create secret scope for TimescaleDB credentials
    2. Create target schemas (source_timescaledb, migration)
    3. Create watermark tracking table
    4. Upload pipeline files to Databricks workspace
    5. Create DLT pipeline (optional)
    6. Test connectivity
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary


# Configuration
TIMESCALE_SECRET_SCOPE = "wakecap-timescale"
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA_BRONZE = "source_timescaledb"
TARGET_SCHEMA_MIGRATION = "migration"
WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/timescaledb"


def print_header(text):
    print("\n" + "=" * 60)
    print(text)
    print("=" * 60)


def print_step(num, text):
    print(f"\n[Step {num}] {text}")
    print("-" * 50)


def print_ok(text):
    print(f"   [OK] {text}")


def print_warn(text):
    print(f"   [WARN] {text}")


def print_error(text):
    print(f"   [ERROR] {text}")


def load_credentials():
    """Load credentials from template file."""
    print_step(1, "Loading credentials")

    template_path = Path(__file__).parent / "credentials_template.yml"

    if not template_path.exists():
        print_error(f"Credentials file not found: {template_path}")
        return None

    with open(template_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    # Validate Databricks credentials
    db_host = creds.get('databricks', {}).get('host', '')
    db_token = creds.get('databricks', {}).get('token', '')

    if not db_host or 'YOUR_' in db_host:
        print_error("Please fill in your Databricks host in credentials_template.yml")
        return None

    if not db_token or 'YOUR_' in db_token:
        print_error("Please fill in your Databricks token in credentials_template.yml")
        return None

    # Validate TimescaleDB credentials
    ts_host = creds.get('timescaledb', {}).get('host', '')
    ts_user = creds.get('timescaledb', {}).get('user', '')
    ts_pass = creds.get('timescaledb', {}).get('password', '')

    if not ts_host or 'YOUR_' in ts_host:
        print_error("Please fill in your TimescaleDB host in credentials_template.yml")
        return None

    if not ts_user or 'YOUR_' in ts_user:
        print_error("Please fill in your TimescaleDB user in credentials_template.yml")
        return None

    if not ts_pass or 'YOUR_' in ts_pass:
        print_error("Please fill in your TimescaleDB password in credentials_template.yml")
        return None

    print_ok(f"Databricks Host: {db_host}")
    print_ok(f"TimescaleDB Host: {ts_host}")
    print_ok(f"TimescaleDB Database: {creds['timescaledb'].get('database', 'wakecap_app')}")

    return creds


def connect_databricks(creds):
    """Connect to Databricks workspace."""
    print_step(2, "Connecting to Databricks")

    try:
        w = WorkspaceClient(
            host=creds['databricks']['host'],
            token=creds['databricks']['token']
        )

        user = w.current_user.me()
        print_ok(f"Connected as: {user.user_name}")
        return w
    except Exception as e:
        print_error(f"Connection failed: {e}")
        return None


def setup_timescale_secrets(w, creds):
    """Setup secret scope with TimescaleDB credentials."""
    print_step(3, "Configuring TimescaleDB secret scope")

    ts = creds['timescaledb']

    # Create scope
    try:
        w.secrets.create_scope(scope=TIMESCALE_SECRET_SCOPE)
        print_ok(f"Created scope: {TIMESCALE_SECRET_SCOPE}")
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            print_ok(f"Scope exists: {TIMESCALE_SECRET_SCOPE}")
        else:
            print_warn(f"Scope issue: {e}")

    # Store secrets
    secrets = [
        ("timescaledb-host", ts['host']),
        ("timescaledb-port", str(ts.get('port', 5432))),
        ("timescaledb-database", ts.get('database', 'wakecap_app')),
        ("timescaledb-user", ts['user']),
        ("timescaledb-password", ts['password']),
    ]

    print("   Storing secrets...")
    for key, value in secrets:
        try:
            w.secrets.put_secret(scope=TIMESCALE_SECRET_SCOPE, key=key, string_value=value)
            print(f"     [OK] {key}")
        except Exception as e:
            print(f"     [ERROR] {key}: {e}")

    # List secrets
    print("\n   Secrets in scope:")
    try:
        for s in w.secrets.list_secrets(scope=TIMESCALE_SECRET_SCOPE):
            print(f"     - {s.key}")
    except Exception as e:
        print_error(f"Failed to list secrets: {e}")

    return True


def create_schemas(w):
    """Create target schemas using SQL statement execution."""
    print_step(4, "Creating target schemas")

    # Find a SQL warehouse to use
    try:
        warehouses = list(w.warehouses.list())
        if not warehouses:
            print_warn("No SQL warehouse found. Please create schemas manually.")
            return False

        # Use the first available warehouse
        warehouse_id = warehouses[0].id
        print_ok(f"Using SQL warehouse: {warehouses[0].name}")
    except Exception as e:
        print_warn(f"Could not list warehouses: {e}")
        print("   Will use notebook for schema creation")
        return False

    # SQL statements to execute
    statements = [
        f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}",
        f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA_BRONZE} COMMENT 'Bronze layer - raw data from TimescaleDB'",
        f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA_MIGRATION} COMMENT 'Migration tracking and metadata'",
    ]

    for sql in statements:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout="30s"
            )
            if result.status.state == StatementState.SUCCEEDED:
                print_ok(f"Executed: {sql[:50]}...")
            else:
                print_warn(f"Statement status: {result.status.state}")
        except Exception as e:
            print_warn(f"Statement failed: {e}")

    return True


def create_watermark_table(w):
    """Create watermark tracking table."""
    print_step(5, "Creating watermark tracking table")

    # Find a SQL warehouse
    try:
        warehouses = list(w.warehouses.list())
        if not warehouses:
            print_warn("No SQL warehouse found. Please create table manually.")
            return False

        warehouse_id = warehouses[0].id
    except Exception as e:
        print_warn(f"Could not list warehouses: {e}")
        return False

    # Read the DDL file
    ddl_path = Path(__file__).parent / "pipelines" / "timescaledb" / "ddl" / "create_watermark_table.sql"

    if not ddl_path.exists():
        print_warn(f"DDL file not found: {ddl_path}")
        # Use inline DDL
        ddl = f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA_MIGRATION}._timescaledb_watermarks (
    source_system STRING NOT NULL,
    source_schema STRING NOT NULL,
    source_table STRING NOT NULL,
    watermark_column STRING NOT NULL,
    watermark_type STRING NOT NULL,
    last_watermark_value STRING,
    last_watermark_timestamp TIMESTAMP,
    last_watermark_bigint BIGINT,
    last_load_start_time TIMESTAMP,
    last_load_end_time TIMESTAMP,
    last_load_status STRING,
    last_load_row_count BIGINT,
    last_error_message STRING,
    pipeline_id STRING,
    pipeline_run_id STRING,
    created_at TIMESTAMP DEFAULT current_timestamp(),
    updated_at TIMESTAMP,
    created_by STRING DEFAULT current_user()
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
"""
    else:
        with open(ddl_path, encoding='utf-8') as f:
            ddl = f.read()

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=ddl,
            wait_timeout="60s"
        )
        if result.status.state == StatementState.SUCCEEDED:
            print_ok("Created watermark table")
        else:
            print_warn(f"Statement status: {result.status.state}")
    except Exception as e:
        print_warn(f"Failed to create watermark table: {e}")

    return True


def upload_files(w):
    """Upload pipeline files to Databricks workspace."""
    print_step(6, "Uploading pipeline files to Databricks workspace")

    base_path = Path(__file__).parent / "pipelines" / "timescaledb"

    # Create workspace directories
    directories = [
        f"{WORKSPACE_BASE_PATH}/notebooks",
        f"{WORKSPACE_BASE_PATH}/src",
        f"{WORKSPACE_BASE_PATH}/config",
    ]

    for dir_path in directories:
        try:
            w.workspace.mkdirs(dir_path)
            print_ok(f"Created directory: {dir_path}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print_ok(f"Directory exists: {dir_path}")
            else:
                print_warn(f"Directory issue: {e}")

    # Files to upload
    files_to_upload = [
        # Notebooks
        ("notebooks/bronze_loader_facts.py", f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_facts", Language.PYTHON),
        ("notebooks/bronze_loader_dimensions.py", f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_dimensions", Language.PYTHON),
        ("notebooks/bronze_loader_assignments.py", f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_assignments", Language.PYTHON),
        # Source files
        ("src/timescaledb_loader.py", f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader.py", Language.PYTHON),
        ("src/table_discovery.py", f"{WORKSPACE_BASE_PATH}/src/table_discovery.py", Language.PYTHON),
        ("src/__init__.py", f"{WORKSPACE_BASE_PATH}/src/__init__.py", Language.PYTHON),
        # DLT pipeline
        ("dlt_timescaledb_bronze.py", f"{WORKSPACE_BASE_PATH}/dlt_timescaledb_bronze", Language.PYTHON),
    ]

    for local_file, workspace_path, language in files_to_upload:
        local_path = base_path / local_file

        if not local_path.exists():
            print_warn(f"File not found: {local_path}")
            continue

        try:
            with open(local_path, 'rb') as f:
                content = f.read()

            w.workspace.upload(
                path=workspace_path,
                content=content,
                format=ImportFormat.SOURCE,
                language=language,
                overwrite=True
            )
            print_ok(f"Uploaded: {local_file}")
        except Exception as e:
            print_warn(f"Failed to upload {local_file}: {e}")

    # Upload config file (as auto format)
    config_file = base_path / "config" / "timescaledb_tables.yml"
    if config_file.exists():
        try:
            with open(config_file, 'rb') as f:
                content = f.read()

            w.workspace.upload(
                path=f"{WORKSPACE_BASE_PATH}/config/timescaledb_tables.yml",
                content=content,
                format=ImportFormat.AUTO,
                overwrite=True
            )
            print_ok("Uploaded: config/timescaledb_tables.yml")
        except Exception as e:
            print_warn(f"Failed to upload config: {e}")

    return True


def create_dlt_pipeline(w):
    """Create DLT pipeline for TimescaleDB Bronze layer."""
    print_step(7, "Creating DLT pipeline")

    pipeline_name = "WakeCapDW_Bronze_TimescaleDB"

    # Check if pipeline already exists
    try:
        pipelines = list(w.pipelines.list_pipelines())
        for p in pipelines:
            if p.name == pipeline_name:
                print_ok(f"Pipeline already exists: {p.pipeline_id}")
                return p.pipeline_id
    except Exception as e:
        print_warn(f"Could not list pipelines: {e}")

    try:
        pipeline = w.pipelines.create(
            name=pipeline_name,
            catalog=TARGET_CATALOG,
            target=TARGET_SCHEMA_BRONZE,
            development=True,
            serverless=True,
            continuous=False,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=f"{WORKSPACE_BASE_PATH}/dlt_timescaledb_bronze"
                    )
                )
            ],
            configuration={
                "spark.databricks.delta.schema.autoMerge.enabled": "true"
            }
        )

        print_ok(f"Created pipeline: {pipeline.pipeline_id}")
        return pipeline.pipeline_id
    except Exception as e:
        print_warn(f"Failed to create pipeline: {e}")
        return None


def show_summary(creds, pipeline_id=None):
    """Show final summary and next steps."""
    print_header("DEPLOYMENT COMPLETE")

    db_host = creds['databricks']['host']

    print(f"""
STATUS:
  [OK] TimescaleDB secrets configured in scope: {TIMESCALE_SECRET_SCOPE}
  [OK] Target schemas created: {TARGET_CATALOG}.{TARGET_SCHEMA_BRONZE}
  [OK] Watermark table created: {TARGET_CATALOG}.{TARGET_SCHEMA_MIGRATION}._timescaledb_watermarks
  [OK] Pipeline files uploaded to: {WORKSPACE_BASE_PATH}
""")

    if pipeline_id:
        print(f"  [OK] DLT Pipeline created: {pipeline_id}")
        print(f"\nPipeline URL: {db_host}/pipelines/{pipeline_id}")

    print(f"""
NEXT STEPS:

1. TEST CONNECTIVITY
   ----------------------------------
   Open a Databricks notebook and run:

   host = dbutils.secrets.get("{TIMESCALE_SECRET_SCOPE}", "timescaledb-host")
   port = dbutils.secrets.get("{TIMESCALE_SECRET_SCOPE}", "timescaledb-port")
   database = dbutils.secrets.get("{TIMESCALE_SECRET_SCOPE}", "timescaledb-database")
   user = dbutils.secrets.get("{TIMESCALE_SECRET_SCOPE}", "timescaledb-user")
   password = dbutils.secrets.get("{TIMESCALE_SECRET_SCOPE}", "timescaledb-password")

   jdbc_url = f"jdbc:postgresql://{{host}}:{{port}}/{{database}}?sslmode=require"

   test_df = (spark.read
       .format("jdbc")
       .option("url", jdbc_url)
       .option("query", "SELECT 1 as test")
       .option("user", user)
       .option("password", password)
       .option("driver", "org.postgresql.Driver")
       .load()
   )
   test_df.show()

2. RUN INITIAL LOAD
   ----------------------------------
   Option A: Run DLT Pipeline
   - Go to: {db_host}/pipelines/{pipeline_id if pipeline_id else 'YOUR_PIPELINE_ID'}
   - Click "Start" to begin loading all 81 tables

   Option B: Run Notebooks
   - {WORKSPACE_BASE_PATH}/notebooks/bronze_loader_dimensions
   - {WORKSPACE_BASE_PATH}/notebooks/bronze_loader_assignments
   - {WORKSPACE_BASE_PATH}/notebooks/bronze_loader_facts

3. MONITOR PROGRESS
   ----------------------------------
   Check watermark table:
   SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMA_MIGRATION}._timescaledb_watermarks
   ORDER BY last_load_end_time DESC

4. VERIFY DATA
   ----------------------------------
   Check loaded tables:
   SHOW TABLES IN {TARGET_CATALOG}.{TARGET_SCHEMA_BRONZE}
""")


def main():
    print_header("TimescaleDB Bronze Layer Deployment")
    print("Phase 1: Loading 81 tables from TimescaleDB")

    # Load credentials
    creds = load_credentials()
    if not creds:
        print("\nPlease fill in credentials_template.yml and run again.")
        return 1

    # Connect to Databricks
    w = connect_databricks(creds)
    if not w:
        return 1

    # Setup TimescaleDB secrets
    setup_timescale_secrets(w, creds)

    # Create schemas
    create_schemas(w)

    # Create watermark table
    create_watermark_table(w)

    # Upload files
    upload_files(w)

    # Create DLT pipeline
    pipeline_id = create_dlt_pipeline(w)

    # Show summary
    show_summary(creds, pipeline_id)

    return 0


if __name__ == "__main__":
    sys.exit(main())
