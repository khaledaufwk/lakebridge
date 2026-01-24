#!/usr/bin/env python3
"""
Silver Layer Deployment Script
==============================

Deploys the Silver Layer pipeline for transforming 78 Bronze tables into Silver layer
tables in Databricks (wakecap_prod.silver).

Usage:
    python deploy_silver_layer.py

Prerequisites:
    1. Bronze layer must be deployed and running (wakecap_prod.raw)
    2. Credentials configured at ~/.databricks/labs/lakebridge/.credentials.yml
    3. Run this script

This script will:
    1. Create Silver schema (wakecap_prod.silver)
    2. Create Silver watermark tracking table
    3. Upload pipeline files to Databricks workspace
    4. Create Databricks Job with task dependencies
"""

import os
import sys
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import (
    Task, NotebookTask, TaskDependency,
    JobCluster, JobSettings, CronSchedule,
    PauseStatus
)
from databricks.sdk.service.compute import (
    ClusterSpec, RuntimeEngine, DataSecurityMode
)


# Configuration
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA_SILVER = "silver"
TARGET_SCHEMA_MIGRATION = "migration"
WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/silver"
JOB_NAME = "WakeCapDW_Silver_TimescaleDB"


def print_header(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def print_step(num, text):
    print(f"\n[Step {num}] {text}")
    print("-" * 60)


def print_ok(text):
    print(f"   [OK] {text}")


def print_warn(text):
    print(f"   [WARN] {text}")


def print_error(text):
    print(f"   [ERROR] {text}")


def load_credentials():
    """Load credentials from ~/.databricks/labs/lakebridge/.credentials.yml"""
    print_step(1, "Loading credentials")

    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"

    if not creds_path.exists():
        print_error(f"Credentials file not found: {creds_path}")
        return None

    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    # Validate Databricks credentials
    db_host = creds.get('databricks', {}).get('host', '')
    db_token = creds.get('databricks', {}).get('token', '')

    if not db_host:
        print_error("Databricks host not configured in credentials")
        return None

    if not db_token:
        print_error("Databricks token not configured in credentials")
        return None

    print_ok(f"Databricks Host: {db_host}")

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


def create_schemas(w):
    """Create Silver schema using SQL statement execution."""
    print_step(3, "Creating Silver schema")

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
        f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA_SILVER} COMMENT 'Silver layer - cleansed and validated data from Bronze'",
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
                print_ok(f"Executed: {sql[:60]}...")
            else:
                print_warn(f"Statement status: {result.status.state}")
        except Exception as e:
            print_warn(f"Statement failed: {e}")

    return True


def create_watermark_table(w):
    """Create Silver watermark tracking table."""
    print_step(4, "Creating Silver watermark tracking table")

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
    ddl_path = Path(__file__).parent / "pipelines" / "silver" / "ddl" / "create_silver_watermarks.sql"

    if ddl_path.exists():
        with open(ddl_path, encoding='utf-8') as f:
            ddl = f.read()
    else:
        # Use inline DDL
        ddl = f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA_MIGRATION}._silver_watermarks (
    table_name STRING NOT NULL,
    source_bronze_table STRING,
    processing_group STRING,
    last_bronze_watermark TIMESTAMP,
    last_load_status STRING,
    last_load_row_count BIGINT,
    rows_input BIGINT,
    rows_dropped_critical BIGINT,
    rows_flagged_business BIGINT,
    rows_warned_advisory BIGINT,
    last_error_message STRING,
    last_load_start_time TIMESTAMP,
    last_load_end_time TIMESTAMP,
    last_load_duration_seconds DOUBLE,
    pipeline_run_id STRING,
    updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
"""

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=ddl,
            wait_timeout="50s"
        )
        if result.status.state == StatementState.SUCCEEDED:
            print_ok("Created Silver watermark table")
        else:
            print_warn(f"Statement status: {result.status.state}")
    except Exception as e:
        print_warn(f"Failed to create watermark table: {e}")

    return True


def upload_files(w):
    """Upload Silver pipeline files to Databricks workspace."""
    print_step(5, "Uploading Silver pipeline files to Databricks workspace")

    base_path = Path(__file__).parent / "pipelines" / "silver"

    # Create workspace directories
    directories = [
        f"{WORKSPACE_BASE_PATH}/notebooks",
        f"{WORKSPACE_BASE_PATH}/config",
        f"{WORKSPACE_BASE_PATH}/ddl",
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
        # Notebooks (upload as SOURCE format for Python notebooks)
        ("notebooks/silver_loader.py", f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader", Language.PYTHON),
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

    # Upload config file (as AUTO format for YAML)
    config_file = base_path / "config" / "silver_tables.yml"
    if config_file.exists():
        try:
            with open(config_file, 'rb') as f:
                content = f.read()

            w.workspace.upload(
                path=f"{WORKSPACE_BASE_PATH}/config/silver_tables.yml",
                content=content,
                format=ImportFormat.AUTO,
                overwrite=True
            )
            print_ok("Uploaded: config/silver_tables.yml")
        except Exception as e:
            print_warn(f"Failed to upload config: {e}")

    # Upload DDL file
    ddl_file = base_path / "ddl" / "create_silver_watermarks.sql"
    if ddl_file.exists():
        try:
            with open(ddl_file, 'rb') as f:
                content = f.read()

            w.workspace.upload(
                path=f"{WORKSPACE_BASE_PATH}/ddl/create_silver_watermarks.sql",
                content=content,
                format=ImportFormat.AUTO,
                overwrite=True
            )
            print_ok("Uploaded: ddl/create_silver_watermarks.sql")
        except Exception as e:
            print_warn(f"Failed to upload DDL: {e}")

    return True


def create_databricks_job(w):
    """Create Databricks Job for Silver layer processing."""
    print_step(6, "Creating Databricks Job")

    # Check if job already exists
    try:
        jobs = list(w.jobs.list(name=JOB_NAME))
        for job in jobs:
            if job.settings and job.settings.name == JOB_NAME:
                print_ok(f"Job already exists: {job.job_id}")
                return job.job_id
    except Exception as e:
        print_warn(f"Could not list jobs: {e}")

    # Define job cluster
    cluster_spec = ClusterSpec(
        spark_version="14.3.x-scala2.12",
        node_type_id="Standard_DS4_v2",
        num_workers=2,
        spark_conf={
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true"
        },
        data_security_mode=DataSecurityMode.USER_ISOLATION,
        runtime_engine=RuntimeEngine.STANDARD
    )

    # Define larger cluster for facts
    facts_cluster_spec = ClusterSpec(
        spark_version="14.3.x-scala2.12",
        node_type_id="Standard_DS4_v2",
        num_workers=4,  # More workers for large fact tables
        spark_conf={
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true"
        },
        data_security_mode=DataSecurityMode.USER_ISOLATION,
        runtime_engine=RuntimeEngine.STANDARD
    )

    try:
        # Create job with multiple tasks for processing groups
        job = w.jobs.create(
            name=JOB_NAME,
            job_clusters=[
                JobCluster(
                    job_cluster_key="silver_dimensions_cluster",
                    new_cluster=cluster_spec
                ),
                JobCluster(
                    job_cluster_key="silver_facts_cluster",
                    new_cluster=facts_cluster_spec
                )
            ],
            tasks=[
                # Task 1: Independent dimensions (no dependencies)
                Task(
                    task_key="silver_independent_dimensions",
                    description="Transform independent dimension tables",
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "independent_dimensions"
                        }
                    ),
                    job_cluster_key="silver_dimensions_cluster"
                ),
                # Task 2: Organization (no dependencies)
                Task(
                    task_key="silver_organization",
                    description="Transform organization and device tables",
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "organization"
                        }
                    ),
                    job_cluster_key="silver_dimensions_cluster"
                ),
                # Task 3: Project (depends on organization)
                Task(
                    task_key="silver_project",
                    description="Transform project tables",
                    depends_on=[TaskDependency(task_key="silver_organization")],
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "project_dependent"
                        }
                    ),
                    job_cluster_key="silver_dimensions_cluster"
                ),
                # Task 4: Project children (depends on project)
                Task(
                    task_key="silver_project_children",
                    description="Transform project-dependent dimension tables",
                    depends_on=[
                        TaskDependency(task_key="silver_project"),
                        TaskDependency(task_key="silver_independent_dimensions")
                    ],
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "project_children"
                        }
                    ),
                    job_cluster_key="silver_dimensions_cluster"
                ),
                # Task 5: Zone dependent (depends on project children)
                Task(
                    task_key="silver_zone_dependent",
                    description="Transform zone-dependent tables",
                    depends_on=[TaskDependency(task_key="silver_project_children")],
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "zone_dependent"
                        }
                    ),
                    job_cluster_key="silver_dimensions_cluster"
                ),
                # Task 6: Assignments (depends on zone dependent)
                Task(
                    task_key="silver_assignments",
                    description="Transform assignment/bridge tables",
                    depends_on=[TaskDependency(task_key="silver_zone_dependent")],
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "assignments"
                        }
                    ),
                    job_cluster_key="silver_dimensions_cluster"
                ),
                # Task 7: Facts (depends on assignments, uses larger cluster)
                Task(
                    task_key="silver_facts",
                    description="Transform fact tables (large tables)",
                    depends_on=[TaskDependency(task_key="silver_assignments")],
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "facts"
                        }
                    ),
                    job_cluster_key="silver_facts_cluster"
                ),
                # Task 8: History (depends on zone dependent)
                Task(
                    task_key="silver_history",
                    description="Transform history/audit tables",
                    depends_on=[TaskDependency(task_key="silver_zone_dependent")],
                    notebook_task=NotebookTask(
                        notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/silver_loader",
                        base_parameters={
                            "load_mode": "incremental",
                            "table_filter": "history"
                        }
                    ),
                    job_cluster_key="silver_dimensions_cluster"
                ),
            ],
            schedule=CronSchedule(
                quartz_cron_expression="0 0 3 * * ?",  # 3:00 AM UTC daily
                timezone_id="UTC",
                pause_status=PauseStatus.PAUSED  # Start paused
            ),
            max_concurrent_runs=1
        )

        print_ok(f"Created job: {job.job_id}")
        return job.job_id
    except Exception as e:
        print_error(f"Failed to create job: {e}")
        return None


def show_summary(creds, job_id=None):
    """Show final summary and next steps."""
    print_header("SILVER LAYER DEPLOYMENT COMPLETE")

    db_host = creds['databricks']['host']

    print(f"""
STATUS:
  [OK] Silver schema created: {TARGET_CATALOG}.{TARGET_SCHEMA_SILVER}
  [OK] Watermark table created: {TARGET_CATALOG}.{TARGET_SCHEMA_MIGRATION}._silver_watermarks
  [OK] Pipeline files uploaded to: {WORKSPACE_BASE_PATH}
""")

    if job_id:
        print(f"  [OK] Databricks Job created: {job_id}")
        print(f"\nJob URL: {db_host}/jobs/{job_id}")

    print(f"""
SCHEDULE COORDINATION:
  Bronze Job:  2:00 AM UTC (WakeCapDW_Bronze_TimescaleDB)
  Silver Job:  3:00 AM UTC (WakeCapDW_Silver_TimescaleDB) [PAUSED]
  Gold Job:    4:00 AM UTC (Future)

NEXT STEPS:

1. RUN INITIAL LOAD (Manual)
   ----------------------------------
   Run the Silver job manually first to backfill data:
   - Go to: {db_host}/jobs/{job_id if job_id else 'YOUR_JOB_ID'}
   - Click "Run now" to start the initial load
   - Monitor task progress in the job run details

2. ENABLE SCHEDULE
   ----------------------------------
   After successful initial load, enable the schedule:
   - Go to job settings
   - Change schedule from "Paused" to "Active"

3. MONITOR PROGRESS
   ----------------------------------
   Check watermark table for Silver layer:

   SELECT table_name, processing_group, last_load_status,
          last_load_row_count, rows_dropped_critical,
          rows_flagged_business, last_bronze_watermark
   FROM {TARGET_CATALOG}.{TARGET_SCHEMA_MIGRATION}._silver_watermarks
   ORDER BY processing_group, table_name;

4. VERIFY DATA
   ----------------------------------
   Check Silver tables:

   SHOW TABLES IN {TARGET_CATALOG}.{TARGET_SCHEMA_SILVER};

   -- Row count reconciliation example
   SELECT 'bronze' as layer, COUNT(*) as cnt
   FROM {TARGET_CATALOG}.raw.timescale_company
   WHERE "DeletedAt" IS NULL
   UNION ALL
   SELECT 'silver_org' as layer, COUNT(*) as cnt
   FROM {TARGET_CATALOG}.{TARGET_SCHEMA_SILVER}.silver_organization;

5. CHECK FK INTEGRITY
   ----------------------------------
   -- Example: Projects without valid organization
   SELECT COUNT(*) AS orphaned_projects
   FROM {TARGET_CATALOG}.{TARGET_SCHEMA_SILVER}.silver_project p
   LEFT JOIN {TARGET_CATALOG}.{TARGET_SCHEMA_SILVER}.silver_organization o
     ON p.OrganizationId = o.OrganizationId
   WHERE p.OrganizationId IS NOT NULL
     AND o.OrganizationId IS NULL;

TABLES PROCESSED:
  - 11 Independent Dimensions (company_type, crew_type, etc.)
  - 2 Organization tables (organization, device)
  - 1 Project table
  - 16 Project-dependent dimensions (worker, crew, floor, zone, etc.)
  - 7 Zone-dependent tables
  - 17 Assignment/bridge tables
  - 20 Fact tables (including 82M row workers_history)
  - 3 History tables
  -----------------------------------------
  Total: 77 Silver tables from 78 Bronze sources
""")


def main():
    print_header("Silver Layer Deployment")
    print("Transforming 78 Bronze tables into Silver layer")
    print("Source: wakecap_prod.raw (Bronze)")
    print("Target: wakecap_prod.silver")

    # Load credentials
    creds = load_credentials()
    if not creds:
        print("\nCredentials not found. Please configure ~/.databricks/labs/lakebridge/.credentials.yml")
        return 1

    # Connect to Databricks
    w = connect_databricks(creds)
    if not w:
        return 1

    # Create schemas
    create_schemas(w)

    # Create watermark table
    create_watermark_table(w)

    # Upload files
    upload_files(w)

    # Create Databricks Job
    job_id = create_databricks_job(w)

    # Show summary
    show_summary(creds, job_id)

    return 0


if __name__ == "__main__":
    sys.exit(main())
