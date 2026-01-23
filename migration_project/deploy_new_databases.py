#!/usr/bin/env python3
"""
Deploy New TimescaleDB Database Loaders
========================================
This script deploys loaders for:
1. wakecap_observation - Observation/incident tracking data (6 tables, ~8M rows)
2. weather-station - Weather monitoring configuration (7 tables, ~115 rows)

Actions:
- Creates secret scopes for each database
- Uploads loader notebooks and configurations
- Updates the WakeCapDW_Bronze_TimescaleDB_Raw job with new tasks

Usage:
    python deploy_new_databases.py
"""

import sys
from pathlib import Path
import yaml
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import Task, NotebookTask
from databricks.sdk.service.compute import ClusterSpec


# Configuration
WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/timescaledb"
JOB_NAME = "WakeCapDW_Bronze_TimescaleDB_Raw"
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "raw"

# Database configurations
DATABASES = {
    "observation": {
        "creds_key": "timescaledb_observation",
        "secret_scope": "wakecap-observation",
        "table_prefix": "observation_",
        "notebook": "bronze_loader_observation",
        "config": "timescaledb_tables_observation.yml",
        "description": "Observation/incident tracking data (~8M rows)",
        "cluster_workers": 2,
        "timeout_seconds": 7200,
    },
    "weather": {
        "creds_key": "timescaledb_weather",
        "secret_scope": "wakecap-weather",
        "table_prefix": "weather_",
        "notebook": "bronze_loader_weather",
        "config": "timescaledb_tables_weather.yml",
        "description": "Weather station configuration (~115 rows)",
        "cluster_workers": 1,
        "timeout_seconds": 1800,
    },
}


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def print_header(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def print_ok(text):
    print(f"   [OK] {text}")


def print_warn(text):
    print(f"   [WARN] {text}")


def print_error(text):
    print(f"   [ERROR] {text}")


def configure_secrets(w, creds, db_key, db_config):
    """Create secret scope and store credentials for a database."""
    print(f"\n   Configuring secrets for {db_key}...")

    creds_key = db_config["creds_key"]
    scope_name = db_config["secret_scope"]

    db_creds = creds.get(creds_key)
    if not db_creds:
        print_error(f"{creds_key} not found in credentials file!")
        return False

    # Create secret scope
    try:
        w.secrets.create_scope(scope=scope_name)
        print_ok(f"Secret scope '{scope_name}' created")
    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print_ok(f"Secret scope '{scope_name}' already exists")
        else:
            print_warn(f"Could not create scope: {e}")

    # Store secrets
    secrets_to_store = [
        ("timescaledb-host", db_creds['host']),
        ("timescaledb-port", str(db_creds['port'])),
        ("timescaledb-database", db_creds['database']),
        ("timescaledb-user", db_creds['user']),
        ("timescaledb-password", db_creds['password']),
    ]

    for key, value in secrets_to_store:
        try:
            w.secrets.put_secret(scope=scope_name, key=key, string_value=value)
        except Exception as e:
            print_error(f"Failed to store {key}: {e}")
            return False

    print_ok(f"Stored 5 secrets in '{scope_name}'")
    return True


def upload_files(w, db_key, db_config):
    """Upload notebook and configuration files for a database."""
    print(f"\n   Uploading files for {db_key}...")

    base_path = Path(__file__).parent / "pipelines" / "timescaledb"

    files = [
        (f"config/{db_config['config']}", f"{WORKSPACE_BASE_PATH}/config/{db_config['config']}", None),
        (f"notebooks/{db_config['notebook']}.py", f"{WORKSPACE_BASE_PATH}/notebooks/{db_config['notebook']}", Language.PYTHON),
    ]

    for local_file, workspace_path, language in files:
        local_path = base_path / local_file

        if not local_path.exists():
            print_warn(f"File not found: {local_path}")
            continue

        try:
            with open(local_path, 'rb') as f:
                content = f.read()

            if language:
                w.workspace.upload(
                    path=workspace_path,
                    content=content,
                    format=ImportFormat.SOURCE,
                    language=language,
                    overwrite=True
                )
            else:
                w.workspace.upload(
                    path=workspace_path,
                    content=content,
                    format=ImportFormat.AUTO,
                    overwrite=True
                )
            print_ok(f"Uploaded: {local_file}")
        except Exception as e:
            print_error(f"Failed to upload {local_file}: {e}")

    # Also ensure the source module is up to date
    loader_path = base_path / "src" / "timescaledb_loader_v2.py"
    if loader_path.exists():
        try:
            with open(loader_path, 'rb') as f:
                content = f.read()
            w.workspace.upload(
                path=f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader_v2.py",
                content=content,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True
            )
            print_ok("Updated: src/timescaledb_loader_v2.py")
        except Exception as e:
            print_warn(f"Could not update loader module: {e}")


def create_job_with_all_tasks(w):
    """Create or update the job with all loader tasks."""
    print_header("STEP 3: Creating/Updating Databricks Job")

    # Check if job exists
    existing_jobs = list(w.jobs.list())
    existing_job_id = None

    for job in existing_jobs:
        if job.settings and job.settings.name == JOB_NAME:
            existing_job_id = job.job_id
            print(f"   Found existing job: {job.job_id}")
            break

    # Define all tasks
    tasks = []

    # Main wakecap_app task
    tasks.append(Task(
        task_key="load_wakecap_app",
        description="Load all tables from TimescaleDB wakecap_app database (81 tables)",
        notebook_task=NotebookTask(
            notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_optimized",
            base_parameters={
                "load_mode": "incremental",
                "category": "ALL",
                "fetch_size": "50000",
                "batch_size": "100000"
            }
        ),
        new_cluster=ClusterSpec(
            spark_version="14.3.x-scala2.12",
            num_workers=4,
            node_type_id="Standard_DS4_v2",
            spark_conf={
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
                "spark.sql.shuffle.partitions": "200"
            }
        ),
        timeout_seconds=14400,  # 4 hours
    ))

    # Add tasks for new databases
    for db_key, db_config in DATABASES.items():
        tasks.append(Task(
            task_key=f"load_{db_key}",
            description=f"Load {db_config['description']}",
            notebook_task=NotebookTask(
                notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/{db_config['notebook']}",
                base_parameters={
                    "load_mode": "incremental",
                    "category": "ALL",
                    "fetch_size": "10000",
                    "batch_size": "50000"
                }
            ),
            new_cluster=ClusterSpec(
                spark_version="14.3.x-scala2.12",
                num_workers=db_config["cluster_workers"],
                node_type_id="Standard_DS3_v2",
                spark_conf={
                    "spark.databricks.delta.schema.autoMerge.enabled": "true",
                }
            ),
            timeout_seconds=db_config["timeout_seconds"],
        ))

    try:
        if existing_job_id:
            print("   Recreating job with all tasks...")
            w.jobs.delete(existing_job_id)
            print_ok(f"Deleted old job: {existing_job_id}")

        # Create new job with all tasks running in parallel
        job = w.jobs.create(
            name=JOB_NAME,
            tasks=tasks,
            max_concurrent_runs=1,
            timeout_seconds=18000,  # 5 hours total
        )
        print_ok(f"Created job with {len(tasks)} tasks: {job.job_id}")
        return job.job_id

    except Exception as e:
        print_error(f"Failed to create/update job: {e}")
        import traceback
        traceback.print_exc()
        return existing_job_id


def show_summary(creds, job_id):
    """Show deployment summary."""
    print_header("DEPLOYMENT COMPLETE")

    db_host = creds['databricks']['host']

    print(f"""
NEW DATABASES INTEGRATED:
=========================

1. OBSERVATION DATABASE (wakecap_observation):
   - Secret Scope: wakecap-observation
   - Tables: 6 tables (~8M rows total)
   - Prefix: observation_
   - Key Tables:
     * observation_observation (1.4M rows) - incidents with Location geometry
     * observation_trackersteps (5.6M rows) - tracker step logs
     * observation_observationtracker (595K rows)
     * observation_observationupdate (405K rows)

2. WEATHER STATION DATABASE (weather-station):
   - Secret Scope: wakecap-weather
   - Tables: 7 tables (~115 rows total)
   - Prefix: weather_
   - Key Tables:
     * weather_projectsettings (44 rows)
     * weather_projectthreshold (41 rows)
     * weather_indicator (13 rows)
     * weather_graph (11 rows)

JOB CONFIGURATION:
==================
Job Name: {JOB_NAME}
Job ID: {job_id}
Tasks (running in parallel):
  1. load_wakecap_app - Main TimescaleDB (81 tables)
  2. load_observation - Observation database (6 tables)
  3. load_weather - Weather station (7 tables)

Total Tables: 94 (81 + 6 + 7)

NEXT STEPS:
===========

1. RUN THE JOB:
   {db_host}/#job/{job_id}
   Click "Run now" to load all databases

2. VERIFY OBSERVATION DATA:
   SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.observation_observation;
   SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.observation_observation LIMIT 10;

3. VERIFY WEATHER DATA:
   SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.weather_indicator;
   SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.weather_projectsettings LIMIT 10;

4. CHECK WATERMARKS:
   SELECT source_table, last_load_status, last_load_row_count
   FROM {TARGET_CATALOG}.migration._timescaledb_watermarks
   ORDER BY last_load_end_time DESC;
""")


def main():
    print_header("DEPLOY NEW TIMESCALEDB DATABASE LOADERS")
    print(f"Timestamp: {datetime.now()}")
    print(f"Databases: {', '.join(DATABASES.keys())}")

    creds = load_credentials()

    try:
        w = WorkspaceClient(
            host=creds['databricks']['host'],
            token=creds['databricks']['token']
        )
        print(f"Connected to Databricks as: {w.current_user.me().user_name}")
    except Exception as e:
        print_error(f"Could not connect to Databricks: {e}")
        return 1

    # Step 1: Configure secrets for each database
    print_header("STEP 1: Configuring Database Secrets")
    for db_key, db_config in DATABASES.items():
        if not configure_secrets(w, creds, db_key, db_config):
            print_error(f"Failed to configure secrets for {db_key}")
            return 1

    # Step 2: Upload files for each database
    print_header("STEP 2: Uploading Files")
    for db_key, db_config in DATABASES.items():
        upload_files(w, db_key, db_config)

    # Step 3: Create/update job
    job_id = create_job_with_all_tasks(w)

    if job_id:
        # Save job ID
        run_file = Path(__file__).parent / "current_run.txt"
        with open(run_file, 'w') as f:
            f.write(f"JOB_ID={job_id}\n")
            f.write(f"# Updated: {datetime.now()}\n")
            f.write(f"# Databases: observation, weather\n")

        # Show summary
        show_summary(creds, job_id)

        # Ask if user wants to run the job
        print("\nWould you like to run the job now? (y/n): ", end='')
        try:
            response = input().strip().lower()
            if response == 'y':
                print("\nStarting job...")
                run = w.jobs.run_now(job_id=job_id)
                print_ok(f"Job started: Run ID = {run.run_id}")

                with open(run_file, 'a') as f:
                    f.write(f"RUN_ID={run.run_id}\n")

                print(f"\nMonitor at: {creds['databricks']['host']}/#job/{job_id}/run/{run.run_id}")
        except EOFError:
            print("\nSkipping job execution (non-interactive mode)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
