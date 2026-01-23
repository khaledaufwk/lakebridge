#!/usr/bin/env python3
"""
Deploy Observation Database Bronze Pipeline
=============================================
This script:
1. Creates the 'wakecap-observation' secret scope with TimescaleDB credentials
2. Uploads the observation loader notebook and configuration
3. Updates the WakeCapDW_Bronze_TimescaleDB_Raw job to include observation loading
4. Optionally runs the job

Usage:
    python deploy_observation_pipeline.py
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
OBSERVATION_SECRET_SCOPE = "wakecap-observation"
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "raw"


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


def configure_observation_secrets(w, creds):
    """Create secret scope and store observation database credentials."""
    print_header("STEP 1: Configuring Observation Database Secrets")

    obs_config = creds.get('timescaledb_observation')
    if not obs_config:
        print_error("timescaledb_observation not found in credentials file!")
        print("   Make sure credentials_template.yml has a 'timescaledb_observation' section")
        return False

    scope_name = OBSERVATION_SECRET_SCOPE

    # Create secret scope
    print(f"\n   Creating secret scope '{scope_name}'...")
    try:
        w.secrets.create_scope(scope=scope_name)
        print_ok(f"Secret scope '{scope_name}' created")
    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print_ok(f"Secret scope '{scope_name}' already exists")
        else:
            print_warn(f"Could not create scope: {e}")

    # Store secrets
    print("\n   Storing secrets...")
    secrets_to_store = [
        ("timescaledb-host", obs_config['host']),
        ("timescaledb-port", str(obs_config['port'])),
        ("timescaledb-database", obs_config['database']),
        ("timescaledb-user", obs_config['user']),
        ("timescaledb-password", obs_config['password']),
    ]

    for key, value in secrets_to_store:
        try:
            w.secrets.put_secret(scope=scope_name, key=key, string_value=value)
            print_ok(f"Stored: {key}")
        except Exception as e:
            print_error(f"Failed to store {key}: {e}")
            return False

    # Verify secrets
    print("\n   Verifying secrets...")
    try:
        secret_list = list(w.secrets.list_secrets(scope=scope_name))
        print(f"   Secrets in scope '{scope_name}': {len(secret_list)}")
        for secret in secret_list:
            print(f"     - {secret.key}")
    except Exception as e:
        print_error(f"Could not list secrets: {e}")

    return True


def upload_observation_files(w):
    """Upload the observation loader notebook and configuration files."""
    print_header("STEP 2: Uploading Observation Files")

    base_path = Path(__file__).parent / "pipelines" / "timescaledb"

    # Files to upload
    files = [
        # Observation config
        ("config/timescaledb_tables_observation.yml", f"{WORKSPACE_BASE_PATH}/config/timescaledb_tables_observation.yml", None),
        # Observation notebook
        ("notebooks/bronze_loader_observation.py", f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_observation", Language.PYTHON),
        # Also ensure the source module is updated
        ("src/timescaledb_loader_v2.py", f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader_v2.py", Language.PYTHON),
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


def update_job_with_observation(w):
    """Update the existing job to add observation loading task, or create a new job."""
    print_header("STEP 3: Updating Databricks Job")

    # Check if job exists
    existing_jobs = list(w.jobs.list())
    existing_job_id = None

    for job in existing_jobs:
        if job.settings and job.settings.name == JOB_NAME:
            existing_job_id = job.job_id
            print(f"   Found existing job: {job.job_id}")
            break

    # Define the observation task
    observation_task = Task(
        task_key="load_observation_tables",
        description="Load observation tables from TimescaleDB wakecap_observation database",
        notebook_task=NotebookTask(
            notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_observation",
            base_parameters={
                "load_mode": "incremental",
                "category": "ALL",
                "fetch_size": "50000",
                "batch_size": "100000"
            }
        ),
        new_cluster=ClusterSpec(
            spark_version="14.3.x-scala2.12",
            num_workers=2,
            node_type_id="Standard_DS3_v2",
            spark_conf={
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
            }
        ),
        timeout_seconds=7200,  # 2 hours
    )

    # Define the main timescale task (for the combined job)
    main_timescale_task = Task(
        task_key="load_all_tables",
        description="Load all tables from TimescaleDB wakecap_app database",
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
    )

    try:
        if existing_job_id:
            # Delete and recreate with both tasks
            print("   Recreating job with observation task added...")
            w.jobs.delete(existing_job_id)
            print_ok(f"Deleted old job: {existing_job_id}")

        # Create new job with both tasks running in parallel
        job = w.jobs.create(
            name=JOB_NAME,
            tasks=[
                main_timescale_task,
                observation_task,  # Runs in parallel since no depends_on
            ],
            max_concurrent_runs=1,
            timeout_seconds=18000,  # 5 hours total
        )
        print_ok(f"Created job with observation task: {job.job_id}")
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
    obs_config = creds.get('timescaledb_observation', {})

    print(f"""
OBSERVATION DATABASE INTEGRATION:
==================================

1. SECRET SCOPE CONFIGURED:
   - Scope: {OBSERVATION_SECRET_SCOPE}
   - Host: {obs_config.get('host', 'N/A')}
   - Database: {obs_config.get('database', 'N/A')}

2. FILES DEPLOYED:
   - bronze_loader_observation.py (notebook)
   - timescaledb_tables_observation.yml (table registry)

3. TABLES TO BE LOADED (6 tables):
   - observation_observation (~1.4M rows) - with Location geometry
   - observation_trackersteps (~5.6M rows) - largest table
   - observation_observationtracker (~595K rows)
   - observation_observationupdate (~405K rows)
   - observation_observationupdateattachment (~3.2K rows)
   - observation_settings (~13 rows)

4. JOB UPDATED:
   Job ID: {job_id}
   Tasks:
     - load_all_tables (wakecap_app) - runs in parallel
     - load_observation_tables (wakecap_observation) - runs in parallel

NEXT STEPS:
===========

1. TEST THE OBSERVATION LOADER:
   Go to: {db_host}/#job/{job_id}
   Click "Run now" or run specific task

2. VERIFY DATA:
   SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.observation_observation LIMIT 10;
   SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.observation_observation;

3. CHECK WATERMARKS:
   SELECT * FROM {TARGET_CATALOG}.migration._timescaledb_watermarks
   WHERE source_table LIKE 'Observation%'
   ORDER BY last_load_end_time DESC;

NOTE: Weather-station database requires additional SELECT permissions.
      Contact DBA to grant access to tsdbdatateam user.
""")


def main():
    print_header("OBSERVATION DATABASE BRONZE PIPELINE DEPLOYMENT")
    print(f"Timestamp: {datetime.now()}")

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

    # Step 1: Configure secrets for observation database
    if not configure_observation_secrets(w, creds):
        print_error("Failed to configure secrets, aborting deployment")
        return 1

    # Step 2: Upload observation files
    upload_observation_files(w)

    # Step 3: Update job with observation task
    job_id = update_job_with_observation(w)

    if job_id:
        # Save job ID
        run_file = Path(__file__).parent / "current_run.txt"
        with open(run_file, 'w') as f:
            f.write(f"JOB_ID={job_id}\n")
            f.write(f"# Updated: {datetime.now()}\n")
            f.write(f"# Observation loader deployed\n")

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

                # Update run file
                with open(run_file, 'a') as f:
                    f.write(f"RUN_ID={run.run_id}\n")

                print(f"\nMonitor at: {creds['databricks']['host']}/#job/{job_id}/run/{run.run_id}")
        except EOFError:
            print("\nSkipping job execution (non-interactive mode)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
