#!/usr/bin/env python3
"""
Deploy Optimized TimescaleDB Bronze Pipeline
=============================================
This script:
1. Checks current job status and logs
2. Uploads the optimized loader and configuration
3. Creates/updates the Databricks job
4. Optionally runs the job

Usage:
    python deploy_optimized_pipeline.py
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


def check_current_job_status(w):
    """Check status of current/recent job runs."""
    print_header("STEP 1: Checking Current Job Status")

    # Read current run info
    run_file = Path(__file__).parent / "current_run.txt"
    job_id = None
    run_id = None

    if run_file.exists():
        with open(run_file) as f:
            for line in f:
                if line.startswith("JOB_ID="):
                    job_id = int(line.strip().split("=")[1])
                elif line.startswith("RUN_ID="):
                    run_id = int(line.strip().split("=")[1])

    if run_id:
        try:
            run = w.jobs.get_run(run_id)
            state = run.state.life_cycle_state if run.state else "Unknown"
            result = run.state.result_state if run.state and run.state.result_state else "In Progress"

            print(f"\n   Current Run: {run_id}")
            print(f"   State: {state}")
            print(f"   Result: {result}")

            if run.start_time:
                start = datetime.fromtimestamp(run.start_time / 1000)
                print(f"   Started: {start}")

                if run.end_time:
                    end = datetime.fromtimestamp(run.end_time / 1000)
                    duration = (end - start).total_seconds() / 60
                    print(f"   Ended: {end}")
                    print(f"   Duration: {duration:.1f} minutes")

            # Check for failures
            if result in ('FAILED', 'TIMEDOUT'):
                print("\n   [!] JOB FAILED - Analyzing errors...")

                for task in (run.tasks or []):
                    task_state = task.state.life_cycle_state if task.state else "Unknown"
                    task_result = task.state.result_state if task.state and task.state.result_state else "N/A"

                    if task_result in ('FAILED', 'TIMEDOUT'):
                        print(f"\n   Failed Task: {task.task_key}")

                        try:
                            output = w.jobs.get_run_output(task.run_id)
                            if output.error:
                                print(f"   Error: {output.error[:300]}...")
                            if output.error_trace:
                                # Extract key error from trace
                                lines = output.error_trace.split('\n')
                                error_lines = [l for l in lines if 'Error' in l or 'Exception' in l]
                                for line in error_lines[:5]:
                                    print(f"     {line[:100]}")
                        except Exception as e:
                            print(f"   Could not get error details: {e}")

        except Exception as e:
            print_warn(f"Could not get run details: {e}")
    else:
        print("   No current run found")

    return job_id, run_id


def upload_optimized_files(w):
    """Upload the optimized loader and configuration files."""
    print_header("STEP 2: Uploading Optimized Files")

    base_path = Path(__file__).parent / "pipelines" / "timescaledb"

    # Files to upload
    files = [
        # Optimized source
        ("src/timescaledb_loader_v2.py", f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader_v2.py", Language.PYTHON),
        # Optimized config
        ("config/timescaledb_tables_v2.yml", f"{WORKSPACE_BASE_PATH}/config/timescaledb_tables_v2.yml", None),
        # Optimized notebook
        ("notebooks/bronze_loader_optimized.py", f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_optimized", Language.PYTHON),
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


def create_optimized_job(w):
    """Create or update the optimized job."""
    print_header("STEP 3: Creating Optimized Job")

    # Check if job exists
    existing_jobs = list(w.jobs.list())
    existing_job_id = None

    for job in existing_jobs:
        if job.settings and job.settings.name == JOB_NAME:
            existing_job_id = job.job_id
            print(f"   Found existing job: {job.job_id}")
            break

    try:
        if existing_job_id:
            # Delete and recreate (simpler than update with SDK changes)
            print("   Deleting existing job to recreate with new settings...")
            w.jobs.delete(existing_job_id)
            print_ok(f"Deleted old job: {existing_job_id}")

        # Create new job
        job = w.jobs.create(
            name=JOB_NAME,
            tasks=[
                Task(
                    task_key="load_all_tables",
                    description="Load all tables from TimescaleDB using optimized loader v2",
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
            ],
            max_concurrent_runs=1,
            timeout_seconds=18000,  # 5 hours total
        )
        print_ok(f"Created new job: {job.job_id}")
        return job.job_id

    except Exception as e:
        print_error(f"Failed to create/update job: {e}")
        import traceback
        traceback.print_exc()
        return existing_job_id  # Return existing job ID if available


def show_summary(creds, job_id):
    """Show deployment summary."""
    print_header("DEPLOYMENT COMPLETE")

    db_host = creds['databricks']['host']

    print(f"""
OPTIMIZATIONS APPLIED:
======================

1. WATERMARK IMPROVEMENTS:
   - Tables now use GREATEST(CreatedAt, UpdatedAt, DeletedAt) for change tracking
   - This captures ALL changes including soft deletes and updates
   - Prevents missing changes that only update UpdatedAt

2. EXCLUDED TABLES:
   - AuditTrail (complex JSON audit data)
   - MobileSyncRejectedActions (debug/sync data)
   - SGSAttendanceLogDebug (debug logging)
   - SGSIntegrationLog (integration logging)
   - BulkUploadBatch (binary file data)

3. GEOMETRY HANDLING:
   - Tables with geometry columns use ST_AsText for conversion
   - Blueprint, Space, Zone, ZoneHistory, etc.

4. PERFORMANCE OPTIMIZATIONS:
   - Increased fetch_size to 50,000 (from 10,000)
   - Retry logic with 3 attempts and 10s delay
   - Better error handling and logging

FILES DEPLOYED:
===============
   - timescaledb_loader_v2.py (enhanced loader)
   - timescaledb_tables_v2.yml (optimized config)
   - bronze_loader_optimized.py (new notebook)

NEXT STEPS:
===========

1. TEST THE JOB:
   Go to: {db_host}/#job/{job_id}
   Click "Run now" with parameters:
   - load_mode: incremental
   - category: dimensions (start small)

2. MONITOR PROGRESS:
   Check watermark table:
   SELECT * FROM {TARGET_CATALOG}.migration._timescaledb_watermarks
   ORDER BY last_load_end_time DESC

3. RUN FULL LOAD:
   After testing dimensions, run with category=ALL

4. VERIFY DATA:
   SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.timescale_*
""")


def main():
    print_header("OPTIMIZED TIMESCALEDB BRONZE PIPELINE DEPLOYMENT")
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

    # Step 1: Check current job status
    check_current_job_status(w)

    # Step 2: Upload optimized files
    upload_optimized_files(w)

    # Step 3: Create/update job
    job_id = create_optimized_job(w)

    if job_id:
        # Save job ID
        run_file = Path(__file__).parent / "current_run.txt"
        with open(run_file, 'w') as f:
            f.write(f"JOB_ID={job_id}\n")
            f.write(f"# Updated: {datetime.now()}\n")

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
                with open(run_file, 'w') as f:
                    f.write(f"JOB_ID={job_id}\n")
                    f.write(f"RUN_ID={run.run_id}\n")

                print(f"\nMonitor at: {creds['databricks']['host']}/#job/{job_id}/run/{run.run_id}")
        except EOFError:
            print("\nSkipping job execution (non-interactive mode)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
