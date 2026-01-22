#!/usr/bin/env python3
"""
Upload and run the bronze_loader_all notebook that loads ALL tables
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import Task, NotebookTask
from databricks.sdk.service.compute import ClusterSpec

WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/timescaledb"
JOB_NAME = "WakeCapDW_Bronze_TimescaleDB_AllTables"


def load_credentials():
    """Load credentials from template file."""
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    creds = load_credentials()
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    print("=" * 60)
    print("Upload and Run Bronze Loader (All Tables)")
    print("=" * 60)

    # Step 1: Upload the new notebook
    print("\n[Step 1] Uploading bronze_loader_all notebook...")

    notebook_path = Path(__file__).parent / "pipelines" / "timescaledb" / "notebooks" / "bronze_loader_all.py"

    with open(notebook_path, 'rb') as f:
        content = f.read()

    workspace_path = f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_all"

    w.workspace.upload(
        path=workspace_path,
        content=content,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print(f"   [OK] Uploaded to: {workspace_path}")

    # Step 2: Create or update the job
    print("\n[Step 2] Creating job...")

    # Delete existing job if exists
    existing_jobs = list(w.jobs.list(name=JOB_NAME))
    for job in existing_jobs:
        if job.settings and job.settings.name == JOB_NAME:
            print(f"   Deleting existing job: {job.job_id}")
            w.jobs.delete(job.job_id)

    # Create new job
    job = w.jobs.create(
        name=JOB_NAME,
        tasks=[
            Task(
                task_key="load_all_tables",
                description="Load all 81 tables from TimescaleDB",
                notebook_task=NotebookTask(
                    notebook_path=f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_all",
                    base_parameters={
                        "load_mode": "incremental",
                        "batch_size": "100000",
                        "max_tables": "0"  # 0 = all tables
                    }
                ),
                new_cluster=ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    num_workers=2,  # Smaller cluster to fit within quota
                    node_type_id="Standard_DS3_v2",  # Smaller nodes
                    spark_conf={
                        "spark.databricks.delta.schema.autoMerge.enabled": "true"
                    }
                ),
                timeout_seconds=28800,  # 8 hours
            ),
        ],
        max_concurrent_runs=1,
        timeout_seconds=28800,
    )

    print(f"   [OK] Job created: {job.job_id}")

    # Step 3: Run the job
    print("\n[Step 3] Starting job...")

    run = w.jobs.run_now(job_id=job.job_id)
    print(f"   [OK] Run started: {run.run_id}")

    print("\n" + "=" * 60)
    print("JOB STARTED!")
    print("=" * 60)
    print(f"""
Job ID: {job.job_id}
Run ID: {run.run_id}

Monitor at:
{creds['databricks']['host']}/#job/{job.job_id}/run/{run.run_id}

Update the monitor script:
python -c "
# Update these in monitor_bronze_job.py:
JOB_ID = {job.job_id}
RUN_ID = {run.run_id}
"
""")

    return job.job_id, run.run_id


if __name__ == "__main__":
    job_id, run_id = main()
