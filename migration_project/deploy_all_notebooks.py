#!/usr/bin/env python3
"""
Deploy and run all converted notebooks to Databricks.

Deploys notebooks in optimized order:
1. Silver dimensions (no dependencies, run in parallel)
2. Gold facts (depend on silver, run in parallel after silver completes)

Uses Databricks Jobs API with task dependencies for optimal execution.
"""

import sys
from pathlib import Path

# Add skills path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / ".claude" / "skills" / "shared" / "scripts"))

from credentials import CredentialsManager
from databricks_client import DatabricksClient
import base64
import time


# Configuration
WORKSPACE_BASE = "/Workspace/Shared/wakecap_migration"
CATALOG = "wakecap_prod"

# Notebooks to deploy - organized by layer and execution order
SILVER_NOTEBOOKS = [
    {
        "name": "WakeCapDW_Silver_TimescaleDB",
        "local_path": "migration_project/pipelines/silver/notebooks/WakeCapDW_Silver_TimescaleDB.py",
        "workspace_path": f"{WORKSPACE_BASE}/silver/WakeCapDW_Silver_TimescaleDB",
    },
]

GOLD_NOTEBOOKS = [
    {
        "name": "gold_fact_workers_history",
        "local_path": "migration_project/pipelines/gold/notebooks/gold_fact_workers_history.py",
        "workspace_path": f"{WORKSPACE_BASE}/gold/gold_fact_workers_history",
        "depends_on": ["WakeCapDW_Silver_TimescaleDB"],
    },
    {
        "name": "gold_fact_weather_observations",
        "local_path": "migration_project/pipelines/gold/notebooks/gold_fact_weather_observations.py",
        "workspace_path": f"{WORKSPACE_BASE}/gold/gold_fact_weather_observations",
        "depends_on": ["WakeCapDW_Silver_TimescaleDB"],
    },
    {
        "name": "gold_fact_reported_attendance",
        "local_path": "migration_project/pipelines/gold/notebooks/gold_fact_reported_attendance.py",
        "workspace_path": f"{WORKSPACE_BASE}/gold/gold_fact_reported_attendance",
        "depends_on": ["WakeCapDW_Silver_TimescaleDB"],
    },
    {
        "name": "gold_fact_progress",
        "local_path": "migration_project/pipelines/gold/notebooks/gold_fact_progress.py",
        "workspace_path": f"{WORKSPACE_BASE}/gold/gold_fact_progress",
        "depends_on": ["WakeCapDW_Silver_TimescaleDB"],
    },
    {
        "name": "gold_worker_location_assignments",
        "local_path": "migration_project/pipelines/gold/notebooks/gold_worker_location_assignments.py",
        "workspace_path": f"{WORKSPACE_BASE}/gold/gold_worker_location_assignments",
        "depends_on": ["WakeCapDW_Silver_TimescaleDB"],
    },
    {
        "name": "gold_manager_assignment_snapshots",
        "local_path": "migration_project/pipelines/gold/notebooks/gold_manager_assignment_snapshots.py",
        "workspace_path": f"{WORKSPACE_BASE}/gold/gold_manager_assignment_snapshots",
        "depends_on": ["WakeCapDW_Silver_TimescaleDB"],
    },
]


def load_credentials():
    """Load Databricks credentials."""
    creds = CredentialsManager()
    creds.load()
    return creds


def create_workspace_folders(client):
    """Create the workspace folder structure."""
    folders = [
        WORKSPACE_BASE,
        f"{WORKSPACE_BASE}/silver",
        f"{WORKSPACE_BASE}/gold",
    ]

    for folder in folders:
        try:
            client.client.workspace.mkdirs(folder)
            print(f"  [OK] Created folder: {folder}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  [OK] Folder exists: {folder}")
            else:
                print(f"  [WARN] Could not create {folder}: {e}")


def upload_notebook(client, local_path: str, workspace_path: str):
    """Upload a single notebook to Databricks."""
    base_path = Path(__file__).parent.parent
    full_path = base_path / local_path

    if not full_path.exists():
        print(f"  [SKIP] File not found: {full_path}")
        return False

    content = full_path.read_text(encoding="utf-8")

    # Ensure notebook format
    if not content.startswith("# Databricks notebook source"):
        content = "# Databricks notebook source\n" + content

    try:
        client.upload_notebook(content, workspace_path)
        print(f"  [OK] Uploaded: {workspace_path}")
        return True
    except Exception as e:
        print(f"  [ERROR] Failed to upload {workspace_path}: {e}")
        return False


def deploy_all_notebooks(client):
    """Deploy all notebooks to Databricks workspace."""
    print("\n" + "=" * 60)
    print("DEPLOYING NOTEBOOKS TO DATABRICKS")
    print("=" * 60)

    deployed = {"silver": [], "gold": []}

    # Deploy Silver notebooks
    print("\n[Silver Layer Notebooks]")
    for nb in SILVER_NOTEBOOKS:
        if upload_notebook(client, nb["local_path"], nb["workspace_path"]):
            deployed["silver"].append(nb)

    # Deploy Gold notebooks
    print("\n[Gold Layer Notebooks]")
    for nb in GOLD_NOTEBOOKS:
        if upload_notebook(client, nb["local_path"], nb["workspace_path"]):
            deployed["gold"].append(nb)

    print(f"\nDeployed: {len(deployed['silver'])} silver, {len(deployed['gold'])} gold notebooks")
    return deployed


def create_workflow_job(client, deployed, cluster_id=None):
    """
    Create a Databricks workflow job with optimized task dependencies.

    Task Graph:

    WakeCapDW_Silver_TimescaleDB ─┬─> gold_fact_weather_observations
    (consolidated silver layer)  ─┼─> gold_fact_workers_history
                                  ├─> gold_fact_reported_attendance
                                  ├─> gold_fact_progress
                                  ├─> gold_worker_location_assignments
                                  └─> gold_manager_assignment_snapshots

    Silver notebook runs first, then all gold notebooks run in parallel after silver completes.
    """
    print("\n" + "=" * 60)
    print("CREATING WORKFLOW JOB")
    print("=" * 60)

    from databricks.sdk.service.jobs import (
        Task,
        NotebookTask,
        TaskDependency,
        JobCluster,
        ClusterSpec,
        JobSettings,
    )

    tasks = []

    # Silver tasks (no dependencies, run in parallel)
    for nb in deployed["silver"]:
        task = Task(
            task_key=nb["name"],
            notebook_task=NotebookTask(
                notebook_path=nb["workspace_path"],
                base_parameters={"load_mode": "incremental"}
            ),
            timeout_seconds=3600,  # 1 hour
        )

        # Use existing cluster if provided, otherwise use serverless
        if cluster_id:
            task.existing_cluster_id = cluster_id

        tasks.append(task)
        print(f"  [TASK] {nb['name']} (no dependencies)")

    # Gold tasks (depend on silver)
    for nb in deployed["gold"]:
        dependencies = []
        if nb.get("depends_on"):
            for dep in nb["depends_on"]:
                # Only add dependency if it was actually deployed
                if any(s["name"] == dep for s in deployed["silver"]):
                    dependencies.append(TaskDependency(task_key=dep))

        task = Task(
            task_key=nb["name"],
            notebook_task=NotebookTask(
                notebook_path=nb["workspace_path"],
                base_parameters={"load_mode": "incremental"}
            ),
            depends_on=dependencies if dependencies else None,
            timeout_seconds=7200,  # 2 hours for gold
        )

        if cluster_id:
            task.existing_cluster_id = cluster_id

        tasks.append(task)
        deps_str = ", ".join([d.task_key for d in dependencies]) if dependencies else "none"
        print(f"  [TASK] {nb['name']} (depends on: {deps_str})")

    # Create the job
    job_name = f"SP_Migration_All_Layers_{int(time.time())}"

    try:
        result = client.client.jobs.create(
            name=job_name,
            tasks=tasks,
            max_concurrent_runs=1,
        )

        job_id = result.job_id
        print(f"\n[OK] Created job: {job_name}")
        print(f"  Job ID: {job_id}")
        print(f"  Tasks: {len(tasks)}")

        return job_id
    except Exception as e:
        print(f"\n[ERROR] Failed to create job: {e}")
        return None


def run_job(client, job_id):
    """Run the workflow job and monitor progress."""
    print("\n" + "=" * 60)
    print("RUNNING WORKFLOW JOB")
    print("=" * 60)

    try:
        # Start the job
        run = client.client.jobs.run_now(job_id=job_id)
        run_id = run.run_id

        print(f"  Run ID: {run_id}")
        print(f"  Job URL: {client.host}/#job/{job_id}/run/{run_id}")
        print("\n  Monitoring progress...")

        # Monitor until complete
        while True:
            run_status = client.client.jobs.get_run(run_id=run_id)
            state = run_status.state.life_cycle_state.value if run_status.state else "UNKNOWN"
            result_state = run_status.state.result_state.value if run_status.state and run_status.state.result_state else ""

            print(f"  Status: {state} {result_state}")

            if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                break

            time.sleep(30)

        # Get final status
        if result_state == "SUCCESS":
            print("\n[SUCCESS] All notebooks completed successfully!")
            return True
        else:
            print(f"\n[FAILED] Job ended with: {result_state}")
            # Print task failures
            for task in run_status.tasks or []:
                if task.state and task.state.result_state:
                    task_result = task.state.result_state.value
                    if task_result != "SUCCESS":
                        print(f"  - {task.task_key}: {task_result}")
            return False

    except Exception as e:
        print(f"\n[ERROR] Failed to run job: {e}")
        return False


def find_cluster(client, cluster_name=None):
    """Find an existing cluster to use."""
    if cluster_name:
        cluster = client.get_cluster_by_name(cluster_name)
        if cluster:
            print(f"  Found cluster: {cluster['cluster_name']} ({cluster['state']})")
            return cluster['cluster_id']

    # Try to find any running cluster
    clusters = client.client.clusters.list()
    for c in clusters:
        if c.state and c.state.value == "RUNNING":
            print(f"  Using running cluster: {c.cluster_name}")
            return c.cluster_id

    return None


def main():
    """Main entry point."""
    print("=" * 60)
    print("WAKECAP SP MIGRATION - DEPLOY ALL NOTEBOOKS")
    print("=" * 60)

    # Load credentials
    print("\n[1] Loading credentials...")
    try:
        creds = load_credentials()
        print(f"  Databricks host: {creds.databricks.host}")
        print(f"  Catalog: {creds.databricks.catalog}")
    except Exception as e:
        print(f"  [ERROR] Failed to load credentials: {e}")
        return 1

    # Initialize client
    print("\n[2] Connecting to Databricks...")
    client = DatabricksClient(
        host=creds.databricks.host,
        token=creds.databricks.token
    )

    # Find cluster
    print("\n[3] Finding compute cluster...")
    cluster_id = None
    if creds.compute and creds.compute.cluster_name:
        cluster_id = find_cluster(client, creds.compute.cluster_name)
    else:
        cluster_id = find_cluster(client)

    if not cluster_id:
        print("  [WARN] No running cluster found - job will use serverless compute")

    # Create workspace folders
    print("\n[4] Creating workspace folders...")
    create_workspace_folders(client)

    # Deploy notebooks
    print("\n[5] Deploying notebooks...")
    deployed = deploy_all_notebooks(client)

    if not deployed["silver"] and not deployed["gold"]:
        print("\n[ERROR] No notebooks were deployed!")
        return 1

    # Create workflow job
    print("\n[6] Creating workflow job...")
    job_id = create_workflow_job(client, deployed, cluster_id)

    if not job_id:
        return 1

    # Run the job
    print("\n[7] Running workflow...")
    success = run_job(client, job_id)

    print("\n" + "=" * 60)
    if success:
        print("DEPLOYMENT AND EXECUTION COMPLETE")
    else:
        print("DEPLOYMENT COMPLETE - EXECUTION HAD ERRORS")
        print(f"Check job at: {client.host}/#job/{job_id}")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
