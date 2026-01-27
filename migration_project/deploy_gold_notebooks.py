"""
Deploy Gold Layer Notebooks to Databricks Workspace

Creates necessary directories and uploads notebooks.
"""

import os
import sys
import base64
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language


def load_credentials():
    """Load Databricks credentials from credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            creds = yaml.safe_load(f)
            return creds.get("databricks", {})
    return {}


def ensure_workspace_dir(w, path):
    """Ensure a workspace directory exists by creating it if needed."""
    try:
        w.workspace.mkdirs(path)
        print(f"  [OK] Directory exists/created: {path}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  [OK] Directory exists: {path}")
        else:
            print(f"  [WARN] Could not create directory {path}: {e}")


def main():
    print("=" * 60)
    print("Deploy Gold Layer Notebooks")
    print("=" * 60)

    # Load credentials
    creds = load_credentials()
    host = creds.get("host", os.environ.get("DATABRICKS_HOST"))
    token = creds.get("token", os.environ.get("DATABRICKS_TOKEN"))

    if not host or not token:
        print("Error: Databricks credentials not found")
        sys.exit(1)

    # Initialize Databricks client
    w = WorkspaceClient(host=host, token=token)
    print(f"\nConnected to: {host}")

    # Configuration - use /Workspace prefix for Unity Catalog workspaces
    WORKSPACE_BASE = "/Workspace/migration_project/pipelines/gold"
    LOCAL_PATH = Path(__file__).parent / "pipelines" / "gold"

    # Create directory structure
    print("\nCreating workspace directories...")
    ensure_workspace_dir(w, "/Workspace/migration_project")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines/gold")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines/gold/notebooks")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines/gold/udfs")

    # Notebooks to upload
    notebooks = [
        ("notebooks/gold_fact_workers_history.py", "notebooks/gold_fact_workers_history"),
        ("notebooks/gold_fact_workers_shifts.py", "notebooks/gold_fact_workers_shifts"),
        ("udfs/time_category_udf.py", "udfs/time_category_udf"),
    ]

    print("\nUploading notebooks...")
    for local_file, workspace_name in notebooks:
        local_path = LOCAL_PATH / local_file
        workspace_file = f"{WORKSPACE_BASE}/{workspace_name}"

        if local_path.exists():
            with open(local_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Ensure it starts with Databricks notebook header
            if not content.startswith("# Databricks notebook source"):
                content = "# Databricks notebook source\n" + content

            try:
                w.workspace.import_(
                    path=workspace_file,
                    content=base64.b64encode(content.encode()).decode(),
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    overwrite=True
                )
                print(f"  [OK] Uploaded: {workspace_file}")
            except Exception as e:
                print(f"  [ERROR] Failed to upload {local_file}: {e}")
        else:
            print(f"  [SKIP] Not found locally: {local_file}")

    print("\n" + "=" * 60)
    print("Deployment complete!")
    print("=" * 60)

    # Verify the job configuration
    print("\nVerifying job configuration...")
    JOB_NAME = "WakeCapDW_Gold_FactWorkersHistory"
    jobs = list(w.jobs.list(name=JOB_NAME))

    if jobs:
        job_id = jobs[0].job_id
        job = w.jobs.get(job_id)
        print(f"\nJob: {JOB_NAME} (ID: {job_id})")

        if job.settings and job.settings.tasks:
            for task in job.settings.tasks:
                print(f"  Task: {task.task_key}")
                if task.notebook_task:
                    print(f"    Notebook: {task.notebook_task.notebook_path}")
    else:
        print(f"  Job '{JOB_NAME}' not found")


if __name__ == "__main__":
    main()
