"""
Deploy observation-related notebooks and create dimension tables.

1. Deploys create_observation_dimensions notebook
2. Deploys updated delta_sync_fact_observations notebook
3. Runs the dimension creation notebook
"""

import os
import sys
import base64
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import Task, NotebookTask, Source


def load_credentials():
    """Load Databricks credentials from credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            creds = yaml.safe_load(f)
            return creds.get("databricks", {})
    return {}


def ensure_workspace_dir(w, path):
    """Ensure a workspace directory exists."""
    try:
        w.workspace.mkdirs(path)
        print(f"  [OK] Directory: {path}")
    except Exception as e:
        if "already exists" not in str(e).lower():
            print(f"  [WARN] {path}: {e}")


def upload_notebook(w, local_path, workspace_path):
    """Upload a notebook to workspace."""
    with open(local_path, "r", encoding="utf-8") as f:
        content = f.read()

    if not content.startswith("# Databricks notebook source"):
        content = "# Databricks notebook source\n" + content

    w.workspace.import_(
        path=workspace_path,
        content=base64.b64encode(content.encode()).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print(f"  [OK] Uploaded: {workspace_path}")


def main():
    print("=" * 70)
    print("Deploy Observation Dependencies")
    print("=" * 70)

    # Load credentials
    creds = load_credentials()
    host = creds.get("host", os.environ.get("DATABRICKS_HOST"))
    token = creds.get("token", os.environ.get("DATABRICKS_TOKEN"))

    if not host or not token:
        print("Error: Databricks credentials not found")
        sys.exit(1)

    # Initialize client
    w = WorkspaceClient(host=host, token=token)
    print(f"\nConnected to: {host}")

    # Paths
    LOCAL_BASE = Path(__file__).parent / "pipelines" / "gold" / "notebooks"
    WORKSPACE_BASE = "/Workspace/migration_project/pipelines/gold/notebooks"

    # Ensure directories
    print("\nCreating directories...")
    ensure_workspace_dir(w, "/Workspace/migration_project")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines/gold")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines/gold/notebooks")

    # Upload notebooks
    print("\nUploading notebooks...")

    notebooks = [
        ("create_observation_dimensions.py", "create_observation_dimensions"),
        ("delta_sync_fact_observations.py", "delta_sync_fact_observations"),
    ]

    for local_file, workspace_name in notebooks:
        local_path = LOCAL_BASE / local_file
        workspace_path = f"{WORKSPACE_BASE}/{workspace_name}"

        if local_path.exists():
            try:
                upload_notebook(w, local_path, workspace_path)
            except Exception as e:
                print(f"  [ERROR] {local_file}: {e}")
        else:
            print(f"  [SKIP] Not found: {local_file}")

    print("\n" + "=" * 70)
    print("Deployment complete!")
    print("=" * 70)

    print(f"""
Next Steps:
===========

1. Run the dimension creation notebook FIRST:
   {WORKSPACE_BASE}/create_observation_dimensions

   This will create the required dimension tables from SQL Server.
   NOTE: You need to set up Databricks secrets first:
   - Create secret scope: lakebridge
   - Add secrets: mssql-user, mssql-password

   Or temporarily hardcode credentials in the notebook for testing.

2. Then run the fact observations notebook:
   {WORKSPACE_BASE}/delta_sync_fact_observations

Workspace URL:
{host}

Alternative: Run the dimension notebook with hardcoded creds:
  - Edit the notebook in Databricks
  - Uncomment the hardcoded credentials section
  - Run the notebook
  - Remove hardcoded credentials after
""")


if __name__ == "__main__":
    main()
