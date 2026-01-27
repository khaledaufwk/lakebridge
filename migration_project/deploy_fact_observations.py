"""
Deploy Gold Layer - Fact Observations Notebook to Databricks Workspace

Uploads the delta_sync_fact_observations notebook converted from stg.spDeltaSyncFactObservations.
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
    print("Deploy Gold Layer - Fact Observations Notebook")
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

    # Configuration
    WORKSPACE_BASE = "/Workspace/migration_project/pipelines/gold"
    LOCAL_PATH = Path(__file__).parent / "pipelines" / "gold"

    # Create directory structure
    print("\nCreating workspace directories...")
    ensure_workspace_dir(w, "/Workspace/migration_project")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines/gold")
    ensure_workspace_dir(w, "/Workspace/migration_project/pipelines/gold/notebooks")

    # Notebook to upload
    local_file = "notebooks/delta_sync_fact_observations.py"
    workspace_name = "notebooks/delta_sync_fact_observations"

    local_path = LOCAL_PATH / local_file
    workspace_file = f"{WORKSPACE_BASE}/{workspace_name}"

    print(f"\nUploading notebook...")
    print(f"  Local: {local_path}")
    print(f"  Workspace: {workspace_file}")

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
            print(f"  [ERROR] Failed to upload: {e}")
            sys.exit(1)
    else:
        print(f"  [ERROR] Not found locally: {local_path}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Deployment complete!")
    print("=" * 60)

    print(f"""
Notebook deployed to:
  {workspace_file}

To run the notebook:
  1. Open Databricks workspace: {host}
  2. Navigate to: {workspace_file}
  3. Attach to a cluster and run

Widget Parameters:
  - load_mode: 'incremental' or 'full'
  - lookback_days: Number of days for incremental (default: 7)
  - project_id: Optional project filter
  - batch_size: Limit records (0 = unlimited)

Required Dependencies:
  - Source: wakecap_prod.raw.timescale_observations
  - 12 Silver dimension tables
  - Target: wakecap_prod.gold.fact_observations (will be created)
""")


if __name__ == "__main__":
    main()
