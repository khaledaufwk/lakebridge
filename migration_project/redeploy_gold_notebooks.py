"""
Re-deploy Gold Layer Notebooks

Uploads updated Gold layer notebooks to Databricks workspace.
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


def main():
    print("=" * 60)
    print("Re-deploy Gold Layer Notebooks")
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
    WORKSPACE_PATH = "/Workspace/migration_project/pipelines/gold"
    LOCAL_PATH = Path(__file__).parent / "pipelines" / "gold"

    # Notebooks to upload
    notebooks = [
        ("notebooks/gold_fact_workers_history.py", "notebooks/gold_fact_workers_history"),
        ("notebooks/gold_fact_workers_shifts.py", "notebooks/gold_fact_workers_shifts"),
        ("udfs/time_category_udf.py", "udfs/time_category_udf"),
    ]

    print("\nUploading notebooks...")
    for local_file, workspace_name in notebooks:
        local_path = LOCAL_PATH / local_file
        workspace_file = f"{WORKSPACE_PATH}/{workspace_name}"

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
                print(f"  [ERROR] Error uploading {local_file}: {e}")
        else:
            print(f"  - Skipping (not found): {local_file}")

    print("\n" + "=" * 60)
    print("Notebook re-deployment complete!")
    print("=" * 60)
    print("\nNote: The Gold layer requires Silver tables to exist.")
    print("If Silver tables are not deployed, deploy them first:")
    print("  python deploy_silver_layer.py")


if __name__ == "__main__":
    main()
