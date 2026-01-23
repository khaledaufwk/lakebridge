#!/usr/bin/env python3
"""
Deploy DeviceLocation loader notebooks and update job configuration.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language


WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/timescaledb"


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    print("=" * 60)
    print("Deploying DeviceLocation Loader Job")
    print("=" * 60)

    # Load credentials
    creds = load_credentials()

    # Connect to Databricks
    print("\n[1] Connecting to Databricks...")
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    print(f"    Connected as: {w.current_user.me().user_name}")

    # Notebooks to deploy
    notebooks = [
        {
            "local_path": "pipelines/timescaledb/notebooks/reset_devicelocation_watermarks.py",
            "remote_path": f"{WORKSPACE_BASE_PATH}/notebooks/reset_devicelocation_watermarks",
            "description": "Reset watermarks for DeviceLocation tables"
        },
        {
            "local_path": "pipelines/timescaledb/notebooks/bronze_loader_devicelocation.py",
            "remote_path": f"{WORKSPACE_BASE_PATH}/notebooks/bronze_loader_devicelocation",
            "description": "Dedicated loader for DeviceLocation tables"
        },
        {
            "local_path": "pipelines/timescaledb/notebooks/optimize_devicelocation.py",
            "remote_path": f"{WORKSPACE_BASE_PATH}/notebooks/optimize_devicelocation",
            "description": "Z-ORDER optimization for DeviceLocation tables"
        },
        {
            "local_path": "pipelines/timescaledb/config/timescaledb_tables_v2.yml",
            "remote_path": f"{WORKSPACE_BASE_PATH}/config/timescaledb_tables_v2.yml",
            "description": "Updated table registry with correct PKs/watermarks",
            "is_config": True
        }
    ]

    # Deploy notebooks
    print("\n[2] Deploying notebooks...")
    for nb in notebooks:
        local_path = Path(__file__).parent / nb["local_path"]
        remote_path = nb["remote_path"]

        print(f"\n    Deploying: {nb['description']}")
        print(f"    From: {local_path}")
        print(f"    To: {remote_path}")

        with open(local_path, 'rb') as f:
            content = f.read()

        if nb.get("is_config"):
            # Config files use AUTO format
            w.workspace.upload(
                path=remote_path,
                content=content,
                format=ImportFormat.AUTO,
                overwrite=True
            )
        else:
            # Notebooks use SOURCE format with PYTHON language
            w.workspace.upload(
                path=remote_path,
                content=content,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True
            )
        print(f"    [OK] Deployed")

    # Deploy the loader module
    print("\n[3] Deploying loader module...")
    loader_path = Path(__file__).parent / "pipelines" / "timescaledb" / "src" / "timescaledb_loader_v2.py"

    with open(loader_path, 'rb') as f:
        content = f.read()

    w.workspace.upload(
        path=f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader_v2",
        content=content,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print("    [OK] Deployed timescaledb_loader_v2.py")

    print("\n" + "=" * 60)
    print("DEPLOYMENT COMPLETE")
    print("=" * 60)
    print(f"""
Deployed notebooks:
  1. {WORKSPACE_BASE_PATH}/notebooks/reset_devicelocation_watermarks
     - Resets watermarks for DeviceLocation tables
     - Run this FIRST if doing a fresh load

  2. {WORKSPACE_BASE_PATH}/notebooks/bronze_loader_devicelocation
     - Dedicated loader for DeviceLocation and DeviceLocationSummary
     - Optimized batch sizes and correct PKs/watermarks
     - Includes Z-ORDER optimization option

  3. {WORKSPACE_BASE_PATH}/notebooks/optimize_devicelocation
     - Standalone optimization notebook
     - Run after data loads for query performance

  4. {WORKSPACE_BASE_PATH}/config/timescaledb_tables_v2.yml
     - Updated registry with correct DeviceLocation config

Execution Order:
================
1. Run reset_devicelocation_watermarks (if fresh load needed)
2. Run bronze_loader_devicelocation with load_mode=full (first time) or incremental
3. Optimization runs automatically if run_optimize=yes

The existing WakeCapDW_Bronze_TimescaleDB_Raw job will also use the updated
config for these tables on its next scheduled run.
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
