#!/usr/bin/env python3
"""Check if notebooks exist in Databricks workspace."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    notebook_paths = [
        "/Workspace/migration_project/pipelines/timescaledb/notebooks/reset_devicelocation_watermarks",
        "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation",
        "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_optimized",
    ]

    print("Checking notebook existence:")
    for path in notebook_paths:
        try:
            status = w.workspace.get_status(path)
            print(f"  [OK] {path}")
            print(f"    Type: {status.object_type}")
            print(f"    Language: {status.language}")
        except Exception as e:
            print(f"  [FAIL] {path}: {e}")

    # List all notebooks in the directory
    print("\n\nAll items in notebooks directory:")
    try:
        items = w.workspace.list("/Workspace/migration_project/pipelines/timescaledb/notebooks")
        for item in items:
            print(f"  {item.path} - {item.object_type}")
    except Exception as e:
        print(f"  Error listing: {e}")


if __name__ == "__main__":
    main()
