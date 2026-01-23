#!/usr/bin/env python3
"""Download notebook from Databricks to check content."""
import sys
import base64
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    notebook_path = "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation"

    print(f"Downloading notebook: {notebook_path}")
    try:
        export = w.workspace.export(notebook_path, format=ExportFormat.SOURCE)
        content = base64.b64decode(export.content).decode('utf-8')
        print("\n=== NOTEBOOK CONTENT (first 3000 chars) ===")
        print(content[:3000])

        # Check for %run or import
        if "%run" in content:
            print("\n✓ Found %run in notebook")
        if "import" in content and "timescaledb_loader" in content:
            print("\n✗ Found import timescaledb_loader (should be %run)")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
