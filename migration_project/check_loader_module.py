#!/usr/bin/env python3
"""Check the loader module in Databricks."""
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

    loader_path = "/Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2"

    print(f"Checking loader module: {loader_path}")
    try:
        export = w.workspace.export(loader_path, format=ExportFormat.SOURCE)
        content = base64.b64decode(export.content).decode('utf-8')

        # Check first 1500 chars
        print("\n=== LOADER MODULE (first 1500 chars) ===")
        print(content[:1500])

        # Check for key classes
        classes_to_check = ["TimescaleDBCredentials", "TimescaleDBLoaderV2", "TableConfigV2", "WatermarkType", "LoadStatus"]
        print("\n=== CLASS CHECK ===")
        for cls in classes_to_check:
            if f"class {cls}" in content:
                print(f"  Found: class {cls}")
            else:
                print(f"  MISSING: class {cls}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
