#!/usr/bin/env python3
"""Verify deployed notebook content."""
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

    print(f"Downloading: {notebook_path}")
    export = w.workspace.export(notebook_path, format=ExportFormat.SOURCE)
    content = base64.b64decode(export.content).decode('utf-8')

    # Check for key patterns
    print("\n=== CHECKING KEY PATTERNS ===")
    checks = [
        ("dbutils=dbutils", "Explicit dbutils parameter"),
        ("%run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2", "%run command"),
        ("from_databricks_secrets", "Credentials method"),
        ("devicelocation_config", "DeviceLocation config"),
    ]

    for pattern, desc in checks:
        if pattern in content:
            print(f"  [OK] Found: {desc}")
        else:
            print(f"  [MISSING] Not found: {desc}")

    # Print first 2000 chars
    print("\n=== FIRST 2000 CHARS ===")
    print(content[:2000])

if __name__ == "__main__":
    main()
