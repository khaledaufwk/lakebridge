#!/usr/bin/env python3
"""
Deploy updated DeviceLocation configuration to Databricks.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat


WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/timescaledb"


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    print("=" * 60)
    print("Deploying DeviceLocation Optimization")
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

    # Upload updated registry
    print("\n[2] Uploading updated table registry...")
    config_path = Path(__file__).parent / "pipelines" / "timescaledb" / "config" / "timescaledb_tables_v2.yml"

    with open(config_path, 'rb') as f:
        content = f.read()

    w.workspace.upload(
        path=f"{WORKSPACE_BASE_PATH}/config/timescaledb_tables_v2.yml",
        content=content,
        format=ImportFormat.AUTO,
        overwrite=True
    )
    print("    [OK] Uploaded timescaledb_tables_v2.yml")

    # Upload updated loader (already deployed but ensure latest)
    print("\n[3] Uploading updated loader...")
    loader_path = Path(__file__).parent / "pipelines" / "timescaledb" / "src" / "timescaledb_loader_v2.py"

    with open(loader_path, 'rb') as f:
        content = f.read()

    from databricks.sdk.service.workspace import Language
    w.workspace.upload(
        path=f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader_v2",
        content=content,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print("    [OK] Uploaded timescaledb_loader_v2.py")

    print("\n" + "=" * 60)
    print("DEPLOYMENT COMPLETE")
    print("=" * 60)
    print("""
Changes deployed:
1. DeviceLocation config:
   - Primary key: [DeviceId, ProjectId, ActiveSequance, InactiveSequance, GeneratedAt]
   - Watermark: GeneratedAt (was UpdatedAt which is NULL)
   - Geometry column: Point (was Geometry)
   - Batch size: 500,000

2. DeviceLocationSummary config:
   - Primary key: [Day, DeviceId, ProjectId]
   - Watermark: GeneratedAt
   - Geometry columns: Point, ConfidenceArea
   - Batch size: 200,000

Next: Run compression on TimescaleDB source for better performance.
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
