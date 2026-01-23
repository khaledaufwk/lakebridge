#!/usr/bin/env python3
"""
Deploy the fixed TimescaleDB Loader v2 to Databricks workspace.

This script uploads the updated timescaledb_loader_v2.py which fixes the
GREATEST watermark calculation bug that was causing full table scans.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language


WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/timescaledb"


def load_credentials():
    """Load credentials from template file."""
    template_path = Path(__file__).parent / "credentials_template.yml"

    if not template_path.exists():
        print(f"[ERROR] Credentials file not found: {template_path}")
        return None

    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    print("=" * 60)
    print("Deploying TimescaleDB Loader v2 Fix")
    print("=" * 60)

    # Load credentials
    print("\n[1] Loading credentials...")
    creds = load_credentials()
    if not creds:
        return 1

    print(f"    Databricks Host: {creds['databricks']['host']}")

    # Connect to Databricks
    print("\n[2] Connecting to Databricks...")
    try:
        w = WorkspaceClient(
            host=creds['databricks']['host'],
            token=creds['databricks']['token']
        )
        user = w.current_user.me()
        print(f"    Connected as: {user.user_name}")
    except Exception as e:
        print(f"    [ERROR] Connection failed: {e}")
        return 1

    # Upload the fixed loader
    print("\n[3] Uploading fixed timescaledb_loader_v2.py...")

    local_path = Path(__file__).parent / "pipelines" / "timescaledb" / "src" / "timescaledb_loader_v2.py"
    workspace_path = f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader_v2"

    if not local_path.exists():
        print(f"    [ERROR] File not found: {local_path}")
        return 1

    try:
        with open(local_path, 'rb') as f:
            content = f.read()

        w.workspace.upload(
            path=workspace_path,
            content=content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )
        print(f"    [OK] Uploaded to: {workspace_path}")
    except Exception as e:
        print(f"    [ERROR] Upload failed: {e}")
        return 1

    # Also upload to the .py extension path (some imports may use this)
    workspace_path_py = f"{WORKSPACE_BASE_PATH}/src/timescaledb_loader_v2.py"
    try:
        w.workspace.upload(
            path=workspace_path_py,
            content=content,
            format=ImportFormat.AUTO,
            overwrite=True
        )
        print(f"    [OK] Uploaded to: {workspace_path_py}")
    except Exception as e:
        print(f"    [WARN] Secondary upload: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("DEPLOYMENT COMPLETE")
    print("=" * 60)
    print("""
FIX DEPLOYED:
  - timescaledb_loader_v2.py updated in Databricks workspace

WHAT WAS FIXED:
  1. _get_max_watermark_value() now correctly calculates MAX(GREATEST(...))
     instead of just MAX(UpdatedAt) for tables with GREATEST expressions

  2. _format_watermark_for_sql() ensures consistent PostgreSQL-compatible
     timestamp formatting

NEXT STEPS:
  1. Run the bronze job with load_mode='incremental' to verify the fix
  2. Check that row counts are now minimal (only new/changed data)
  3. Monitor watermark values in _timescaledb_watermarks table

OPTIONAL - Reset watermarks for accurate tracking:
  Run once with load_mode='full' to establish correct GREATEST-based watermarks,
  then switch back to 'incremental' for subsequent runs.
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
