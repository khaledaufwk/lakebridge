#!/usr/bin/env python3
"""
Deploy and run DeviceLocation optimizations on Databricks.
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
    print("Deploying DeviceLocation Databricks Optimizations")
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

    # Upload optimization notebook
    print("\n[2] Uploading optimization notebook...")
    notebook_path = Path(__file__).parent / "pipelines" / "timescaledb" / "notebooks" / "optimize_devicelocation.py"

    with open(notebook_path, 'rb') as f:
        content = f.read()

    target_path = f"{WORKSPACE_BASE_PATH}/notebooks/optimize_devicelocation"
    w.workspace.upload(
        path=target_path,
        content=content,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print(f"    [OK] Uploaded to {target_path}")

    print("\n" + "=" * 60)
    print("DEPLOYMENT COMPLETE")
    print("=" * 60)
    print(f"""
Optimization notebook deployed to:
  {target_path}

To run the optimizations:
1. Open the notebook in Databricks workspace
2. Attach to a cluster
3. Run all cells

Or run via API:
  databricks jobs run-now --job-id <job-id>

Optimizations included:
- Z-ORDER by (ProjectId, GeneratedAt) for timescale_devicelocation
- Z-ORDER by (ProjectId, Day) for timescale_devicelocationsummary
- Auto-optimize and auto-compact enabled
- Target file size 128MB for large table
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
