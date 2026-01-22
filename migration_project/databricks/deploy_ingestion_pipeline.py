"""
Deploy WakeCapDW Ingestion Pipeline to Databricks

This script uploads the incremental load notebooks and creates the migration job.
Replaces ADF-based extraction with Databricks-native JDBC ingestion.

Usage:
    python deploy_ingestion_pipeline.py --host <workspace-url> --token <pat-token>

Requirements:
    pip install databricks-sdk
"""

import argparse
import json
import os
from pathlib import Path

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Task, NotebookTask, JobCluster, ClusterSpec
    from databricks.sdk.service.workspace import ImportFormat, Language
except ImportError:
    print("Please install databricks-sdk: pip install databricks-sdk")
    exit(1)


def get_workspace_client(host: str, token: str) -> WorkspaceClient:
    """Create Databricks workspace client."""
    return WorkspaceClient(host=host, token=token)


def upload_notebook(client: WorkspaceClient, local_path: str, workspace_path: str):
    """Upload a Python notebook to Databricks workspace."""
    print(f"Uploading {local_path} -> {workspace_path}")

    with open(local_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Ensure parent directory exists
    parent_dir = os.path.dirname(workspace_path)
    try:
        client.workspace.mkdirs(parent_dir)
    except Exception:
        pass  # Directory may already exist

    # Import the notebook
    import base64
    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')

    client.workspace.import_(
        path=workspace_path,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=content_b64,
        overwrite=True
    )
    print(f"  ✓ Uploaded successfully")


def create_job(client: WorkspaceClient, job_config_path: str):
    """Create or update the migration job."""
    print(f"\nCreating job from {job_config_path}")

    with open(job_config_path, 'r') as f:
        job_config = json.load(f)

    job_name = job_config['name']

    # Check if job already exists
    existing_jobs = list(client.jobs.list(name=job_name))

    if existing_jobs:
        job_id = existing_jobs[0].job_id
        print(f"  Job '{job_name}' already exists (ID: {job_id}). Updating...")
        client.jobs.reset(job_id=job_id, new_settings=job_config)
        print(f"  ✓ Job updated successfully")
    else:
        result = client.jobs.create(**job_config)
        print(f"  ✓ Job created successfully (ID: {result.job_id})")
        return result.job_id


def main():
    parser = argparse.ArgumentParser(description='Deploy WakeCapDW Ingestion Pipeline to Databricks')
    parser.add_argument('--host', required=True, help='Databricks workspace URL (e.g., https://adb-xxx.azuredatabricks.net)')
    parser.add_argument('--token', required=True, help='Databricks personal access token')
    parser.add_argument('--workspace-base', default='/Workspace/WakeCapDW', help='Base path in Databricks workspace')
    parser.add_argument('--job-type', choices=['orchestrator', 'per_table', 'both'], default='orchestrator',
                       help='Which job configuration to deploy')
    args = parser.parse_args()

    # Get script directory
    script_dir = Path(__file__).parent

    # Create client
    client = get_workspace_client(args.host, args.token)
    print(f"Connected to Databricks workspace: {args.host}")

    # Upload notebooks
    notebooks = [
        ('notebooks/sqlserver_incremental_load.py', f'{args.workspace_base}/notebooks/sqlserver_incremental_load'),
        ('notebooks/wakecapdw_orchestrator.py', f'{args.workspace_base}/notebooks/wakecapdw_orchestrator'),
    ]

    print("\n" + "=" * 60)
    print("Uploading Notebooks")
    print("=" * 60)

    for local_name, workspace_path in notebooks:
        local_path = script_dir / local_name
        if local_path.exists():
            upload_notebook(client, str(local_path), workspace_path)
        else:
            print(f"  ⚠ Skipping {local_name} (file not found)")

    # Create job(s)
    print("\n" + "=" * 60)
    print("Creating Jobs")
    print("=" * 60)

    if args.job_type in ['orchestrator', 'both']:
        job_config_path = script_dir / 'jobs' / 'wakecapdw_migration_job.json'
        if job_config_path.exists():
            create_job(client, str(job_config_path))

    if args.job_type in ['per_table', 'both']:
        job_config_path = script_dir / 'jobs' / 'wakecapdw_per_table_job.json'
        if job_config_path.exists():
            create_job(client, str(job_config_path))

    print("\n" + "=" * 60)
    print("Deployment Complete!")
    print("=" * 60)
    print(f"\nNotebooks uploaded to: {args.workspace_base}/notebooks/")
    print("\nNext steps:")
    print("1. Configure secrets in Azure Key Vault scope 'akv-wakecap24':")
    print("   - sqlserver-wakecap-password: SQL Server password")
    print("2. Open Databricks > Workflows > Jobs")
    print("3. Find 'WakeCapDW_Migration_Incremental' job")
    print("4. Run job manually for initial load, then enable schedule")


if __name__ == '__main__':
    main()
