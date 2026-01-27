"""
Deploy Gold Layer Pipeline to Databricks

This script:
1. Creates Gold schema and watermark table
2. Uploads notebooks to workspace
3. Creates/updates Databricks job
4. Optionally triggers initial run
"""

import os
import sys
import base64
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, TaskDependency,
    JobCluster, ClusterSpec, JobSettings,
    CronSchedule, PauseStatus
)
from databricks.sdk.service.workspace import ImportFormat, Language

# Configuration
WORKSPACE_PATH = "/Workspace/migration_project/pipelines/gold"
LOCAL_PATH = Path(__file__).parent / "pipelines" / "gold"
JOB_NAME = "WakeCapDW_Gold_FactWorkersHistory"
CATALOG = "wakecap_prod"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"


def load_credentials():
    """Load Databricks credentials from credentials file."""
    import yaml
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            creds = yaml.safe_load(f)
            return creds.get("databricks", {})
    return {}


def create_schema_and_watermarks(w: WorkspaceClient, catalog: str):
    """Create Gold schema and watermark table using SQL."""
    print("\n1. Creating Gold schema and watermark table...")

    # Create schemas
    sql_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{GOLD_SCHEMA}",
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{MIGRATION_SCHEMA}._gold_watermarks (
            table_name STRING NOT NULL,
            last_processed_at TIMESTAMP,
            last_watermark_value TIMESTAMP,
            row_count BIGINT,
            updated_at TIMESTAMP DEFAULT current_timestamp()
        ) USING DELTA
        """,
        f"""
        MERGE INTO {catalog}.{MIGRATION_SCHEMA}._gold_watermarks AS target
        USING (
            SELECT 'gold_fact_workers_history' as table_name, NULL as last_processed_at, TIMESTAMP '1900-01-01' as last_watermark_value, NULL as row_count, current_timestamp() as updated_at
            UNION ALL
            SELECT 'gold_fact_workers_shifts', NULL, TIMESTAMP '1900-01-01', NULL, current_timestamp()
        ) AS source
        ON target.table_name = source.table_name
        WHEN NOT MATCHED THEN INSERT *
        """
    ]

    # Execute using SQL warehouse
    try:
        warehouses = list(w.warehouses.list())
        if warehouses:
            warehouse_id = warehouses[0].id
            print(f"   Using SQL warehouse: {warehouses[0].name}")

            for i, sql in enumerate(sql_statements):
                print(f"   Executing SQL statement {i+1}...")
                try:
                    result = w.statement_execution.execute_statement(
                        warehouse_id=warehouse_id,
                        statement=sql,
                        wait_timeout="30s"
                    )
                    if result.status.state.value == "SUCCEEDED":
                        print(f"   Statement {i+1} succeeded")
                    else:
                        print(f"   Statement {i+1} status: {result.status.state}")
                except Exception as e:
                    print(f"   Warning: Statement {i+1} failed: {e}")
        else:
            print("   No SQL warehouse available - skipping schema creation")
            print("   Run the following SQL manually in Databricks:")
            for sql in sql_statements:
                print(f"\n   {sql.strip()}")
    except Exception as e:
        print(f"   Warning: Could not create schema: {e}")


def upload_notebooks(w: WorkspaceClient):
    """Upload notebooks to Databricks workspace."""
    print("\n2. Creating workspace directories...")

    # Create directories
    for subdir in ["notebooks", "udfs", "config"]:
        try:
            w.workspace.mkdirs(f"{WORKSPACE_PATH}/{subdir}")
            print(f"   Created: {WORKSPACE_PATH}/{subdir}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"   Already exists: {WORKSPACE_PATH}/{subdir}")
            else:
                print(f"   Warning: {e}")

    print("\n3. Uploading notebooks...")

    notebooks = [
        ("notebooks/gold_fact_workers_history.py", "notebooks/gold_fact_workers_history"),
        ("notebooks/gold_fact_workers_shifts.py", "notebooks/gold_fact_workers_shifts"),
        ("udfs/time_category_udf.py", "udfs/time_category_udf"),
    ]

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
                print(f"   Uploaded: {workspace_file}")
            except Exception as e:
                print(f"   Error uploading {local_file}: {e}")
        else:
            print(f"   Skipping (not found): {local_file}")

    # Upload config file
    config_path = LOCAL_PATH / "config" / "gold_tables.yml"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()

        try:
            w.workspace.import_(
                path=f"{WORKSPACE_PATH}/config/gold_tables.yml",
                content=base64.b64encode(content.encode()).decode(),
                format=ImportFormat.AUTO,
                overwrite=True
            )
            print(f"   Uploaded: {WORKSPACE_PATH}/config/gold_tables.yml")
        except Exception as e:
            print(f"   Warning uploading config: {e}")


def get_cluster_id(w: WorkspaceClient, cluster_name: str = None):
    """Get existing cluster ID by name or from environment."""
    # First check environment variable
    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    if cluster_id:
        return cluster_id

    # Try to find cluster by name
    if cluster_name:
        clusters = list(w.clusters.list())
        for cluster in clusters:
            if cluster.cluster_name == cluster_name:
                print(f"   Found cluster: {cluster.cluster_name} ({cluster.cluster_id})")
                return cluster.cluster_id

    # Try to find a running cluster
    clusters = list(w.clusters.list())
    for cluster in clusters:
        if cluster.state and cluster.state.value == "RUNNING":
            print(f"   Using running cluster: {cluster.cluster_name} ({cluster.cluster_id})")
            return cluster.cluster_id

    # Return first available cluster
    if clusters:
        print(f"   Using cluster: {clusters[0].cluster_name} ({clusters[0].cluster_id})")
        return clusters[0].cluster_id

    return None


def create_or_update_job(w: WorkspaceClient, cluster_id: str):
    """Create or update the Databricks job."""
    print("\n4. Creating/updating Databricks job...")

    # Define tasks
    tasks = [
        Task(
            task_key="gold_fact_workers_history",
            description="Transform Silver to Gold FactWorkersHistory",
            notebook_task=NotebookTask(
                notebook_path=f"{WORKSPACE_PATH}/notebooks/gold_fact_workers_history",
                base_parameters={
                    "load_mode": "incremental",
                    "lookback_days": "7"
                }
            ),
            existing_cluster_id=cluster_id,
            timeout_seconds=14400,  # 4 hours
            max_retries=1
        ),
        Task(
            task_key="gold_fact_workers_shifts",
            description="Calculate shift aggregates from FactWorkersHistory",
            depends_on=[TaskDependency(task_key="gold_fact_workers_history")],
            notebook_task=NotebookTask(
                notebook_path=f"{WORKSPACE_PATH}/notebooks/gold_fact_workers_shifts",
                base_parameters={
                    "load_mode": "incremental"
                }
            ),
            existing_cluster_id=cluster_id,
            timeout_seconds=7200,  # 2 hours
            max_retries=1
        )
    ]

    # Check if job exists
    existing_jobs = list(w.jobs.list(name=JOB_NAME))

    if existing_jobs:
        job_id = existing_jobs[0].job_id
        print(f"   Updating existing job: {job_id}")
        w.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(
                name=JOB_NAME,
                tasks=tasks,
                schedule=CronSchedule(
                    quartz_cron_expression="0 0 5 * * ?",  # 5:00 AM UTC
                    timezone_id="UTC",
                    pause_status=PauseStatus.PAUSED  # Start paused
                ),
                max_concurrent_runs=1
            )
        )
    else:
        print(f"   Creating new job: {JOB_NAME}")
        job = w.jobs.create(
            name=JOB_NAME,
            tasks=tasks,
            schedule=CronSchedule(
                quartz_cron_expression="0 0 5 * * ?",
                timezone_id="UTC",
                pause_status=PauseStatus.PAUSED
            ),
            max_concurrent_runs=1
        )
        job_id = job.job_id

    return job_id


def main():
    print("=" * 60)
    print("Deploy Gold Layer Pipeline")
    print("=" * 60)

    # Load credentials
    creds = load_credentials()
    host = creds.get("host", os.environ.get("DATABRICKS_HOST"))
    token = creds.get("token", os.environ.get("DATABRICKS_TOKEN"))

    if not host or not token:
        print("Error: Databricks credentials not found")
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables")
        print("Or configure ~/.databricks/labs/lakebridge/.credentials.yml")
        sys.exit(1)

    # Initialize Databricks client
    w = WorkspaceClient(host=host, token=token)
    print(f"\nConnected to: {host}")

    # Get cluster ID
    cluster_id = get_cluster_id(w, "Migrate Compute - Khaled Auf")
    if not cluster_id:
        print("Warning: No cluster found. Job will be created but may fail to run.")
        print("Set DATABRICKS_CLUSTER_ID environment variable or ensure a cluster exists.")

    # Step 1: Create schema and watermarks
    create_schema_and_watermarks(w, CATALOG)

    # Step 2-3: Upload notebooks
    upload_notebooks(w)

    # Step 4: Create/update job
    job_id = create_or_update_job(w, cluster_id)

    print(f"\n   Job ID: {job_id}")
    job_url = f"{host}/#job/{job_id}"
    print(f"   Job URL: {job_url}")

    # Step 5: Optionally trigger run
    if os.environ.get("TRIGGER_RUN", "false").lower() == "true":
        print("\n5. Triggering job run...")
        run = w.jobs.run_now(job_id=job_id)
        print(f"   Run ID: {run.run_id}")
        print(f"   Monitor at: {host}/#job/{job_id}/run/{run.run_id}")
    else:
        print("\n5. Skipping job trigger (set TRIGGER_RUN=true to run)")

    print("\n" + "=" * 60)
    print("Deployment complete!")
    print("=" * 60)
    print(f"\nJob URL: {job_url}")
    print("\nNext steps:")
    print("1. Review the job configuration in Databricks UI")
    print("2. Run the job manually for initial load:")
    print(f"   - Set load_mode=full for first run")
    print("3. After successful run, unpause the schedule")
    print("\nFiles deployed:")
    print(f"  - {WORKSPACE_PATH}/notebooks/gold_fact_workers_history")
    print(f"  - {WORKSPACE_PATH}/notebooks/gold_fact_workers_shifts")
    print(f"  - {WORKSPACE_PATH}/udfs/time_category_udf")
    print(f"  - {WORKSPACE_PATH}/config/gold_tables.yml")


if __name__ == "__main__":
    main()
