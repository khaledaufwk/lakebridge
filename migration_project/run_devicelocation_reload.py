#!/usr/bin/env python3
"""
Run DeviceLocation reload: reset watermarks and full load.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    RunLifeCycleState, RunResultState,
    SubmitTask, NotebookTask
)
from databricks.sdk.service.compute import ClusterSpec, AzureAttributes


WORKSPACE_BASE_PATH = "/Workspace/migration_project/pipelines/timescaledb/notebooks"


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_cluster_id(w: WorkspaceClient) -> str:
    """Get an available RUNNING all-purpose cluster ID, or None to use job cluster."""
    from databricks.sdk.service.compute import ClusterSource

    clusters = list(w.clusters.list())

    # Filter to all-purpose clusters only (not job clusters, not DLT clusters)
    all_purpose_clusters = []
    for cluster in clusters:
        # Skip DLT and job clusters
        if cluster.cluster_source in [ClusterSource.JOB, ClusterSource.PIPELINE]:
            continue
        # Skip if name contains dlt or job
        if cluster.cluster_name and ("dlt" in cluster.cluster_name.lower() or "job" in cluster.cluster_name.lower()):
            continue
        all_purpose_clusters.append(cluster)

    # Only use RUNNING all-purpose clusters (don't wait for terminated ones)
    for cluster in all_purpose_clusters:
        if cluster.state and cluster.state.value == "RUNNING":
            print(f"    Found running all-purpose cluster: {cluster.cluster_name} ({cluster.cluster_id})")
            return cluster.cluster_id

    # No running cluster - use job cluster instead (faster than waiting for cluster to start)
    print("    No running all-purpose cluster found - will use new job cluster")
    return None


def run_notebook(w: WorkspaceClient, cluster_id: str, notebook_path: str, parameters: dict = None) -> dict:
    """Run a notebook and wait for completion."""
    print(f"\n    Running notebook: {notebook_path}")
    if parameters:
        print(f"    Parameters: {parameters}")

    # Build the task
    if cluster_id:
        # Use existing all-purpose cluster
        task = SubmitTask(
            task_key="main",
            existing_cluster_id=cluster_id,
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters=parameters or {}
            )
        )
    else:
        # Create a new job cluster
        print("    Using new job cluster (Standard_DS3_v2, 2 workers)")
        task = SubmitTask(
            task_key="main",
            new_cluster=ClusterSpec(
                spark_version="14.3.x-scala2.12",
                node_type_id="Standard_DS3_v2",
                num_workers=2,
                azure_attributes=AzureAttributes(
                    first_on_demand=1,
                    availability="ON_DEMAND_AZURE",
                    spot_bid_max_price=-1
                ),
                spark_conf={
                    "spark.databricks.delta.preview.enabled": "true"
                }
            ),
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters=parameters or {}
            )
        )

    # Submit the run
    run = w.jobs.submit(
        run_name=f"DeviceLocation Reload - {notebook_path.split('/')[-1]}",
        tasks=[task]
    )

    run_id = run.run_id
    print(f"    Run ID: {run_id}")
    print(f"    Waiting for completion...")

    # Poll for completion
    start_time = time.time()
    while True:
        run_status = w.jobs.get_run(run_id)
        state = run_status.state

        if state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
            break

        elapsed = int(time.time() - start_time)
        print(f"    Status: {state.life_cycle_state.value} (elapsed: {elapsed}s)", end="\r")
        time.sleep(10)

    elapsed = int(time.time() - start_time)
    print(f"\n    Completed in {elapsed} seconds")
    print(f"    Result: {state.result_state.value if state.result_state else 'N/A'}")

    if state.result_state == RunResultState.FAILED:
        print(f"    Error: {state.state_message}")

    return {
        "run_id": run_id,
        "state": state.life_cycle_state.value,
        "result": state.result_state.value if state.result_state else None,
        "message": state.state_message,
        "duration_seconds": elapsed
    }


def main():
    print("=" * 70)
    print("DeviceLocation Reload - Reset Watermarks and Full Load")
    print("=" * 70)

    # Load credentials
    creds = load_credentials()

    # Connect to Databricks
    print("\n[1] Connecting to Databricks...")
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    print(f"    Connected as: {w.current_user.me().user_name}")

    # Get cluster
    print("\n[2] Finding cluster...")
    cluster_id = get_cluster_id(w)

    # Step 1: Reset watermarks
    print("\n" + "=" * 70)
    print("STEP 1: Reset DeviceLocation Watermarks")
    print("=" * 70)

    reset_result = run_notebook(
        w,
        cluster_id,
        f"{WORKSPACE_BASE_PATH}/reset_devicelocation_watermarks",
        parameters={"drop_existing_tables": "yes"}  # Drop tables for clean reload
    )

    if reset_result["result"] != "SUCCESS":
        print("\n[ERROR] Watermark reset failed. Aborting.")
        return 1

    print("\n    ✓ Watermarks reset successfully")

    # Step 2: Run full load
    print("\n" + "=" * 70)
    print("STEP 2: Full Load DeviceLocation Tables")
    print("=" * 70)

    load_result = run_notebook(
        w,
        cluster_id,
        f"{WORKSPACE_BASE_PATH}/bronze_loader_devicelocation",
        parameters={
            "load_mode": "full",
            "run_optimize": "yes",
            "max_rows_per_batch": "500000"
        }
    )

    if load_result["result"] != "SUCCESS":
        print("\n[ERROR] Full load failed.")
        return 1

    print("\n    ✓ Full load completed successfully")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"""
Step 1 - Reset Watermarks:
  Run ID: {reset_result['run_id']}
  Result: {reset_result['result']}
  Duration: {reset_result['duration_seconds']}s

Step 2 - Full Load:
  Run ID: {load_result['run_id']}
  Result: {load_result['result']}
  Duration: {load_result['duration_seconds']}s

Total Duration: {reset_result['duration_seconds'] + load_result['duration_seconds']}s

Next: Verify row counts in Databricks:
  SELECT COUNT(*) FROM wakecap_prod.raw.timescale_devicelocation;
  SELECT COUNT(*) FROM wakecap_prod.raw.timescale_devicelocationsummary;
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
