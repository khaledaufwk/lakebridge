#!/usr/bin/env python3
"""Test running a known-working notebook."""
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


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find running all-purpose cluster
    cluster_id = None
    for cluster in w.clusters.list():
        if cluster.state and cluster.state.value == "RUNNING":
            if cluster.cluster_name and "dlt" not in cluster.cluster_name.lower():
                cluster_id = cluster.cluster_id
                print(f"Using cluster: {cluster.cluster_name} ({cluster_id})")
                break

    if not cluster_id:
        print("No running cluster found!")
        return

    # Run a simple test - the working bronze_loader_optimized with max_tables=1
    notebook_path = "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_optimized"
    parameters = {
        "load_mode": "incremental",
        "category": "dimensions",
        "max_tables": "1"  # Only load 1 table as a test
    }

    print(f"\nRunning notebook: {notebook_path}")
    print(f"Parameters: {parameters}")

    run = w.jobs.submit(
        run_name="Test Run - bronze_loader_optimized",
        tasks=[
            SubmitTask(
                task_key="main",
                existing_cluster_id=cluster_id,
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=parameters
                )
            )
        ]
    )

    run_id = run.run_id
    print(f"Run ID: {run_id}")
    print("Waiting for completion...")

    start_time = time.time()
    while True:
        run_status = w.jobs.get_run(run_id)
        state = run_status.state

        if state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
            break

        elapsed = int(time.time() - start_time)
        print(f"  Status: {state.life_cycle_state.value} (elapsed: {elapsed}s)")
        time.sleep(10)

    elapsed = int(time.time() - start_time)
    print(f"\nCompleted in {elapsed} seconds")
    print(f"Result: {state.result_state.value if state.result_state else 'N/A'}")
    if state.state_message:
        print(f"Message: {state.state_message}")


if __name__ == "__main__":
    main()
