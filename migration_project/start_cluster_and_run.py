#!/usr/bin/env python3
"""Start cluster and run the DeviceLocation loader notebook."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    RunLifeCycleState, SubmitTask, NotebookTask
)
from databricks.sdk.service.compute import State


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find the Migrate cluster
    print("Finding cluster...")
    target_cluster = None
    for cluster in w.clusters.list():
        if cluster.cluster_name and "Migrate" in cluster.cluster_name:
            target_cluster = cluster
            print(f"  Found: {cluster.cluster_name} ({cluster.cluster_id})")
            print(f"  State: {cluster.state.value}")
            break

    if not target_cluster:
        print("Migrate cluster not found!")
        return 1

    # Start cluster if not running
    if target_cluster.state != State.RUNNING:
        print(f"\nStarting cluster {target_cluster.cluster_name}...")
        w.clusters.start(target_cluster.cluster_id)

        # Wait for cluster to start
        for i in range(60):  # Wait up to 10 minutes
            status = w.clusters.get(target_cluster.cluster_id)
            print(f"  [{i*10}s] State: {status.state.value}")
            if status.state == State.RUNNING:
                print("  Cluster started!")
                break
            if status.state in [State.ERROR, State.TERMINATED]:
                print(f"  Cluster failed to start: {status.state_message}")
                return 1
            time.sleep(10)
        else:
            print("  Timeout waiting for cluster to start")
            return 1

    cluster_id = target_cluster.cluster_id

    # Submit the job
    notebook_path = "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation"
    parameters = {
        "load_mode": "full",
        "run_optimize": "yes",
        "max_rows_per_batch": "500000"
    }

    print(f"\nSubmitting notebook: {notebook_path}")
    print(f"Parameters: {parameters}")

    run = w.jobs.submit(
        run_name="DeviceLocation Full Load",
        tasks=[
            SubmitTask(
                task_key="load",
                existing_cluster_id=cluster_id,
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=parameters
                )
            )
        ]
    )

    run_id = run.run_id
    print(f"\nRun ID: {run_id}")
    print(f"URL: {creds['databricks']['host']}/#job/0/run/{run_id}")
    print("\nMonitoring... (Ctrl+C to stop, job continues in Databricks)")

    start_time = time.time()
    last_state = None

    try:
        while True:
            run_status = w.jobs.get_run(run_id)
            state = run_status.state
            current_state = state.life_cycle_state.value if state else "UNKNOWN"

            if current_state != last_state:
                elapsed = int(time.time() - start_time)
                print(f"  [{elapsed}s] State: {current_state}")
                last_state = current_state

            if state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                break

            time.sleep(15)
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped. Job continues in Databricks.")
        return 0

    elapsed = int(time.time() - start_time)
    result = state.result_state.value if state.result_state else "N/A"

    print(f"\n{'='*60}")
    print(f"COMPLETED in {elapsed} seconds")
    print(f"Result: {result}")
    if state.state_message:
        print(f"Message: {state.state_message}")
    print(f"{'='*60}")

    return 0 if result == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
