#!/usr/bin/env python3
"""Run the DeviceLocation loader notebook."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    RunLifeCycleState, SubmitTask, NotebookTask
)


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find a running cluster
    print("Finding cluster...")
    cluster_id = None
    for cluster in w.clusters.list():
        if cluster.state and cluster.state.value == "RUNNING":
            if cluster.cluster_name and "dlt" not in cluster.cluster_name.lower():
                cluster_id = cluster.cluster_id
                print(f"  Using: {cluster.cluster_name} ({cluster_id})")
                break

    if not cluster_id:
        print("No running cluster found! Please start a cluster first.")
        return 1

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
    print(f"URL: {creds['databricks']['host']}/#job/{run.tasks[0].run_id if run.tasks else 'unknown'}/run/{run_id}")
    print("\nMonitoring... (Ctrl+C to stop monitoring, job will continue)")

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

            time.sleep(10)
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped. Job continues running in Databricks.")
        print(f"Check status at: {creds['databricks']['host']}/#job/0/run/{run_id}")
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
