#!/usr/bin/env python3
"""Deploy and run the DeviceLocation full load."""
import sys
import base64
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import RunLifeCycleState, SubmitTask, NotebookTask

def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Deploy updated loader module
    loader_local = Path(__file__).parent / "pipelines/timescaledb/src/timescaledb_loader_v2.py"
    loader_path = "/Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2"

    print("Deploying updated loader module...")
    content = loader_local.read_text(encoding='utf-8')
    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')

    w.workspace.import_(
        path=loader_path,
        content=content_b64,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print(f"  Deployed to: {loader_path}")

    # Deploy main notebook
    notebook_local = Path(__file__).parent / "pipelines/timescaledb/notebooks/bronze_loader_devicelocation.py"
    notebook_path = "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation"

    print("\nDeploying DeviceLocation loader notebook...")
    content = notebook_local.read_text(encoding='utf-8')
    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')

    w.workspace.import_(
        path=notebook_path,
        content=content_b64,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print(f"  Deployed to: {notebook_path}")

    # Find running cluster
    print("\nFinding cluster...")
    cluster_id = None
    for cluster in w.clusters.list():
        if cluster.state and cluster.state.value == "RUNNING":
            if cluster.cluster_name and "dlt" not in cluster.cluster_name.lower():
                cluster_id = cluster.cluster_id
                print(f"  Using: {cluster.cluster_name} ({cluster_id})")
                break

    if not cluster_id:
        print("No running cluster found!")
        return 1

    # Submit notebook with parameters
    parameters = {
        "load_mode": "full",
        "run_optimize": "yes",
        "max_rows_per_batch": "500000"
    }

    print(f"\nSubmitting DeviceLocation full load...")
    print(f"  Parameters: {parameters}")

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
    print("\nMonitoring... (this will take a while for 82M rows)")
    print("Press Ctrl+C to stop monitoring (job continues in Databricks)")

    # Wait for completion
    start_time = time.time()
    last_state = None
    try:
        while True:
            run_status = w.jobs.get_run(run_id)
            state = run_status.state

            if state.life_cycle_state.value != last_state:
                elapsed = int(time.time() - start_time)
                print(f"  [{elapsed}s] {state.life_cycle_state.value}")
                last_state = state.life_cycle_state.value

            if state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                break

            time.sleep(30)
    except KeyboardInterrupt:
        print(f"\n\nMonitoring stopped. Job continues in Databricks.")
        print(f"Check status at: {creds['databricks']['host']}/#job/0/run/{run_id}")
        return 0

    elapsed = int(time.time() - start_time)
    result = state.result_state.value if state.result_state else "N/A"

    print(f"\n{'='*60}")
    print(f"COMPLETED in {elapsed} seconds ({elapsed/60:.1f} minutes)")
    print(f"Result: {result}")
    if state.state_message:
        print(f"Message: {state.state_message}")
    print(f"{'='*60}")

    return 0 if result == "SUCCESS" else 1

if __name__ == "__main__":
    sys.exit(main())
