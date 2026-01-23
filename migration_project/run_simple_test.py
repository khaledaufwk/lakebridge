#!/usr/bin/env python3
"""Deploy and run a simple test notebook."""
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

    # Deploy simple test notebook
    notebook_local = Path(__file__).parent / "pipelines/timescaledb/notebooks/test_credentials.py"
    notebook_path = "/Workspace/migration_project/pipelines/timescaledb/notebooks/test_credentials"

    print(f"Deploying simple test notebook...")
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
    cluster_name = None
    for cluster in w.clusters.list():
        if cluster.state and cluster.state.value == "RUNNING":
            if cluster.cluster_name and "dlt" not in cluster.cluster_name.lower():
                cluster_id = cluster.cluster_id
                cluster_name = cluster.cluster_name
                print(f"  Using: {cluster.cluster_name} ({cluster_id})")
                break

    if not cluster_id:
        print("No running cluster found!")
        return 1

    # Check cluster access mode
    cluster_info = w.clusters.get(cluster_id)
    print(f"  Access mode: {cluster_info.data_security_mode}")
    print(f"  Single user: {cluster_info.single_user_name}")

    # Submit notebook
    print("\nSubmitting simple test notebook...")
    run = w.jobs.submit(
        run_name="Simple Test",
        tasks=[
            SubmitTask(
                task_key="test",
                existing_cluster_id=cluster_id,
                notebook_task=NotebookTask(
                    notebook_path=notebook_path
                )
            )
        ]
    )

    run_id = run.run_id
    print(f"Run ID: {run_id}")
    print(f"URL: {creds['databricks']['host']}/#job/0/run/{run_id}")
    print("\nWaiting for completion...")

    # Wait for completion
    start_time = time.time()
    while True:
        run_status = w.jobs.get_run(run_id)
        state = run_status.state

        if state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
            break

        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed}s] {state.life_cycle_state.value}")
        time.sleep(5)

    elapsed = int(time.time() - start_time)
    result = state.result_state.value if state.result_state else "N/A"

    print(f"\nCompleted in {elapsed}s")
    print(f"Result: {result}")
    if state.state_message:
        print(f"Message: {state.state_message}")

    return 0 if result == "SUCCESS" else 1

if __name__ == "__main__":
    sys.exit(main())
