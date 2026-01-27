"""
Trigger Gold Layer Job Run
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            creds = yaml.safe_load(f)
            return creds.get("databricks", {})
    return {}


def main():
    from databricks.sdk import WorkspaceClient

    creds = load_credentials()
    host = creds.get("host", os.environ.get("DATABRICKS_HOST"))
    token = creds.get("token", os.environ.get("DATABRICKS_TOKEN"))

    if not host or not token:
        print("Error: Databricks credentials not found")
        sys.exit(1)

    w = WorkspaceClient(host=host, token=token)
    print(f"Connected to: {host}\n")

    # Find the Gold job
    JOB_NAME = "WakeCapDW_Gold_FactWorkersHistory"
    jobs = list(w.jobs.list(name=JOB_NAME))

    if not jobs:
        print(f"Error: Job '{JOB_NAME}' not found")
        sys.exit(1)

    job_id = jobs[0].job_id
    print(f"Found job: {JOB_NAME} (ID: {job_id})")

    # Trigger a new run
    print("\nTriggering job run...")
    run = w.jobs.run_now(
        job_id=job_id,
        notebook_params={"load_mode": "incremental", "lookback_days": "1"}
    )
    run_id = run.run_id
    print(f"Run ID: {run_id}")
    print(f"Monitor at: {host}/#job/{job_id}/run/{run_id}")

    # Wait for run to complete (with timeout)
    print("\nWaiting for run to complete...")
    max_wait = 300  # 5 minutes
    start_time = time.time()

    while True:
        run_info = w.jobs.get_run(run_id=run_id)
        state = run_info.state.life_cycle_state.value if run_info.state else "UNKNOWN"
        result = run_info.state.result_state.value if run_info.state and run_info.state.result_state else None

        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed}s] State: {state}, Result: {result}")

        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break

        if elapsed > max_wait:
            print("\nTimeout waiting for run to complete.")
            break

        time.sleep(10)

    # Get final status
    run_info = w.jobs.get_run(run_id=run_id)
    print(f"\n{'=' * 60}")
    print("RUN COMPLETE")
    print(f"{'=' * 60}")
    print(f"State: {run_info.state.life_cycle_state.value if run_info.state else 'UNKNOWN'}")
    print(f"Result: {run_info.state.result_state.value if run_info.state and run_info.state.result_state else 'N/A'}")

    if run_info.state and run_info.state.state_message:
        print(f"Message: {run_info.state.state_message}")

    # Check task outputs
    if run_info.tasks:
        for task in run_info.tasks:
            task_key = task.task_key
            task_state = task.state.life_cycle_state.value if task.state else "UNKNOWN"
            task_result = task.state.result_state.value if task.state and task.state.result_state else "N/A"

            print(f"\nTask: {task_key}")
            print(f"  State: {task_state}")
            print(f"  Result: {task_result}")

            # Try to get notebook output
            if task.run_id:
                try:
                    output = w.jobs.get_run_output(run_id=task.run_id)
                    if output.notebook_output and output.notebook_output.result:
                        print(f"  Output: {output.notebook_output.result}")
                except Exception as e:
                    print(f"  (Could not get output: {str(e)[:50]})")


if __name__ == "__main__":
    main()
