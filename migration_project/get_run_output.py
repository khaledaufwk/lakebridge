"""
Get detailed run output for the Gold layer job
"""

import os
import sys
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

    # Get the latest run
    RUN_ID = 336859334076869  # Latest run ID from trigger_gold_job_full.py

    run_info = w.jobs.get_run(run_id=RUN_ID)

    print(f"Run ID: {RUN_ID}")
    print(f"State: {run_info.state.life_cycle_state.value if run_info.state else 'UNKNOWN'}")
    print(f"Result: {run_info.state.result_state.value if run_info.state and run_info.state.result_state else 'N/A'}")

    if run_info.tasks:
        for task in run_info.tasks:
            print(f"\n{'=' * 60}")
            print(f"Task: {task.task_key}")
            print(f"{'=' * 60}")
            print(f"State: {task.state.life_cycle_state.value if task.state else 'UNKNOWN'}")
            print(f"Result: {task.state.result_state.value if task.state and task.state.result_state else 'N/A'}")

            if task.run_id:
                print(f"Task Run ID: {task.run_id}")

                # Get run output
                try:
                    output = w.jobs.get_run_output(run_id=task.run_id)

                    if output.error:
                        print(f"\nERROR: {output.error}")

                    if output.error_trace:
                        print(f"\nERROR TRACE:\n{output.error_trace}")

                    if output.notebook_output:
                        print(f"\nNOTEBOOK OUTPUT:")
                        if output.notebook_output.result:
                            print(f"  Result: {output.notebook_output.result}")
                        if output.notebook_output.truncated:
                            print(f"  (output truncated)")

                    if output.logs:
                        print(f"\nLOGS (first 2000 chars):\n{output.logs[:2000]}")

                    if output.logs_truncated:
                        print("  (logs truncated)")

                except Exception as e:
                    print(f"\nCould not get output: {e}")

                # Try to get cluster logs
                try:
                    if task.cluster_instance and task.cluster_instance.cluster_id:
                        cluster_id = task.cluster_instance.cluster_id
                        print(f"\nCluster ID: {cluster_id}")
                except Exception as e:
                    pass

                # Check state message
                if task.state and task.state.state_message:
                    print(f"\nState Message: {task.state.state_message}")

    # Try to get job-level error
    if run_info.state and run_info.state.state_message:
        print(f"\n{'=' * 60}")
        print("JOB-LEVEL ERROR MESSAGE")
        print(f"{'=' * 60}")
        print(run_info.state.state_message)


if __name__ == "__main__":
    main()
