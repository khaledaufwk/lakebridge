"""
Check status of Gold layer job run
"""

import os
import sys
from pathlib import Path

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

    # Check the latest run
    RUN_ID = 48811622374083

    run_info = w.jobs.get_run(run_id=RUN_ID)
    print(f"Run ID: {RUN_ID}")
    print(f"State: {run_info.state.life_cycle_state.value if run_info.state else 'UNKNOWN'}")
    print(f"Result: {run_info.state.result_state.value if run_info.state and run_info.state.result_state else 'N/A'}")

    if run_info.tasks:
        for task in run_info.tasks:
            print(f"\nTask: {task.task_key}")
            print(f"  State: {task.state.life_cycle_state.value if task.state else 'UNKNOWN'}")
            print(f"  Result: {task.state.result_state.value if task.state and task.state.result_state else 'N/A'}")

            if task.run_id:
                try:
                    output = w.jobs.get_run_output(run_id=task.run_id)
                    if output.notebook_output and output.notebook_output.result:
                        print(f"  Output: {output.notebook_output.result}")
                    if output.error:
                        print(f"  Error: {output.error}")
                    if output.error_trace:
                        print(f"  Error Trace (first 500 chars): {output.error_trace[:500]}")
                except Exception as e:
                    print(f"  Could not get output: {e}")


if __name__ == "__main__":
    main()
