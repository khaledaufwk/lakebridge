"""
Get details about the timed out task
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from databricks.sdk import WorkspaceClient


def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            creds = yaml.safe_load(f)
            return creds.get("databricks", {})
    return {}


def main():
    creds = load_credentials()
    host = creds.get("host", os.environ.get("DATABRICKS_HOST"))
    token = creds.get("token", os.environ.get("DATABRICKS_TOKEN"))

    w = WorkspaceClient(host=host, token=token)
    print(f"Connected to: {host}\n")

    RUN_ID = 161373636312999

    run_info = w.jobs.get_run(run_id=RUN_ID)

    print(f"Run ID: {RUN_ID}")
    print(f"State: {run_info.state.life_cycle_state.value if run_info.state else 'UNKNOWN'}")
    print(f"Result: {run_info.state.result_state.value if run_info.state and run_info.state.result_state else 'N/A'}")

    if run_info.state and run_info.state.state_message:
        print(f"Message: {run_info.state.state_message}")

    # Get run duration
    if run_info.start_time and run_info.end_time:
        duration = (run_info.end_time - run_info.start_time) / 1000 / 60  # minutes
        print(f"Duration: {duration:.1f} minutes")

    if run_info.tasks:
        for task in run_info.tasks:
            print(f"\nTask: {task.task_key}")
            print(f"  State: {task.state.life_cycle_state.value if task.state else 'UNKNOWN'}")
            print(f"  Result: {task.state.result_state.value if task.state and task.state.result_state else 'N/A'}")

            if task.state and task.state.state_message:
                print(f"  Message: {task.state.state_message}")

            if task.run_id:
                try:
                    output = w.jobs.get_run_output(run_id=task.run_id)
                    if output.notebook_output and output.notebook_output.result:
                        print(f"  Output: {output.notebook_output.result}")
                    if output.error:
                        print(f"  Error: {output.error}")
                    if output.error_trace:
                        print(f"  Error Trace (first 1000 chars):\n{output.error_trace[:1000]}")
                    if output.logs:
                        print(f"  Logs (last 2000 chars):\n{output.logs[-2000:]}")
                except Exception as e:
                    print(f"  Could not get output: {e}")

    # Check job configuration for timeout settings
    JOB_ID = 76090089956881
    job = w.jobs.get(JOB_ID)
    print(f"\nJob Configuration:")
    if job.settings:
        if job.settings.timeout_seconds:
            print(f"  Timeout: {job.settings.timeout_seconds / 3600:.1f} hours")

        if job.settings.tasks:
            for task in job.settings.tasks:
                print(f"  Task: {task.task_key}")
                if task.timeout_seconds:
                    print(f"    Task Timeout: {task.timeout_seconds / 60:.0f} minutes")


if __name__ == "__main__":
    main()
