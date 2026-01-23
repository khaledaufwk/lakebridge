#!/usr/bin/env python3
"""Get detailed info about a specific run."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient


def main():
    run_id = 351399508244937  # DeviceLocation reset job (new)

    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    run = w.jobs.get_run(run_id)

    print(f"Run ID: {run.run_id}")
    print(f"Name: {run.run_name}")
    print(f"State: {run.state.life_cycle_state.value if run.state else 'N/A'}")
    if run.state and run.state.result_state:
        print(f"Result: {run.state.result_state.value}")
    if run.state and run.state.state_message:
        print(f"Message: {run.state.state_message}")

    if run.tasks:
        for task in run.tasks:
            print(f"\nTask: {task.task_key}")
            if task.state:
                print(f"  State: {task.state.life_cycle_state.value if task.state.life_cycle_state else 'N/A'}")
                if task.state.result_state:
                    print(f"  Result: {task.state.result_state.value}")
                if task.state.state_message:
                    print(f"  Message: {task.state.state_message}")

    # Get run output
    try:
        output = w.jobs.get_run_output(run_id)
        if output.error:
            print(f"\nError: {output.error}")
        if output.error_trace:
            print(f"\nError Trace:\n{output.error_trace}")
    except Exception as e:
        print(f"\nCould not get output: {e}")


if __name__ == "__main__":
    main()
