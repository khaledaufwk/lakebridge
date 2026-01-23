#!/usr/bin/env python3
"""Get error details from last run."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient

def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    run_id = 336881816654572

    print(f"Getting run {run_id}...")
    run = w.jobs.get_run(run_id)

    if run.tasks:
        task = run.tasks[0]
        print(f"Task run ID: {task.run_id}")

        try:
            output = w.jobs.get_run_output(task.run_id)
            print(f"\n=== OUTPUT ===")
            print(f"Error: {output.error}")
            print(f"\nError Trace:")
            print(output.error_trace)
            print(f"\nLogs: {output.logs}")
            print(f"\nNotebook output: {output.notebook_output}")
        except Exception as e:
            print(f"Error getting output: {e}")

if __name__ == "__main__":
    main()
