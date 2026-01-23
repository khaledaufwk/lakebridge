#!/usr/bin/env python3
"""Check details of a failed Databricks run."""
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

    task_run_id = 904317818855856

    print(f"Getting output for task run {task_run_id}...")

    try:
        output = w.jobs.get_run_output(task_run_id)
        print("\n=== RUN OUTPUT ===")
        if output.error:
            print(f"Error: {output.error}")
        if output.error_trace:
            print(f"\nError Trace:\n{output.error_trace}")
        if output.logs:
            print(f"\nLogs:\n{output.logs}")
        if output.notebook_output:
            print(f"\nNotebook Output: {output.notebook_output}")
        if output.metadata:
            print(f"\nMetadata: {output.metadata}")
    except Exception as e:
        print(f"Error getting output: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
