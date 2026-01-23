#!/usr/bin/env python3
"""Check latest run status."""
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

    # List recent runs
    print("Recent runs:")
    runs = list(w.jobs.list_runs(limit=5))
    for run in runs:
        state = run.state
        print(f"  Run {run.run_id}: {run.run_name}")
        print(f"    State: {state.life_cycle_state.value if state else 'N/A'}")
        print(f"    Result: {state.result_state.value if state and state.result_state else 'N/A'}")
        print()

if __name__ == "__main__":
    main()
