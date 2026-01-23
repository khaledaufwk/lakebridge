#!/usr/bin/env python3
"""Check specific run status."""
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

    run_id = 824926232460667
    run = w.jobs.get_run(run_id)

    state = run.state
    print(f"Run ID: {run_id}")
    print(f"Name: {run.run_name}")
    print(f"State: {state.life_cycle_state.value if state else 'N/A'}")
    print(f"Result: {state.result_state.value if state and state.result_state else 'N/A'}")
    if state and state.state_message:
        print(f"Message: {state.state_message}")

    # Calculate duration
    if run.start_time:
        import time
        elapsed = (time.time() * 1000 - run.start_time) / 1000 / 60
        print(f"Running for: {elapsed:.1f} minutes")

if __name__ == "__main__":
    main()
