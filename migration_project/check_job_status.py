#!/usr/bin/env python3
"""Quick check of job status - recent runs."""

import sys
from pathlib import Path
from datetime import datetime

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

    print("Recent job runs (last 10):")
    print("=" * 80)

    runs = list(w.jobs.list_runs(limit=10))

    for run in runs:
        start_time = datetime.fromtimestamp(run.start_time / 1000) if run.start_time else None
        state = run.state.life_cycle_state.value if run.state else "UNKNOWN"
        result = run.state.result_state.value if run.state and run.state.result_state else ""

        print(f"\nRun ID: {run.run_id}")
        print(f"  Name: {run.run_name}")
        print(f"  State: {state} {result}")
        print(f"  Started: {start_time}")
        print(f"  URL: {creds['databricks']['host']}/#job/{run.job_id}/run/{run.run_id}" if run.job_id else "  URL: N/A")

        if "DeviceLocation" in (run.run_name or ""):
            print("  >>> THIS IS THE DEVICELOCATION JOB <<<")


if __name__ == "__main__":
    main()
