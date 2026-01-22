#!/usr/bin/env python3
"""Run the optimized TimescaleDB Bronze job."""

import sys
from pathlib import Path
import yaml
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from databricks.sdk import WorkspaceClient

JOB_NAME = "WakeCapDW_Bronze_TimescaleDB_Raw"


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def find_job(w, job_name):
    """Find job by name."""
    jobs = list(w.jobs.list(name=job_name))
    for job in jobs:
        if job.settings and job.settings.name == job_name:
            return job.job_id
    return None


def main():
    creds = load_credentials()

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    print(f"Looking for job: {JOB_NAME}")
    JOB_ID = find_job(w, JOB_NAME)

    if not JOB_ID:
        print(f"ERROR: Job '{JOB_NAME}' not found!")
        return 1

    print(f"Found job ID: {JOB_ID}")
    print("Starting TimescaleDB Bronze job...")

    run = w.jobs.run_now(job_id=JOB_ID)

    print(f"Job started!")
    print(f"Job ID: {JOB_ID}")
    print(f"Run ID: {run.run_id}")
    print(f"\nMonitor at: {creds['databricks']['host']}/#job/{JOB_ID}/run/{run.run_id}")

    # Save run info
    run_file = Path(__file__).parent / "current_run.txt"
    with open(run_file, 'w') as f:
        f.write(f"JOB_ID={JOB_ID}\n")
        f.write(f"RUN_ID={run.run_id}\n")
        f.write(f"STARTED={datetime.now()}\n")

    print(f"\nRun info saved to: {run_file}")


if __name__ == "__main__":
    main()
