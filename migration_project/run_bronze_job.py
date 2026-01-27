#!/usr/bin/env python3
"""Run the TimescaleDB Bronze job with optimized settings."""

import yaml
from pathlib import Path
from databricks.sdk import WorkspaceClient

def main():
    # Load credentials
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find the job
    job_name = "WakeCapDW_Bronze_TimescaleDB_Raw"
    jobs = list(w.jobs.list(name=job_name))

    if not jobs:
        print(f"Job '{job_name}' not found!")
        return 1

    job = jobs[0]
    print(f"Found job: {job.settings.name} (ID: {job.job_id})")

    # Run the job with optimized parameters
    print("\nStarting job with parallel loading enabled...")
    run = w.jobs.run_now(
        job_id=job.job_id,
        notebook_params={
            "load_mode": "incremental",
            "category": "ALL",
            "batch_size": "100000",
            "fetch_size": "50000",
            "parallel": "true",
            "max_workers": "4"
        }
    )

    print(f"\n[OK] Job started!")
    print(f"  Run ID: {run.run_id}")
    print(f"  Monitor at: {creds['databricks']['host']}/#job/{job.job_id}/run/{run.run_id}")

    # Save run info
    run_file = Path(__file__).parent / "current_run.txt"
    with open(run_file, 'w') as f:
        f.write(f"JOB_ID={job.job_id}\n")
        f.write(f"RUN_ID={run.run_id}\n")

    return 0

if __name__ == "__main__":
    exit(main())
