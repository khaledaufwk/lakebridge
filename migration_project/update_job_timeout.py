"""
Update job timeout settings for gold_fact_workers_shifts
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

    JOB_ID = 76090089956881

    # Get current job settings
    job = w.jobs.get(JOB_ID)
    print(f"Job: {job.settings.name}")

    # Update task timeouts
    if job.settings and job.settings.tasks:
        for task in job.settings.tasks:
            if task.task_key == "gold_fact_workers_shifts":
                print(f"\nUpdating {task.task_key} timeout:")
                print(f"  Current: {task.timeout_seconds / 60:.0f} minutes")

                # Increase timeout to 6 hours
                task.timeout_seconds = 21600  # 6 hours
                print(f"  New: {task.timeout_seconds / 60:.0f} minutes (6 hours)")

    # Update the job
    w.jobs.update(
        job_id=JOB_ID,
        new_settings=job.settings
    )

    print("\nJob updated successfully!")

    # Verify
    job = w.jobs.get(JOB_ID)
    print("\nVerified configuration:")
    if job.settings and job.settings.tasks:
        for task in job.settings.tasks:
            timeout_mins = task.timeout_seconds / 60 if task.timeout_seconds else "default"
            print(f"  {task.task_key}: {timeout_mins} minutes")


if __name__ == "__main__":
    main()
