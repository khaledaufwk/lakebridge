#!/usr/bin/env python3
"""Check full run details."""

import sys
from pathlib import Path
import yaml
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from databricks.sdk import WorkspaceClient

def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_run_info():
    run_file = Path(__file__).parent / "current_run.txt"
    job_id = None
    run_id = None

    if run_file.exists():
        with open(run_file) as f:
            for line in f:
                if line.startswith("JOB_ID="):
                    job_id = int(line.strip().split("=")[1])
                elif line.startswith("RUN_ID="):
                    run_id = int(line.strip().split("=")[1])

    return job_id, run_id

creds = load_credentials()
w = WorkspaceClient(host=creds['databricks']['host'], token=creds['databricks']['token'])

job_id, run_id = load_run_info()
print(f"Job ID: {job_id}")
print(f"Run ID: {run_id}")

run = w.jobs.get_run(run_id)

print(f"\nState: {run.state}")
print(f"Status: {run.status if hasattr(run, 'status') else 'N/A'}")

if run.state:
    print(f"  Life Cycle: {run.state.life_cycle_state}")
    print(f"  Result: {run.state.result_state}")
    print(f"  Queue Reason: {run.state.queue_reason if hasattr(run.state, 'queue_reason') else 'N/A'}")
    print(f"  State Message: {run.state.state_message if run.state.state_message else 'N/A'}")

if run.tasks:
    for task in run.tasks:
        print(f"\nTask: {task.task_key}")
        if task.state:
            print(f"  State: {task.state.life_cycle_state}")
            print(f"  Message: {task.state.state_message if task.state.state_message else 'N/A'}")
        if task.cluster_instance:
            print(f"  Cluster ID: {task.cluster_instance.cluster_id if task.cluster_instance.cluster_id else 'N/A'}")
