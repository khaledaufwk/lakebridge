#!/usr/bin/env python3
"""Check job configuration."""

import sys
from pathlib import Path
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from databricks.sdk import WorkspaceClient

def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)

creds = load_credentials()
w = WorkspaceClient(host=creds['databricks']['host'], token=creds['databricks']['token'])

JOB_NAME = "WakeCapDW_Bronze_TimescaleDB_Raw"

# Find job
jobs = list(w.jobs.list(name=JOB_NAME))
for job in jobs:
    if job.settings and job.settings.name == JOB_NAME:
        job_details = w.jobs.get(job.job_id)

        print(f"Job: {job_details.settings.name}")
        print(f"Job ID: {job.job_id}")

        if job_details.settings.tasks:
            for task in job_details.settings.tasks:
                print(f"\nTask: {task.task_key}")
                if task.new_cluster:
                    cluster = task.new_cluster
                    print(f"  Spark Version: {cluster.spark_version}")
                    print(f"  Node Type: {cluster.node_type_id}")
                    print(f"  Num Workers: {cluster.num_workers}")
                    if cluster.spark_conf:
                        print(f"  Spark Config: {cluster.spark_conf}")
                elif task.existing_cluster_id:
                    print(f"  Using existing cluster: {task.existing_cluster_id}")
        break
