"""Check job cluster configuration."""
from databricks.sdk import WorkspaceClient
import yaml
import json

with open(r'C:\Users\khaledadmin\.databricks\labs\lakebridge\.credentials.yml') as f:
    creds = yaml.safe_load(f)

w = WorkspaceClient(host=creds['databricks']['host'], token=creds['databricks']['token'])

# Get the job
jobs = list(w.jobs.list(name='WakeCapDW_Bronze_TimescaleDB_Raw'))
job = jobs[0]
job_details = w.jobs.get(job_id=job.job_id)

print("=" * 80)
print(f"Job: {job_details.settings.name}")
print(f"Job ID: {job.job_id}")
print("=" * 80)

# Check job clusters
if job_details.settings.job_clusters:
    print("\nJob Clusters:")
    for jc in job_details.settings.job_clusters:
        print(f"\n  Cluster Key: {jc.job_cluster_key}")
        if jc.new_cluster:
            nc = jc.new_cluster
            print(f"    Spark Version: {nc.spark_version}")
            print(f"    Node Type: {nc.node_type_id}")
            print(f"    Num Workers: {nc.num_workers}")
            if nc.autoscale:
                print(f"    Autoscale: {nc.autoscale.min_workers} - {nc.autoscale.max_workers}")
            if nc.spark_conf:
                print(f"    Spark Config: {nc.spark_conf}")

# Check tasks
print("\nTasks:")
for task in job_details.settings.tasks or []:
    print(f"\n  Task: {task.task_key}")
    if task.job_cluster_key:
        print(f"    Cluster Key: {task.job_cluster_key}")
    if task.existing_cluster_id:
        print(f"    Existing Cluster: {task.existing_cluster_id}")
    if task.notebook_task:
        print(f"    Notebook: {task.notebook_task.notebook_path}")
        if task.notebook_task.base_parameters:
            print(f"    Parameters: {task.notebook_task.base_parameters}")
    if task.depends_on:
        print(f"    Depends On: {[d.task_key for d in task.depends_on]}")

# Check schedule
if job_details.settings.schedule:
    sched = job_details.settings.schedule
    print(f"\nSchedule: {sched.quartz_cron_expression} ({sched.timezone_id})")
    print(f"Paused: {sched.pause_status}")
