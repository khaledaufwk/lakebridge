"""Analyze Bronze job run for optimization opportunities."""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState
import yaml
from datetime import datetime

# Load credentials
with open(r'C:\Users\khaledadmin\.databricks\labs\lakebridge\.credentials.yml') as f:
    creds = yaml.safe_load(f)

w = WorkspaceClient(
    host=creds['databricks']['host'],
    token=creds['databricks']['token']
)

# Find the job
jobs = list(w.jobs.list(name='WakeCapDW_Bronze_TimescaleDB_Raw'))
if not jobs:
    print("Job not found!")
    exit(1)

job = jobs[0]
print(f"Job ID: {job.job_id}")
print(f"Job Name: {job.settings.name}")

# Get recent runs
runs = list(w.jobs.list_runs(job_id=job.job_id, limit=5))
print(f"\n{'='*80}")
print("Recent runs:")
print(f"{'='*80}")

for run in runs:
    duration_mins = (run.run_duration or 0) / 60000
    state = run.state.life_cycle_state.value if run.state.life_cycle_state else "unknown"
    result = run.state.result_state.value if run.state.result_state else "running"
    start_time = datetime.fromtimestamp(run.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S") if run.start_time else "N/A"
    print(f"  Run {run.run_id}: {state} / {result} - {duration_mins:.1f} min - Started: {start_time}")

# Get the most recent run (first in the list)
latest_run = runs[0] if runs else None
if not latest_run:
    print("No runs found!")
    exit(1)

print(f"\n{'='*80}")
print(f"Analyzing Run: {latest_run.run_id}")
print(f"{'='*80}")

# Get run details
run_details = w.jobs.get_run(run_id=latest_run.run_id)
print(f"State: {run_details.state.life_cycle_state.value}")
print(f"Result: {run_details.state.result_state.value if run_details.state.result_state else 'In Progress'}")
print(f"Start Time: {datetime.fromtimestamp(run_details.start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")

if run_details.end_time:
    print(f"End Time: {datetime.fromtimestamp(run_details.end_time / 1000).strftime('%Y-%m-%d %H:%M:%S')}")

duration_mins = (run_details.run_duration or 0) / 60000
print(f"Duration: {duration_mins:.1f} minutes")

# Get cluster info
if run_details.cluster_instance:
    print(f"\nCluster ID: {run_details.cluster_instance.cluster_id}")

# Get task info
if run_details.tasks:
    print(f"\n{'='*80}")
    print("Task Details:")
    print(f"{'='*80}")
    for task in run_details.tasks:
        task_duration = (task.run_duration or 0) / 60000
        task_state = task.state.life_cycle_state.value if task.state and task.state.life_cycle_state else "unknown"
        task_result = task.state.result_state.value if task.state and task.state.result_state else "running"
        print(f"  Task: {task.task_key}")
        print(f"    State: {task_state} / {task_result}")
        print(f"    Duration: {task_duration:.1f} min")

# Get run output/logs
print(f"\n{'='*80}")
print("Getting run output...")
print(f"{'='*80}")

try:
    run_output = w.jobs.get_run_output(run_id=latest_run.run_id)
    if run_output.notebook_output and run_output.notebook_output.result:
        result = run_output.notebook_output.result
        # Print first 5000 chars
        print(result[:5000] if len(result) > 5000 else result)
    if run_output.error:
        print(f"Error: {run_output.error}")
    if run_output.logs:
        print(f"\nLogs (truncated):")
        print(run_output.logs[:3000] if len(run_output.logs) > 3000 else run_output.logs)
except Exception as e:
    print(f"Could not get run output: {e}")

# If there are tasks, try to get output from each
if run_details.tasks:
    for task in run_details.tasks:
        if task.run_id:
            try:
                task_output = w.jobs.get_run_output(run_id=task.run_id)
                if task_output.notebook_output and task_output.notebook_output.result:
                    print(f"\n{'='*80}")
                    print(f"Output from task {task.task_key}:")
                    print(f"{'='*80}")
                    result = task_output.notebook_output.result
                    print(result[:10000] if len(result) > 10000 else result)
            except Exception as e:
                print(f"Could not get output for task {task.task_key}: {e}")
