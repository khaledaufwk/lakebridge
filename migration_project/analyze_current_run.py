#!/usr/bin/env python3
"""Analyze current Databricks job run."""
import yaml
from pathlib import Path
from datetime import datetime, timezone
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
    
    # Get the job ID from current_run.txt
    job_id = 28181369160316
    
    print("=" * 80)
    print("DATABRICKS JOB RUN ANALYSIS")
    print("=" * 80)
    
    # Get job details
    try:
        job = w.jobs.get(job_id)
        print(f"\nJob: {job.settings.name}")
        print(f"Job ID: {job_id}")
    except Exception as e:
        print(f"Error getting job: {e}")
    
    # List recent runs for this job
    print("\n" + "-" * 80)
    print("RECENT RUNS")
    print("-" * 80)
    
    runs = list(w.jobs.list_runs(job_id=job_id, limit=5))
    
    for run in runs:
        state = run.state
        life_cycle = state.life_cycle_state.value if state and state.life_cycle_state else "N/A"
        result = state.result_state.value if state and state.result_state else "N/A"
        
        # Calculate duration
        start_time = datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc) if run.start_time else None
        end_time = datetime.fromtimestamp(run.end_time / 1000, tz=timezone.utc) if run.end_time else None
        
        if start_time and end_time:
            duration = end_time - start_time
            duration_str = str(duration).split('.')[0]  # Remove microseconds
        elif start_time:
            duration = datetime.now(timezone.utc) - start_time
            duration_str = f"{str(duration).split('.')[0]} (running)"
        else:
            duration_str = "N/A"
        
        print(f"\nRun ID: {run.run_id}")
        print(f"  State: {life_cycle} | Result: {result}")
        print(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC') if start_time else 'N/A'}")
        print(f"  Duration: {duration_str}")
        
        # If this is a running or recent run, get more details
        if life_cycle in ("RUNNING", "PENDING") or (runs.index(run) == 0):
            print(f"\n  Getting detailed run info...")
            try:
                run_details = w.jobs.get_run(run.run_id)
                
                # Check for tasks
                if run_details.tasks:
                    print(f"  Tasks: {len(run_details.tasks)}")
                    for task in run_details.tasks:
                        task_state = task.state
                        task_life = task_state.life_cycle_state.value if task_state and task_state.life_cycle_state else "N/A"
                        task_result = task_state.result_state.value if task_state and task_state.result_state else "N/A"
                        print(f"    - {task.task_key}: {task_life} / {task_result}")
                
                # Get run output/logs
                try:
                    output = w.jobs.get_run_output(run.run_id)
                    if output.error:
                        print(f"\n  ERROR: {output.error}")
                    if output.logs:
                        print(f"\n  LOGS (last 500 chars):")
                        print(f"  {output.logs[-500:] if len(output.logs) > 500 else output.logs}")
                except Exception as e:
                    pass  # Output might not be available yet
                    
            except Exception as e:
                print(f"  Error getting run details: {e}")

if __name__ == "__main__":
    main()
