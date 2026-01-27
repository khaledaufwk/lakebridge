#!/usr/bin/env python3
"""Monitor job in a loop every 20 minutes."""

import yaml
import time
import sys
from pathlib import Path
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        return yaml.safe_load(f)

def check_job(w, run_id):
    """Check job and return status dict."""
    run = w.jobs.get_run(run_id)
    state = run.state
    life_cycle = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"
    result_state = state.result_state.value if state.result_state else "N/A"

    # Calculate duration
    duration_str = "N/A"
    if run.start_time:
        start = datetime.fromtimestamp(run.start_time / 1000)
        if run.end_time:
            end = datetime.fromtimestamp(run.end_time / 1000)
            duration = end - start
        else:
            duration = datetime.now() - start
        duration_str = str(duration).split('.')[0]

    # Count tasks
    completed = running = failed = 0
    errors = []
    serious_errors = False
    failed_tasks = []

    if run.tasks:
        for task in run.tasks:
            ts = task.state
            if ts:
                tr = ts.result_state.value if ts.result_state else ""
                tl = ts.life_cycle_state.value if ts.life_cycle_state else ""
                if tr == "FAILED":
                    failed += 1
                    failed_tasks.append(task.task_key)
                    if ts.state_message:
                        errors.append(f"{task.task_key}: {ts.state_message[:100]}")
                        serious_kw = ["quota", "capacity", "permission", "auth", "connection", "timeout", "oom", "memory"]
                        if any(k in ts.state_message.lower() for k in serious_kw):
                            serious_errors = True
                elif tl == "RUNNING":
                    running += 1
                elif tr == "SUCCESS":
                    completed += 1

    return {
        "run_id": run_id,
        "status": life_cycle,
        "result": result_state,
        "duration": duration_str,
        "completed": completed,
        "running": running,
        "failed": failed,
        "errors": errors,
        "serious_errors": serious_errors,
        "failed_tasks": failed_tasks
    }

def main():
    run_id = int(sys.argv[1]) if len(sys.argv) > 1 else 275031301341569
    interval = 20 * 60  # 20 minutes

    creds = load_credentials()
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    print(f"Starting monitoring for run {run_id}")
    print(f"Checking every 20 minutes...")
    print("=" * 80)

    check_num = 0
    while True:
        check_num += 1
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            status = check_job(w, run_id)

            print(f"\n[CHECK #{check_num}] {now}")
            print(f"Status: {status['status']} | Result: {status['result']} | Duration: {status['duration']}")
            print(f"Tasks: {status['completed']} done, {status['running']} running, {status['failed']} failed")

            if status['failed'] > 0:
                print(f"FAILED TASKS: {', '.join(status['failed_tasks'])}")
                for err in status['errors'][:3]:
                    print(f"  ERROR: {err}")

            # Check if job finished
            if status['status'] == "TERMINATED":
                if status['result'] == "SUCCESS":
                    print("\n*** JOB COMPLETED SUCCESSFULLY ***")
                else:
                    print(f"\n*** JOB TERMINATED: {status['result']} ***")
                    if status['errors']:
                        print("Errors:")
                        for e in status['errors']:
                            print(f"  - {e}")
                break

            # Check for serious errors
            if status['serious_errors']:
                print("\n*** SERIOUS ERRORS DETECTED - STOPPING JOB ***")
                try:
                    w.jobs.cancel_run(run_id)
                    print("Job canceled. Analyze errors above.")
                except Exception as e:
                    print(f"Could not cancel: {e}")
                break

            # Wait for next check
            print(f"Next check in 20 minutes...")
            sys.stdout.flush()
            time.sleep(interval)

        except Exception as e:
            print(f"\n[CHECK #{check_num}] {now}")
            print(f"Error checking status: {e}")
            time.sleep(60)  # Wait 1 minute on error

    print("\nMonitoring complete.")

if __name__ == "__main__":
    main()
