#!/usr/bin/env python3
"""Monitor Databricks job run and report status."""

import yaml
import sys
from pathlib import Path
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_run_output(w, run_id):
    """Get detailed output from a run."""
    try:
        output = w.jobs.get_run_output(run_id)
        return output
    except:
        return None

def check_job_status(run_id=None):
    """Check job status and return details."""
    creds = load_credentials()
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    job_id = 181959206191493  # Silver job

    print("=" * 80)
    print(f"JOB STATUS CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Get specific run or find active run
    if run_id:
        run = w.jobs.get_run(run_id)
    else:
        runs = list(w.jobs.list_runs(job_id=job_id, active_only=True))
        if not runs:
            # Check most recent run
            runs = list(w.jobs.list_runs(job_id=job_id, limit=1))
            if runs:
                run = w.jobs.get_run(runs[0].run_id)
            else:
                print("No runs found")
                return None
        else:
            run = w.jobs.get_run(runs[0].run_id)

    # Print run status
    state = run.state
    life_cycle = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"
    result_state = state.result_state.value if state.result_state else "N/A"

    print(f"\nRun ID: {run.run_id}")
    print(f"Status: {life_cycle}")
    print(f"Result: {result_state}")

    # Calculate duration
    if run.start_time:
        start = datetime.fromtimestamp(run.start_time / 1000)
        if run.end_time:
            end = datetime.fromtimestamp(run.end_time / 1000)
            duration = end - start
        else:
            duration = datetime.now() - start
        print(f"Duration: {duration}")
        print(f"Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")

    # Check for errors
    errors = []
    serious_errors = False

    if state.state_message:
        print(f"\nState Message: {state.state_message}")
        if "error" in state.state_message.lower() or "failed" in state.state_message.lower():
            errors.append(state.state_message)

    # Get task details if available
    if run.tasks:
        print(f"\n--- Task Status ({len(run.tasks)} tasks) ---")
        failed_tasks = []
        running_tasks = []
        completed_tasks = []

        for task in run.tasks:
            task_state = task.state
            if task_state:
                task_status = task_state.life_cycle_state.value if task_state.life_cycle_state else "UNKNOWN"
                task_result = task_state.result_state.value if task_state.result_state else ""

                if task_result == "FAILED":
                    failed_tasks.append(task)
                    if task_state.state_message:
                        errors.append(f"Task {task.task_key}: {task_state.state_message}")
                elif task_status == "RUNNING":
                    running_tasks.append(task)
                elif task_result == "SUCCESS":
                    completed_tasks.append(task)

        print(f"Completed: {len(completed_tasks)}")
        print(f"Running: {len(running_tasks)}")
        print(f"Failed: {len(failed_tasks)}")

        if running_tasks:
            print("\nCurrently Running:")
            for task in running_tasks[:5]:
                print(f"  - {task.task_key}")

        if failed_tasks:
            print("\nFailed Tasks:")
            for task in failed_tasks:
                print(f"  - {task.task_key}")
                if task.state and task.state.state_message:
                    msg = task.state.state_message[:200]
                    print(f"    Error: {msg}")
                    # Check for serious errors
                    serious_keywords = ["quota", "capacity", "permission", "authentication", "connection refused", "timeout"]
                    if any(kw in msg.lower() for kw in serious_keywords):
                        serious_errors = True

    # Check cluster errors
    if hasattr(run, 'cluster_instance') and run.cluster_instance:
        cluster = run.cluster_instance
        if hasattr(cluster, 'spark_context_id'):
            print(f"\nCluster: {cluster.cluster_id if hasattr(cluster, 'cluster_id') else 'N/A'}")

    # Summary
    print("\n" + "-" * 80)
    if life_cycle == "TERMINATED":
        if result_state == "SUCCESS":
            print("STATUS: JOB COMPLETED SUCCESSFULLY")
            return {"status": "success", "run_id": run.run_id}
        else:
            print(f"STATUS: JOB TERMINATED - {result_state}")
            if errors:
                print("\nErrors found:")
                for err in errors[:5]:
                    print(f"  - {err[:150]}")
            return {"status": "failed", "run_id": run.run_id, "errors": errors}
    elif life_cycle == "RUNNING":
        if serious_errors:
            print("STATUS: RUNNING WITH SERIOUS ERRORS - RECOMMEND STOPPING")
            return {"status": "serious_errors", "run_id": run.run_id, "errors": errors}
        elif errors:
            print("STATUS: RUNNING WITH SOME ERRORS")
            return {"status": "running_with_errors", "run_id": run.run_id, "errors": errors}
        else:
            print("STATUS: RUNNING NORMALLY")
            return {"status": "running", "run_id": run.run_id}
    else:
        print(f"STATUS: {life_cycle}")
        return {"status": life_cycle.lower(), "run_id": run.run_id}

if __name__ == "__main__":
    run_id = int(sys.argv[1]) if len(sys.argv) > 1 else None
    result = check_job_status(run_id)
    if result:
        print(f"\nMonitor URL: https://adb-3022397433351638.18.azuredatabricks.net/#job/181959206191493/run/{result['run_id']}")
