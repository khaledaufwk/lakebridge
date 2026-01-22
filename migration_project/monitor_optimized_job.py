#!/usr/bin/env python3
"""Monitor the optimized TimescaleDB Bronze job."""

import sys
from pathlib import Path
import yaml
from datetime import datetime
import time

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


def main():
    creds = load_credentials()
    job_id, run_id = load_run_info()

    if not run_id:
        print("No run ID found in current_run.txt")
        return 1

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    print("=" * 70)
    print("TIMESCALEDB BRONZE JOB MONITOR")
    print("=" * 70)
    print(f"Job ID: {job_id}")
    print(f"Run ID: {run_id}")
    print(f"Check time: {datetime.now()}")
    print("=" * 70)

    try:
        run = w.jobs.get_run(run_id)

        state = run.state.life_cycle_state if run.state else "Unknown"
        result = run.state.result_state if run.state and run.state.result_state else "In Progress"

        print(f"\nState: {state}")
        print(f"Result: {result}")

        if run.start_time:
            start = datetime.fromtimestamp(run.start_time / 1000)
            elapsed = (datetime.now() - start).total_seconds() / 60
            print(f"Started: {start}")
            print(f"Elapsed: {elapsed:.1f} minutes")

        if run.end_time:
            end = datetime.fromtimestamp(run.end_time / 1000)
            print(f"Ended: {end}")

        # Task details
        if run.tasks:
            print("\n" + "-" * 70)
            print("TASK STATUS:")
            print("-" * 70)

            for task in run.tasks:
                task_state = task.state.life_cycle_state if task.state else "Unknown"
                task_result = task.state.result_state if task.state and task.state.result_state else "N/A"

                # Status icon
                if "RUNNING" in str(task_state):
                    icon = "[RUNNING]"
                elif "SUCCESS" in str(task_result):
                    icon = "[SUCCESS]"
                elif "FAILED" in str(task_result):
                    icon = "[FAILED]"
                else:
                    icon = "[PENDING]"

                print(f"\n{icon} Task: {task.task_key}")
                print(f"   State: {task_state}")
                print(f"   Result: {task_result}")

                if task.start_time:
                    print(f"   Started: {datetime.fromtimestamp(task.start_time / 1000)}")

                # Try to get output/logs
                if task.run_id and str(task_result) in ('FAILED', 'RunResultState.FAILED'):
                    try:
                        output = w.jobs.get_run_output(task.run_id)
                        if output.error:
                            print(f"   Error: {output.error[:200]}")
                        if output.notebook_output and output.notebook_output.result:
                            print(f"   Output: {output.notebook_output.result[:200]}")
                    except Exception as e:
                        pass

                elif task.run_id and str(task_result) in ('SUCCESS', 'RunResultState.SUCCESS'):
                    try:
                        output = w.jobs.get_run_output(task.run_id)
                        if output.notebook_output and output.notebook_output.result:
                            print(f"   Output: {output.notebook_output.result}")
                    except Exception as e:
                        pass

        print("\n" + "=" * 70)
        print(f"Monitor URL: {creds['databricks']['host']}/#job/{job_id}/run/{run_id}")
        print("=" * 70)

        # Return status
        if "TERMINATED" in str(state) or "INTERNAL_ERROR" in str(state):
            if "SUCCESS" in str(result):
                print("\n[OK] JOB COMPLETED SUCCESSFULLY!")
                return 0
            else:
                print("\n[FAILED] JOB FAILED")
                return 1
        else:
            print("\n[RUNNING] JOB STILL RUNNING...")
            return 2

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
