#!/usr/bin/env python3
"""
Check and run the Bronze job with incremental mode.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    print("=" * 70)
    print("Bronze Job - Incremental Load")
    print("=" * 70)

    # Load credentials and connect
    print("\n[1] Connecting to Databricks...")
    creds = load_credentials()
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    user = w.current_user.me()
    print(f"    Connected as: {user.user_name}")

    # Find the bronze job
    print("\n[2] Finding Bronze job...")
    bronze_job = None
    job_name_patterns = ["WakeCapDW_Bronze_TimescaleDB", "Bronze_TimescaleDB", "bronze"]

    jobs = list(w.jobs.list())
    print(f"    Found {len(jobs)} jobs total")

    for job in jobs:
        job_name_lower = job.settings.name.lower() if job.settings and job.settings.name else ""
        if "bronze" in job_name_lower and "timescale" in job_name_lower:
            bronze_job = job
            print(f"    [FOUND] {job.settings.name} (ID: {job.job_id})")
            break

    if not bronze_job:
        # List all jobs to help identify the correct one
        print("\n    Available jobs:")
        for job in jobs:
            if job.settings and job.settings.name:
                print(f"      - {job.settings.name} (ID: {job.job_id})")
        print("\n    [ERROR] Bronze job not found. Please check job name.")
        return 1

    # Get full job details
    job_id = bronze_job.job_id
    job_details = w.jobs.get(job_id)

    print(f"\n[3] Checking job configuration...")
    print(f"    Job Name: {job_details.settings.name}")
    print(f"    Job ID: {job_id}")

    # Check current parameters
    current_params = {}
    if job_details.settings.tasks:
        for task in job_details.settings.tasks:
            if task.notebook_task and task.notebook_task.base_parameters:
                current_params = task.notebook_task.base_parameters
                break

    print(f"    Current base_parameters: {current_params}")

    # Check if load_mode is incremental
    current_mode = current_params.get("load_mode", "not set")
    print(f"    Current load_mode: {current_mode}")

    if current_mode != "incremental":
        print(f"\n[4] Updating job to use incremental mode by default...")

        # Update the job's base parameters
        if job_details.settings.tasks:
            for task in job_details.settings.tasks:
                if task.notebook_task:
                    if task.notebook_task.base_parameters is None:
                        task.notebook_task.base_parameters = {}
                    task.notebook_task.base_parameters["load_mode"] = "incremental"
                    task.notebook_task.base_parameters["category"] = "ALL"

        try:
            w.jobs.update(
                job_id=job_id,
                new_settings=job_details.settings
            )
            print("    [OK] Job updated with load_mode=incremental as default")
        except Exception as e:
            print(f"    [WARN] Could not update job defaults: {e}")
            print("    Will pass parameters at runtime instead")
    else:
        print("    [OK] Job already configured for incremental mode")

    # Run the job with incremental parameters
    print(f"\n[5] Starting job run with incremental mode...")

    try:
        run = w.jobs.run_now(
            job_id=job_id,
            notebook_params={
                "load_mode": "incremental",
                "category": "ALL"
            }
        )
        run_id = run.run_id
        print(f"    [OK] Job started!")
        print(f"    Run ID: {run_id}")
        print(f"    Run URL: {creds['databricks']['host']}/#job/{job_id}/run/{run_id}")
    except Exception as e:
        print(f"    [ERROR] Failed to start job: {e}")
        return 1

    # Monitor the job
    print(f"\n[6] Monitoring job execution...")
    print("    (Press Ctrl+C to stop monitoring - job will continue running)")
    print("-" * 70)

    try:
        start_time = time.time()
        last_state = None

        while True:
            run_status = w.jobs.get_run(run_id)
            state = run_status.state.life_cycle_state
            result_state = run_status.state.result_state

            elapsed = int(time.time() - start_time)
            elapsed_str = f"{elapsed // 60}m {elapsed % 60}s"

            if state != last_state:
                print(f"    [{elapsed_str}] State: {state}")
                last_state = state

            # Check if completed
            if state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED,
                         RunLifeCycleState.INTERNAL_ERROR]:
                print(f"\n    Final State: {state}")
                if result_state:
                    print(f"    Result: {result_state}")

                # Get task results
                if run_status.tasks:
                    for task in run_status.tasks:
                        if task.state:
                            print(f"    Task '{task.task_key}': {task.state.result_state}")

                if result_state and "SUCCESS" in str(result_state):
                    print("\n    [SUCCESS] Job completed successfully!")

                    # Show output if available
                    if run_status.run_page_url:
                        print(f"\n    View full output: {run_status.run_page_url}")
                else:
                    print("\n    [FAILED] Job did not complete successfully")
                    if run_status.state.state_message:
                        print(f"    Message: {run_status.state.state_message}")

                break

            time.sleep(15)  # Check every 15 seconds

    except KeyboardInterrupt:
        print("\n\n    Monitoring stopped. Job continues running in background.")
        print(f"    Check status at: {creds['databricks']['host']}/#job/{job_id}/run/{run_id}")

    print("\n" + "=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
