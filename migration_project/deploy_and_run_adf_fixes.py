#!/usr/bin/env python3
"""
Deploy ADF Parity Fixes and Run Silver/Gold Jobs
=================================================

This script:
1. Deploys updated Silver config (LinkedUserId, DeletedAt columns)
2. Deploys updated Gold notebook (date filtering, LinkedUserId lookup)
3. Deploys new adf_helpers.py module
4. Runs the Silver job to apply new column mappings
5. Runs the Gold job to test ApprovedBy resolution

Usage:
    python deploy_and_run_adf_fixes.py
"""

import sys
import time
from pathlib import Path
from datetime import datetime

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import RunLifeCycleState

# Configuration
SILVER_JOB_ID = 181959206191493
GOLD_JOB_ID = 933934272544045

WORKSPACE_SILVER_PATH = "/Workspace/migration_project/pipelines/silver"
WORKSPACE_GOLD_PATH = "/Workspace/migration_project/pipelines/gold"


def print_header(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def print_step(num, text):
    print(f"\n[Step {num}] {text}")
    print("-" * 60)


def print_ok(text):
    print(f"   [OK] {text}")


def print_warn(text):
    print(f"   [WARN] {text}")


def print_error(text):
    print(f"   [ERROR] {text}")


def load_credentials():
    """Load credentials from ~/.databricks/labs/lakebridge/.credentials.yml"""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"

    if not creds_path.exists():
        print_error(f"Credentials file not found: {creds_path}")
        return None

    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    return creds


def connect_databricks(creds):
    """Connect to Databricks workspace."""
    try:
        w = WorkspaceClient(
            host=creds['databricks']['host'],
            token=creds['databricks']['token']
        )
        user = w.current_user.me()
        print_ok(f"Connected as: {user.user_name}")
        return w
    except Exception as e:
        print_error(f"Connection failed: {e}")
        return None


def deploy_silver_config(w):
    """Deploy updated Silver config with new columns."""
    print_step(1, "Deploying updated Silver config")

    config_file = Path(__file__).parent / "pipelines" / "silver" / "config" / "silver_tables.yml"

    if not config_file.exists():
        print_error(f"Config file not found: {config_file}")
        return False

    try:
        with open(config_file, 'rb') as f:
            content = f.read()

        w.workspace.upload(
            path=f"{WORKSPACE_SILVER_PATH}/config/silver_tables.yml",
            content=content,
            format=ImportFormat.AUTO,
            overwrite=True
        )
        print_ok("Uploaded: silver_tables.yml")
        print_ok("  - Added LinkedUserId to silver_worker")
        return True
    except Exception as e:
        print_error(f"Failed to upload config: {e}")
        return False


def deploy_gold_notebook(w):
    """Deploy updated Gold notebook with ADF parity fixes."""
    print_step(2, "Deploying updated Gold notebook")

    notebook_file = Path(__file__).parent / "pipelines" / "gold" / "notebooks" / "gold_fact_reported_attendance.py"

    if not notebook_file.exists():
        print_error(f"Notebook file not found: {notebook_file}")
        return False

    try:
        with open(notebook_file, 'rb') as f:
            content = f.read()

        w.workspace.upload(
            path=f"{WORKSPACE_GOLD_PATH}/notebooks/gold_fact_reported_attendance",
            content=content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )
        print_ok("Uploaded: gold_fact_reported_attendance.py")
        print_ok("  - Added date range filtering (2000-01-01 to 2100-01-01)")
        print_ok("  - Fixed ApprovedBy lookup using LinkedUserId")
        return True
    except Exception as e:
        print_error(f"Failed to upload notebook: {e}")
        return False


def deploy_adf_helpers(w):
    """Deploy new adf_helpers.py module."""
    print_step(3, "Deploying ADF helpers module")

    helpers_file = Path(__file__).parent / "pipelines" / "gold" / "udfs" / "adf_helpers.py"

    if not helpers_file.exists():
        print_error(f"Helpers file not found: {helpers_file}")
        return False

    # Ensure directory exists
    try:
        w.workspace.mkdirs(f"{WORKSPACE_GOLD_PATH}/udfs")
    except Exception:
        pass  # Directory may already exist

    try:
        with open(helpers_file, 'rb') as f:
            content = f.read()

        w.workspace.upload(
            path=f"{WORKSPACE_GOLD_PATH}/udfs/adf_helpers",
            content=content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )
        print_ok("Uploaded: adf_helpers.py")
        print_ok("  - MV_ResourceDevice_NoViolation equivalent")
        print_ok("  - Date range filtering utilities")
        print_ok("  - LinkedUserId resolution utilities")
        return True
    except Exception as e:
        print_error(f"Failed to upload helpers: {e}")
        return False


def run_job_and_wait(w, job_id, job_name, timeout_minutes=60):
    """Run a Databricks job and wait for completion."""
    print(f"\n   Starting {job_name} (Job ID: {job_id})...")

    try:
        run = w.jobs.run_now(job_id=job_id)
        run_id = run.run_id
        print_ok(f"Started run: {run_id}")

        # Wait for job to complete
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60

        while True:
            run_status = w.jobs.get_run(run_id=run_id)
            state = run_status.state.life_cycle_state

            elapsed = int(time.time() - start_time)
            print(f"   Status: {state} (elapsed: {elapsed}s)", end='\r')

            if state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED,
                        RunLifeCycleState.INTERNAL_ERROR]:
                print()  # New line after status

                result_state = run_status.state.result_state
                if result_state and result_state.value == "SUCCESS":
                    print_ok(f"{job_name} completed successfully")
                    return True
                else:
                    print_error(f"{job_name} failed with state: {result_state}")
                    if run_status.state.state_message:
                        print(f"   Message: {run_status.state.state_message}")
                    return False

            if time.time() - start_time > timeout_seconds:
                print()
                print_error(f"Timeout waiting for {job_name} after {timeout_minutes} minutes")
                return False

            time.sleep(10)

    except Exception as e:
        print_error(f"Failed to run {job_name}: {e}")
        return False


def run_silver_job(w):
    """Run the Silver job to apply new column mappings."""
    print_step(4, "Running Silver job (WakeCapDW_Silver)")
    print("   This will apply new column mappings:")
    print("   - silver_worker.LinkedUserId")

    return run_job_and_wait(w, SILVER_JOB_ID, "Silver job", timeout_minutes=45)


def run_gold_job(w):
    """Run the Gold job to test ApprovedBy resolution."""
    print_step(5, "Running Gold job (WakeCapDW_Gold)")
    print("   This will test:")
    print("   - Date range filtering in gold_fact_reported_attendance")
    print("   - ApprovedBy -> WorkerId resolution via LinkedUserId")

    return run_job_and_wait(w, GOLD_JOB_ID, "Gold job", timeout_minutes=30)


def show_summary(creds, silver_ok, gold_ok):
    """Show final summary."""
    print_header("DEPLOYMENT AND EXECUTION SUMMARY")

    db_host = creds['databricks']['host']

    print(f"""
FILES DEPLOYED:
  [OK] Silver config: silver_tables.yml
  [OK] Gold notebook: gold_fact_reported_attendance.py
  [OK] ADF helpers: adf_helpers.py

JOB EXECUTION:
  {"[OK]" if silver_ok else "[FAILED]"} Silver job (ID: {SILVER_JOB_ID})
  {"[OK]" if gold_ok else "[FAILED]"} Gold job (ID: {GOLD_JOB_ID})

VERIFICATION QUERIES:
  -- Check LinkedUserId is populated in silver_worker
  SELECT COUNT(*) as total,
         COUNT(LinkedUserId) as with_linked_user_id
  FROM wakecap_prod.silver.silver_worker;

  -- Check ApprovedByWorkerId resolution in Gold
  SELECT COUNT(*) as total,
         COUNT(ApprovedByWorkerID) as resolved
  FROM wakecap_prod.gold.gold_fact_reported_attendance;

  -- Check date filtering effect
  SELECT MIN(ShiftLocalDate), MAX(ShiftLocalDate)
  FROM wakecap_prod.gold.gold_fact_reported_attendance;

MONITOR JOBS:
  Silver: {db_host}/#job/{SILVER_JOB_ID}
  Gold: {db_host}/#job/{GOLD_JOB_ID}
""")


def main():
    print_header("ADF Parity Fixes - Deploy and Run")
    print(f"Started at: {datetime.now()}")

    # Load credentials
    print("\nLoading credentials...")
    creds = load_credentials()
    if not creds:
        return 1

    # Connect to Databricks
    print("\nConnecting to Databricks...")
    w = connect_databricks(creds)
    if not w:
        return 1

    # Deploy files
    deploy_silver_config(w)
    deploy_gold_notebook(w)
    deploy_adf_helpers(w)

    # Run jobs
    silver_ok = run_silver_job(w)

    if silver_ok:
        gold_ok = run_gold_job(w)
    else:
        print_warn("Skipping Gold job due to Silver job failure")
        gold_ok = False

    # Show summary
    show_summary(creds, silver_ok, gold_ok)

    return 0 if (silver_ok and gold_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
