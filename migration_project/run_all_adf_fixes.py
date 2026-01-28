#!/usr/bin/env python3
"""
Run All ADF Parity Fixes
========================

This script:
1. Deploys updated Bronze/Silver/Gold configurations
2. Runs Bronze job (loads weather_station_sensor)
3. Runs Silver job (creates silver_fact_weather_sensor + new columns)
4. Runs create_observation_dimensions notebook
5. Runs Gold job (weather observations + other Gold tables)

Usage:
    python run_all_adf_fixes.py
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
BRONZE_JOB_ID = 28181369160316
SILVER_JOB_ID = 181959206191493
GOLD_JOB_ID = 933934272544045

WORKSPACE_BASE = "/Workspace/migration_project/pipelines"


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


def upload_file(w, local_path, remote_path, is_notebook=False):
    """Upload a file to Databricks workspace."""
    try:
        with open(local_path, 'rb') as f:
            content = f.read()

        if is_notebook:
            w.workspace.upload(
                path=remote_path,
                content=content,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True
            )
        else:
            w.workspace.upload(
                path=remote_path,
                content=content,
                format=ImportFormat.AUTO,
                overwrite=True
            )
        return True
    except Exception as e:
        print_error(f"Failed to upload {local_path}: {e}")
        return False


def deploy_configs(w):
    """Deploy all updated configuration files."""
    print_step(1, "Deploying Updated Configurations")

    base_path = Path(__file__).parent / "pipelines"
    uploads = []

    # Bronze weather config
    uploads.append({
        "local": base_path / "timescaledb" / "config" / "timescaledb_tables_weather.yml",
        "remote": f"{WORKSPACE_BASE}/timescaledb/config/timescaledb_tables_weather.yml",
        "desc": "Bronze weather config (added weather_station_sensor)"
    })

    # Silver config
    uploads.append({
        "local": base_path / "silver" / "config" / "silver_tables.yml",
        "remote": f"{WORKSPACE_BASE}/silver/config/silver_tables.yml",
        "desc": "Silver config (weather sensor + observation dims)"
    })

    # Gold notebooks
    gold_notebooks = [
        ("gold_fact_weather_observations.py", "Weather observations (reads from Silver)"),
        ("create_observation_dimensions.py", "Observation dimensions creator"),
        ("gold_fact_reported_attendance.py", "Reported attendance (LinkedUserId fix)"),
    ]

    for notebook, desc in gold_notebooks:
        local_path = base_path / "gold" / "notebooks" / notebook
        if local_path.exists():
            uploads.append({
                "local": local_path,
                "remote": f"{WORKSPACE_BASE}/gold/notebooks/{notebook.replace('.py', '')}",
                "desc": desc,
                "is_notebook": True
            })

    # ADF helpers
    helpers_path = base_path / "gold" / "udfs" / "adf_helpers.py"
    if helpers_path.exists():
        uploads.append({
            "local": helpers_path,
            "remote": f"{WORKSPACE_BASE}/gold/udfs/adf_helpers",
            "desc": "ADF helper functions",
            "is_notebook": True
        })

    # Execute uploads
    success_count = 0
    for upload in uploads:
        if upload["local"].exists():
            is_notebook = upload.get("is_notebook", False)
            if upload_file(w, upload["local"], upload["remote"], is_notebook):
                print_ok(upload["desc"])
                success_count += 1
            else:
                print_error(f"Failed: {upload['desc']}")
        else:
            print_warn(f"File not found: {upload['local']}")

    print(f"\n   Uploaded {success_count}/{len(uploads)} files")
    return success_count > 0


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
            mins = elapsed // 60
            secs = elapsed % 60
            print(f"   Status: {state} (elapsed: {mins}m {secs}s)", end='\r')

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


def run_notebook(w, notebook_path, timeout_minutes=30):
    """Run a notebook using jobs API."""
    print(f"\n   Running notebook: {notebook_path}...")

    try:
        # Create a one-time run
        run = w.jobs.submit(
            run_name=f"ADF Fix - {Path(notebook_path).name}",
            tasks=[{
                "task_key": "main",
                "notebook_task": {
                    "notebook_path": notebook_path
                },
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 0,
                    "spark_conf": {
                        "spark.databricks.cluster.profile": "serverless"
                    }
                }
            }]
        )
        run_id = run.run_id
        print_ok(f"Started run: {run_id}")

        # Wait for completion
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60

        while True:
            run_status = w.jobs.get_run(run_id=run_id)
            state = run_status.state.life_cycle_state

            elapsed = int(time.time() - start_time)
            mins = elapsed // 60
            secs = elapsed % 60
            print(f"   Status: {state} (elapsed: {mins}m {secs}s)", end='\r')

            if state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED,
                        RunLifeCycleState.INTERNAL_ERROR]:
                print()

                result_state = run_status.state.result_state
                if result_state and result_state.value == "SUCCESS":
                    print_ok(f"Notebook completed successfully")
                    return True
                else:
                    print_error(f"Notebook failed: {result_state}")
                    return False

            if time.time() - start_time > timeout_seconds:
                print()
                print_error(f"Timeout after {timeout_minutes} minutes")
                return False

            time.sleep(10)

    except Exception as e:
        print_error(f"Failed to run notebook: {e}")
        return False


def run_bronze_job(w):
    """Run the Bronze job to load weather_station_sensor."""
    print_step(2, "Running Bronze Job (WakeCapDW_Bronze)")
    print("   This will load:")
    print("   - weather_weather_station_sensor from weather-station database")
    print("   - Other Bronze tables...")

    return run_job_and_wait(w, BRONZE_JOB_ID, "Bronze job", timeout_minutes=60)


def run_silver_job(w):
    """Run the Silver job to create silver_fact_weather_sensor."""
    print_step(3, "Running Silver Job (WakeCapDW_Silver)")
    print("   This will create:")
    print("   - silver_fact_weather_sensor")
    print("   - silver_worker.LinkedUserId")
    print("   - Other Silver tables...")

    return run_job_and_wait(w, SILVER_JOB_ID, "Silver job", timeout_minutes=45)


def run_observation_dimensions(w):
    """Run the create_observation_dimensions notebook."""
    print_step(4, "Running Observation Dimensions Notebook")
    print("   This will create:")
    print("   - silver_observation_source")
    print("   - silver_observation_status")
    print("   - silver_observation_type")
    print("   - silver_observation_severity")
    print("   - silver_observation_discriminator")
    print("   - silver_observation_clinic_violation_status")

    notebook_path = f"{WORKSPACE_BASE}/gold/notebooks/create_observation_dimensions"
    return run_notebook(w, notebook_path, timeout_minutes=15)


def run_gold_job(w):
    """Run the Gold job."""
    print_step(5, "Running Gold Job (WakeCapDW_Gold)")
    print("   This will run:")
    print("   - gold_fact_weather_observations (from Silver)")
    print("   - gold_fact_reported_attendance (with LinkedUserId fix)")
    print("   - Other Gold tables...")

    return run_job_and_wait(w, GOLD_JOB_ID, "Gold job", timeout_minutes=30)


def show_summary(results, creds):
    """Show final summary."""
    print_header("EXECUTION SUMMARY")

    db_host = creds['databricks']['host']

    print("\nJOB RESULTS:")
    for step, (name, success) in enumerate(results.items(), 1):
        status = "[OK]" if success else "[FAILED]"
        print(f"  {step}. {name}: {status}")

    all_success = all(results.values())

    print(f"\nOVERALL: {'SUCCESS' if all_success else 'PARTIAL FAILURE'}")

    print(f"""
VERIFICATION QUERIES:
  -- Check weather sensor data in Silver
  SELECT COUNT(*) FROM wakecap_prod.silver.silver_fact_weather_sensor;

  -- Check weather observations in Gold
  SELECT COUNT(*) FROM wakecap_prod.gold.gold_fact_weather_observations;

  -- Check observation dimensions
  SELECT 'source' as dim, COUNT(*) as cnt FROM wakecap_prod.silver.silver_observation_source
  UNION ALL SELECT 'status', COUNT(*) FROM wakecap_prod.silver.silver_observation_status
  UNION ALL SELECT 'type', COUNT(*) FROM wakecap_prod.silver.silver_observation_type
  UNION ALL SELECT 'severity', COUNT(*) FROM wakecap_prod.silver.silver_observation_severity
  UNION ALL SELECT 'discriminator', COUNT(*) FROM wakecap_prod.silver.silver_observation_discriminator;

  -- Check LinkedUserId in silver_worker
  SELECT COUNT(*) as total, COUNT(LinkedUserId) as with_linked_user_id
  FROM wakecap_prod.silver.silver_worker;

MONITOR JOBS:
  Bronze: {db_host}/#job/{BRONZE_JOB_ID}
  Silver: {db_host}/#job/{SILVER_JOB_ID}
  Gold: {db_host}/#job/{GOLD_JOB_ID}
""")

    return all_success


def main():
    print_header("ADF Parity Fixes - Full Deployment and Execution")
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

    results = {}

    # Step 1: Deploy configs
    results["Deploy Configurations"] = deploy_configs(w)

    # Step 2: Run Bronze job
    results["Bronze Job"] = run_bronze_job(w)

    # Step 3: Run Silver job
    if results["Bronze Job"]:
        results["Silver Job"] = run_silver_job(w)
    else:
        print_warn("Skipping Silver job due to Bronze job failure")
        results["Silver Job"] = False

    # Step 4: Run observation dimensions notebook
    if results["Silver Job"]:
        results["Observation Dimensions"] = run_observation_dimensions(w)
    else:
        print_warn("Skipping Observation Dimensions due to Silver job failure")
        results["Observation Dimensions"] = False

    # Step 5: Run Gold job
    if results["Silver Job"]:
        results["Gold Job"] = run_gold_job(w)
    else:
        print_warn("Skipping Gold job due to Silver job failure")
        results["Gold Job"] = False

    # Show summary
    success = show_summary(results, creds)

    print(f"\nCompleted at: {datetime.now()}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
