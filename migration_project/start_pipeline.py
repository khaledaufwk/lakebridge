#!/usr/bin/env python3
"""
Start the DLT Pipeline and monitor its status.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient

PIPELINE_ID = "bc59257f-73aa-42ed-9fc6-01a6c14dbb0a"


def main():
    print("=" * 60)
    print("Start WakeCapDW Migration Pipeline")
    print("=" * 60)

    # Load credentials
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    # Connect to Databricks
    print("\n[1] Connecting to Databricks...")
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    user = w.current_user.me()
    print(f"    Connected as: {user.user_name}")

    # Check current pipeline status
    print(f"\n[2] Checking pipeline status...")
    pipeline = w.pipelines.get(pipeline_id=PIPELINE_ID)
    print(f"    Name: {pipeline.name}")
    print(f"    State: {pipeline.state}")

    # Start the pipeline
    print(f"\n[3] Starting pipeline...")
    try:
        response = w.pipelines.start_update(pipeline_id=PIPELINE_ID)
        update_id = response.update_id
        print(f"    [OK] Pipeline started. Update ID: {update_id}")
    except Exception as e:
        print(f"    [ERROR] Failed to start pipeline: {e}")
        return 1

    # Monitor for a short period
    print(f"\n[4] Monitoring pipeline (30 seconds)...")
    for i in range(6):
        time.sleep(5)
        pipeline = w.pipelines.get(pipeline_id=PIPELINE_ID)
        state = pipeline.state

        # Get latest update status
        if pipeline.latest_updates:
            latest = pipeline.latest_updates[0]
            print(f"    [{i*5+5}s] Pipeline State: {state}, Update: {latest.state}")
        else:
            print(f"    [{i*5+5}s] Pipeline State: {state}")

    # Final status
    print(f"\n[5] Current Status:")
    pipeline = w.pipelines.get(pipeline_id=PIPELINE_ID)
    print(f"    State: {pipeline.state}")

    if pipeline.latest_updates:
        latest = pipeline.latest_updates[0]
        print(f"    Latest Update ID: {latest.update_id}")
        print(f"    Latest Update State: {latest.state}")
        if latest.creation_time:
            print(f"    Started: {latest.creation_time}")

    print(f"\n[6] Pipeline URL: {creds['databricks']['host']}/pipelines/{PIPELINE_ID}")
    print("\n    Monitor the pipeline in the Databricks UI for detailed status.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
