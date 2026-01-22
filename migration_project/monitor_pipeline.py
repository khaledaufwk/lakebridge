#!/usr/bin/env python3
"""
Monitor DLT pipeline status and display results.
"""
import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient


def load_databricks_config():
    """Load Databricks credentials."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)['databricks']


def monitor_pipeline(pipeline_id: str, poll_interval: int = 30, max_wait: int = 600):
    """Monitor pipeline until completion or timeout."""
    
    db_config = load_databricks_config()
    w = WorkspaceClient(host=db_config['host'], token=db_config['token'])
    
    print("=" * 60)
    print("DLT PIPELINE MONITOR")
    print("=" * 60)
    print(f"Pipeline ID: {pipeline_id}")
    print(f"Poll Interval: {poll_interval}s")
    print(f"Max Wait: {max_wait}s")
    print(f"URL: {db_config['host']}/pipelines/{pipeline_id}")
    print("=" * 60)
    
    start_time = time.time()
    last_state = None
    
    while True:
        elapsed = time.time() - start_time
        
        try:
            pipeline = w.pipelines.get(pipeline_id)
            state = pipeline.state.value if pipeline.state else "UNKNOWN"
            
            # Get latest update
            updates = list(w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=1))
            if updates:
                update = updates[0]
                update_state = update.state.value if update.state else "UNKNOWN"
            else:
                update_state = "NO_UPDATES"
            
            # Print status if changed
            if state != last_state:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"\n[{timestamp}] Pipeline State: {state}, Update: {update_state}")
                last_state = state
            else:
                print(".", end="", flush=True)
            
            # Check terminal states
            if state == "IDLE" and update_state in ["COMPLETED", "FAILED", "CANCELED"]:
                print(f"\n\nPipeline finished with state: {update_state}")
                
                # Get summary
                if update_state == "COMPLETED":
                    print("\n[SUCCESS] Pipeline completed successfully!")
                    
                    # List created tables
                    print("\nCreated tables:")
                    try:
                        datasets = list(w.pipelines.list_pipeline_events(
                            pipeline_id=pipeline_id,
                            filter="event_type = 'create_update'",
                            max_results=50
                        ))
                        for ds in datasets:
                            if ds.maturation_level:
                                print(f"  - {ds.dataset_name or 'unknown'}")
                    except:
                        print("  (Could not list tables)")
                        
                elif update_state == "FAILED":
                    print("\n[FAILED] Pipeline failed!")
                    
                    # Get error messages
                    print("\nError details:")
                    try:
                        events = list(w.pipelines.list_pipeline_events(
                            pipeline_id=pipeline_id,
                            filter="level = 'ERROR'",
                            max_results=10
                        ))
                        for event in events:
                            if event.message:
                                print(f"  - {event.message[:200]}")
                    except:
                        print("  (Could not retrieve error details)")
                
                break
            
            # Check timeout
            if elapsed > max_wait:
                print(f"\n\n[TIMEOUT] Max wait time ({max_wait}s) exceeded")
                print(f"Pipeline is still in state: {state}")
                print("Continue monitoring at the URL above")
                break
                
        except Exception as e:
            print(f"\nError checking status: {e}")
        
        time.sleep(poll_interval)
    
    print("\n" + "=" * 60)
    print(f"Total elapsed time: {int(elapsed)}s")
    print(f"Pipeline URL: {db_config['host']}/pipelines/{pipeline_id}")
    print("=" * 60)
    
    return state, update_state


def main():
    pipeline_id = "bc59257f-73aa-42ed-9fc6-01a6c14dbb0a"
    
    # Monitor with 30 second poll, 10 minute max wait
    final_state, update_state = monitor_pipeline(
        pipeline_id, 
        poll_interval=30, 
        max_wait=600
    )
    
    # Return exit code based on result
    if update_state == "COMPLETED":
        return 0
    elif update_state == "FAILED":
        return 1
    else:
        return 2  # Timeout or unknown


if __name__ == "__main__":
    sys.exit(main())
