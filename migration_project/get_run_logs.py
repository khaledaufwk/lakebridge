#!/usr/bin/env python3
"""Get detailed run logs and watermark data."""
import yaml
from pathlib import Path
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient

def main():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)
    
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    
    run_id = 270021266706721  # Current running job
    
    print("=" * 80)
    print("CURRENT RUN DETAILS")
    print("=" * 80)
    
    try:
        run = w.jobs.get_run(run_id)
        
        # Get cluster info
        if run.cluster_instance:
            print(f"\nCluster: {run.cluster_instance.cluster_id}")
        
        # Get task details
        if run.tasks:
            for task in run.tasks:
                print(f"\nTask: {task.task_key}")
                if task.state:
                    print(f"  State: {task.state.life_cycle_state.value if task.state.life_cycle_state else 'N/A'}")
                
                # Get notebook run details
                if task.notebook_task:
                    print(f"  Notebook: {task.notebook_task.notebook_path}")
                    if task.notebook_task.base_parameters:
                        print(f"  Parameters: {task.notebook_task.base_parameters}")
                
                # Duration
                if task.start_time and task.end_time:
                    duration = (task.end_time - task.start_time) / 1000 / 60
                    print(f"  Duration: {duration:.1f} minutes")
                elif task.start_time:
                    start = datetime.fromtimestamp(task.start_time / 1000, tz=timezone.utc)
                    duration = (datetime.now(timezone.utc) - start).total_seconds() / 60
                    print(f"  Running for: {duration:.1f} minutes")
    except Exception as e:
        print(f"Error: {e}")
    
    # Query watermarks to see current state
    print("\n" + "=" * 80)
    print("WATERMARK STATUS (via SQL)")
    print("=" * 80)
    
    try:
        # Use Databricks SQL to query watermarks
        result = w.statement_execution.execute_statement(
            warehouse_id="d1b49c0f3cf44bd2",  # Serverless warehouse
            statement="""
                SELECT 
                    source_table,
                    last_load_status,
                    last_load_row_count,
                    ROUND(TIMESTAMPDIFF(MINUTE, last_load_start_time, last_load_end_time), 1) as load_duration_min,
                    last_watermark_timestamp,
                    last_load_end_time
                FROM wakecap_prod.migration._timescaledb_watermarks
                WHERE source_system = 'timescaledb'
                ORDER BY last_load_end_time DESC
                LIMIT 20
            """,
            wait_timeout="30s"
        )
        
        if result.result and result.result.data_array:
            print(f"\nRecently loaded tables:")
            print(f"{'Table':<35} {'Status':<10} {'Rows':<12} {'Duration':<10} {'Last Load'}")
            print("-" * 100)
            for row in result.result.data_array:
                table = row[0] or ''
                status = row[1] or ''
                rows = row[2] or 0
                duration = row[3] or 0
                last_load = row[5] or ''
                print(f"{table:<35} {status:<10} {rows:<12} {duration:<10} {last_load}")
    except Exception as e:
        print(f"SQL Error: {e}")

if __name__ == "__main__":
    main()
