#!/usr/bin/env python3
"""Check what the current job is doing."""
import yaml
from pathlib import Path
from databricks.sdk import WorkspaceClient

def main():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)
    
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    
    run_id = 270021266706721
    
    print("=" * 100)
    print("CURRENT RUN ACTIVITY")
    print("=" * 100)
    
    # Get run details
    run = w.jobs.get_run(run_id)
    
    # Get cluster ID from the running task
    cluster_id = None
    for task in run.tasks:
        if task.state and task.state.life_cycle_state and task.state.life_cycle_state.value == "RUNNING":
            if task.cluster_instance:
                cluster_id = task.cluster_instance.cluster_id
                print(f"\nRunning task: {task.task_key}")
                print(f"Cluster ID: {cluster_id}")
                break
    
    if cluster_id:
        # Get cluster details
        try:
            cluster = w.clusters.get(cluster_id)
            print(f"\nCluster: {cluster.cluster_name}")
            print(f"State: {cluster.state.value if cluster.state else 'N/A'}")
            print(f"Spark Version: {cluster.spark_version}")
            print(f"Node Type: {cluster.node_type_id}")
            print(f"Workers: {cluster.num_workers if cluster.num_workers else 'Autoscaling'}")
            
            # Get driver logs (if available)
            print("\n" + "-" * 100)
            print("DRIVER LOG (recent)")
            print("-" * 100)
            try:
                # Get Spark UI info
                spark_context_id = cluster.spark_context_id
                print(f"Spark Context ID: {spark_context_id}")
            except Exception as e:
                print(f"No Spark context info: {e}")
                
        except Exception as e:
            print(f"Error getting cluster: {e}")
    
    # Check for tables currently being processed
    print("\n" + "=" * 100)
    print("TABLES WITH 'running' STATUS IN WATERMARKS")
    print("=" * 100)
    
    warehouses = list(w.warehouses.list())
    warehouse_id = None
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            warehouse_id = wh.id
            break
    
    if warehouse_id:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement="""
                SELECT 
                    source_table,
                    last_load_status,
                    last_load_start_time,
                    ROUND(TIMESTAMPDIFF(MINUTE, last_load_start_time, current_timestamp()), 1) as minutes_running,
                    last_error_message
                FROM wakecap_prod.migration._timescaledb_watermarks
                WHERE source_system = 'timescaledb'
                  AND (last_load_status = 'running' 
                       OR last_load_start_time > '2026-01-23 11:00:00')
                ORDER BY last_load_start_time DESC
            """,
            wait_timeout="30s"
        )
        
        if result.result and result.result.data_array:
            print(f"\n{'Table':<35} {'Status':<12} {'Start Time':<20} {'Running (min)':<15} Error")
            print("-" * 120)
            for row in result.result.data_array:
                print(f"{row[0] or '':<35} {row[1] or '':<12} {str(row[2])[:19]:<20} {row[3] or 0:<15} {row[4][:40] if row[4] else ''}")
        else:
            print("No tables currently in 'running' status or started today")
    
    # Get full error details for failed tables
    print("\n" + "=" * 100)
    print("FAILED TABLES - FULL ERROR MESSAGES")
    print("=" * 100)
    
    if warehouse_id:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement="""
                SELECT 
                    source_table,
                    last_error_message,
                    last_load_start_time
                FROM wakecap_prod.migration._timescaledb_watermarks
                WHERE source_system = 'timescaledb'
                  AND last_load_status = 'failed'
                ORDER BY last_load_start_time DESC
            """,
            wait_timeout="30s"
        )
        
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                print(f"\n{row[0]}:")
                print(f"  Time: {row[2]}")
                print(f"  Error: {row[1]}")

if __name__ == "__main__":
    main()
