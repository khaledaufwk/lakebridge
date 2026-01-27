#!/usr/bin/env python3
"""Check notebook output to see current table being processed."""

import yaml
from pathlib import Path
from datetime import datetime
from databricks.sdk import WorkspaceClient

def main():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    run_id = 275031301341569

    # Get run details
    run = w.jobs.get_run(run_id)

    print(f"Run ID: {run_id}")
    print(f"Status: {run.state.life_cycle_state.value if run.state.life_cycle_state else 'N/A'}")

    # Check each task
    if run.tasks:
        for task in run.tasks:
            print(f"\n--- Task: {task.task_key} ---")
            ts = task.state
            if ts:
                print(f"State: {ts.life_cycle_state.value if ts.life_cycle_state else 'N/A'}")
                print(f"Result: {ts.result_state.value if ts.result_state else 'N/A'}")

            # Get cluster run info
            if task.cluster_instance:
                cluster_id = task.cluster_instance.cluster_id
                print(f"Cluster: {cluster_id}")

            # Try to get task output
            if task.run_id:
                try:
                    output = w.jobs.get_run_output(task.run_id)
                    if output.notebook_output and output.notebook_output.result:
                        result = output.notebook_output.result
                        # Show last 500 chars
                        if len(result) > 500:
                            print(f"Output (last 500 chars): ...{result[-500:]}")
                        else:
                            print(f"Output: {result}")
                    if output.error:
                        print(f"Error: {output.error}")
                    if output.error_trace:
                        print(f"Trace: {output.error_trace[:300]}")
                except Exception as e:
                    print(f"Could not get output: {e}")

    # Also check cluster activity via recent jobs API
    print("\n--- Recent Activity Check ---")

    # Query watermarks to see latest updates
    warehouse_id = None
    for wh in w.warehouses.list():
        if "serverless" in wh.name.lower() and wh.state and wh.state.value == "RUNNING":
            warehouse_id = wh.id
            break

    if warehouse_id:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement="""
                    SELECT source_table, high_watermark, updated_at
                    FROM wakecap_prod.migration._timescaledb_watermarks
                    WHERE source_system = 'timescaledb'
                    ORDER BY updated_at DESC
                    LIMIT 10
                """,
                wait_timeout="30s"
            )
            if result.result and result.result.data_array:
                print("\nMost recently updated watermarks:")
                for row in result.result.data_array:
                    print(f"  {row[0]}: {row[1]} (updated: {row[2]})")
        except Exception as e:
            print(f"Could not query watermarks: {e}")

if __name__ == "__main__":
    main()
