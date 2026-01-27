#!/usr/bin/env python3
"""Quick watermark check."""

import yaml
import time
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

    # Find running warehouse or start one
    warehouse_id = None
    for wh in w.warehouses.list():
        if "serverless" in wh.name.lower():
            if wh.state and wh.state.value == "RUNNING":
                warehouse_id = wh.id
                break
            elif wh.state and wh.state.value == "STOPPED":
                print(f"Starting warehouse {wh.name}...")
                w.warehouses.start(wh.id)
                for _ in range(12):
                    time.sleep(5)
                    status = w.warehouses.get(wh.id)
                    if status.state and status.state.value == "RUNNING":
                        warehouse_id = wh.id
                        break
                break

    if not warehouse_id:
        print("No warehouse available")
        return

    # Check recent watermarks
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="""
            SELECT source_table,
                   high_watermark,
                   CAST(updated_at AS STRING) as updated
            FROM wakecap_prod.migration._timescaledb_watermarks
            WHERE source_system = 'timescaledb'
            ORDER BY updated_at DESC
            LIMIT 15
        """,
        wait_timeout="50s"
    )

    if result.result and result.result.data_array:
        print("Most recently updated tables:")
        print("-" * 60)
        for row in result.result.data_array:
            print(f"{row[0]:40} | {row[2]}")
    else:
        print("No watermark data found")

if __name__ == "__main__":
    main()
