#!/usr/bin/env python3
"""Check table counts and watermarks."""

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

    # Find running warehouse
    warehouse_id = None
    for wh in w.warehouses.list():
        if "serverless" in wh.name.lower():
            if wh.state and wh.state.value == "RUNNING":
                warehouse_id = wh.id
                print(f"Using warehouse: {wh.name}")
                break
            elif wh.state and wh.state.value == "STOPPED":
                print(f"Starting warehouse {wh.name}...")
                w.warehouses.start(wh.id)
                for _ in range(12):
                    time.sleep(5)
                    status = w.warehouses.get(wh.id)
                    if status.state and status.state.value == "RUNNING":
                        warehouse_id = wh.id
                        print("Warehouse started")
                        break
                break

    if not warehouse_id:
        print("No warehouse available")
        return

    # Check watermarks table exists
    print("\n--- Checking watermarks table ---")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SELECT COUNT(*) as cnt FROM wakecap_prod.migration._timescaledb_watermarks",
        wait_timeout="30s"
    )
    if result.result and result.result.data_array:
        print(f"Watermarks count: {result.result.data_array[0][0]}")

    # Check recent tables
    print("\n--- Recently modified tables in raw schema ---")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="""
            SHOW TABLES IN wakecap_prod.raw LIKE 'timescale_*'
        """,
        wait_timeout="30s"
    )
    if result.result and result.result.data_array:
        print(f"Found {len(result.result.data_array)} timescale tables")

    # Get count of a few key tables to verify data is loading
    print("\n--- Sample table row counts ---")
    tables = ["timescale_worker", "timescale_project", "timescale_company", "timescale_devicelocation"]
    for table in tables:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"SELECT COUNT(*) FROM wakecap_prod.raw.{table}",
                wait_timeout="30s"
            )
            if result.result and result.result.data_array:
                cnt = result.result.data_array[0][0]
                print(f"  {table}: {cnt} rows")
        except Exception as e:
            print(f"  {table}: Error - {str(e)[:50]}")

if __name__ == "__main__":
    main()
