#!/usr/bin/env python3
"""Quick check of gold_fact_workers_history table."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, State


def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_sql(w, sql, warehouse_id):
    statement = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="0s"
    )
    statement_id = statement.statement_id

    for _ in range(60):
        status = w.statement_execution.get_statement(statement_id)
        if status.status.state == StatementState.SUCCEEDED:
            if status.result and status.result.data_array:
                return status.result.data_array
            return []
        elif status.status.state in [StatementState.FAILED, StatementState.CANCELED]:
            raise Exception(f"Failed: {status.status.error}")
        time.sleep(2)
    return []


def main():
    creds = load_credentials()
    db = creds['databricks']
    w = WorkspaceClient(host=db['host'], token=db['token'])

    # Find running warehouse or start one
    warehouse_id = None
    for wh in w.warehouses.list():
        print(f"Warehouse {wh.name}: {wh.state}")
        if wh.state == State.RUNNING:
            warehouse_id = wh.id
            break
        elif "serverless" in wh.name.lower() and warehouse_id is None:
            warehouse_id = wh.id

    if not warehouse_id:
        warehouse_id = list(w.warehouses.list())[0].id

    # Check if running, if not start
    wh = w.warehouses.get(warehouse_id)
    if wh.state != State.RUNNING:
        print(f"\nStarting warehouse {wh.name}...")
        w.warehouses.start(warehouse_id)

        # Wait for startup
        for _ in range(60):
            wh = w.warehouses.get(warehouse_id)
            print(f"\r   State: {wh.state}", end="", flush=True)
            if wh.state == State.RUNNING:
                print()
                break
            time.sleep(5)
        else:
            print("\nWarehouse did not start!")
            return 1

    print(f"\nUsing warehouse: {warehouse_id}\n")

    # Check if table exists and get basic info
    queries = [
        ("Table exists check", "DESCRIBE TABLE wakecap_prod.gold.gold_fact_workers_history"),
        ("Total row count", "SELECT COUNT(*) FROM wakecap_prod.gold.gold_fact_workers_history"),
        ("Date range", "SELECT MIN(ShiftLocalDate), MAX(ShiftLocalDate) FROM wakecap_prod.gold.gold_fact_workers_history"),
        ("Latest 5 dates", """
            SELECT ShiftLocalDate, COUNT(*) as cnt
            FROM wakecap_prod.gold.gold_fact_workers_history
            GROUP BY ShiftLocalDate
            ORDER BY ShiftLocalDate DESC
            LIMIT 5
        """),
    ]

    for name, sql in queries:
        print(f"{name}:")
        try:
            rows = run_sql(w, sql, warehouse_id)
            if rows:
                for row in rows[:10]:
                    print(f"   {row}")
            else:
                print("   (no results)")
        except Exception as e:
            print(f"   ERROR: {e}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
