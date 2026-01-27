#!/usr/bin/env python3
"""Wait for warehouse and run validation."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
import pymssql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, State


def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def wait_for_warehouse(w, warehouse_id, timeout=600):
    """Wait for warehouse to be running, starting it if needed."""
    print("Checking SQL Warehouse state...", flush=True)

    # Check initial state
    wh = w.warehouses.get(warehouse_id)
    if wh.state == State.STOPPED:
        print(f"   Warehouse is stopped. Starting...", flush=True)
        w.warehouses.start(warehouse_id)

    print("Waiting for SQL Warehouse to be RUNNING...", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        wh = w.warehouses.get(warehouse_id)
        print(f"\r   State: {wh.state} ({int(time.time()-start)}s)", end="", flush=True)
        if wh.state == State.RUNNING:
            print(flush=True)
            return True
        elif wh.state == State.STOPPED:
            # Try starting again
            print(f"\n   Still stopped, starting again...", flush=True)
            w.warehouses.start(warehouse_id)
        time.sleep(10)
    print(flush=True)
    return False


def run_databricks_sql(w, sql, warehouse_id):
    """Execute SQL on Databricks with polling."""
    statement = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="0s"
    )

    statement_id = statement.statement_id
    start = time.time()

    for i in range(180):
        status = w.statement_execution.get_statement(statement_id)
        state = status.status.state

        if i % 12 == 0:  # Print every minute
            print(f"\r   Query running ({int(time.time()-start)}s)...", end="", flush=True)

        if state == StatementState.SUCCEEDED:
            print(flush=True)
            if status.result and status.result.data_array:
                return status.result.data_array
            return []
        elif state in [StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED]:
            print(flush=True)
            raise Exception(f"Failed: {status.status.error}")

        time.sleep(5)

    print(flush=True)
    raise Exception("Query timeout after 15 minutes")


def main():
    print("=" * 65)
    print("GOLD FACTWORKERSHISTORY VALIDATION")
    print("SQL Server vs Databricks (through Jan 23, 2025)")
    print("=" * 65)

    creds = load_credentials()
    target_date = "2025-01-23"

    # Connect to SQL Server
    print("\n1. Connecting to SQL Server...", flush=True)
    mssql = creds['mssql']
    sql_conn = pymssql.connect(
        server=mssql['server'],
        user=mssql['user'],
        password=mssql['password'],
        database=mssql['database'],
        port=mssql.get('port', 1433)
    )
    sql_cursor = sql_conn.cursor()
    print("   Connected", flush=True)

    # Connect to Databricks
    print("\n2. Connecting to Databricks...", flush=True)
    db = creds['databricks']
    w = WorkspaceClient(host=db['host'], token=db['token'])
    warehouses = list(w.warehouses.list())
    # Prefer serverless warehouse
    warehouse = None
    for wh in warehouses:
        if "serverless" in wh.name.lower():
            warehouse = wh
            break
    if not warehouse:
        warehouse = warehouses[0]
    warehouse_id = warehouse.id
    print(f"   Using warehouse: {warehouse.name} ({warehouse.state})", flush=True)

    # Wait for warehouse to start
    if not wait_for_warehouse(w, warehouse_id):
        print("   ERROR: Warehouse did not start in time")
        return 1

    # Query SQL Server
    print(f"\n3. Querying SQL Server (Jan 23)...", flush=True)
    sql_cursor.execute(f"""
        SELECT
            COUNT(*) as cnt,
            COUNT(DISTINCT WorkerId) as workers,
            COUNT(DISTINCT ProjectId) as projects,
            SUM(CAST(ActiveTime AS FLOAT)) as active_time,
            SUM(CAST(InactiveTime AS FLOAT)) as inactive_time
        FROM dbo.FactWorkersHistory WITH (NOLOCK)
        WHERE ShiftLocalDate = '{target_date}'
    """)
    sql_row = sql_cursor.fetchone()
    sql_data = {
        'count': sql_row[0],
        'workers': sql_row[1],
        'projects': sql_row[2],
        'active_time': float(sql_row[3]) if sql_row[3] else 0,
        'inactive_time': float(sql_row[4]) if sql_row[4] else 0
    }
    print(f"   Count: {sql_data['count']:,}", flush=True)
    print(f"   Workers: {sql_data['workers']:,}, Projects: {sql_data['projects']:,}", flush=True)

    # Query Databricks
    print(f"\n4. Querying Databricks (Jan 23)...", flush=True)
    rows = run_databricks_sql(w, f"""
        SELECT
            COUNT(*) as cnt,
            COUNT(DISTINCT WorkerId) as workers,
            COUNT(DISTINCT ProjectId) as projects,
            SUM(ActiveTime) as active_time,
            SUM(InactiveTime) as inactive_time
        FROM wakecap_prod.gold.gold_fact_workers_history
        WHERE ShiftLocalDate = '{target_date}'
    """, warehouse_id)

    db_data = {
        'count': int(rows[0][0]) if rows else 0,
        'workers': int(rows[0][1]) if rows else 0,
        'projects': int(rows[0][2]) if rows else 0,
        'active_time': float(rows[0][3]) if rows and rows[0][3] else 0,
        'inactive_time': float(rows[0][4]) if rows and rows[0][4] else 0
    }
    print(f"   Count: {db_data['count']:,}", flush=True)
    print(f"   Workers: {db_data['workers']:,}, Projects: {db_data['projects']:,}", flush=True)

    # Comparison
    print("\n" + "=" * 65)
    print("COMPARISON: January 23, 2025")
    print("=" * 65)

    def calc_match(a, b):
        return (min(a, b) / max(a, b) * 100) if max(a, b) > 0 else 100

    metrics = [
        ('Row Count', sql_data['count'], db_data['count']),
        ('Workers', sql_data['workers'], db_data['workers']),
        ('Projects', sql_data['projects'], db_data['projects']),
        ('ActiveTime', sql_data['active_time'], db_data['active_time']),
        ('InactiveTime', sql_data['inactive_time'], db_data['inactive_time']),
    ]

    print(f"\n{'Metric':<15} {'SQL Server':>15} {'Databricks':>15} {'Diff':>12} {'Match':>8}")
    print("-" * 67)

    min_match = 100
    for name, sql_val, db_val in metrics:
        diff = db_val - sql_val
        match = calc_match(sql_val, db_val)
        min_match = min(min_match, match)

        if isinstance(sql_val, float):
            print(f"{name:<15} {sql_val:>15,.4f} {db_val:>15,.4f} {diff:>+12,.4f} {match:>7.2f}%")
        else:
            print(f"{name:<15} {sql_val:>15,} {db_val:>15,} {diff:>+12,} {match:>7.2f}%")

    # Additional dates
    print("\n" + "=" * 65)
    print("COMPARISON: Multiple Dates (Jan 20-23)")
    print("=" * 65)

    dates = ["2025-01-20", "2025-01-21", "2025-01-22", "2025-01-23"]

    print(f"\n{'Date':<12} {'SQL Server':>12} {'Databricks':>12} {'Diff':>10} {'Match':>8}")
    print("-" * 55)

    for dt in dates:
        # SQL Server
        sql_cursor.execute(f"""
            SELECT COUNT(*) FROM dbo.FactWorkersHistory WITH (NOLOCK)
            WHERE ShiftLocalDate = '{dt}'
        """)
        sql_cnt = sql_cursor.fetchone()[0]

        # Databricks
        rows = run_databricks_sql(w, f"""
            SELECT COUNT(*) FROM wakecap_prod.gold.gold_fact_workers_history
            WHERE ShiftLocalDate = '{dt}'
        """, warehouse_id)
        db_cnt = int(rows[0][0]) if rows else 0

        diff = db_cnt - sql_cnt
        match = calc_match(sql_cnt, db_cnt)
        min_match = min(min_match, match)

        marker = "" if match >= 99.0 else " <--"
        print(f"{dt:<12} {sql_cnt:>12,} {db_cnt:>12,} {diff:>+10,} {match:>7.2f}%{marker}")

    # Overall status
    print("\n" + "=" * 65)
    status = "PASS" if min_match >= 99.0 else "WARN" if min_match >= 95.0 else "FAIL"
    print(f"OVERALL STATUS: {status} (lowest match: {min_match:.2f}%)")
    print("=" * 65)

    sql_cursor.close()
    sql_conn.close()

    return 0 if status in ["PASS", "WARN"] else 1


if __name__ == "__main__":
    sys.exit(main())
