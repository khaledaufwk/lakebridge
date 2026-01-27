#!/usr/bin/env python3
"""Single day validation - much faster."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
import pymssql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_databricks_sql(w, sql, warehouse_id):
    import time
    from databricks.sdk.service.sql import StatementState

    # Start async execution
    statement = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="0s"  # Don't wait, poll instead
    )

    statement_id = statement.statement_id
    print(f"      Statement ID: {statement_id}", flush=True)

    # Poll for completion
    for i in range(180):  # Wait up to 15 minutes
        status = w.statement_execution.get_statement(statement_id)
        state = status.status.state
        print(f"\r      Poll {i+1}: {state}", end="", flush=True)

        if state == StatementState.SUCCEEDED:
            print(flush=True)
            if status.result and status.result.data_array:
                return status.result.data_array
            return []
        elif state in [StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED]:
            print(flush=True)
            raise Exception(f"Failed: {status.status.error}")
        elif state == StatementState.PENDING or state == StatementState.RUNNING:
            time.sleep(5)
        else:
            time.sleep(5)

    print(flush=True)
    raise Exception("Query timeout")


def main():
    print("=" * 60)
    print("SINGLE DAY VALIDATION: January 23, 2025")
    print("=" * 60)

    creds = load_credentials()

    # SQL Server
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

    # Databricks
    print("\n2. Connecting to Databricks...", flush=True)
    db = creds['databricks']
    w = WorkspaceClient(host=db['host'], token=db['token'])
    warehouses = list(w.warehouses.list())
    warehouse_id = warehouses[0].id
    print(f"   Connected ({warehouses[0].name})", flush=True)

    target_date = "2025-01-23"

    # Query SQL Server for single day
    print(f"\n3. Querying SQL Server for {target_date}...", flush=True)
    sql_cursor.execute(f"""
        SELECT
            COUNT(*) as cnt,
            COUNT(DISTINCT WorkerId) as workers,
            COUNT(DISTINCT ProjectId) as projects,
            SUM(CAST(ActiveTime AS FLOAT)) as active_time
        FROM dbo.FactWorkersHistory WITH (NOLOCK)
        WHERE ShiftLocalDate = '{target_date}'
    """)
    sql_row = sql_cursor.fetchone()
    sql_count = sql_row[0]
    sql_workers = sql_row[1]
    sql_projects = sql_row[2]
    sql_active = float(sql_row[3]) if sql_row[3] else 0
    print(f"   Count: {sql_count:,}", flush=True)
    print(f"   Workers: {sql_workers:,}", flush=True)
    print(f"   Projects: {sql_projects:,}", flush=True)
    print(f"   ActiveTime: {sql_active:,.4f}", flush=True)

    # Query Databricks for single day
    print(f"\n4. Querying Databricks for {target_date}...", flush=True)
    rows = run_databricks_sql(w, f"""
        SELECT
            COUNT(*) as cnt,
            COUNT(DISTINCT WorkerId) as workers,
            COUNT(DISTINCT ProjectId) as projects,
            SUM(ActiveTime) as active_time
        FROM wakecap_prod.gold.gold_fact_workers_history
        WHERE ShiftLocalDate = '{target_date}'
    """, warehouse_id)
    db_count = int(rows[0][0]) if rows else 0
    db_workers = int(rows[0][1]) if rows else 0
    db_projects = int(rows[0][2]) if rows else 0
    db_active = float(rows[0][3]) if rows and rows[0][3] else 0
    print(f"   Count: {db_count:,}", flush=True)
    print(f"   Workers: {db_workers:,}", flush=True)
    print(f"   Projects: {db_projects:,}", flush=True)
    print(f"   ActiveTime: {db_active:,.4f}", flush=True)

    # Comparison
    print("\n" + "=" * 60)
    print("COMPARISON")
    print("=" * 60)

    count_match = (min(sql_count, db_count) / max(sql_count, db_count) * 100) if max(sql_count, db_count) > 0 else 100
    workers_match = (min(sql_workers, db_workers) / max(sql_workers, db_workers) * 100) if max(sql_workers, db_workers) > 0 else 100
    projects_match = (min(sql_projects, db_projects) / max(sql_projects, db_projects) * 100) if max(sql_projects, db_projects) > 0 else 100
    active_match = (min(sql_active, db_active) / max(sql_active, db_active) * 100) if max(sql_active, db_active) > 0 else 100

    print(f"\n{'Metric':<15} {'SQL Server':>15} {'Databricks':>15} {'Diff':>12} {'Match':>8}")
    print("-" * 65)
    print(f"{'Count':<15} {sql_count:>15,} {db_count:>15,} {db_count-sql_count:>+12,} {count_match:>7.2f}%")
    print(f"{'Workers':<15} {sql_workers:>15,} {db_workers:>15,} {db_workers-sql_workers:>+12,} {workers_match:>7.2f}%")
    print(f"{'Projects':<15} {sql_projects:>15,} {db_projects:>15,} {db_projects-sql_projects:>+12,} {projects_match:>7.2f}%")
    print(f"{'ActiveTime':<15} {sql_active:>15,.4f} {db_active:>15,.4f} {db_active-sql_active:>+12,.4f} {active_match:>7.2f}%")

    # Status
    overall_match = min(count_match, workers_match, projects_match, active_match)
    status = "PASS" if overall_match >= 99.0 else "WARN" if overall_match >= 95.0 else "FAIL"

    print("\n" + "=" * 60)
    print(f"STATUS: {status} (lowest match: {overall_match:.2f}%)")
    print("=" * 60)

    # Now check a few more dates
    print("\n\n" + "=" * 60)
    print("MULTI-DAY CHECK (Jan 20-23)")
    print("=" * 60)

    dates_to_check = ["2025-01-20", "2025-01-21", "2025-01-22", "2025-01-23"]

    print(f"\n{'Date':<12} {'SQL Server':>12} {'Databricks':>12} {'Diff':>10} {'Match':>8}")
    print("-" * 55)

    for dt in dates_to_check:
        print(f"Querying {dt}...", end=" ", flush=True)

        # SQL Server
        sql_cursor.execute(f"""
            SELECT COUNT(*)
            FROM dbo.FactWorkersHistory WITH (NOLOCK)
            WHERE ShiftLocalDate = '{dt}'
        """)
        sql_cnt = sql_cursor.fetchone()[0]

        # Databricks
        rows = run_databricks_sql(w, f"""
            SELECT COUNT(*)
            FROM wakecap_prod.gold.gold_fact_workers_history
            WHERE ShiftLocalDate = '{dt}'
        """, warehouse_id)
        db_cnt = int(rows[0][0]) if rows else 0

        diff = db_cnt - sql_cnt
        match = (min(sql_cnt, db_cnt) / max(sql_cnt, db_cnt) * 100) if max(sql_cnt, db_cnt) > 0 else 100

        print(f"\r{dt:<12} {sql_cnt:>12,} {db_cnt:>12,} {diff:>+10,} {match:>7.2f}%")

    sql_cursor.close()
    sql_conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
