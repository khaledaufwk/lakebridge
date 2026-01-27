#!/usr/bin/env python3
"""Quick validation with progress output."""
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
    statement = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="10m"
    )
    if statement.status.state == StatementState.SUCCEEDED:
        if statement.result and statement.result.data_array:
            return statement.result.data_array
    else:
        raise Exception(f"Failed: {statement.status.error}")
    return []


def main():
    print("Loading credentials...", flush=True)
    creds = load_credentials()

    cutoff_date = "2025-01-23"

    # SQL Server connection
    print("Connecting to SQL Server...", flush=True)
    mssql = creds['mssql']
    sql_conn = pymssql.connect(
        server=mssql['server'],
        user=mssql['user'],
        password=mssql['password'],
        database=mssql['database'],
        port=mssql.get('port', 1433),
        login_timeout=30
    )
    sql_cursor = sql_conn.cursor()
    print("  Connected to SQL Server", flush=True)

    # Databricks connection
    print("Connecting to Databricks...", flush=True)
    db = creds['databricks']
    w = WorkspaceClient(host=db['host'], token=db['token'])
    warehouses = list(w.warehouses.list())
    warehouse_id = warehouses[0].id
    print(f"  Connected to Databricks ({warehouses[0].name})", flush=True)

    print("\n" + "="*60, flush=True)
    print("VALIDATION: Jan 17-23, 2025 Daily Row Counts", flush=True)
    print("="*60, flush=True)

    # SQL Server query
    print("\nQuerying SQL Server (Jan 17-23)...", flush=True)
    sql_cursor.execute(f"""
        SELECT CAST(ShiftLocalDate AS DATE) as dt, COUNT(*) as cnt
        FROM dbo.FactWorkersHistory WITH (NOLOCK)
        WHERE ShiftLocalDate BETWEEN '2025-01-17' AND '{cutoff_date}'
        GROUP BY CAST(ShiftLocalDate AS DATE)
        ORDER BY dt
    """)
    sql_results = {str(row[0]): row[1] for row in sql_cursor.fetchall()}
    print(f"  SQL Server returned {len(sql_results)} dates", flush=True)

    # Databricks query
    print("\nQuerying Databricks (Jan 17-23)...", flush=True)
    rows = run_databricks_sql(w, f"""
        SELECT CAST(ShiftLocalDate AS STRING), COUNT(*)
        FROM wakecap_prod.gold.gold_fact_workers_history
        WHERE ShiftLocalDate BETWEEN '2025-01-17' AND '{cutoff_date}'
        GROUP BY ShiftLocalDate
        ORDER BY ShiftLocalDate
    """, warehouse_id)
    db_results = {row[0]: int(row[1]) for row in rows}
    print(f"  Databricks returned {len(db_results)} dates", flush=True)

    # Compare results
    print("\n" + "-"*60, flush=True)
    print(f"{'Date':<12} {'SQL Server':>12} {'Databricks':>12} {'Diff':>10} {'Match':>8}", flush=True)
    print("-"*60, flush=True)

    all_dates = sorted(set(sql_results.keys()) | set(db_results.keys()))
    total_sql = 0
    total_db = 0

    for dt in all_dates:
        sql_cnt = sql_results.get(dt, 0)
        db_cnt = db_results.get(dt, 0)
        total_sql += sql_cnt
        total_db += db_cnt
        diff = db_cnt - sql_cnt
        match = (min(sql_cnt, db_cnt) / max(sql_cnt, db_cnt) * 100) if max(sql_cnt, db_cnt) > 0 else 100
        marker = "" if abs(diff) < 100 else " <--"
        print(f"{dt:<12} {sql_cnt:>12,} {db_cnt:>12,} {diff:>+10,} {match:>7.1f}%{marker}", flush=True)

    print("-"*60, flush=True)
    total_match = (min(total_sql, total_db) / max(total_sql, total_db) * 100) if max(total_sql, total_db) > 0 else 100
    print(f"{'TOTAL':<12} {total_sql:>12,} {total_db:>12,} {total_db-total_sql:>+10,} {total_match:>7.1f}%", flush=True)

    # Get full counts
    print("\n" + "="*60, flush=True)
    print("FULL DATA COUNTS (through Jan 23, 2025)", flush=True)
    print("="*60, flush=True)

    print("\nQuerying SQL Server total count...", flush=True)
    sql_cursor.execute(f"""
        SELECT COUNT(*) FROM dbo.FactWorkersHistory WITH (NOLOCK)
        WHERE ShiftLocalDate <= '{cutoff_date}'
    """)
    sql_total = sql_cursor.fetchone()[0]
    print(f"  SQL Server total: {sql_total:,}", flush=True)

    print("\nQuerying Databricks total count...", flush=True)
    rows = run_databricks_sql(w, f"""
        SELECT COUNT(*) FROM wakecap_prod.gold.gold_fact_workers_history
        WHERE ShiftLocalDate <= '{cutoff_date}'
    """, warehouse_id)
    db_total = int(rows[0][0])
    print(f"  Databricks total: {db_total:,}", flush=True)

    diff = db_total - sql_total
    match_pct = (min(sql_total, db_total) / max(sql_total, db_total) * 100) if max(sql_total, db_total) > 0 else 100

    print(f"\n  Difference: {diff:+,} ({diff/sql_total*100:+.4f}%)", flush=True)
    print(f"  Match: {match_pct:.4f}%", flush=True)

    status = "PASS" if match_pct >= 99.9 else "WARN" if match_pct >= 99.0 else "FAIL"
    print(f"\n  STATUS: {status}", flush=True)

    sql_cursor.close()
    sql_conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
