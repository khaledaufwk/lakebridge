#!/usr/bin/env python3
"""
Validate Gold FactWorkerHistory Data

Compares data between:
- SQL Server: WakeCapDW.dbo.FactWorkersHistory
- Databricks: wakecap_prod.gold.gold_fact_workers_history

For data up to January 23, 2025.
"""
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
import pymssql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def load_credentials():
    """Load credentials from the lakebridge credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def connect_sqlserver(creds):
    """Connect to SQL Server using pymssql."""
    mssql = creds['mssql']
    return pymssql.connect(
        server=mssql['server'],
        user=mssql['user'],
        password=mssql['password'],
        database=mssql['database'],
        port=mssql.get('port', 1433),
        login_timeout=30,
        as_dict=False
    )


def get_databricks_client(creds):
    """Get Databricks workspace client."""
    db = creds['databricks']
    return WorkspaceClient(host=db['host'], token=db['token'])


def run_databricks_sql(w, sql, warehouse_id):
    """Execute SQL on Databricks and return results."""
    statement = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="5m"
    )

    if statement.status.state == StatementState.SUCCEEDED:
        if statement.result and statement.result.data_array:
            columns = [col.name for col in statement.manifest.schema.columns]
            rows = statement.result.data_array
            return columns, rows
    else:
        error = statement.status.error
        raise Exception(f"SQL execution failed: {error}")

    return [], []


def main():
    print("=" * 70)
    print("GOLD FACTWORKERHISTORY VALIDATION")
    print("Comparing SQL Server vs Databricks Gold Layer")
    print("Data through January 23, 2025")
    print("=" * 70)

    # Load credentials
    creds = load_credentials()

    # SQL Server configuration
    cutoff_date = "2025-01-23"

    # Connect to SQL Server
    print("\n1. Connecting to SQL Server...")
    try:
        sql_conn = connect_sqlserver(creds)
        sql_cursor = sql_conn.cursor()
        print("   [OK] Connected to SQL Server")
    except Exception as e:
        print(f"   [ERROR] SQL Server connection failed: {e}")
        return 1

    # Connect to Databricks
    print("\n2. Connecting to Databricks...")
    try:
        w = get_databricks_client(creds)
        user = w.current_user.me()
        print(f"   [OK] Connected as: {user.user_name}")

        # Get SQL warehouse
        warehouses = list(w.warehouses.list())
        if not warehouses:
            print("   [ERROR] No SQL warehouse found")
            return 1
        warehouse_id = warehouses[0].id
        print(f"   Using warehouse: {warehouses[0].name}")
    except Exception as e:
        print(f"   [ERROR] Databricks connection failed: {e}")
        return 1

    # Validation results
    results = {}

    # =========================================================================
    # Validation 1: Total Row Count
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 1: Total Row Count (through Jan 23)")
    print("=" * 70)

    # SQL Server count
    sql_query = f"""
    SELECT COUNT(*) as cnt
    FROM dbo.FactWorkersHistory
    WHERE ShiftLocalDate <= '{cutoff_date}'
    """
    sql_cursor.execute(sql_query)
    sql_count = sql_cursor.fetchone()[0]
    print(f"   SQL Server count: {sql_count:,}")

    # Databricks count
    db_query = f"""
    SELECT COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate <= '{cutoff_date}'
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_count = int(rows[0][0]) if rows else 0
        print(f"   Databricks count: {db_count:,}")
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_count = 0

    diff = db_count - sql_count
    match_pct = (min(sql_count, db_count) / max(sql_count, db_count) * 100) if max(sql_count, db_count) > 0 else 100
    status = "PASS" if match_pct >= 99.0 else "WARN" if match_pct >= 95.0 else "FAIL"

    print(f"   Difference: {diff:,} ({'+' if diff >= 0 else ''}{diff})")
    print(f"   Match: {match_pct:.2f}%")
    print(f"   Status: {status}")

    results['total_count'] = {
        'sql_server': sql_count,
        'databricks': db_count,
        'difference': diff,
        'match_pct': match_pct,
        'status': status
    }

    # =========================================================================
    # Validation 2: Count by Project
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 2: Row Count by Project (Top 10)")
    print("=" * 70)

    # SQL Server by project
    sql_query = f"""
    SELECT TOP 20 ProjectId, COUNT(*) as cnt
    FROM dbo.FactWorkersHistory
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY ProjectId
    ORDER BY cnt DESC
    """
    sql_cursor.execute(sql_query)
    sql_by_project = {row[0]: row[1] for row in sql_cursor.fetchall()}

    # Databricks by project
    db_query = f"""
    SELECT ProjectId, COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY ProjectId
    ORDER BY cnt DESC
    LIMIT 20
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_by_project = {int(row[0]): int(row[1]) for row in rows}
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_by_project = {}

    print(f"\n   {'ProjectId':<12} {'SQL Server':>15} {'Databricks':>15} {'Diff':>12} {'Match%':>10}")
    print("   " + "-" * 64)

    all_projects = set(sql_by_project.keys()) | set(db_by_project.keys())
    project_results = []

    for proj_id in sorted(all_projects, key=lambda x: sql_by_project.get(x, 0), reverse=True)[:10]:
        sql_cnt = sql_by_project.get(proj_id, 0)
        db_cnt = db_by_project.get(proj_id, 0)
        diff = db_cnt - sql_cnt
        match = (min(sql_cnt, db_cnt) / max(sql_cnt, db_cnt) * 100) if max(sql_cnt, db_cnt) > 0 else 100

        print(f"   {proj_id:<12} {sql_cnt:>15,} {db_cnt:>15,} {diff:>12,} {match:>9.2f}%")
        project_results.append({
            'project_id': proj_id,
            'sql_server': sql_cnt,
            'databricks': db_cnt,
            'difference': diff,
            'match_pct': match
        })

    results['by_project'] = project_results

    # =========================================================================
    # Validation 3: Sum of ActiveTime
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 3: Sum of ActiveTime (through Jan 23)")
    print("=" * 70)

    # SQL Server sum
    sql_query = f"""
    SELECT
        SUM(CAST(ActiveTime AS FLOAT)) as total_active,
        SUM(CAST(InactiveTime AS FLOAT)) as total_inactive
    FROM dbo.FactWorkersHistory
    WHERE ShiftLocalDate <= '{cutoff_date}'
    """
    sql_cursor.execute(sql_query)
    row = sql_cursor.fetchone()
    sql_active = float(row[0]) if row[0] else 0
    sql_inactive = float(row[1]) if row[1] else 0
    print(f"   SQL Server - ActiveTime: {sql_active:,.6f}")
    print(f"   SQL Server - InactiveTime: {sql_inactive:,.6f}")

    # Databricks sum
    db_query = f"""
    SELECT
        SUM(ActiveTime) as total_active,
        SUM(InactiveTime) as total_inactive
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate <= '{cutoff_date}'
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_active = float(rows[0][0]) if rows and rows[0][0] else 0
        db_inactive = float(rows[0][1]) if rows and rows[0][1] else 0
        print(f"   Databricks - ActiveTime: {db_active:,.6f}")
        print(f"   Databricks - InactiveTime: {db_inactive:,.6f}")
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_active = db_inactive = 0

    active_match = (min(sql_active, db_active) / max(sql_active, db_active) * 100) if max(sql_active, db_active) > 0 else 100
    inactive_match = (min(sql_inactive, db_inactive) / max(sql_inactive, db_inactive) * 100) if max(sql_inactive, db_inactive) > 0 else 100

    print(f"\n   ActiveTime Match: {active_match:.2f}%")
    print(f"   InactiveTime Match: {inactive_match:.2f}%")

    results['aggregations'] = {
        'active_time': {
            'sql_server': sql_active,
            'databricks': db_active,
            'match_pct': active_match
        },
        'inactive_time': {
            'sql_server': sql_inactive,
            'databricks': db_inactive,
            'match_pct': inactive_match
        }
    }

    # =========================================================================
    # Validation 4: Count by Date (Last 7 days before Jan 23)
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 4: Row Count by Date (Jan 17-23)")
    print("=" * 70)

    # SQL Server by date
    sql_query = f"""
    SELECT ShiftLocalDate, COUNT(*) as cnt
    FROM dbo.FactWorkersHistory
    WHERE ShiftLocalDate BETWEEN '2025-01-17' AND '{cutoff_date}'
    GROUP BY ShiftLocalDate
    ORDER BY ShiftLocalDate
    """
    sql_cursor.execute(sql_query)
    sql_by_date = {str(row[0]): row[1] for row in sql_cursor.fetchall()}

    # Databricks by date
    db_query = f"""
    SELECT CAST(ShiftLocalDate AS STRING) as dt, COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate BETWEEN '2025-01-17' AND '{cutoff_date}'
    GROUP BY ShiftLocalDate
    ORDER BY ShiftLocalDate
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_by_date = {row[0]: int(row[1]) for row in rows}
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_by_date = {}

    print(f"\n   {'Date':<12} {'SQL Server':>15} {'Databricks':>15} {'Diff':>12} {'Match%':>10}")
    print("   " + "-" * 64)

    all_dates = sorted(set(sql_by_date.keys()) | set(db_by_date.keys()))
    for dt in all_dates:
        sql_cnt = sql_by_date.get(dt, 0)
        db_cnt = db_by_date.get(dt, 0)
        diff = db_cnt - sql_cnt
        match = (min(sql_cnt, db_cnt) / max(sql_cnt, db_cnt) * 100) if max(sql_cnt, db_cnt) > 0 else 100

        print(f"   {dt:<12} {sql_cnt:>15,} {db_cnt:>15,} {diff:>12,} {match:>9.2f}%")

    results['by_date'] = {
        'sql_server': sql_by_date,
        'databricks': db_by_date
    }

    # =========================================================================
    # Validation 5: Unique Workers and Projects
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 5: Distinct Counts (through Jan 23)")
    print("=" * 70)

    # SQL Server distinct counts
    sql_query = f"""
    SELECT
        COUNT(DISTINCT WorkerId) as workers,
        COUNT(DISTINCT ProjectId) as projects
    FROM dbo.FactWorkersHistory
    WHERE ShiftLocalDate <= '{cutoff_date}'
    """
    sql_cursor.execute(sql_query)
    row = sql_cursor.fetchone()
    sql_workers = row[0]
    sql_projects = row[1]
    print(f"   SQL Server - Distinct Workers: {sql_workers:,}")
    print(f"   SQL Server - Distinct Projects: {sql_projects:,}")

    # Databricks distinct counts
    db_query = f"""
    SELECT
        COUNT(DISTINCT WorkerId) as workers,
        COUNT(DISTINCT ProjectId) as projects
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate <= '{cutoff_date}'
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_workers = int(rows[0][0]) if rows else 0
        db_projects = int(rows[0][1]) if rows else 0
        print(f"   Databricks - Distinct Workers: {db_workers:,}")
        print(f"   Databricks - Distinct Projects: {db_projects:,}")
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_workers = db_projects = 0

    print(f"\n   Workers Match: {db_workers}/{sql_workers} ({db_workers/sql_workers*100:.2f}%)" if sql_workers else "")
    print(f"   Projects Match: {db_projects}/{sql_projects} ({db_projects/sql_projects*100:.2f}%)" if sql_projects else "")

    results['distinct_counts'] = {
        'workers': {'sql_server': sql_workers, 'databricks': db_workers},
        'projects': {'sql_server': sql_projects, 'databricks': db_projects}
    }

    # =========================================================================
    # Validation 6: TimeCategoryId Distribution
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 6: TimeCategoryId Distribution")
    print("=" * 70)

    # SQL Server distribution
    sql_query = f"""
    SELECT TimeCategoryId, COUNT(*) as cnt
    FROM dbo.FactWorkersHistory
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY TimeCategoryId
    ORDER BY TimeCategoryId
    """
    sql_cursor.execute(sql_query)
    sql_time_cat = {row[0]: row[1] for row in sql_cursor.fetchall()}

    # Databricks distribution
    db_query = f"""
    SELECT TimeCategoryId, COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY TimeCategoryId
    ORDER BY TimeCategoryId
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_time_cat = {int(row[0]) if row[0] else None: int(row[1]) for row in rows}
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_time_cat = {}

    cat_names = {
        1: "During Shift",
        2: "No Shift",
        3: "Break",
        4: "After Shift",
        5: "Before Shift",
        None: "NULL"
    }

    print(f"\n   {'Category':<20} {'SQL Server':>15} {'Databricks':>15} {'Diff':>12}")
    print("   " + "-" * 62)

    all_cats = sorted(set(sql_time_cat.keys()) | set(db_time_cat.keys()), key=lambda x: x if x else 0)
    for cat in all_cats:
        sql_cnt = sql_time_cat.get(cat, 0)
        db_cnt = db_time_cat.get(cat, 0)
        diff = db_cnt - sql_cnt
        name = cat_names.get(cat, f"Unknown({cat})")

        print(f"   {name:<20} {sql_cnt:>15,} {db_cnt:>15,} {diff:>12,}")

    results['time_category'] = {
        'sql_server': sql_time_cat,
        'databricks': db_time_cat
    }

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)

    total_status = results['total_count']['status']
    total_match = results['total_count']['match_pct']

    print(f"""
   Total Row Count:
     - SQL Server:  {results['total_count']['sql_server']:,}
     - Databricks:  {results['total_count']['databricks']:,}
     - Difference:  {results['total_count']['difference']:,}
     - Match:       {total_match:.2f}%
     - Status:      {total_status}

   Distinct Counts:
     - Workers:     SQL={results['distinct_counts']['workers']['sql_server']:,}, DB={results['distinct_counts']['workers']['databricks']:,}
     - Projects:    SQL={results['distinct_counts']['projects']['sql_server']:,}, DB={results['distinct_counts']['projects']['databricks']:,}

   Aggregations:
     - ActiveTime:   {results['aggregations']['active_time']['match_pct']:.2f}% match
     - InactiveTime: {results['aggregations']['inactive_time']['match_pct']:.2f}% match
""")

    overall_status = "PASS" if total_match >= 99.0 else "WARN" if total_match >= 95.0 else "FAIL"

    print(f"   OVERALL STATUS: {overall_status}")
    print("=" * 70)

    # Close connections
    sql_cursor.close()
    sql_conn.close()

    return 0 if overall_status in ["PASS", "WARN"] else 1


if __name__ == "__main__":
    sys.exit(main())
