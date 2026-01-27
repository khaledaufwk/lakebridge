#!/usr/bin/env python3
"""
Fast Validation: Gold FactWorkersHistory Data

Compares data between:
- SQL Server: WakeCapDW.dbo.FactWorkersHistory
- Databricks: wakecap_prod.gold.gold_fact_workers_history

For data up to January 23, 2025.
Uses efficient aggregation queries to avoid full table scans.
"""
import sys
from pathlib import Path
from datetime import datetime

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
        wait_timeout="10m"
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
    print("GOLD FACTWORKERSHISTORY VALIDATION (Fast Mode)")
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

    results = {}

    # =========================================================================
    # Validation 1: Total Row Count by Date Range
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 1: Row Count by Date Range (through Jan 23)")
    print("=" * 70)

    # SQL Server - Use indexed date column
    print("\n   Querying SQL Server (this may take a moment)...")
    sql_query = f"""
    SELECT
        YEAR(ShiftLocalDate) as yr,
        MONTH(ShiftLocalDate) as mo,
        COUNT(*) as cnt
    FROM dbo.FactWorkersHistory WITH (NOLOCK)
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY YEAR(ShiftLocalDate), MONTH(ShiftLocalDate)
    ORDER BY yr, mo
    """
    sql_cursor.execute(sql_query)
    sql_by_month = {}
    sql_total = 0
    for row in sql_cursor.fetchall():
        key = f"{row[0]}-{row[1]:02d}"
        sql_by_month[key] = row[2]
        sql_total += row[2]

    print(f"   SQL Server total: {sql_total:,}")

    # Databricks
    print("   Querying Databricks...")
    db_query = f"""
    SELECT
        YEAR(ShiftLocalDate) as yr,
        MONTH(ShiftLocalDate) as mo,
        COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY YEAR(ShiftLocalDate), MONTH(ShiftLocalDate)
    ORDER BY yr, mo
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_by_month = {}
        db_total = 0
        for row in rows:
            key = f"{int(row[0])}-{int(row[1]):02d}"
            db_by_month[key] = int(row[2])
            db_total += int(row[2])
        print(f"   Databricks total: {db_total:,}")
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_by_month = {}
        db_total = 0

    # Show month comparison
    print(f"\n   {'Month':<10} {'SQL Server':>15} {'Databricks':>15} {'Diff':>12} {'Match%':>10}")
    print("   " + "-" * 62)

    all_months = sorted(set(sql_by_month.keys()) | set(db_by_month.keys()))
    for month in all_months[-12:]:  # Last 12 months
        sql_cnt = sql_by_month.get(month, 0)
        db_cnt = db_by_month.get(month, 0)
        diff = db_cnt - sql_cnt
        match = (min(sql_cnt, db_cnt) / max(sql_cnt, db_cnt) * 100) if max(sql_cnt, db_cnt) > 0 else 100
        print(f"   {month:<10} {sql_cnt:>15,} {db_cnt:>15,} {diff:>12,} {match:>9.2f}%")

    diff = db_total - sql_total
    match_pct = (min(sql_total, db_total) / max(sql_total, db_total) * 100) if max(sql_total, db_total) > 0 else 100
    status = "PASS" if match_pct >= 99.0 else "WARN" if match_pct >= 95.0 else "FAIL"

    print(f"\n   TOTAL:      {sql_total:>15,} {db_total:>15,} {diff:>12,} {match_pct:>9.2f}%")
    print(f"   Status: {status}")

    results['total_count'] = {
        'sql_server': sql_total,
        'databricks': db_total,
        'difference': diff,
        'match_pct': match_pct,
        'status': status
    }

    # =========================================================================
    # Validation 2: January 2025 Daily Detail
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 2: January 2025 Daily Detail (through Jan 23)")
    print("=" * 70)

    # SQL Server
    sql_query = f"""
    SELECT
        CAST(ShiftLocalDate AS DATE) as dt,
        COUNT(*) as cnt
    FROM dbo.FactWorkersHistory WITH (NOLOCK)
    WHERE ShiftLocalDate BETWEEN '2025-01-01' AND '{cutoff_date}'
    GROUP BY CAST(ShiftLocalDate AS DATE)
    ORDER BY dt
    """
    sql_cursor.execute(sql_query)
    sql_by_date = {str(row[0]): row[1] for row in sql_cursor.fetchall()}

    # Databricks
    db_query = f"""
    SELECT
        CAST(ShiftLocalDate AS STRING) as dt,
        COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate BETWEEN '2025-01-01' AND '{cutoff_date}'
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
    jan_sql_total = 0
    jan_db_total = 0
    for dt in all_dates:
        sql_cnt = sql_by_date.get(dt, 0)
        db_cnt = db_by_date.get(dt, 0)
        jan_sql_total += sql_cnt
        jan_db_total += db_cnt
        diff = db_cnt - sql_cnt
        match = (min(sql_cnt, db_cnt) / max(sql_cnt, db_cnt) * 100) if max(sql_cnt, db_cnt) > 0 else 100

        # Highlight mismatches
        marker = "" if match >= 99.0 else " <-- MISMATCH"
        print(f"   {dt:<12} {sql_cnt:>15,} {db_cnt:>15,} {diff:>12,} {match:>9.2f}%{marker}")

    jan_match = (min(jan_sql_total, jan_db_total) / max(jan_sql_total, jan_db_total) * 100) if max(jan_sql_total, jan_db_total) > 0 else 100
    print(f"\n   Jan Total:  {jan_sql_total:>15,} {jan_db_total:>15,} {jan_db_total - jan_sql_total:>12,} {jan_match:>9.2f}%")

    results['jan_2025'] = {
        'sql_server': jan_sql_total,
        'databricks': jan_db_total,
        'by_date': {'sql': sql_by_date, 'db': db_by_date}
    }

    # =========================================================================
    # Validation 3: Distinct Worker/Project Counts
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 3: Distinct Counts (through Jan 23)")
    print("=" * 70)

    # SQL Server
    sql_query = f"""
    SELECT
        COUNT(DISTINCT WorkerId) as workers,
        COUNT(DISTINCT ProjectId) as projects
    FROM dbo.FactWorkersHistory WITH (NOLOCK)
    WHERE ShiftLocalDate <= '{cutoff_date}'
    """
    sql_cursor.execute(sql_query)
    row = sql_cursor.fetchone()
    sql_workers = row[0]
    sql_projects = row[1]
    print(f"   SQL Server - Workers: {sql_workers:,}, Projects: {sql_projects:,}")

    # Databricks
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
        print(f"   Databricks - Workers: {db_workers:,}, Projects: {db_projects:,}")
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_workers = db_projects = 0

    workers_match = (min(sql_workers, db_workers) / max(sql_workers, db_workers) * 100) if max(sql_workers, db_workers) > 0 else 100
    projects_match = (min(sql_projects, db_projects) / max(sql_projects, db_projects) * 100) if max(sql_projects, db_projects) > 0 else 100

    print(f"\n   Workers Match: {workers_match:.2f}%")
    print(f"   Projects Match: {projects_match:.2f}%")

    results['distinct'] = {
        'workers': {'sql': sql_workers, 'db': db_workers, 'match': workers_match},
        'projects': {'sql': sql_projects, 'db': db_projects, 'match': projects_match}
    }

    # =========================================================================
    # Validation 4: Top Projects Comparison
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 4: Top 10 Projects by Row Count")
    print("=" * 70)

    # SQL Server top projects
    sql_query = f"""
    SELECT TOP 10 ProjectId, COUNT(*) as cnt
    FROM dbo.FactWorkersHistory WITH (NOLOCK)
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY ProjectId
    ORDER BY cnt DESC
    """
    sql_cursor.execute(sql_query)
    sql_top_projects = {row[0]: row[1] for row in sql_cursor.fetchall()}

    # Databricks top projects
    db_query = f"""
    SELECT ProjectId, COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate <= '{cutoff_date}'
    GROUP BY ProjectId
    ORDER BY cnt DESC
    LIMIT 10
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_top_projects = {int(row[0]): int(row[1]) for row in rows}
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_top_projects = {}

    print(f"\n   {'ProjectId':<12} {'SQL Server':>15} {'Databricks':>15} {'Diff':>12} {'Match%':>10}")
    print("   " + "-" * 64)

    all_top = sorted(set(sql_top_projects.keys()) | set(db_top_projects.keys()),
                     key=lambda x: sql_top_projects.get(x, 0), reverse=True)[:10]
    for proj in all_top:
        sql_cnt = sql_top_projects.get(proj, 0)
        db_cnt = db_top_projects.get(proj, 0)
        diff = db_cnt - sql_cnt
        match = (min(sql_cnt, db_cnt) / max(sql_cnt, db_cnt) * 100) if max(sql_cnt, db_cnt) > 0 else 100
        marker = "" if match >= 99.0 else " <--"
        print(f"   {proj:<12} {sql_cnt:>15,} {db_cnt:>15,} {diff:>12,} {match:>9.2f}%{marker}")

    # =========================================================================
    # Validation 5: Aggregate Sums (Recent Data)
    # =========================================================================
    print("\n" + "=" * 70)
    print("VALIDATION 5: Aggregate Sums (Jan 2025)")
    print("=" * 70)

    # SQL Server aggregates for Jan 2025 only (faster)
    sql_query = f"""
    SELECT
        SUM(CAST(ActiveTime AS FLOAT)) as total_active,
        SUM(CAST(InactiveTime AS FLOAT)) as total_inactive
    FROM dbo.FactWorkersHistory WITH (NOLOCK)
    WHERE ShiftLocalDate BETWEEN '2025-01-01' AND '{cutoff_date}'
    """
    sql_cursor.execute(sql_query)
    row = sql_cursor.fetchone()
    sql_active = float(row[0]) if row[0] else 0
    sql_inactive = float(row[1]) if row[1] else 0
    print(f"   SQL Server (Jan 2025):")
    print(f"     ActiveTime:   {sql_active:,.6f}")
    print(f"     InactiveTime: {sql_inactive:,.6f}")

    # Databricks aggregates
    db_query = f"""
    SELECT
        SUM(ActiveTime) as total_active,
        SUM(InactiveTime) as total_inactive
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate BETWEEN '2025-01-01' AND '{cutoff_date}'
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_active = float(rows[0][0]) if rows and rows[0][0] else 0
        db_inactive = float(rows[0][1]) if rows and rows[0][1] else 0
        print(f"\n   Databricks (Jan 2025):")
        print(f"     ActiveTime:   {db_active:,.6f}")
        print(f"     InactiveTime: {db_inactive:,.6f}")
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        db_active = db_inactive = 0

    active_match = (min(sql_active, db_active) / max(sql_active, db_active) * 100) if max(sql_active, db_active) > 0 else 100
    inactive_match = (min(sql_inactive, db_inactive) / max(sql_inactive, db_inactive) * 100) if max(sql_inactive, db_inactive) > 0 else 100

    print(f"\n   ActiveTime Match:   {active_match:.2f}%")
    print(f"   InactiveTime Match: {inactive_match:.2f}%")

    results['aggregates'] = {
        'active': {'sql': sql_active, 'db': db_active, 'match': active_match},
        'inactive': {'sql': sql_inactive, 'db': db_inactive, 'match': inactive_match}
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
   Overall Row Count (through Jan 23, 2025):
     - SQL Server:  {results['total_count']['sql_server']:,}
     - Databricks:  {results['total_count']['databricks']:,}
     - Difference:  {results['total_count']['difference']:,}
     - Match:       {total_match:.2f}%
     - Status:      {total_status}

   January 2025 Data:
     - SQL Server:  {results['jan_2025']['sql_server']:,}
     - Databricks:  {results['jan_2025']['databricks']:,}

   Distinct Counts:
     - Workers:  {results['distinct']['workers']['match']:.2f}% match
     - Projects: {results['distinct']['projects']['match']:.2f}% match

   Aggregates (Jan 2025):
     - ActiveTime:   {results['aggregates']['active']['match']:.2f}% match
     - InactiveTime: {results['aggregates']['inactive']['match']:.2f}% match
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
