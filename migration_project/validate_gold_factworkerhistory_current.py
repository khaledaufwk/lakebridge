#!/usr/bin/env python3
"""
Validate Gold FactWorkerHistory Data - Current Date Range

Compares data between:
- SQL Server: WakeCapDW.dbo.FactWorkersHistory
- Databricks: wakecap_prod.gold.gold_fact_workers_history

For data from December 2025 onwards (current incremental load).

Based on reference: migration_project/validate_gold_factworkerhistory.py (commit 28971f9)
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
        wait_timeout="50s"
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
    print("GOLD FACTWORKERHISTORY VALIDATION - CURRENT DATA")
    print("Comparing SQL Server vs Databricks Gold Layer")
    print("Data from December 2025 onwards")
    print("=" * 70)

    # Load credentials
    creds = load_credentials()

    # Date range for current incremental data
    start_date = "2025-12-18"
    end_date = "2026-01-23"

    # Connect to Databricks first (faster)
    print("\n1. Connecting to Databricks...")
    try:
        w = get_databricks_client(creds)
        user = w.current_user.me()
        print(f"   [OK] Connected as: {user.user_name}")

        # Use serverless warehouse
        warehouse_id = "7ac8830ec54eb738"
        print(f"   Using warehouse: {warehouse_id}")
    except Exception as e:
        print(f"   [ERROR] Databricks connection failed: {e}")
        return 1

    # Get Databricks counts first
    print(f"\n2. Querying Databricks (date range: {start_date} to {end_date})...")

    db_query = f"""
    SELECT COUNT(*) as cnt
    FROM wakecap_prod.gold.gold_fact_workers_history
    WHERE ShiftLocalDate >= '{start_date}' AND ShiftLocalDate <= '{end_date}'
    """
    try:
        cols, rows = run_databricks_sql(w, db_query, warehouse_id)
        db_count = int(rows[0][0]) if rows else 0
        print(f"   Databricks count: {db_count:,}")
    except Exception as e:
        print(f"   [ERROR] Databricks query failed: {e}")
        return 1

    # Connect to SQL Server
    print("\n3. Connecting to SQL Server...")
    try:
        sql_conn = connect_sqlserver(creds)
        sql_cursor = sql_conn.cursor()
        print("   [OK] Connected to SQL Server")
    except Exception as e:
        print(f"   [ERROR] SQL Server connection failed: {e}")
        return 1

    # Get SQL Server counts
    print(f"\n4. Querying SQL Server (date range: {start_date} to {end_date})...")
    sql_query = f"""
    SELECT COUNT(*) as cnt
    FROM dbo.FactWorkersHistory
    WHERE ShiftLocalDate >= '{start_date}' AND ShiftLocalDate <= '{end_date}'
    """
    try:
        sql_cursor.execute(sql_query)
        sql_count = sql_cursor.fetchone()[0]
        print(f"   SQL Server count: {sql_count:,}")
    except Exception as e:
        print(f"   [ERROR] SQL Server query failed: {e}")
        return 1

    # Validation results
    print("\n" + "=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)

    diff = db_count - sql_count
    match_pct = (min(sql_count, db_count) / max(sql_count, db_count) * 100) if max(sql_count, db_count) > 0 else 100
    status = "PASS" if match_pct >= 99.0 else "WARN" if match_pct >= 95.0 else "FAIL"

    print(f"""
   Date Range: {start_date} to {end_date}

   Row Counts:
     - SQL Server:  {sql_count:,}
     - Databricks:  {db_count:,}
     - Difference:  {diff:,} ({'+' if diff >= 0 else ''}{diff})

   Match: {match_pct:.2f}%
   Status: {status}
""")

    print("=" * 70)

    # Close connections
    sql_cursor.close()
    sql_conn.close()

    return 0 if status in ["PASS", "WARN"] else 1


if __name__ == "__main__":
    sys.exit(main())
