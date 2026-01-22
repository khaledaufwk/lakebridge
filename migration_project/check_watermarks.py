#!/usr/bin/env python3
"""Check watermark table for loading progress."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def wait_for_statement(w, result, max_wait=60):
    """Wait for SQL statement to complete."""
    waited = 0
    while result.status.state in [StatementState.PENDING, StatementState.RUNNING] and waited < max_wait:
        time.sleep(2)
        waited += 2
        result = w.statement_execution.get_statement(result.statement_id)
    return result


def main():
    creds = load_credentials()
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    print("=" * 70)
    print("LOADING PROGRESS - Watermark Tracking")
    print("=" * 70)

    # Get warehouse
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("No SQL warehouses available")
        return

    warehouse_id = warehouses[0].id
    print(f"Using warehouse: {warehouses[0].name}")
    print(f"State: {warehouses[0].state}")

    # Check successful tables
    print("\n--- SUCCESSFULLY LOADED TABLES ---")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="""
        SELECT
            source_table,
            last_load_row_count,
            last_watermark_timestamp,
            updated_at
        FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE last_load_status = 'success'
        ORDER BY last_load_row_count DESC
        """,
        wait_timeout="0s"
    )
    result = wait_for_statement(w, result)

    if result.status.state == StatementState.SUCCEEDED:
        if result.result and result.result.data_array:
            tables = result.result.data_array
            print(f"\nSuccessfully loaded: {len(tables)} tables")
            print("-" * 70)
            print(f"{'Table':<40} {'Rows':>12}")
            print("-" * 70)
            total_rows = 0
            for row in tables[:50]:
                table = row[0]
                rows = int(row[1]) if row[1] else 0
                total_rows += rows
                print(f"{table:<40} {rows:>12,}")
            if len(tables) > 50:
                print(f"\n... and {len(tables) - 50} more tables")
            print("-" * 70)
            print(f"TOTAL: {total_rows:,} rows")
        else:
            print("No successful loads found")
    else:
        print(f"Query failed: {result.status.state}")

    # Check failed tables
    print("\n--- FAILED TABLES ---")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="""
        SELECT
            source_table,
            last_error_message
        FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE last_load_status = 'failed'
        ORDER BY source_table
        """,
        wait_timeout="0s"
    )
    result = wait_for_statement(w, result)

    if result.status.state == StatementState.SUCCEEDED:
        if result.result and result.result.data_array:
            tables = result.result.data_array
            print(f"\nFailed: {len(tables)} tables")
            print("-" * 70)
            for row in tables:
                table = row[0]
                error = row[1][:60] if row[1] else "Unknown"
                print(f"  {table}: {error}")
        else:
            print("No failed tables!")
    else:
        print(f"Query failed: {result.status.state}")

    # Check raw tables count
    print("\n--- Raw Tables Created ---")
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SHOW TABLES IN wakecap_prod.raw LIKE 'timescale_*'",
        wait_timeout="0s"
    )
    result = wait_for_statement(w, result)

    if result.status.state == StatementState.SUCCEEDED:
        if result.result and result.result.data_array:
            tables = [row[1] for row in result.result.data_array]
            print(f"Tables found: {len(tables)} / 81")
            print(f"Progress: {len(tables)/81*100:.1f}%")
        else:
            print("No tables created yet")
    else:
        print(f"Query state: {result.status.state}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
