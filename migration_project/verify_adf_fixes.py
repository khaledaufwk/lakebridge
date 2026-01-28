#!/usr/bin/env python3
"""Verify ADF parity fixes are working correctly."""

import sys
from pathlib import Path
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def run_query(w, warehouse_id, sql, description):
    """Run a query and print results."""
    print(f"\n{description}")
    print("-" * 60)

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s"
        )

        if result.status.state == StatementState.SUCCEEDED:
            # Print column headers
            if result.manifest and result.manifest.schema:
                cols = [c.name for c in result.manifest.schema.columns]
                print(" | ".join(cols))
                print("-" * 60)

            # Print data
            if result.result and result.result.data_array:
                for row in result.result.data_array:
                    print(" | ".join(str(v) if v else "NULL" for v in row))
            else:
                print("(no data returned)")
        else:
            print(f"Query failed: {result.status.state}")
            if result.status.error:
                print(f"Error: {result.status.error.message}")

    except Exception as e:
        print(f"Error: {e}")


def main():
    print("=" * 60)
    print("Verifying ADF Parity Fixes")
    print("=" * 60)

    creds = load_credentials()
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find SQL warehouse
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("ERROR: No SQL warehouse found")
        return 1

    warehouse_id = warehouses[0].id
    print(f"Using warehouse: {warehouses[0].name}")

    # Check LinkedUserId in silver_worker
    run_query(w, warehouse_id, """
        SELECT
            COUNT(*) as total_workers,
            COUNT(LinkedUserId) as with_linked_user_id,
            COUNT(DISTINCT LinkedUserId) as unique_linked_user_ids
        FROM wakecap_prod.silver.silver_worker
    """, "1. LinkedUserId in silver_worker")

    # Check silver_resource_device columns (no DeletedAt/ProjectId - source doesn't have them)
    run_query(w, warehouse_id, """
        SELECT
            COUNT(*) as total_assignments,
            COUNT(DeviceId) as with_device_id,
            COUNT(WorkerId) as with_worker_id,
            COUNT(AssignedAt) as with_assigned_at,
            COUNT(UnassignedAt) as with_unassigned_at
        FROM wakecap_prod.silver.silver_resource_device
    """, "2. silver_resource_device columns (DeviceId, WorkerId, AssignedAt, UnassignedAt)")

    # Check ApprovedByWorkerId resolution in Gold
    run_query(w, warehouse_id, """
        SELECT
            COUNT(*) as total_attendance,
            COUNT(ApprovedByWorkerID) as resolved_approved_by,
            ROUND(COUNT(ApprovedByWorkerID) * 100.0 / COUNT(*), 2) as pct_resolved
        FROM wakecap_prod.gold.gold_fact_reported_attendance
    """, "3. ApprovedByWorkerId Resolution in Gold")

    # Check date range in Gold
    run_query(w, warehouse_id, """
        SELECT
            MIN(ShiftLocalDate) as min_date,
            MAX(ShiftLocalDate) as max_date,
            COUNT(*) as total_rows
        FROM wakecap_prod.gold.gold_fact_reported_attendance
    """, "4. Date Range in gold_fact_reported_attendance")

    # Check if any dates are outside valid range (should be 0)
    run_query(w, warehouse_id, """
        SELECT
            COUNT(*) as rows_outside_range
        FROM wakecap_prod.gold.gold_fact_reported_attendance
        WHERE ShiftLocalDate < '2000-01-01' OR ShiftLocalDate > '2100-01-01'
    """, "5. Rows Outside Valid Date Range (should be 0)")

    print("\n" + "=" * 60)
    print("Verification Complete")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
