#!/usr/bin/env python3
"""
Verify watermarks are reset and drop DeviceLocation tables.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient


def execute_sql(w, warehouse_id, statement, description="Query"):
    """Execute SQL and wait for result."""
    print(f"\n{description}...")
    print(f"  SQL: {statement[:100]}...")

    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s"
    )

    # Wait for async execution
    if result.status.state.value in ["PENDING", "RUNNING"]:
        statement_id = result.statement_id
        for _ in range(30):  # Wait up to 2.5 minutes
            result = w.statement_execution.get_statement(statement_id)
            if result.status.state.value not in ["PENDING", "RUNNING"]:
                break
            time.sleep(5)

    print(f"  Status: {result.status.state.value}")
    if result.status.error:
        print(f"  Error: {result.status.error.message}")

    return result


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find SQL warehouse
    print("Finding SQL warehouse...")
    warehouse = None
    for wh in w.warehouses.list():
        warehouse = wh
        print(f"  Found: {wh.name} ({wh.id}) - State: {wh.state.value}")
        break

    if not warehouse:
        print("No SQL warehouse found!")
        return 1

    # Start if not running
    if warehouse.state.value != "RUNNING":
        print(f"\nStarting warehouse {warehouse.name}...")
        w.warehouses.start(warehouse.id)
        for i in range(60):
            status = w.warehouses.get(warehouse.id)
            if status.state.value == "RUNNING":
                print(f"  Warehouse started!")
                break
            print(f"  State: {status.state.value}")
            time.sleep(5)
        else:
            print("Warehouse failed to start!")
            return 1

    # Step 1: Verify watermarks are deleted
    print("\n" + "=" * 60)
    print("Verifying watermarks are deleted")
    print("=" * 60)

    result = execute_sql(
        w, warehouse.id,
        """
        SELECT source_table, watermark_column, last_load_status
        FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE source_system = 'timescaledb'
          AND source_schema = 'public'
          AND source_table IN ('DeviceLocation', 'DeviceLocationSummary')
        """,
        "Checking watermarks"
    )

    if result.result and result.result.data_array:
        print("\nWARNING: Watermarks still exist:")
        for row in result.result.data_array:
            print(f"  {row}")
    else:
        print("\n[OK] No watermark entries for DeviceLocation tables")

    # Step 2: Drop existing tables
    print("\n" + "=" * 60)
    print("Dropping existing tables")
    print("=" * 60)

    for table in ['timescale_devicelocation', 'timescale_devicelocationsummary']:
        execute_sql(
            w, warehouse.id,
            f"DROP TABLE IF EXISTS wakecap_prod.raw.{table}",
            f"Dropping {table}"
        )

    # Step 3: Verify tables are dropped
    print("\n" + "=" * 60)
    print("Verifying tables are dropped")
    print("=" * 60)

    for table in ['timescale_devicelocation', 'timescale_devicelocationsummary']:
        result = execute_sql(
            w, warehouse.id,
            f"SHOW TABLES IN wakecap_prod.raw LIKE '{table}'",
            f"Checking {table}"
        )
        if result.result and result.result.data_array:
            print(f"  WARNING: {table} still exists!")
        else:
            print(f"  [OK] {table} dropped")

    print("\n" + "=" * 60)
    print("RESET COMPLETE")
    print("=" * 60)
    print("""
Ready for full load. Run the bronze_loader_devicelocation notebook in Databricks UI:

1. Navigate to: /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation
2. Attach to a cluster with Unity Catalog access
3. Set parameters:
   - load_mode = full
   - run_optimize = yes
4. Run all cells
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
