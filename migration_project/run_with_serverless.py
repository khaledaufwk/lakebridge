#!/usr/bin/env python3
"""
Use Serverless SQL Warehouse to reset DeviceLocation watermarks.
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

    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s"
    )

    # Wait for async execution
    if result.status.state.value in ["PENDING", "RUNNING"]:
        statement_id = result.statement_id
        for _ in range(30):
            result = w.statement_execution.get_statement(statement_id)
            if result.status.state.value not in ["PENDING", "RUNNING"]:
                break
            time.sleep(5)

    print(f"  Status: {result.status.state.value}")
    if result.status.error:
        print(f"  Error: {result.status.error.message}")
    if result.result and result.result.data_array:
        for row in result.result.data_array:
            print(f"  {row}")

    return result


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Use Serverless SQL Warehouse
    print("Finding Serverless SQL warehouse...")
    warehouse = None
    for wh in w.warehouses.list():
        if "serverless" in wh.name.lower():
            warehouse = wh
            print(f"  Found: {wh.name} ({wh.id}) - State: {wh.state.value}")
            break

    if not warehouse:
        print("No Serverless SQL warehouse found!")
        return 1

    # Start if not running
    if warehouse.state.value != "RUNNING":
        print(f"\nStarting warehouse {warehouse.name}...")
        w.warehouses.start(warehouse.id)
        for i in range(120):  # Wait longer for serverless
            status = w.warehouses.get(warehouse.id)
            if status.state.value == "RUNNING":
                print(f"  Warehouse started!")
                break
            if i % 6 == 0:  # Print every 30 seconds
                print(f"  State: {status.state.value}")
            time.sleep(5)
        else:
            print("Warehouse failed to start!")
            return 1

    wh_id = warehouse.id

    print("\n" + "=" * 60)
    print("STEP 1: Delete DeviceLocation watermarks")
    print("=" * 60)

    execute_sql(w, wh_id,
        """
        DELETE FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE source_system = 'timescaledb'
          AND source_schema = 'public'
          AND source_table = 'DeviceLocation'
        """,
        "Deleting DeviceLocation watermark"
    )

    execute_sql(w, wh_id,
        """
        DELETE FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE source_system = 'timescaledb'
          AND source_schema = 'public'
          AND source_table = 'DeviceLocationSummary'
        """,
        "Deleting DeviceLocationSummary watermark"
    )

    print("\n" + "=" * 60)
    print("STEP 2: Drop existing tables")
    print("=" * 60)

    execute_sql(w, wh_id,
        "DROP TABLE IF EXISTS wakecap_prod.raw.timescale_devicelocation",
        "Dropping timescale_devicelocation"
    )

    execute_sql(w, wh_id,
        "DROP TABLE IF EXISTS wakecap_prod.raw.timescale_devicelocationsummary",
        "Dropping timescale_devicelocationsummary"
    )

    print("\n" + "=" * 60)
    print("STEP 3: Verify")
    print("=" * 60)

    execute_sql(w, wh_id,
        """
        SELECT source_table, watermark_column
        FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE source_system = 'timescaledb'
          AND source_table IN ('DeviceLocation', 'DeviceLocationSummary')
        """,
        "Checking remaining watermarks"
    )

    execute_sql(w, wh_id,
        "SHOW TABLES IN wakecap_prod.raw LIKE 'timescale_devicelocation%'",
        "Checking remaining tables"
    )

    print("\n" + "=" * 60)
    print("RESET COMPLETE")
    print("=" * 60)
    print("""
Step 1 done! Now run Step 2 manually in Databricks UI:

1. Open: /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation
2. Attach to: Migrate Compute - Khaled Auf (or any UC-enabled cluster)
3. Parameters:
   - load_mode = full
   - run_optimize = yes
4. Run All
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
