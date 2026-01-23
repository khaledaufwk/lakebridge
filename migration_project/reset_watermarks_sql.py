#!/usr/bin/env python3
"""
Reset DeviceLocation watermarks using SQL warehouse (not compute cluster).
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find a SQL warehouse
    print("Finding SQL warehouse...")
    warehouses = list(w.warehouses.list())

    warehouse = None
    for wh in warehouses:
        print(f"  {wh.name} ({wh.id}) - State: {wh.state}")
        if warehouse is None:
            warehouse = wh

    if not warehouse:
        print("No SQL warehouse found!")
        return 1

    # Start warehouse if stopped
    if warehouse.state.value != "RUNNING":
        print(f"\nStarting warehouse {warehouse.name}...")
        w.warehouses.start(warehouse.id)

        # Wait for it to start
        for i in range(60):  # Wait up to 5 minutes
            status = w.warehouses.get(warehouse.id)
            print(f"  State: {status.state.value}")
            if status.state.value == "RUNNING":
                break
            time.sleep(5)

    print(f"\nUsing warehouse: {warehouse.name} ({warehouse.id})")

    # STEP 1: Check current watermarks
    print("\n" + "=" * 60)
    print("STEP 1: Check current DeviceLocation watermarks")
    print("=" * 60)

    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse.id,
        statement="""
            SELECT source_table, watermark_column, last_watermark_timestamp, last_load_status
            FROM wakecap_prod.migration._timescaledb_watermarks
            WHERE source_system = 'timescaledb'
              AND source_schema = 'public'
              AND source_table IN ('DeviceLocation', 'DeviceLocationSummary')
        """,
        wait_timeout="30s"
    )

    print(f"Query status: {result.status.state}")
    if result.result and result.result.data_array:
        print("Current watermarks:")
        for row in result.result.data_array:
            print(f"  {row}")
    else:
        print("No existing watermark entries found")

    # STEP 2: Delete watermarks
    print("\n" + "=" * 60)
    print("STEP 2: Delete DeviceLocation watermarks")
    print("=" * 60)

    for table in ['DeviceLocation', 'DeviceLocationSummary']:
        print(f"\nDeleting watermark for {table}...")
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse.id,
            statement=f"""
                DELETE FROM wakecap_prod.migration._timescaledb_watermarks
                WHERE source_system = 'timescaledb'
                  AND source_schema = 'public'
                  AND source_table = '{table}'
            """,
            wait_timeout="30s"
        )
        print(f"  Status: {result.status.state}")

    # STEP 3: Verify deletion
    print("\n" + "=" * 60)
    print("STEP 3: Verify watermarks deleted")
    print("=" * 60)

    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse.id,
        statement="""
            SELECT COUNT(*) as cnt
            FROM wakecap_prod.migration._timescaledb_watermarks
            WHERE source_system = 'timescaledb'
              AND source_schema = 'public'
              AND source_table IN ('DeviceLocation', 'DeviceLocationSummary')
        """,
        wait_timeout="30s"
    )

    if result.result and result.result.data_array:
        count = result.result.data_array[0][0]
        if count == '0' or count == 0:
            print("Watermarks successfully deleted!")
        else:
            print(f"Warning: {count} watermark entries still exist")
    else:
        print("Could not verify deletion")

    # STEP 4: Drop existing tables (optional)
    print("\n" + "=" * 60)
    print("STEP 4: Drop existing DeviceLocation tables")
    print("=" * 60)

    for table in ['timescale_devicelocation', 'timescale_devicelocationsummary']:
        full_name = f"wakecap_prod.raw.{table}"
        print(f"\nDropping {full_name}...")
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse.id,
                statement=f"DROP TABLE IF EXISTS {full_name}",
                wait_timeout="50s"
            )
            print(f"  Status: {result.status.state}")
        except Exception as e:
            print(f"  Error: {e}")

    print("\n" + "=" * 60)
    print("WATERMARK RESET COMPLETE")
    print("=" * 60)
    print("""
Next steps:
1. Run the bronze_loader_devicelocation notebook in Databricks UI
2. Set load_mode = full
3. Set run_optimize = yes
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
