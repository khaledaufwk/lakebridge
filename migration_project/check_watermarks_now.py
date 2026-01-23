#!/usr/bin/env python3
"""Check watermark state for DeviceLocation tables."""
import sys
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

    # Query watermarks for DeviceLocation tables
    query = """
    SELECT
        source_table,
        watermark_column,
        last_watermark_timestamp,
        last_load_status,
        last_load_row_count,
        last_load_end_time
    FROM wakecap_prod.migration._timescaledb_watermarks
    WHERE source_system = 'timescaledb'
      AND source_schema = 'public'
      AND source_table IN ('DeviceLocation', 'DeviceLocationSummary')
    ORDER BY source_table
    """

    print("Checking watermarks for DeviceLocation tables...")
    print("=" * 80)

    # Use SQL warehouse
    warehouses = list(w.warehouses.list())
    serverless = None
    for wh in warehouses:
        if "serverless" in wh.name.lower():
            serverless = wh
            break

    if not serverless:
        print("No serverless warehouse found")
        return 1

    # Execute query
    result = w.statement_execution.execute_statement(
        warehouse_id=serverless.id,
        statement=query,
        wait_timeout="30s"
    )

    if result.result and result.result.data_array:
        for row in result.result.data_array:
            print(f"\nTable: {row[0]}")
            print(f"  Watermark Column: {row[1]}")
            print(f"  Last Watermark: {row[2]}")
            print(f"  Last Status: {row[3]}")
            print(f"  Last Row Count: {row[4]}")
            print(f"  Last Load Time: {row[5]}")
    else:
        print("No watermarks found!")

    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
