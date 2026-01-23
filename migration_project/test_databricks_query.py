#!/usr/bin/env python3
"""Test Databricks SQL query via SDK."""
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

    # List SQL warehouses
    print("SQL Warehouses:")
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        print(f"  {wh.name} ({wh.id}) - State: {wh.state}")

    # Try to run a query if warehouse is available
    running_wh = None
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            running_wh = wh
            break

    if running_wh:
        print(f"\nUsing warehouse: {running_wh.name}")

        # Query watermarks for DeviceLocation
        result = w.statement_execution.execute_statement(
            warehouse_id=running_wh.id,
            statement="""
                SELECT source_table, watermark_column, last_watermark_timestamp, last_load_status
                FROM wakecap_prod.migration._timescaledb_watermarks
                WHERE source_table IN ('DeviceLocation', 'DeviceLocationSummary')
            """,
            wait_timeout="30s"
        )

        print(f"\nQuery status: {result.status.state}")
        if result.manifest and result.manifest.schema and result.manifest.schema.columns:
            print("\nColumns:", [c.name for c in result.manifest.schema.columns])
        if result.result and result.result.data_array:
            print("\nData:")
            for row in result.result.data_array:
                print(f"  {row}")
        else:
            print("No data found (tables may not exist in watermarks)")
    else:
        print("\nNo running SQL warehouse found")


if __name__ == "__main__":
    main()
