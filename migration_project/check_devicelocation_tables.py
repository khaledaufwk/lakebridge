#!/usr/bin/env python3
"""Check DeviceLocation target tables."""
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

    print("Checking DeviceLocation tables...")
    print("=" * 80)

    # Check if tables exist and get counts
    queries = [
        ("Table exists check", "SHOW TABLES IN wakecap_prod.raw LIKE 'timescale_device*'"),
        ("DeviceLocation count", "SELECT COUNT(*) as cnt FROM wakecap_prod.raw.timescale_devicelocation"),
        ("DeviceLocationSummary count", "SELECT COUNT(*) as cnt FROM wakecap_prod.raw.timescale_devicelocationsummary"),
        ("DeviceLocation max GeneratedAt", "SELECT MAX(GeneratedAt) as max_ts FROM wakecap_prod.raw.timescale_devicelocation"),
        ("DeviceLocationSummary max GeneratedAt", "SELECT MAX(GeneratedAt) as max_ts FROM wakecap_prod.raw.timescale_devicelocationsummary"),
    ]

    for name, query in queries:
        print(f"\n{name}:")
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=serverless.id,
                statement=query,
                wait_timeout="50s"
            )
            if result.result and result.result.data_array:
                for row in result.result.data_array:
                    print(f"  {row}")
            elif result.status:
                print(f"  Status: {result.status.state}")
        except Exception as e:
            print(f"  Error: {e}")

    # Check watermarks specifically for these tables
    print("\n" + "=" * 80)
    print("Checking watermarks table for DeviceLocation entries...")

    wm_query = """
    SELECT * FROM wakecap_prod.migration._timescaledb_watermarks
    WHERE source_table LIKE '%Device%'
    """

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=serverless.id,
            statement=wm_query,
            wait_timeout="30s"
        )
        if result.result and result.result.data_array:
            print(f"Found {len(result.result.data_array)} entries")
            for row in result.result.data_array:
                print(f"  {row}")
        else:
            print("No DeviceLocation watermark entries found!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
