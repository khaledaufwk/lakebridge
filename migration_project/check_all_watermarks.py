#!/usr/bin/env python3
"""Check all watermarks in the table."""
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

    # Query all watermarks
    query = """
    SELECT
        source_table,
        watermark_column,
        last_watermark_timestamp,
        last_load_status,
        last_load_row_count
    FROM wakecap_prod.migration._timescaledb_watermarks
    WHERE source_system = 'timescaledb'
    ORDER BY source_table
    """

    print("All watermarks in table...")
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
        print(f"Found {len(result.result.data_array)} watermarks:\n")
        for row in result.result.data_array:
            print(f"  {row[0]}: {row[1]} = {row[2]} ({row[3]}, {row[4]} rows)")
    else:
        print("No watermarks found in table!")

    # Also check target tables
    print("\n" + "=" * 80)
    print("Checking target table row counts...")

    for table in ['timescale_devicelocation', 'timescale_devicelocationsummary']:
        count_query = f"SELECT COUNT(*) as cnt FROM wakecap_prod.raw.{table}"
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=serverless.id,
                statement=count_query,
                wait_timeout="60s"
            )
            if result.result and result.result.data_array:
                count = result.result.data_array[0][0]
                print(f"  {table}: {count:,} rows")
        except Exception as e:
            print(f"  {table}: Error - {e}")

if __name__ == "__main__":
    main()
