"""
Diagnose Gold Layer Prerequisites

Checks if all required Silver tables and columns exist for Gold layer transformation.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

def load_credentials():
    """Load Databricks credentials from credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            creds = yaml.safe_load(f)
            return creds.get("databricks", {})
    return {}


def main():
    from databricks.sdk import WorkspaceClient

    creds = load_credentials()
    host = creds.get("host", os.environ.get("DATABRICKS_HOST"))
    token = creds.get("token", os.environ.get("DATABRICKS_TOKEN"))

    if not host or not token:
        print("Error: Databricks credentials not found")
        sys.exit(1)

    w = WorkspaceClient(host=host, token=token)
    print(f"Connected to: {host}\n")

    # Get SQL warehouse
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("Error: No SQL warehouse available")
        sys.exit(1)

    warehouse_id = warehouses[0].id
    print(f"Using warehouse: {warehouses[0].name}\n")

    catalog = "wakecap_prod"

    # Check schemas
    print("=" * 60)
    print("CHECKING SCHEMAS")
    print("=" * 60)

    schemas_to_check = ["silver", "gold", "migration"]
    for schema in schemas_to_check:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'",
            wait_timeout="30s"
        )
        exists = result.result and result.result.data_array and len(result.result.data_array) > 0
        status = "EXISTS" if exists else "MISSING"
        print(f"  {catalog}.{schema}: {status}")

    # Check required Silver tables
    print("\n" + "=" * 60)
    print("CHECKING SILVER TABLES")
    print("=" * 60)

    required_tables = [
        "silver_fact_workers_history",
        "silver_device_assignment",
        "silver_worker",
        "silver_crew_assignment",
        "silver_workshift_assignment",
        "silver_workshift_day",
        "silver_project",
        "silver_zone",
        "silver_location_assignment",
    ]

    for table in required_tables:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"DESCRIBE TABLE {catalog}.silver.{table}",
                wait_timeout="30s"
            )
            if result.status.state.value == "SUCCEEDED":
                row_count_result = w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=f"SELECT COUNT(*) FROM {catalog}.silver.{table}",
                    wait_timeout="60s"
                )
                if row_count_result.result and row_count_result.result.data_array:
                    count = row_count_result.result.data_array[0][0]
                    print(f"  {table}: EXISTS ({count:,} rows)")
                else:
                    print(f"  {table}: EXISTS (row count unknown)")
            else:
                print(f"  {table}: ERROR - {result.status.error}")
        except Exception as e:
            print(f"  {table}: MISSING or ERROR - {str(e)[:50]}")

    # Check watermark table
    print("\n" + "=" * 60)
    print("CHECKING WATERMARK TABLE")
    print("=" * 60)

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SELECT * FROM {catalog}.migration._gold_watermarks",
            wait_timeout="30s"
        )
        if result.status.state.value == "SUCCEEDED" and result.result:
            print(f"  _gold_watermarks: EXISTS")
            if result.result.data_array:
                for row in result.result.data_array:
                    print(f"    - {row}")
            else:
                print(f"    (no entries)")
        else:
            print(f"  _gold_watermarks: ERROR - {result.status.error}")
    except Exception as e:
        print(f"  _gold_watermarks: MISSING or ERROR - {str(e)[:50]}")

    # Check silver_fact_workers_history columns
    print("\n" + "=" * 60)
    print("CHECKING silver_fact_workers_history COLUMNS")
    print("=" * 60)

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE TABLE {catalog}.silver.silver_fact_workers_history",
            wait_timeout="30s"
        )
        if result.status.state.value == "SUCCEEDED" and result.result and result.result.data_array:
            print("  Columns found:")
            for row in result.result.data_array:
                col_name = row[0] if row else "?"
                col_type = row[1] if len(row) > 1 else "?"
                print(f"    - {col_name}: {col_type}")
        else:
            print(f"  Error: {result.status.error if result.status.error else 'No columns returned'}")
    except Exception as e:
        print(f"  Error: {e}")

    # Check Gold schema and table
    print("\n" + "=" * 60)
    print("CHECKING GOLD TABLES")
    print("=" * 60)

    gold_tables = ["gold_fact_workers_history", "gold_fact_workers_shifts"]
    for table in gold_tables:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"DESCRIBE TABLE {catalog}.gold.{table}",
                wait_timeout="30s"
            )
            if result.status.state.value == "SUCCEEDED":
                print(f"  {table}: EXISTS")
            else:
                print(f"  {table}: DOES NOT EXIST YET (expected)")
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
                print(f"  {table}: DOES NOT EXIST YET (expected)")
            else:
                print(f"  {table}: ERROR - {str(e)[:50]}")

    print("\n" + "=" * 60)
    print("DIAGNOSIS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
