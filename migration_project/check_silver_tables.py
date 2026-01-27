"""
Check if Silver layer tables exist in Databricks.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

def load_credentials():
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

    for schema in ["raw", "silver", "gold", "migration"]:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'",
            wait_timeout="30s"
        )
        exists = result.result and result.result.data_array and len(result.result.data_array) > 0
        status = "EXISTS" if exists else "MISSING"
        print(f"  {catalog}.{schema}: {status}")

    # Check key Silver tables
    print("\n" + "=" * 60)
    print("CHECKING KEY SILVER TABLES")
    print("=" * 60)

    silver_tables = [
        "silver_fact_workers_history",
        "silver_project",
        "silver_device",
        "silver_resource_device",
        "silver_worker",
        "silver_zone",
    ]

    silver_exists = 0
    for table in silver_tables:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"DESCRIBE TABLE {catalog}.silver.{table}",
                wait_timeout="30s"
            )
            if result.status.state.value == "SUCCEEDED":
                print(f"  {table}: EXISTS")
                silver_exists += 1
            else:
                print(f"  {table}: NOT FOUND")
        except Exception as e:
            print(f"  {table}: NOT FOUND")

    # Check raw tables (Bronze)
    print("\n" + "=" * 60)
    print("CHECKING KEY BRONZE/RAW TABLES")
    print("=" * 60)

    raw_tables = [
        "timescale_devicelocation",
        "timescale_company",
        "timescale_avldevice",
        "timescale_resourcedevice",
        "timescale_people",
        "timescale_zone",
    ]

    raw_exists = 0
    for table in raw_tables:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"DESCRIBE TABLE {catalog}.raw.{table}",
                wait_timeout="30s"
            )
            if result.status.state.value == "SUCCEEDED":
                print(f"  {table}: EXISTS")
                raw_exists += 1
            else:
                print(f"  {table}: NOT FOUND")
        except Exception as e:
            print(f"  {table}: NOT FOUND")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Silver tables found: {silver_exists}/{len(silver_tables)}")
    print(f"  Raw/Bronze tables found: {raw_exists}/{len(raw_tables)}")

    if silver_exists == 0:
        print("\n[WARNING] Silver layer is NOT deployed!")
        print("The Gold layer requires Silver tables.")
        print("\nOptions:")
        print("  1. Deploy Silver layer: python deploy_silver_layer.py")
        print("  2. Or modify Gold to use Bronze tables directly")
    elif silver_exists < len(silver_tables):
        print("\n[WARNING] Silver layer is partially deployed.")
        print("Some required tables are missing.")
    else:
        print("\n[OK] Silver layer appears to be fully deployed.")
        print("Gold layer should be able to run.")


if __name__ == "__main__":
    main()
