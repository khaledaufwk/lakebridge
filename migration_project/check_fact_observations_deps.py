"""
Check dependencies for delta_sync_fact_observations notebook.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from databricks.sdk import WorkspaceClient


def load_credentials():
    """Load Databricks credentials from credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            creds = yaml.safe_load(f)
            return creds.get("databricks", {})
    return {}


def main():
    print("=" * 70)
    print("Checking Dependencies for delta_sync_fact_observations")
    print("=" * 70)

    # Load credentials
    creds = load_credentials()
    host = creds.get("host", os.environ.get("DATABRICKS_HOST"))
    token = creds.get("token", os.environ.get("DATABRICKS_TOKEN"))

    if not host or not token:
        print("Error: Databricks credentials not found")
        sys.exit(1)

    # Initialize Databricks client
    w = WorkspaceClient(host=host, token=token)
    print(f"Connected to: {host}\n")

    # Tables to check
    source_table = "wakecap_prod.raw.timescale_observations"

    # Required dimension tables (INNER joins - must exist)
    required_dims = [
        ("Project", "wakecap_prod.silver.silver_project"),
        ("ObservationDiscriminator", "wakecap_prod.silver.silver_observation_discriminator"),
        ("ObservationSource", "wakecap_prod.silver.silver_observation_source"),
        ("ObservationType", "wakecap_prod.silver.silver_observation_type"),
    ]

    # Optional dimension tables (LEFT joins - nice to have)
    optional_dims = [
        ("ObservationSeverity", "wakecap_prod.silver.silver_observation_severity"),
        ("ObservationStatus", "wakecap_prod.silver.silver_observation_status"),
        ("ObservationClinicViolationStatus", "wakecap_prod.silver.silver_observation_clinic_violation_status"),
        ("Organization/Company", "wakecap_prod.silver.silver_organization"),
        ("Floor", "wakecap_prod.silver.silver_floor"),
        ("Zone", "wakecap_prod.silver.silver_zone"),
        ("Worker", "wakecap_prod.silver.silver_worker"),
        ("Device", "wakecap_prod.silver.silver_device"),
    ]

    def check_table(table_name):
        """Check if table exists and get row count."""
        try:
            # Use SQL to check table
            result = w.statement_execution.execute_statement(
                warehouse_id=get_warehouse_id(w),
                statement=f"SELECT COUNT(*) as cnt FROM {table_name}",
                wait_timeout="30s"
            )
            if result.result and result.result.data_array:
                count = result.result.data_array[0][0]
                return True, int(count)
            return True, 0
        except Exception as e:
            return False, str(e)[:50]

    def get_warehouse_id(w):
        """Get first available SQL warehouse."""
        warehouses = list(w.warehouses.list())
        for wh in warehouses:
            if wh.state and wh.state.value in ["RUNNING", "STARTING"]:
                return wh.id
        # Return first warehouse if none running
        if warehouses:
            return warehouses[0].id
        return None

    warehouse_id = get_warehouse_id(w)
    if not warehouse_id:
        print("[ERROR] No SQL warehouse available. Checking via catalog API instead.\n")
        # Fallback to catalog API
        def check_table_catalog(table_name):
            try:
                parts = table_name.split(".")
                if len(parts) == 3:
                    catalog, schema, table = parts
                    t = w.tables.get(f"{catalog}.{schema}.{table}")
                    return True, "exists"
            except Exception as e:
                return False, str(e)[:50]
        check_table = check_table_catalog

    print(f"Using warehouse: {warehouse_id}\n")

    # Check source table
    print("SOURCE TABLE:")
    print("-" * 70)
    exists, info = check_table(source_table)
    if exists:
        print(f"  [OK] {source_table}")
        if isinstance(info, int):
            print(f"       Rows: {info:,}")
    else:
        print(f"  [MISSING] {source_table}")
        print(f"            Error: {info}")

    # Check required dimensions
    print("\nREQUIRED DIMENSION TABLES (INNER joins):")
    print("-" * 70)
    required_missing = []
    for name, table in required_dims:
        exists, info = check_table(table)
        if exists:
            print(f"  [OK] {name}")
            print(f"       {table}")
            if isinstance(info, int):
                print(f"       Rows: {info:,}")
        else:
            print(f"  [MISSING] {name}")
            print(f"            {table}")
            required_missing.append(name)

    # Check optional dimensions
    print("\nOPTIONAL DIMENSION TABLES (LEFT joins):")
    print("-" * 70)
    optional_missing = []
    for name, table in optional_dims:
        exists, info = check_table(table)
        if exists:
            print(f"  [OK] {name}")
            print(f"       {table}")
            if isinstance(info, int):
                print(f"       Rows: {info:,}")
        else:
            print(f"  [MISSING] {name}")
            print(f"            {table}")
            optional_missing.append(name)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if not exists:
        print("\n[BLOCKER] Source table is missing!")
        print("  The notebook cannot run without the source observations table.")

    if required_missing:
        print(f"\n[BLOCKER] {len(required_missing)} required dimension tables missing:")
        for name in required_missing:
            print(f"  - {name}")
        print("\n  These are INNER joins - records will fail to process without them.")

    if optional_missing:
        print(f"\n[WARNING] {len(optional_missing)} optional dimension tables missing:")
        for name in optional_missing:
            print(f"  - {name}")
        print("\n  These are LEFT joins - the notebook will still work but those")
        print("  dimension IDs will be NULL in the output.")

    if not required_missing and exists:
        print("\n[OK] All required dependencies are present!")
        print("     The notebook is ready to run.")

    print("=" * 70)


if __name__ == "__main__":
    main()
