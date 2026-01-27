"""Simple dependency check using catalog API."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from databricks.sdk import WorkspaceClient

# Load credentials
creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
with open(creds_path) as f:
    creds = yaml.safe_load(f).get("databricks", {})

w = WorkspaceClient(host=creds["host"], token=creds["token"])

print("=" * 70)
print("Checking Dependencies for delta_sync_fact_observations")
print("=" * 70)

# Tables to check
tables = {
    "SOURCE (required)": [
        "wakecap_prod.raw.observation_observation",
    ],
    "REQUIRED DIMENSIONS (INNER joins)": [
        "wakecap_prod.silver.silver_project",
        "wakecap_prod.silver.silver_observation_discriminator",
        "wakecap_prod.silver.silver_observation_source",
        "wakecap_prod.silver.silver_observation_type",
    ],
    "OPTIONAL DIMENSIONS (LEFT joins)": [
        "wakecap_prod.silver.silver_observation_severity",
        "wakecap_prod.silver.silver_observation_status",
        "wakecap_prod.silver.silver_observation_clinic_violation_status",
        "wakecap_prod.silver.silver_organization",
        "wakecap_prod.silver.silver_floor",
        "wakecap_prod.silver.silver_zone",
        "wakecap_prod.silver.silver_worker",
        "wakecap_prod.silver.silver_device",
    ],
}

missing_required = []
missing_optional = []

for category, table_list in tables.items():
    print(f"\n{category}:")
    print("-" * 70)
    for table in table_list:
        parts = table.split(".")
        try:
            t = w.tables.get(full_name=table)
            print(f"  [OK] {table}")
        except Exception as e:
            err = str(e)
            if "NOT_FOUND" in err or "does not exist" in err.lower():
                print(f"  [MISSING] {table}")
                if "REQUIRED" in category or "SOURCE" in category:
                    missing_required.append(table)
                else:
                    missing_optional.append(table)
            else:
                print(f"  [ERROR] {table}: {err[:60]}")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

if missing_required:
    print(f"\n[BLOCKER] {len(missing_required)} required tables missing:")
    for t in missing_required:
        print(f"  - {t}")
else:
    print("\n[OK] All required tables exist!")

if missing_optional:
    print(f"\n[WARNING] {len(missing_optional)} optional tables missing:")
    for t in missing_optional:
        print(f"  - {t}")
    print("  (Notebook will still work but these dimension IDs will be NULL)")
