"""Verify fact observations results."""
import sys
from pathlib import Path
import requests
import time
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

# Load credentials
creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
with open(creds_path) as f:
    creds = yaml.safe_load(f).get("databricks", {})

host = creds["host"].rstrip("/")
token = creds["token"]
cluster_id = "0118-134705-lklfkwvh"

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Create execution context
resp = requests.post(f"{host}/api/1.2/contexts/create", headers=headers, json={
    "clusterId": cluster_id,
    "language": "python"
})
ctx = resp.json()
context_id = ctx.get("id")
print(f"Context ID: {context_id}")

cmd = '''
TARGET_TABLE = "wakecap_prod.gold.fact_observations"
STALLED_TABLE = "wakecap_prod.silver.silver_observations_stalled"
SOURCE_TABLE = "wakecap_prod.raw.observation_observation"
EXT_SOURCE_ID = 19

print("=" * 60)
print("FACT OBSERVATIONS VERIFICATION")
print("=" * 60)

# Row counts
source_total = spark.table(SOURCE_TABLE).count()
target_total = spark.table(TARGET_TABLE).count()
target_ext19 = spark.table(TARGET_TABLE).filter(f"ExtSourceID = {EXT_SOURCE_ID}").count()
stalled_total = spark.table(STALLED_TABLE).count()

print(f"\\nRow Counts:")
print(f"  Source table: {source_total:,}")
print(f"  Target table: {target_total:,}")
print(f"  Target (ExtSourceID=19): {target_ext19:,}")
print(f"  Stalled table: {stalled_total:,}")

# Sample data from target
print("\\n" + "=" * 60)
print("SAMPLE DATA FROM TARGET")
print("=" * 60)

target_df = spark.table(TARGET_TABLE)
print(f"\\nColumns: {len(target_df.columns)}")
print("\\nSample rows:")
target_df.select(
    "ExtObservationID",
    "ExtSourceID",
    "ProjectID",
    "ObservationDiscriminatorID",
    "ObservationSourceID",
    "ObservationTypeID",
    "TimestampUTC"
).limit(5).show()

# Check NULL rates
print("\\n" + "=" * 60)
print("NULL RATES FOR DIMENSION IDs")
print("=" * 60)

from pyspark.sql import functions as F

dim_cols = [
    "ProjectID", "ObservationDiscriminatorID", "ObservationSourceID",
    "ObservationTypeID", "ObservationSeverityID", "ObservationStatusID",
    "CompanyID", "FloorID", "ZoneID", "WorkerID", "DeviceID"
]

total = target_df.count()
for col in dim_cols:
    nulls = target_df.filter(F.col(col).isNull()).count()
    pct = (nulls / total * 100) if total > 0 else 0
    print(f"  {col}: {nulls:,} NULL ({pct:.1f}%)")

print("\\n" + "=" * 60)
print("SUCCESS!")
print("=" * 60)
'''

resp = requests.post(f"{host}/api/1.2/commands/execute", headers=headers, json={
    "clusterId": cluster_id,
    "contextId": context_id,
    "language": "python",
    "command": cmd
})
cmd_resp = resp.json()
command_id = cmd_resp.get("id")
print(f"Command ID: {command_id}")

# Wait for completion
for i in range(60):
    resp = requests.get(
        f"{host}/api/1.2/commands/status",
        headers=headers,
        params={
            "clusterId": cluster_id,
            "contextId": context_id,
            "commandId": command_id
        }
    )
    status = resp.json()
    state = status.get("status")

    if state in ["Finished", "Error", "Cancelled"]:
        print(f"\n=== Result ({state}) ===")
        results = status.get("results", {})
        result_type = results.get("resultType")
        if result_type == "text":
            print(results.get("data"))
        elif result_type == "error":
            summary = results.get("summary", "")
            print(f"Error: {summary}")
            cause = results.get("cause", "")
            if cause:
                print(cause[:2000])
        else:
            print(json.dumps(results, indent=2, default=str)[:2000])
        break
    time.sleep(2)

# Destroy context
requests.post(f"{host}/api/1.2/contexts/destroy", headers=headers, json={
    "clusterId": cluster_id,
    "contextId": context_id
})
