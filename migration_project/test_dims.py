"""Test dimension tables and joins directly on cluster."""
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

# Test dimension tables
cmd = '''
from pyspark.sql import functions as F

print("=== Dimension Tables ===")
dims = [
    ("silver_project", "wakecap_prod.silver.silver_project"),
    ("silver_observation_discriminator", "wakecap_prod.silver.silver_observation_discriminator"),
    ("silver_observation_source", "wakecap_prod.silver.silver_observation_source"),
    ("silver_observation_type", "wakecap_prod.silver.silver_observation_type")
]

for name, table in dims:
    try:
        df = spark.table(table)
        count = df.count()
        cols = df.columns
        print(f"  [OK] {name}: {count} rows")
        print(f"       Columns: {cols}")
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")

print("")
print("=== Test Join with Project ===")
try:
    source = spark.table("wakecap_prod.raw.observation_observation").limit(1000)
    source = source.withColumn("ProjectId_int", F.col("ProjectId").cast("int"))

    project = spark.table("wakecap_prod.silver.silver_project")

    joined = source.alias("s").join(
        F.broadcast(project).alias("p"),
        F.col("s.ProjectId_int") == F.col("p.ExtProjectID"),
        "inner"
    )
    count = joined.count()
    print(f"Join result: {count} rows")
except Exception as e:
    import traceback
    print(f"ERROR: {e}")
    traceback.print_exc()
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
            print(json.dumps(results, indent=2, default=str)[:3000])
        break
    time.sleep(2)

# Destroy context
requests.post(f"{host}/api/1.2/contexts/destroy", headers=headers, json={
    "clusterId": cluster_id,
    "contextId": context_id
})
