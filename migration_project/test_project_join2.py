"""Test project dimension and join with string UUID."""
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

# Test the fixed Project dimension loading and join
cmd = '''
from pyspark.sql import functions as F

DIM_PROJECT = "wakecap_prod.silver.silver_project"

print("=== Test Fixed Project Dimension (String UUID) ===")

# Load Project dimension using string join
dim_project = spark.table(DIM_PROJECT).select(
    F.col("ProjectId").alias("ExtProjectID"),  # UUID string
    F.col("ProjectId").alias("ProjectID2")      # UUID string - no surrogate key available
)
print(f"Project dimension rows: {dim_project.count()}")
print(f"Project columns: {dim_project.columns}")

# Sample values
print("\\nSample Project values:")
dim_project.show(5, truncate=False)

print("\\n=== Test Join (String UUID) ===")

# Load source
source = spark.table("wakecap_prod.raw.observation_observation").limit(1000)

# Show sample ProjectId values from source
print("\\nSample source ProjectId values:")
source.select("ProjectId").distinct().limit(5).show(truncate=False)

# Do the join - both are strings now
joined = source.alias("ta").join(
    F.broadcast(dim_project).alias("t1"),
    F.col("ta.ProjectId") == F.col("t1.ExtProjectID"),
    "inner"
)

count = joined.count()
print(f"\\nJoined rows: {count}")

if count > 0:
    print("\\n=== SUCCESS - Join worked! ===")
    print("\\nSample joined row ProjectID2 values:")
    joined.select("ta.ProjectId", "t1.ProjectID2").limit(5).show(truncate=False)
else:
    print("\\n=== WARNING - No matches found! ===")
    print("Check if ProjectId formats match between source and dimension")
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
                print(cause[:3000])
        else:
            print(json.dumps(results, indent=2, default=str)[:3000])
        break
    time.sleep(2)

# Destroy context
requests.post(f"{host}/api/1.2/contexts/destroy", headers=headers, json={
    "clusterId": cluster_id,
    "contextId": context_id
})
