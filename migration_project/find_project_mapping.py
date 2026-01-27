"""Find how observations map to projects."""
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
# Check raw project table for UUID columns
print("=== raw.project_project Schema ===")
try:
    proj = spark.table("wakecap_prod.raw.project_project")
    proj.printSchema()
    print("\\nSample data:")
    proj.limit(3).show(truncate=False)
except Exception as e:
    print(f"No raw project table: {e}")

# List all raw tables
print("\\n=== All RAW tables ===")
raw_tables = spark.sql("SHOW TABLES IN wakecap_prod.raw").collect()
for t in raw_tables[:30]:
    print(f"  {t.tableName}")

# Check if there's an ID column in observations that's numeric
print("\\n=== Check observation columns ===")
obs = spark.table("wakecap_prod.raw.observation_observation")
print("Columns with 'id' in name:")
for c in obs.columns:
    if "id" in c.lower():
        print(f"  {c}")

# Check if ProjectId in observation is actually an ExternalId
print("\\n=== Sample observation with all ID columns ===")
obs.select("Id", "ExternalId", "ProjectId", "CompanyId", "SpaceId", "ZoneId", "ResourceId", "DeviceId").limit(3).show(truncate=False)
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
for i in range(30):
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
