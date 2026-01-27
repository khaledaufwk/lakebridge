"""Check stalled table."""
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
STALLED_TABLE = "wakecap_prod.silver.silver_observations_stalled"
TARGET_TABLE = "wakecap_prod.gold.fact_observations"

print("=== Check Stalled Table ===")
try:
    exists = spark.catalog.tableExists(STALLED_TABLE)
    print(f"Stalled table exists: {exists}")

    if exists:
        df = spark.table(STALLED_TABLE)
        print(f"\\nSchema:")
        df.printSchema()
        print(f"\\nRow count: {df.count()}")
except Exception as e:
    print(f"Error: {e}")

print("\\n=== Check Target Table ===")
try:
    exists = spark.catalog.tableExists(TARGET_TABLE)
    print(f"Target table exists: {exists}")

    if exists:
        df = spark.table(TARGET_TABLE)
        print(f"\\nSchema:")
        df.printSchema()
        print(f"\\nRow count: {df.count()}")
except Exception as e:
    print(f"Error: {e}")
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
