"""Find project table with UUID mapping."""
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
# Find project-related tables
print("=== Project-related RAW tables ===")
raw_tables = spark.sql("SHOW TABLES IN wakecap_prod.raw").collect()
for t in raw_tables:
    if "project" in t.tableName.lower():
        print(f"  {t.tableName}")

# Check timescale tables for project info
print("\\n=== Timescale tables with 'project' ===")
for t in raw_tables:
    if "timescale" in t.tableName.lower() and "project" in t.tableName.lower():
        print(f"  {t.tableName}")
        try:
            df = spark.table(f"wakecap_prod.raw.{t.tableName}")
            df.printSchema()
        except Exception as e:
            print(f"    Error: {e}")

# Check if silver has a project with UUID
print("\\n=== Silver tables ===")
silver_tables = spark.sql("SHOW TABLES IN wakecap_prod.silver").collect()
for t in silver_tables:
    if "project" in t.tableName.lower():
        print(f"  {t.tableName}")
        try:
            df = spark.table(f"wakecap_prod.silver.{t.tableName}")
            df.printSchema()
        except Exception as e:
            print(f"    Error: {e}")

# Option: Skip project join and use ProjectId as-is
print("\\n=== Proposed Solution ===")
print("Since silver_project uses integer ProjectId and observations use UUID ProjectId,")
print("we have a schema mismatch. Options:")
print("1. Load Project dimension from SQL Server DW (has ExtProjectID UUID)")
print("2. Store observation.ProjectId as ExtProjectID directly in fact table (no surrogate key lookup)")
print("3. Create a mapping table between UUID and integer project IDs")
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
