"""Debug dimension loading issues."""
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
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Config
TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
DIM_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project_dw"
DIM_OBSERVATION_DISCRIMINATOR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_discriminator"
DIM_OBSERVATION_SOURCE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_source"
DIM_OBSERVATION_TYPE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_type"
TARGET_EXT_SOURCE_ALIAS = 15

# ExtSourceIDAlias UDF
@F.udf("int")
def ext_source_id_alias(ext_source_id):
    if ext_source_id is None:
        return None
    if ext_source_id in (14, 15, 16, 17, 18, 19):
        return 15
    if ext_source_id in (1, 2, 11, 12):
        return 2
    return ext_source_id

def load_dimension_with_dedup(table_path, partition_col, id_col, match_col=None):
    if match_col is None:
        match_col = partition_col

    df = spark.table(table_path)

    print(f"  Loading {table_path}...")
    print(f"    Columns: {df.columns}")
    print(f"    Count before filter: {df.count()}")

    # Add ExtSourceIDAlias column
    df = df.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))

    # Filter to matching source alias
    df = df.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)
    print(f"    Count after ExtSourceIDAlias filter: {df.count()}")

    # Apply ROW_NUMBER for deduplication
    window = Window.partitionBy(partition_col, "ExtSourceIDAlias").orderBy(F.lit(1))
    df = df.withColumn("rn", F.row_number().over(window))
    df = df.filter(F.col("rn") == 1).drop("rn")

    # Select only needed columns
    select_cols = [match_col, id_col]
    return df.select(*select_cols)

print("=== Testing Dimension Loading ===")

# Test Project
try:
    print("\\n1. Project:")
    dim_project = load_dimension_with_dedup(
        DIM_PROJECT,
        partition_col="ExtProjectID",
        id_col="ProjectID",
        match_col="ExtProjectID"
    ).withColumnRenamed("ProjectID", "ProjectID2")
    print(f"    Final count: {dim_project.count()}")
except Exception as e:
    print(f"    ERROR: {e}")

# Test Discriminator
try:
    print("\\n2. Discriminator:")
    dim_discriminator = load_dimension_with_dedup(
        DIM_OBSERVATION_DISCRIMINATOR,
        partition_col="ObservationDiscriminator",
        id_col="ObservationDiscriminatorID",
        match_col="ObservationDiscriminator"
    )
    print(f"    Final count: {dim_discriminator.count()}")
    print(f"    Sample:")
    dim_discriminator.show(5)
except Exception as e:
    print(f"    ERROR: {e}")

# Test Source
try:
    print("\\n3. Source:")
    dim_source = load_dimension_with_dedup(
        DIM_OBSERVATION_SOURCE,
        partition_col="ObservationSource",
        id_col="ObservationSourceID",
        match_col="ObservationSource"
    )
    print(f"    Final count: {dim_source.count()}")
except Exception as e:
    print(f"    ERROR: {e}")

# Test Type
try:
    print("\\n4. Type:")
    dim_type = load_dimension_with_dedup(
        DIM_OBSERVATION_TYPE,
        partition_col="ObservationType",
        id_col="ObservationTypeID",
        match_col="ObservationType"
    )
    print(f"    Final count: {dim_type.count()}")
except Exception as e:
    print(f"    ERROR: {e}")

print("\\n=== Debug Complete ===")
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
