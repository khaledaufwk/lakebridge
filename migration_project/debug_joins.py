"""Debug join issues."""
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
SOURCE_TABLE = "wakecap_prod.raw.observation_observation"
EXT_SOURCE_ID = 19
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
    df = df.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
    df = df.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)
    window = Window.partitionBy(partition_col, "ExtSourceIDAlias").orderBy(F.lit(1))
    df = df.withColumn("rn", F.row_number().over(window))
    df = df.filter(F.col("rn") == 1).drop("rn")
    return df.select(match_col, id_col)

print("=== Loading Dimensions ===")

# Load dimensions
dim_project = load_dimension_with_dedup(
    f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project_dw",
    partition_col="ExtProjectID",
    id_col="ProjectID",
    match_col="ExtProjectID"
).withColumnRenamed("ProjectID", "ProjectID2")
print(f"Project: {dim_project.count()} rows")

dim_discriminator = load_dimension_with_dedup(
    f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_discriminator",
    partition_col="ObservationDiscriminator",
    id_col="ObservationDiscriminatorID",
    match_col="ObservationDiscriminator"
)
print(f"Discriminator: {dim_discriminator.count()} rows")

dim_source = load_dimension_with_dedup(
    f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_source",
    partition_col="ObservationSource",
    id_col="ObservationSourceID",
    match_col="ObservationSource"
)
print(f"Source: {dim_source.count()} rows")

dim_type = load_dimension_with_dedup(
    f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_type",
    partition_col="ObservationType",
    id_col="ObservationTypeID",
    match_col="ObservationType"
)
print(f"Type: {dim_type.count()} rows")

print("\\n=== Loading Source Data ===")

# Load source data (limit for testing)
source_df = spark.table(SOURCE_TABLE).limit(1000)
print(f"Source rows: {source_df.count()}")

# Alias source DataFrame for joins
batch_df = source_df.alias("ta")

# Add ExtSourceID constant
batch_df = batch_df.withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID))

print("\\n=== Testing Joins ===")

# Test 1: Project join
print("\\n1. Project join (case-insensitive):")
try:
    joined = batch_df.join(
        F.broadcast(dim_project).alias("t1"),
        F.lower(F.col("ta.ProjectId")) == F.lower(F.col("t1.ExtProjectID")),
        "inner"
    )
    count = joined.count()
    print(f"   Rows after join: {count}")
    batch_df = joined
except Exception as e:
    print(f"   ERROR: {e}")

# Test 2: Discriminator join
print("\\n2. Discriminator join:")
try:
    joined = batch_df.join(
        F.broadcast(dim_discriminator).alias("t2"),
        F.col("ta.Discriminator") == F.col("t2.ObservationDiscriminator"),
        "inner"
    )
    count = joined.count()
    print(f"   Rows after join: {count}")

    if count == 0:
        print("   No matches! Checking values...")
        print("   Source Discriminator values:")
        batch_df.select("ta.Discriminator").distinct().show(10)
        print("   Dimension values:")
        dim_discriminator.show(10)
    else:
        batch_df = joined
except Exception as e:
    print(f"   ERROR: {e}")

# Test 3: Source join
print("\\n3. Source join:")
try:
    joined = batch_df.join(
        F.broadcast(dim_source).alias("t3"),
        F.col("ta.Source") == F.col("t3.ObservationSource"),
        "inner"
    )
    count = joined.count()
    print(f"   Rows after join: {count}")

    if count == 0:
        print("   No matches! Checking values...")
        print("   Source values:")
        batch_df.select("ta.Source").distinct().show(10)
        print("   Dimension values:")
        dim_source.show(10)
    else:
        batch_df = joined
except Exception as e:
    print(f"   ERROR: {e}")

# Test 4: Type join
print("\\n4. Type join:")
try:
    joined = batch_df.join(
        F.broadcast(dim_type).alias("t4"),
        F.col("ta.Type") == F.col("t4.ObservationType"),
        "inner"
    )
    count = joined.count()
    print(f"   Rows after join: {count}")

    if count == 0:
        print("   No matches! Checking values...")
        print("   Source Type values:")
        batch_df.select("ta.Type").distinct().show(10)
        print("   Dimension values (sample):")
        dim_type.show(10)
except Exception as e:
    print(f"   ERROR: {e}")

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
