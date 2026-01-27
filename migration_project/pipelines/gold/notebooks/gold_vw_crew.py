# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Crew Dimension View
# MAGIC
# MAGIC **Converted from:** `dbo.vwCrew`
# MAGIC
# MAGIC **Purpose:** Provide a consolidated crew dimension view with project details,
# MAGIC crew groups extracted using pattern matching, and productivity status.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Joins Crew with Project
# MAGIC - Computes CrewGroup1, CrewGroup2 using pattern extraction (fnExtractPattern equivalent)
# MAGIC - Includes crew type and productivity status
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_crew`
# MAGIC - `wakecap_prod.silver.silver_project`
# MAGIC - `wakecap_prod.silver.silver_crew_type`
# MAGIC - `wakecap_prod.silver.silver_crew_productivity_status`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_crew`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_CREW = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"
SOURCE_CREW_TYPE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_type"
SOURCE_CREW_PROD_STATUS = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_productivity_status"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_crew"

print(f"Source Crew: {SOURCE_CREW}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Required
try:
    crew_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_CREW}").collect()[0][0]
    print(f"[OK] Crew: {crew_cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Crew: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional
opt_status = {}
for name, table in [
    ("Project", SOURCE_PROJECT),
    ("Crew Type", SOURCE_CREW_TYPE),
    ("Crew Productivity Status", SOURCE_CREW_PROD_STATUS),
]:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        opt_status[name] = True
        print(f"[OK] {name} available")
    except:
        opt_status[name] = False
        print(f"[WARN] {name} not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Pattern Extraction UDF

# COMMAND ----------

# Equivalent to SQL Server dbo.fnExtractPattern
@F.udf(StringType())
def extract_pattern(start_pattern, stop_pattern, value):
    """Extract substring between start and stop patterns."""
    if not value or not start_pattern:
        return None
    try:
        start_idx = value.find(start_pattern)
        if start_idx == -1:
            return None
        start_idx += len(start_pattern)
        if stop_pattern:
            stop_idx = value.find(stop_pattern, start_idx)
            if stop_idx == -1:
                return value[start_idx:]
            return value[start_idx:stop_idx]
        return value[start_idx:]
    except:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Crew View

# COMMAND ----------

print("=" * 60)
print("BUILD CREW VIEW")
print("=" * 60)

# Load base crew table
crew_df = spark.table(SOURCE_CREW)
print(f"Base crews: {crew_df.count():,}")

# Join with project to get project name
# Note: silver_project does NOT have CrewAutoGroup columns
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
if opt_status.get("Project"):
    project_df = spark.table(SOURCE_PROJECT).select(
        F.col("ProjectId").cast("string").alias("p_ProjectId"),
        F.col("ProjectName")
    )

    crew_df = crew_df.alias("c").join(
        project_df.alias("p"),
        F.col("c.ProjectId").cast("string") == F.col("p.p_ProjectId"),
        "left"
    ).drop("p_ProjectId")

    # CrewGroup columns are not available in silver schema
    crew_df = crew_df \
        .withColumn("CrewGroup1", F.lit(None).cast("string")) \
        .withColumn("CrewGroup2", F.lit(None).cast("string"))

    print("[OK] Project joined")
else:
    crew_df = crew_df \
        .withColumn("ProjectName", F.lit(None).cast("string")) \
        .withColumn("CrewGroup1", F.lit(None).cast("string")) \
        .withColumn("CrewGroup2", F.lit(None).cast("string"))
    print("[SKIP] Project not available")

# Join with crew type (silver_crew_type has CrewTypeName column)
# Note: CrewTypeId in silver_crew is UUID, but CrewTypeId in silver_crew_type may be BIGINT
# Cast both to STRING for safe comparison
if opt_status.get("Crew Type"):
    crew_type_df = spark.table(SOURCE_CREW_TYPE).select(
        F.col("CrewTypeId").cast("string").alias("ct_CrewTypeId"),
        F.col("CrewTypeName")
    )

    crew_df = crew_df.join(
        crew_type_df,
        F.col("CrewTypeId").cast("string") == F.col("ct_CrewTypeId"),
        "left"
    ).drop("ct_CrewTypeId")

    print("[OK] Crew type joined")
else:
    crew_df = crew_df.withColumn("CrewTypeName", F.lit(None).cast("string"))
    print("[SKIP] Crew type not available")

# Note: silver_crew_productivity_status doesn't have a status name column
# It only has CrewProductivityStatusId, CrewId, ManagerId, CreatedAt, UpdatedAt
# Skip this join for now - no status name to add
crew_df = crew_df.withColumn("ProductivityStatusName", F.lit(None).cast("string"))
print("[SKIP] Productivity status name not available in schema")

# Add metadata
crew_df = crew_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final crew view: {crew_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Target

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

crew_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

print(f"Rows before: {before_count:,}")
print(f"Rows after: {after_count:,}")

# COMMAND ----------

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_CREW}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
