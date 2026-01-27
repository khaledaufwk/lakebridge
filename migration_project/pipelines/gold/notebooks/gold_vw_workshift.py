# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Workshift Dimension View
# MAGIC
# MAGIC **Converted from:** `dbo.vwWorkshift`
# MAGIC
# MAGIC **Purpose:** Provide a consolidated workshift dimension view with project details
# MAGIC and basic workshift information.
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_workshift`
# MAGIC - `wakecap_prod.silver.silver_project`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_workshift`

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

SOURCE_WORKSHIFT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_workshift"

print(f"Source Workshift: {SOURCE_WORKSHIFT}")
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

try:
    ws_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_WORKSHIFT}").collect()[0][0]
    print(f"[OK] Workshift: {ws_cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Workshift: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional
has_project = False
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_PROJECT} LIMIT 0")
    has_project = True
    print(f"[OK] Project available")
except:
    print(f"[WARN] Project not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Workshift View

# COMMAND ----------

print("=" * 60)
print("BUILD WORKSHIFT VIEW")
print("=" * 60)

# Load base workshift table
ws_df = spark.table(SOURCE_WORKSHIFT)
print(f"Base workshifts: {ws_df.count():,}")

# Join with project
# Note: ProjectId may be UUID (string) in some tables - cast to string for safe comparison
if has_project:
    project_df = spark.table(SOURCE_PROJECT).select(
        F.col("ProjectId").cast("string").alias("p_ProjectId"),
        F.col("ProjectName")
    )

    ws_df = ws_df.alias("w").join(
        project_df.alias("p"),
        F.col("w.ProjectId").cast("string") == F.col("p.p_ProjectId"),
        "left"
    ).drop("p_ProjectId")
    print("[OK] Project joined")
else:
    ws_df = ws_df.withColumn("ProjectName", F.lit(None).cast("string"))
    print("[SKIP] Project not available")

# Add metadata
ws_df = ws_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final workshift view: {ws_df.count():,} rows")

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

ws_df.write \
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
print(f"Source: {SOURCE_WORKSHIFT}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
