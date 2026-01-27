# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Crew Assignments View
# MAGIC
# MAGIC **Converted from:** `dbo.vwCrewAssignments`
# MAGIC
# MAGIC **Purpose:** Provide crew assignments with calculated ValidTo using LEAD window function.
# MAGIC ValidTo is determined by the next assignment's ValidFrom for the same worker/project.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - LEAD(ValidFrom) OVER (PARTITION BY ProjectID, WorkerID ORDER BY ValidFrom) as ValidTo
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_crew_composition`
# MAGIC - `wakecap_prod.silver.silver_crew`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_crew_assignments`

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

SOURCE_CREW_COMP = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"
SOURCE_CREW = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_crew_assignments"

print(f"Source: {SOURCE_CREW_COMP}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_CREW_COMP}").collect()[0][0]
    print(f"[OK] Crew Composition: {cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Crew Composition: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional
opt_status = {}
for name, table in [("Crew", SOURCE_CREW), ("Worker", SOURCE_WORKER)]:
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
# MAGIC ## Build Crew Assignments View

# COMMAND ----------

print("=" * 60)
print("BUILD CREW ASSIGNMENTS VIEW")
print("=" * 60)

# Load crew composition
# Note: silver_crew_composition has CreatedAt/DeletedAt, not ValidFrom/ValidTo
assign_df = spark.table(SOURCE_CREW_COMP)
print(f"Base assignments: {assign_df.count():,}")

# Rename CreatedAt to ValidFrom for consistency
assign_df = assign_df.withColumn("ValidFrom", F.col("CreatedAt"))

# Calculate ValidTo using LEAD
# LEAD(ValidFrom) OVER (PARTITION BY ProjectID, WorkerID ORDER BY ValidFrom)
assign_window = Window.partitionBy("ProjectId", "WorkerId").orderBy("ValidFrom")

assign_df = assign_df.withColumn(
    "ValidTo_Calculated",
    F.lead("ValidFrom").over(assign_window)
)

# Use DeletedAt as ValidTo if set, otherwise use calculated
assign_df = assign_df.withColumn(
    "ValidTo_Final",
    F.coalesce(F.col("DeletedAt"), F.col("ValidTo_Calculated"))
)

# Add crew details (silver_crew has CrewName column)
if opt_status.get("Crew"):
    crew_df = spark.table(SOURCE_CREW).select(
        F.col("CrewId").alias("c_CrewId"),
        F.col("CrewName")
    )
    assign_df = assign_df.join(crew_df, F.col("CrewId") == F.col("c_CrewId"), "left").drop("c_CrewId")
    print("[OK] Crew details joined")
else:
    assign_df = assign_df.withColumn("CrewName", F.lit(None).cast("string"))

# Add worker details (silver_worker has WorkerName column)
if opt_status.get("Worker"):
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId").alias("w_WorkerId"),
        F.col("WorkerName")
    )
    assign_df = assign_df.join(worker_df, F.col("WorkerId") == F.col("w_WorkerId"), "left").drop("w_WorkerId")
    print("[OK] Worker details joined")
else:
    assign_df = assign_df.withColumn("WorkerName", F.lit(None).cast("string"))

# Add computed columns
assign_df = assign_df \
    .withColumn("IsCurrent", F.when(F.col("ValidTo_Final").isNull(), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("AssignmentDurationDays",
        F.when(F.col("ValidTo_Final").isNotNull(),
               F.datediff(F.col("ValidTo_Final"), F.col("ValidFrom")))
    ) \
    .withColumn("_view_generated_at", F.current_timestamp())

# Cleanup
assign_df = assign_df.drop("ValidTo_Calculated")

print(f"Final view: {assign_df.count():,} rows")

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

assign_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

print(f"Rows before: {before_count:,}")
print(f"Rows after: {after_count:,}")

# COMMAND ----------

print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
