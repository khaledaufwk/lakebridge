# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Workshift Assignments View
# MAGIC
# MAGIC **Converted from:** `dbo.vwWorkshiftAssignments`
# MAGIC
# MAGIC **Purpose:** Provide workshift assignments with calculated ValidTo using LEAD window function.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - LEAD(EffectiveDate) OVER (PARTITION BY ProjectID, WorkerID ORDER BY EffectiveDate) as ValidTo
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_workshift_resource_assignment`
# MAGIC - `wakecap_prod.silver.silver_workshift`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_workshift_assignments`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_WS_ASSIGN = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift_resource_assignment"
SOURCE_WORKSHIFT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_workshift_assignments"

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_WS_ASSIGN}").collect()[0][0]
    print(f"[OK] Workshift Assignment: {cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Workshift Assignment: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

opt_status = {}
for name, table in [("Workshift", SOURCE_WORKSHIFT), ("Worker", SOURCE_WORKER)]:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        opt_status[name] = True
        print(f"[OK] {name} available")
    except:
        opt_status[name] = False
        print(f"[WARN] {name} not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

print("=" * 60)
print("BUILD WORKSHIFT ASSIGNMENTS VIEW")
print("=" * 60)

# Load workshift assignments
# Note: uses EffectiveDate, not ValidFrom
assign_df = spark.table(SOURCE_WS_ASSIGN)
print(f"Base assignments: {assign_df.count():,}")

# Rename EffectiveDate to ValidFrom for consistency
assign_df = assign_df.withColumn("ValidFrom", F.col("EffectiveDate"))

# Calculate ValidTo using LEAD
assign_window = Window.partitionBy("ProjectId", "WorkerId").orderBy("ValidFrom")

assign_df = assign_df.withColumn(
    "ValidTo_Calculated",
    F.lead("ValidFrom").over(assign_window)
).withColumn(
    "ValidTo_Final",
    F.col("ValidTo_Calculated")  # No DeletedAt in this table
)

# Add workshift details (silver_workshift has WorkshiftName column)
if opt_status.get("Workshift"):
    ws_df = spark.table(SOURCE_WORKSHIFT).select(
        F.col("WorkshiftId").alias("ws_WorkshiftId"),
        F.col("WorkshiftName")
    )
    assign_df = assign_df.join(ws_df, F.col("WorkshiftId") == F.col("ws_WorkshiftId"), "left").drop("ws_WorkshiftId")
else:
    assign_df = assign_df.withColumn("WorkshiftName", F.lit(None).cast("string"))

# Add worker details (silver_worker has WorkerName column)
if opt_status.get("Worker"):
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId").alias("w_WorkerId"),
        F.col("WorkerName")
    )
    assign_df = assign_df.join(worker_df, F.col("WorkerId") == F.col("w_WorkerId"), "left").drop("w_WorkerId")
else:
    assign_df = assign_df.withColumn("WorkerName", F.lit(None).cast("string"))

# Add computed columns
assign_df = assign_df \
    .withColumn("IsCurrent", F.when(F.col("ValidTo_Final").isNull(), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("AssignmentDurationDays",
        F.when(F.col("ValidTo_Final").isNotNull(),
               F.datediff(F.col("ValidTo_Final"), F.col("ValidFrom")))
    ) \
    .withColumn("_view_generated_at", F.current_timestamp()) \
    .drop("ValidTo_Calculated")

print(f"Final view: {assign_df.count():,} rows")

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

assign_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Rows: {after_count:,}")

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
