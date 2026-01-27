# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Device Assignment Continuous View
# MAGIC
# MAGIC **Converted from:** `dbo.vwDeviceAssignment_Continuous`
# MAGIC
# MAGIC **Purpose:** Provide continuous device assignments with no gaps.
# MAGIC Uses LEAD to determine ValidTo from next assignment's ValidFrom.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - LEAD(AssignedAt) OVER (PARTITION BY WorkerId ORDER BY AssignedAt) as ValidTo
# MAGIC - Creates continuous time ranges for device assignments per worker
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_resource_device`
# MAGIC - `wakecap_prod.silver.silver_device`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_device_assignment_continuous`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_DEVICE_ASSIGN = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_resource_device"
SOURCE_DEVICE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_device"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_device_assignment_continuous"

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_DEVICE_ASSIGN}").collect()[0][0]
    print(f"[OK] Device Assignment: {cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Device Assignment: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

opt_status = {}
for name, table in [("Device", SOURCE_DEVICE), ("Worker", SOURCE_WORKER)]:
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
print("BUILD DEVICE ASSIGNMENT CONTINUOUS VIEW")
print("=" * 60)

# Load resource device assignments
# Note: uses AssignedAt/UnassignedAt, not ValidFrom/ValidTo
assign_df = spark.table(SOURCE_DEVICE_ASSIGN)
print(f"Base assignments: {assign_df.count():,}")

# Rename AssignedAt to ValidFrom for consistency
assign_df = assign_df.withColumn("ValidFrom", F.col("AssignedAt"))
assign_df = assign_df.withColumn("ValidTo", F.col("UnassignedAt"))

# Calculate ValidTo using LEAD to create continuous ranges
# Partition by WorkerId only (device assignment per worker over time)
assign_window = Window.partitionBy("WorkerId").orderBy("ValidFrom")

assign_df = assign_df.withColumn(
    "ValidTo_Continuous",
    F.lead("ValidFrom").over(assign_window)
)

# Use the continuous ValidTo (from LEAD) to fill gaps
# Original ValidTo is kept for actual unassignment, continuous for next assignment start
assign_df = assign_df.withColumn(
    "ValidTo_Final",
    F.coalesce(F.col("ValidTo"), F.col("ValidTo_Continuous"))
)

# Add device details (silver_device has DeviceName column)
if opt_status.get("Device"):
    device_df = spark.table(SOURCE_DEVICE).select(
        F.col("DeviceId").alias("d_DeviceId"),
        F.col("DeviceName"),
        F.col("Imei").alias("DeviceSerial")
    )
    assign_df = assign_df.join(device_df, F.col("DeviceId") == F.col("d_DeviceId"), "left").drop("d_DeviceId")
else:
    assign_df = assign_df \
        .withColumn("DeviceName", F.lit(None).cast("string")) \
        .withColumn("DeviceSerial", F.lit(None).cast("string"))

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
    .withColumn("HasGap",
        F.when(
            F.col("ValidTo").isNotNull() &
            F.col("ValidTo_Continuous").isNotNull() &
            (F.col("ValidTo") < F.col("ValidTo_Continuous")),
            F.lit(True)
        ).otherwise(F.lit(False))
    ) \
    .withColumn("_view_generated_at", F.current_timestamp()) \
    .drop("ValidTo_Continuous")

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
