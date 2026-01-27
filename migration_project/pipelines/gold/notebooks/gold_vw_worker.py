# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Worker Dimension View
# MAGIC
# MAGIC **Converted from:** `dbo.vwWorker`
# MAGIC
# MAGIC **Purpose:** Provide a consolidated worker dimension view with current device assignment,
# MAGIC crew assignment, trade assignment, and other enriched attributes.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Joins Worker with current DeviceAssignment (ValidTo IS NULL)
# MAGIC - Joins with Device to get device details
# MAGIC - Uses ROW_NUMBER to get latest assignment when multiple exist
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC - `wakecap_prod.silver.silver_resource_device`
# MAGIC - `wakecap_prod.silver.silver_device`
# MAGIC - `wakecap_prod.silver.silver_crew_composition`
# MAGIC - `wakecap_prod.silver.silver_trade_assignment`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_worker`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
SOURCE_DEVICE_ASSIGN = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_resource_device"
SOURCE_DEVICE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_device"
SOURCE_CREW_COMP = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"
SOURCE_TRADE_ASSIGN = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_trade_assignment"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_worker"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

print(f"Source Worker: {SOURCE_WORKER}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")
print(f"Load Mode: {load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check source tables
sources = [
    ("Worker", SOURCE_WORKER),
]

all_ok = True
for name, table in sources:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
        print(f"[OK] {name}: {cnt:,} rows")
    except Exception as e:
        print(f"[ERROR] {name}: {str(e)[:50]}")
        all_ok = False

# Optional tables
optional = [
    ("Device Assignment", SOURCE_DEVICE_ASSIGN),
    ("Device", SOURCE_DEVICE),
    ("Crew Composition", SOURCE_CREW_COMP),
    ("Trade Assignment", SOURCE_TRADE_ASSIGN),
]

opt_status = {}
for name, table in optional:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} available")
        opt_status[name] = True
    except:
        print(f"[WARN] {name} not available")
        opt_status[name] = False

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

if not all_ok:
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Worker View

# COMMAND ----------

print("=" * 60)
print("BUILD WORKER VIEW")
print("=" * 60)

# Load base worker table
worker_df = spark.table(SOURCE_WORKER)
print(f"Base workers: {worker_df.count():,}")

# Get current device assignment (UnassignedAt IS NULL, latest AssignedAt)
if opt_status.get("Device Assignment") and opt_status.get("Device"):
    device_assign_df = spark.table(SOURCE_DEVICE_ASSIGN)
    device_df = spark.table(SOURCE_DEVICE)

    # Window to get latest active assignment per worker
    device_window = Window.partitionBy("WorkerId").orderBy(F.col("AssignedAt").desc())

    current_device = device_assign_df \
        .filter(F.col("UnassignedAt").isNull()) \
        .withColumn("_rn", F.row_number().over(device_window)) \
        .filter(F.col("_rn") == 1) \
        .select(
            F.col("WorkerId").alias("da_WorkerId"),
            F.col("DeviceId").alias("CurrentDeviceId"),
            F.col("AssignedAt").alias("CurrentDeviceValidFrom")
        )

    # Join device details (silver_device has DeviceName column)
    # Note: IDs may be UUID (string) - cast to string for safe comparison
    current_device_with_details = current_device.join(
        device_df.select(
            F.col("DeviceId").cast("string").alias("d_DeviceId"),
            F.col("DeviceName").alias("CurrentDeviceName"),
            F.col("Imei").alias("CurrentDeviceImei")
        ),
        F.col("CurrentDeviceId").cast("string") == F.col("d_DeviceId"),
        "left"
    ).drop("d_DeviceId")

    # Join to worker
    worker_df = worker_df.alias("w").join(
        current_device_with_details.alias("da"),
        F.col("w.WorkerId").cast("string") == F.col("da.da_WorkerId").cast("string"),
        "left"
    ).drop("da_WorkerId")

    print("[OK] Device assignment joined")
else:
    worker_df = worker_df \
        .withColumn("CurrentDeviceId", F.lit(None).cast("int")) \
        .withColumn("CurrentDeviceValidFrom", F.lit(None).cast("timestamp")) \
        .withColumn("CurrentDeviceName", F.lit(None).cast("string")) \
        .withColumn("CurrentDeviceImei", F.lit(None).cast("string"))
    print("[SKIP] Device assignment not available")

# Get current crew assignment (DeletedAt IS NULL, latest CreatedAt)
if opt_status.get("Crew Composition"):
    crew_df = spark.table(SOURCE_CREW_COMP)

    crew_window = Window.partitionBy("WorkerId", "ProjectId").orderBy(F.col("CreatedAt").desc())

    current_crew = crew_df \
        .filter(F.col("DeletedAt").isNull()) \
        .withColumn("_rn", F.row_number().over(crew_window)) \
        .filter(F.col("_rn") == 1) \
        .select(
            F.col("WorkerId").alias("cc_WorkerId"),
            F.col("ProjectId").alias("cc_ProjectId"),
            F.col("CrewId").alias("CurrentCrewId")
        )

    # Join to worker (may have multiple projects)
    # Use first project's crew for simplicity
    first_crew_window = Window.partitionBy("cc_WorkerId").orderBy(F.col("cc_ProjectId"))
    current_crew_first = current_crew \
        .withColumn("_rn2", F.row_number().over(first_crew_window)) \
        .filter(F.col("_rn2") == 1) \
        .select("cc_WorkerId", "CurrentCrewId")

    worker_df = worker_df.join(
        current_crew_first,
        F.col("WorkerId").cast("string") == F.col("cc_WorkerId").cast("string"),
        "left"
    ).drop("cc_WorkerId")

    print("[OK] Crew assignment joined")
else:
    worker_df = worker_df.withColumn("CurrentCrewId", F.lit(None).cast("int"))
    print("[SKIP] Crew assignment not available")

# Get current trade assignment
if opt_status.get("Trade Assignment"):
    trade_df = spark.table(SOURCE_TRADE_ASSIGN)

    trade_window = Window.partitionBy("WorkerId", "ProjectId").orderBy(F.col("ValidFrom").desc())

    current_trade = trade_df \
        .filter(F.col("ValidTo").isNull()) \
        .withColumn("_rn", F.row_number().over(trade_window)) \
        .filter(F.col("_rn") == 1) \
        .select(
            F.col("WorkerId").alias("ta_WorkerId"),
            F.col("TradeId").alias("CurrentTradeId")
        )

    # Use first trade
    first_trade_window = Window.partitionBy("ta_WorkerId").orderBy(F.col("CurrentTradeId"))
    current_trade_first = current_trade \
        .withColumn("_rn2", F.row_number().over(first_trade_window)) \
        .filter(F.col("_rn2") == 1) \
        .select("ta_WorkerId", "CurrentTradeId")

    worker_df = worker_df.join(
        current_trade_first,
        F.col("WorkerId").cast("string") == F.col("ta_WorkerId").cast("string"),
        "left"
    ).drop("ta_WorkerId")

    print("[OK] Trade assignment joined")
else:
    worker_df = worker_df.withColumn("CurrentTradeId", F.lit(None).cast("int"))
    print("[SKIP] Trade assignment not available")

# Add metadata columns
worker_df = worker_df \
    .withColumn("_view_generated_at", F.current_timestamp())

print(f"Final worker view: {worker_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Target Table

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

# Get row count before
try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

# Write using overwrite mode for dimension views (full refresh)
worker_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

print(f"Rows before: {before_count:,}")
print(f"Rows after: {after_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_WORKER}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print(f"Mode: {load_mode}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
