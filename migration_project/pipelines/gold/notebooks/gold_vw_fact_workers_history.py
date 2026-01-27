# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Workers History View
# MAGIC
# MAGIC **Converted from:** `dbo.vwFactWorkersHistory`
# MAGIC
# MAGIC **Purpose:** Provide an enriched view of worker location history with dimension lookups.
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.gold.gold_fact_workers_history`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC - `wakecap_prod.silver.silver_project`
# MAGIC - `wakecap_prod.silver.silver_floor`
# MAGIC - `wakecap_prod.silver.silver_zone`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_fact_workers_history`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_HISTORY = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_workers_history"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"
SOURCE_FLOOR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_floor"
SOURCE_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_fact_workers_history"

print(f"Source: {SOURCE_HISTORY}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "7", "Lookback Days")

load_mode = dbutils.widgets.get("load_mode")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    hist_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_HISTORY}").collect()[0][0]
    print(f"[OK] History: {hist_cnt:,} rows")
except Exception as e:
    print(f"[ERROR] History: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional
opt_status = {}
for name, table in [
    ("Worker", SOURCE_WORKER),
    ("Project", SOURCE_PROJECT),
    ("Floor", SOURCE_FLOOR),
    ("Zone", SOURCE_ZONE),
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
# MAGIC ## Build History View

# COMMAND ----------

print("=" * 60)
print("BUILD HISTORY VIEW")
print("=" * 60)

# Load base history with date filter
if load_mode == "full":
    # For full load, sample recent data to avoid overwhelming memory
    max_date = datetime.now().date()
    min_date = max_date - timedelta(days=30)  # Last 30 days for full view
    hist_df = spark.table(SOURCE_HISTORY) \
        .filter(F.col("ShiftLocalDate").between(min_date, max_date))
else:
    max_date = datetime.now().date()
    min_date = max_date - timedelta(days=lookback_days)
    hist_df = spark.table(SOURCE_HISTORY) \
        .filter(F.col("ShiftLocalDate").between(min_date, max_date))

print(f"Base history: {hist_df.count():,}")

# Convert time to hours
hist_df = hist_df \
    .withColumn("ActiveHours", F.col("ActiveTime") * 24) \
    .withColumn("InactiveHours", F.col("InactiveTime") * 24)

# Add worker details
# Note: IDs may be UUID (string) - cast to string for safe comparison
if opt_status.get("Worker"):
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId").cast("string").alias("w_WorkerId"),
        F.col("WorkerName"),
        F.col("WorkerCode")
    )

    hist_df = hist_df.join(
        worker_df,
        F.col("WorkerId").cast("string") == F.col("w_WorkerId"),
        "left"
    ).drop("w_WorkerId")
    print("[OK] Worker details joined")
else:
    hist_df = hist_df \
        .withColumn("WorkerName", F.lit(None).cast("string")) \
        .withColumn("WorkerCode", F.lit(None).cast("string"))

# Add project details
if opt_status.get("Project"):
    project_df = spark.table(SOURCE_PROJECT).select(
        F.col("ProjectId").cast("string").alias("p_ProjectId"),
        F.col("ProjectName")
    )

    hist_df = hist_df.join(
        project_df,
        F.col("ProjectId").cast("string") == F.col("p_ProjectId"),
        "left"
    ).drop("p_ProjectId")
    print("[OK] Project details joined")
else:
    hist_df = hist_df.withColumn("ProjectName", F.lit(None).cast("string"))

# Add floor details
if opt_status.get("Floor"):
    floor_df = spark.table(SOURCE_FLOOR).select(
        F.col("FloorId").cast("string").alias("f_FloorId"),
        F.col("FloorName")
    )

    hist_df = hist_df.join(
        floor_df,
        F.col("FloorId").cast("string") == F.col("f_FloorId"),
        "left"
    ).drop("f_FloorId")
    print("[OK] Floor details joined")
else:
    hist_df = hist_df.withColumn("FloorName", F.lit(None).cast("string"))

# Add zone details
if opt_status.get("Zone"):
    zone_df = spark.table(SOURCE_ZONE).select(
        F.col("ZoneId").cast("string").alias("z_ZoneId"),
        F.col("ZoneName")
    )

    hist_df = hist_df.join(
        zone_df,
        F.col("ZoneId").cast("string") == F.col("z_ZoneId"),
        "left"
    ).drop("z_ZoneId")
    print("[OK] Zone details joined")
else:
    hist_df = hist_df.withColumn("ZoneName", F.lit(None).cast("string"))

# Add time category description
hist_df = hist_df.withColumn(
    "TimeCategoryDesc",
    F.when(F.col("TimeCategoryId") == 1, F.lit("During Shift"))
     .when(F.col("TimeCategoryId") == 4, F.lit("After Shift"))
     .when(F.col("TimeCategoryId") == 5, F.lit("Before Shift"))
     .otherwise(F.lit("Unknown"))
)

# Add productive class description
hist_df = hist_df.withColumn(
    "ProductiveClassDesc",
    F.when(F.col("ProductiveClassId") == 1, F.lit("Direct Productive"))
     .when(F.col("ProductiveClassId") == 2, F.lit("Indirect Productive"))
     .when(F.col("ProductiveClassId") == 3, F.lit("Non-Productive"))
     .otherwise(F.lit("Unknown"))
)

# Add location assignment description
hist_df = hist_df.withColumn(
    "LocationAssignmentClassDesc",
    F.when(F.col("LocationAssignmentClassId") == 1, F.lit("Assigned Location"))
     .when(F.col("LocationAssignmentClassId") == 2, F.lit("Unassigned Location"))
     .otherwise(F.lit("Unknown"))
)

# Add metadata
hist_df = hist_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final history view: {hist_df.count():,} rows")

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

hist_df.write \
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
print(f"Source: {SOURCE_HISTORY}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
