# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Reported Attendance View
# MAGIC
# MAGIC **Converted from:** `dbo.vwFactReportedAttendance`
# MAGIC
# MAGIC **Purpose:** Provide an enriched view of reported attendance with worker and project details.
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.gold.gold_fact_reported_attendance`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC - `wakecap_prod.silver.silver_project`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_fact_reported_attendance`

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

SOURCE_ATTENDANCE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_reported_attendance"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_fact_reported_attendance"

print(f"Source: {SOURCE_ATTENDANCE}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "30", "Lookback Days")

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
    att_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_ATTENDANCE}").collect()[0][0]
    print(f"[OK] Attendance: {att_cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Attendance: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional
opt_status = {}
for name, table in [
    ("Worker", SOURCE_WORKER),
    ("Project", SOURCE_PROJECT),
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
# MAGIC ## Build Attendance View

# COMMAND ----------

print("=" * 60)
print("BUILD ATTENDANCE VIEW")
print("=" * 60)

# Load base attendance
if load_mode == "full":
    att_df = spark.table(SOURCE_ATTENDANCE)
else:
    max_date = datetime.now().date()
    min_date = max_date - timedelta(days=lookback_days)
    att_df = spark.table(SOURCE_ATTENDANCE) \
        .filter(F.col("ShiftLocalDate").between(min_date, max_date))

print(f"Base attendance: {att_df.count():,}")

# Convert ReportedTime to hours
att_df = att_df.withColumn(
    "ReportedHours",
    F.col("ReportedTime") * 24
)

# Add attendance status descriptions
att_df = att_df \
    .withColumn("CustomerAttendanceStatusDesc",
        F.when(F.col("CustomerReportedAttendance") == 1, F.lit("Present"))
         .when(F.col("CustomerReportedAttendance") == 0, F.lit("Absent"))
         .otherwise(F.lit("Unknown"))
    ) \
    .withColumn("ManagerAttendanceStatusDesc",
        F.when(F.col("ManagerReportedAttendance") == 1, F.lit("Present"))
         .when(F.col("ManagerReportedAttendance") == 0, F.lit("Absent"))
         .otherwise(F.lit("Unknown"))
    )

# Add worker details (silver_worker has WorkerName, not Worker; no ExtWorkerId)
if opt_status.get("Worker"):
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId").alias("w_WorkerId"),
        F.col("WorkerName"),
        F.col("WorkerCode")
    )

    att_df = att_df.join(
        worker_df,
        F.col("WorkerID") == F.col("w_WorkerId"),
        "left"
    ).drop("w_WorkerId")
    print("[OK] Worker details joined")
else:
    att_df = att_df \
        .withColumn("WorkerName", F.lit(None).cast("string")) \
        .withColumn("WorkerCode", F.lit(None).cast("string"))

# Add project details
if opt_status.get("Project"):
    project_df = spark.table(SOURCE_PROJECT).select(
        F.col("ProjectId").alias("p_ProjectId"),
        F.col("ProjectName")
    )

    att_df = att_df.join(
        project_df,
        F.col("ProjectID") == F.col("p_ProjectId"),
        "left"
    ).drop("p_ProjectId")
    print("[OK] Project details joined")
else:
    att_df = att_df.withColumn("ProjectName", F.lit(None).cast("string"))

# Add computed columns
att_df = att_df \
    .withColumn("HasCustomerReport",
        F.when(F.col("CustomerReportedAttendance").isNotNull(), F.lit(True))
         .otherwise(F.lit(False))
    ) \
    .withColumn("HasManagerReport",
        F.when(F.col("ManagerReportedAttendance").isNotNull(), F.lit(True))
         .otherwise(F.lit(False))
    ) \
    .withColumn("AttendanceConflict",
        F.when(
            F.col("CustomerReportedAttendance").isNotNull() &
            F.col("ManagerReportedAttendance").isNotNull() &
            (F.col("CustomerReportedAttendance") != F.col("ManagerReportedAttendance")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

# Add metadata
att_df = att_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final attendance view: {att_df.count():,} rows")

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

att_df.write \
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
print(f"Source: {SOURCE_ATTENDANCE}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
