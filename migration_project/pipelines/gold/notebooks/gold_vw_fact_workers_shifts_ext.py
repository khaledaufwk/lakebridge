# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Extended Fact Workers Shifts View
# MAGIC
# MAGIC **Converted from:** `dbo.vwFactWorkersShifts_Ext`
# MAGIC
# MAGIC **Purpose:** Provide an extended view of FactWorkersShifts with:
# MAGIC - All base shift metrics
# MAGIC - Computed hours columns (ActiveHours, InactiveHours, OnlineHours, OfflineHours)
# MAGIC - Late arrival/early leave calculations
# MAGIC - Timezone-converted local times
# MAGIC - Assigned shift duration calculations
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.gold.gold_fact_workers_shifts`
# MAGIC - `wakecap_prod.silver.silver_workshift`
# MAGIC - `wakecap_prod.silver.silver_workshift_schedule`
# MAGIC - `wakecap_prod.silver.silver_project` (for timezone)
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_fact_workers_shifts_ext`

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

SOURCE_SHIFTS = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_workers_shifts"
SOURCE_WORKSHIFT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift"
SOURCE_SCHEDULE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift_schedule"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
SOURCE_WORKSHIFT_ASSIGN = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift_resource_assignment"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_fact_workers_shifts_ext"

print(f"Source Shifts: {SOURCE_SHIFTS}")
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
    shifts_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_SHIFTS}").collect()[0][0]
    print(f"[OK] Shifts: {shifts_cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Shifts: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional
opt_status = {}
for name, table in [
    ("Workshift", SOURCE_WORKSHIFT),
    ("Schedule", SOURCE_SCHEDULE),
    ("Project", SOURCE_PROJECT),
    ("Worker", SOURCE_WORKER),
    ("Workshift Assignment", SOURCE_WORKSHIFT_ASSIGN),
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
# MAGIC ## Build Extended Shifts View

# COMMAND ----------

print("=" * 60)
print("BUILD EXTENDED SHIFTS VIEW")
print("=" * 60)

# Load base shifts
if load_mode == "full":
    shifts_df = spark.table(SOURCE_SHIFTS)
else:
    max_date = datetime.now().date()
    from datetime import timedelta
    min_date = max_date - timedelta(days=lookback_days)
    shifts_df = spark.table(SOURCE_SHIFTS) \
        .filter(F.col("ShiftLocalDate").between(min_date, max_date))

print(f"Base shifts: {shifts_df.count():,}")

# Convert time fractions to hours (time is stored as fraction of day)
# 1 day = 24 hours, so multiply by 24
shifts_df = shifts_df \
    .withColumn("ActiveHours", F.col("ActiveTime") * 24) \
    .withColumn("InactiveHours", F.col("InactiveTime") * 24) \
    .withColumn("ActiveHoursBeforeShift", F.col("ActiveTimeBeforeShift") * 24) \
    .withColumn("ActiveHoursDuringShift", F.col("ActiveTimeDuringShift") * 24) \
    .withColumn("ActiveHoursAfterShift", F.col("ActiveTimeAfterShift") * 24) \
    .withColumn("InactiveHoursBeforeShift", F.col("InactiveTimeBeforeShift") * 24) \
    .withColumn("InactiveHoursDuringShift", F.col("InactiveTimeDuringShift") * 24) \
    .withColumn("InactiveHoursAfterShift", F.col("InactiveTimeAfterShift") * 24)

# Calculate online/offline hours
# Online = Active + Inactive (when device is communicating)
shifts_df = shifts_df \
    .withColumn("OnlineHours",
        F.coalesce(F.col("ActiveHours"), F.lit(0)) +
        F.coalesce(F.col("InactiveHours"), F.lit(0))
    ) \
    .withColumn("OnlineHoursDuringShift",
        F.coalesce(F.col("ActiveHoursDuringShift"), F.lit(0)) +
        F.coalesce(F.col("InactiveHoursDuringShift"), F.lit(0))
    )

# Calculate shift duration in hours
shifts_df = shifts_df.withColumn(
    "ShiftDurationHours",
    (F.unix_timestamp("FinishAtUTC") - F.unix_timestamp("StartAtUTC")) / 3600.0
)

# COMMAND ----------

# Get workshift assignment for the shift date
# Note: silver_workshift_resource_assignment uses EffectiveDate (no ValidTo)
if opt_status.get("Workshift Assignment"):
    ws_assign_df = spark.table(SOURCE_WORKSHIFT_ASSIGN)

    # Get assignment - use window to find latest assignment before shift date
    assign_window = Window.partitionBy("ProjectId", "WorkerId").orderBy(F.col("EffectiveDate").desc())

    assign_df = ws_assign_df \
        .withColumn("_rn", F.row_number().over(assign_window)) \
        .alias("a")

    shifts_df = shifts_df.alias("s").join(
        assign_df,
        (F.col("s.ProjectId") == F.col("a.ProjectId")) &
        (F.col("s.WorkerId") == F.col("a.WorkerId")) &
        (F.col("s.ShiftLocalDate") >= F.col("a.EffectiveDate")) &
        (F.col("a._rn") == 1),
        "left"
    ).select(
        "s.*",
        F.col("a.WorkshiftId").alias("AssignedWorkshiftId")
    )

    print("[OK] Workshift assignment joined")
else:
    shifts_df = shifts_df \
        .withColumn("AssignedWorkshiftId", F.lit(None).cast("int"))
    print("[SKIP] Workshift assignment not available")

# COMMAND ----------

# Get assigned shift timing from schedule
# Note: silver_workshift_schedule uses StartHour/EndHour as TIMESTAMP, WorkshiftDayId (not DayOfWeek)
if opt_status.get("Schedule") and opt_status.get("Workshift"):
    # Load schedule and convert TIMESTAMP to decimal hours
    schedule_df = spark.table(SOURCE_SCHEDULE).select(
        F.col("WorkshiftId").alias("sch_WorkshiftId"),
        # Convert TIMESTAMP StartHour/EndHour to decimal hours for calculations
        (F.hour(F.col("StartHour")) + F.minute(F.col("StartHour")) / 60.0).alias("AssignedStartHour"),
        (F.hour(F.col("EndHour")) + F.minute(F.col("EndHour")) / 60.0).alias("AssignedEndHour"),
        F.col("Duration").alias("AssignedDuration"),
        F.col("WorkshiftDayId").alias("sch_DayId")
    )

    # Get day of week from ShiftLocalDate
    shifts_df = shifts_df.withColumn(
        "ShiftDayOfWeek",
        F.dayofweek("ShiftLocalDate") - 1  # Convert to 0-based (Sunday=0)
    )

    # Join to get assigned times - note: WorkshiftDayId may not match day of week directly
    # Just join on WorkshiftId for now
    shifts_df = shifts_df.join(
        schedule_df,
        F.col("AssignedWorkshiftId") == F.col("sch_WorkshiftId"),
        "left"
    ).drop("sch_WorkshiftId", "sch_DayId")

    # Calculate assigned shift duration from Duration column (hours)
    shifts_df = shifts_df.withColumn(
        "AssignedShiftDurationHours",
        F.col("AssignedDuration")
    )

    # Late arrival calculation - compare actual start hour with assigned start hour
    shifts_df = shifts_df.withColumn(
        "ActualStartHour",
        F.hour("StartAtUTC") + F.minute("StartAtUTC") / 60.0
    ).withColumn(
        "LateArrivalMinutes",
        F.when(
            F.col("ActualStartHour").isNotNull() & F.col("AssignedStartHour").isNotNull(),
            (F.col("ActualStartHour") - F.col("AssignedStartHour")) * 60
        )
    )

    # Early leave calculation
    shifts_df = shifts_df.withColumn(
        "ActualEndHour",
        F.hour("FinishAtUTC") + F.minute("FinishAtUTC") / 60.0
    ).withColumn(
        "EarlyLeaveMinutes",
        F.when(
            F.col("ActualEndHour").isNotNull() & F.col("AssignedEndHour").isNotNull(),
            (F.col("AssignedEndHour") - F.col("ActualEndHour")) * 60
        )
    )

    # Late arrival flag (more than 15 minutes late)
    shifts_df = shifts_df.withColumn(
        "IsLateArrival",
        F.when(F.col("LateArrivalMinutes") > 15, F.lit(True)).otherwise(F.lit(False))
    )

    # Early leave flag (more than 15 minutes early)
    shifts_df = shifts_df.withColumn(
        "IsEarlyLeave",
        F.when(F.col("EarlyLeaveMinutes") > 15, F.lit(True)).otherwise(F.lit(False))
    )

    # Clean up intermediate columns
    shifts_df = shifts_df.drop("ActualStartHour", "ActualEndHour")

    print("[OK] Schedule timing calculations added")
else:
    shifts_df = shifts_df \
        .withColumn("ShiftDayOfWeek", F.dayofweek("ShiftLocalDate") - 1) \
        .withColumn("AssignedStartHour", F.lit(None).cast("double")) \
        .withColumn("AssignedEndHour", F.lit(None).cast("double")) \
        .withColumn("AssignedDuration", F.lit(None).cast("double")) \
        .withColumn("AssignedShiftDurationHours", F.lit(None).cast("double")) \
        .withColumn("LateArrivalMinutes", F.lit(None).cast("double")) \
        .withColumn("EarlyLeaveMinutes", F.lit(None).cast("double")) \
        .withColumn("IsLateArrival", F.lit(False)) \
        .withColumn("IsEarlyLeave", F.lit(False))
    print("[SKIP] Schedule not available")

# COMMAND ----------

# Add worker and project details (silver_worker has WorkerName, not Worker; no ExtWorkerId)
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
if opt_status.get("Worker"):
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId").cast("string").alias("w_WorkerId"),
        F.col("WorkerName"),
        F.col("WorkerCode")
    )

    shifts_df = shifts_df.join(
        worker_df,
        F.col("WorkerId").cast("string") == F.col("w_WorkerId"),
        "left"
    ).drop("w_WorkerId")
    print("[OK] Worker details joined")
else:
    shifts_df = shifts_df \
        .withColumn("WorkerName", F.lit(None).cast("string")) \
        .withColumn("WorkerCode", F.lit(None).cast("string"))
    print("[SKIP] Worker not available")

if opt_status.get("Project"):
    # Note: TimeZone column doesn't exist in silver_project - skip it
    project_df = spark.table(SOURCE_PROJECT).select(
        F.col("ProjectId").cast("string").alias("p_ProjectId"),
        F.col("ProjectName")
    )

    shifts_df = shifts_df.join(
        project_df,
        F.col("ProjectId").cast("string") == F.col("p_ProjectId"),
        "left"
    ).drop("p_ProjectId")
    print("[OK] Project details joined")
else:
    shifts_df = shifts_df \
        .withColumn("ProjectName", F.lit(None).cast("string"))
    print("[SKIP] Project not available")

# COMMAND ----------

# Add computed productivity metrics
shifts_df = shifts_df \
    .withColumn("ActiveHoursInDirectProductive", F.col("ActiveTimeInDirectProductive") * 24) \
    .withColumn("ActiveHoursInIndirectProductive", F.col("ActiveTimeInIndirectProductive") * 24) \
    .withColumn("ActiveHoursInAssignedLocation", F.col("ActiveTimeInAssignedLocation") * 24) \
    .withColumn("InactiveHoursInDirectProductive", F.col("InactiveTimeInDirectProductive") * 24) \
    .withColumn("InactiveHoursInIndirectProductive", F.col("InactiveTimeInIndirectProductive") * 24) \
    .withColumn("InactiveHoursInAssignedLocation", F.col("InactiveTimeInAssignedLocation") * 24)

# Calculate utilization percentages
shifts_df = shifts_df \
    .withColumn("ActiveUtilization",
        F.when(F.col("OnlineHours") > 0,
               F.col("ActiveHours") / F.col("OnlineHours") * 100)
         .otherwise(F.lit(None))
    ) \
    .withColumn("DirectProductiveUtilization",
        F.when(F.col("ActiveHours") > 0,
               F.col("ActiveHoursInDirectProductive") / F.col("ActiveHours") * 100)
         .otherwise(F.lit(None))
    ) \
    .withColumn("AssignedLocationUtilization",
        F.when(F.col("ActiveHours") > 0,
               F.col("ActiveHoursInAssignedLocation") / F.col("ActiveHours") * 100)
         .otherwise(F.lit(None))
    )

# Add metadata
shifts_df = shifts_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final extended shifts view: {shifts_df.count():,} rows, {len(shifts_df.columns)} columns")

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

shifts_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

print(f"Rows before: {before_count:,}")
print(f"Rows after: {after_count:,}")
print(f"Columns: {len(shifts_df.columns)}")

# COMMAND ----------

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_SHIFTS}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print(f"Columns: {len(shifts_df.columns)}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}, cols={len(shifts_df.columns)}")
