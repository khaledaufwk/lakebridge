# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Workshift Details View
# MAGIC
# MAGIC **Converted from:** `dbo.vwWorkshiftDetails`
# MAGIC
# MAGIC **Purpose:** Provide workshift details with calculated durations,
# MAGIC breaks parsing, and schedule information.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Calculate ShiftDuration from StartTime and EndTime
# MAGIC - Parse Breaks string to calculate BreaksDuration
# MAGIC - Handle overnight shifts (EndTime < StartTime)
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_workshift`
# MAGIC - `wakecap_prod.silver.silver_workshift_schedule`
# MAGIC - `wakecap_prod.silver.silver_workshift_schedule_break`
# MAGIC - `wakecap_prod.silver.silver_workshift_day`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_workshift_details`

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
SOURCE_SCHEDULE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift_schedule"
SOURCE_BREAK = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift_schedule_break"
SOURCE_DAY = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift_day"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_workshift_details"

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
opt_status = {}
for name, table in [
    ("Schedule", SOURCE_SCHEDULE),
    ("Break", SOURCE_BREAK),
    ("Day", SOURCE_DAY),
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
# MAGIC ## Helper Functions

# COMMAND ----------

# Note: Breaks are now stored in a separate table (silver_workshift_schedule_break)
# with Duration column already calculated, so no UDF needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Workshift Details View

# COMMAND ----------

print("=" * 60)
print("BUILD WORKSHIFT DETAILS VIEW")
print("=" * 60)

# Load base workshift table
ws_df = spark.table(SOURCE_WORKSHIFT)
print(f"Base workshifts: {ws_df.count():,}")

# Join with schedule to get start/end hours
# Note: silver_workshift_schedule has StartHour, EndHour, Duration (not StartTime/EndTime)
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
if opt_status.get("Schedule"):
    schedule_df = spark.table(SOURCE_SCHEDULE).select(
        F.col("WorkshiftId").cast("string").alias("s_WorkshiftId"),
        F.col("WorkshiftScheduleId").cast("string").alias("WorkshiftScheduleId"),  # Cast to string for later joins
        F.col("StartHour"),
        F.col("EndHour"),
        F.col("Duration"),
        F.col("WorkshiftDayId").cast("string").alias("WorkshiftDayId")  # Cast to string
    )

    # Get all schedules per workshift
    ws_df = ws_df.alias("w").join(
        schedule_df.alias("s"),
        F.col("w.WorkshiftId").cast("string") == F.col("s.s_WorkshiftId"),
        "left"
    ).drop("s_WorkshiftId")

    # Duration may be stored as TIME (HH:mm:ss) or numeric
    # Convert to hours first, handling both formats
    ws_df = ws_df.withColumn(
        "DurationHours",
        F.when(
            F.col("Duration").cast("string").contains(":"),
            # TIME format: extract hours + minutes/60 + seconds/3600
            F.hour(F.col("Duration")) + F.minute(F.col("Duration")) / 60.0 + F.second(F.col("Duration")) / 3600.0
        ).otherwise(
            # Already numeric hours
            F.col("Duration").cast("double")
        )
    )

    # Convert Duration to days
    ws_df = ws_df.withColumn(
        "ShiftDuration",
        F.col("DurationHours") / 24.0  # Convert hours to days
    )

    # Calculate shift duration in seconds for display
    ws_df = ws_df.withColumn(
        "ShiftDurationSeconds",
        F.col("DurationHours") * 3600  # Hours to seconds
    )

    print("[OK] Schedule joined with duration calculations")
else:
    ws_df = ws_df \
        .withColumn("WorkshiftScheduleId", F.lit(None).cast("int")) \
        .withColumn("StartHour", F.lit(None).cast("timestamp")) \
        .withColumn("EndHour", F.lit(None).cast("timestamp")) \
        .withColumn("Duration", F.lit(None).cast("double")) \
        .withColumn("WorkshiftDayId", F.lit(None).cast("int")) \
        .withColumn("ShiftDuration", F.lit(None).cast("double")) \
        .withColumn("ShiftDurationSeconds", F.lit(None).cast("long"))
    print("[SKIP] Schedule not available")

# Add break count and total break duration
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
if opt_status.get("Break"):
    break_df = spark.table(SOURCE_BREAK)

    # Break Duration may be TIME format (HH:mm:ss) - convert to hours first
    break_with_hours = break_df.withColumn(
        "DurationHours",
        F.when(
            F.col("Duration").cast("string").contains(":"),
            # TIME format: extract hours + minutes/60 + seconds/3600
            F.hour(F.col("Duration")) + F.minute(F.col("Duration")) / 60.0 + F.second(F.col("Duration")) / 3600.0
        ).otherwise(
            # Already numeric
            F.col("Duration").cast("double")
        )
    )

    # Calculate break counts and total duration per schedule
    break_agg = break_with_hours.groupBy("WorkshiftScheduleId").agg(
        F.count("*").alias("BreakCount"),
        F.sum("DurationHours").alias("TotalBreakDurationHours")  # Sum of break durations in hours
    ).select(
        F.col("WorkshiftScheduleId").cast("string").alias("b_WorkshiftScheduleId"),
        F.col("BreakCount"),
        F.col("TotalBreakDurationHours")
    )

    ws_df = ws_df.join(
        break_agg,
        F.col("WorkshiftScheduleId") == F.col("b_WorkshiftScheduleId"),
        "left"
    ).drop("b_WorkshiftScheduleId")

    # Calculate BreaksDuration in days and NetShiftDuration
    ws_df = ws_df.withColumn(
        "BreaksDuration",
        F.coalesce(F.col("TotalBreakDurationHours"), F.lit(0)) / 24.0  # Hours to days
    ).withColumn(
        "NetShiftDuration",
        F.col("ShiftDuration") - F.coalesce(F.col("BreaksDuration"), F.lit(0))
    ).drop("TotalBreakDurationHours")

    print("[OK] Break counts and durations added")
else:
    ws_df = ws_df \
        .withColumn("BreakCount", F.lit(0).cast("int")) \
        .withColumn("BreaksDuration", F.lit(0).cast("double")) \
        .withColumn("NetShiftDuration", F.col("ShiftDuration"))
    print("[SKIP] Break table not available")

# Note: WorkshiftDayId references silver_workshift_day, not a simple day-of-week number
# We'll leave DayOfWeekName as NULL for now - it would need a join to workshift_day
ws_df = ws_df.withColumn("DayOfWeekName", F.lit(None).cast("string"))

# Format start/end hours for display
# Note: StartHour/EndHour are stored as TIMESTAMP in silver layer
# Extract hour and minute from TIMESTAMP for display
ws_df = ws_df.withColumn(
    "StartTimeDisplay",
    F.when(
        F.col("StartHour").isNotNull(),
        F.date_format(F.col("StartHour"), "HH:mm")
    ).otherwise(F.lit(None))
).withColumn(
    "EndTimeDisplay",
    F.when(
        F.col("EndHour").isNotNull(),
        F.date_format(F.col("EndHour"), "HH:mm")
    ).otherwise(F.lit(None))
)

# Convert TIMESTAMP to decimal hours for calculations
ws_df = ws_df.withColumn(
    "StartHourDecimal",
    F.hour(F.col("StartHour")) + F.minute(F.col("StartHour")) / 60.0
).withColumn(
    "EndHourDecimal",
    F.hour(F.col("EndHour")) + F.minute(F.col("EndHour")) / 60.0
)

# Add overnight shift indicator (EndHour < StartHour)
ws_df = ws_df.withColumn(
    "IsOvernightShift",
    F.when(
        F.col("EndHourDecimal").isNotNull() & F.col("StartHourDecimal").isNotNull() &
        (F.col("EndHourDecimal") < F.col("StartHourDecimal")),
        F.lit(True)
    ).otherwise(F.lit(False))
)

# Cleanup intermediate columns
ws_df = ws_df.drop("ShiftDurationSeconds")

# Add metadata
ws_df = ws_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final workshift details view: {ws_df.count():,} rows")

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
