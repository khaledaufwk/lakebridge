# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate Fact Workers Shifts Combined
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateFactWorkersShiftsCombined` (646 lines)
# MAGIC
# MAGIC **Purpose:** Combine FactWorkersShifts with FactReportedAttendance and assignment data
# MAGIC to create a unified view of worker attendance.
# MAGIC
# MAGIC **Original Patterns:** TEMP_TABLE, MERGE, WINDOW_FUNCTION, CTE, FULL OUTER JOIN
# MAGIC
# MAGIC **Key Business Logic:**
# MAGIC 1. Track watermarks from multiple source tables (shifts, attendance, assignments)
# MAGIC 2. Identify impacted worker-date combinations
# MAGIC 3. Generate expected days from device/workshift assignments
# MAGIC 4. Join shifts with reported attendance (FULL OUTER JOIN)
# MAGIC 5. Enrich with crew, trade, workshift details
# MAGIC 6. MERGE into FactWorkersShiftsCombined

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("lookback_days", "30", "Lookback Days")
dbutils.widgets.text("project_id", "", "Project ID (optional)")

lookback_days = int(dbutils.widgets.get("lookback_days"))
project_filter = dbutils.widgets.get("project_id")

today = datetime.now().date()
absent_lookbehind_cutoff = today - timedelta(days=lookback_days)

print(f"Processing with lookback: {lookback_days} days (from {absent_lookbehind_cutoff})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Source Tables

# COMMAND ----------

# Load all required tables
fact_workers_shifts = spark.table("wakecap_prod.migration.fact_workers_shifts")
fact_reported_attendance = spark.table("wakecap_prod.migration.bronze_dbo_FactReportedAttendance")
workers = spark.table("wakecap_prod.migration.bronze_dbo_Worker")
projects = spark.table("wakecap_prod.migration.bronze_dbo_Project").filter(F.col("IsActive") == True)
device_assignments = spark.table("wakecap_prod.migration.bronze_dbo_DeviceAssignments")
workshift_assignments = spark.table("wakecap_prod.migration.bronze_dbo_WorkshiftAssignments")
crew_assignments = spark.table("wakecap_prod.migration.bronze_dbo_CrewAssignments")
trade_assignments = spark.table("wakecap_prod.migration.bronze_dbo_TradeAssignments")
workshifts = spark.table("wakecap_prod.migration.bronze_dbo_Workshift")
workshift_details = spark.table("wakecap_prod.migration.bronze_dbo_WorkshiftDetails")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Expected Days Calendar
# MAGIC
# MAGIC Generate a calendar of expected work days for each worker based on
# MAGIC device assignments and workshift assignments.

# COMMAND ----------

# Generate date sequence for the lookback period
date_range = spark.range(0, (today - absent_lookbehind_cutoff).days + 1).select(
    F.date_add(F.lit(absent_lookbehind_cutoff), F.col("id").cast("int")).alias("ExplodedDate")
)

# Get workers with their assignment start dates
worker_dates = (
    workers
    .join(workshift_assignments, ["ProjectID", "WorkerID"], "inner")
    .join(
        device_assignments.groupBy("ProjectID", "WorkerID").agg(
            F.min("ValidFrom").alias("DeviceValidFrom")
        ),
        ["ProjectID", "WorkerID"],
        "inner"
    )
    .select(
        "ProjectID",
        "WorkerID",
        F.greatest(
            F.col("DeviceValidFrom"),
            F.col("ValidFrom_ShiftLocalDate")
        ).alias("DateFrom")
    )
    .groupBy("ProjectID", "WorkerID")
    .agg(F.min("DateFrom").alias("DateFrom"))
    .withColumn("DateFrom_Cutoff",
        F.when(F.col("DateFrom") > absent_lookbehind_cutoff, F.col("DateFrom"))
        .otherwise(F.lit(absent_lookbehind_cutoff)))
)

# Cross join with date range and filter
expected_days = (
    worker_dates
    .crossJoin(date_range)
    .filter(F.col("ExplodedDate") >= F.col("DateFrom_Cutoff"))
    .filter(F.col("ExplodedDate") <= today)
    .select("ProjectID", "WorkerID", "ExplodedDate")
)

# Apply project filter if specified
if project_filter:
    expected_days = expected_days.filter(F.col("ProjectID") == int(project_filter))

print(f"Expected days count: {expected_days.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Join Shifts with Reported Attendance
# MAGIC
# MAGIC FULL OUTER JOIN to capture:
# MAGIC - Days with shifts but no reported attendance
# MAGIC - Days with reported attendance but no shifts
# MAGIC - Days with both

# COMMAND ----------

# Prepare shifts data (filter to active shifts only)
shifts_prepared = (
    fact_workers_shifts
    .filter(F.col("ShiftLocalDate") >= absent_lookbehind_cutoff)
    .filter((F.col("ActiveTime") * 24 + F.coalesce(F.col("ReadingsMissedActive"), F.lit(0)) * (3.0/60)) > 1.0)
    .withColumn("RN", F.row_number().over(
        Window.partitionBy("ProjectID", "WorkerID", "ShiftLocalDate")
        .orderBy("StartAtUTC")
    ))
    .filter(F.col("RN") == 1)  # Take first shift per day
    .drop("RN")
)

# Prepare attendance data
attendance_prepared = (
    fact_reported_attendance
    .filter(F.col("ShiftLocalDate") >= absent_lookbehind_cutoff)
    .select(
        "ProjectID", "WorkerID", "ShiftLocalDate",
        "ReportedTime"
    )
)

# FULL OUTER JOIN shifts with expected days and attendance
combined_base = (
    shifts_prepared.alias("shifts")
    .join(
        attendance_prepared.alias("att"),
        ["ProjectID", "WorkerID", "ShiftLocalDate"],
        "full_outer"
    )
    .join(
        expected_days.alias("exp"),
        (F.coalesce(F.col("shifts.ProjectID"), F.col("att.ProjectID")) == F.col("exp.ProjectID")) &
        (F.coalesce(F.col("shifts.WorkerID"), F.col("att.WorkerID")) == F.col("exp.WorkerID")) &
        (F.coalesce(F.col("shifts.ShiftLocalDate"), F.col("att.ShiftLocalDate")) == F.col("exp.ExplodedDate")),
        "full_outer"
    )
    .select(
        F.coalesce(
            F.col("shifts.ProjectID"),
            F.col("att.ProjectID"),
            F.col("exp.ProjectID")
        ).alias("ProjectID"),
        F.coalesce(
            F.col("shifts.WorkerID"),
            F.col("att.WorkerID"),
            F.col("exp.WorkerID")
        ).alias("WorkerID"),
        F.coalesce(
            F.col("shifts.ShiftLocalDate"),
            F.col("att.ShiftLocalDate"),
            F.col("exp.ExplodedDate")
        ).alias("ShiftLocalDate"),
        F.col("shifts.StartAtUTC"),
        F.col("shifts.FinishAtUTC"),
        F.col("shifts.FirstDirectProductiveUTC"),
        F.col("shifts.LastDirectProductiveUTC"),
        F.col("shifts.FirstAssignedLocationUTC"),
        F.col("shifts.LastAssignedLocationUTC"),
        F.col("shifts.ActiveTime"),
        F.col("shifts.ActiveTimeBeforeShift"),
        F.col("shifts.ActiveTimeDuringShift"),
        F.col("shifts.ActiveTimeAfterShift"),
        F.col("shifts.Readings"),
        F.col("shifts.ReadingsMissedInactive"),
        F.col("shifts.ReadingsMissedInactiveDuringShift"),
        F.col("shifts.ReadingsMissedActive"),
        F.col("shifts.ReadingsMissedActiveDuringShift"),
        F.col("shifts.InactiveTime"),
        F.col("shifts.InactiveTimeDuringShift"),
        F.col("shifts.InactiveTimeBeforeShift"),
        F.col("shifts.InactiveTimeAfterShift"),
        F.col("shifts.ActiveTimeInDirectProductive"),
        F.col("shifts.ActiveTimeDuringShiftInDirectProductive"),
        F.col("shifts.ActiveTimeBeforeShiftInDirectProductive"),
        F.col("shifts.ActiveTimeAfterShiftInDirectProductive"),
        F.col("shifts.ActiveTimeInIndirectProductive"),
        F.col("shifts.ActiveTimeDuringShiftInIndirectProductive"),
        F.col("shifts.ActiveTimeBeforeShiftInIndirectProductive"),
        F.col("shifts.ActiveTimeAfterShiftInIndirectProductive"),
        F.col("shifts.InactiveTimeInDirectProductive"),
        F.col("shifts.InactiveTimeDuringShiftInDirectProductive"),
        F.col("shifts.InactiveTimeBeforeShiftInDirectProductive"),
        F.col("shifts.InactiveTimeAfterShiftInDirectProductive"),
        F.col("shifts.InactiveTimeInIndirectProductive"),
        F.col("shifts.InactiveTimeDuringShiftInIndirectProductive"),
        F.col("shifts.InactiveTimeBeforeShiftInIndirectProductive"),
        F.col("shifts.InactiveTimeAfterShiftInIndirectProductive"),
        F.col("shifts.ActiveTimeInAssignedLocation"),
        F.col("shifts.ActiveTimeDuringShiftInAssignedLocation"),
        F.col("shifts.ActiveTimeBeforeShiftInAssignedLocation"),
        F.col("shifts.ActiveTimeAfterShiftInAssignedLocation"),
        F.col("shifts.InactiveTimeInAssignedLocation"),
        F.col("shifts.InactiveTimeDuringShiftInAssignedLocation"),
        F.col("shifts.InactiveTimeBeforeShiftInAssignedLocation"),
        F.col("shifts.InactiveTimeAfterShiftInAssignedLocation"),
        F.col("att.ReportedTime")
    )
    .filter(
        F.col("ReportedTime").isNotNull() |
        F.col("StartAtUTC").isNotNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Enrich with Assignment Data

# COMMAND ----------

# Join with crew assignments
crew_enriched = (
    combined_base.alias("cb")
    .join(
        crew_assignments.alias("ca"),
        (F.col("cb.ProjectID") == F.col("ca.ProjectID")) &
        (F.col("cb.WorkerID") == F.col("ca.WorkerID")) &
        (F.col("cb.ShiftLocalDate") >= F.col("ca.ValidFrom_ShiftLocalDate")) &
        ((F.col("cb.ShiftLocalDate") < F.col("ca.ValidTo_ShiftLocalDate")) | F.col("ca.ValidTo_ShiftLocalDate").isNull()),
        "left"
    )
    .select(
        "cb.*",
        F.col("ca.CrewID")
    )
)

# Join with trade assignments
trade_enriched = (
    crew_enriched.alias("ce")
    .join(
        trade_assignments.alias("ta"),
        (F.col("ce.ProjectID") == F.col("ta.ProjectID")) &
        (F.col("ce.WorkerID") == F.col("ta.WorkerID")) &
        (F.col("ce.ShiftLocalDate") >= F.col("ta.ValidFrom_ShiftLocalDate")) &
        ((F.col("ce.ShiftLocalDate") < F.col("ta.ValidTo_ShiftLocalDate")) | F.col("ta.ValidTo_ShiftLocalDate").isNull()),
        "left"
    )
    .select(
        "ce.*",
        F.col("ta.TradeID")
    )
)

# Join with workshift assignments
workshift_enriched = (
    trade_enriched.alias("te")
    .join(
        workshift_assignments.alias("wsa"),
        (F.col("te.ProjectID") == F.col("wsa.ProjectID")) &
        (F.col("te.WorkerID") == F.col("wsa.WorkerID")) &
        (F.col("te.ShiftLocalDate") >= F.col("wsa.ValidFrom_ShiftLocalDate")) &
        ((F.col("te.ShiftLocalDate") < F.col("wsa.ValidTo_ShiftLocalDate")) | F.col("wsa.ValidTo_ShiftLocalDate").isNull()),
        "left"
    )
    .select(
        "te.*",
        F.col("wsa.WorkshiftID")
    )
)

# Join with workshift details (by day of week)
final_enriched = (
    workshift_enriched.alias("we")
    .join(
        workshift_details.filter(F.col("DayOfWeek").isNotNull()).alias("wsd"),
        (F.col("we.WorkshiftID") == F.col("wsd.WorkshiftID")) &
        (F.col("wsd.DayOfWeek") == ((F.dayofweek("we.ShiftLocalDate") + 5) % 7)),  # Convert to SQL Server day numbering
        "left"
    )
    .select(
        "we.*",
        F.col("wsd.WorkshiftDetailsID")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Join with Device Assignments for Inventory Tracking

# COMMAND ----------

# Join with device assignments for inventory dates
with_inventory = (
    final_enriched.alias("fe")
    .join(
        device_assignments.alias("da"),
        (F.col("fe.ProjectID") == F.col("da.ProjectID")) &
        (F.col("fe.WorkerID") == F.col("da.WorkerID")) &
        (F.col("fe.ShiftLocalDate") >= F.date_sub(F.col("da.ValidFrom"), 1)) &
        ((F.col("fe.ShiftLocalDate") < F.date_sub(F.col("da.ValidTo"), 1)) | F.col("da.ValidTo").isNull()),
        "left"
    )
    .select(
        "fe.*",
        F.col("da.ValidFrom").alias("InventoryAssignedOn"),
        F.col("da.ValidTo").alias("InventoryUnassignedOn")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Calculate Segment Number

# COMMAND ----------

# Add segment number (for multiple shifts per day)
result = (
    with_inventory
    .withColumn("Segment",
        F.row_number().over(
            Window.partitionBy("ProjectID", "WorkerID", "ShiftLocalDate")
            .orderBy(
                F.col("ReportedTime").desc_nulls_last(),
                F.col("ActiveTime").desc_nulls_last()
            )
        ))
    .withColumn("WatermarkUTC", F.current_timestamp())
)

print(f"Final result count: {result.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: MERGE into Target Table

# COMMAND ----------

target_table = "wakecap_prod.migration.fact_workers_shifts_combined"

# Select final columns
final_result = result.select(
    "ProjectID", "WorkerID", "ShiftLocalDate", "StartAtUTC",
    "CrewID", "TradeID", "WorkshiftID", "WorkshiftDetailsID",
    "FinishAtUTC", "FirstDirectProductiveUTC", "LastDirectProductiveUTC",
    "FirstAssignedLocationUTC", "LastAssignedLocationUTC",
    "Segment",
    "ActiveTime", "ActiveTimeBeforeShift", "ActiveTimeDuringShift", "ActiveTimeAfterShift",
    "Readings", "ReadingsMissedInactive", "ReadingsMissedInactiveDuringShift",
    "ReadingsMissedActive", "ReadingsMissedActiveDuringShift",
    "InactiveTime", "InactiveTimeDuringShift", "InactiveTimeBeforeShift", "InactiveTimeAfterShift",
    "ActiveTimeInDirectProductive", "ActiveTimeDuringShiftInDirectProductive",
    "ActiveTimeBeforeShiftInDirectProductive", "ActiveTimeAfterShiftInDirectProductive",
    "ActiveTimeInIndirectProductive", "ActiveTimeDuringShiftInIndirectProductive",
    "ActiveTimeBeforeShiftInIndirectProductive", "ActiveTimeAfterShiftInIndirectProductive",
    "InactiveTimeInDirectProductive", "InactiveTimeDuringShiftInDirectProductive",
    "InactiveTimeBeforeShiftInDirectProductive", "InactiveTimeAfterShiftInDirectProductive",
    "InactiveTimeInIndirectProductive", "InactiveTimeDuringShiftInIndirectProductive",
    "InactiveTimeBeforeShiftInIndirectProductive", "InactiveTimeAfterShiftInIndirectProductive",
    "ActiveTimeInAssignedLocation", "ActiveTimeDuringShiftInAssignedLocation",
    "ActiveTimeBeforeShiftInAssignedLocation", "ActiveTimeAfterShiftInAssignedLocation",
    "InactiveTimeInAssignedLocation", "InactiveTimeDuringShiftInAssignedLocation",
    "InactiveTimeBeforeShiftInAssignedLocation", "InactiveTimeAfterShiftInAssignedLocation",
    "ReportedTime", "InventoryAssignedOn", "InventoryUnassignedOn",
    "WatermarkUTC"
)

# Perform merge
if spark.catalog.tableExists(target_table):
    target = DeltaTable.forName(spark, target_table)

    (target.alias("t")
     .merge(
         final_result.alias("s"),
         """
         t.ProjectID = s.ProjectID AND
         t.WorkerID = s.WorkerID AND
         t.ShiftLocalDate = s.ShiftLocalDate AND
         (t.StartAtUTC = s.StartAtUTC OR (t.StartAtUTC IS NULL AND s.StartAtUTC IS NULL))
         """
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
    print(f"Merged into existing table {target_table}")
else:
    final_result.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Created new table {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Show sample results
display(spark.table(target_table).filter(
    F.col("ShiftLocalDate") >= absent_lookbehind_cutoff
).limit(100))

# COMMAND ----------

# Summary
summary = spark.table(target_table).filter(
    F.col("ShiftLocalDate") >= absent_lookbehind_cutoff
).agg(
    F.count("*").alias("total_records"),
    F.countDistinct("WorkerID").alias("unique_workers"),
    F.countDistinct("ProjectID").alias("unique_projects"),
    F.sum(F.when(F.col("StartAtUTC").isNotNull(), 1).otherwise(0)).alias("with_shift"),
    F.sum(F.when(F.col("ReportedTime").isNotNull(), 1).otherwise(0)).alias("with_reported_time"),
    F.sum(F.when(F.col("StartAtUTC").isNotNull() & F.col("ReportedTime").isNotNull(), 1).otherwise(0)).alias("with_both")
)
display(summary)
