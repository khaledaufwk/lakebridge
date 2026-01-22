# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate Fact Workers Shifts
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateFactWorkersShifts` (822 lines)
# MAGIC
# MAGIC **Purpose:** Calculate daily shift metrics for each worker based on observations from FactWorkersHistory.
# MAGIC
# MAGIC **Original Patterns:** CURSOR (over partitions), TEMP_TABLE, MERGE, WINDOW_FUNCTION, CTE
# MAGIC
# MAGIC **Conversion Approach:**
# MAGIC - CURSOR over partitions → Spark processes all partitions in parallel
# MAGIC - TEMP TABLE #PartitionedResults → DataFrame intermediate result
# MAGIC - Complex nested CTEs → Step-by-step DataFrame transformations
# MAGIC - MERGE → Delta MERGE operation
# MAGIC
# MAGIC **Key Business Logic:**
# MAGIC 1. Identify "islands" (sessions) separated by >5 hour gaps
# MAGIC 2. Split sessions that exceed 24 hours
# MAGIC 3. Calculate active/inactive time during shift, before shift, after shift
# MAGIC 4. Track readings missed (active vs inactive)
# MAGIC 5. Calculate time in direct/indirect productive areas
# MAGIC 6. Calculate time in assigned locations

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

# Widget for run date (optional)
dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("project_id", "", "Project ID (optional)")
dbutils.widgets.text("worker_id", "", "Worker ID (optional)")

# COMMAND ----------

# Get parameters
run_date_str = dbutils.widgets.get("run_date")
project_filter = dbutils.widgets.get("project_id")
worker_filter = dbutils.widgets.get("worker_id")

# Default lookback: 7 days if no date specified
if run_date_str:
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    min_date = run_date - timedelta(days=5)
    max_date = run_date + timedelta(days=5)
else:
    # Process recent changes based on watermark
    max_date = datetime.now().date()
    min_date = max_date - timedelta(days=14)

print(f"Processing date range: {min_date} to {max_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Source Data with Impacted Area Filter

# COMMAND ----------

# Load FactWorkersHistory - the main source table
fact_workers_history = spark.table("wakecap_prod.migration.bronze_dbo_FactWorkersHistory")

# Apply date filter
fwh_filtered = (
    fact_workers_history
    .filter(F.col("LocalDate").between(min_date, max_date))
)

# Apply optional project/worker filters
if project_filter:
    fwh_filtered = fwh_filtered.filter(F.col("ProjectID") == int(project_filter))
if worker_filter:
    fwh_filtered = fwh_filtered.filter(F.col("WorkerID") == int(worker_filter))

print(f"Source records to process: {fwh_filtered.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate Time Gaps and Reading Metrics
# MAGIC
# MAGIC This replaces the complex nested CTEs in the original procedure.

# COMMAND ----------

# Define window for calculations within worker-project-date
worker_window = Window.partitionBy("ProjectID", "WorkerID").orderBy("LocalDate", "TimestampUTC")
worker_window_unbounded = Window.partitionBy("ProjectID", "WorkerID").orderBy("LocalDate", "TimestampUTC").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Step 2a: Calculate base metrics and future reading gaps
step2a = (
    fwh_filtered
    .withColumn("TimestampUTC_Active",
        F.when(F.col("ActiveTime") > 0, F.col("TimestampUTC")))
    .withColumn("xFutureReadingsMissingActive",
        F.lead("Sequence", 1).over(worker_window) - F.col("Sequence") - 1)
    .withColumn("xFutureReadingsMissingInactive",
        F.lead("SequenceInactive", 1).over(worker_window) - F.col("SequenceInactive") - 1)
    .withColumn("FutureReadingIsActive",
        F.when(F.lead("ActiveTime", 1).over(worker_window) > 0, 1))
    .withColumn("FutureDuration",
        (F.unix_timestamp(F.lead("TimestampUTC", 1).over(worker_window)) -
         F.unix_timestamp("TimestampUTC")) / 86400.0)  # Convert to days
)

# Step 2b: Adjust missed readings to fit within time window
# FutureReadingsMissedActive: capped by available duration
step2b = (
    step2a
    .withColumn("FutureReadingsMissedActive",
        F.when(
            F.col("xFutureReadingsMissingActive").between(1, 400),
            F.col("xFutureReadingsMissingActive") +
            F.when(
                F.col("xFutureReadingsMissingInactive").between(1, 200) &
                (F.col("FutureReadingIsActive") != 1), 1
            ).otherwise(0)
        ))
    .withColumn("FutureReadingsMissedInactive",
        F.when(
            F.col("xFutureReadingsMissingInactive").between(1, 200),
            F.col("xFutureReadingsMissingInactive") +
            F.when(
                F.col("xFutureReadingsMissingActive").between(1, 200) &
                (F.col("FutureReadingIsActive") == 1), 1
            ).otherwise(0)
        ))
)

# Step 2c: Adjust for actual time available (3 min active, 10 min inactive readings)
step2c = (
    step2b
    .withColumn("FutureReadingsMissedActiveAdj",
        F.when(
            F.col("FutureReadingsMissedActive") * (3.0/(60*24)) > F.col("FutureDuration"),
            (F.col("FutureDuration") / (3.0/(60*24))).cast("int")
        ).otherwise(F.col("FutureReadingsMissedActive")))
    .withColumn("FutureReadingsMissedInactiveAdj",
        F.when(
            F.col("FutureReadingsMissedInactive") * (10.0/(60*24)) >
            F.col("FutureDuration") - F.coalesce(F.col("FutureReadingsMissedActiveAdj") * (3.0/(60*24)), F.lit(0)),
            F.when(
                F.col("FutureDuration") > F.coalesce(F.col("FutureReadingsMissedActiveAdj") * (3.0/(60*24)), F.lit(0)),
                ((F.col("FutureDuration") - F.coalesce(F.col("FutureReadingsMissedActiveAdj") * (3.0/(60*24)), F.lit(0))) / (10.0/(60*24))).cast("int")
            )
        ).otherwise(F.col("FutureReadingsMissedInactive")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Detect Session Islands (>5 hour gaps)

# COMMAND ----------

# Calculate lag of active timestamps to detect islands
step3 = (
    step2c
    .withColumn("TimestampUTC_Active_Lag",
        F.when(
            F.col("TimestampUTC_Active").isNotNull(),
            F.max("TimestampUTC_Active").over(
                Window.partitionBy("ProjectID", "WorkerID")
                .orderBy("LocalDate", "TimestampUTC")
                .rowsBetween(Window.unboundedPreceding, -1)
            )
        ))
    .withColumn("IslandStartFiltered",
        F.when(
            (F.col("TimestampUTC_Active").isNotNull()) &
            ((F.unix_timestamp("TimestampUTC_Active") - F.unix_timestamp("TimestampUTC_Active_Lag")) / 3600.0 > 5) |
            (F.col("TimestampUTC_Active_Lag").isNull()),
            F.col("TimestampUTC_Active")
        ))
)

# Assign island group numbers
step3b = (
    step3
    .withColumn("IslandGroupFiltered",
        F.sum(F.when(F.col("IslandStartFiltered").isNotNull(), 1)).over(worker_window_unbounded))
    .withColumn("FullDaysInSession",
        F.floor(
            (F.unix_timestamp("TimestampUTC") -
             F.unix_timestamp(F.max("IslandStartFiltered").over(worker_window_unbounded))) / 86400.0
        ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Determine Shift Edge Eligibility

# COMMAND ----------

# ShiftEdgeEligibleFilter: filter to readings that are within active shift boundaries
step4 = (
    step3b
    .withColumn("ShiftEdgeEligibleFilter",
        F.when(F.col("ActiveTime") > 0, F.col("TimestampUTC")))
)

# Calculate min/max shift edge times per island
island_window = Window.partitionBy("ProjectID", "WorkerID", "IslandGroupFiltered", "FullDaysInSession")
island_window_forward = island_window.orderBy("LocalDate", "TimestampUTC").rowsBetween(0, Window.unboundedFollowing)

step4b = (
    step4
    .withColumn("MinShiftEdge", F.min("ShiftEdgeEligibleFilter").over(island_window))
    .withColumn("MaxShiftEdge", F.max("ShiftEdgeEligibleFilter").over(island_window_forward))
    .withColumn("DuringShiftDetectedFilter",
        F.when(
            (F.col("TimestampUTC") >= F.col("MinShiftEdge")) &
            (F.col("TimestampUTC") <= F.col("MaxShiftEdge")),
            1
        ))
)

# Filter to only records during detected shift
step4c = step4b.filter(F.col("DuringShiftDetectedFilter") == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Aggregate Shift Metrics per Worker-Project-Island

# COMMAND ----------

# First reading duration for shift start adjustment
step5 = (
    step4c
    .withColumn("FirstReadingDuration",
        F.when(
            F.col("IslandStartFiltered").isNotNull() |
            (F.col("FullDaysInSession") != F.lag("FullDaysInSession", 1).over(worker_window)),
            F.coalesce(F.col("ActiveTime"), F.lit(0)) + F.coalesce(F.col("InactiveTime"), F.lit(0))
        ))
)

# Aggregate by ProjectID, WorkerID, IslandGroupFiltered, FullDaysInSession
partitioned_results = (
    step5
    .groupBy("ProjectID", "WorkerID", "IslandGroupFiltered", "FullDaysInSession")
    .agg(
        F.min("LocalDate").alias("ShiftLocalDate"),
        (F.min("TimestampUTC") - F.expr("INTERVAL 4 MINUTES")).alias("StartAtUTC"),  # Approx first reading duration
        (F.max("TimestampUTC") + F.expr("INTERVAL 3 MINUTES")).alias("FinishAtUTC"),  # Per Hassan 2024-10-06
        F.sum("ActiveTime").alias("ActiveTime"),
        F.sum(F.when(F.col("TimeCategoryID") == 5, F.col("ActiveTime"))).alias("ActiveTimeBeforeShift"),
        F.sum(F.when(F.col("TimeCategoryID") == 1, F.col("ActiveTime"))).alias("ActiveTimeDuringShift"),
        F.sum(F.when(F.col("TimeCategoryID") == 4, F.col("ActiveTime"))).alias("ActiveTimeAfterShift"),
        F.min(F.when((F.col("ActiveTime") > 0) & (F.col("ProductiveClassID") == 1), F.col("TimestampUTC"))).alias("FirstDirectProductiveUTC"),
        F.max(F.when((F.col("ActiveTime") > 0) & (F.col("ProductiveClassID") == 1), F.col("TimestampUTC"))).alias("LastDirectProductiveUTC"),
        F.min(F.when((F.col("ActiveTime") > 0) & (F.col("LocationAssignmentClassID") == 1), F.col("TimestampUTC"))).alias("FirstAssignedLocationUTC"),
        F.max(F.when((F.col("ActiveTime") > 0) & (F.col("LocationAssignmentClassID") == 1), F.col("TimestampUTC"))).alias("LastAssignedLocationUTC"),
        F.sum("FutureReadingsMissedActiveAdj").alias("ReadingsMissedActive"),
        F.sum(F.when(F.col("TimeCategoryID") == 1, F.col("FutureReadingsMissedActiveAdj"))).alias("ReadingsMissedActiveDuringShift"),
        F.sum("FutureReadingsMissedInactiveAdj").alias("ReadingsMissedInactive"),
        F.sum(F.when(F.col("TimeCategoryID") == 1, F.col("FutureReadingsMissedInactiveAdj"))).alias("ReadingsMissedInactiveDuringShift"),
        F.count("*").alias("Readings"),
        F.sum("InactiveTime").alias("InactiveTime"),
        F.sum(F.when(F.col("TimeCategoryID") == 1, F.col("InactiveTime"))).alias("InactiveTimeDuringShift"),
        F.sum(F.when(F.col("TimeCategoryID") == 5, F.col("InactiveTime"))).alias("InactiveTimeBeforeShift"),
        F.sum(F.when(F.col("TimeCategoryID") == 4, F.col("InactiveTime"))).alias("InactiveTimeAfterShift"),
        # Direct Productive time metrics
        F.sum(F.when(F.col("ProductiveClassID") == 1, F.col("ActiveTime"))).alias("ActiveTimeInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 1) & (F.col("ProductiveClassID") == 1), F.col("ActiveTime"))).alias("ActiveTimeDuringShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 5) & (F.col("ProductiveClassID") == 1), F.col("ActiveTime"))).alias("ActiveTimeBeforeShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 4) & (F.col("ProductiveClassID") == 1), F.col("ActiveTime"))).alias("ActiveTimeAfterShiftInDirectProductive"),
        # Indirect Productive time metrics
        F.sum(F.when(F.col("ProductiveClassID") == 2, F.col("ActiveTime"))).alias("ActiveTimeInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 1) & (F.col("ProductiveClassID") == 2), F.col("ActiveTime"))).alias("ActiveTimeDuringShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 5) & (F.col("ProductiveClassID") == 2), F.col("ActiveTime"))).alias("ActiveTimeBeforeShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 4) & (F.col("ProductiveClassID") == 2), F.col("ActiveTime"))).alias("ActiveTimeAfterShiftInIndirectProductive"),
        # Inactive in Direct Productive
        F.sum(F.when(F.col("ProductiveClassID") == 1, F.col("InactiveTime"))).alias("InactiveTimeInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 1) & (F.col("ProductiveClassID") == 1), F.col("InactiveTime"))).alias("InactiveTimeDuringShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 5) & (F.col("ProductiveClassID") == 1), F.col("InactiveTime"))).alias("InactiveTimeBeforeShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 4) & (F.col("ProductiveClassID") == 1), F.col("InactiveTime"))).alias("InactiveTimeAfterShiftInDirectProductive"),
        # Inactive in Indirect Productive
        F.sum(F.when(F.col("ProductiveClassID") == 2, F.col("InactiveTime"))).alias("InactiveTimeInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 1) & (F.col("ProductiveClassID") == 2), F.col("InactiveTime"))).alias("InactiveTimeDuringShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 5) & (F.col("ProductiveClassID") == 2), F.col("InactiveTime"))).alias("InactiveTimeBeforeShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryID") == 4) & (F.col("ProductiveClassID") == 2), F.col("InactiveTime"))).alias("InactiveTimeAfterShiftInIndirectProductive"),
        # Assigned Location time metrics
        F.sum(F.when(F.col("LocationAssignmentClassID") == 1, F.col("ActiveTime"))).alias("ActiveTimeInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryID") == 1) & (F.col("LocationAssignmentClassID") == 1), F.col("ActiveTime"))).alias("ActiveTimeDuringShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryID") == 5) & (F.col("LocationAssignmentClassID") == 1), F.col("ActiveTime"))).alias("ActiveTimeBeforeShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryID") == 4) & (F.col("LocationAssignmentClassID") == 1), F.col("ActiveTime"))).alias("ActiveTimeAfterShiftInAssignedLocation"),
        F.sum(F.when(F.col("LocationAssignmentClassID") == 1, F.col("InactiveTime"))).alias("InactiveTimeInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryID") == 1) & (F.col("LocationAssignmentClassID") == 1), F.col("InactiveTime"))).alias("InactiveTimeDuringShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryID") == 5) & (F.col("LocationAssignmentClassID") == 1), F.col("InactiveTime"))).alias("InactiveTimeBeforeShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryID") == 4) & (F.col("LocationAssignmentClassID") == 1), F.col("InactiveTime"))).alias("InactiveTimeAfterShiftInAssignedLocation"),
    )
)

print(f"Partitioned results count: {partitioned_results.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Stitch Adjacent Sessions (<5 hours apart, <20 hours total)

# COMMAND ----------

# Order sessions within worker and detect session groups
session_window = Window.partitionBy("ProjectID", "WorkerID").orderBy("StartAtUTC")

stitched = (
    partitioned_results
    .withColumn("PrevFinishAtUTC", F.lag("FinishAtUTC", 1).over(session_window))
    .withColumn("PrevStartAtUTC", F.lag("StartAtUTC", 1).over(session_window))
    .withColumn("GapHours",
        (F.unix_timestamp("StartAtUTC") - F.unix_timestamp("PrevFinishAtUTC")) / 3600.0)
    .withColumn("PrevDuration",
        (F.unix_timestamp("PrevFinishAtUTC") - F.unix_timestamp("PrevStartAtUTC")) / 3600.0)
    .withColumn("CurrentDuration",
        (F.unix_timestamp("FinishAtUTC") - F.unix_timestamp("StartAtUTC")) / 3600.0)
    .withColumn("NewSession",
        F.when(
            (F.col("GapHours") > 5) |  # Gap > 5 hours
            (F.col("PrevDuration") + F.col("CurrentDuration") > 20),  # Combined > 20 hours
            1
        ))
    .withColumn("SessionGroup",
        F.sum("NewSession").over(
            Window.partitionBy("ProjectID", "WorkerID")
            .orderBy("StartAtUTC")
            .rowsBetween(Window.unboundedPreceding, 0)
        ))
)

# Aggregate stitched sessions
final_shifts = (
    stitched
    .groupBy("ProjectID", "WorkerID", "SessionGroup")
    .agg(
        F.min("ShiftLocalDate").alias("ShiftLocalDate_Unaligned"),
        F.min("StartAtUTC").alias("StartAtUTC"),
        F.max("FinishAtUTC").alias("FinishAtUTC"),
        F.sum("ActiveTime").alias("ActiveTime"),
        F.sum("ActiveTimeBeforeShift").alias("ActiveTimeBeforeShift"),
        F.sum("ActiveTimeDuringShift").alias("ActiveTimeDuringShift"),
        F.sum("ActiveTimeAfterShift").alias("ActiveTimeAfterShift"),
        F.min("FirstDirectProductiveUTC").alias("FirstDirectProductiveUTC"),
        F.max("LastDirectProductiveUTC").alias("LastDirectProductiveUTC"),
        F.min("FirstAssignedLocationUTC").alias("FirstAssignedLocationUTC"),
        F.max("LastAssignedLocationUTC").alias("LastAssignedLocationUTC"),
        F.sum("ReadingsMissedActive").alias("ReadingsMissedActive"),
        F.sum("ReadingsMissedActiveDuringShift").alias("ReadingsMissedActiveDuringShift"),
        F.sum("ReadingsMissedInactive").alias("ReadingsMissedInactive"),
        F.sum("ReadingsMissedInactiveDuringShift").alias("ReadingsMissedInactiveDuringShift"),
        F.sum("Readings").alias("Readings"),
        F.sum("InactiveTime").alias("InactiveTime"),
        F.sum("InactiveTimeDuringShift").alias("InactiveTimeDuringShift"),
        F.sum("InactiveTimeBeforeShift").alias("InactiveTimeBeforeShift"),
        F.sum("InactiveTimeAfterShift").alias("InactiveTimeAfterShift"),
        # All the productive/location metrics
        F.sum("ActiveTimeInDirectProductive").alias("ActiveTimeInDirectProductive"),
        F.sum("ActiveTimeDuringShiftInDirectProductive").alias("ActiveTimeDuringShiftInDirectProductive"),
        F.sum("ActiveTimeBeforeShiftInDirectProductive").alias("ActiveTimeBeforeShiftInDirectProductive"),
        F.sum("ActiveTimeAfterShiftInDirectProductive").alias("ActiveTimeAfterShiftInDirectProductive"),
        F.sum("ActiveTimeInIndirectProductive").alias("ActiveTimeInIndirectProductive"),
        F.sum("ActiveTimeDuringShiftInIndirectProductive").alias("ActiveTimeDuringShiftInIndirectProductive"),
        F.sum("ActiveTimeBeforeShiftInIndirectProductive").alias("ActiveTimeBeforeShiftInIndirectProductive"),
        F.sum("ActiveTimeAfterShiftInIndirectProductive").alias("ActiveTimeAfterShiftInIndirectProductive"),
        F.sum("InactiveTimeInDirectProductive").alias("InactiveTimeInDirectProductive"),
        F.sum("InactiveTimeDuringShiftInDirectProductive").alias("InactiveTimeDuringShiftInDirectProductive"),
        F.sum("InactiveTimeBeforeShiftInDirectProductive").alias("InactiveTimeBeforeShiftInDirectProductive"),
        F.sum("InactiveTimeAfterShiftInDirectProductive").alias("InactiveTimeAfterShiftInDirectProductive"),
        F.sum("InactiveTimeInIndirectProductive").alias("InactiveTimeInIndirectProductive"),
        F.sum("InactiveTimeDuringShiftInIndirectProductive").alias("InactiveTimeDuringShiftInIndirectProductive"),
        F.sum("InactiveTimeBeforeShiftInIndirectProductive").alias("InactiveTimeBeforeShiftInIndirectProductive"),
        F.sum("InactiveTimeAfterShiftInIndirectProductive").alias("InactiveTimeAfterShiftInIndirectProductive"),
        F.sum("ActiveTimeInAssignedLocation").alias("ActiveTimeInAssignedLocation"),
        F.sum("ActiveTimeDuringShiftInAssignedLocation").alias("ActiveTimeDuringShiftInAssignedLocation"),
        F.sum("ActiveTimeBeforeShiftInAssignedLocation").alias("ActiveTimeBeforeShiftInAssignedLocation"),
        F.sum("ActiveTimeAfterShiftInAssignedLocation").alias("ActiveTimeAfterShiftInAssignedLocation"),
        F.sum("InactiveTimeInAssignedLocation").alias("InactiveTimeInAssignedLocation"),
        F.sum("InactiveTimeDuringShiftInAssignedLocation").alias("InactiveTimeDuringShiftInAssignedLocation"),
        F.sum("InactiveTimeBeforeShiftInAssignedLocation").alias("InactiveTimeBeforeShiftInAssignedLocation"),
        F.sum("InactiveTimeAfterShiftInAssignedLocation").alias("InactiveTimeAfterShiftInAssignedLocation"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Align ShiftLocalDate with Workshift Schedule

# COMMAND ----------

# Load Project and WorkshiftAssignments for timezone and shift alignment
projects = spark.table("wakecap_prod.migration.bronze_dbo_Project").select("ProjectID", "TimeZoneName")
workshift_assignments = spark.table("wakecap_prod.migration.bronze_dbo_WorkshiftAssignments")
workshift_details_dow = spark.table("wakecap_prod.migration.bronze_dbo_WorkshiftDetails").filter(F.col("DayOfWeek").isNotNull())

# Join to get timezone and workshift info
aligned = (
    final_shifts
    .join(projects, "ProjectID", "left")
    .join(
        workshift_assignments.alias("wsa"),
        (F.col("ProjectID") == F.col("wsa.ProjectID")) &
        (F.col("WorkerID") == F.col("wsa.WorkerID")) &
        (F.col("ShiftLocalDate_Unaligned") >= F.col("wsa.ValidFrom_ShiftLocalDate")) &
        ((F.col("ShiftLocalDate_Unaligned") < F.col("wsa.ValidTo_ShiftLocalDate")) | F.col("wsa.ValidTo_ShiftLocalDate").isNull()),
        "left"
    )
)

# Calculate start discrepancy to determine if shift should be +/- 1 day
# Simplified: use ShiftLocalDate_Unaligned directly for now
# In production, would need full workshift details lookup
final_with_date = (
    aligned
    .withColumn("ShiftLocalDate", F.col("ShiftLocalDate_Unaligned"))
    .withColumn("ExtSourceID", F.lit(5))
    .withColumn("WatermarkUTC", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: MERGE into Target Table

# COMMAND ----------

target_table = "wakecap_prod.migration.fact_workers_shifts"

# Select final columns
shift_metrics = final_with_date.select(
    "ProjectID", "WorkerID", "ShiftLocalDate", "StartAtUTC", "FinishAtUTC",
    "ActiveTime", "ActiveTimeBeforeShift", "ActiveTimeDuringShift", "ActiveTimeAfterShift",
    "FirstDirectProductiveUTC", "LastDirectProductiveUTC",
    "FirstAssignedLocationUTC", "LastAssignedLocationUTC",
    "ReadingsMissedActive", "ReadingsMissedActiveDuringShift",
    "ReadingsMissedInactive", "ReadingsMissedInactiveDuringShift",
    "Readings", "InactiveTime", "InactiveTimeDuringShift",
    "InactiveTimeBeforeShift", "InactiveTimeAfterShift",
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
    "ExtSourceID", "WatermarkUTC"
)

# Check if target exists
if spark.catalog.tableExists(target_table):
    target = DeltaTable.forName(spark, target_table)

    (target.alias("t")
     .merge(
         shift_metrics.alias("s"),
         """
         t.ProjectID = s.ProjectID AND
         t.WorkerID = s.WorkerID AND
         t.StartAtUTC = s.StartAtUTC
         """
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
    print(f"Merged into existing table {target_table}")
else:
    # Create table if doesn't exist
    shift_metrics.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Created new table {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Show sample results
display(spark.table(target_table).filter(
    F.col("ShiftLocalDate").between(min_date, max_date)
).limit(100))

# COMMAND ----------

# Summary statistics
summary = spark.table(target_table).filter(
    F.col("ShiftLocalDate").between(min_date, max_date)
).agg(
    F.count("*").alias("total_shifts"),
    F.countDistinct("WorkerID").alias("unique_workers"),
    F.countDistinct("ProjectID").alias("unique_projects"),
    F.avg("ActiveTime").alias("avg_active_time"),
    F.avg("Readings").alias("avg_readings")
)
display(summary)
