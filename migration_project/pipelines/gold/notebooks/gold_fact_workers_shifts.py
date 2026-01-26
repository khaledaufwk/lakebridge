# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Workers Shifts
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateFactWorkersShifts` (822 lines)
# MAGIC
# MAGIC **Purpose:** Calculate daily shift metrics for each worker based on observations from gold_fact_workers_history.
# MAGIC
# MAGIC **Original Patterns:** CURSOR (over partitions), TEMP_TABLE, MERGE, WINDOW_FUNCTION, CTE
# MAGIC
# MAGIC **Conversion Approach:**
# MAGIC - CURSOR over partitions -> Spark processes all partitions in parallel
# MAGIC - TEMP TABLE #PartitionedResults -> DataFrame intermediate result
# MAGIC - Complex nested CTEs -> Step-by-step DataFrame transformations
# MAGIC - MERGE -> Delta MERGE operation
# MAGIC
# MAGIC **Key Business Logic:**
# MAGIC 1. Identify "islands" (sessions) separated by >5 hour gaps
# MAGIC 2. Split sessions that exceed 24 hours
# MAGIC 3. Calculate active/inactive time during shift, before shift, after shift
# MAGIC 4. Track readings missed (active vs inactive)
# MAGIC 5. Calculate time in direct/indirect productive areas
# MAGIC 6. Calculate time in assigned locations
# MAGIC
# MAGIC **Source:** `wakecap_prod.gold.gold_fact_workers_history`
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_workers_shifts`

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

# Catalog and Schema Configuration
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source and target tables
SOURCE_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.gold_fact_workers_history"
TARGET_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.gold_fact_workers_shifts"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# COMMAND ----------

# Widget for run parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("project_id", "", "Project ID (optional)")
dbutils.widgets.text("worker_id", "", "Worker ID (optional)")
dbutils.widgets.text("lookback_days", "14", "Lookback Days")

# COMMAND ----------

# Get parameters
load_mode = dbutils.widgets.get("load_mode")
run_date_str = dbutils.widgets.get("run_date")
project_filter = dbutils.widgets.get("project_id")
worker_filter = dbutils.widgets.get("worker_id")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# Determine date range
if run_date_str:
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    min_date = run_date - timedelta(days=5)
    max_date = run_date + timedelta(days=5)
else:
    max_date = datetime.now().date()
    min_date = max_date - timedelta(days=lookback_days)

print(f"Load Mode: {load_mode}")
print(f"Processing date range: {min_date} to {max_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_watermark(table_name):
    """Get last processed watermark for incremental loading."""
    try:
        result = spark.sql(f"""
            SELECT last_watermark_value
            FROM {WATERMARK_TABLE}
            WHERE table_name = '{table_name}'
        """).collect()
        if result:
            return result[0][0]
    except:
        pass
    return datetime(1900, 1, 1)


def update_watermark(table_name, watermark_value, row_count):
    """Update watermark after successful processing."""
    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING (SELECT '{table_name}' as table_name,
                      CAST('{watermark_value}' AS TIMESTAMP) as last_watermark_value,
                      {row_count} as row_count,
                      current_timestamp() as last_processed_at,
                      current_timestamp() as updated_at) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            target.last_watermark_value = source.last_watermark_value,
            target.row_count = source.row_count,
            target.last_processed_at = source.last_processed_at,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Source Data with Impacted Area Filter

# COMMAND ----------

# Load gold_fact_workers_history - the main source table
print("Loading source table...")
fact_workers_history = spark.table(SOURCE_TABLE)

# Apply date filter
fwh_filtered = (
    fact_workers_history
    .filter(F.col("ShiftLocalDate").between(min_date, max_date))
)

# Apply optional project/worker filters
if project_filter:
    fwh_filtered = fwh_filtered.filter(F.col("ProjectId") == int(project_filter))
if worker_filter:
    fwh_filtered = fwh_filtered.filter(F.col("WorkerId") == int(worker_filter))

# CRITICAL: Filter to only records with valid WorkerId
# The shifts calculation requires WorkerId for grouping
fwh_filtered = fwh_filtered.filter(F.col("WorkerId").isNotNull())

source_count = fwh_filtered.count()
print(f"Source records with valid WorkerId: {source_count:,}")

if source_count == 0:
    print("[WARN] No records with valid WorkerId found in date range.")
    print("       The gold_fact_workers_history table may have placeholder WorkerIds.")
    print("       Shifts calculation requires valid WorkerId values.")
    print("       Exiting with success - no data to process.")
    dbutils.notebook.exit("SUCCESS - No records with valid WorkerId")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate Time Gaps and Reading Metrics
# MAGIC
# MAGIC This replaces the complex nested CTEs in the original procedure.

# COMMAND ----------

# Define window for calculations within worker-project-date
worker_window = Window.partitionBy("ProjectId", "WorkerId").orderBy("ShiftLocalDate", "TimestampUTC")
worker_window_unbounded = Window.partitionBy("ProjectId", "WorkerId").orderBy("ShiftLocalDate", "TimestampUTC").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

print("Step 2 complete - Time gaps and reading metrics calculated")

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
                Window.partitionBy("ProjectId", "WorkerId")
                .orderBy("ShiftLocalDate", "TimestampUTC")
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

print("Step 3 complete - Session islands detected")

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
island_window = Window.partitionBy("ProjectId", "WorkerId", "IslandGroupFiltered", "FullDaysInSession")
island_window_forward = island_window.orderBy("ShiftLocalDate", "TimestampUTC").rowsBetween(0, Window.unboundedFollowing)

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

print("Step 4 complete - Shift edge eligibility determined")

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

# Aggregate by ProjectId, WorkerId, IslandGroupFiltered, FullDaysInSession
partitioned_results = (
    step5
    .groupBy("ProjectId", "WorkerId", "IslandGroupFiltered", "FullDaysInSession")
    .agg(
        F.min("ShiftLocalDate").alias("ShiftLocalDate"),
        (F.min("TimestampUTC") - F.expr("INTERVAL 4 MINUTES")).alias("StartAtUTC"),  # Approx first reading duration
        (F.max("TimestampUTC") + F.expr("INTERVAL 3 MINUTES")).alias("FinishAtUTC"),  # Per Hassan 2024-10-06
        F.sum("ActiveTime").alias("ActiveTime"),
        F.sum(F.when(F.col("TimeCategoryId") == 5, F.col("ActiveTime"))).alias("ActiveTimeBeforeShift"),
        F.sum(F.when(F.col("TimeCategoryId") == 1, F.col("ActiveTime"))).alias("ActiveTimeDuringShift"),
        F.sum(F.when(F.col("TimeCategoryId") == 4, F.col("ActiveTime"))).alias("ActiveTimeAfterShift"),
        F.min(F.when((F.col("ActiveTime") > 0) & (F.col("ProductiveClassId") == 1), F.col("TimestampUTC"))).alias("FirstDirectProductiveUTC"),
        F.max(F.when((F.col("ActiveTime") > 0) & (F.col("ProductiveClassId") == 1), F.col("TimestampUTC"))).alias("LastDirectProductiveUTC"),
        F.min(F.when((F.col("ActiveTime") > 0) & (F.col("LocationAssignmentClassId") == 1), F.col("TimestampUTC"))).alias("FirstAssignedLocationUTC"),
        F.max(F.when((F.col("ActiveTime") > 0) & (F.col("LocationAssignmentClassId") == 1), F.col("TimestampUTC"))).alias("LastAssignedLocationUTC"),
        F.sum("FutureReadingsMissedActiveAdj").alias("ReadingsMissedActive"),
        F.sum(F.when(F.col("TimeCategoryId") == 1, F.col("FutureReadingsMissedActiveAdj"))).alias("ReadingsMissedActiveDuringShift"),
        F.sum("FutureReadingsMissedInactiveAdj").alias("ReadingsMissedInactive"),
        F.sum(F.when(F.col("TimeCategoryId") == 1, F.col("FutureReadingsMissedInactiveAdj"))).alias("ReadingsMissedInactiveDuringShift"),
        F.count("*").alias("Readings"),
        F.sum("InactiveTime").alias("InactiveTime"),
        F.sum(F.when(F.col("TimeCategoryId") == 1, F.col("InactiveTime"))).alias("InactiveTimeDuringShift"),
        F.sum(F.when(F.col("TimeCategoryId") == 5, F.col("InactiveTime"))).alias("InactiveTimeBeforeShift"),
        F.sum(F.when(F.col("TimeCategoryId") == 4, F.col("InactiveTime"))).alias("InactiveTimeAfterShift"),
        # Direct Productive time metrics
        F.sum(F.when(F.col("ProductiveClassId") == 1, F.col("ActiveTime"))).alias("ActiveTimeInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 1) & (F.col("ProductiveClassId") == 1), F.col("ActiveTime"))).alias("ActiveTimeDuringShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 5) & (F.col("ProductiveClassId") == 1), F.col("ActiveTime"))).alias("ActiveTimeBeforeShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 4) & (F.col("ProductiveClassId") == 1), F.col("ActiveTime"))).alias("ActiveTimeAfterShiftInDirectProductive"),
        # Indirect Productive time metrics
        F.sum(F.when(F.col("ProductiveClassId") == 2, F.col("ActiveTime"))).alias("ActiveTimeInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 1) & (F.col("ProductiveClassId") == 2), F.col("ActiveTime"))).alias("ActiveTimeDuringShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 5) & (F.col("ProductiveClassId") == 2), F.col("ActiveTime"))).alias("ActiveTimeBeforeShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 4) & (F.col("ProductiveClassId") == 2), F.col("ActiveTime"))).alias("ActiveTimeAfterShiftInIndirectProductive"),
        # Inactive in Direct Productive
        F.sum(F.when(F.col("ProductiveClassId") == 1, F.col("InactiveTime"))).alias("InactiveTimeInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 1) & (F.col("ProductiveClassId") == 1), F.col("InactiveTime"))).alias("InactiveTimeDuringShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 5) & (F.col("ProductiveClassId") == 1), F.col("InactiveTime"))).alias("InactiveTimeBeforeShiftInDirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 4) & (F.col("ProductiveClassId") == 1), F.col("InactiveTime"))).alias("InactiveTimeAfterShiftInDirectProductive"),
        # Inactive in Indirect Productive
        F.sum(F.when(F.col("ProductiveClassId") == 2, F.col("InactiveTime"))).alias("InactiveTimeInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 1) & (F.col("ProductiveClassId") == 2), F.col("InactiveTime"))).alias("InactiveTimeDuringShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 5) & (F.col("ProductiveClassId") == 2), F.col("InactiveTime"))).alias("InactiveTimeBeforeShiftInIndirectProductive"),
        F.sum(F.when((F.col("TimeCategoryId") == 4) & (F.col("ProductiveClassId") == 2), F.col("InactiveTime"))).alias("InactiveTimeAfterShiftInIndirectProductive"),
        # Assigned Location time metrics
        F.sum(F.when(F.col("LocationAssignmentClassId") == 1, F.col("ActiveTime"))).alias("ActiveTimeInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryId") == 1) & (F.col("LocationAssignmentClassId") == 1), F.col("ActiveTime"))).alias("ActiveTimeDuringShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryId") == 5) & (F.col("LocationAssignmentClassId") == 1), F.col("ActiveTime"))).alias("ActiveTimeBeforeShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryId") == 4) & (F.col("LocationAssignmentClassId") == 1), F.col("ActiveTime"))).alias("ActiveTimeAfterShiftInAssignedLocation"),
        F.sum(F.when(F.col("LocationAssignmentClassId") == 1, F.col("InactiveTime"))).alias("InactiveTimeInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryId") == 1) & (F.col("LocationAssignmentClassId") == 1), F.col("InactiveTime"))).alias("InactiveTimeDuringShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryId") == 5) & (F.col("LocationAssignmentClassId") == 1), F.col("InactiveTime"))).alias("InactiveTimeBeforeShiftInAssignedLocation"),
        F.sum(F.when((F.col("TimeCategoryId") == 4) & (F.col("LocationAssignmentClassId") == 1), F.col("InactiveTime"))).alias("InactiveTimeAfterShiftInAssignedLocation"),
    )
)

print(f"Step 5 complete - Partitioned results count: {partitioned_results.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Stitch Adjacent Sessions (<5 hours apart, <20 hours total)

# COMMAND ----------

# Order sessions within worker and detect session groups
session_window = Window.partitionBy("ProjectId", "WorkerId").orderBy("StartAtUTC")

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
            Window.partitionBy("ProjectId", "WorkerId")
            .orderBy("StartAtUTC")
            .rowsBetween(Window.unboundedPreceding, 0)
        ))
)

# Aggregate stitched sessions
final_shifts = (
    stitched
    .groupBy("ProjectId", "WorkerId", "SessionGroup")
    .agg(
        F.min("ShiftLocalDate").alias("ShiftLocalDate"),
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

print("Step 6 complete - Sessions stitched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Add Metadata and Prepare Final Output

# COMMAND ----------

# Add metadata columns
shift_metrics = (
    final_shifts
    .withColumn("ExtSourceId", F.lit(17))  # 17 = TimescaleDB
    .withColumn("WatermarkUTC", F.current_timestamp())
    .withColumn("_processed_at", F.current_timestamp())
    .drop("SessionGroup")
)

# Select final columns in order
shift_metrics = shift_metrics.select(
    "ProjectId", "WorkerId", "ShiftLocalDate", "StartAtUTC", "FinishAtUTC",
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
    "ExtSourceId", "WatermarkUTC", "_processed_at"
)

print("Sample output:")
shift_metrics.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: MERGE into Target Table

# COMMAND ----------

# Check if target exists
if spark.catalog.tableExists(TARGET_TABLE):
    print(f"Merging into existing table: {TARGET_TABLE}")

    target = DeltaTable.forName(spark, TARGET_TABLE)

    (target.alias("t")
     .merge(
         shift_metrics.alias("s"),
         """
         t.ProjectId = s.ProjectId AND
         t.WorkerId = s.WorkerId AND
         t.StartAtUTC = s.StartAtUTC
         """
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )

    merge_count = shift_metrics.count()
    print(f"Merged {merge_count:,} records")
else:
    print(f"Creating new table: {TARGET_TABLE}")

    (shift_metrics
     .write
     .format("delta")
     .mode("overwrite")
     .partitionBy("ShiftLocalDate")
     .option("overwriteSchema", "true")
     .saveAsTable(TARGET_TABLE)
    )

    print(f"Created table with {shift_metrics.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Update Watermark

# COMMAND ----------

# Get max watermark from processed data
max_watermark = shift_metrics.agg(F.max("WatermarkUTC")).collect()[0][0]
final_count = spark.table(TARGET_TABLE).count()

update_watermark("gold_fact_workers_shifts", max_watermark, final_count)
print(f"Updated watermark to: {max_watermark}")
print(f"Total records in table: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Show sample results
display(spark.table(TARGET_TABLE).filter(
    F.col("ShiftLocalDate").between(min_date, max_date)
).limit(100))

# COMMAND ----------

# Summary statistics
summary = spark.sql(f"""
    SELECT
        COUNT(*) as total_shifts,
        COUNT(DISTINCT WorkerId) as unique_workers,
        COUNT(DISTINCT ProjectId) as unique_projects,
        MIN(ShiftLocalDate) as min_date,
        MAX(ShiftLocalDate) as max_date,
        AVG(ActiveTime) as avg_active_time,
        AVG(Readings) as avg_readings
    FROM {TARGET_TABLE}
""")
display(summary)

# COMMAND ----------

print("Gold Fact Workers Shifts pipeline complete!")
dbutils.notebook.exit("SUCCESS")
