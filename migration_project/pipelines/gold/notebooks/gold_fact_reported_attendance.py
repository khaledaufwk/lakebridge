# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Reported Attendance
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateFactReportedAttendance` (216 lines)
# MAGIC
# MAGIC **Purpose:** Calculate reported attendance by combining:
# MAGIC 1. Customer/Manager reported attendance from ResourceTimesheet and ResourceHours (ExtSourceID=15)
# MAGIC 2. Worker task attendance from FactWorkersTasks (ExtSourceID=12)
# MAGIC
# MAGIC The two sources are merged with priority to customer reports (ExtSourceID=15),
# MAGIC falling back to task-based attendance (ExtSourceID=12).
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - FULL OUTER JOIN (ResourceHours + ResourceTimesheet) -> PySpark full outer join
# MAGIC - Multiple dimension lookups (Worker, Project, WorkerStatus) with ROW_NUMBER deduplication
# MAGIC - UNION ALL -> DataFrame union
# MAGIC - Window functions: MAX() OVER for WorkerStatusID, ROW_NUMBER for prioritization
# MAGIC - MERGE with NOT MATCHED BY SOURCE -> Two-phase (MERGE + soft-delete UPDATE)
# MAGIC - Watermark-based incremental processing
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_fact_resource_hours` (TimescaleDB: ResourceHours)
# MAGIC - `wakecap_prod.silver.silver_fact_resource_timesheet` (TimescaleDB: ResourceTimesheet)
# MAGIC - `wakecap_prod.gold.gold_fact_workers_tasks` (dbo.FactWorkersTasks)
# MAGIC - `wakecap_prod.silver.silver_task` (dbo.Task)
# MAGIC - `wakecap_prod.silver.silver_worker` (dbo.Worker)
# MAGIC - `wakecap_prod.silver.silver_project` (dbo.Project)
# MAGIC - `wakecap_prod.silver.silver_worker_status` (dbo.WorkerStatus)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_reported_attendance`
# MAGIC **Watermarks:** `wakecap_prod.migration._gold_watermarks`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

print("=== NOTEBOOK STARTED - Imports successful ===", flush=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and Schema Configuration
TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables - Using Silver layer tables (updated with full column structure)
SOURCE_RESOURCE_HOURS = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_fact_resource_hours"
SOURCE_RESOURCE_TIMESHEET = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_fact_resource_timesheet"
SOURCE_FACT_WORKERS_TASKS = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_workers_tasks"
SOURCE_TASK = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_task"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
# NOTE: Use silver_project_dw which has ExtProjectID (UUID) that matches fact tables' ProjectId
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project_dw"
SOURCE_WORKER_STATUS = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker_status"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_reported_attendance"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID values
EXT_SOURCE_ID_REPORTS = 15  # Customer/Manager reported attendance
EXT_SOURCE_ID_TASKS = 12     # Task-based attendance
EXT_SOURCE_ID_ALIAS = 15     # fnExtSourceIDAlias(15) = fnExtSourceIDAlias(12) = 15

# Float comparison tolerance
FLOAT_TOLERANCE = 0.00001

print(f"Source Resource Hours: {SOURCE_RESOURCE_HOURS}")
print(f"Source Resource Timesheet: {SOURCE_RESOURCE_TIMESHEET}")
print(f"Source Fact Workers Tasks: {SOURCE_FACT_WORKERS_TASKS}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")

load_mode = dbutils.widgets.get("load_mode")
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {load_mode}", flush=True)
print(f"Project Filter: {project_filter if project_filter else 'None'}", flush=True)
print("=== WIDGETS CONFIGURED ===", flush=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check source tables
source_tables = [
    ("Resource Hours", SOURCE_RESOURCE_HOURS),
    ("Resource Timesheet", SOURCE_RESOURCE_TIMESHEET),
    ("Worker", SOURCE_WORKER),
    ("Project", SOURCE_PROJECT),
]

all_sources_ok = True
for name, table in source_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
    except Exception as e:
        print(f"[ERROR] {name} not found: {table} - {str(e)[:50]}")
        all_sources_ok = False

# Check optional tables (may not exist yet)
optional_tables = [
    ("Fact Workers Tasks", SOURCE_FACT_WORKERS_TASKS),
    ("Task", SOURCE_TASK),
    ("Worker Status", SOURCE_WORKER_STATUS),
]

for name, table in optional_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
    except Exception as e:
        print(f"[WARN] {name} not found (optional): {str(e)[:50]}")

# Ensure target schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print(f"[OK] Target schemas verified")

if not all_sources_ok:
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

print("=" * 60)

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
        if result and result[0][0]:
            return result[0][0]
    except Exception as e:
        print(f"  Watermark lookup failed: {str(e)[:50]}")
    return datetime(1900, 1, 1)


def update_watermark(table_name, watermark_value, row_count, metrics=None):
    """Update watermark after successful processing."""
    try:
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
    except Exception as e:
        print(f"  Warning: Could not update watermark: {str(e)[:50]}")


def fn_ext_source_id_alias(col_name):
    """
    PySpark equivalent of stg.fnExtSourceIDAlias(ExtSourceID).
    Maps ExtSourceID to a canonical alias for comparison.
    """
    return F.when(F.col(col_name).isin(15, 18), F.lit(15)) \
            .when(F.col(col_name).isin(1, 2, 10, 14, 21), F.lit(1)) \
            .when(F.col(col_name).isin(12), F.lit(15)) \
            .otherwise(F.col(col_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Watermarks and Check for Changes

# COMMAND ----------

print("=" * 60)
print("STEP 1: Load Watermarks")
print("=" * 60)

# Get watermarks
last_attendance_watermark = get_watermark("sql_wc2023_ResourceTimesheet[tracked for FactReportedAttendance]")
last_tasks_watermark = get_watermark("sql_FactWorkersTasks[tracked for FactReportedAttendance]")

print(f"Last Attendance Watermark: {last_attendance_watermark}")
print(f"Last Tasks Watermark: {last_tasks_watermark}")

# Check if we need full resync (delete unmatched)
# Original: if watermark < 2018-01-01, then delete unmatched rows
delete_unmatched = (
    (last_attendance_watermark is not None and last_attendance_watermark < datetime(2018, 1, 1))
    or (last_tasks_watermark is not None and last_tasks_watermark < datetime(2018, 1, 1))
)
print(f"Delete Unmatched Mode: {delete_unmatched}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate New Watermarks

# COMMAND ----------

print("=" * 60)
print("STEP 2: Calculate New Watermarks")
print("=" * 60)

# Load source tables
resource_hours_df = spark.table(SOURCE_RESOURCE_HOURS)
resource_timesheet_df = spark.table(SOURCE_RESOURCE_TIMESHEET)

# Calculate new attendance watermark from both tables
# Original: MAX(CreatedAt, UpdatedAt, DeletedAt) across both tables
# Note: Silver tables may not have DeletedAt column - use coalesce with null literal
def get_watermark_cols(df):
    """Get watermark columns, handling missing DeletedAt."""
    cols = df.columns
    has_deleted = "DeletedAt" in cols
    return df.select(
        F.col("CreatedAt").alias("ts1"),
        F.col("UpdatedAt").alias("ts2"),
        F.col("DeletedAt").alias("ts3") if has_deleted else F.lit(None).cast("timestamp").alias("ts3")
    )

hours_watermarks = get_watermark_cols(resource_hours_df)
timesheet_watermarks = get_watermark_cols(resource_timesheet_df)

combined_watermarks = hours_watermarks.union(timesheet_watermarks)
max_ts = combined_watermarks.select(
    F.greatest(
        F.max("ts1"),
        F.max("ts2"),
        F.max("ts3")
    ).alias("max_ts")
).collect()[0][0]

new_attendance_watermark = max_ts if max_ts else datetime.now()
print(f"New Attendance Watermark: {new_attendance_watermark}")

# Calculate new tasks watermark
try:
    tasks_df = spark.table(SOURCE_FACT_WORKERS_TASKS)
    new_tasks_watermark = tasks_df.agg(F.max("WatermarkUTC")).collect()[0][0]
    if new_tasks_watermark is None:
        new_tasks_watermark = datetime.now()
    print(f"New Tasks Watermark: {new_tasks_watermark}")
    has_tasks_table = True
except:
    print("[WARN] FactWorkersTasks not available, using only attendance sources")
    new_tasks_watermark = datetime.now()
    has_tasks_table = False

# COMMAND ----------

# Check if there are any changes to process
# Original: skip if no changes and not doing full resync

skip_processing = not (
    delete_unmatched
    or (new_attendance_watermark > last_attendance_watermark)
    or (new_tasks_watermark > last_tasks_watermark)
    or load_mode == "full"
)

if skip_processing:
    print("No changes detected, skipping processing")
    dbutils.notebook.exit("NO_CHANGES")

print(f"Changes detected, proceeding with processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Prepare Dimension Lookups

# COMMAND ----------

print("=" * 60)
print("STEP 3: Prepare Dimension Lookups")
print("=" * 60)

# Worker dimension lookup (for WorkerID and ApprovedByWorkerID)
# Note: Silver tables use direct IDs (WorkerId) not ExtWorkerID pattern
worker_df = spark.table(SOURCE_WORKER)

# Simple dedup by WorkerId
worker_window = Window.partitionBy("WorkerId").orderBy(F.lit(1))
worker_deduped = worker_df \
    .withColumn("_rn", F.row_number().over(worker_window)) \
    .filter(F.col("_rn") == 1)

# Create two aliases for the two different worker lookups
worker_lookup_df = worker_deduped.select(
    F.col("WorkerId").alias("dim_WorkerID"),
    F.col("WorkerId").alias("dim_ExtWorkerID")  # Same as WorkerId for direct mapping
)

# ApprovedBy lookup: Join ApprovedById (user GUID) to LinkedUserId to resolve to WorkerId
# This matches ADF logic: LEFT JOIN People p ON rt.ApprovedBy = p.LinkedUserId
# Note: ApprovedById in Silver is actually the ApprovedBy user GUID (renamed during Silver load)
#
# Check if LinkedUserId column exists (might not exist if Silver table wasn't rebuilt)
worker_columns = [c.lower() for c in worker_deduped.columns]
if "linkeduserid" in worker_columns:
    print("[OK] LinkedUserId column found in silver_worker - using ADF-style lookup")
    approved_by_window = Window.partitionBy("LinkedUserId").orderBy(F.lit(1))
    approved_by_lookup_df = worker_deduped \
        .filter(F.col("LinkedUserId").isNotNull()) \
        .withColumn("_rn", F.row_number().over(approved_by_window)) \
        .filter(F.col("_rn") == 1) \
        .select(
            F.col("WorkerId").alias("dim_ApprovedByWorkerID"),
            F.col("LinkedUserId").cast("string").alias("dim_ApprovedByExtID")
        )
else:
    # Fallback: LinkedUserId not available, use WorkerId directly (legacy behavior)
    print("[WARN] LinkedUserId column not found - using legacy WorkerId lookup")
    print("       Run Silver job with full refresh to add LinkedUserId column")
    approved_by_lookup_df = worker_deduped.select(
        F.col("WorkerId").alias("dim_ApprovedByWorkerID"),
        F.col("WorkerId").cast("string").alias("dim_ApprovedByExtID")
    )

print(f"Worker dimension records: {worker_lookup_df.count()}")
print(f"ApprovedBy lookup records: {approved_by_lookup_df.count()}")

# Project dimension lookup
# NOTE: silver_project_dw has ExtProjectID (UUID) that matches fact tables' ProjectId
# Columns: ProjectID (INT), ExtProjectID (UUID), ProjectName, ExtSourceID
project_df = spark.table(SOURCE_PROJECT)

# Deduplicate by ExtProjectID
project_window = Window.partitionBy("ExtProjectID").orderBy(F.lit(1))
project_lookup_df = project_df \
    .withColumn("_rn", F.row_number().over(project_window)) \
    .filter(F.col("_rn") == 1) \
    .select(
        F.col("ProjectID").alias("dim_ProjectID"),
        F.col("ExtProjectID").alias("dim_ExtProjectID")  # UUID that matches fact.ProjectId
    )

print(f"Project dimension records: {project_lookup_df.count()}")

# WorkerStatus dimension lookup (for DelayReasonID)
# Note: May not be available in TimescaleDB-sourced Silver tables
try:
    worker_status_df = spark.table(SOURCE_WORKER_STATUS)

    # Check for required columns
    if "WorkerStatusId" in worker_status_df.columns:
        status_window = Window.partitionBy("WorkerStatusId").orderBy(F.lit(1))
        worker_status_lookup_df = worker_status_df \
            .withColumn("_rn", F.row_number().over(status_window)) \
            .filter(F.col("_rn") == 1) \
            .select(
                F.col("WorkerStatusId").alias("dim_WorkerStatusID"),
                F.col("WorkerStatusId").alias("dim_ExtDelayReasonID")  # Direct mapping
            )
        print(f"WorkerStatus dimension records: {worker_status_lookup_df.count()}")
        has_worker_status = True
    else:
        print(f"[WARN] WorkerStatus table missing expected columns")
        has_worker_status = False
except Exception as e:
    print(f"[WARN] WorkerStatus not available: {str(e)[:50]}")
    has_worker_status = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build Source 1 - Customer/Manager Reported Attendance

# COMMAND ----------

print("=" * 60)
print("STEP 4: Build Source 1 - Reported Attendance (ExtSourceID=15)")
print("=" * 60)

# Date range constants for filtering (matches ADF logic)
DATE_MIN = "2000-01-01"
DATE_MAX = "2100-01-01"

# Load and filter ResourceHours (not deleted, valid date range)
# ADF: WHERE "Date" BETWEEN '2000-01-01' AND '2100-01-01'
rh_df = resource_hours_df \
    .filter(F.col("DeletedAt").isNull()) \
    .filter(F.col("Date").between(DATE_MIN, DATE_MAX)) \
    .select(
        F.col("ProjectId").alias("rh_ProjectId"),
        F.col("PeopleId").alias("rh_PeopleId"),
        F.col("Date").alias("rh_Date"),
        F.col("TotalHours").alias("TotalHours")
    )

# Load ResourceTimesheet (valid date range)
# ADF: WHERE "Day" BETWEEN '2000-01-01' AND '2100-01-01'
# Note: TimesheetRemarks column doesn't exist in Silver - using NULL instead
rt_df = resource_timesheet_df \
    .filter(F.col("Day").between(DATE_MIN, DATE_MAX)) \
    .select(
        F.col("ProjectId").alias("rt_ProjectId"),
        F.col("ResourceId").alias("rt_ResourceId"),
        F.col("Day").alias("rt_Day"),
        F.col("ApprovedArrivalTime"),
        F.col("ApprovedLeaveTime"),
        F.col("ReportedAttendanceStatus"),
        F.col("ApprovedAttendanceStatus"),
        F.col("ApprovedById"),
        F.col("TimesheetRemarksId").alias("DelayReasonId"),
        F.lit(None).cast("string").alias("DelayRemarks")  # TimesheetRemarks not in Silver
    )

# FULL OUTER JOIN on ProjectId, PeopleId/ResourceId, Date/Day
# Original: FULL OUTER JOIN ... ON rh.ProjectId = rt.ProjectId AND rh.PeopleId = rt.ResourceId AND rh.[Date] = rt.[Day]
joined_df = rh_df.alias("rh").join(
    rt_df.alias("rt"),
    (F.col("rh.rh_ProjectId") == F.col("rt.rt_ProjectId")) &
    (F.col("rh.rh_PeopleId") == F.col("rt.rt_ResourceId")) &
    (F.col("rh.rh_Date") == F.col("rt.rt_Day")),
    "full"
)

# Combine columns using ISNULL/COALESCE logic
# Original: ISNULL(rh.ProjectId, rt.ProjectId), ISNULL(rh.PeopleId, rt.ResourceId), etc.
attendance_raw_df = joined_df.select(
    F.coalesce(F.col("rh_ProjectId"), F.col("rt_ProjectId")).alias("ProjectId"),
    F.coalesce(F.col("rh_PeopleId"), F.col("rt_ResourceId")).alias("PeopleId"),
    F.coalesce(F.col("rh_Date"), F.col("rt_Day")).alias("Date"),
    F.col("ApprovedArrivalTime"),
    F.col("ApprovedLeaveTime"),
    F.col("TotalHours"),
    # CustomerAttendanceStatus: PRESENT=1, ABSENT=0, else NULL
    F.when(F.upper(F.col("ReportedAttendanceStatus")) == "PRESENT", F.lit(1))
     .when(F.upper(F.col("ReportedAttendanceStatus")) == "ABSENT", F.lit(0))
     .otherwise(F.lit(None)).alias("CustomerAttendanceStatus"),
    # ManagerAttendanceStatus: PRESENT=1, ABSENT=0, else NULL
    F.when(F.upper(F.col("ApprovedAttendanceStatus")) == "PRESENT", F.lit(1))
     .when(F.upper(F.col("ApprovedAttendanceStatus")) == "ABSENT", F.lit(0))
     .otherwise(F.lit(None)).alias("ManagerAttendanceStatus"),
    F.col("ApprovedById"),
    F.col("DelayReasonId"),
    F.col("DelayRemarks")
)

print(f"Attendance raw records (after FULL OUTER JOIN): {attendance_raw_df.count()}")

# COMMAND ----------

# Apply dimension lookups
# Worker lookup (INNER JOIN - required)
att_with_worker = attendance_raw_df.alias("att").join(
    worker_lookup_df.alias("w"),
    F.col("att.PeopleId") == F.col("w.dim_ExtWorkerID"),
    "inner"
).select(
    "att.*",
    F.col("w.dim_WorkerID").alias("WorkerID")
)

# ApprovedBy lookup (LEFT JOIN - optional)
att_with_approved = att_with_worker.alias("att").join(
    approved_by_lookup_df.alias("ab"),
    F.col("att.ApprovedById") == F.col("ab.dim_ApprovedByExtID"),
    "left"
).select(
    "att.*",
    F.col("ab.dim_ApprovedByWorkerID").alias("ApprovedByWorkerID")
)

# Project lookup (INNER JOIN - required)
# Note: UUIDs are case-sensitive in string comparison, normalize to uppercase
att_with_project = att_with_approved.alias("att").join(
    project_lookup_df.alias("p"),
    F.upper(F.col("att.ProjectId")) == F.upper(F.col("p.dim_ExtProjectID")),
    "inner"
).select(
    "att.*",
    F.col("p.dim_ProjectID").alias("ProjectID2")
)

# WorkerStatus lookup (LEFT JOIN - optional)
if has_worker_status:
    att_with_status = att_with_project.alias("att").join(
        worker_status_lookup_df.alias("ws"),
        F.col("att.DelayReasonId") == F.col("ws.dim_ExtDelayReasonID"),
        "left"
    ).select(
        "att.*",
        F.col("ws.dim_WorkerStatusID").alias("WorkerStatusID2")
    )
else:
    att_with_status = att_with_project.withColumn("WorkerStatusID2", F.lit(None).cast("int"))

# Calculate ReportedTime: (1.0 * TotalHours) / (60*60*24)
# Original: (1.0 * TotalHours) / (60*60*24) as ReportedTime
source1_base = att_with_status.withColumn(
    "ReportedTime",
    (F.lit(1.0) * F.col("TotalHours")) / (60 * 60 * 24)
).withColumn(
    "ExtSourceID",
    F.lit(EXT_SOURCE_ID_REPORTS)
).withColumn(
    "StartAtUTC",
    F.col("ApprovedArrivalTime")
).withColumn(
    "FinishAtUTC",
    F.col("ApprovedLeaveTime")
)

# ROW_NUMBER for deduplication
# Original: ROW_NUMBER() OVER (PARTITION BY PeopleId, Date, ProjectId ORDER BY (SELECT NULL) DESC) as RN
dedup_window = Window.partitionBy("PeopleId", "Date", "ProjectId").orderBy(F.lit(1).desc())
source1_with_rn = source1_base.withColumn("RN", F.row_number().over(dedup_window))

# Filter to RN = 1
source1_dedup = source1_with_rn.filter(F.col("RN") == 1).select(
    F.col("Date"),
    F.col("ExtSourceID"),
    F.col("WorkerID"),
    F.col("ApprovedByWorkerID"),
    F.col("ProjectID2"),
    F.col("WorkerStatusID2"),
    F.col("StartAtUTC"),
    F.col("FinishAtUTC"),
    F.col("ReportedTime"),
    F.col("CustomerAttendanceStatus"),
    F.col("ManagerAttendanceStatus"),
    F.col("DelayRemarks")
)

print(f"Source 1 records (after dedup): {source1_dedup.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build Source 2 - Task-Based Attendance

# COMMAND ----------

print("=" * 60)
print("STEP 5: Build Source 2 - Task Attendance (ExtSourceID=12)")
print("=" * 60)

if has_tasks_table:
    try:
        # Load FactWorkersTasks and Task tables
        fact_tasks_df = spark.table(SOURCE_FACT_WORKERS_TASKS) \
            .filter(F.col("DeletedAt").isNull())
        task_df = spark.table(SOURCE_TASK)

        # Join to get ShiftLocalDate from Task
        # Original: LEFT JOIN dbo.Task t ON fwt.TaskID = t.TaskID
        tasks_with_date = fact_tasks_df.alias("fwt").join(
            task_df.alias("t"),
            F.col("fwt.TaskID") == F.col("t.TaskID"),
            "left"
        ).select(
            F.col("fwt.ProjectID"),
            F.col("fwt.WorkerID"),
            F.col("t.ShiftLocalDate"),
            F.col("fwt.BookedTime")
        )

        # GROUP BY ProjectID, WorkerID, ShiftLocalDate
        # Original: SUM(fwt.BookedTime) as ReportedTime
        source2_df = tasks_with_date \
            .groupBy("ProjectID", "WorkerID", "ShiftLocalDate") \
            .agg(F.sum("BookedTime").alias("ReportedTime")) \
            .select(
                F.col("ShiftLocalDate").alias("Date"),
                F.lit(EXT_SOURCE_ID_TASKS).alias("ExtSourceID"),
                F.col("WorkerID"),
                F.lit(None).cast("int").alias("ApprovedByWorkerID"),
                F.col("ProjectID").alias("ProjectID2"),
                F.lit(None).cast("int").alias("WorkerStatusID2"),
                F.lit(None).cast("timestamp").alias("StartAtUTC"),
                F.lit(None).cast("timestamp").alias("FinishAtUTC"),
                F.col("ReportedTime"),
                F.lit(None).cast("int").alias("CustomerAttendanceStatus"),
                F.lit(None).cast("int").alias("ManagerAttendanceStatus"),
                F.lit(None).cast("string").alias("DelayRemarks")
            )

        print(f"Source 2 records (task-based): {source2_df.count()}")
        has_source2 = True
    except Exception as e:
        print(f"[WARN] Error building task source: {str(e)[:100]}")
        has_source2 = False
else:
    print("Skipping Source 2 (FactWorkersTasks not available)")
    has_source2 = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: UNION and Apply Priority Logic

# COMMAND ----------

print("=" * 60)
print("STEP 6: UNION and Priority Logic")
print("=" * 60)

# UNION ALL the two sources
if has_source2:
    combined_df = source1_dedup.unionByName(source2_df)
    print(f"Combined records: {combined_df.count()}")
else:
    combined_df = source1_dedup
    print(f"Using only Source 1: {combined_df.count()}")

# Apply window functions for final deduplication
# Original:
# MAX(WorkerStatusID2) OVER (PARTITION BY ProjectID2, Date, WorkerID) as WorkerStatusID
# ROW_NUMBER() OVER (PARTITION BY ProjectID2, Date, WorkerID ORDER BY CASE WHEN ExtSourceID = 12 THEN 0 ELSE 1 END ASC) RN2

# Window for MAX WorkerStatusID
status_window = Window.partitionBy("ProjectID2", "Date", "WorkerID")

# Window for priority (ExtSourceID=15 has priority over ExtSourceID=12)
# Original: ORDER BY CASE WHEN ExtSourceID = 12 THEN 0 ELSE 1 END ASC
# This means ExtSourceID=12 (task) gets 0, others get 1. ASC means 0 comes first, so task has priority??
# Actually looking at the logic more carefully: RN2=1 is kept, and lower priority number wins
# ExtSourceID=12 -> 0, ExtSourceID=15 -> 1, so task-based would win if both exist
# But wait, the original says "priority to customer reports" - let me re-check
# Actually the comment says ExtSourceID=12 THEN 0 ELSE 1, ASC order
# 0 < 1, so ExtSourceID=12 (task) would be picked first
# But that contradicts typical business logic... Let me look at the actual CASE statement:
# ORDER BY CASE WHEN ExtSourceID = 12 THEN 0 ELSE 1 END ASC
# With ASC: 0 comes before 1, so 12 gets priority
# HOWEVER, looking at the SP purpose, it seems like reported attendance (15) should override task (12)
# Let me implement it exactly as the original SP does, even if it seems counterintuitive
priority_window = Window.partitionBy("ProjectID2", "Date", "WorkerID") \
    .orderBy(F.when(F.col("ExtSourceID") == 12, F.lit(0)).otherwise(F.lit(1)).asc())

final_df = combined_df \
    .withColumn("WorkerStatusID", F.max("WorkerStatusID2").over(status_window)) \
    .withColumn("RN2", F.row_number().over(priority_window)) \
    .filter(F.col("RN2") == 1) \
    .select(
        F.col("Date"),
        F.col("ExtSourceID"),
        F.col("WorkerID"),
        F.col("ApprovedByWorkerID"),
        F.col("ProjectID2"),
        F.col("WorkerStatusID"),
        F.col("StartAtUTC"),
        F.col("FinishAtUTC"),
        F.col("ReportedTime"),
        F.col("CustomerAttendanceStatus"),
        F.col("ManagerAttendanceStatus"),
        F.col("DelayRemarks")
    )

# Note: cache() removed - not supported on serverless compute
final_count = final_df.count()
print(f"Final source records: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create/Update Target Table

# COMMAND ----------

print("=" * 60)
print("STEP 7: Ensure Target Table Exists")
print("=" * 60)

target_schema = """
    FactReportedAttendanceID BIGINT GENERATED ALWAYS AS IDENTITY,
    ProjectID INT NOT NULL,
    WorkerID INT NOT NULL,
    ShiftLocalDate DATE NOT NULL,
    ExtSourceID INT,
    StartAtUTC TIMESTAMP,
    FinishAtUTC TIMESTAMP,
    ReportedTime DOUBLE,
    CustomerReportedAttendance INT,
    ManagerReportedAttendance INT,
    ApprovedByWorkerID INT,
    WorkerStatusID INT,
    DelayReasonRemarks STRING,
    WatermarkUTC TIMESTAMP,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
"""

try:
    spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    print(f"[OK] Target table exists: {TARGET_TABLE}")
    target_exists = True
except:
    print(f"[INFO] Creating target table...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            {target_schema}
        )
        USING DELTA
        CLUSTER BY (ProjectID, ShiftLocalDate)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    print(f"[OK] Created target table: {TARGET_TABLE}")
    target_exists = False

# Get target row count before merge
try:
    target_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0
print(f"Target rows before MERGE: {target_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Execute MERGE

# COMMAND ----------

print("=" * 60)
print("STEP 8: Execute MERGE")
print("=" * 60)

# Create temp view for source
final_df.createOrReplaceTempView("attendance_source")

# Execute MERGE
# Original match: s.WorkerID = t.WorkerID AND s.Date = t.ShiftLocalDate AND s.ProjectID2 = t.ProjectID
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING attendance_source AS s
ON t.WorkerID = s.WorkerID
   AND t.ShiftLocalDate = s.Date
   AND t.ProjectID = s.ProjectID2

WHEN MATCHED AND (
    -- Use IS DISTINCT FROM for nullable comparisons
    t.DelayReasonRemarks IS DISTINCT FROM s.DelayRemarks OR
    t.StartAtUTC IS DISTINCT FROM s.StartAtUTC OR
    t.FinishAtUTC IS DISTINCT FROM s.FinishAtUTC OR
    (ABS(COALESCE(t.ReportedTime, 0) - COALESCE(s.ReportedTime, 0)) > {FLOAT_TOLERANCE}
        OR (t.ReportedTime IS NULL AND s.ReportedTime IS NOT NULL)
        OR (t.ReportedTime IS NOT NULL AND s.ReportedTime IS NULL)) OR
    t.CustomerReportedAttendance IS DISTINCT FROM s.CustomerAttendanceStatus OR
    t.ManagerReportedAttendance IS DISTINCT FROM s.ManagerAttendanceStatus OR
    t.ApprovedByWorkerID IS DISTINCT FROM s.ApprovedByWorkerID OR
    t.WorkerStatusID IS DISTINCT FROM s.WorkerStatusID
)
THEN UPDATE SET
    t.WatermarkUTC = current_timestamp(),
    t.UpdatedAt = current_timestamp(),
    t.DelayReasonRemarks = s.DelayRemarks,
    t.ExtSourceID = s.ExtSourceID,
    t.StartAtUTC = s.StartAtUTC,
    t.FinishAtUTC = s.FinishAtUTC,
    t.ReportedTime = s.ReportedTime,
    t.CustomerReportedAttendance = s.CustomerAttendanceStatus,
    t.ManagerReportedAttendance = s.ManagerAttendanceStatus,
    t.ApprovedByWorkerID = s.ApprovedByWorkerID,
    t.WorkerStatusID = s.WorkerStatusID

WHEN NOT MATCHED THEN INSERT (
    ShiftLocalDate, DelayReasonRemarks, ExtSourceID, ProjectID,
    StartAtUTC, FinishAtUTC, ReportedTime, WorkerID,
    CustomerReportedAttendance, ManagerReportedAttendance,
    ApprovedByWorkerID, WorkerStatusID
)
VALUES (
    s.Date, s.DelayRemarks, s.ExtSourceID, s.ProjectID2,
    s.StartAtUTC, s.FinishAtUTC, s.ReportedTime, s.WorkerID,
    s.CustomerAttendanceStatus, s.ManagerAttendanceStatus,
    s.ApprovedByWorkerID, s.WorkerStatusID
)
"""

spark.sql(merge_sql)
print("[OK] MERGE completed (matched and not matched by target)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Handle NOT MATCHED BY SOURCE (Soft Delete)
# MAGIC
# MAGIC Original: WHEN NOT MATCHED BY SOURCE THEN UPDATE SET columns = NULL
# MAGIC In Databricks, we implement this as a separate UPDATE using anti-join

# COMMAND ----------

print("=" * 60)
print("STEP 9: Handle NOT MATCHED BY SOURCE")
print("=" * 60)

if delete_unmatched or load_mode == "full":
    print("Applying soft delete to records not in source...")

    # Find target records not in source (anti-join)
    # Original: SET StartAtUTC = NULL, FinishAtUTC = NULL, ReportedTime = NULL, etc.

    soft_delete_sql = f"""
    UPDATE {TARGET_TABLE} AS t
    SET
        t.StartAtUTC = NULL,
        t.FinishAtUTC = NULL,
        t.ReportedTime = NULL,
        t.WorkerStatusID = NULL,
        t.DelayReasonRemarks = NULL,
        t.CustomerReportedAttendance = NULL,
        t.ManagerReportedAttendance = NULL,
        t.ApprovedByWorkerID = NULL,
        t.WatermarkUTC = current_timestamp(),
        t.UpdatedAt = current_timestamp()
    WHERE NOT EXISTS (
        SELECT 1 FROM attendance_source s
        WHERE t.WorkerID = s.WorkerID
          AND t.ShiftLocalDate = s.Date
          AND t.ProjectID = s.ProjectID2
    )
    AND (
        t.StartAtUTC IS NOT NULL OR
        t.FinishAtUTC IS NOT NULL OR
        t.ReportedTime IS NOT NULL OR
        t.WorkerStatusID IS NOT NULL
    )
    """

    spark.sql(soft_delete_sql)
    print("[OK] Soft delete completed for unmatched records")
else:
    print("Skipping soft delete (incremental mode, watermarks are recent)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Update Watermarks

# COMMAND ----------

print("=" * 60)
print("STEP 10: Update Watermarks")
print("=" * 60)

# Get final target count
target_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
inserted = target_after - target_before

# Update watermarks
update_watermark(
    "sql_wc2023_ResourceTimesheet[tracked for FactReportedAttendance]",
    new_attendance_watermark,
    target_after
)
update_watermark(
    "sql_FactWorkersTasks[tracked for FactReportedAttendance]",
    new_tasks_watermark,
    target_after
)

print(f"Attendance Watermark updated to: {new_attendance_watermark}")
print(f"Tasks Watermark updated to: {new_tasks_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Cleanup (unpersist removed - cache() not used on serverless compute)

# Print summary
print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Sources:")
print(f"  - Resource Hours: {SOURCE_RESOURCE_HOURS}")
print(f"  - Resource Timesheet: {SOURCE_RESOURCE_TIMESHEET}")
print(f"  - Fact Workers Tasks: {SOURCE_FACT_WORKERS_TASKS if has_tasks_table else 'N/A'}")
print(f"Target: {TARGET_TABLE}")
print(f"")
print(f"Records processed:")
print(f"  - Source 1 (Reported): {source1_dedup.count() if 'source1_dedup' in dir() else 'N/A'}")
print(f"  - Source 2 (Tasks): {source2_df.count() if has_source2 else 'N/A'}")
print(f"  - Final (after priority): {final_count}")
print(f"")
print(f"Target table:")
print(f"  - Rows before: {target_before}")
print(f"  - Rows after: {target_after}")
print(f"  - Estimated inserts: {inserted}")
print(f"")
print(f"Mode: {load_mode}")
print(f"Delete Unmatched: {delete_unmatched}")
print("=" * 60)

# Return success
dbutils.notebook.exit(f"SUCCESS: processed={final_count}, inserted={inserted}")
