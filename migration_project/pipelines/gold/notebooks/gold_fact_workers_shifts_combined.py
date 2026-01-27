# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Workers Shifts Combined
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateFactWorkersShiftsCombined` (1287 lines)
# MAGIC
# MAGIC **Purpose:** Combine shift metrics from FactWorkersShifts with reported attendance
# MAGIC data from FactReportedAttendance to create a unified view of worker shifts with
# MAGIC both calculated and reported metrics.
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - TEMP_TABLE, MERGE, WINDOW_FUNCTION patterns
# MAGIC - Combines data from FactWorkersShifts + FactReportedAttendance
# MAGIC - Joins with dimension tables (Worker, Crew, Trade, Workshift)
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.gold.gold_fact_workers_shifts` (calculated shift metrics)
# MAGIC - `wakecap_prod.gold.gold_fact_reported_attendance` (reported attendance)
# MAGIC - `wakecap_prod.silver.silver_crew_composition` (crew assignments)
# MAGIC - `wakecap_prod.silver.silver_trade_assignment` (trade assignments)
# MAGIC - `wakecap_prod.silver.silver_workshift_resource_assignment` (workshift assignments)
# MAGIC - `wakecap_prod.silver.silver_resource_device` (inventory assignments)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_workers_shifts_combined`
# MAGIC **Watermarks:** `wakecap_prod.migration._gold_watermarks`

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
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables
SOURCE_WORKERS_SHIFTS = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_workers_shifts"
SOURCE_REPORTED_ATTENDANCE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_reported_attendance"
SOURCE_CREW_COMPOSITION = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"
SOURCE_TRADE_ASSIGNMENT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_trade_assignment"
SOURCE_WORKSHIFT_ASSIGNMENT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_workshift_resource_assignment"
SOURCE_DEVICE_ASSIGNMENT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_resource_device"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_workers_shifts_combined"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# Float comparison tolerance
FLOAT_TOLERANCE = 0.00001

print(f"Source Workers Shifts: {SOURCE_WORKERS_SHIFTS}")
print(f"Source Reported Attendance: {SOURCE_REPORTED_ATTENDANCE}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "14", "Lookback Days")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")

load_mode = dbutils.widgets.get("load_mode")
lookback_days = int(dbutils.widgets.get("lookback_days"))
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {load_mode}")
print(f"Lookback Days: {lookback_days}")
print(f"Project Filter: {project_filter if project_filter else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check required source tables
source_tables = [
    ("Workers Shifts", SOURCE_WORKERS_SHIFTS),
    ("Reported Attendance", SOURCE_REPORTED_ATTENDANCE),
]

all_sources_ok = True
for name, table in source_tables:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
        print(f"[OK] {name} exists: {table} ({cnt:,} rows)")
    except Exception as e:
        print(f"[ERROR] {name} not found: {table} - {str(e)[:50]}")
        all_sources_ok = False

# Check optional dimension tables
optional_tables = [
    ("Crew Composition", SOURCE_CREW_COMPOSITION),
    ("Trade Assignment", SOURCE_TRADE_ASSIGNMENT),
    ("Workshift Assignment", SOURCE_WORKSHIFT_ASSIGNMENT),
    ("Device Assignment", SOURCE_DEVICE_ASSIGNMENT),
]

optional_table_status = {}
for name, table in optional_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
        optional_table_status[name] = True
    except Exception as e:
        print(f"[WARN] {name} not found (optional): {str(e)[:50]}")
        optional_table_status[name] = False

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Source Data with Date Range

# COMMAND ----------

print("=" * 60)
print("STEP 1: Load Source Data")
print("=" * 60)

# Determine date range for processing
if load_mode == "full":
    min_date = datetime(2020, 1, 1).date()
    max_date = datetime.now().date()
else:
    # Get last watermark
    last_watermark = get_watermark("gold_fact_workers_shifts_combined")
    print(f"Last watermark: {last_watermark}")

    # Process from lookback_days ago
    max_date = datetime.now().date()
    min_date = max_date - timedelta(days=lookback_days)

print(f"Processing date range: {min_date} to {max_date}")

# Load workers shifts with date filter
shifts_df = spark.table(SOURCE_WORKERS_SHIFTS) \
    .filter(F.col("ShiftLocalDate").between(min_date, max_date))

if project_filter:
    shifts_df = shifts_df.filter(F.col("ProjectId") == int(project_filter))

shifts_count = shifts_df.count()
print(f"Workers Shifts records in range: {shifts_count:,}")

if shifts_count == 0:
    print("[WARN] No records to process")
    dbutils.notebook.exit("NO_RECORDS_TO_PROCESS")

# Load reported attendance with date filter
attendance_df = spark.table(SOURCE_REPORTED_ATTENDANCE) \
    .filter(F.col("ShiftLocalDate").between(min_date, max_date))

if project_filter:
    attendance_df = attendance_df.filter(F.col("ProjectID") == int(project_filter))

attendance_count = attendance_df.count()
print(f"Reported Attendance records in range: {attendance_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare Dimension Lookups for Assignments

# COMMAND ----------

print("=" * 60)
print("STEP 2: Prepare Assignment Lookups")
print("=" * 60)

# Crew Assignment lookup - uses CreatedAt/DeletedAt not ValidFrom/ValidTo
if optional_table_status.get("Crew Composition"):
    crew_assign_df = spark.table(SOURCE_CREW_COMPOSITION) \
        .select(
            F.col("WorkerId").alias("ca_WorkerId"),
            F.col("ProjectId").alias("ca_ProjectId"),
            F.col("CrewId").alias("CrewID"),
            F.col("CreatedAt").alias("ca_ValidFrom"),
            F.col("DeletedAt").alias("ca_ValidTo")
        )
    print(f"Crew assignments loaded: {crew_assign_df.count():,} records")
else:
    crew_assign_df = None
    print("[SKIP] Crew assignments not available")

# Trade Assignment - NOTE: silver_trade_assignment doesn't exist
# TradeId comes from crew_composition or worker table directly
trade_assign_df = None
print("[SKIP] Trade assignments not available (table doesn't exist)")

# Workshift Assignment lookup - uses EffectiveDate not ValidFrom/ValidTo
if optional_table_status.get("Workshift Assignment"):
    workshift_assign_df = spark.table(SOURCE_WORKSHIFT_ASSIGNMENT) \
        .select(
            F.col("WorkerId").alias("wa_WorkerId"),
            F.col("ProjectId").alias("wa_ProjectId"),
            F.col("WorkshiftId").alias("WorkshiftID"),
            F.col("EffectiveDate").alias("wa_ValidFrom")
        )
    # No ValidTo in this table - use window to get applicable assignment
    print(f"Workshift assignments loaded: {workshift_assign_df.count():,} records")
else:
    workshift_assign_df = None
    print("[SKIP] Workshift assignments not available")

# Device/Inventory Assignment lookup - uses AssignedAt/UnassignedAt
if optional_table_status.get("Device Assignment"):
    device_assign_df = spark.table(SOURCE_DEVICE_ASSIGNMENT) \
        .select(
            F.col("WorkerId").alias("da_WorkerId"),
            F.col("AssignedAt").alias("InventoryAssignedOn"),
            F.col("UnassignedAt").alias("InventoryUnassignedOn")
        )
    print(f"Device assignments loaded: {device_assign_df.count():,} records")
else:
    device_assign_df = None
    print("[SKIP] Device assignments not available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Join Shifts with Reported Attendance

# COMMAND ----------

print("=" * 60)
print("STEP 3: Join Shifts with Reported Attendance")
print("=" * 60)

# Prepare attendance data for join
# Select only the columns we need from reported attendance
attendance_for_join = attendance_df.select(
    F.col("ProjectID").alias("att_ProjectID"),
    F.col("WorkerID").alias("att_WorkerID"),
    F.col("ShiftLocalDate").alias("att_ShiftLocalDate"),
    F.col("ReportedTime"),
    F.col("CustomerReportedAttendance"),
    F.col("ManagerReportedAttendance"),
    F.col("StartAtUTC").alias("att_StartAtUTC"),
    F.col("FinishAtUTC").alias("att_FinishAtUTC"),
    F.col("ApprovedByWorkerID"),
    F.col("WorkerStatusID").alias("DelayReasonID"),
    F.col("DelayReasonRemarks")
)

# LEFT OUTER JOIN shifts with attendance
# Match on ProjectId, WorkerId, ShiftLocalDate
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
combined_df = shifts_df.alias("s").join(
    attendance_for_join.alias("a"),
    (F.col("s.ProjectId").cast("string") == F.col("a.att_ProjectID").cast("string")) &
    (F.col("s.WorkerId").cast("string") == F.col("a.att_WorkerID").cast("string")) &
    (F.col("s.ShiftLocalDate") == F.col("a.att_ShiftLocalDate")),
    "left"
)

print(f"Combined records after attendance join: {combined_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Assignment Dimensions

# COMMAND ----------

print("=" * 60)
print("STEP 4: Add Assignment Dimensions")
print("=" * 60)

# Join with Crew Assignment (valid on ShiftLocalDate using CreatedAt/DeletedAt)
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
if crew_assign_df is not None:
    combined_df = combined_df.alias("c").join(
        crew_assign_df.alias("ca"),
        (F.col("c.ProjectId").cast("string") == F.col("ca.ca_ProjectId").cast("string")) &
        (F.col("c.WorkerId").cast("string") == F.col("ca.ca_WorkerId").cast("string")) &
        (F.col("c.ShiftLocalDate") >= F.col("ca.ca_ValidFrom")) &
        (
            F.col("ca.ca_ValidTo").isNull() |
            (F.col("c.ShiftLocalDate") < F.col("ca.ca_ValidTo"))
        ),
        "left"
    ).drop("ca_WorkerId", "ca_ProjectId", "ca_ValidFrom", "ca_ValidTo")
    print("[OK] Crew assignment joined")
else:
    combined_df = combined_df.withColumn("CrewID", F.lit(None).cast("int"))
    print("[SKIP] Crew assignment skipped")

# Trade Assignment - table doesn't exist, add NULL column
combined_df = combined_df.withColumn("TradeID", F.lit(None).cast("int"))
print("[SKIP] Trade assignment skipped (table doesn't exist)")

# Join with Workshift Assignment (valid on ShiftLocalDate using EffectiveDate)
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
if workshift_assign_df is not None:
    # Use window to get the latest assignment before or on shift date
    wa_window = Window.partitionBy("wa_ProjectId", "wa_WorkerId").orderBy(F.col("wa_ValidFrom").desc())
    workshift_ranked = workshift_assign_df.withColumn("_wa_rn", F.row_number().over(wa_window))

    combined_df = combined_df.alias("c").join(
        workshift_ranked.alias("wa"),
        (F.col("c.ProjectId").cast("string") == F.col("wa.wa_ProjectId").cast("string")) &
        (F.col("c.WorkerId").cast("string") == F.col("wa.wa_WorkerId").cast("string")) &
        (F.col("c.ShiftLocalDate") >= F.col("wa.wa_ValidFrom")) &
        (F.col("wa._wa_rn") == 1),
        "left"
    ).drop("wa_WorkerId", "wa_ProjectId", "wa_ValidFrom", "_wa_rn")
    print("[OK] Workshift assignment joined")
else:
    combined_df = combined_df.withColumn("WorkshiftID", F.lit(None).cast("int"))
    print("[SKIP] Workshift assignment skipped")

# Join with Device Assignment to get inventory dates
# Note: silver_resource_device has WorkerId but no ProjectId
# Cast IDs to STRING to avoid UUID vs BIGINT type mismatch
if device_assign_df is not None:
    # Get device assignment - partition by worker only
    device_window = Window.partitionBy("da_WorkerId") \
        .orderBy(F.col("InventoryAssignedOn").desc())

    device_with_rank = device_assign_df \
        .withColumn("_rn", F.row_number().over(device_window))

    # Get the latest assignment per worker
    combined_df = combined_df.alias("c").join(
        device_with_rank.filter(F.col("_rn") == 1).alias("da"),
        F.col("c.WorkerId").cast("string") == F.col("da.da_WorkerId").cast("string"),
        "left"
    ).drop("da_WorkerId", "_rn")
    print("[OK] Device assignment joined")
else:
    combined_df = combined_df.withColumn("InventoryAssignedOn", F.lit(None).cast("timestamp"))
    combined_df = combined_df.withColumn("InventoryUnassignedOn", F.lit(None).cast("timestamp"))
    print("[SKIP] Device assignment skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Apply ROW_NUMBER for Deduplication

# COMMAND ----------

print("=" * 60)
print("STEP 5: Deduplicate Records")
print("=" * 60)

# If there are multiple assignments for same worker/project/date, keep one
# Use ROW_NUMBER to pick one record per ProjectId, WorkerId, ShiftLocalDate
dedup_window = Window.partitionBy("ProjectId", "WorkerId", "ShiftLocalDate") \
    .orderBy(F.col("StartAtUTC").asc_nulls_last())

deduped_df = combined_df \
    .withColumn("_rn", F.row_number().over(dedup_window)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn")

# Clean up attendance columns that were prefixed for join
final_df = deduped_df \
    .drop("att_ProjectID", "att_WorkerID", "att_ShiftLocalDate", "att_StartAtUTC", "att_FinishAtUTC")

print(f"Records after deduplication: {final_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Add Segment and Watermark Columns

# COMMAND ----------

print("=" * 60)
print("STEP 6: Add Metadata Columns")
print("=" * 60)

# Add Segment column (always 1 for now - could be extended for multi-segment shifts)
# Add WatermarkUTC for incremental tracking
final_df = final_df \
    .withColumn("Segment", F.lit(1)) \
    .withColumn("WatermarkUTC", F.current_timestamp())

# Select and order columns to match target schema
final_columns = [
    # Key columns
    "ProjectId", "WorkerId", "ShiftLocalDate",
    # Assignment dimensions
    "CrewID", "TradeID", "WorkshiftID", "WorkshiftDetailsID", "Segment",
    # Shift timing
    "StartAtUTC", "FinishAtUTC",
    "FirstDirectProductiveUTC", "LastDirectProductiveUTC",
    "FirstAssignedLocationUTC", "LastAssignedLocationUTC",
    # Active time metrics
    "ActiveTime", "ActiveTimeBeforeShift", "ActiveTimeDuringShift", "ActiveTimeAfterShift",
    # Inactive time metrics
    "InactiveTime", "InactiveTimeDuringShift", "InactiveTimeBeforeShift", "InactiveTimeAfterShift",
    # Reading counts
    "Readings", "ReadingsMissedActive", "ReadingsMissedActiveDuringShift",
    "ReadingsMissedInactive", "ReadingsMissedInactiveDuringShift",
    # Direct productive time
    "ActiveTimeInDirectProductive", "ActiveTimeDuringShiftInDirectProductive",
    "ActiveTimeBeforeShiftInDirectProductive", "ActiveTimeAfterShiftInDirectProductive",
    # Indirect productive time
    "ActiveTimeInIndirectProductive", "ActiveTimeDuringShiftInIndirectProductive",
    "ActiveTimeBeforeShiftInIndirectProductive", "ActiveTimeAfterShiftInIndirectProductive",
    # Assigned location active time
    "ActiveTimeInAssignedLocation", "ActiveTimeDuringShiftInAssignedLocation",
    "ActiveTimeBeforeShiftInAssignedLocation", "ActiveTimeAfterShiftInAssignedLocation",
    # Direct productive inactive time
    "InactiveTimeInDirectProductive", "InactiveTimeDuringShiftInDirectProductive",
    "InactiveTimeBeforeShiftInDirectProductive", "InactiveTimeAfterShiftInDirectProductive",
    # Indirect productive inactive time
    "InactiveTimeInIndirectProductive", "InactiveTimeDuringShiftInIndirectProductive",
    "InactiveTimeBeforeShiftInIndirectProductive", "InactiveTimeAfterShiftInIndirectProductive",
    # Assigned location inactive time
    "InactiveTimeInAssignedLocation", "InactiveTimeDuringShiftInAssignedLocation",
    "InactiveTimeBeforeShiftInAssignedLocation", "InactiveTimeAfterShiftInAssignedLocation",
    # Reported attendance columns
    "ReportedTime",
    # Inventory columns
    "InventoryAssignedOn", "InventoryUnassignedOn",
    # Metadata
    "WatermarkUTC"
]

# Select only columns that exist
existing_cols = set(final_df.columns)
select_cols = [c for c in final_columns if c in existing_cols]

# Add missing columns as NULL
for col in final_columns:
    if col not in existing_cols:
        final_df = final_df.withColumn(col, F.lit(None))

final_df = final_df.select(*final_columns)
# Note: cache() removed - not supported on serverless compute

print(f"Final record count: {final_df.count():,}")
print(f"Columns: {len(final_columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create/Update Target Table

# COMMAND ----------

print("=" * 60)
print("STEP 7: Ensure Target Table Exists")
print("=" * 60)

# Target table schema
# Note: ID columns use STRING to support UUID values from TimescaleDB
target_schema = """
    FactShiftCombinedID BIGINT GENERATED ALWAYS AS IDENTITY,
    ProjectID STRING NOT NULL,
    WorkerID STRING NOT NULL,
    ShiftLocalDate DATE NOT NULL,
    CrewID STRING,
    TradeID STRING,
    WorkshiftID STRING,
    WorkshiftDetailsID STRING,
    Segment INT,
    StartAtUTC TIMESTAMP,
    FinishAtUTC TIMESTAMP,
    FirstDirectProductiveUTC TIMESTAMP,
    LastDirectProductiveUTC TIMESTAMP,
    FirstAssignedLocationUTC TIMESTAMP,
    LastAssignedLocationUTC TIMESTAMP,
    ActiveTime DOUBLE,
    ActiveTimeBeforeShift DOUBLE,
    ActiveTimeDuringShift DOUBLE,
    ActiveTimeAfterShift DOUBLE,
    InactiveTime DOUBLE,
    InactiveTimeDuringShift DOUBLE,
    InactiveTimeBeforeShift DOUBLE,
    InactiveTimeAfterShift DOUBLE,
    Readings INT,
    ReadingsMissedActive INT,
    ReadingsMissedActiveDuringShift INT,
    ReadingsMissedInactive INT,
    ReadingsMissedInactiveDuringShift INT,
    ActiveTimeInDirectProductive DOUBLE,
    ActiveTimeDuringShiftInDirectProductive DOUBLE,
    ActiveTimeBeforeShiftInDirectProductive DOUBLE,
    ActiveTimeAfterShiftInDirectProductive DOUBLE,
    ActiveTimeInIndirectProductive DOUBLE,
    ActiveTimeDuringShiftInIndirectProductive DOUBLE,
    ActiveTimeBeforeShiftInIndirectProductive DOUBLE,
    ActiveTimeAfterShiftInIndirectProductive DOUBLE,
    ActiveTimeInAssignedLocation DOUBLE,
    ActiveTimeDuringShiftInAssignedLocation DOUBLE,
    ActiveTimeBeforeShiftInAssignedLocation DOUBLE,
    ActiveTimeAfterShiftInAssignedLocation DOUBLE,
    InactiveTimeInDirectProductive DOUBLE,
    InactiveTimeDuringShiftInDirectProductive DOUBLE,
    InactiveTimeBeforeShiftInDirectProductive DOUBLE,
    InactiveTimeAfterShiftInDirectProductive DOUBLE,
    InactiveTimeInIndirectProductive DOUBLE,
    InactiveTimeDuringShiftInIndirectProductive DOUBLE,
    InactiveTimeBeforeShiftInIndirectProductive DOUBLE,
    InactiveTimeAfterShiftInIndirectProductive DOUBLE,
    InactiveTimeInAssignedLocation DOUBLE,
    InactiveTimeDuringShiftInAssignedLocation DOUBLE,
    InactiveTimeBeforeShiftInAssignedLocation DOUBLE,
    InactiveTimeAfterShiftInAssignedLocation DOUBLE,
    ReportedTime DOUBLE,
    InventoryAssignedOn TIMESTAMP,
    InventoryUnassignedOn TIMESTAMP,
    WatermarkUTC TIMESTAMP,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
"""

# Check if table exists and has correct schema (STRING IDs)
table_needs_recreate = False
try:
    schema_df = spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    project_id_type = schema_df.filter("col_name = 'ProjectID'").select("data_type").collect()
    if project_id_type and project_id_type[0][0].upper() == "INT":
        print(f"[WARN] Table exists with INT schema, need to recreate with STRING")
        table_needs_recreate = True
    else:
        print(f"[OK] Target table exists with correct schema: {TARGET_TABLE}")
        target_exists = True
except:
    table_needs_recreate = True

if table_needs_recreate:
    print(f"[INFO] Dropping and recreating target table with STRING IDs...")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    spark.sql(f"""
        CREATE TABLE {TARGET_TABLE} (
            {target_schema}
        )
        USING DELTA
        CLUSTER BY (ProjectID, ShiftLocalDate)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    print(f"[OK] Created target table with STRING IDs: {TARGET_TABLE}")
    target_exists = False

# Get target row count before merge
try:
    target_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0
print(f"Target rows before MERGE: {target_before:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Execute MERGE

# COMMAND ----------

print("=" * 60)
print("STEP 8: Execute MERGE")
print("=" * 60)

# Create temp view for source
final_df.createOrReplaceTempView("shifts_combined_source")

# Build MERGE statement
# Match on ProjectId, WorkerId, ShiftLocalDate
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING shifts_combined_source AS s
ON t.ProjectID = s.ProjectId
   AND t.WorkerID = s.WorkerId
   AND t.ShiftLocalDate = s.ShiftLocalDate

WHEN MATCHED AND (
    -- Check for changes in key metrics using float tolerance
    (ABS(COALESCE(t.ActiveTime, 0) - COALESCE(s.ActiveTime, 0)) > {FLOAT_TOLERANCE}) OR
    (ABS(COALESCE(t.InactiveTime, 0) - COALESCE(s.InactiveTime, 0)) > {FLOAT_TOLERANCE}) OR
    (ABS(COALESCE(t.ReportedTime, 0) - COALESCE(s.ReportedTime, 0)) > {FLOAT_TOLERANCE}) OR
    t.StartAtUTC IS DISTINCT FROM s.StartAtUTC OR
    t.FinishAtUTC IS DISTINCT FROM s.FinishAtUTC OR
    t.CrewID IS DISTINCT FROM s.CrewID OR
    t.TradeID IS DISTINCT FROM s.TradeID OR
    t.WorkshiftID IS DISTINCT FROM s.WorkshiftID OR
    t.Readings IS DISTINCT FROM s.Readings
)
THEN UPDATE SET
    t.CrewID = s.CrewID,
    t.TradeID = s.TradeID,
    t.WorkshiftID = s.WorkshiftID,
    t.WorkshiftDetailsID = s.WorkshiftDetailsID,
    t.Segment = s.Segment,
    t.StartAtUTC = s.StartAtUTC,
    t.FinishAtUTC = s.FinishAtUTC,
    t.FirstDirectProductiveUTC = s.FirstDirectProductiveUTC,
    t.LastDirectProductiveUTC = s.LastDirectProductiveUTC,
    t.FirstAssignedLocationUTC = s.FirstAssignedLocationUTC,
    t.LastAssignedLocationUTC = s.LastAssignedLocationUTC,
    t.ActiveTime = s.ActiveTime,
    t.ActiveTimeBeforeShift = s.ActiveTimeBeforeShift,
    t.ActiveTimeDuringShift = s.ActiveTimeDuringShift,
    t.ActiveTimeAfterShift = s.ActiveTimeAfterShift,
    t.InactiveTime = s.InactiveTime,
    t.InactiveTimeDuringShift = s.InactiveTimeDuringShift,
    t.InactiveTimeBeforeShift = s.InactiveTimeBeforeShift,
    t.InactiveTimeAfterShift = s.InactiveTimeAfterShift,
    t.Readings = s.Readings,
    t.ReadingsMissedActive = s.ReadingsMissedActive,
    t.ReadingsMissedActiveDuringShift = s.ReadingsMissedActiveDuringShift,
    t.ReadingsMissedInactive = s.ReadingsMissedInactive,
    t.ReadingsMissedInactiveDuringShift = s.ReadingsMissedInactiveDuringShift,
    t.ActiveTimeInDirectProductive = s.ActiveTimeInDirectProductive,
    t.ActiveTimeDuringShiftInDirectProductive = s.ActiveTimeDuringShiftInDirectProductive,
    t.ActiveTimeBeforeShiftInDirectProductive = s.ActiveTimeBeforeShiftInDirectProductive,
    t.ActiveTimeAfterShiftInDirectProductive = s.ActiveTimeAfterShiftInDirectProductive,
    t.ActiveTimeInIndirectProductive = s.ActiveTimeInIndirectProductive,
    t.ActiveTimeDuringShiftInIndirectProductive = s.ActiveTimeDuringShiftInIndirectProductive,
    t.ActiveTimeBeforeShiftInIndirectProductive = s.ActiveTimeBeforeShiftInIndirectProductive,
    t.ActiveTimeAfterShiftInIndirectProductive = s.ActiveTimeAfterShiftInIndirectProductive,
    t.ActiveTimeInAssignedLocation = s.ActiveTimeInAssignedLocation,
    t.ActiveTimeDuringShiftInAssignedLocation = s.ActiveTimeDuringShiftInAssignedLocation,
    t.ActiveTimeBeforeShiftInAssignedLocation = s.ActiveTimeBeforeShiftInAssignedLocation,
    t.ActiveTimeAfterShiftInAssignedLocation = s.ActiveTimeAfterShiftInAssignedLocation,
    t.InactiveTimeInDirectProductive = s.InactiveTimeInDirectProductive,
    t.InactiveTimeDuringShiftInDirectProductive = s.InactiveTimeDuringShiftInDirectProductive,
    t.InactiveTimeBeforeShiftInDirectProductive = s.InactiveTimeBeforeShiftInDirectProductive,
    t.InactiveTimeAfterShiftInDirectProductive = s.InactiveTimeAfterShiftInDirectProductive,
    t.InactiveTimeInIndirectProductive = s.InactiveTimeInIndirectProductive,
    t.InactiveTimeDuringShiftInIndirectProductive = s.InactiveTimeDuringShiftInIndirectProductive,
    t.InactiveTimeBeforeShiftInIndirectProductive = s.InactiveTimeBeforeShiftInIndirectProductive,
    t.InactiveTimeAfterShiftInIndirectProductive = s.InactiveTimeAfterShiftInIndirectProductive,
    t.InactiveTimeInAssignedLocation = s.InactiveTimeInAssignedLocation,
    t.InactiveTimeDuringShiftInAssignedLocation = s.InactiveTimeDuringShiftInAssignedLocation,
    t.InactiveTimeBeforeShiftInAssignedLocation = s.InactiveTimeBeforeShiftInAssignedLocation,
    t.InactiveTimeAfterShiftInAssignedLocation = s.InactiveTimeAfterShiftInAssignedLocation,
    t.ReportedTime = s.ReportedTime,
    t.InventoryAssignedOn = s.InventoryAssignedOn,
    t.InventoryUnassignedOn = s.InventoryUnassignedOn,
    t.WatermarkUTC = current_timestamp(),
    t.UpdatedAt = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
    ProjectID, WorkerID, ShiftLocalDate,
    CrewID, TradeID, WorkshiftID, WorkshiftDetailsID, Segment,
    StartAtUTC, FinishAtUTC,
    FirstDirectProductiveUTC, LastDirectProductiveUTC,
    FirstAssignedLocationUTC, LastAssignedLocationUTC,
    ActiveTime, ActiveTimeBeforeShift, ActiveTimeDuringShift, ActiveTimeAfterShift,
    InactiveTime, InactiveTimeDuringShift, InactiveTimeBeforeShift, InactiveTimeAfterShift,
    Readings, ReadingsMissedActive, ReadingsMissedActiveDuringShift,
    ReadingsMissedInactive, ReadingsMissedInactiveDuringShift,
    ActiveTimeInDirectProductive, ActiveTimeDuringShiftInDirectProductive,
    ActiveTimeBeforeShiftInDirectProductive, ActiveTimeAfterShiftInDirectProductive,
    ActiveTimeInIndirectProductive, ActiveTimeDuringShiftInIndirectProductive,
    ActiveTimeBeforeShiftInIndirectProductive, ActiveTimeAfterShiftInIndirectProductive,
    ActiveTimeInAssignedLocation, ActiveTimeDuringShiftInAssignedLocation,
    ActiveTimeBeforeShiftInAssignedLocation, ActiveTimeAfterShiftInAssignedLocation,
    InactiveTimeInDirectProductive, InactiveTimeDuringShiftInDirectProductive,
    InactiveTimeBeforeShiftInDirectProductive, InactiveTimeAfterShiftInDirectProductive,
    InactiveTimeInIndirectProductive, InactiveTimeDuringShiftInIndirectProductive,
    InactiveTimeBeforeShiftInIndirectProductive, InactiveTimeAfterShiftInIndirectProductive,
    InactiveTimeInAssignedLocation, InactiveTimeDuringShiftInAssignedLocation,
    InactiveTimeBeforeShiftInAssignedLocation, InactiveTimeAfterShiftInAssignedLocation,
    ReportedTime, InventoryAssignedOn, InventoryUnassignedOn, WatermarkUTC
)
VALUES (
    s.ProjectId, s.WorkerId, s.ShiftLocalDate,
    s.CrewID, s.TradeID, s.WorkshiftID, s.WorkshiftDetailsID, s.Segment,
    s.StartAtUTC, s.FinishAtUTC,
    s.FirstDirectProductiveUTC, s.LastDirectProductiveUTC,
    s.FirstAssignedLocationUTC, s.LastAssignedLocationUTC,
    s.ActiveTime, s.ActiveTimeBeforeShift, s.ActiveTimeDuringShift, s.ActiveTimeAfterShift,
    s.InactiveTime, s.InactiveTimeDuringShift, s.InactiveTimeBeforeShift, s.InactiveTimeAfterShift,
    s.Readings, s.ReadingsMissedActive, s.ReadingsMissedActiveDuringShift,
    s.ReadingsMissedInactive, s.ReadingsMissedInactiveDuringShift,
    s.ActiveTimeInDirectProductive, s.ActiveTimeDuringShiftInDirectProductive,
    s.ActiveTimeBeforeShiftInDirectProductive, s.ActiveTimeAfterShiftInDirectProductive,
    s.ActiveTimeInIndirectProductive, s.ActiveTimeDuringShiftInIndirectProductive,
    s.ActiveTimeBeforeShiftInIndirectProductive, s.ActiveTimeAfterShiftInIndirectProductive,
    s.ActiveTimeInAssignedLocation, s.ActiveTimeDuringShiftInAssignedLocation,
    s.ActiveTimeBeforeShiftInAssignedLocation, s.ActiveTimeAfterShiftInAssignedLocation,
    s.InactiveTimeInDirectProductive, s.InactiveTimeDuringShiftInDirectProductive,
    s.InactiveTimeBeforeShiftInDirectProductive, s.InactiveTimeAfterShiftInDirectProductive,
    s.InactiveTimeInIndirectProductive, s.InactiveTimeDuringShiftInIndirectProductive,
    s.InactiveTimeBeforeShiftInIndirectProductive, s.InactiveTimeAfterShiftInIndirectProductive,
    s.InactiveTimeInAssignedLocation, s.InactiveTimeDuringShiftInAssignedLocation,
    s.InactiveTimeBeforeShiftInAssignedLocation, s.InactiveTimeAfterShiftInAssignedLocation,
    s.ReportedTime, s.InventoryAssignedOn, s.InventoryUnassignedOn, current_timestamp()
)
"""

spark.sql(merge_sql)

# Get metrics
target_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
inserted = target_after - target_before

print(f"[OK] MERGE completed")
print(f"Target rows after MERGE: {target_after:,}")
print(f"Estimated inserts: {inserted:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Update Watermark

# COMMAND ----------

print("=" * 60)
print("STEP 9: Update Watermark")
print("=" * 60)

# Get max watermark from processed batch
new_watermark = datetime.now()

update_watermark(
    "gold_fact_workers_shifts_combined",
    new_watermark,
    target_after
)

print(f"Watermark updated to: {new_watermark}")

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
print(f"  - Workers Shifts: {SOURCE_WORKERS_SHIFTS}")
print(f"  - Reported Attendance: {SOURCE_REPORTED_ATTENDANCE}")
print(f"Target: {TARGET_TABLE}")
print(f"")
print(f"Records processed:")
print(f"  - Source shifts: {shifts_count:,}")
print(f"  - Source attendance: {attendance_count:,}")
print(f"")
print(f"Target table:")
print(f"  - Rows before: {target_before:,}")
print(f"  - Rows after: {target_after:,}")
print(f"  - Estimated inserts: {inserted:,}")
print(f"")
print(f"Date range: {min_date} to {max_date}")
print(f"Mode: {load_mode}")
print("=" * 60)

# Return success
dbutils.notebook.exit(f"SUCCESS: processed={shifts_count}, inserted={inserted}")
