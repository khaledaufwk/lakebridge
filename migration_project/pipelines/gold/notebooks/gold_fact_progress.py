# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Progress
# MAGIC
# MAGIC **Converted from:**
# MAGIC - `stg.spDeltaSyncFactProgress` (117 lines) - Staging sync
# MAGIC - `stg.spCalculateFactProgress` (112 lines) - Fact calculation
# MAGIC
# MAGIC **Purpose:** Track approved working time and overtime by worker, project, activity, and date.
# MAGIC Combines data from wc2023_ResourceTimesheet and wc2023_ResourceApprovedHoursSegment.
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - Two-phase staging (source -> _full -> fact) -> Simplified to direct Delta MERGE
# MAGIC - Dimension lookups (Worker, Project, Activity) with fnExtSourceIDAlias
# MAGIC - ROW_NUMBER deduplication on (ResourceId, Day/Date, ProjectId, ActivityID)
# MAGIC - MERGE with float tolerance (0.00001) for ApprovedTime and Overtime
# MAGIC - NOT MATCHED BY SOURCE with DeleteFlag -> Two-phase delete pattern
# MAGIC - Calculated columns: (ApprovedHours/Seconds) / (60*60*24)
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_fact_resource_timesheet` (TimescaleDB: ResourceTimesheet)
# MAGIC - `wakecap_prod.silver.silver_fact_resource_hours_segment` (TimescaleDB: ResourceApprovedHoursSegment)
# MAGIC - `wakecap_prod.silver.silver_worker` (dbo.Worker)
# MAGIC - `wakecap_prod.silver.silver_project` (dbo.Project)
# MAGIC - `wakecap_prod.silver.silver_activity` (dbo.Activity)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_progress`
# MAGIC **Watermarks:** `wakecap_prod.migration._gold_watermarks`
# MAGIC
# MAGIC **ExtSourceID:** 15 (ExtSourceIDAlias: 15)

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

# Source tables - Using Silver layer tables with full column structure
SOURCE_TIMESHEET = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_fact_resource_timesheet"
SOURCE_APPROVED_HOURS = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_fact_resource_hours_segment"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
# NOTE: Use silver_project_dw which has ExtProjectID (UUID) that matches fact tables' ProjectId
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project_dw"
SOURCE_ACTIVITY = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_activity"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_progress"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID
EXT_SOURCE_ID = 15
EXT_SOURCE_ID_ALIAS = 15

# Float comparison tolerance
FLOAT_TOLERANCE = 0.00001

print(f"Source Timesheet: {SOURCE_TIMESHEET}")
print(f"Source Approved Hours: {SOURCE_APPROVED_HOURS}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("source", "all", ["all", "timesheet", "approved_hours"], "Data Source")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")

load_mode = dbutils.widgets.get("load_mode")
source_mode = dbutils.widgets.get("source")
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {load_mode}")
print(f"Source Mode: {source_mode}")
print(f"Project Filter: {project_filter if project_filter else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check source tables
source_tables = [
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

# Check data source tables (at least one must exist)
data_sources_exist = []
for name, table in [("Timesheet", SOURCE_TIMESHEET), ("Approved Hours", SOURCE_APPROVED_HOURS)]:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
        data_sources_exist.append(name)
    except Exception as e:
        print(f"[WARN] {name} not found: {str(e)[:50]}")

if not data_sources_exist:
    print("[ERROR] No data source tables found")
    dbutils.notebook.exit("NO_DATA_SOURCES")

# Activity dimension is optional
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_ACTIVITY} LIMIT 0")
    print(f"[OK] Activity exists: {SOURCE_ACTIVITY}")
    has_activity = True
except:
    print(f"[WARN] Activity not found (using ActivityID=-1 fallback)")
    has_activity = False

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
            .otherwise(F.col(col_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare Dimension Lookups

# COMMAND ----------

print("=" * 60)
print("STEP 1: Prepare Dimension Lookups")
print("=" * 60)

# Worker dimension lookup
# Note: Silver tables use direct IDs (WorkerId) not ExtWorkerID pattern
# The source data ResourceId maps directly to WorkerId
worker_df = spark.table(SOURCE_WORKER)

# Simple dedup by WorkerId
worker_window = Window.partitionBy("WorkerId").orderBy(F.lit(1))
worker_lookup_df = worker_df \
    .withColumn("_rn", F.row_number().over(worker_window)) \
    .filter(F.col("_rn") == 1) \
    .select(
        F.col("WorkerId").alias("dim_WorkerID"),
        F.col("WorkerId").alias("dim_ExtWorkerID")  # Same as WorkerId for direct mapping
    )

print(f"Worker dimension records: {worker_lookup_df.count()}")

# Project dimension lookup
# NOTE: silver_project_dw has ExtProjectID (UUID) that matches fact tables' ProjectId
# Columns: ProjectID (INT), ExtProjectID (UUID), ProjectName, ExtSourceID
project_df = spark.table(SOURCE_PROJECT)

# Filter to ExtSourceID matching our source (14/15 -> alias 15)
# and deduplicate by ExtProjectID
project_window = Window.partitionBy("ExtProjectID").orderBy(F.lit(1))
project_lookup_df = project_df \
    .withColumn("_rn", F.row_number().over(project_window)) \
    .filter(F.col("_rn") == 1) \
    .select(
        F.col("ProjectID").alias("dim_ProjectID"),
        F.col("ExtProjectID").alias("dim_ExtProjectID")  # UUID that matches fact.ProjectId
    )

print(f"Project dimension records: {project_lookup_df.count()}")

# Activity dimension lookup (optional)
# Note: May not be available in TimescaleDB-sourced Silver tables
if has_activity:
    try:
        activity_df = spark.table(SOURCE_ACTIVITY)

        # Check if DataGroupId column exists
        if "DataGroupId" in activity_df.columns:
            activity_window = Window.partitionBy("DataGroupId").orderBy(F.lit(1))
            activity_lookup_df = activity_df \
                .withColumn("_rn", F.row_number().over(activity_window)) \
                .filter(F.col("_rn") == 1) \
                .select(
                    F.col("ActivityId").alias("dim_ActivityID"),
                    F.col("DataGroupId").alias("dim_ExtDataGroupID")
                )
            print(f"Activity dimension records: {activity_lookup_df.count()}")
        else:
            print(f"[WARN] Activity table missing DataGroupId column, using fallback")
            has_activity = False
    except Exception as e:
        print(f"[WARN] Activity lookup failed: {str(e)[:50]}")
        has_activity = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Source DataFrame from Timesheet
# MAGIC
# MAGIC From spCalculateFactProgress:
# MAGIC - Source: wc2023_ResourceTimesheet_full
# MAGIC - ActivityID = -1 (fixed value, no activity lookup)
# MAGIC - ApprovedTime = ApprovedSeconds / (60*60*24)
# MAGIC - Overtime = OverTimeSeconds / (60*60*24)

# COMMAND ----------

print("=" * 60)
print("STEP 2: Build Source from Timesheet")
print("=" * 60)

timesheet_df = None
if "Timesheet" in data_sources_exist and source_mode in ["all", "timesheet"]:
    try:
        # Note: DeletedAt column may not exist in Silver table, so we skip soft-delete filter here
        ts_raw = spark.table(SOURCE_TIMESHEET)

        # Join with Worker dimension
        ts_with_worker = ts_raw.alias("ta").join(
            worker_lookup_df.alias("w"),
            F.col("ta.ResourceId") == F.col("w.dim_ExtWorkerID"),
            "inner"
        ).select(
            "ta.*",
            F.col("w.dim_WorkerID").alias("WorkerID")
        )

        # Join with Project dimension
        # Note: UUIDs are case-sensitive in string comparison, normalize to uppercase
        ts_with_project = ts_with_worker.alias("ta").join(
            project_lookup_df.alias("p"),
            F.upper(F.col("ta.ProjectId")) == F.upper(F.col("p.dim_ExtProjectID")),
            "inner"
        ).select(
            "ta.*",
            F.col("ta.WorkerID"),
            F.col("p.dim_ProjectID").alias("ProjectID2")
        )

        # Add calculated columns and fixed ActivityID=-1
        # Original: (1.0 * ApprovedSeconds) / (60*60*24) as ApprovedTime
        ts_calculated = ts_with_project \
            .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID)) \
            .withColumn("ActivityID", F.lit(-1)) \
            .withColumn("ApprovedTime", (F.lit(1.0) * F.col("ApprovedSeconds")) / (60 * 60 * 24)) \
            .withColumn("Overtime", (F.lit(1.0) * F.col("OverTimeSeconds")) / (60 * 60 * 24)) \
            .withColumn("ShiftLocalDate", F.col("Day"))

        # ROW_NUMBER deduplication
        # Original: PARTITION BY ResourceId, Day, ProjectId, -1 ORDER BY Id DESC
        dedup_window = Window.partitionBy(
            "ResourceId", "Day", "ProjectId", "ActivityID"
        ).orderBy(F.col("Id").desc())

        ts_with_rn = ts_calculated.withColumn("RN", F.row_number().over(dedup_window))
        timesheet_df = ts_with_rn.filter(F.col("RN") == 1)

        print(f"Timesheet source records: {timesheet_df.count()}")
    except Exception as e:
        print(f"[ERROR] Failed to build timesheet source: {str(e)[:100]}")
else:
    print("Skipping timesheet source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build Source DataFrame from Approved Hours
# MAGIC
# MAGIC From spDeltaSyncFactProgress header comments:
# MAGIC - Source: wc2023_ResourceApprovedHoursSegment
# MAGIC - ActivityID from Activity dimension (matched on DataGroupId)
# MAGIC - ApprovedTime = ApprovedHours / (60*60*24)
# MAGIC - Overtime = OverTimeHours / (60*60*24)

# COMMAND ----------

print("=" * 60)
print("STEP 3: Build Source from Approved Hours Segment")
print("=" * 60)

approved_hours_df = None
if "Approved Hours" in data_sources_exist and source_mode in ["all", "approved_hours"]:
    try:
        # silver_fact_resource_hours_segment doesn't have ResourceId or Date directly
        # We need to join with silver_fact_resource_approved_hours via ApprovalId to get them
        SOURCE_APPROVED_HOURS_PARENT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_fact_resource_approved_hours"

        # Check if parent table exists
        try:
            spark.sql(f"SELECT 1 FROM {SOURCE_APPROVED_HOURS_PARENT} LIMIT 0")
            has_parent = True
            print(f"[OK] Approved Hours parent exists: {SOURCE_APPROVED_HOURS_PARENT}")
        except:
            has_parent = False
            print(f"[WARN] Approved Hours parent not found - skipping approved hours source")

        if has_parent:
            # Get segment data
            ah_segment = spark.table(SOURCE_APPROVED_HOURS)

            # Get parent data (has ResourceId and Date)
            ah_parent = spark.table(SOURCE_APPROVED_HOURS_PARENT).select(
                F.col("ResourceApprovedHoursId").alias("parent_Id"),
                F.col("WorkerId").alias("parent_ResourceId"),
                F.col("Date").alias("parent_Date")
            )

            # Join segment with parent to get ResourceId and Date
            ah_raw = ah_segment.alias("seg").join(
                ah_parent.alias("par"),
                F.col("seg.ApprovalId") == F.col("par.parent_Id"),
                "inner"
            ).select(
                "seg.*",
                F.col("par.parent_ResourceId").alias("ResourceId"),
                F.col("par.parent_Date").alias("Date")
            )

            # Join with Worker dimension
            ah_with_worker = ah_raw.alias("ta").join(
                worker_lookup_df.alias("w"),
                F.col("ta.ResourceId") == F.col("w.dim_ExtWorkerID"),
                "inner"
            ).select(
                "ta.*",
                F.col("w.dim_WorkerID").alias("WorkerID")
            )

            # Join with Project dimension
            # Note: UUIDs are case-sensitive in string comparison, normalize to uppercase
            ah_with_project = ah_with_worker.alias("ta").join(
                project_lookup_df.alias("p"),
                F.upper(F.col("ta.ProjectId")) == F.upper(F.col("p.dim_ExtProjectID")),
                "inner"
            ).select(
                "ta.*",
                F.col("ta.WorkerID"),
                F.col("p.dim_ProjectID").alias("ProjectID2")
            )

            # Join with Activity dimension (if available)
            if has_activity:
                ah_with_activity = ah_with_project.alias("ta").join(
                    activity_lookup_df.alias("a"),
                    F.col("ta.DataGroupId") == F.col("a.dim_ExtDataGroupID"),
                    "inner"  # INNER per original SP
                ).select(
                    "ta.*",
                    F.col("ta.WorkerID"),
                    F.col("ta.ProjectID2"),
                    F.col("a.dim_ActivityID").alias("ActivityID")
                )
            else:
                ah_with_activity = ah_with_project.withColumn("ActivityID", F.lit(-1))

            # Add calculated columns
            # Original: (1.0 * ApprovedHours) / (60*60*24) as ApprovedTime
            ah_calculated = ah_with_activity \
                .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID)) \
                .withColumn("ApprovedTime", (F.lit(1.0) * F.col("ApprovedHours")) / (60 * 60 * 24)) \
                .withColumn("Overtime", (F.lit(1.0) * F.col("OverTimeHours")) / (60 * 60 * 24)) \
                .withColumn("ShiftLocalDate", F.col("Date"))

            # ROW_NUMBER deduplication
            # Original: PARTITION BY ResourceId, Date, ProjectId, DataGroupId ORDER BY Id DESC
            dedup_window = Window.partitionBy(
                "ResourceId", "Date", "ProjectId", "DataGroupId"
            ).orderBy(F.col("Id").desc())

            ah_with_rn = ah_calculated.withColumn("RN", F.row_number().over(dedup_window))
            approved_hours_df = ah_with_rn.filter(F.col("RN") == 1)

            print(f"Approved Hours source records: {approved_hours_df.count()}")
    except Exception as e:
        print(f"[ERROR] Failed to build approved hours source: {str(e)[:100]}")
else:
    print("Skipping approved hours source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Combine Sources

# COMMAND ----------

print("=" * 60)
print("STEP 4: Combine Sources")
print("=" * 60)

# Select common columns for union
common_columns = [
    "WorkerID", "ProjectID2", "ActivityID", "ShiftLocalDate",
    "ExtSourceID", "ApprovedTime", "Overtime"
]

source_dfs = []
if timesheet_df is not None:
    source_dfs.append(timesheet_df.select(common_columns))
if approved_hours_df is not None:
    source_dfs.append(approved_hours_df.select(common_columns))

if not source_dfs:
    print("[ERROR] No source data available")
    dbutils.notebook.exit("NO_SOURCE_DATA")

# Union all sources
# Note: Timesheet source uses ActivityID=-1, ApprovedHours uses actual ActivityIDs
# These won't overlap on the same key, so we just union (no aggregation needed)
combined_df = source_dfs[0]
for df in source_dfs[1:]:
    combined_df = combined_df.unionByName(df)

# Final deduplication: if somehow same key appears, keep one record
# This handles edge cases where both sources might produce same key
dedup_window = Window.partitionBy(
    "WorkerID", "ProjectID2", "ActivityID", "ShiftLocalDate", "ExtSourceID"
).orderBy(F.lit(1))

final_source_df = combined_df \
    .withColumn("_final_rn", F.row_number().over(dedup_window)) \
    .filter(F.col("_final_rn") == 1) \
    .drop("_final_rn")

# Note: cache() removed - not supported on serverless compute
final_count = final_source_df.count()
print(f"Combined source records: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create/Update Target Table

# COMMAND ----------

print("=" * 60)
print("STEP 5: Ensure Target Table Exists")
print("=" * 60)

target_schema = """
    FactProgressID BIGINT GENERATED ALWAYS AS IDENTITY,
    ProjectID INT NOT NULL,
    WorkerID INT NOT NULL,
    ActivityID INT NOT NULL,
    ShiftLocalDate DATE NOT NULL,
    ExtSourceID INT,
    ApprovedTime DOUBLE,
    Overtime DOUBLE,
    DeleteFlag INT,
    WatermarkUTC TIMESTAMP,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
"""

try:
    spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    print(f"[OK] Target table exists: {TARGET_TABLE}")
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

# Get target row count before merge
try:
    target_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0
print(f"Target rows before MERGE: {target_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Execute MERGE

# COMMAND ----------

print("=" * 60)
print("STEP 6: Execute MERGE")
print("=" * 60)

# Create temp view for source
final_source_df.createOrReplaceTempView("progress_source")

# Execute MERGE
# Original match: WorkerID, ShiftLocalDate (Day), ProjectID, ActivityID, fnExtSourceIDAlias
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING progress_source AS s
ON t.WorkerID = s.WorkerID
   AND t.ShiftLocalDate = s.ShiftLocalDate
   AND t.ProjectID = s.ProjectID2
   AND t.ActivityID = s.ActivityID
   AND CASE WHEN t.ExtSourceID IN (15, 18) THEN 15 WHEN t.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE t.ExtSourceID END
     = CASE WHEN s.ExtSourceID IN (15, 18) THEN 15 WHEN s.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE s.ExtSourceID END

WHEN MATCHED AND (
    -- ActivityID changed
    (t.ActivityID <> s.ActivityID OR (t.ActivityID IS NULL AND s.ActivityID IS NOT NULL) OR (t.ActivityID IS NOT NULL AND s.ActivityID IS NULL)) OR
    -- ApprovedTime changed (with tolerance)
    (ABS(COALESCE(t.ApprovedTime, 0) - COALESCE(s.ApprovedTime, 0)) > {FLOAT_TOLERANCE}
        OR (t.ApprovedTime IS NULL AND s.ApprovedTime IS NOT NULL)
        OR (t.ApprovedTime IS NOT NULL AND s.ApprovedTime IS NULL)) OR
    -- Overtime changed (with tolerance)
    (ABS(COALESCE(t.Overtime, 0) - COALESCE(s.Overtime, 0)) > {FLOAT_TOLERANCE}
        OR (t.Overtime IS NULL AND s.Overtime IS NOT NULL)
        OR (t.Overtime IS NOT NULL AND s.Overtime IS NULL))
)
THEN UPDATE SET
    t.WatermarkUTC = current_timestamp(),
    t.UpdatedAt = current_timestamp(),
    t.ActivityID = s.ActivityID,
    t.ApprovedTime = s.ApprovedTime,
    t.Overtime = s.Overtime,
    t.ExtSourceID = s.ExtSourceID,
    t.DeleteFlag = 0

WHEN NOT MATCHED THEN INSERT (
    ProjectID, WorkerID, ActivityID, ShiftLocalDate,
    ExtSourceID, ApprovedTime, Overtime, DeleteFlag
)
VALUES (
    s.ProjectID2, s.WorkerID, s.ActivityID, s.ShiftLocalDate,
    s.ExtSourceID, s.ApprovedTime, s.Overtime, 0
)
"""

spark.sql(merge_sql)
print("[OK] MERGE completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Handle NOT MATCHED BY SOURCE (Soft Delete via DeleteFlag)
# MAGIC
# MAGIC Original pattern:
# MAGIC 1. WHEN NOT MATCHED BY SOURCE THEN UPDATE SET DeleteFlag = 1
# MAGIC 2. DELETE FROM FactProgress WHERE DeleteFlag = 1
# MAGIC
# MAGIC In Databricks, we implement this as:
# MAGIC 1. UPDATE SET DeleteFlag = 1 WHERE NOT EXISTS in source
# MAGIC 2. DELETE WHERE DeleteFlag = 1

# COMMAND ----------

print("=" * 60)
print("STEP 7: Handle NOT MATCHED BY SOURCE (Delete)")
print("=" * 60)

if load_mode == "full":
    print("Full mode: Marking records not in source for deletion...")

    # Mark records not in source
    mark_delete_sql = f"""
    UPDATE {TARGET_TABLE} AS t
    SET t.DeleteFlag = 1,
        t.WatermarkUTC = current_timestamp(),
        t.UpdatedAt = current_timestamp()
    WHERE NOT EXISTS (
        SELECT 1 FROM progress_source s
        WHERE t.WorkerID = s.WorkerID
          AND t.ShiftLocalDate = s.ShiftLocalDate
          AND t.ProjectID = s.ProjectID2
          AND t.ActivityID = s.ActivityID
    )
    AND t.DeleteFlag = 0
    """
    spark.sql(mark_delete_sql)

    # Delete flagged records
    delete_sql = f"DELETE FROM {TARGET_TABLE} WHERE DeleteFlag = 1"
    spark.sql(delete_sql)
    print("[OK] Deleted records not in source")
else:
    print("Incremental mode: Skipping delete phase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Update Watermark

# COMMAND ----------

print("=" * 60)
print("STEP 8: Update Watermark")
print("=" * 60)

# Get final target count
target_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
inserted = target_after - target_before

# Update watermark
new_watermark = datetime.now()
update_watermark("gold_fact_progress", new_watermark, target_after)

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
print(f"  - Timesheet: {SOURCE_TIMESHEET}")
print(f"  - Approved Hours: {SOURCE_APPROVED_HOURS}")
print(f"Target: {TARGET_TABLE}")
print(f"")
print(f"Records processed:")
print(f"  - Timesheet: {timesheet_df.count() if timesheet_df else 'N/A'}")
print(f"  - Approved Hours: {approved_hours_df.count() if approved_hours_df else 'N/A'}")
print(f"  - Combined: {final_count}")
print(f"")
print(f"Target table:")
print(f"  - Rows before: {target_before}")
print(f"  - Rows after: {target_after}")
print(f"  - Estimated inserts: {inserted}")
print(f"")
print(f"Mode: {load_mode}")
print("=" * 60)

# Return success
dbutils.notebook.exit(f"SUCCESS: processed={final_count}, inserted={inserted}")
