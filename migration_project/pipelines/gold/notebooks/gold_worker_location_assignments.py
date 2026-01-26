# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Worker Location Assignments
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateWorkerLocationAssignments` (290 lines)
# MAGIC
# MAGIC **Purpose:** Calculate worker location assignments by combining:
# MAGIC 1. eTQR assignments from FactWorkersTasks (ExtSourceID=12)
# MAGIC 2. Manual location assignments from TimescaleDB (ExtSourceID=15)
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - TEMP_TABLE (`#rules`) -> Spark DataFrame with cache()
# MAGIC - MERGE INTO with two-phase (MATCHED UPDATE + NOT MATCHED INSERT + soft delete) -> DeltaTable.merge()
# MAGIC - ROW_NUMBER() OVER (PARTITION BY...) -> PySpark Window functions
# MAGIC - Custom function `stg.fnExtSourceIDAlias()` -> Inline CASE expression
# MAGIC - LEAD/LAG for period merging -> PySpark Window functions
# MAGIC - SUM() OVER for overlapping period consolidation -> PySpark Window functions
# MAGIC - Post-MERGE #pre_deletes pattern -> Separate DataFrame operations
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.gold.fact_workers_tasks` (dbo.FactWorkersTasks)
# MAGIC - `wakecap_prod.silver.silver_task` (dbo.Task)
# MAGIC - `wakecap_prod.silver.silver_location_group` (dbo.LocationGroup)
# MAGIC - `wakecap_prod.raw.timescale_manual_location_assignment` (stg.wc2023_ManualLocationAssignment_full)
# MAGIC - `wakecap_prod.silver.silver_worker` (dbo.Worker)
# MAGIC - `wakecap_prod.silver.silver_zone` (dbo.Zone)
# MAGIC - `wakecap_prod.silver.silver_crew` (dbo.Crew)
# MAGIC - `wakecap_prod.silver.silver_floor` (dbo.Floor)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_worker_location_assignments`

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
RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables
FACT_WORKERS_TASKS = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.fact_workers_tasks"
SILVER_TASK = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_task"
SILVER_LOCATION_GROUP = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_location_group"
MANUAL_LOCATION_ASSIGNMENT = f"{TARGET_CATALOG}.{RAW_SCHEMA}.timescale_manual_location_assignment"

# Dimension tables for manual assignments
DIM_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
DIM_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone"
DIM_CREW = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew"
DIM_FLOOR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_floor"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_worker_location_assignments"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID constants
EXT_SOURCE_ID_ETQR = 12      # eTQR Assignments
EXT_SOURCE_ID_MANUAL = 15    # Manual Location Assignments

print(f"Fact Workers Tasks: {FACT_WORKERS_TASKS}")
print(f"Manual Assignments: {MANUAL_LOCATION_ASSIGNMENT}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")
dbutils.widgets.text("lookback_days", "30", "Lookback Days for Incremental")

load_mode = dbutils.widgets.get("load_mode")
project_filter = dbutils.widgets.get("project_id")
lookback_days = int(dbutils.widgets.get("lookback_days"))

print(f"Load Mode: {load_mode}")
print(f"Project Filter: {project_filter if project_filter else 'None'}")
print(f"Lookback Days: {lookback_days}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check required tables
required_tables = [
    ("FactWorkersTasks", FACT_WORKERS_TASKS),
    ("Task", SILVER_TASK),
    ("LocationGroup", SILVER_LOCATION_GROUP),
    ("ManualLocationAssignment", MANUAL_LOCATION_ASSIGNMENT),
    ("Worker", DIM_WORKER),
]

for name, table in required_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
    except Exception as e:
        print(f"[WARN] {name} not found: {table} - {str(e)[:50]}")

# Check optional dimension tables
optional_tables = [
    ("Zone", DIM_ZONE),
    ("Crew", DIM_CREW),
    ("Floor", DIM_FLOOR),
]

for name, table in optional_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
    except Exception as e:
        print(f"[WARN] {name} not found: {table}")

# Ensure target schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print(f"[OK] Target schemas verified")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def ext_source_id_alias(ext_source_id_col):
    """
    Equivalent of stg.fnExtSourceIDAlias() function.
    Groups ExtSourceID values:
    - 14, 15, 16, 17, 18, 19 -> 15
    - 1, 2, 11, 12 -> 2
    - Otherwise -> original value
    """
    return (
        F.when(F.col(ext_source_id_col).isin(14, 15, 16, 17, 18, 19), F.lit(15))
         .when(F.col(ext_source_id_col).isin(1, 2, 11, 12), F.lit(2))
         .otherwise(F.col(ext_source_id_col))
    )


def ext_source_id_alias_value(value):
    """
    Return the alias for a specific ExtSourceID value.
    """
    if value in (14, 15, 16, 17, 18, 19):
        return 15
    elif value in (1, 2, 11, 12):
        return 2
    else:
        return value


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
# MAGIC ## Step 1: Load Dimension Tables with Deduplication

# COMMAND ----------

print("Loading dimension tables with deduplication...")

# ExtSourceIDAlias for manual assignments (15 -> 15)
TARGET_EXT_SOURCE_ALIAS = ext_source_id_alias_value(EXT_SOURCE_ID_MANUAL)

def load_dimension_deduped(table_path, ext_id_col, internal_id_col, extra_cols=None):
    """
    Load dimension table with ROW_NUMBER deduplication.
    Matches the original pattern:
    ROW_NUMBER() OVER (PARTITION BY ExtID, fnExtSourceIDAlias(ExtSourceID) ORDER BY (SELECT NULL)) rn
    """
    try:
        df = spark.table(table_path)

        # Check if ExtSourceID column exists
        if "ExtSourceID" in df.columns:
            df = df.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
            df = df.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)

            # ROW_NUMBER for deduplication
            window = Window.partitionBy(ext_id_col, "ExtSourceIDAlias").orderBy(F.lit(1))
            df = df.withColumn("rn", F.row_number().over(window))
            df = df.filter(F.col("rn") == 1).drop("rn", "ExtSourceIDAlias")
        else:
            # Simple dedup without ExtSourceID
            window = Window.partitionBy(ext_id_col).orderBy(F.lit(1))
            df = df.withColumn("rn", F.row_number().over(window))
            df = df.filter(F.col("rn") == 1).drop("rn")

        # Select needed columns
        select_cols = [ext_id_col, internal_id_col]
        if extra_cols:
            select_cols.extend(extra_cols)

        available_cols = [c for c in select_cols if c in df.columns]
        return df.select(*available_cols)
    except Exception as e:
        print(f"  Warning: Could not load {table_path}: {str(e)[:50]}")
        return None


# 1. Worker dimension - Match on ExtWorkerID
try:
    dim_worker = spark.table(DIM_WORKER)

    if "ExtSourceID" in dim_worker.columns:
        dim_worker = dim_worker.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
        dim_worker = dim_worker.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)

        worker_window = Window.partitionBy("ExtWorkerID", "ExtSourceIDAlias").orderBy(F.lit(1))
        dim_worker = dim_worker.withColumn("rn", F.row_number().over(worker_window))
        dim_worker = dim_worker.filter(F.col("rn") == 1).drop("rn", "ExtSourceIDAlias")

    dim_worker = dim_worker.select(
        F.col("ExtWorkerID").alias("dim_ExtWorkerID"),
        F.col("WorkerID").alias("dim_WorkerID"),
        F.col("ProjectID").alias("dim_Worker_ProjectID")
    ).filter(F.col("DeletedAt").isNull())
    print(f"  Worker dimension: {dim_worker.count()} rows")
except Exception as e:
    dim_worker = None
    print(f"  Worker dimension: Not available - {str(e)[:50]}")


# 2. Zone dimension - Match on ExtZoneID
try:
    dim_zone = spark.table(DIM_ZONE)

    if "ExtSourceID" in dim_zone.columns:
        dim_zone = dim_zone.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
        dim_zone = dim_zone.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)

        zone_window = Window.partitionBy("ExtZoneID", "ExtSourceIDAlias").orderBy(F.lit(1))
        dim_zone = dim_zone.withColumn("rn", F.row_number().over(zone_window))
        dim_zone = dim_zone.filter(F.col("rn") == 1).drop("rn", "ExtSourceIDAlias")

    dim_zone = dim_zone.select(
        F.col("ExtZoneID").alias("dim_ExtZoneID"),
        F.col("ZoneID").alias("dim_ZoneID")
    ).filter(F.col("DeletedAt").isNull())
    print(f"  Zone dimension: {dim_zone.count()} rows")
except Exception as e:
    dim_zone = None
    print(f"  Zone dimension: Not available - {str(e)[:50]}")


# 3. Crew dimension - Match on ExtCrewID
try:
    dim_crew = spark.table(DIM_CREW)

    if "ExtSourceID" in dim_crew.columns:
        dim_crew = dim_crew.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
        dim_crew = dim_crew.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)

        crew_window = Window.partitionBy("ExtCrewID", "ExtSourceIDAlias").orderBy(F.lit(1))
        dim_crew = dim_crew.withColumn("rn", F.row_number().over(crew_window))
        dim_crew = dim_crew.filter(F.col("rn") == 1).drop("rn", "ExtSourceIDAlias")

    dim_crew = dim_crew.select(
        F.col("ExtCrewID").alias("dim_ExtCrewID"),
        F.col("CrewID").alias("dim_CrewID"),
        F.col("ProjectID").alias("dim_Crew_ProjectID")
    ).filter(F.col("DeletedAt").isNull())
    print(f"  Crew dimension: {dim_crew.count()} rows")
except Exception as e:
    dim_crew = None
    print(f"  Crew dimension: Not available - {str(e)[:50]}")


# 4. Floor dimension - Match on ExtSpaceID
try:
    dim_floor = spark.table(DIM_FLOOR)

    if "ExtSourceID" in dim_floor.columns:
        dim_floor = dim_floor.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
        dim_floor = dim_floor.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)

        floor_window = Window.partitionBy("ExtSpaceID", "ExtSourceIDAlias").orderBy(F.lit(1))
        dim_floor = dim_floor.withColumn("rn", F.row_number().over(floor_window))
        dim_floor = dim_floor.filter(F.col("rn") == 1).drop("rn", "ExtSourceIDAlias")

    dim_floor = dim_floor.select(
        F.col("ExtSpaceID").alias("dim_ExtSpaceID"),
        F.col("FloorID").alias("dim_FloorID")
    ).filter(F.col("DeletedAt").isNull())
    print(f"  Floor dimension: {dim_floor.count()} rows")
except Exception as e:
    dim_floor = None
    print(f"  Floor dimension: Not available - {str(e)[:50]}")


# 5. LocationGroup dimension - Match on ExtLocationGroupID
try:
    dim_location_group = spark.table(SILVER_LOCATION_GROUP)

    if "ExtSourceID" in dim_location_group.columns:
        dim_location_group = dim_location_group.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
        dim_location_group = dim_location_group.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)

        lg_window = Window.partitionBy("ExtLocationGroupID", "ExtSourceIDAlias").orderBy(F.lit(1))
        dim_location_group = dim_location_group.withColumn("rn", F.row_number().over(lg_window))
        dim_location_group = dim_location_group.filter(F.col("rn") == 1).drop("rn", "ExtSourceIDAlias")

    dim_location_group = dim_location_group.select(
        F.col("ExtLocationGroupID").alias("dim_ExtLocationGroupID"),
        F.col("LocationGroupID").alias("dim_LocationGroupID")
    )
    print(f"  LocationGroup dimension: {dim_location_group.count()} rows")
except Exception as e:
    dim_location_group = None
    print(f"  LocationGroup dimension: Not available - {str(e)[:50]}")


print("\nDimension tables loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build eTQR Assignments DataFrame (ExtSourceID=12)
# MAGIC
# MAGIC This implements the first part of the `#rules` temp table from the original SP:
# MAGIC ```sql
# MAGIC WITH eTQRAssignments AS (
# MAGIC     SELECT ... FROM FactWorkersTasks fwt
# MAGIC     INNER JOIN Task t ON fwt.TaskID = t.TaskID
# MAGIC     INNER JOIN LocationGroup lg ON lg.LocationGroupID = t.LocationGroupID
# MAGIC     WHERE ROW_NUMBER() OVER (...) = 1
# MAGIC )
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("STEP 2: Build eTQR Assignments DataFrame")
print("=" * 60)

try:
    # Load FactWorkersTasks
    fwt_df = spark.table(FACT_WORKERS_TASKS).filter(F.col("DeletedAt").isNull())

    # Load Task
    task_df = spark.table(SILVER_TASK).select(
        F.col("TaskID"),
        F.col("ShiftLocalDate"),
        F.col("LocationGroupID"),
        F.col("ActivityID")
    )

    # Load LocationGroup (just to verify existence, as in original SP)
    lg_df = spark.table(SILVER_LOCATION_GROUP).select(
        F.col("LocationGroupID").alias("lg_LocationGroupID")
    )

    # Join FactWorkersTasks with Task
    etqr_base = fwt_df.alias("fwt").join(
        task_df.alias("t"),
        F.col("fwt.TaskID") == F.col("t.TaskID"),
        "inner"
    )

    # Join with LocationGroup to ensure it exists
    etqr_base = etqr_base.join(
        lg_df.alias("lg"),
        F.col("t.LocationGroupID") == F.col("lg.lg_LocationGroupID"),
        "inner"
    )

    # ROW_NUMBER for deduplication
    # PARTITION BY fwt.ProjectID, fwt.WorkerID, t.ShiftLocalDate, t.LocationGroupID ORDER BY t.ActivityID
    etqr_window = Window.partitionBy(
        "fwt.ProjectID", "fwt.WorkerID", "t.ShiftLocalDate", "t.LocationGroupID"
    ).orderBy("t.ActivityID")

    etqr_deduped = etqr_base.withColumn("RN", F.row_number().over(etqr_window))
    etqr_deduped = etqr_deduped.filter(F.col("RN") == 1)

    # Select columns for eTQRAssignments CTE
    etqr_assignments = etqr_deduped.select(
        F.col("fwt.ProjectID"),
        F.col("fwt.WorkerID"),
        F.col("t.ShiftLocalDate").alias("ValidFrom_ShiftLocalDate"),
        F.col("t.LocationGroupID"),
        F.lit(EXT_SOURCE_ID_ETQR).alias("ExtSourceID")
    )

    etqr_count = etqr_assignments.count()
    print(f"eTQR Assignments (deduped): {etqr_count:,} rows")

except Exception as e:
    print(f"Warning: Could not build eTQR Assignments: {str(e)[:100]}")
    etqr_assignments = None
    etqr_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2a: Calculate ValidTo using LEAD and Period Merging
# MAGIC
# MAGIC This implements the complex nested CTEs with LAG/LEAD for period merging:
# MAGIC - Calculate ValidTo_ShiftLocalDate using LEAD
# MAGIC - Identify IgnoreEnd (next period starts when this ends)
# MAGIC - Identify IgnoreStart (this period starts when previous ends)
# MAGIC - Merge consecutive periods with RealEnd

# COMMAND ----------

if etqr_assignments is not None and etqr_count > 0:
    print("Calculating ValidTo and merging consecutive periods...")

    # Step 1: Calculate ValidTo_ShiftLocalDate using LEAD
    # Group by ProjectID, WorkerID, ValidFrom_ShiftLocalDate to get unique dates first
    etqr_dates = etqr_assignments.groupBy("ProjectID", "WorkerID", "ValidFrom_ShiftLocalDate").agg(
        F.first("LocationGroupID").alias("LocationGroupID"),
        F.first("ExtSourceID").alias("ExtSourceID")
    )

    valid_to_window = Window.partitionBy("ProjectID", "WorkerID").orderBy("ValidFrom_ShiftLocalDate")

    etqr_with_valid_to = etqr_dates.withColumn(
        "ValidTo_ShiftLocalDate",
        F.lead("ValidFrom_ShiftLocalDate", 1).over(valid_to_window)
    )

    # Step 2: Add CrewID = NULL, ZoneID = NULL, FloorID = NULL for eTQR
    etqr_with_dims = etqr_with_valid_to.withColumn("CrewID", F.lit(None).cast("int"))
    etqr_with_dims = etqr_with_dims.withColumn("ZoneID", F.lit(None).cast("int"))
    etqr_with_dims = etqr_with_dims.withColumn("FloorID", F.lit(None).cast("int"))

    # Step 3: Calculate IgnoreEnd and IgnoreStart for period merging
    # Partition by all assignment attributes
    merge_window = Window.partitionBy(
        "ProjectID", "WorkerID", "CrewID", "ZoneID", "FloorID", "LocationGroupID"
    ).orderBy("ValidFrom_ShiftLocalDate")

    etqr_with_flags = etqr_with_dims.withColumn(
        "IgnoreEnd",
        F.when(
            F.lead("ValidFrom_ShiftLocalDate", 1).over(merge_window) == F.col("ValidTo_ShiftLocalDate"),
            F.lit(1)
        ).otherwise(F.lit(None))
    ).withColumn(
        "IgnoreStart",
        F.when(
            F.lag("ValidTo_ShiftLocalDate", 1).over(merge_window) == F.col("ValidFrom_ShiftLocalDate"),
            F.lit(1)
        ).otherwise(F.lit(None))
    )

    # Step 4: Calculate RealEnd (for records where IgnoreEnd = 1)
    etqr_with_real_end = etqr_with_flags.withColumn(
        "RealEnd",
        F.when(
            F.col("IgnoreEnd") == 1,
            F.lead("ValidTo_ShiftLocalDate", 1).over(merge_window)
        ).otherwise(F.lit(None))
    )

    # Step 5: Filter to keep only records where IgnoreStart IS NULL OR IgnoreEnd IS NULL
    etqr_filtered = etqr_with_real_end.filter(
        F.col("IgnoreStart").isNull() | F.col("IgnoreEnd").isNull()
    )

    # Step 6: Further filter to IgnoreStart IS NULL (keep only start of merged periods)
    etqr_final = etqr_filtered.filter(F.col("IgnoreStart").isNull())

    # Step 7: Calculate final ValidTo (use RealEnd if IgnoreEnd=1, else ValidTo)
    etqr_rules = etqr_final.withColumn(
        "ValidTo_Final",
        F.when(
            F.col("IgnoreEnd") == 1,
            F.col("RealEnd")
        ).otherwise(F.col("ValidTo_ShiftLocalDate"))
    ).select(
        "ProjectID",
        "WorkerID",
        "CrewID",
        F.col("ValidFrom_ShiftLocalDate"),
        F.col("ValidTo_Final").alias("ValidTo_ShiftLocalDate"),
        "ZoneID",
        "FloorID",
        "LocationGroupID",
        "ExtSourceID"
    )

    etqr_rules_count = etqr_rules.count()
    print(f"eTQR Rules (after period merging): {etqr_rules_count:,} rows")
else:
    etqr_rules = None
    etqr_rules_count = 0
    print("Skipping eTQR period merging - no data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build Manual Assignments DataFrame (ExtSourceID=15)
# MAGIC
# MAGIC This implements the second part of the UNION ALL:
# MAGIC ```sql
# MAGIC SELECT ...
# MAGIC FROM stg.wc2023_ManualLocationAssignment_full la
# MAGIC LEFT JOIN Worker w ON ...
# MAGIC LEFT JOIN Zone z ON ...
# MAGIC LEFT JOIN Crew c ON ...
# MAGIC LEFT JOIN Floor f ON ...
# MAGIC LEFT JOIN LocationGroup lg ON ...
# MAGIC WHERE (w.WorkerID IS NOT NULL OR c.CrewID IS NOT NULL)
# MAGIC   AND (z.ZoneID IS NOT NULL OR f.FloorID IS NOT NULL OR lg.LocationGroupID IS NOT NULL)
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("STEP 3: Build Manual Assignments DataFrame")
print("=" * 60)

try:
    # Load Manual Location Assignments
    manual_df = spark.table(MANUAL_LOCATION_ASSIGNMENT).filter(F.col("DeletedAt").isNull())
    manual_count_raw = manual_df.count()
    print(f"Manual Assignments (raw): {manual_count_raw:,} rows")

    # Alias for joins
    manual_df = manual_df.alias("la")

    # LEFT join with Worker dimension
    if dim_worker is not None:
        manual_df = manual_df.join(
            F.broadcast(dim_worker).alias("w"),
            F.col("la.ResourceId") == F.col("w.dim_ExtWorkerID"),
            "left"
        )
    else:
        manual_df = manual_df.withColumn("dim_WorkerID", F.lit(None).cast("int"))
        manual_df = manual_df.withColumn("dim_Worker_ProjectID", F.lit(None).cast("int"))

    # LEFT join with Zone dimension
    if dim_zone is not None:
        manual_df = manual_df.join(
            F.broadcast(dim_zone).alias("z"),
            F.col("la.ZoneId") == F.col("z.dim_ExtZoneID"),
            "left"
        )
    else:
        manual_df = manual_df.withColumn("dim_ZoneID", F.lit(None).cast("int"))

    # LEFT join with Crew dimension
    if dim_crew is not None:
        manual_df = manual_df.join(
            F.broadcast(dim_crew).alias("c"),
            F.col("la.CrewId") == F.col("c.dim_ExtCrewID"),
            "left"
        )
    else:
        manual_df = manual_df.withColumn("dim_CrewID", F.lit(None).cast("int"))
        manual_df = manual_df.withColumn("dim_Crew_ProjectID", F.lit(None).cast("int"))

    # LEFT join with Floor dimension
    if dim_floor is not None:
        manual_df = manual_df.join(
            F.broadcast(dim_floor).alias("f"),
            F.col("la.SpaceId") == F.col("f.dim_ExtSpaceID"),
            "left"
        )
    else:
        manual_df = manual_df.withColumn("dim_FloorID", F.lit(None).cast("int"))

    # LEFT join with LocationGroup dimension
    if dim_location_group is not None:
        manual_df = manual_df.join(
            F.broadcast(dim_location_group).alias("lg"),
            F.col("la.LocationGroupId") == F.col("lg.dim_ExtLocationGroupID"),
            "left"
        )
    else:
        manual_df = manual_df.withColumn("dim_LocationGroupID", F.lit(None).cast("int"))

    # Apply filter: (WorkerID IS NOT NULL OR CrewID IS NOT NULL)
    #               AND (ZoneID IS NOT NULL OR FloorID IS NOT NULL OR LocationGroupID IS NOT NULL)
    manual_filtered = manual_df.filter(
        (F.col("dim_WorkerID").isNotNull() | F.col("dim_CrewID").isNotNull()) &
        (F.col("dim_ZoneID").isNotNull() | F.col("dim_FloorID").isNotNull() | F.col("dim_LocationGroupID").isNotNull())
    )

    # Build output columns matching original SP logic:
    # - ProjectID from Worker or Crew
    # - ValidFrom = DateFrom
    # - ValidTo = DATEADD(DAY, 1, DateTo) - converting closed to open interval
    # - ZoneID from dimension
    # - FloorID = dim_FloorID if ZoneId IS NULL
    # - LocationGroupID = dim_LocationGroupID if ZoneId IS NULL AND SpaceId IS NULL
    manual_rules = manual_filtered.select(
        F.coalesce(F.col("dim_Worker_ProjectID"), F.col("dim_Crew_ProjectID")).alias("ProjectID"),
        F.col("dim_WorkerID").alias("WorkerID"),
        F.col("dim_CrewID").alias("CrewID"),
        F.col("la.DateFrom").alias("ValidFrom_ShiftLocalDate"),
        # DATEADD(DAY, 1, DateTo) to convert closed interval to open interval
        F.date_add(F.col("la.DateTo"), 1).alias("ValidTo_ShiftLocalDate"),
        F.col("dim_ZoneID").alias("ZoneID"),
        # FloorID = dim_FloorID if ZoneId IS NULL
        F.when(
            F.col("la.ZoneId").isNull(),
            F.col("dim_FloorID")
        ).otherwise(F.lit(None)).alias("FloorID"),
        # LocationGroupID = dim_LocationGroupID if ZoneId IS NULL AND SpaceId IS NULL
        F.when(
            F.col("la.ZoneId").isNull() & F.col("la.SpaceId").isNull(),
            F.col("dim_LocationGroupID")
        ).otherwise(F.lit(None)).alias("LocationGroupID"),
        F.lit(EXT_SOURCE_ID_MANUAL).alias("ExtSourceID")
    )

    manual_rules_count = manual_rules.count()
    print(f"Manual Rules (after dimension joins): {manual_rules_count:,} rows")

except Exception as e:
    print(f"Warning: Could not build Manual Assignments: {str(e)[:100]}")
    manual_rules = None
    manual_rules_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Combine Rules with UNION ALL

# COMMAND ----------

print("=" * 60)
print("STEP 4: Combine Rules with UNION ALL")
print("=" * 60)

# Combine eTQR and Manual rules
rules_list = []

if etqr_rules is not None and etqr_rules_count > 0:
    rules_list.append(etqr_rules)
    print(f"  Added eTQR rules: {etqr_rules_count:,} rows")

if manual_rules is not None and manual_rules_count > 0:
    rules_list.append(manual_rules)
    print(f"  Added Manual rules: {manual_rules_count:,} rows")

if len(rules_list) > 0:
    if len(rules_list) == 1:
        rules_df = rules_list[0]
    else:
        rules_df = rules_list[0].unionByName(rules_list[1], allowMissingColumns=True)

    rules_df = rules_df.cache()
    total_rules = rules_df.count()
    print(f"\nCombined rules (#rules temp table): {total_rules:,} rows")
else:
    print("ERROR: No rules to process")
    dbutils.notebook.exit("NO_RULES_TO_PROCESS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Consolidate Overlapping Periods
# MAGIC
# MAGIC This implements the complex overlapping period consolidation logic:
# MAGIC ```sql
# MAGIC SELECT ProjectID, WorkerID, CrewID, MIN(ValidFrom), MAX(ValidTo), ZoneID, FloorID, LocationGroupID
# MAGIC FROM (
# MAGIC     SELECT *, SUM(IsStart) OVER (...) AS Grp
# MAGIC     FROM (
# MAGIC         SELECT *, CASE WHEN ValidFrom <= PrevEnd THEN NULL ELSE 1 END as IsStart
# MAGIC         FROM (
# MAGIC             SELECT *, MAX(ValidTo) OVER (...ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as PrevEnd
# MAGIC             FROM #rules
# MAGIC         )
# MAGIC     )
# MAGIC )
# MAGIC GROUP BY ProjectID, WorkerID, CrewID, ZoneID, FloorID, LocationGroupID, Grp
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("STEP 5: Consolidate Overlapping Periods")
print("=" * 60)

# Define partition window for period consolidation
partition_cols = ["ProjectID", "WorkerID", "CrewID", "ZoneID", "FloorID", "LocationGroupID"]
order_col = "ValidFrom_ShiftLocalDate"

# Step 1: Calculate PrevEnd (MAX ValidTo for all preceding rows)
prev_end_window = Window.partitionBy(*partition_cols).orderBy(order_col).rowsBetween(
    Window.unboundedPreceding, -1
)

# Replace NULL ValidTo with far future date for MAX calculation
rules_with_valid_to = rules_df.withColumn(
    "ValidTo_Coalesced",
    F.coalesce(F.col("ValidTo_ShiftLocalDate"), F.to_date(F.lit("2100-01-01")))
)

rules_with_prev_end = rules_with_valid_to.withColumn(
    "PrevEnd",
    F.max("ValidTo_Coalesced").over(prev_end_window)
)

# Step 2: Calculate IsStart (1 if this starts a new group, NULL if it overlaps with previous)
rules_with_is_start = rules_with_prev_end.withColumn(
    "IsStart",
    F.when(
        F.col("ValidFrom_ShiftLocalDate") <= F.col("PrevEnd"),
        F.lit(None)
    ).otherwise(F.lit(1))
)

# Step 3: Calculate Grp using SUM(IsStart) OVER (... ROWS UNBOUNDED PRECEDING)
grp_window = Window.partitionBy(*partition_cols).orderBy(order_col, "PrevEnd").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

rules_with_grp = rules_with_is_start.withColumn(
    "Grp",
    F.sum("IsStart").over(grp_window)
)

# Step 4: Group by Grp and aggregate
# MIN(ValidFrom), MAX(ValidTo) with NULL handling, MAX(ExtSourceID)
consolidated_rules = rules_with_grp.groupBy(
    "ProjectID", "WorkerID", "CrewID", "ZoneID", "FloorID", "LocationGroupID", "Grp"
).agg(
    F.min("ValidFrom_ShiftLocalDate").alias("ValidFrom_ShiftLocalDate"),
    # Convert '2100-01-01' back to NULL for ValidTo
    F.when(
        F.max("ValidTo_Coalesced") == F.to_date(F.lit("2100-01-01")),
        F.lit(None)
    ).otherwise(F.max("ValidTo_Coalesced")).alias("ValidTo_ShiftLocalDate"),
    F.max("ExtSourceID").alias("ExtSourceID")
)

# Drop the Grp column
consolidated_rules = consolidated_rules.drop("Grp")

consolidated_rules = consolidated_rules.cache()
consolidated_count = consolidated_rules.count()
print(f"Consolidated rules (merge source): {consolidated_count:,} rows")

# Show sample
print("\nSample consolidated rules:")
display(consolidated_rules.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Target Table if Not Exists

# COMMAND ----------

# Check if target table exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

if not target_exists:
    print(f"Creating target table: {TARGET_TABLE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            WorkerLocationAssignmentID BIGINT GENERATED ALWAYS AS IDENTITY,
            ProjectID INT,
            CrewID INT,
            WorkerID INT,
            ValidFrom_ShiftLocalDate DATE,
            ValidTo_ShiftLocalDate DATE,
            ZoneID INT,
            FloorID INT,
            LocationGroupID INT,
            ExtSourceID INT,
            WatermarkUTC TIMESTAMP,
            CutoffDateHint DATE,
            DeleteFlag INT DEFAULT 0
        )
        USING DELTA
        PARTITIONED BY (ProjectID)
    """)
    print(f"  Target table created")
else:
    print(f"Target table exists: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Two-Phase MERGE
# MAGIC
# MAGIC **Phase A:** Standard MERGE
# MAGIC - WHEN MATCHED AND (ValidTo changed) -> UPDATE
# MAGIC - WHEN NOT MATCHED BY TARGET -> INSERT
# MAGIC
# MAGIC **Phase B:** Soft Delete (NOT MATCHED BY SOURCE)
# MAGIC - Records in target but not in source with same ExtSourceIDAlias -> SET DeleteFlag=1

# COMMAND ----------

print("=" * 60)
print("STEP 7: Two-Phase MERGE")
print("=" * 60)

# Add computed columns for merge
merge_source = consolidated_rules.withColumn("WatermarkUTC", F.current_timestamp())
merge_source = merge_source.withColumn("CutoffDateHint", F.col("ValidFrom_ShiftLocalDate"))

# Create temp view for merge
merge_source.createOrReplaceTempView("merge_source_view")

# PHASE A: MERGE (UPDATE + INSERT)
print("\nPhase A: MERGE (UPDATE + INSERT)...")

merge_sql_phase_a = f"""
    MERGE INTO {TARGET_TABLE} AS t
    USING merge_source_view AS s
    ON (
        s.ValidFrom_ShiftLocalDate = t.ValidFrom_ShiftLocalDate AND
        (s.WorkerID = t.WorkerID OR s.CrewID = t.CrewID) AND
        s.ProjectID = t.ProjectID AND
        COALESCE(s.ZoneID, -1) = COALESCE(t.ZoneID, -1) AND
        COALESCE(s.FloorID, -1) = COALESCE(t.FloorID, -1) AND
        COALESCE(s.LocationGroupID, -1) = COALESCE(t.LocationGroupID, -1)
    )
    WHEN MATCHED AND (
        (s.ValidTo_ShiftLocalDate IS DISTINCT FROM t.ValidTo_ShiftLocalDate)
    )
    THEN UPDATE SET
        t.ValidTo_ShiftLocalDate = s.ValidTo_ShiftLocalDate,
        t.ExtSourceID = s.ExtSourceID,
        t.WatermarkUTC = current_timestamp(),
        t.CutoffDateHint = CASE
            WHEN s.ValidFrom_ShiftLocalDate <> t.ValidFrom_ShiftLocalDate
            THEN s.ValidFrom_ShiftLocalDate
            ELSE s.ValidTo_ShiftLocalDate
        END
    WHEN NOT MATCHED THEN INSERT (
        ProjectID,
        CrewID,
        WorkerID,
        ValidFrom_ShiftLocalDate,
        ValidTo_ShiftLocalDate,
        ZoneID,
        FloorID,
        LocationGroupID,
        ExtSourceID,
        CutoffDateHint,
        WatermarkUTC
    )
    VALUES (
        s.ProjectID,
        s.CrewID,
        s.WorkerID,
        s.ValidFrom_ShiftLocalDate,
        s.ValidTo_ShiftLocalDate,
        s.ZoneID,
        s.FloorID,
        s.LocationGroupID,
        s.ExtSourceID,
        s.ValidFrom_ShiftLocalDate,
        current_timestamp()
    )
"""

spark.sql(merge_sql_phase_a)
print("  Phase A completed")

# COMMAND ----------

# PHASE B: Soft Delete (NOT MATCHED BY SOURCE)
# Mark records for deletion that are in target but not in source
# Only for records with matching ExtSourceIDAlias
print("\nPhase B: Soft Delete (NOT MATCHED BY SOURCE)...")

# Get source keys
source_keys = merge_source.select(
    "ProjectID", "WorkerID", "CrewID", "ValidFrom_ShiftLocalDate",
    "ZoneID", "FloorID", "LocationGroupID"
).distinct()
source_keys.createOrReplaceTempView("source_keys_view")

# Mark for deletion: target records not in source with same ExtSourceIDAlias
# ExtSourceIDAlias(15) = 15, ExtSourceIDAlias(12) = 2
# We process both 15 and 12 sources, so alias 15 and alias 2 records should be checked

soft_delete_sql = f"""
    UPDATE {TARGET_TABLE} AS t
    SET
        t.DeleteFlag = 1,
        t.WatermarkUTC = current_timestamp()
    WHERE
        -- Match ExtSourceIDAlias = 15 (covers 14-19)
        CASE
            WHEN t.ExtSourceID IN (14, 15, 16, 17, 18, 19) THEN 15
            WHEN t.ExtSourceID IN (1, 2, 11, 12) THEN 2
            ELSE t.ExtSourceID
        END = 15
        AND NOT EXISTS (
            SELECT 1 FROM source_keys_view AS s
            WHERE
                s.ValidFrom_ShiftLocalDate = t.ValidFrom_ShiftLocalDate AND
                (s.WorkerID = t.WorkerID OR s.CrewID = t.CrewID) AND
                s.ProjectID = t.ProjectID AND
                COALESCE(s.ZoneID, -1) = COALESCE(t.ZoneID, -1) AND
                COALESCE(s.FloorID, -1) = COALESCE(t.FloorID, -1) AND
                COALESCE(s.LocationGroupID, -1) = COALESCE(t.LocationGroupID, -1)
        )
"""

spark.sql(soft_delete_sql)
print("  Phase B completed")

# Clean up temp views
spark.catalog.dropTempView("merge_source_view")
spark.catalog.dropTempView("source_keys_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Post-MERGE Operations
# MAGIC
# MAGIC Implements the #pre_deletes pattern:
# MAGIC 1. Build pre_deletes: records preceding deletions
# MAGIC 2. Update WatermarkUTC and CutoffDateHint for preceding records
# MAGIC 3. Delete records where DeleteFlag=1

# COMMAND ----------

print("=" * 60)
print("STEP 8: Post-MERGE Operations")
print("=" * 60)

# Step 8a: Build #pre_deletes equivalent
# Find records that precede a deletion (DeleteFlag=1)
print("\nBuilding pre_deletes...")

pre_deletes_df = spark.sql(f"""
    SELECT
        ProjectID,
        CrewID,
        WorkerID,
        ValidFrom_ShiftLocalDate,
        DeleteFlag,
        LEAD(ValidFrom_ShiftLocalDate, 1) OVER (
            PARTITION BY ProjectID, CrewID, WorkerID
            ORDER BY ValidFrom_ShiftLocalDate ASC
        ) as DateHint,
        LEAD(DeleteFlag, 1) OVER (
            PARTITION BY ProjectID, CrewID, WorkerID
            ORDER BY ValidFrom_ShiftLocalDate ASC
        ) as NextDeleteFlag
    FROM (
        SELECT
            ProjectID,
            CrewID,
            WorkerID,
            ValidFrom_ShiftLocalDate,
            MAX(CAST(COALESCE(DeleteFlag, 0) as INT)) as DeleteFlag
        FROM {TARGET_TABLE}
        GROUP BY ProjectID, CrewID, WorkerID, ValidFrom_ShiftLocalDate
    ) grouped
""")

# Filter to records where next record has DeleteFlag=1
pre_deletes = pre_deletes_df.filter(F.col("NextDeleteFlag") == 1)
pre_deletes_count = pre_deletes.count()
print(f"  Pre-deletes found: {pre_deletes_count:,}")

# Step 8b: Update preceding records' WatermarkUTC and CutoffDateHint
if pre_deletes_count > 0:
    print("\nUpdating preceding records...")

    pre_deletes.createOrReplaceTempView("pre_deletes_view")

    update_preceding_sql = f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING pre_deletes_view AS x
        ON (
            t.ProjectID = x.ProjectID AND
            (t.WorkerID = x.WorkerID OR t.CrewID = x.CrewID) AND
            t.ValidFrom_ShiftLocalDate = x.ValidFrom_ShiftLocalDate
        )
        WHEN MATCHED THEN UPDATE SET
            t.WatermarkUTC = current_timestamp(),
            t.CutoffDateHint = x.DateHint
    """

    spark.sql(update_preceding_sql)
    spark.catalog.dropTempView("pre_deletes_view")
    print(f"  Updated {pre_deletes_count} preceding records")

# Step 8c: Delete records where DeleteFlag=1
print("\nDeleting soft-deleted records...")

delete_count = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM {TARGET_TABLE} WHERE DeleteFlag = 1
""").collect()[0]["cnt"]

if delete_count > 0:
    spark.sql(f"DELETE FROM {TARGET_TABLE} WHERE DeleteFlag = 1")
    print(f"  Deleted {delete_count} records")
else:
    print("  No records to delete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Update Watermark

# COMMAND ----------

# Get max timestamp from processed data
max_loaded_at = datetime.now()

final_count = spark.table(TARGET_TABLE).count()

# Prepare metrics
metrics = {
    'etqr_rules': etqr_rules_count,
    'manual_rules': manual_rules_count,
    'consolidated': consolidated_count,
    'deleted': delete_count if 'delete_count' in dir() else 0
}

update_watermark("gold_worker_location_assignments", max_loaded_at, final_count, metrics)
print(f"Updated watermark to: {max_loaded_at}")
print(f"Total records in target table: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 60)
print("VALIDATION")
print("=" * 60)

# Row counts by ExtSourceID
print("\nRow Counts by ExtSourceID:")
display(spark.sql(f"""
    SELECT
        ExtSourceID,
        CASE
            WHEN ExtSourceID IN (14, 15, 16, 17, 18, 19) THEN 15
            WHEN ExtSourceID IN (1, 2, 11, 12) THEN 2
            ELSE ExtSourceID
        END as ExtSourceIDAlias,
        COUNT(*) as cnt
    FROM {TARGET_TABLE}
    GROUP BY ExtSourceID
    ORDER BY ExtSourceID
"""))

# Check for duplicates on business key
print("\nDuplicate Check (business key):")
dup_check = spark.sql(f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT CONCAT_WS('|',
            COALESCE(CAST(ProjectID AS STRING), ''),
            COALESCE(CAST(WorkerID AS STRING), ''),
            COALESCE(CAST(CrewID AS STRING), ''),
            CAST(ValidFrom_ShiftLocalDate AS STRING),
            COALESCE(CAST(ZoneID AS STRING), ''),
            COALESCE(CAST(FloorID AS STRING), ''),
            COALESCE(CAST(LocationGroupID AS STRING), '')
        )) as distinct_keys
    FROM {TARGET_TABLE}
""")
display(dup_check)

# Check data quality
print("\nData Quality Check:")
quality_check = spark.sql(f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN ProjectID IS NULL THEN 1 ELSE 0 END) as null_project,
        SUM(CASE WHEN WorkerID IS NULL AND CrewID IS NULL THEN 1 ELSE 0 END) as null_worker_and_crew,
        SUM(CASE WHEN ZoneID IS NULL AND FloorID IS NULL AND LocationGroupID IS NULL THEN 1 ELSE 0 END) as null_all_locations,
        SUM(CASE WHEN ValidFrom_ShiftLocalDate IS NULL THEN 1 ELSE 0 END) as null_valid_from
    FROM {TARGET_TABLE}
""")
display(quality_check)

# Sample output
print("\nSample Output:")
display(spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY ProjectID, WorkerID, ValidFrom_ShiftLocalDate LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Clean up cached DataFrames
rules_df.unpersist()
consolidated_rules.unpersist()

# Final summary
print("=" * 60)
print("WORKER LOCATION ASSIGNMENTS - COMPLETE")
print("=" * 60)
print(f"""
Processing Summary:
  eTQR Assignments (ExtSourceID=12):     {etqr_rules_count:,}
  Manual Assignments (ExtSourceID=15):   {manual_rules_count:,}
  Consolidated rules:                    {consolidated_count:,}
  Records deleted:                       {delete_count if 'delete_count' in dir() else 0:,}

Target Table:
  {TARGET_TABLE}
  Total rows: {final_count:,}

Converted from: stg.spCalculateWorkerLocationAssignments (290 lines)
""")

dbutils.notebook.exit("SUCCESS")
