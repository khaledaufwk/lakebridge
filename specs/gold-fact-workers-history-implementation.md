# Plan: Gold Layer FactWorkersHistory Implementation

## Task Description

Create a comprehensive Gold layer implementation for FactWorkersHistory in Databricks, converting the SQL Server stored procedures to PySpark. This plan contains complete, buildable code for all components.

## Objective

When complete, the Gold layer will provide:
1. **gold_fact_workers_history** - Enriched worker location history with computed fields
2. **gold_fact_workers_shifts** - Aggregated shift-level metrics derived from raw location data
3. Supporting UDFs for time category calculations and spatial operations

---

## Source to Target Column Mapping

### Silver → Gold Fact Workers History

| Silver Column (timescale_devicelocation) | Gold Column | Transformation |
|------------------------------------------|-------------|----------------|
| Id | DeviceLocationId | Direct map |
| GeneratedAt | TimestampUTC | Direct map |
| ProjectId | ProjectId | Direct map |
| DeviceId | DeviceId | Join to get WorkerId |
| SpaceId | FloorId | Direct map |
| ActiveSequance | Sequence | Direct map |
| InactiveSequance | SequenceInactive | Direct map |
| ActiveTime | ActiveTime | Direct map (in days: seconds/86400) |
| InActiveTime | InactiveTime | Direct map (in days: seconds/86400) |
| IsActive | - | Used for calculations only |
| PointWKT | Latitude, Longitude | Parse from WKT |
| CreatedAt | CreatedAt | Direct map |
| - | LocalDate | Computed: from_utc_timestamp → to_date |
| - | ShiftLocalDate | Computed: nearest neighbor algorithm |
| - | WorkerId | Lookup from DeviceAssignments |
| - | ZoneId | Spatial lookup or source |
| - | CrewId | Lookup from CrewAssignments |
| - | WorkshiftId | Lookup from WorkshiftAssignments |
| - | WorkshiftDetailsId | Lookup from WorkshiftDetails |
| - | TimeCategoryId | Computed: 1=During, 2=NoShift, 3=Break, 4=After, 5=Before |
| - | LocationAssignmentClassId | Computed: 1=In assigned area, NULL=Not |
| - | ProductiveClassId | Lookup from Zone (1=Direct, 2=Indirect) |
| - | ErrorDistance | From source or default |
| - | ExtSourceId | Constant: 17 (TimescaleDB) |

---

## Files to Create

### Directory Structure

```
migration_project/
├── pipelines/
│   └── gold/
│       ├── notebooks/
│       │   ├── gold_fact_workers_history.py
│       │   └── gold_fact_workers_shifts.py
│       ├── udfs/
│       │   └── time_category_udf.py
│       └── config/
│           └── gold_tables.yml
└── deploy_gold_layer.py
```

---

## Step by Step Tasks

Execute every step in order, top to bottom.

### Task 1: Create Gold Layer Schema and Watermark Table

**File:** Execute in Databricks SQL or notebook

```sql
-- Create Gold schema
CREATE SCHEMA IF NOT EXISTS wakecap_prod.gold;

-- Create watermark tracking table
CREATE TABLE IF NOT EXISTS wakecap_prod.migration._gold_watermarks (
    table_name STRING NOT NULL,
    last_processed_at TIMESTAMP,
    last_watermark_value TIMESTAMP,
    row_count BIGINT,
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    PRIMARY KEY (table_name)
) USING DELTA;

-- Initialize watermarks
INSERT INTO wakecap_prod.migration._gold_watermarks (table_name, last_processed_at, last_watermark_value)
VALUES
    ('gold_fact_workers_history', NULL, '1900-01-01'),
    ('gold_fact_workers_shifts', NULL, '1900-01-01')
ON CONFLICT (table_name) DO NOTHING;
```

---

### Task 2: Create Time Category UDF

**File:** `migration_project/pipelines/gold/udfs/time_category_udf.py`

This UDF replaces `stg.fnCalcTimeCategory` from SQL Server.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Time Category UDF
# MAGIC
# MAGIC Calculates the time category for a reading relative to scheduled shift.
# MAGIC
# MAGIC **Time Categories:**
# MAGIC - 1 = During Shift
# MAGIC - 2 = No Shift Defined
# MAGIC - 3 = During Break
# MAGIC - 4 = After Shift
# MAGIC - 5 = Before Shift
# MAGIC
# MAGIC **Original:** `stg.fnCalcTimeCategory` (54 lines)

# COMMAND ----------

from pyspark.sql.functions import udf, col, when, lit
from pyspark.sql.types import IntegerType, StringType, TimestampType
from datetime import datetime, timedelta, time

# COMMAND ----------

def parse_breaks(breaks_str):
    """
    Parse breaks string like '23:00-01:00;04:00-05:00' into list of (start, end) tuples.
    """
    if not breaks_str:
        return []

    breaks = []
    for break_period in breaks_str.split(';'):
        if '-' not in break_period:
            continue
        parts = break_period.strip().split('-')
        if len(parts) == 2:
            try:
                start_time = datetime.strptime(parts[0].strip(), '%H:%M').time()
                end_time = datetime.strptime(parts[1].strip(), '%H:%M').time()
                breaks.append((start_time, end_time))
            except ValueError:
                continue
    return breaks


def is_time_in_break(local_time, breaks):
    """
    Check if local_time falls within any break period.
    Handles overnight breaks (e.g., 23:00-01:00).
    """
    if not breaks or not local_time:
        return False

    for break_start, break_end in breaks:
        if break_start < break_end:
            # Normal break (e.g., 12:00-13:00)
            if break_start <= local_time <= break_end:
                return True
        else:
            # Overnight break (e.g., 23:00-01:00)
            if local_time >= break_start or local_time <= break_end:
                return True
    return False


def calc_time_category_impl(ws_start_time, ws_end_time, breaks_str, shift_local_date, local_datetime):
    """
    Calculate time category based on shift schedule.

    Parameters:
    - ws_start_time: Shift start time (time object or None)
    - ws_end_time: Shift end time (time object or None)
    - breaks_str: Break periods string like '23:00-01:00;04:00-05:00'
    - shift_local_date: The shift date (date object)
    - local_datetime: The timestamp to categorize (datetime object)

    Returns:
    - 1: During Shift
    - 2: No Shift Defined
    - 3: During Break
    - 4: After Shift
    - 5: Before Shift
    """
    # No shift defined
    if ws_start_time is None or ws_end_time is None:
        return 2

    if shift_local_date is None or local_datetime is None:
        return 2

    # Convert to datetime for comparison
    if isinstance(shift_local_date, datetime):
        base_date = shift_local_date.date()
    else:
        base_date = shift_local_date

    # Create shift start datetime
    ws_start_datetime = datetime.combine(base_date, ws_start_time)

    # Create shift end datetime (handle overnight shifts)
    if ws_start_time > ws_end_time:
        # Overnight shift - end is next day
        ws_end_datetime = datetime.combine(base_date + timedelta(days=1), ws_end_time)
    else:
        ws_end_datetime = datetime.combine(base_date, ws_end_time)

    # Compare local_datetime with shift boundaries
    if local_datetime < ws_start_datetime:
        return 5  # Before Shift
    elif local_datetime > ws_end_datetime:
        return 4  # After Shift
    else:
        # During shift time - check if in break
        breaks = parse_breaks(breaks_str)
        local_time = local_datetime.time() if isinstance(local_datetime, datetime) else local_datetime

        if is_time_in_break(local_time, breaks):
            return 3  # During Break
        return 1  # During Shift


# Register as Spark UDF
@udf(returnType=IntegerType())
def calc_time_category_udf(ws_start_time, ws_end_time, breaks_str, shift_local_date, local_datetime):
    """Spark UDF wrapper for calc_time_category_impl."""
    try:
        return calc_time_category_impl(ws_start_time, ws_end_time, breaks_str, shift_local_date, local_datetime)
    except Exception:
        return 2  # Default to "No Shift Defined" on error

# COMMAND ----------

# Alternative: SQL CASE expression for inline use (no UDF overhead)
# Use this pattern when UDF performance is a concern

def get_time_category_sql_expr():
    """
    Returns SQL expression for time category calculation.
    Use with: df.withColumn("TimeCategoryId", expr(get_time_category_sql_expr()))

    Note: This simplified version doesn't handle breaks. Use UDF for full logic.
    """
    return """
    CASE
        WHEN StartTime IS NULL OR EndTime IS NULL THEN 2
        WHEN LocalTimestamp <
             CAST(CONCAT(CAST(ShiftLocalDate AS STRING), ' ', CAST(StartTime AS STRING)) AS TIMESTAMP)
        THEN 5
        WHEN LocalTimestamp >
             CASE
                WHEN StartTime > EndTime
                THEN CAST(CONCAT(CAST(DATE_ADD(ShiftLocalDate, 1) AS STRING), ' ', CAST(EndTime AS STRING)) AS TIMESTAMP)
                ELSE CAST(CONCAT(CAST(ShiftLocalDate AS STRING), ' ', CAST(EndTime AS STRING)) AS TIMESTAMP)
             END
        THEN 4
        ELSE 1
    END
    """
```

---

### Task 3: Create Gold Fact Workers History Notebook

**File:** `migration_project/pipelines/gold/notebooks/gold_fact_workers_history.py`

This is the main transformation notebook. It converts `stg.spDeltaSyncFactWorkersHistory` (783 lines) and merges the logic from `spWorkersHistory_UpdateAssignments_*` procedures.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Workers History
# MAGIC
# MAGIC **Converts:** `stg.spDeltaSyncFactWorkersHistory` (783 lines)
# MAGIC **Merges:** `spWorkersHistory_UpdateAssignments_1_Crews`, `spWorkersHistory_UpdateAssignments_2_WorkShiftDates`, `spWorkersHistory_UpdateAssignments_3_LocationClass`
# MAGIC
# MAGIC **Purpose:** Transform Silver layer DeviceLocation into enriched FactWorkersHistory with:
# MAGIC - Shift date assignment (which shift does this reading belong to)
# MAGIC - Time category classification (before/during/after shift)
# MAGIC - Location assignment detection (is worker in assigned zone)
# MAGIC - Productive area classification (direct/indirect productive)
# MAGIC - Crew assignment resolution
# MAGIC
# MAGIC **Source:** `wakecap_prod.silver.silver_fact_workers_history` (82M rows from timescale_devicelocation)
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_workers_history`

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
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.gold_fact_workers_history"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "7", "Lookback Days for Incremental")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")

load_mode = dbutils.widgets.get("load_mode")
lookback_days = int(dbutils.widgets.get("lookback_days"))
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {load_mode}")
print(f"Lookback Days: {lookback_days}")
print(f"Project Filter: {project_filter if project_filter else 'None'}")

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
# MAGIC ## Step 1: Load Source Data
# MAGIC
# MAGIC Source is `silver_fact_workers_history` which contains DeviceLocation data.
# MAGIC For incremental mode, filter by `_loaded_at` watermark.

# COMMAND ----------

# Load source fact table
source_table = f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_fact_workers_history"

if load_mode == "incremental":
    last_watermark = get_watermark("gold_fact_workers_history")
    cutoff_date = datetime.now() - timedelta(days=lookback_days)

    source_df = (
        spark.table(source_table)
        .filter(
            (F.col("_loaded_at") > last_watermark) |
            (F.col("GeneratedAt") >= cutoff_date)
        )
    )
    print(f"Incremental load from watermark: {last_watermark}")
else:
    source_df = spark.table(source_table)
    print("Full load - processing all records")

# Apply optional project filter
if project_filter:
    source_df = source_df.filter(F.col("ProjectId") == int(project_filter))

source_count = source_df.count()
print(f"Source records to process: {source_count:,}")

if source_count == 0:
    print("No records to process. Exiting.")
    dbutils.notebook.exit("No records to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Dimension Tables for Enrichment

# COMMAND ----------

# Projects - for timezone conversion
projects = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_project")
    .select(
        F.col("ProjectId"),
        F.col("TimeZoneName"),
        F.col("ZoneBufferDistance").alias("ProjectZoneBufferDistance")
    )
)

# Device Assignments - to resolve DeviceId → WorkerId
device_assignments = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_device_assignment")
    .select(
        "DeviceId",
        "WorkerId",
        "ProjectId",
        F.col("ValidFrom").alias("DA_ValidFrom"),
        F.col("ValidTo").alias("DA_ValidTo")
    )
)

# Workers - for WorkerId details
workers = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_worker")
    .select("WorkerId", "ProjectId", "CompanyId")
)

# Zones - for ProductiveClassId lookup
zones = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_zone")
    .filter(F.col("DeletedAt").isNull())
    .select(
        "ZoneId",
        "FloorId",
        "ProjectId",
        "ProductiveClassId",
        "Coordinates"
    )
)

# Floors
floors = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_floor")
    .select("FloorId", "ProjectId")
)

# Workshift Assignments
workshift_assignments = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_workshift_assignment")
    .select(
        "ProjectId",
        "WorkerId",
        "WorkshiftId",
        F.col("ValidFrom").alias("WSA_ValidFrom"),
        F.col("ValidTo").alias("WSA_ValidTo")
    )
)

# Workshift Details (for schedule info)
workshift_details = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_workshift_day")
    .select(
        "WorkshiftId",
        "WorkshiftDetailsId",
        "DayOfWeek",
        "Date",
        "StartTime",
        "EndTime",
        "Breaks"
    )
)

# Crew Assignments
crew_assignments = (
    spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_crew_assignment")
    .select(
        "ProjectId",
        "WorkerId",
        "CrewId",
        F.col("ValidFrom").alias("CA_ValidFrom"),
        F.col("ValidTo").alias("CA_ValidTo")
    )
)

print("Dimension tables loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse WKT to Latitude/Longitude and Convert Timestamps

# COMMAND ----------

# Parse PointWKT to extract Latitude and Longitude
# Format: "POINT (longitude latitude)" or "POINT(longitude latitude)"
step3 = (
    source_df
    .withColumn(
        "PointWKT_Clean",
        F.regexp_replace(F.col("PointWKT"), "POINT\\s*\\(", "")
    )
    .withColumn(
        "PointWKT_Clean",
        F.regexp_replace(F.col("PointWKT_Clean"), "\\)", "")
    )
    .withColumn(
        "Longitude",
        F.split(F.trim(F.col("PointWKT_Clean")), "\\s+").getItem(0).cast("double")
    )
    .withColumn(
        "Latitude",
        F.split(F.trim(F.col("PointWKT_Clean")), "\\s+").getItem(1).cast("double")
    )
    .drop("PointWKT_Clean")
)

# Join with Projects to get timezone
step3 = (
    step3
    .join(F.broadcast(projects), "ProjectId", "left")
)

# Convert UTC to Local timezone
step3 = (
    step3
    .withColumn(
        "LocalTimestamp",
        F.from_utc_timestamp(F.col("GeneratedAt"), F.coalesce(F.col("TimeZoneName"), F.lit("UTC")))
    )
    .withColumn("LocalDate", F.to_date(F.col("LocalTimestamp")))
)

print(f"Step 3 complete - parsed coordinates and converted timezones")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Resolve DeviceId → WorkerId via DeviceAssignments

# COMMAND ----------

# Join with DeviceAssignments to get WorkerId at time of reading
step4 = (
    step3
    .join(
        device_assignments,
        (step3.DeviceId == device_assignments.DeviceId) &
        (step3.ProjectId == device_assignments.ProjectId) &
        (step3.GeneratedAt >= device_assignments.DA_ValidFrom) &
        ((step3.GeneratedAt < device_assignments.DA_ValidTo) | device_assignments.DA_ValidTo.isNull()),
        "left"
    )
    .drop(device_assignments.DeviceId)
    .drop(device_assignments.ProjectId)
)

# Filter out records without worker assignment (device not assigned)
records_with_worker = step4.filter(F.col("WorkerId").isNotNull())
records_without_worker = step4.filter(F.col("WorkerId").isNull()).count()

print(f"Records with worker assignment: {records_with_worker.count():,}")
print(f"Records without worker (dropped): {records_without_worker:,}")

step4 = records_with_worker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Determine ShiftLocalDate
# MAGIC
# MAGIC For readings that fall within an existing shift period, use that shift's date.
# MAGIC For others, default to LocalDate (simplified from original nearest-neighbor algorithm).

# COMMAND ----------

# Check if gold_fact_workers_shifts exists for shift matching
try:
    existing_shifts = (
        spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.gold_fact_workers_shifts")
        .select("ProjectId", "WorkerId", "ShiftLocalDate", "StartAtUTC", "FinishAtUTC")
    )
    has_existing_shifts = True
    print("Found existing shifts table for matching")
except:
    has_existing_shifts = False
    print("No existing shifts table - using LocalDate as ShiftLocalDate")

if has_existing_shifts:
    # Join to find readings within existing shift boundaries
    step5 = (
        step4
        .join(
            existing_shifts,
            (step4.ProjectId == existing_shifts.ProjectId) &
            (step4.WorkerId == existing_shifts.WorkerId) &
            (step4.GeneratedAt >= existing_shifts.StartAtUTC) &
            (step4.GeneratedAt <= existing_shifts.FinishAtUTC),
            "left"
        )
        .withColumn(
            "ShiftLocalDate",
            F.coalesce(existing_shifts.ShiftLocalDate, F.col("LocalDate"))
        )
        .drop(existing_shifts.ProjectId)
        .drop(existing_shifts.WorkerId)
        .drop("StartAtUTC", "FinishAtUTC")
    )
else:
    step5 = step4.withColumn("ShiftLocalDate", F.col("LocalDate"))

print("Step 5 complete - ShiftLocalDate assigned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Resolve Crew Assignment
# MAGIC
# MAGIC Look up the crew assignment valid for each worker at the ShiftLocalDate.

# COMMAND ----------

step6 = (
    step5
    .join(
        crew_assignments.alias("ca"),
        (step5.ProjectId == F.col("ca.ProjectId")) &
        (step5.WorkerId == F.col("ca.WorkerId")) &
        (step5.ShiftLocalDate >= F.col("ca.CA_ValidFrom")) &
        ((step5.ShiftLocalDate < F.col("ca.CA_ValidTo")) | F.col("ca.CA_ValidTo").isNull()),
        "left"
    )
    .drop(F.col("ca.ProjectId"))
    .drop(F.col("ca.WorkerId"))
    .drop("CA_ValidFrom", "CA_ValidTo")
)

print("Step 6 complete - Crew assignments resolved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Resolve Workshift Assignment and Details

# COMMAND ----------

# Get workshift assignment
step7 = (
    step6
    .join(
        workshift_assignments.alias("wsa"),
        (step6.ProjectId == F.col("wsa.ProjectId")) &
        (step6.WorkerId == F.col("wsa.WorkerId")) &
        (step6.ShiftLocalDate >= F.col("wsa.WSA_ValidFrom")) &
        ((step6.ShiftLocalDate < F.col("wsa.WSA_ValidTo")) | F.col("wsa.WSA_ValidTo").isNull()),
        "left"
    )
    .drop(F.col("wsa.ProjectId"))
    .drop(F.col("wsa.WorkerId"))
    .drop("WSA_ValidFrom", "WSA_ValidTo")
)

# Get workshift details for the shift date
# First try special date match, then day of week match
step7 = (
    step7
    .withColumn("ShiftDayOfWeek", (F.dayofweek(F.col("ShiftLocalDate")) + 5) % 7)  # Convert to 0=Monday
)

# Join with workshift details - special date first
workshift_details_special = (
    workshift_details
    .filter(F.col("Date").isNotNull())
    .select(
        "WorkshiftId",
        "WorkshiftDetailsId",
        "Date",
        "StartTime",
        "EndTime",
        "Breaks"
    )
)

workshift_details_dow = (
    workshift_details
    .filter(F.col("DayOfWeek").isNotNull())
    .select(
        "WorkshiftId",
        F.col("WorkshiftDetailsId").alias("WSD_DOW_Id"),
        "DayOfWeek",
        F.col("StartTime").alias("StartTime_DOW"),
        F.col("EndTime").alias("EndTime_DOW"),
        F.col("Breaks").alias("Breaks_DOW")
    )
)

# Join special date
step7 = (
    step7
    .join(
        workshift_details_special.alias("wsd_special"),
        (step7.WorkshiftId == F.col("wsd_special.WorkshiftId")) &
        (step7.ShiftLocalDate == F.col("wsd_special.Date")),
        "left"
    )
    .drop(F.col("wsd_special.WorkshiftId"))
)

# Join day of week
step7 = (
    step7
    .join(
        workshift_details_dow.alias("wsd_dow"),
        (step7.WorkshiftId == F.col("wsd_dow.WorkshiftId")) &
        (step7.ShiftDayOfWeek == F.col("wsd_dow.DayOfWeek")),
        "left"
    )
    .drop(F.col("wsd_dow.WorkshiftId"))
)

# Coalesce special date with day of week
step7 = (
    step7
    .withColumn("FinalWorkshiftDetailsId",
        F.coalesce(F.col("WorkshiftDetailsId"), F.col("WSD_DOW_Id")))
    .withColumn("FinalStartTime",
        F.coalesce(F.col("StartTime"), F.col("StartTime_DOW")))
    .withColumn("FinalEndTime",
        F.coalesce(F.col("EndTime"), F.col("EndTime_DOW")))
    .withColumn("FinalBreaks",
        F.coalesce(F.col("Breaks"), F.col("Breaks_DOW")))
    .drop("WorkshiftDetailsId", "WSD_DOW_Id", "StartTime", "EndTime", "Breaks",
          "StartTime_DOW", "EndTime_DOW", "Breaks_DOW", "Date", "DayOfWeek", "ShiftDayOfWeek")
)

print("Step 7 complete - Workshift details resolved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Calculate Time Category
# MAGIC
# MAGIC Using simplified SQL CASE logic (UDF available for full break handling).

# COMMAND ----------

# Calculate TimeCategoryId using SQL expression (simplified - no break handling)
step8 = (
    step7
    .withColumn(
        "ShiftStartDateTime",
        F.when(
            F.col("FinalStartTime").isNotNull(),
            F.concat(F.col("ShiftLocalDate").cast("string"), F.lit(" "), F.col("FinalStartTime").cast("string")).cast("timestamp")
        )
    )
    .withColumn(
        "ShiftEndDateTime",
        F.when(
            F.col("FinalEndTime").isNotNull(),
            F.when(
                F.col("FinalStartTime") > F.col("FinalEndTime"),
                # Overnight shift - end is next day
                F.concat(F.date_add(F.col("ShiftLocalDate"), 1).cast("string"), F.lit(" "), F.col("FinalEndTime").cast("string")).cast("timestamp")
            ).otherwise(
                F.concat(F.col("ShiftLocalDate").cast("string"), F.lit(" "), F.col("FinalEndTime").cast("string")).cast("timestamp")
            )
        )
    )
    .withColumn(
        "TimeCategoryId",
        F.when(
            F.col("FinalStartTime").isNull() | F.col("FinalEndTime").isNull(),
            F.lit(2)  # No shift defined
        ).when(
            F.col("LocalTimestamp") < F.col("ShiftStartDateTime"),
            F.lit(5)  # Before shift
        ).when(
            F.col("LocalTimestamp") > F.col("ShiftEndDateTime"),
            F.lit(4)  # After shift
        ).otherwise(
            F.lit(1)  # During shift (simplified - not checking breaks)
        )
    )
    .drop("ShiftStartDateTime", "ShiftEndDateTime")
)

print("Step 8 complete - Time category calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Lookup Zone and Productive Class
# MAGIC
# MAGIC Determine ProductiveClassId based on which zone the worker is in.
# MAGIC Simplified version - matches by FloorId only (spatial matching would require H3).

# COMMAND ----------

# Get the most specific zone for the floor
# In production, this would use H3 spatial matching
zone_lookup = (
    zones
    .groupBy("FloorId")
    .agg(
        F.min("ProductiveClassId").alias("FloorProductiveClassId")
    )
)

step9 = (
    step8
    .join(
        zone_lookup,
        step8.FloorId == zone_lookup.FloorId,
        "left"
    )
    .withColumn("ProductiveClassId", F.col("FloorProductiveClassId"))
    .drop("FloorProductiveClassId")
    .drop(zone_lookup.FloorId)
)

print("Step 9 complete - Productive class assigned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Calculate Location Assignment Class
# MAGIC
# MAGIC Simplified version - sets LocationAssignmentClassId to 1 if worker has any location assignment.
# MAGIC Full spatial matching would require H3 indexing.

# COMMAND ----------

# Check if worker has location assignment for the shift date
try:
    location_assignments = (
        spark.table(f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_worker_location_assignment")
        .select(
            "ProjectId",
            "WorkerId",
            "CrewId",
            F.col("ValidFrom_ShiftLocalDate").alias("LA_ValidFrom"),
            F.col("ValidTo_ShiftLocalDate").alias("LA_ValidTo")
        )
    )

    # Mark if worker has location assignment (simplified - not doing spatial check)
    step10 = (
        step9
        .join(
            location_assignments.alias("la"),
            (
                ((step9.ProjectId == F.col("la.ProjectId")) & (step9.WorkerId == F.col("la.WorkerId"))) |
                ((step9.ProjectId == F.col("la.ProjectId")) & (step9.CrewId == F.col("la.CrewId")))
            ) &
            (step9.ShiftLocalDate >= F.col("la.LA_ValidFrom")) &
            ((step9.ShiftLocalDate < F.col("la.LA_ValidTo")) | F.col("la.LA_ValidTo").isNull()),
            "left"
        )
        .withColumn(
            "LocationAssignmentClassId",
            F.when(F.col("la.ProjectId").isNotNull(), F.lit(1))
        )
        .drop(F.col("la.ProjectId"), F.col("la.WorkerId"), F.col("la.CrewId"), "LA_ValidFrom", "LA_ValidTo")
    )
except:
    # No location assignments table
    step10 = step9.withColumn("LocationAssignmentClassId", F.lit(None).cast("int"))
    print("No location assignments table found - LocationAssignmentClassId set to NULL")

print("Step 10 complete - Location assignment class calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Convert ActiveTime/InactiveTime to Days

# COMMAND ----------

# Original times are in seconds, convert to days (fraction)
# Formula: seconds / (60 * 60 * 24) = seconds / 86400
step11 = (
    step10
    .withColumn(
        "ActiveTimeDays",
        F.when(F.col("IsActive") == True, F.col("ActiveTime") / 86400.0)
    )
    .withColumn(
        "InactiveTimeDays",
        F.when(F.col("IsActive") == False, F.col("InactiveTime") / 86400.0)
    )
)

print("Step 11 complete - Time converted to days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Select Final Columns and Write

# COMMAND ----------

# Prepare final DataFrame with all columns
gold_fact_workers_history = (
    step11
    .select(
        F.col("DeviceLocationId").alias("WorkersHistoryId"),
        F.col("GeneratedAt").alias("TimestampUTC"),
        "LocalDate",
        "ShiftLocalDate",
        "WorkerId",
        "ProjectId",
        F.col("FloorId").alias("ZoneId"),  # Note: Using FloorId as ZoneId for now
        "FloorId",
        "CrewId",
        "WorkshiftId",
        F.col("FinalWorkshiftDetailsId").alias("WorkshiftDetailsId"),
        F.col("ActiveTimeDays").alias("ActiveTime"),
        F.col("InactiveTimeDays").alias("InactiveTime"),
        "Latitude",
        "Longitude",
        F.coalesce(F.col("ProjectZoneBufferDistance"), F.lit(0)).alias("ErrorDistance"),
        F.col("ActiveSequence").alias("Sequence"),
        F.col("InactiveSequence").alias("SequenceInactive"),
        "TimeCategoryId",
        "LocationAssignmentClassId",
        "ProductiveClassId",
        F.lit(17).alias("ExtSourceId"),  # 17 = TimescaleDB
        "CreatedAt",
        F.current_timestamp().alias("_processed_at"),
        F.col("_loaded_at")
    )
)

# Show sample
print("Sample output:")
gold_fact_workers_history.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Write to Delta Table with MERGE

# COMMAND ----------

# Check if target table exists
if spark.catalog.tableExists(TARGET_TABLE):
    print(f"Merging into existing table: {TARGET_TABLE}")

    delta_table = DeltaTable.forName(spark, TARGET_TABLE)

    (delta_table.alias("target")
     .merge(
         gold_fact_workers_history.alias("source"),
         "target.WorkersHistoryId = source.WorkersHistoryId"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )

    merge_count = gold_fact_workers_history.count()
    print(f"Merged {merge_count:,} records")
else:
    print(f"Creating new table: {TARGET_TABLE}")

    (gold_fact_workers_history
     .write
     .format("delta")
     .mode("overwrite")
     .partitionBy("ShiftLocalDate")
     .option("overwriteSchema", "true")
     .saveAsTable(TARGET_TABLE)
    )

    print(f"Created table with {gold_fact_workers_history.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Update Watermark

# COMMAND ----------

# Get max watermark from processed data
max_loaded_at = gold_fact_workers_history.agg(F.max("_loaded_at")).collect()[0][0]
final_count = spark.table(TARGET_TABLE).count()

update_watermark("gold_fact_workers_history", max_loaded_at, final_count)
print(f"Updated watermark to: {max_loaded_at}")
print(f"Total records in table: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Show summary statistics
summary = spark.sql(f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT WorkerId) as unique_workers,
        COUNT(DISTINCT ProjectId) as unique_projects,
        MIN(ShiftLocalDate) as min_date,
        MAX(ShiftLocalDate) as max_date,
        SUM(CASE WHEN TimeCategoryId = 1 THEN 1 ELSE 0 END) as during_shift,
        SUM(CASE WHEN TimeCategoryId = 2 THEN 1 ELSE 0 END) as no_shift,
        SUM(CASE WHEN TimeCategoryId = 4 THEN 1 ELSE 0 END) as after_shift,
        SUM(CASE WHEN TimeCategoryId = 5 THEN 1 ELSE 0 END) as before_shift,
        SUM(CASE WHEN LocationAssignmentClassId = 1 THEN 1 ELSE 0 END) as in_assigned_location
    FROM {TARGET_TABLE}
""")

display(summary)

# COMMAND ----------

print("Gold Fact Workers History pipeline complete!")
```

---

### Task 4: Create Gold Fact Workers Shifts Notebook

**File:** `migration_project/pipelines/gold/notebooks/gold_fact_workers_shifts.py`

This notebook converts `stg.spCalculateFactWorkersShifts` (821 lines).

**Note:** The existing `calc_fact_workers_shifts.py` (515 lines) already contains a complete implementation. Reference that file and ensure it writes to the correct Gold layer table.

Key modifications needed to existing file:
1. Change target table from `wakecap_prod.migration.fact_workers_shifts` to `wakecap_prod.gold.gold_fact_workers_shifts`
2. Change source from `bronze_dbo_FactWorkersHistory` to `wakecap_prod.gold.gold_fact_workers_history`
3. Add watermark tracking

The existing implementation handles:
- Island detection (>5 hour gaps create new sessions)
- Session splitting (sessions >24 hours)
- 40+ aggregation metrics (ActiveTime by category, ProductiveClass, LocationAssignment)
- Session stitching (<5 hours gap, <20 hours combined)
- Delta MERGE for upserts

---

### Task 5: Create Gold Tables Configuration

**File:** `migration_project/pipelines/gold/config/gold_tables.yml`

```yaml
# Gold Layer Table Configuration
# ================================
# Defines the Gold layer tables for WakeCapDW migration

catalog: wakecap_prod
schema: gold

tables:
  # =====================
  # FACT TABLES
  # =====================

  - name: gold_fact_workers_history
    source_table: silver_fact_workers_history
    description: "Enriched worker location history with computed fields"
    row_estimate: 82000000
    partition_by: [ShiftLocalDate]
    z_order_by: [ProjectId, WorkerId]
    primary_key: [WorkersHistoryId]

    columns:
      - name: WorkersHistoryId
        type: BIGINT
        source: DeviceLocationId
        description: "Unique identifier (from DeviceLocation.Id)"

      - name: TimestampUTC
        type: TIMESTAMP
        source: GeneratedAt
        description: "Reading timestamp in UTC"

      - name: LocalDate
        type: DATE
        computed: true
        description: "Local date based on project timezone"

      - name: ShiftLocalDate
        type: DATE
        computed: true
        description: "Assigned shift date (may differ from LocalDate)"

      - name: WorkerId
        type: INT
        lookup: DeviceAssignments
        description: "Worker ID from device assignment"

      - name: ProjectId
        type: INT
        source: ProjectId

      - name: ZoneId
        type: INT
        spatial_lookup: true
        description: "Zone containing the location"

      - name: FloorId
        type: INT
        source: SpaceId

      - name: CrewId
        type: INT
        lookup: CrewAssignments
        description: "Crew at shift date"

      - name: WorkshiftId
        type: INT
        lookup: WorkshiftAssignments

      - name: WorkshiftDetailsId
        type: INT
        lookup: WorkshiftDetails

      - name: ActiveTime
        type: DOUBLE
        description: "Active time in days (fraction)"

      - name: InactiveTime
        type: DOUBLE
        description: "Inactive time in days (fraction)"

      - name: Latitude
        type: DOUBLE
        parsed_from: PointWKT

      - name: Longitude
        type: DOUBLE
        parsed_from: PointWKT

      - name: ErrorDistance
        type: DOUBLE
        description: "Location error buffer in meters"

      - name: Sequence
        type: INT
        source: ActiveSequence

      - name: SequenceInactive
        type: INT
        source: InactiveSequence

      - name: TimeCategoryId
        type: INT
        computed: true
        description: "1=During, 2=NoShift, 3=Break, 4=After, 5=Before"

      - name: LocationAssignmentClassId
        type: INT
        computed: true
        description: "1=In assigned area, NULL=Not"

      - name: ProductiveClassId
        type: INT
        lookup: Zone
        description: "1=Direct, 2=Indirect"

      - name: ExtSourceId
        type: INT
        constant: 17
        description: "17=TimescaleDB"

      - name: CreatedAt
        type: TIMESTAMP
        source: CreatedAt

  - name: gold_fact_workers_shifts
    source_table: gold_fact_workers_history
    description: "Aggregated shift-level metrics per worker"
    row_estimate: 5000000
    partition_by: [ShiftLocalDate]
    z_order_by: [ProjectId, WorkerId]
    primary_key: [ProjectId, WorkerId, StartAtUTC]

    columns:
      - name: ProjectId
        type: INT

      - name: WorkerId
        type: INT

      - name: ShiftLocalDate
        type: DATE

      - name: StartAtUTC
        type: TIMESTAMP
        description: "Shift start (first active reading - ~4 min)"

      - name: FinishAtUTC
        type: TIMESTAMP
        description: "Shift end (last reading + ~3 min)"

      - name: ActiveTime
        type: DOUBLE
        aggregation: SUM

      - name: ActiveTimeBeforeShift
        type: DOUBLE
        aggregation: "SUM WHERE TimeCategoryId = 5"

      - name: ActiveTimeDuringShift
        type: DOUBLE
        aggregation: "SUM WHERE TimeCategoryId = 1"

      - name: ActiveTimeAfterShift
        type: DOUBLE
        aggregation: "SUM WHERE TimeCategoryId = 4"

      # ... 40+ additional metrics (see calc_fact_workers_shifts.py)

# Schedule Configuration
schedule:
  cron: "0 0 5 * * ?"  # 5:00 AM UTC daily
  timezone: UTC
  depends_on:
    - WakeCapDW_Silver_TimescaleDB  # Must complete first
```

---

### Task 6: Create Deployment Script

**File:** `migration_project/deploy_gold_layer.py`

```python
"""
Deploy Gold Layer Pipeline to Databricks

This script:
1. Uploads notebooks to workspace
2. Creates/updates Databricks job
3. Optionally triggers initial run
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, TaskDependency,
    JobCluster, ClusterSpec, JobSettings
)
from databricks.sdk.service.workspace import ImportFormat


def main():
    # Initialize Databricks client
    w = WorkspaceClient()

    # Configuration
    WORKSPACE_PATH = "/Workspace/migration_project/pipelines/gold"
    LOCAL_PATH = Path(__file__).parent / "pipelines" / "gold"
    JOB_NAME = "WakeCapDW_Gold_FactWorkersHistory"

    # Get existing cluster ID (or create job cluster)
    # Using existing cluster for development
    CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID")

    print(f"Deploying Gold Layer Pipeline")
    print(f"Workspace path: {WORKSPACE_PATH}")
    print(f"Cluster ID: {CLUSTER_ID}")

    # Step 1: Create workspace directories
    print("\n1. Creating workspace directories...")
    for subdir in ["notebooks", "udfs", "config"]:
        try:
            w.workspace.mkdirs(f"{WORKSPACE_PATH}/{subdir}")
            print(f"   Created: {WORKSPACE_PATH}/{subdir}")
        except Exception as e:
            print(f"   Already exists: {WORKSPACE_PATH}/{subdir}")

    # Step 2: Upload notebooks
    print("\n2. Uploading notebooks...")
    notebooks = [
        ("notebooks/gold_fact_workers_history.py", "gold_fact_workers_history"),
        ("notebooks/gold_fact_workers_shifts.py", "gold_fact_workers_shifts"),
        ("udfs/time_category_udf.py", "time_category_udf"),
    ]

    for local_file, notebook_name in notebooks:
        local_path = LOCAL_PATH / local_file
        if local_path.exists():
            with open(local_path, "r") as f:
                content = f.read()

            # Convert to Databricks notebook format if needed
            if not content.startswith("# Databricks notebook source"):
                content = "# Databricks notebook source\n" + content

            workspace_file = f"{WORKSPACE_PATH}/{local_file.replace('.py', '')}"
            try:
                w.workspace.import_(
                    path=workspace_file,
                    content=content.encode(),
                    format=ImportFormat.SOURCE,
                    overwrite=True
                )
                print(f"   Uploaded: {workspace_file}")
            except Exception as e:
                print(f"   Error uploading {notebook_name}: {e}")
        else:
            print(f"   Skipping (not found): {local_file}")

    # Step 3: Create or update job
    print("\n3. Creating/updating Databricks job...")

    # Define job cluster (for production)
    job_cluster = JobCluster(
        job_cluster_key="gold_cluster",
        new_cluster=ClusterSpec(
            spark_version="14.3.x-scala2.12",
            node_type_id="Standard_DS4_v2",
            num_workers=4,
            spark_conf={
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.sql.adaptive.enabled": "true"
            },
            custom_tags={
                "layer": "gold",
                "pipeline": "fact_workers_history"
            }
        )
    )

    # Define tasks
    tasks = [
        Task(
            task_key="gold_fact_workers_history",
            description="Transform Silver to Gold FactWorkersHistory",
            notebook_task=NotebookTask(
                notebook_path=f"{WORKSPACE_PATH}/notebooks/gold_fact_workers_history",
                base_parameters={
                    "load_mode": "incremental",
                    "lookback_days": "7"
                }
            ),
            existing_cluster_id=CLUSTER_ID,
            timeout_seconds=14400,  # 4 hours
            max_retries=1
        ),
        Task(
            task_key="gold_fact_workers_shifts",
            description="Calculate shift aggregates from FactWorkersHistory",
            depends_on=[TaskDependency(task_key="gold_fact_workers_history")],
            notebook_task=NotebookTask(
                notebook_path=f"{WORKSPACE_PATH}/notebooks/gold_fact_workers_shifts",
                base_parameters={
                    "load_mode": "incremental"
                }
            ),
            existing_cluster_id=CLUSTER_ID,
            timeout_seconds=7200,  # 2 hours
            max_retries=1
        )
    ]

    # Check if job exists
    existing_jobs = list(w.jobs.list(name=JOB_NAME))

    if existing_jobs:
        job_id = existing_jobs[0].job_id
        print(f"   Updating existing job: {job_id}")
        w.jobs.update(
            job_id=job_id,
            new_settings=JobSettings(
                name=JOB_NAME,
                tasks=tasks,
                schedule={
                    "quartz_cron_expression": "0 0 5 * * ?",  # 5:00 AM UTC
                    "timezone_id": "UTC"
                },
                max_concurrent_runs=1,
                email_notifications={
                    "on_failure": ["data-engineering@wakecap.com"]
                }
            )
        )
    else:
        print(f"   Creating new job: {JOB_NAME}")
        job = w.jobs.create(
            name=JOB_NAME,
            tasks=tasks,
            schedule={
                "quartz_cron_expression": "0 0 5 * * ?",
                "timezone_id": "UTC"
            },
            max_concurrent_runs=1
        )
        job_id = job.job_id

    print(f"   Job ID: {job_id}")

    # Step 4: Optionally trigger run
    if os.environ.get("TRIGGER_RUN", "false").lower() == "true":
        print("\n4. Triggering job run...")
        run = w.jobs.run_now(job_id=job_id)
        print(f"   Run ID: {run.run_id}")
        print(f"   Monitor at: {w.config.host}/#job/{job_id}/run/{run.run_id}")
    else:
        print("\n4. Skipping job trigger (set TRIGGER_RUN=true to run)")

    print("\nDeployment complete!")
    print(f"Job URL: {w.config.host}/#job/{job_id}")


if __name__ == "__main__":
    main()
```

---

## Validation Queries

Execute these after deployment to validate the pipeline:

```sql
-- 1. Check Gold tables created
SHOW TABLES IN wakecap_prod.gold;

-- 2. Row count comparison
SELECT 'silver' as layer, COUNT(*) as cnt
FROM wakecap_prod.silver.silver_fact_workers_history
UNION ALL
SELECT 'gold' as layer, COUNT(*) as cnt
FROM wakecap_prod.gold.gold_fact_workers_history;

-- 3. Column coverage validation
SELECT
    COUNT(*) as total,
    COUNT(ShiftLocalDate) as has_shift_date,
    COUNT(TimeCategoryId) as has_time_category,
    COUNT(LocationAssignmentClassId) as has_location_class,
    COUNT(ProductiveClassId) as has_productive_class,
    COUNT(CrewId) as has_crew,
    COUNT(WorkshiftId) as has_workshift
FROM wakecap_prod.gold.gold_fact_workers_history;

-- 4. Time category distribution
SELECT
    TimeCategoryId,
    CASE TimeCategoryId
        WHEN 1 THEN 'During Shift'
        WHEN 2 THEN 'No Shift'
        WHEN 3 THEN 'Break'
        WHEN 4 THEN 'After Shift'
        WHEN 5 THEN 'Before Shift'
    END as category_name,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM wakecap_prod.gold.gold_fact_workers_history
GROUP BY TimeCategoryId
ORDER BY TimeCategoryId;

-- 5. Shifts table validation
SELECT
    COUNT(*) as total_shifts,
    COUNT(DISTINCT WorkerId) as unique_workers,
    COUNT(DISTINCT ProjectId) as unique_projects,
    MIN(ShiftLocalDate) as min_date,
    MAX(ShiftLocalDate) as max_date,
    AVG(ActiveTime) as avg_active_time
FROM wakecap_prod.gold.gold_fact_workers_shifts;

-- 6. Watermark status
SELECT * FROM wakecap_prod.migration._gold_watermarks;

-- 7. Sample data check
SELECT *
FROM wakecap_prod.gold.gold_fact_workers_history
WHERE ShiftLocalDate >= CURRENT_DATE - INTERVAL 7 DAYS
LIMIT 100;
```

---

## Acceptance Criteria

| Criteria | Target | Validation |
|----------|--------|------------|
| Row count | >= 99% of Silver | Compare counts |
| ShiftLocalDate populated | 100% | COUNT(ShiftLocalDate) = COUNT(*) |
| TimeCategoryId populated | 100% | No NULLs |
| WorkerId populated | 100% | No NULLs (after filtering) |
| Incremental performance | < 30 min/day | Monitor job runs |
| Full load performance | < 4 hours | Initial load timing |
| No duplicate keys | 0 | COUNT(DISTINCT WorkersHistoryId) = COUNT(*) |

---

## Dependencies

### Required Libraries

Install on cluster:
- `h3` (optional, for spatial operations): `pip install h3`

### Cluster Configuration

```json
{
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_DS4_v2",
  "num_workers": 4,
  "spark_conf": {
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "200"
  }
}
```

### Schedule Dependencies

| Job | Schedule | Depends On |
|-----|----------|------------|
| WakeCapDW_Bronze_TimescaleDB_Raw | 2:00 AM UTC | - |
| WakeCapDW_Silver_TimescaleDB | 3:00 AM UTC | Bronze |
| **WakeCapDW_Gold_FactWorkersHistory** | **5:00 AM UTC** | Silver |

---

## Notes

### Simplifications Made

1. **Spatial Operations**: The original uses `geography::STIntersects` for location assignment detection. This implementation uses a simplified lookup. For full accuracy, implement H3 spatial indexing.

2. **Break Handling**: The time category calculation doesn't parse breaks. Use the UDF version for full break handling.

3. **Zone Detection**: Using FloorId as ZoneId proxy. For accurate zone detection, implement spatial point-in-polygon.

4. **Nearest Neighbor**: Using LocalDate as ShiftLocalDate when no shift match. For full accuracy, implement the 3-day nearest neighbor algorithm.

### Future Enhancements

1. Add H3 spatial indexing for zone and location assignment detection
2. Implement full break parsing in time category
3. Add Structured Streaming for near-real-time updates
4. Implement overlap detection (`spWorkersHistory_FixOverlaps`)

---

## Update Log

| Date | Update |
|------|--------|
| 2026-01-24 | Comprehensive plan with complete buildable code |
