# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Device Location Zone Assignment
# MAGIC
# MAGIC **Replaces:** ADF `DeltaCopyAssetLocation` activity in `SyncFacts` pipeline
# MAGIC
# MAGIC **Purpose:** Determine which zone each device location falls within using H3 spatial indexing.
# MAGIC Links device locations to workers via resource device assignments.
# MAGIC
# MAGIC **Logic:**
# MAGIC 1. Read incremental DeviceLocation from Bronze
# MAGIC 2. Compute H3 index for each point
# MAGIC 3. Join with zone H3 coverage to find containing zone
# MAGIC 4. Join with resource_device to link to worker (temporal validity)
# MAGIC 5. Write to Gold fact table
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.raw.timescale_devicelocation` (Bronze)
# MAGIC - `wakecap_prod.silver.silver_zone_h3_coverage` (Zone H3 mapping)
# MAGIC - `wakecap_prod.silver.silver_resource_device` (Device-Worker assignment)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_device_location_zone`

# COMMAND ----------

# MAGIC %pip install h3 --quiet

# COMMAND ----------

# Restart Python to pick up newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

import h3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables
BRONZE_DEVICE_LOCATION = f"{TARGET_CATALOG}.{RAW_SCHEMA}.timescale_devicelocation"
ZONE_H3_COVERAGE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone_h3_coverage"
RESOURCE_DEVICE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_resource_device"
SILVER_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_device_location_zone"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# H3 Configuration
H3_RESOLUTION = 9

print(f"Bronze Device Location: {BRONZE_DEVICE_LOCATION}")
print(f"Zone H3 Coverage: {ZONE_H3_COVERAGE}")
print(f"Resource Device: {RESOURCE_DEVICE}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_hours", "2", "Lookback Hours (for incremental)")
dbutils.widgets.text("batch_size", "1000000", "Batch Size")

load_mode = dbutils.widgets.get("load_mode")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Load Mode: {load_mode}")
print(f"Lookback Hours: {lookback_hours}")
print(f"Batch Size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 UDF

# COMMAND ----------

@F.udf(returnType=StringType())
def point_to_h3(lat, lon):
    """Convert latitude/longitude to H3 cell index at resolution 9."""
    if lat is None or lon is None:
        return None
    try:
        return h3.geo_to_h3(float(lat), float(lon), H3_RESOLUTION)
    except Exception:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Last Watermark

# COMMAND ----------

def get_last_watermark():
    """Get last processed watermark for incremental loading."""
    try:
        result = spark.sql(f"""
            SELECT MAX(last_watermark_timestamp) as wm
            FROM {WATERMARK_TABLE}
            WHERE table_name = 'gold_fact_device_location_zone'
              AND last_load_status = 'success'
        """).collect()
        return result[0].wm if result and result[0].wm else None
    except:
        return None

def update_watermark(watermark_value, status, row_count, error_msg=None):
    """Update watermark after processing."""
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (table_name STRING, last_watermark_timestamp TIMESTAMP, last_load_status STRING, last_load_row_count BIGINT, last_error_message STRING, updated_at TIMESTAMP) USING DELTA")

        ts_sql = f"TIMESTAMP'{watermark_value}'" if watermark_value else "NULL"
        err_sql = f"'{str(error_msg)[:500]}'" if error_msg else "NULL"

        spark.sql(f"""
            MERGE INTO {WATERMARK_TABLE} t
            USING (SELECT 'gold_fact_device_location_zone' as table_name) s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET
                last_watermark_timestamp = {ts_sql},
                last_load_status = '{status}',
                last_load_row_count = {row_count},
                last_error_message = {err_sql},
                updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT VALUES (
                'gold_fact_device_location_zone', {ts_sql}, '{status}', {row_count}, {err_sql}, current_timestamp()
            )
        """)
    except Exception as e:
        print(f"Warning: Could not update watermark: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Device Location Data

# COMMAND ----------

# Get watermark for incremental
last_watermark = None if load_mode == "full" else get_last_watermark()

if last_watermark:
    print(f"Last watermark: {last_watermark}")
    # Use lookback for safety
    filter_time = last_watermark - timedelta(hours=lookback_hours)
    print(f"Filter from: {filter_time}")
else:
    print("Full load - no watermark filter")
    filter_time = None

# COMMAND ----------

# Read device location from Bronze
# Point column is stored as WKT: "POINT(longitude latitude)"
device_loc_df = spark.table(BRONZE_DEVICE_LOCATION)

# Check available columns
print("Available columns:", device_loc_df.columns)

# COMMAND ----------

# Apply incremental filter if applicable
if filter_time:
    device_loc_df = device_loc_df.filter(F.col("GeneratedAt") > filter_time)

# Select and parse columns
# Note: Point column format is "POINT(lon lat)" - extract lat/lon
# Check for PointWKT (geometry converted to WKT) or Point column
point_col = "PointWKT" if "PointWKT" in device_loc_df.columns else "Point"

device_loc_parsed = device_loc_df.select(
    F.col("DeviceId"),
    F.col("ProjectId"),
    F.col("SpaceId"),  # FloorId
    F.col("GeneratedAt"),
    F.col("ActiveSequance"),
    F.col("InactiveSequance"),
    F.col(point_col).alias("Point"),  # Geometry as WKT
    F.col("_loaded_at")
)

# Extract latitude and longitude from WKT POINT
# Format: "POINT(longitude latitude)"
device_loc_parsed = device_loc_parsed.withColumn(
    "_coords", F.regexp_replace(F.col("Point"), r"POINT\s*\(", "")
).withColumn(
    "_coords", F.regexp_replace(F.col("_coords"), r"\)", "")
).withColumn(
    "_coords", F.trim(F.col("_coords"))
).withColumn(
    "Longitude", F.split(F.col("_coords"), r"\s+").getItem(0).cast("double")
).withColumn(
    "Latitude", F.split(F.col("_coords"), r"\s+").getItem(1).cast("double")
).drop("_coords")

# Filter valid coordinates
device_loc_parsed = device_loc_parsed.filter(
    F.col("Latitude").isNotNull() &
    F.col("Longitude").isNotNull() &
    F.col("Latitude").between(-90, 90) &
    F.col("Longitude").between(-180, 180)
)

row_count = device_loc_parsed.count()
print(f"Device locations to process: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute H3 Index for Device Locations

# COMMAND ----------

# Add H3 index to device locations
device_loc_h3 = device_loc_parsed.withColumn(
    "h3_index",
    point_to_h3(F.col("Latitude"), F.col("Longitude"))
).filter(
    F.col("h3_index").isNotNull()
)

print(f"Locations with valid H3 index: {device_loc_h3.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with Zone H3 Coverage

# COMMAND ----------

# Read zone H3 coverage
zone_h3_df = spark.table(ZONE_H3_COVERAGE).select(
    F.col("ZoneId").alias("zone_ZoneId"),
    F.col("FloorId").alias("zone_FloorId"),
    F.col("h3_index").alias("zone_h3_index")
)

# Join device locations to zones via H3 index
# Also match on FloorId (SpaceId) for additional precision
device_with_zone = device_loc_h3.join(
    zone_h3_df,
    (device_loc_h3.h3_index == zone_h3_df.zone_h3_index) &
    (device_loc_h3.SpaceId.cast("string") == zone_h3_df.zone_FloorId.cast("string")),
    "left"
).select(
    device_loc_h3["*"],
    F.col("zone_ZoneId").alias("ZoneId")
)

# Handle multiple zone matches (H3 cell on boundary)
# Take the first match - could be enhanced with distance calculation
window = Window.partitionBy("DeviceId", "GeneratedAt").orderBy(F.col("ZoneId"))
device_with_zone = device_with_zone.withColumn(
    "_rn", F.row_number().over(window)
).filter(F.col("_rn") == 1).drop("_rn")

print(f"Locations with zone assignment: {device_with_zone.filter(F.col('ZoneId').isNotNull()).count()}")
print(f"Locations without zone match: {device_with_zone.filter(F.col('ZoneId').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with Resource Device (Worker Assignment)

# COMMAND ----------

# Get non-violating device-worker assignments (equivalent to MV_ResourceDevice_NoViolation)
# The MV uses a window function to detect overlapping assignments, NOT DeletedAt filtering
# A "violation" occurs when previous UnAssignedAt > current AssignedAt (overlapping assignments)
resource_device_raw = spark.table(RESOURCE_DEVICE).select(
    F.col("DeviceId"),
    F.col("WorkerId"),
    F.col("AssignedAt"),
    F.col("UnassignedAt")
)

# Apply window function to detect violations (same logic as PostgreSQL MV)
resource_device_df = resource_device_raw.withColumn(
    "_prev_unassigned_max",
    F.max(F.coalesce(F.col("UnassignedAt"), F.lit("2100-01-01").cast("timestamp")))
     .over(Window.partitionBy("DeviceId").orderBy("AssignedAt")
           .rowsBetween(Window.unboundedPreceding, -1))
).withColumn(
    "_violation",
    F.when(F.col("_prev_unassigned_max") > F.col("AssignedAt"), 1).otherwise(None)
).filter(
    F.col("_violation").isNull()  # Keep only non-violating assignments
).select(
    F.col("DeviceId").alias("rd_DeviceId"),
    F.col("WorkerId"),
    F.col("AssignedAt"),
    F.col("UnassignedAt")
)

# Join with temporal validity check
# Device was assigned to worker at the time of the location reading
device_with_worker = device_with_zone.join(
    resource_device_df,
    (device_with_zone.DeviceId.cast("string") == resource_device_df.rd_DeviceId.cast("string")) &
    (device_with_zone.GeneratedAt >= resource_device_df.AssignedAt) &
    (
        (resource_device_df.UnassignedAt.isNull()) |
        (device_with_zone.GeneratedAt < resource_device_df.UnassignedAt)
    ),
    "inner"  # Only keep locations with valid worker assignment
).select(
    device_with_zone["DeviceId"],
    device_with_zone["ProjectId"],
    device_with_zone["SpaceId"].alias("FloorId"),
    device_with_zone["ZoneId"],
    device_with_zone["Latitude"],
    device_with_zone["Longitude"],
    device_with_zone["GeneratedAt"],
    device_with_zone["h3_index"],
    F.col("WorkerId"),
    F.col("AssignedAt").alias("DeviceAssignedAt")
)

print(f"Final records with worker assignment: {device_with_worker.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

# Prepare final output
final_df = device_with_worker.withColumn(
    "_loaded_at", F.current_timestamp()
).withColumn(
    "_source", F.lit("h3_spatial_join")
)

# Create target table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        DeviceId STRING,
        ProjectId STRING,
        FloorId STRING,
        ZoneId STRING,
        Latitude DOUBLE,
        Longitude DOUBLE,
        GeneratedAt TIMESTAMP,
        h3_index STRING,
        WorkerId STRING,
        DeviceAssignedAt TIMESTAMP,
        _loaded_at TIMESTAMP,
        _source STRING
    )
    USING DELTA
    PARTITIONED BY (ProjectId)
    COMMENT 'Device location with zone assignment via H3 spatial join'
""")

# COMMAND ----------

# Write using MERGE for incremental
if load_mode == "full" or not spark.catalog.tableExists(TARGET_TABLE):
    final_df.write.format("delta").mode("overwrite").partitionBy("ProjectId").saveAsTable(TARGET_TABLE)
    print("Full load complete")
else:
    delta_table = DeltaTable.forName(spark, TARGET_TABLE)

    # Merge on natural key
    delta_table.alias("target").merge(
        final_df.alias("source"),
        """target.DeviceId = source.DeviceId
           AND target.GeneratedAt = source.GeneratedAt
           AND target.ProjectId = source.ProjectId"""
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Incremental merge complete")

# COMMAND ----------

# Update watermark
new_watermark = device_with_worker.agg(F.max("GeneratedAt")).collect()[0][0]
update_watermark(new_watermark, "success", device_with_worker.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Summary statistics
print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Records processed: {row_count}")
print(f"Records with zone assignment: {device_with_zone.filter(F.col('ZoneId').isNotNull()).count()}")
print(f"Records written to Gold: {device_with_worker.count()}")
print(f"New watermark: {new_watermark}")

# Sample output
display(spark.table(TARGET_TABLE).limit(10))
