# Plan: H3 Spatial Joins for DeviceLocation Zone Assignment

## Task Description

Implement H3 hexagonal spatial indexing to replicate the ADF `DeltaCopyAssetLocation` spatial containment logic (`ST_CONTAINS`) in Databricks. This will enable determining which zone each device location point falls within, linking device locations to workers via the resource device assignment table.

## Objective

When this plan is complete:
1. A Silver-layer table `silver_zone_h3_coverage` will contain H3 cell indexes covering each zone polygon
2. DeviceLocation records will have H3 indexes computed via a PySpark UDF
3. A Gold-layer notebook `gold_fact_device_location_zone` will join device locations to zones via H3 index matching
4. The system will replicate the ADF hourly sync functionality for zone assignment

## Problem Statement

The ADF pipeline `SyncFacts` performs spatial containment queries using PostGIS `ST_CONTAINS`:

```sql
SELECT
    dl."DeviceId" AS node_id,
    dl."ProjectId" AS project_id,
    dl."SpaceId" AS space_id,
    ST_Y(dl."Point") as latitude,
    ST_X(dl."Point") as longitude,
    z."Id" as "ZoneId",
    da."ResourceId"
FROM public."DeviceLocation" dl
LEFT JOIN public."Zone" z
    ON dl."SpaceId" = z."SpaceId"
    AND ST_CONTAINS(z."Coordinates", dl."Point")  -- Spatial containment
    AND (z."DeletedAt" IS NULL OR z."DeletedAt" > dl."GeneratedAt")
INNER JOIN public."MV_ResourceDevice_NoViolation" da
    ON da."DeviceId" = dl."DeviceId"
    AND dl."GeneratedAt" >= da."AssignedAt"
    AND (dl."GeneratedAt" < da."UnAssignedAt" OR da."UnAssignedAt" IS NULL)
```

**Scale:**
- `DeviceLocation`: 85M+ rows (1.7M rows/day, ~52GB)
- `Zone`: ~10K polygons
- Hourly sync frequency

**Current Gap:** Databricks has no spatial containment logic - workers are not assigned to correct zones.

## Solution Approach

Use **H3 hexagonal indexing** (Uber's open-source library) to convert expensive spatial joins into simple index equality joins:

1. **Pre-compute zone coverage**: Convert each zone polygon to a set of H3 cells that cover it
2. **Index device locations**: Convert each lat/lon point to its H3 cell index
3. **Join on H3 index**: Simple equality join replaces `ST_CONTAINS`

**Why H3 over Sedona:**
- 85M rows requires excellent join performance
- H3 converts spatial joins to simple index lookups (~100x faster)
- No cluster library management required
- Acceptable ~1-5% boundary approximation for zone attendance

## Source System Analysis

### Bronze Layer Tables

| Table | Rows | Key Columns |
|-------|------|-------------|
| `timescale_devicelocation` | 85M+ | DeviceId, ProjectId, Point (WKT), GeneratedAt, SpaceId |
| `timescale_zone` | ~10K | Id, Coordinates (WKT polygon), SpaceId, DeletedAt |

### Silver Layer Tables

| Table | Key Columns |
|-------|-------------|
| `silver_zone` | ZoneId, FloorId (SpaceId), Coordinates, ProjectId, DeletedAt |
| `silver_resource_device` | DeviceId, WorkerId, AssignedAt, UnassignedAt |
| `silver_fact_workers_history` | Existing fact table with device location data |

**Note:** `silver_resource_device` does NOT have DeletedAt or ProjectId columns - the source TimescaleDB `ResourceDevice` table lacks these columns.

### Key Observations

1. **Coordinates column** is already in `silver_zone` as WKT text (converted via ST_AsText in Bronze)
2. **DeviceLocation** uses `Point` column for geometry (also WKT via ST_AsText)
3. **MV_ResourceDevice_NoViolation** equivalent: Uses window function to detect overlapping device assignments (NOT DeletedAt filtering). The MV filters rows where previous `UnAssignedAt` > current `AssignedAt` (violation), keeping only clean assignments.

## Target Architecture

- **Catalog**: `wakecap_prod`
- **Schema**: Silver (`silver`), Gold (`gold`)
- **Pipeline Type**: Databricks Job (add to existing `WakeCapDW_Gold` job)
- **New Tables**:
  - `silver.silver_zone_h3_coverage` - Zone to H3 cell mapping
  - `gold.gold_fact_device_location_zone` - Device locations with zone assignment

## Relevant Files

### Existing Files to Reference

- `migration_project/pipelines/silver/config/silver_tables.yml` - Silver table definitions
- `migration_project/pipelines/timescaledb/config/timescaledb_tables_v2.yml` - Bronze DeviceLocation config
- `migration_project/pipelines/gold/notebooks/gold_worker_location_assignments.py` - Similar pattern for location assignments
- `migration_project/pipelines/gold/notebooks/gold_fact_workers_history.py` - Gold fact table pattern
- `migration_project/DEVICELOCATION_SPATIAL_OPTIONS.md` - H3 vs Sedona analysis

### New Files to Create

| File | Purpose |
|------|---------|
| `migration_project/pipelines/silver/notebooks/silver_zone_h3_coverage.py` | Compute H3 coverage for zones |
| `migration_project/pipelines/gold/notebooks/gold_fact_device_location_zone.py` | Main spatial join notebook |
| `migration_project/pipelines/gold/udfs/h3_udfs.py` | H3 UDF definitions |

## Implementation Phases

### Phase 1: Foundation - H3 Library and UDFs

**Goal:** Set up H3 library and create reusable UDFs

**Tasks:**
1. Verify H3 library availability on compute cluster
2. Create H3 UDF module with:
   - `point_to_h3(lat, lon, resolution)` - Convert point to H3 index
   - `polygon_to_h3_cells(wkt, resolution)` - Convert polygon to H3 cell set
3. Test UDFs with sample data

### Phase 2: Zone H3 Coverage Table

**Goal:** Pre-compute H3 coverage for all zones

**Tasks:**
1. Create `silver_zone_h3_coverage` notebook
2. Read zone polygons from `silver_zone`
3. Convert each polygon to H3 cells using `polyfill`
4. Store zone_id → h3_index mapping table
5. Add to Silver job or run as one-time setup

### Phase 3: Device Location Zone Assignment

**Goal:** Build Gold fact table with zone assignments

**Tasks:**
1. Create `gold_fact_device_location_zone` notebook
2. Read incremental DeviceLocation data from Bronze
3. Compute H3 index for each point
4. Join with zone H3 coverage
5. Join with `silver_resource_device` for worker assignment
6. Apply temporal validity logic (device assignment at time of location)
7. Write to Gold table with MERGE

### Phase 4: Integration and Validation

**Goal:** Integrate into production and validate accuracy

**Tasks:**
1. Add new task to `WakeCapDW_Gold` job
2. Run initial full load
3. Compare sample results with ADF output
4. Tune H3 resolution if needed
5. Enable incremental processing

## Step by Step Tasks

### 1. Create H3 UDF Module

Create `migration_project/pipelines/gold/udfs/h3_udfs.py`:

```python
# Databricks notebook source
"""
H3 Spatial Indexing UDFs

Provides UDFs for H3 hexagonal spatial indexing:
- point_to_h3: Convert lat/lon to H3 cell index
- polygon_to_h3_cells: Convert WKT polygon to H3 cell coverage
"""

import h3
from shapely import wkt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType

# Default resolution: 9 (~175m hexagons, suitable for construction zones)
DEFAULT_H3_RESOLUTION = 9

@F.udf(returnType=StringType())
def point_to_h3(lat, lon, resolution=DEFAULT_H3_RESOLUTION):
    """Convert latitude/longitude to H3 cell index."""
    if lat is None or lon is None:
        return None
    try:
        return h3.geo_to_h3(float(lat), float(lon), resolution)
    except Exception:
        return None

@F.udf(returnType=ArrayType(StringType()))
def polygon_to_h3_cells(wkt_string, resolution=DEFAULT_H3_RESOLUTION):
    """Convert WKT polygon to list of H3 cells that cover it."""
    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        if geom.is_empty or not geom.is_valid:
            return None
        # polyfill returns set of H3 indexes covering the polygon
        cells = h3.polyfill_geojson(geom.__geo_interface__, resolution)
        return list(cells)
    except Exception:
        return None

def extract_lat_lon_from_point_wkt(point_wkt_col):
    """
    Extract latitude and longitude from a WKT POINT string.
    Input: "POINT(longitude latitude)" or "POINT (longitude latitude)"
    Output: Two columns (latitude, longitude)
    """
    # Remove "POINT(" or "POINT (" prefix and ")" suffix
    coords = F.regexp_replace(point_wkt_col, r"POINT\s*\(", "")
    coords = F.regexp_replace(coords, r"\)", "")
    coords = F.trim(coords)

    # Split by space - WKT format is "lon lat"
    lon = F.split(coords, r"\s+").getItem(0).cast("double")
    lat = F.split(coords, r"\s+").getItem(1).cast("double")

    return lat, lon
```

### 2. Create Zone H3 Coverage Notebook

Create `migration_project/pipelines/silver/notebooks/silver_zone_h3_coverage.py`:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Zone H3 Coverage
# MAGIC
# MAGIC Pre-computes H3 hexagonal cell coverage for all zones.
# MAGIC This enables efficient spatial joins by converting polygon containment
# MAGIC to simple index equality joins.
# MAGIC
# MAGIC **Source:** `silver_zone` (zone polygons as WKT)
# MAGIC **Target:** `silver_zone_h3_coverage` (zone_id to h3_index mapping)
# MAGIC
# MAGIC **H3 Resolution:** 9 (~175m hexagons)

# COMMAND ----------

import h3
from shapely import wkt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# COMMAND ----------

# Configuration
TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
SOURCE_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone"
TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone_h3_coverage"
H3_RESOLUTION = 9

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"H3 Resolution: {H3_RESOLUTION} (~175m hexagons)")

# COMMAND ----------

# Widget for load mode
dbutils.widgets.dropdown("load_mode", "full", ["full", "incremental"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")
print(f"Load Mode: {load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDF: Polygon to H3 Cells

# COMMAND ----------

def polygon_to_h3_cells_py(wkt_string, resolution=H3_RESOLUTION):
    """Convert WKT polygon to list of H3 cells that cover it."""
    if wkt_string is None:
        return []
    try:
        geom = wkt.loads(wkt_string)
        if geom.is_empty or not geom.is_valid:
            return []
        # polyfill returns set of H3 indexes covering the polygon
        cells = h3.polyfill_geojson(geom.__geo_interface__, resolution)
        return list(cells)
    except Exception as e:
        print(f"Error processing polygon: {e}")
        return []

# Register as UDF
polygon_to_h3_udf = F.udf(polygon_to_h3_cells_py, ArrayType(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Zone Polygons

# COMMAND ----------

# Read active zones with coordinates
zone_df = spark.table(SOURCE_TABLE).filter(
    F.col("Coordinates").isNotNull()
).select(
    F.col("ZoneId"),
    F.col("FloorId"),  # SpaceId in ADF terms
    F.col("ProjectId"),
    F.col("Coordinates")
)

zone_count = zone_df.count()
print(f"Zones with coordinates: {zone_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute H3 Coverage

# COMMAND ----------

# Apply UDF to get H3 cells for each zone
zone_h3_df = zone_df.withColumn(
    "h3_cells",
    polygon_to_h3_udf(F.col("Coordinates"))
)

# Explode to create one row per (zone_id, h3_index) pair
zone_h3_exploded = zone_h3_df.select(
    F.col("ZoneId"),
    F.col("FloorId"),
    F.col("ProjectId"),
    F.explode(F.col("h3_cells")).alias("h3_index")
).filter(
    F.col("h3_index").isNotNull()
)

# Add metadata
zone_h3_final = zone_h3_exploded.withColumn(
    "h3_resolution", F.lit(H3_RESOLUTION)
).withColumn(
    "_computed_at", F.current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Target Table

# COMMAND ----------

# Create table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        ZoneId STRING NOT NULL,
        FloorId STRING,
        ProjectId STRING,
        h3_index STRING NOT NULL,
        h3_resolution INT,
        _computed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'H3 hexagonal cell coverage for zones - enables spatial joins'
""")

# For full load, overwrite; for incremental, merge
if load_mode == "full":
    zone_h3_final.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    print(f"Full load complete")
else:
    # Incremental: merge based on ZoneId + h3_index
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forName(spark, TARGET_TABLE)

    delta_table.alias("target").merge(
        zone_h3_final.alias("source"),
        "target.ZoneId = source.ZoneId AND target.h3_index = source.h3_index"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"Incremental merge complete")

# COMMAND ----------

# Verify
result_count = spark.table(TARGET_TABLE).count()
zone_count_result = spark.table(TARGET_TABLE).select("ZoneId").distinct().count()
print(f"Total H3 cells: {result_count}")
print(f"Zones covered: {zone_count_result}")
print(f"Avg cells per zone: {result_count / zone_count_result if zone_count_result > 0 else 0:.1f}")

# COMMAND ----------

# Sample output
display(spark.table(TARGET_TABLE).limit(20))
```

### 3. Create Device Location Zone Assignment Notebook

Create `migration_project/pipelines/gold/notebooks/gold_fact_device_location_zone.py`:

```python
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
device_loc_parsed = device_loc_df.select(
    F.col("DeviceId"),
    F.col("ProjectId"),
    F.col("SpaceId"),  # FloorId
    F.col("GeneratedAt"),
    F.col("ActiveSequance"),
    F.col("InactiveSequance"),
    F.col("PointWKT").alias("Point"),  # Geometry as WKT
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
```

### 4. Update Silver Tables Config

Add zone H3 coverage table to `migration_project/pipelines/silver/config/silver_tables.yml`:

```yaml
# Add to tables list in silver_tables.yml

  # =====================================================
  # H3 SPATIAL INDEX TABLES
  # =====================================================

  - name: silver_zone_h3_coverage
    source_bronze_table: null  # Computed from silver_zone
    primary_key_columns: [ZoneId, h3_index]
    processing_group: zone_dependent
    transformation_type: computed
    comment: "H3 hexagonal cell coverage for zones - enables efficient spatial joins"
    expectations:
      critical:
        - "ZoneId IS NOT NULL"
        - "h3_index IS NOT NULL"
    columns:
      - source: ZoneId
        target: ZoneId
      - source: FloorId
        target: FloorId
      - source: ProjectId
        target: ProjectId
      - source: h3_index
        target: h3_index
      - source: h3_resolution
        target: h3_resolution
```

### 5. Add to WakeCapDW_Gold Job

Add new task to the existing Gold job:

```python
# Add this task to WakeCapDW_Gold job (ID: 933934272544045)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, TaskDependency

w = WorkspaceClient()

# Get existing job
job = w.jobs.get(933934272544045)

# Add new task for device location zone assignment
new_task = Task(
    task_key="gold_fact_device_location_zone",
    description="Device location zone assignment via H3 spatial join",
    depends_on=[],  # Independent, can run in parallel
    notebook_task=NotebookTask(
        notebook_path="/Workspace/migration_project/pipelines/gold/notebooks/gold_fact_device_location_zone",
        base_parameters={
            "load_mode": "incremental",
            "lookback_hours": "2"
        }
    ),
    existing_cluster_id="<cluster_id>",  # Use existing cluster
    timeout_seconds=7200
)

# Update job with new task
# (Implementation depends on existing job structure)
```

### 6. Deploy and Test

Execute deployment steps:

```bash
# 1. Upload notebooks to Databricks workspace
databricks workspace import migration_project/pipelines/gold/udfs/h3_udfs.py \
    /Workspace/migration_project/pipelines/gold/udfs/h3_udfs \
    --format SOURCE --language PYTHON --overwrite

databricks workspace import migration_project/pipelines/silver/notebooks/silver_zone_h3_coverage.py \
    /Workspace/migration_project/pipelines/silver/notebooks/silver_zone_h3_coverage \
    --format SOURCE --language PYTHON --overwrite

databricks workspace import migration_project/pipelines/gold/notebooks/gold_fact_device_location_zone.py \
    /Workspace/migration_project/pipelines/gold/notebooks/gold_fact_device_location_zone \
    --format SOURCE --language PYTHON --overwrite

# 2. Run zone H3 coverage (one-time full load)
databricks jobs run-now --job-id <silver_job_id> \
    --notebook-params '{"load_mode": "full"}'

# 3. Run device location zone assignment
databricks jobs run-now --job-id 933934272544045
```

## Testing Strategy

### Unit Tests

1. **H3 UDF Tests**
   - Test `point_to_h3` with valid lat/lon
   - Test `point_to_h3` with null/invalid inputs
   - Test `polygon_to_h3_cells` with various polygon shapes

2. **Zone Coverage Tests**
   - Verify all zones have H3 cells generated
   - Check cell count is reasonable (not too few, not too many)

### Integration Tests

1. **Sample Validation**
   - Select 100 random device locations
   - Manually verify zone assignment is correct
   - Compare with ADF output for same records

2. **Boundary Testing**
   - Test points exactly on zone boundaries
   - Verify consistent assignment

### Performance Tests

1. **Load Test**
   - Process 1M device locations
   - Measure execution time
   - Target: < 10 minutes for 1M rows

## Acceptance Criteria

1. **Zone H3 Coverage Table**
   - [ ] All zones with coordinates have H3 cells computed
   - [ ] Table is queryable and correctly partitioned
   - [ ] Average 50-500 cells per zone (resolution 9)

2. **Device Location Zone Assignment**
   - [ ] 95%+ of device locations get zone assignment
   - [ ] Worker assignment temporal logic is correct
   - [ ] Incremental processing works with watermark

3. **Performance**
   - [ ] Full load of 85M rows completes in < 2 hours
   - [ ] Incremental (2 hours of data, ~140K rows) completes in < 5 minutes

4. **Accuracy**
   - [ ] Sample validation shows > 95% match with ADF output
   - [ ] Boundary cases handled consistently

## Validation Commands

```sql
-- Check zone H3 coverage
SELECT COUNT(*) as total_cells, COUNT(DISTINCT ZoneId) as zones_covered
FROM wakecap_prod.silver.silver_zone_h3_coverage;

-- Check Gold table
SELECT COUNT(*) as total,
       COUNT(ZoneId) as with_zone,
       COUNT(WorkerId) as with_worker
FROM wakecap_prod.gold.gold_fact_device_location_zone;

-- Sample validation
SELECT dl.DeviceId, dl.Latitude, dl.Longitude, dl.ZoneId, z.ZoneName
FROM wakecap_prod.gold.gold_fact_device_location_zone dl
LEFT JOIN wakecap_prod.silver.silver_zone z ON dl.ZoneId = z.ZoneId
LIMIT 20;

-- Compare with Bronze count
SELECT
    (SELECT COUNT(*) FROM wakecap_prod.raw.timescale_devicelocation) as bronze_count,
    (SELECT COUNT(*) FROM wakecap_prod.gold.gold_fact_device_location_zone) as gold_count;
```

## Notes

### H3 Resolution Selection

| Resolution | Hex Edge | Use Case |
|------------|----------|----------|
| 7 | ~1.2 km | Very large zones |
| 8 | ~460 m | Large construction sites |
| **9** | **~175 m** | **Default - typical zones** |
| 10 | ~65 m | Small zones |
| 11 | ~25 m | Very precise zones |

Resolution 9 is recommended as default. Can be tuned per project if needed.

### Boundary Handling

H3 provides approximate containment. Points on boundaries may be assigned to adjacent zones. This is acceptable for zone attendance reporting. For compliance-critical applications, consider:
- Using finer resolution (10 or 11)
- Post-processing with exact geometry check for boundary points
- Implementing Sedona for exact containment

### Incremental Processing

The notebook uses watermark-based incremental loading:
- Reads `GeneratedAt` > last_watermark - lookback_hours
- Lookback provides safety margin for late-arriving data
- MERGE ensures idempotent updates

### Dependencies

- H3 Python library (`h3>=3.7.0`)
- Shapely (`shapely>=2.0.0`) for WKT parsing
- Both are typically available in Databricks Runtime 13.x+

---

## Deployment Status

**Status:** DEPLOYED TO DATABRICKS (2026-01-28)

| Component | Workspace Path | Status |
|-----------|----------------|--------|
| H3 UDFs | `/Workspace/migration_project/pipelines/gold/udfs/h3_udfs` | Deployed |
| Zone H3 Coverage | `/Workspace/migration_project/pipelines/silver/notebooks/silver_zone_h3_coverage` | Deployed |
| Device Location Zone | `/Workspace/migration_project/pipelines/gold/notebooks/gold_fact_device_location_zone` | Deployed |
| ADF Helpers | `/Workspace/migration_project/pipelines/gold/udfs/adf_helpers` | Updated (window function fix) |

**Pending:**
- Bronze Zone table geometry data (CoordinatesWKT column) needs to be loaded
- Run zone H3 coverage notebook to populate `silver_zone_h3_coverage` table
- Add Gold notebook to WakeCapDW_Gold job schedule

---

*Plan created: 2026-01-28*
*Deployed: 2026-01-28*
*Task Type: migration*
*Complexity: complex*
