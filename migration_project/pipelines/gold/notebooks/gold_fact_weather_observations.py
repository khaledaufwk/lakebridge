# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Weather Observations
# MAGIC
# MAGIC **Converted from:** `stg.spDeltaSyncFactWeatherObservations` (243 lines)
# MAGIC
# MAGIC **Purpose:** Synchronize weather station sensor data from TimescaleDB source
# MAGIC to the gold FactWeatherObservations table. Handles 16 weather/environmental
# MAGIC sensor metrics including temperature, humidity, wind, rainfall, and air quality.
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - TEMP_TABLE (`#batch`) -> Spark DataFrame with cache()
# MAGIC - Dimension Lookup (Project) with ROW_NUMBER deduplication -> PySpark Window functions
# MAGIC - MERGE with float comparison tolerance (0.00001) -> DeltaTable merge with ABS() conditions
# MAGIC - Stalled Record Recovery/Tracking -> Separate staging tables for unresolved records
# MAGIC - Calculated Column (`humidity / 100 as Humidity2`) -> PySpark withColumn
# MAGIC - fnExtSourceIDAlias(18) -> Inline CASE expression (returns 15 for ExtSourceID=18)
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.bronze.wc2023_weather_station_sensor` (stg.wc2023_weather_station_sensor)
# MAGIC - `wakecap_prod.bronze.wc2023_weather_station_sensor_stalled` (stg.wc2023_weather_station_sensor_stalled)
# MAGIC - `wakecap_prod.silver.silver_project` (dbo.Project)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_weather_observations`
# MAGIC **Watermarks:** `wakecap_prod.migration._gold_watermarks`
# MAGIC
# MAGIC **ExtSourceID:** 18 (ExtSourceIDAlias: 15)

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
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables
SOURCE_WEATHER_SENSOR = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_weather_station_sensor"
SOURCE_WEATHER_STALLED = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_weather_station_sensor_stalled"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_weather_observations"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID for this sync
EXT_SOURCE_ID = 18
# fnExtSourceIDAlias(18) = 15 (weather sensors share alias with observations)
EXT_SOURCE_ID_ALIAS = 15

# Float comparison tolerance (from original SP)
FLOAT_TOLERANCE = 0.00001

print(f"Source Weather Sensor: {SOURCE_WEATHER_SENSOR}")
print(f"Source Weather Stalled: {SOURCE_WEATHER_STALLED}")
print(f"Source Project: {SOURCE_PROJECT}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")

load_mode = dbutils.widgets.get("load_mode")
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {load_mode}")
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
    ("Weather Sensor", SOURCE_WEATHER_SENSOR),
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

# Check stalled table (optional - may not exist initially)
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_WEATHER_STALLED} LIMIT 0")
    print(f"[OK] Weather Stalled exists: {SOURCE_WEATHER_STALLED}")
    stalled_table_exists = True
except Exception as e:
    print(f"[WARN] Weather Stalled not found (will skip stalled recovery): {str(e)[:50]}")
    stalled_table_exists = False

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


def fn_ext_source_id_alias(ext_source_id):
    """
    PySpark equivalent of stg.fnExtSourceIDAlias(ExtSourceID).
    Maps ExtSourceID to a canonical alias for comparison.

    Key mappings:
    - 15 (Observations), 18 (Weather) -> 15
    - 1, 2, 10, 14, 21 -> 1 (TimeClock variations)
    - Others map to themselves
    """
    return F.when(F.col(ext_source_id).isin(15, 18), F.lit(15)) \
            .when(F.col(ext_source_id).isin(1, 2, 10, 14, 21), F.lit(1)) \
            .otherwise(F.col(ext_source_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Recover Stalled Records
# MAGIC
# MAGIC Pick up any stalled records that we can now resolve (Project dimension now exists).
# MAGIC Original: INSERT INTO [stg].[wc2023_weather_station_sensor] SELECT ... FROM stalled

# COMMAND ----------

print("=" * 60)
print("STEP 1: Recover Stalled Records")
print("=" * 60)

recovered_count = 0

if stalled_table_exists:
    try:
        # Load stalled records
        stalled_df = spark.table(SOURCE_WEATHER_STALLED)
        stalled_count = stalled_df.count()
        print(f"Stalled records to check: {stalled_count}")

        if stalled_count > 0:
            # Load project dimension with deduplication
            # Original: ROW_NUMBER() OVER (PARTITION BY ExtProjectID, fnExtSourceIDAlias(ExtSourceID) ORDER BY (SELECT NULL)) rn = 1
            project_df = spark.table(SOURCE_PROJECT) \
                .withColumn("_ext_source_alias", fn_ext_source_id_alias("ExtSourceID")) \
                .withColumn("_rn", F.row_number().over(
                    Window.partitionBy("ExtProjectID", "_ext_source_alias").orderBy(F.lit(1))
                )) \
                .filter(F.col("_rn") == 1) \
                .filter(F.col("_ext_source_alias") == EXT_SOURCE_ID_ALIAS) \
                .select("ProjectID", "ExtProjectID", "ExtSourceID")

            # Find stalled records that can now be resolved
            # Original: INNER JOIN on t1.ExtProjectID = ta.project_id AND fnExtSourceIDAlias(t1.ExtSourceID) = fnExtSourceIDAlias(18)
            recoverable_df = stalled_df.alias("stalled") \
                .join(
                    project_df.alias("proj"),
                    F.col("stalled.project_id") == F.col("proj.ExtProjectID"),
                    "inner"
                ) \
                .select("stalled.*")

            recovered_count = recoverable_df.count()
            print(f"Records that can be recovered: {recovered_count}")

            if recovered_count > 0:
                # Insert recovered records back into source table for processing
                # Note: In Databricks, we'll include them in the batch processing directly
                recoverable_df.createOrReplaceTempView("recovered_weather_records")
                print(f"[OK] {recovered_count} stalled records will be included in batch")
    except Exception as e:
        print(f"[WARN] Error processing stalled records: {str(e)[:100]}")
        recovered_count = 0
else:
    print("Skipping stalled recovery (table does not exist)")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Batch DataFrame
# MAGIC
# MAGIC Create the #batch temp table equivalent with:
# MAGIC - Source weather sensor data
# MAGIC - Project dimension lookup
# MAGIC - Calculated Humidity2 column (humidity / 100)
# MAGIC - ROW_NUMBER deduplication on (project_id, serial_no, network_id, generated_at)

# COMMAND ----------

print("=" * 60)
print("STEP 2: Build Batch DataFrame")
print("=" * 60)

# Load source weather sensor data
weather_df = spark.table(SOURCE_WEATHER_SENSOR)
source_count = weather_df.count()
print(f"Source weather records: {source_count}")

# Load project dimension with deduplication for dimension lookup
# Original: ROW_NUMBER() OVER (PARTITION BY ExtProjectID, fnExtSourceIDAlias(ExtSourceID) ORDER BY (SELECT NULL)) rn = 1
project_lookup_df = spark.table(SOURCE_PROJECT) \
    .withColumn("_ext_source_alias", fn_ext_source_id_alias("ExtSourceID")) \
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("ExtProjectID", "_ext_source_alias").orderBy(F.lit(1))
    )) \
    .filter(F.col("_rn") == 1) \
    .filter(F.col("_ext_source_alias") == EXT_SOURCE_ID_ALIAS) \
    .select(
        F.col("ProjectID").alias("dim_ProjectID"),
        F.col("ExtProjectID").alias("dim_ExtProjectID")
    )

print(f"Project dimension records: {project_lookup_df.count()}")

# Combine source with recovered stalled records (if any)
if recovered_count > 0:
    recovered_df = spark.table("recovered_weather_records")
    combined_source_df = weather_df.unionByName(recovered_df)
    print(f"Combined source (including recovered): {combined_source_df.count()}")
else:
    combined_source_df = weather_df

# COMMAND ----------

# Build batch with dimension lookup and deduplication
# Original SQL:
# SELECT ta.*, 18 as ExtSourceID, t1.ProjectID, humidity / 100 as Humidity2,
#        ROW_NUMBER() OVER (PARTITION BY project_id, serial_no, network_id, generated_at ORDER BY created_at DESC) as RN
# FROM source ta INNER JOIN project t1 ON t1.ExtProjectID = ta.project_id

# Join with project dimension (INNER JOIN - drops records without matching project)
# Note: UUIDs are case-sensitive in string comparison, normalize to uppercase
batch_with_project = combined_source_df.alias("src") \
    .join(
        project_lookup_df.alias("proj"),
        F.upper(F.col("src.project_id")) == F.upper(F.col("proj.dim_ExtProjectID")),
        "inner"
    ) \
    .select(
        "src.*",
        F.lit(EXT_SOURCE_ID).alias("ExtSourceID"),
        F.col("proj.dim_ProjectID").alias("ProjectID"),
        (F.col("src.humidity") / 100).alias("Humidity2")
    )

# Add ROW_NUMBER for deduplication
# Original: PARTITION BY project_id, serial_no, network_id, generated_at ORDER BY created_at DESC
dedup_window = Window.partitionBy(
    "project_id", "serial_no", "network_id", "generated_at"
).orderBy(F.col("created_at").desc())

batch_df = batch_with_project \
    .withColumn("RN", F.row_number().over(dedup_window))

# Note: cache() removed - not supported on serverless compute
batch_count = batch_df.count()
dedup_count = batch_df.filter(F.col("RN") == 1).count()

print(f"Batch records (all): {batch_count}")
print(f"Batch records (deduplicated, RN=1): {dedup_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create/Update Target Table Schema

# COMMAND ----------

print("=" * 60)
print("STEP 3: Ensure Target Table Exists")
print("=" * 60)

# Target table schema matching FactWeatherObservations
target_schema = """
    FactWeatherObservationID BIGINT GENERATED ALWAYS AS IDENTITY,
    ProjectID INT NOT NULL,
    NetworkID VARCHAR(100),
    Station VARCHAR(100),
    TimestampUTC TIMESTAMP NOT NULL,
    ExtSourceID INT,
    WindSpeed DOUBLE,
    Rainfall DOUBLE,
    Temperature DOUBLE,
    AtmosphericPressure DOUBLE,
    PM25 DOUBLE,
    WindDirection DOUBLE,
    PM10 DOUBLE,
    Humidity DOUBLE,
    TSP DOUBLE,
    H2S DOUBLE,
    CustomSensor4 DOUBLE,
    CustomSensor5 DOUBLE,
    RainfallCumulative DOUBLE,
    CO2 DOUBLE,
    SO2 DOUBLE,
    CO DOUBLE,
    WatermarkUTC TIMESTAMP,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
"""

# Check if target table exists
try:
    spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    print(f"[OK] Target table exists: {TARGET_TABLE}")
    target_exists = True
except Exception as e:
    print(f"[INFO] Target table does not exist, creating...")
    target_exists = False

if not target_exists:
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            {target_schema}
        )
        USING DELTA
        CLUSTER BY (ProjectID, TimestampUTC)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """
    spark.sql(create_sql)
    print(f"[OK] Created target table: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Execute MERGE
# MAGIC
# MAGIC Original MERGE logic:
# MAGIC - Match on: ProjectID, Station (serial_no), NetworkID (network_id), TimestampUTC (generated_at), ExtSourceIDAlias
# MAGIC - Update when ANY sensor value changes beyond tolerance (0.00001)
# MAGIC - Insert when not matched

# COMMAND ----------

print("=" * 60)
print("STEP 4: Execute MERGE")
print("=" * 60)

# Get deduplicated source records
source_df = batch_df.filter(F.col("RN") == 1).select(
    F.col("ProjectID"),
    F.col("network_id").alias("NetworkID"),
    F.col("serial_no").alias("Station"),
    F.col("generated_at").alias("TimestampUTC"),
    F.col("ExtSourceID"),
    F.col("wind_speed").alias("WindSpeed"),
    F.col("rain_fall").alias("Rainfall"),
    F.col("temperature").alias("Temperature"),
    F.col("air_pressure").alias("AtmosphericPressure"),
    F.col("pm25").alias("PM25"),
    F.col("wind_direction").alias("WindDirection"),
    F.col("pm10").alias("PM10"),
    F.col("Humidity2").alias("Humidity"),
    F.col("tsp").alias("TSP"),
    F.col("h2s").alias("H2S"),
    F.col("custom_sensor_4").alias("CustomSensor4"),
    F.col("custom_sensor_5").alias("CustomSensor5"),
    F.col("cumulative_rain_fall").alias("RainfallCumulative"),
    F.col("co2").alias("CO2"),
    F.col("so2").alias("SO2"),
    F.col("co").alias("CO")
)

source_count = source_df.count()
print(f"Source records for MERGE: {source_count}")

if source_count == 0:
    print("No records to process, skipping MERGE")
    dbutils.notebook.exit("NO_RECORDS_TO_PROCESS")

# COMMAND ----------

# Build MERGE using SQL for complex conditions
# The original SP uses fnExtSourceIDAlias for matching - we handle this with CASE

source_df.createOrReplaceTempView("weather_source")

# Check target row count before merge
try:
    target_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0
print(f"Target rows before MERGE: {target_before}")

# COMMAND ----------

# Execute MERGE with float tolerance comparison
# Original comparison: ABS(t.Column - s.column) > 0.00001 OR (t.Column IS NULL AND s.column IS NOT NULL) OR (t.Column IS NOT NULL AND s.column IS NULL)

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING weather_source AS s
ON t.ProjectID = s.ProjectID
   AND t.Station = s.Station
   AND t.NetworkID = s.NetworkID
   AND t.TimestampUTC = s.TimestampUTC
   AND CASE WHEN t.ExtSourceID IN (15, 18) THEN 15 WHEN t.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE t.ExtSourceID END
     = CASE WHEN s.ExtSourceID IN (15, 18) THEN 15 WHEN s.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE s.ExtSourceID END

WHEN MATCHED AND (
    -- WindSpeed changed
    (ABS(COALESCE(t.WindSpeed, 0) - COALESCE(s.WindSpeed, 0)) > {FLOAT_TOLERANCE} OR (t.WindSpeed IS NULL AND s.WindSpeed IS NOT NULL) OR (t.WindSpeed IS NOT NULL AND s.WindSpeed IS NULL)) OR
    -- Rainfall changed
    (ABS(COALESCE(t.Rainfall, 0) - COALESCE(s.Rainfall, 0)) > {FLOAT_TOLERANCE} OR (t.Rainfall IS NULL AND s.Rainfall IS NOT NULL) OR (t.Rainfall IS NOT NULL AND s.Rainfall IS NULL)) OR
    -- Temperature changed
    (ABS(COALESCE(t.Temperature, 0) - COALESCE(s.Temperature, 0)) > {FLOAT_TOLERANCE} OR (t.Temperature IS NULL AND s.Temperature IS NOT NULL) OR (t.Temperature IS NOT NULL AND s.Temperature IS NULL)) OR
    -- AtmosphericPressure changed
    (ABS(COALESCE(t.AtmosphericPressure, 0) - COALESCE(s.AtmosphericPressure, 0)) > {FLOAT_TOLERANCE} OR (t.AtmosphericPressure IS NULL AND s.AtmosphericPressure IS NOT NULL) OR (t.AtmosphericPressure IS NOT NULL AND s.AtmosphericPressure IS NULL)) OR
    -- PM25 changed
    (ABS(COALESCE(t.PM25, 0) - COALESCE(s.PM25, 0)) > {FLOAT_TOLERANCE} OR (t.PM25 IS NULL AND s.PM25 IS NOT NULL) OR (t.PM25 IS NOT NULL AND s.PM25 IS NULL)) OR
    -- WindDirection changed
    (ABS(COALESCE(t.WindDirection, 0) - COALESCE(s.WindDirection, 0)) > {FLOAT_TOLERANCE} OR (t.WindDirection IS NULL AND s.WindDirection IS NOT NULL) OR (t.WindDirection IS NOT NULL AND s.WindDirection IS NULL)) OR
    -- PM10 changed
    (ABS(COALESCE(t.PM10, 0) - COALESCE(s.PM10, 0)) > {FLOAT_TOLERANCE} OR (t.PM10 IS NULL AND s.PM10 IS NOT NULL) OR (t.PM10 IS NOT NULL AND s.PM10 IS NULL)) OR
    -- Humidity changed
    (ABS(COALESCE(t.Humidity, 0) - COALESCE(s.Humidity, 0)) > {FLOAT_TOLERANCE} OR (t.Humidity IS NULL AND s.Humidity IS NOT NULL) OR (t.Humidity IS NOT NULL AND s.Humidity IS NULL)) OR
    -- TSP changed
    (ABS(COALESCE(t.TSP, 0) - COALESCE(s.TSP, 0)) > {FLOAT_TOLERANCE} OR (t.TSP IS NULL AND s.TSP IS NOT NULL) OR (t.TSP IS NOT NULL AND s.TSP IS NULL)) OR
    -- H2S changed
    (ABS(COALESCE(t.H2S, 0) - COALESCE(s.H2S, 0)) > {FLOAT_TOLERANCE} OR (t.H2S IS NULL AND s.H2S IS NOT NULL) OR (t.H2S IS NOT NULL AND s.H2S IS NULL)) OR
    -- CustomSensor4 changed
    (ABS(COALESCE(t.CustomSensor4, 0) - COALESCE(s.CustomSensor4, 0)) > {FLOAT_TOLERANCE} OR (t.CustomSensor4 IS NULL AND s.CustomSensor4 IS NOT NULL) OR (t.CustomSensor4 IS NOT NULL AND s.CustomSensor4 IS NULL)) OR
    -- CustomSensor5 changed
    (ABS(COALESCE(t.CustomSensor5, 0) - COALESCE(s.CustomSensor5, 0)) > {FLOAT_TOLERANCE} OR (t.CustomSensor5 IS NULL AND s.CustomSensor5 IS NOT NULL) OR (t.CustomSensor5 IS NOT NULL AND s.CustomSensor5 IS NULL)) OR
    -- RainfallCumulative changed
    (ABS(COALESCE(t.RainfallCumulative, 0) - COALESCE(s.RainfallCumulative, 0)) > {FLOAT_TOLERANCE} OR (t.RainfallCumulative IS NULL AND s.RainfallCumulative IS NOT NULL) OR (t.RainfallCumulative IS NOT NULL AND s.RainfallCumulative IS NULL)) OR
    -- CO2 changed
    (ABS(COALESCE(t.CO2, 0) - COALESCE(s.CO2, 0)) > {FLOAT_TOLERANCE} OR (t.CO2 IS NULL AND s.CO2 IS NOT NULL) OR (t.CO2 IS NOT NULL AND s.CO2 IS NULL)) OR
    -- SO2 changed
    (ABS(COALESCE(t.SO2, 0) - COALESCE(s.SO2, 0)) > {FLOAT_TOLERANCE} OR (t.SO2 IS NULL AND s.SO2 IS NOT NULL) OR (t.SO2 IS NOT NULL AND s.SO2 IS NULL)) OR
    -- CO changed
    (ABS(COALESCE(t.CO, 0) - COALESCE(s.CO, 0)) > {FLOAT_TOLERANCE} OR (t.CO IS NULL AND s.CO IS NOT NULL) OR (t.CO IS NOT NULL AND s.CO IS NULL))
)
THEN UPDATE SET
    t.WatermarkUTC = current_timestamp(),
    t.UpdatedAt = current_timestamp(),
    t.WindSpeed = s.WindSpeed,
    t.Rainfall = s.Rainfall,
    t.Temperature = s.Temperature,
    t.AtmosphericPressure = s.AtmosphericPressure,
    t.PM25 = s.PM25,
    t.WindDirection = s.WindDirection,
    t.PM10 = s.PM10,
    t.Humidity = s.Humidity,
    t.TSP = s.TSP,
    t.H2S = s.H2S,
    t.CustomSensor4 = s.CustomSensor4,
    t.CustomSensor5 = s.CustomSensor5,
    t.RainfallCumulative = s.RainfallCumulative,
    t.CO2 = s.CO2,
    t.SO2 = s.SO2,
    t.CO = s.CO,
    t.ExtSourceID = s.ExtSourceID

WHEN NOT MATCHED THEN INSERT (
    ProjectID, NetworkID, Station, TimestampUTC, ExtSourceID,
    WindSpeed, Rainfall, Temperature, AtmosphericPressure, PM25,
    WindDirection, PM10, Humidity, TSP, H2S,
    CustomSensor4, CustomSensor5, RainfallCumulative, CO2, SO2, CO
)
VALUES (
    s.ProjectID, s.NetworkID, s.Station, s.TimestampUTC, s.ExtSourceID,
    s.WindSpeed, s.Rainfall, s.Temperature, s.AtmosphericPressure, s.PM25,
    s.WindDirection, s.PM10, s.Humidity, s.TSP, s.H2S,
    s.CustomSensor4, s.CustomSensor5, s.RainfallCumulative, s.CO2, s.SO2, s.CO
)
"""

spark.sql(merge_sql)

# Get metrics
target_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
inserted = target_after - target_before
print(f"Target rows after MERGE: {target_after}")
print(f"Estimated inserts: {inserted}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Handle Stalled Records
# MAGIC
# MAGIC Two operations:
# MAGIC 1. DELETE previously stalled records that were processed in this batch
# MAGIC 2. INSERT new stalled records (source records that couldn't be resolved)

# COMMAND ----------

print("=" * 60)
print("STEP 5: Handle Stalled Records")
print("=" * 60)

# Create stalled table if it doesn't exist
stalled_table_schema = """
    id BIGINT,
    network_id STRING,
    project_id STRING,
    generated_at TIMESTAMP,
    gateway_received_at TIMESTAMP,
    wind_speed DOUBLE,
    rain_fall DOUBLE,
    temperature DOUBLE,
    air_pressure DOUBLE,
    pm25 DOUBLE,
    wind_direction DOUBLE,
    pm10 DOUBLE,
    humidity DOUBLE,
    tsp DOUBLE,
    h2s DOUBLE,
    custom_sensor_4 DOUBLE,
    custom_sensor_5 DOUBLE,
    created_at TIMESTAMP,
    serial_no STRING,
    cumulative_rain_fall DOUBLE,
    co2 DOUBLE,
    so2 DOUBLE,
    co DOUBLE
"""

try:
    spark.sql(f"DESCRIBE TABLE {SOURCE_WEATHER_STALLED}")
    print(f"[OK] Stalled table exists: {SOURCE_WEATHER_STALLED}")
except:
    print(f"[INFO] Creating stalled table: {SOURCE_WEATHER_STALLED}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SOURCE_WEATHER_STALLED} (
            {stalled_table_schema}
        )
        USING DELTA
    """)

# COMMAND ----------

# Step 5a: Remove previously stalled records that were processed
# Original: DELETE s FROM stalled s INNER JOIN #batch t ON ... WHERE RN = 1

if recovered_count > 0:
    print(f"Removing {recovered_count} recovered records from stalled table...")

    # Get the keys of processed records
    processed_keys = batch_df.filter(F.col("RN") == 1) \
        .select("project_id", "serial_no", "network_id", "generated_at") \
        .distinct()

    processed_keys.createOrReplaceTempView("processed_keys")

    delete_sql = f"""
        DELETE FROM {SOURCE_WEATHER_STALLED} AS s
        WHERE EXISTS (
            SELECT 1 FROM processed_keys p
            WHERE s.project_id = p.project_id
              AND s.serial_no = p.serial_no
              AND s.network_id = p.network_id
              AND s.generated_at = p.generated_at
        )
    """
    spark.sql(delete_sql)
    print("[OK] Removed recovered records from stalled table")
else:
    print("No recovered records to remove from stalled table")

# COMMAND ----------

# Step 5b: Insert new stalled records (source records without matching project)
# Original: Records in source but NOT in #batch (failed dimension lookup)

print("Checking for new stalled records...")

# Find source records that didn't make it into the batch (no matching project)
weather_df_with_key = weather_df.select(
    "id", "network_id", "project_id", "generated_at", "gateway_received_at",
    "wind_speed", "rain_fall", "temperature", "air_pressure", "pm25",
    "wind_direction", "pm10", "humidity", "tsp", "h2s",
    "custom_sensor_4", "custom_sensor_5", "created_at", "serial_no",
    "cumulative_rain_fall", "co2", "so2", "co"
)

# Records in batch (resolved)
resolved_keys = batch_df.filter(F.col("RN") == 1) \
    .select("project_id", "serial_no", "network_id", "generated_at")

# Find unresolved (stalled) records using anti-join
new_stalled_df = weather_df_with_key.alias("src") \
    .join(
        resolved_keys.alias("resolved"),
        (F.col("src.project_id") == F.col("resolved.project_id")) &
        (F.col("src.serial_no") == F.col("resolved.serial_no")) &
        (F.col("src.network_id") == F.col("resolved.network_id")) &
        (F.col("src.generated_at") == F.col("resolved.generated_at")),
        "left_anti"
    )

new_stalled_count = new_stalled_df.count()
print(f"New stalled records: {new_stalled_count}")

if new_stalled_count > 0:
    # MERGE into stalled table (insert only if not already there)
    new_stalled_df.createOrReplaceTempView("new_stalled")

    stalled_merge_sql = f"""
        MERGE INTO {SOURCE_WEATHER_STALLED} AS t
        USING new_stalled AS s
        ON t.project_id = s.project_id
           AND t.serial_no = s.serial_no
           AND t.network_id = s.network_id
           AND t.generated_at = s.generated_at
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(stalled_merge_sql)
    print(f"[OK] Inserted {new_stalled_count} new stalled records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Update Watermark

# COMMAND ----------

print("=" * 60)
print("STEP 6: Update Watermark")
print("=" * 60)

# Get max timestamp from processed batch as new watermark
new_watermark = batch_df.filter(F.col("RN") == 1) \
    .agg(F.max("generated_at")).collect()[0][0]

if new_watermark is None:
    new_watermark = datetime.now()

update_watermark(
    "gold_fact_weather_observations",
    new_watermark,
    target_after,
    {"inserted": inserted, "stalled": new_stalled_count}
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
print(f"Source table: {SOURCE_WEATHER_SENSOR}")
print(f"Target table: {TARGET_TABLE}")
print(f"")
print(f"Records processed:")
print(f"  - Source records: {source_count}")
print(f"  - Recovered from stalled: {recovered_count}")
print(f"  - After deduplication: {dedup_count}")
print(f"  - New stalled: {new_stalled_count}")
print(f"")
print(f"Target table:")
print(f"  - Rows before: {target_before}")
print(f"  - Rows after: {target_after}")
print(f"  - Estimated inserts: {inserted}")
print(f"")
print(f"Watermark: {new_watermark}")
print("=" * 60)

# Return success
dbutils.notebook.exit(f"SUCCESS: processed={dedup_count}, inserted={inserted}, stalled={new_stalled_count}")
