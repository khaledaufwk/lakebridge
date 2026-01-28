# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Weather Observations
# MAGIC
# MAGIC **Purpose:** Transform weather station sensor data from Silver to Gold layer.
# MAGIC This table contains weather station sensor data including temperature, humidity, wind, rainfall, and air quality metrics.
# MAGIC
# MAGIC **Source:** `wakecap_prod.silver.silver_fact_weather_sensor` (from weather-station TimescaleDB)
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_weather_observations`
# MAGIC
# MAGIC **ADF Equivalent:** DeltaCopyWeatherStationSensor -> stg.spDeltaSyncFactWeatherObservations
# MAGIC
# MAGIC **Note:** This notebook reads from Silver layer (populated from weather-station TimescaleDB),
# MAGIC matching the ADF pattern of syncing from weather_station_sensor table.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and Schema Configuration
TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source table (Silver layer)
SOURCE_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_fact_weather_sensor"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_weather_observations"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID for weather station (matches ADF)
EXT_SOURCE_ID = 15

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")
print(f"Load Mode: {load_mode}")

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


def update_watermark(table_name, watermark_value, row_count):
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
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Ensure target schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print(f"[OK] Target schemas verified")

# Check source table
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} LIMIT 0")
    print(f"[OK] Source table exists: {SOURCE_TABLE}")
except Exception as e:
    print(f"[ERROR] Source table not found: {SOURCE_TABLE}")
    print(f"  Error: {str(e)[:100]}")
    print(f"\n[INFO] The Silver table needs to be populated first.")
    print(f"       Run the Bronze job to load weather_weather_station_sensor,")
    print(f"       then run the Silver job to create silver_fact_weather_sensor.")
    dbutils.notebook.exit("SOURCE_TABLE_NOT_FOUND")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Data from Silver

# COMMAND ----------

print("=" * 60)
print("STEP 1: Load Data from Silver")
print("=" * 60)

# Get watermark for incremental load
last_watermark = get_watermark("gold_fact_weather_observations")
print(f"Last watermark: {last_watermark}")

# Load source data
source_df = spark.table(SOURCE_TABLE)

# Apply incremental filter if needed
if load_mode == "incremental":
    source_df = source_df.filter(F.col("CreatedAt") > last_watermark)
    print(f"Incremental filter: CreatedAt > {last_watermark}")
else:
    print("Full load mode - processing all records")

source_count = source_df.count()
print(f"Source records: {source_count}")

if source_count == 0:
    print("No new records to process")
    dbutils.notebook.exit("NO_RECORDS_TO_PROCESS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Transform to Gold Schema

# COMMAND ----------

print("=" * 60)
print("STEP 2: Transform to Gold Schema")
print("=" * 60)

# Transform Silver to Gold schema
# Mapping from Silver columns to Gold columns (matching DBO schema):
#   Silver                    Gold
#   ------                    ----
#   WeatherSensorId           (not used - id is internal)
#   ProjectId                 ProjectID
#   SerialNo                  Station
#   NetworkId                 NetworkID
#   GeneratedAt               TimestampUTC
#   Rainfall                  Rainfall
#   RainfallCumulative        RainfallCumulative
#   Humidity                  Humidity
#   Temperature               Temperature
#   AtmosphericPressure       AtmosphericPressure
#   WindSpeed                 WindSpeed
#   WindDirection             WindDirection
#   PM25                      PM25
#   PM10                      PM10
#   TSP                       TSP
#   H2S                       H2S
#   SO2                       SO2
#   CO2                       CO2
#   CO                        CO
#   CustomSensor4             CustomSensor4
#   CustomSensor5             CustomSensor5
#   CreatedAt                 WatermarkUTC

gold_df = source_df.select(
    F.col("ProjectId").cast("int").alias("ProjectID"),
    F.lit(None).cast("int").alias("StationID"),  # Always NULL in source
    F.col("SerialNo").alias("Station"),
    F.col("NetworkId").cast("int").alias("NetworkID"),
    F.col("GeneratedAt").alias("TimestampUTC"),
    F.col("Rainfall").cast("double").alias("Rainfall"),
    F.col("RainfallCumulative").cast("double").alias("RainfallCumulative"),
    F.col("Humidity").cast("double").alias("Humidity"),
    F.col("Temperature").cast("double").alias("Temperature"),
    F.col("AtmosphericPressure").cast("double").alias("AtmosphericPressure"),
    F.col("WindSpeed").cast("double").alias("WindSpeed"),
    F.col("WindDirection").cast("double").alias("WindDirection"),
    F.col("PM25").cast("double").alias("PM25"),
    F.col("PM10").cast("double").alias("PM10"),
    F.col("TSP").cast("double").alias("TSP"),
    F.col("H2S").cast("double").alias("H2S"),
    F.col("SO2").cast("double").alias("SO2"),
    F.col("CO2").cast("double").alias("CO2"),
    F.col("CO").cast("double").alias("CO"),
    F.col("CustomSensor4").cast("double").alias("CustomSensor4"),
    F.col("CustomSensor5").cast("double").alias("CustomSensor5"),
    F.col("CreatedAt").alias("WatermarkUTC"),
    F.lit(EXT_SOURCE_ID).alias("ExtSourceID")
)

# Filter out records with NULL Station (SerialNo) since it's part of the composite key
gold_df = gold_df.filter(F.col("Station").isNotNull())

gold_count = gold_df.count()
print(f"Gold records after transformation: {gold_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Target Table

# COMMAND ----------

print("=" * 60)
print("STEP 3: Ensure Target Table Exists")
print("=" * 60)

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
            ProjectID INT NOT NULL,
            StationID INT,
            Station STRING NOT NULL,
            NetworkID INT NOT NULL,
            TimestampUTC TIMESTAMP NOT NULL,
            Rainfall DOUBLE,
            RainfallCumulative DOUBLE,
            Humidity DOUBLE,
            Temperature DOUBLE,
            AtmosphericPressure DOUBLE,
            WindSpeed DOUBLE,
            WindDirection DOUBLE,
            PM25 DOUBLE,
            PM10 DOUBLE,
            TSP DOUBLE,
            H2S DOUBLE,
            SO2 DOUBLE,
            CO2 DOUBLE,
            CO DOUBLE,
            CustomSensor4 DOUBLE,
            CustomSensor5 DOUBLE,
            WatermarkUTC TIMESTAMP,
            ExtSourceID INT
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

# COMMAND ----------

print("=" * 60)
print("STEP 4: Execute MERGE")
print("=" * 60)

# Create temp view for source
gold_df.createOrReplaceTempView("weather_source")

# Get target row count before merge
try:
    target_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0
print(f"Target rows before MERGE: {target_before}")

# Float comparison tolerance
FLOAT_TOLERANCE = 0.00001

# Execute MERGE with composite key (ProjectID, Station, NetworkID, TimestampUTC)
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING weather_source AS s
ON t.ProjectID = s.ProjectID
   AND t.Station = s.Station
   AND t.NetworkID = s.NetworkID
   AND t.TimestampUTC = s.TimestampUTC

WHEN MATCHED AND (
    -- Check if any sensor value changed beyond tolerance
    ABS(COALESCE(t.WindSpeed, 0) - COALESCE(s.WindSpeed, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.Rainfall, 0) - COALESCE(s.Rainfall, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.Temperature, 0) - COALESCE(s.Temperature, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.AtmosphericPressure, 0) - COALESCE(s.AtmosphericPressure, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.PM25, 0) - COALESCE(s.PM25, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.WindDirection, 0) - COALESCE(s.WindDirection, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.PM10, 0) - COALESCE(s.PM10, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.Humidity, 0) - COALESCE(s.Humidity, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.TSP, 0) - COALESCE(s.TSP, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.H2S, 0) - COALESCE(s.H2S, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.CustomSensor4, 0) - COALESCE(s.CustomSensor4, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.CustomSensor5, 0) - COALESCE(s.CustomSensor5, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.RainfallCumulative, 0) - COALESCE(s.RainfallCumulative, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.CO2, 0) - COALESCE(s.CO2, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.SO2, 0) - COALESCE(s.SO2, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.CO, 0) - COALESCE(s.CO, 0)) > {FLOAT_TOLERANCE} OR
    -- Also check for NULL to non-NULL transitions
    (t.WindSpeed IS NULL AND s.WindSpeed IS NOT NULL) OR (t.WindSpeed IS NOT NULL AND s.WindSpeed IS NULL) OR
    (t.Temperature IS NULL AND s.Temperature IS NOT NULL) OR (t.Temperature IS NOT NULL AND s.Temperature IS NULL)
)
THEN UPDATE SET
    t.StationID = s.StationID,
    t.Rainfall = s.Rainfall,
    t.RainfallCumulative = s.RainfallCumulative,
    t.Humidity = s.Humidity,
    t.Temperature = s.Temperature,
    t.AtmosphericPressure = s.AtmosphericPressure,
    t.WindSpeed = s.WindSpeed,
    t.WindDirection = s.WindDirection,
    t.PM25 = s.PM25,
    t.PM10 = s.PM10,
    t.TSP = s.TSP,
    t.H2S = s.H2S,
    t.SO2 = s.SO2,
    t.CO2 = s.CO2,
    t.CO = s.CO,
    t.CustomSensor4 = s.CustomSensor4,
    t.CustomSensor5 = s.CustomSensor5,
    t.WatermarkUTC = s.WatermarkUTC,
    t.ExtSourceID = s.ExtSourceID

WHEN NOT MATCHED THEN INSERT (
    ProjectID, StationID, Station, NetworkID, TimestampUTC,
    Rainfall, RainfallCumulative, Humidity, Temperature, AtmosphericPressure,
    WindSpeed, WindDirection, PM25, PM10, TSP, H2S, SO2, CO2, CO,
    CustomSensor4, CustomSensor5, WatermarkUTC, ExtSourceID
)
VALUES (
    s.ProjectID, s.StationID, s.Station, s.NetworkID, s.TimestampUTC,
    s.Rainfall, s.RainfallCumulative, s.Humidity, s.Temperature, s.AtmosphericPressure,
    s.WindSpeed, s.WindDirection, s.PM25, s.PM10, s.TSP, s.H2S, s.SO2, s.CO2, s.CO,
    s.CustomSensor4, s.CustomSensor5, s.WatermarkUTC, s.ExtSourceID
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
# MAGIC ## Step 5: Update Watermark

# COMMAND ----------

print("=" * 60)
print("STEP 5: Update Watermark")
print("=" * 60)

# Get max watermark from source
new_watermark = gold_df.agg(F.max("WatermarkUTC")).collect()[0][0]

if new_watermark is None:
    new_watermark = datetime.now()

update_watermark("gold_fact_weather_observations", new_watermark, target_after)
print(f"Watermark updated to: {new_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"")
print(f"Records processed: {gold_count}")
print(f"Target rows before: {target_before}")
print(f"Target rows after: {target_after}")
print(f"Estimated inserts: {inserted}")
print(f"")
print(f"Watermark: {new_watermark}")
print("=" * 60)

# Return success
dbutils.notebook.exit(f"SUCCESS: processed={gold_count}, inserted={inserted}, total={target_after}")
