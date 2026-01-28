# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Zone H3 Coverage
# MAGIC
# MAGIC Pre-computes H3 hexagonal cell coverage for all zones.
# MAGIC This enables efficient spatial joins by converting polygon containment
# MAGIC to simple index equality joins.
# MAGIC
# MAGIC **Source:** `wakecap_prod.raw.timescale_zone` (Bronze layer with CoordinatesWKT)
# MAGIC **Target:** `silver_zone_h3_coverage` (zone_id to h3_index mapping)
# MAGIC
# MAGIC **H3 Resolution:** 9 (~175m hexagons)

# COMMAND ----------

# MAGIC %pip install h3 shapely --quiet

# COMMAND ----------

# Restart Python to pick up newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

import h3
from shapely import wkt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# COMMAND ----------

# Configuration
TARGET_CATALOG = "wakecap_prod"
RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"
SOURCE_TABLE = f"{TARGET_CATALOG}.{RAW_SCHEMA}.timescale_zone"
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
# MAGIC ## Read Zone Polygons from Bronze
# MAGIC
# MAGIC Reading from Bronze layer where CoordinatesWKT contains the WKT polygon string

# COMMAND ----------

# Read zones from Bronze - CoordinatesWKT contains the geometry as WKT text
zone_df = spark.table(SOURCE_TABLE).filter(
    (F.col("CoordinatesWKT").isNotNull()) &
    (F.col("DeletedAt").isNull())
).select(
    F.col("Id").cast("string").alias("ZoneId"),
    F.col("SpaceId").cast("string").alias("FloorId"),
    F.col("ProjectId").cast("string").alias("ProjectId"),
    F.col("CoordinatesWKT").alias("Coordinates")  # WKT polygon from ST_AsText
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

# For full load, drop and recreate; for incremental, merge
if load_mode == "full":
    # Drop existing table for clean slate
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    print(f"Dropped existing table (if any)")

    # Write fresh data
    zone_h3_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

    # Add comment to table
    spark.sql(f"COMMENT ON TABLE {TARGET_TABLE} IS 'H3 hexagonal cell coverage for zones - enables spatial joins'")
    print(f"Full load complete")
else:
    # Create table if not exists for incremental
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
