# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Zone Dimension View
# MAGIC
# MAGIC **Converted from:** `dbo.vwZone`
# MAGIC
# MAGIC **Purpose:** Provide a consolidated zone dimension view with floor details,
# MAGIC location group assignments, and zone category information.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Joins Zone with Floor for floor details
# MAGIC - Gets current LocationGroupID from LocationGroupZone assignments
# MAGIC - Includes zone category for classification
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_zone`
# MAGIC - `wakecap_prod.silver.silver_floor`
# MAGIC - `wakecap_prod.silver.silver_location_group_zone`
# MAGIC - `wakecap_prod.silver.silver_zone_category`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_zone`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone"
SOURCE_FLOOR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_floor"
SOURCE_LG_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_location_group_zone"
SOURCE_ZONE_CATEGORY = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone_category"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_zone"

print(f"Source Zone: {SOURCE_ZONE}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    zone_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_ZONE}").collect()[0][0]
    print(f"[OK] Zone: {zone_cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Zone: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional tables
opt_status = {}
for name, table in [
    ("Floor", SOURCE_FLOOR),
    ("Location Group Zone", SOURCE_LG_ZONE),
    ("Zone Category", SOURCE_ZONE_CATEGORY),
]:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        opt_status[name] = True
        print(f"[OK] {name} available")
    except:
        opt_status[name] = False
        print(f"[WARN] {name} not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Zone View

# COMMAND ----------

print("=" * 60)
print("BUILD ZONE VIEW")
print("=" * 60)

# Load base zone table
zone_df = spark.table(SOURCE_ZONE)
print(f"Base zones: {zone_df.count():,}")

# Join with floor
# Note: IDs may be UUID (string) - cast to string for safe comparison
if opt_status.get("Floor"):
    floor_df = spark.table(SOURCE_FLOOR).select(
        F.col("FloorId").cast("string").alias("f_FloorId"),
        F.col("FloorName")
    )

    zone_df = zone_df.alias("z").join(
        floor_df.alias("f"),
        F.col("z.FloorId").cast("string") == F.col("f.f_FloorId"),
        "left"
    ).drop("f_FloorId")
    print("[OK] Floor joined")
else:
    zone_df = zone_df \
        .withColumn("FloorName", F.lit(None).cast("string"))
    print("[SKIP] Floor not available")

# Get current location group assignment (latest by CreatedAt)
if opt_status.get("Location Group Zone"):
    lg_zone_df = spark.table(SOURCE_LG_ZONE)

    # Get latest assignment per zone (no soft-delete, use CreatedAt ordering)
    lg_window = Window.partitionBy("ZoneId").orderBy(F.col("CreatedAt").desc())

    current_lg = lg_zone_df \
        .withColumn("_rn", F.row_number().over(lg_window)) \
        .filter(F.col("_rn") == 1) \
        .select(
            F.col("ZoneId").cast("string").alias("lg_ZoneId"),
            F.col("LocationGroupId")
        )

    zone_df = zone_df.join(
        current_lg,
        F.col("ZoneId").cast("string") == F.col("lg_ZoneId"),
        "left"
    ).drop("lg_ZoneId")
    print("[OK] Location group joined")
else:
    zone_df = zone_df.withColumn("LocationGroupId", F.lit(None).cast("int"))
    print("[SKIP] Location group not available")

# Join with zone category (silver_zone_category has Id and ZoneCategoryName)
if opt_status.get("Zone Category"):
    category_df = spark.table(SOURCE_ZONE_CATEGORY).select(
        F.col("Id").cast("string").alias("zc_Id"),
        F.col("ZoneCategoryName")
    )

    zone_df = zone_df.join(
        category_df,
        F.col("ZoneCategoryId").cast("string") == F.col("zc_Id"),
        "left"
    ).drop("zc_Id")
    print("[OK] Zone category joined")
else:
    zone_df = zone_df.withColumn("ZoneCategoryName", F.lit(None).cast("string"))
    print("[SKIP] Zone category not available")

# Add computed columns
zone_df = zone_df.withColumn(
    "ZoneDisplayName",
    F.concat(
        F.coalesce(F.col("ZoneName"), F.lit("")),
        F.when(F.col("FloorName").isNotNull(),
               F.concat(F.lit(" ("), F.col("FloorName"), F.lit(")")))
         .otherwise(F.lit(""))
    )
)

# Add metadata
zone_df = zone_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final zone view: {zone_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Target

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

zone_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

print(f"Rows before: {before_count:,}")
print(f"Rows after: {after_count:,}")

# COMMAND ----------

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_ZONE}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
