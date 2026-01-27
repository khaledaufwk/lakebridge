# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Location Group Assignments View
# MAGIC
# MAGIC **Converted from:** `dbo.vwLocationGroupAssignments`
# MAGIC
# MAGIC **Purpose:** Provide location group zone assignments with calculated ValidTo using LEAD.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - LEAD(CreatedAt) OVER (PARTITION BY LocationGroupId, ZoneId ORDER BY CreatedAt) as ValidTo
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_location_group_zone`
# MAGIC - `wakecap_prod.silver.silver_zone`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_location_group_assignments`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_LG_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_location_group_zone"
SOURCE_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_location_group_assignments"

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_LG_ZONE}").collect()[0][0]
    print(f"[OK] Location Group Zone: {cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Location Group Zone: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

has_zone = False
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_ZONE} LIMIT 0")
    has_zone = True
    print(f"[OK] Zone available")
except:
    print(f"[WARN] Zone not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

print("=" * 60)
print("BUILD LOCATION GROUP ASSIGNMENTS VIEW")
print("=" * 60)

# Load location group zone assignments
# Note: uses CreatedAt, not ValidFrom
assign_df = spark.table(SOURCE_LG_ZONE)
print(f"Base assignments: {assign_df.count():,}")

# Rename CreatedAt to ValidFrom for consistency
assign_df = assign_df.withColumn("ValidFrom", F.col("CreatedAt"))

# Calculate ValidTo using LEAD
assign_window = Window.partitionBy("LocationGroupId", "ZoneId").orderBy("ValidFrom")

assign_df = assign_df.withColumn(
    "ValidTo_Calculated",
    F.lead("ValidFrom").over(assign_window)
).withColumn(
    "ValidTo_Final",
    F.col("ValidTo_Calculated")
)

# Add zone details (silver_zone has ZoneName column)
if has_zone:
    zone_df = spark.table(SOURCE_ZONE).select(
        F.col("ZoneId").alias("z_ZoneId"),
        F.col("ZoneName"),
        F.col("FloorId")
    )
    assign_df = assign_df.join(zone_df, F.col("ZoneId") == F.col("z_ZoneId"), "left").drop("z_ZoneId")
else:
    assign_df = assign_df \
        .withColumn("ZoneName", F.lit(None).cast("string")) \
        .withColumn("FloorId", F.lit(None).cast("int"))

# Add computed columns
assign_df = assign_df \
    .withColumn("IsCurrent", F.when(F.col("ValidTo_Final").isNull(), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("AssignmentDurationDays",
        F.when(F.col("ValidTo_Final").isNotNull(),
               F.datediff(F.col("ValidTo_Final"), F.col("ValidFrom")))
    ) \
    .withColumn("_view_generated_at", F.current_timestamp()) \
    .drop("ValidTo_Calculated")

print(f"Final view: {assign_df.count():,} rows")

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

assign_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Rows: {after_count:,}")

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
