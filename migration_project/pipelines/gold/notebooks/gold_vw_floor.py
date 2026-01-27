# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Floor Dimension View
# MAGIC
# MAGIC **Converted from:** `dbo.vwFloor`
# MAGIC
# MAGIC **Purpose:** Provide a consolidated floor dimension view with computed floor ordering,
# MAGIC basement detection, and floor number extraction.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Complex floor ordering with basement detection (B prefix = negative)
# MAGIC - Uses fnStripNonNumerics equivalent for floor number extraction
# MAGIC - Floor type prefixes: G (Ground), M (Mezzanine), P (Podium), R (Roof)
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_floor`
# MAGIC - `wakecap_prod.silver.silver_project`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_floor`

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

SOURCE_FLOOR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_floor"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_floor"

print(f"Source Floor: {SOURCE_FLOOR}")
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
    floor_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_FLOOR}").collect()[0][0]
    print(f"[OK] Floor: {floor_cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Floor: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Optional
has_project = False
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_PROJECT} LIMIT 0")
    has_project = True
    print(f"[OK] Project available")
except:
    print(f"[WARN] Project not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Floor View

# COMMAND ----------

print("=" * 60)
print("BUILD FLOOR VIEW")
print("=" * 60)

# Load base floor table
floor_df = spark.table(SOURCE_FLOOR)
print(f"Base floors: {floor_df.count():,}")

# Join with project
# Note: ProjectId may be UUID (string) in some tables - cast to string for safe comparison
if has_project:
    project_df = spark.table(SOURCE_PROJECT).select(
        F.col("ProjectId").cast("string").alias("p_ProjectId"),
        F.col("ProjectName")
    )

    floor_df = floor_df.alias("f").join(
        project_df.alias("p"),
        F.col("f.ProjectId").cast("string") == F.col("p.p_ProjectId"),
        "left"
    ).drop("p_ProjectId")
    print("[OK] Project joined")
else:
    floor_df = floor_df.withColumn("ProjectName", F.lit(None).cast("string"))
    print("[SKIP] Project not available")

# Extract numeric part from floor name (fnStripNonNumerics equivalent)
# Remove all non-numeric characters
# Note: Use try_cast to handle empty strings AND overflow (numbers > INT max)
# Some FloorNames may contain large numeric IDs that exceed INT range
floor_df = floor_df.withColumn(
    "_numeric_str",
    F.regexp_replace(F.col("FloorName"), "[^0-9]", "")
).withColumn(
    "FloorNumericPart",
    F.when(
        (F.col("_numeric_str").isNotNull()) & (F.col("_numeric_str") != "") & (F.length("_numeric_str") <= 9),
        F.col("_numeric_str").cast("int")
    ).otherwise(F.lit(None).cast("int"))
).drop("_numeric_str")

# Determine floor sign (B = basement = negative)
floor_df = floor_df.withColumn(
    "FloorSign",
    F.when(F.upper(F.substring("FloorName", 1, 1)) == "B", F.lit(-1))
     .otherwise(F.lit(1))
)

# Determine floor type offset for ordering
# G (Ground) = 0, M (Mezzanine) = 10, P (Podium) = 50, R (Roof) = 500
floor_df = floor_df.withColumn(
    "FloorTypeOffset",
    F.when(F.upper(F.substring("FloorName", 1, 1)) == "G", F.lit(0))
     .when(F.upper(F.substring("FloorName", 1, 1)) == "M", F.lit(10))
     .when(F.upper(F.substring("FloorName", 1, 1)) == "P", F.lit(50))
     .when(F.upper(F.substring("FloorName", 1, 1)) == "R", F.lit(500))
     .otherwise(F.lit(0))
)

# Calculate floor order
# Order = Sign * (NumericPart + TypeOffset)
floor_df = floor_df.withColumn(
    "FloorOrder",
    F.col("FloorSign") * (
        F.coalesce(F.col("FloorNumericPart"), F.lit(0)) +
        F.col("FloorTypeOffset")
    )
)

# Determine if basement
floor_df = floor_df.withColumn(
    "IsBasement",
    F.when(F.upper(F.substring("FloorName", 1, 1)) == "B", F.lit(True))
     .otherwise(F.lit(False))
)

# Determine floor category
floor_df = floor_df.withColumn(
    "FloorCategory",
    F.when(F.upper(F.substring("FloorName", 1, 1)) == "B", F.lit("Basement"))
     .when(F.upper(F.substring("FloorName", 1, 1)) == "G", F.lit("Ground"))
     .when(F.upper(F.substring("FloorName", 1, 1)) == "M", F.lit("Mezzanine"))
     .when(F.upper(F.substring("FloorName", 1, 1)) == "P", F.lit("Podium"))
     .when(F.upper(F.substring("FloorName", 1, 1)) == "R", F.lit("Roof"))
     .otherwise(F.lit("Standard"))
)

# Add computed floor display name
floor_df = floor_df.withColumn(
    "FloorDisplayName",
    F.concat(
        F.coalesce(F.col("FloorName"), F.lit("")),
        F.lit(" ("),
        F.col("FloorCategory"),
        F.lit(")")
    )
)

# Cleanup intermediate columns
floor_df = floor_df.drop("FloorSign", "FloorTypeOffset", "FloorNumericPart")

# Add metadata
floor_df = floor_df.withColumn("_view_generated_at", F.current_timestamp())

print(f"Final floor view: {floor_df.count():,} rows")

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

floor_df.write \
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
print(f"Source: {SOURCE_FLOOR}")
print(f"Target: {TARGET_TABLE}")
print(f"Rows: {after_count:,}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
