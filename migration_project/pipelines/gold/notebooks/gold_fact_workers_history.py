# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Workers History (Simplified)
# MAGIC
# MAGIC **Purpose:** Transform Silver layer DeviceLocation into enriched FactWorkersHistory
# MAGIC
# MAGIC **Source:** `wakecap_prod.silver.silver_fact_workers_history`
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
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check main source table
SOURCE_TABLE = f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_fact_workers_history"
try:
    # Check if table exists
    spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} LIMIT 0")
    source_exists = True
    print(f"[OK] Source table exists: {SOURCE_TABLE}")
except Exception as e:
    print(f"[FATAL] Source table not found: {SOURCE_TABLE}")
    print(f"       Error: {str(e)[:100]}")
    dbutils.notebook.exit(f"SOURCE_TABLE_NOT_FOUND: {SOURCE_TABLE}")

# Check Gold schema exists
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
    print(f"[OK] Target schema exists: {TARGET_CATALOG}.{TARGET_SCHEMA}")
except Exception as e:
    print(f"[WARN] Could not create/verify schema: {str(e)[:50]}")

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
# MAGIC ## Step 1: Load Source Data

# COMMAND ----------

print("Loading source data...")

if load_mode == "incremental":
    last_watermark = get_watermark("gold_fact_workers_history")
    cutoff_date = datetime.now() - timedelta(days=lookback_days)

    source_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("GeneratedAt") >= cutoff_date)
    )
    print(f"  Incremental load: GeneratedAt >= {cutoff_date}")
else:
    source_df = spark.table(SOURCE_TABLE)
    print("  Full load - processing all records")

# Apply optional project filter
if project_filter:
    source_df = source_df.filter(F.col("ProjectId") == int(project_filter))
    print(f"  Filtered to ProjectId: {project_filter}")

# Show schema for debugging
print("\nSource schema:")
source_df.printSchema()

source_count = source_df.count()
print(f"\nSource records to process: {source_count:,}")

if source_count == 0:
    print("No records to process. Exiting.")
    dbutils.notebook.exit("No records to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Basic Transformation
# MAGIC
# MAGIC Simplified transformation that doesn't require dimension table joins.

# COMMAND ----------

print("Applying transformations...")

# Parse PointWKT to Latitude/Longitude
# Format: "POINT (longitude latitude)" or "POINT(longitude latitude)"
transformed = (
    source_df
    # Parse coordinates from WKT
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
    # Add local timestamp (UTC for now - timezone conversion would need project dimension)
    .withColumn("LocalTimestamp", F.col("GeneratedAt"))
    .withColumn("LocalDate", F.to_date(F.col("GeneratedAt")))
    # ShiftLocalDate - use LocalDate for now (proper calculation would need shift lookup)
    .withColumn("ShiftLocalDate", F.to_date(F.col("GeneratedAt")))
    # Add placeholder columns for fields that require dimension joins
    .withColumn("WorkerId", F.lit(None).cast("int"))  # Would come from device_assignments
    .withColumn("CrewId", F.lit(None).cast("int"))    # Would come from crew_assignments
    .withColumn("WorkshiftId", F.lit(None).cast("int"))
    .withColumn("WorkshiftDetailsId", F.lit(None).cast("int"))
    .withColumn("TimeCategoryId", F.lit(2))  # 2 = No Shift Defined
    .withColumn("LocationAssignmentClassId", F.lit(None).cast("int"))
    .withColumn("ProductiveClassId", F.lit(None).cast("int"))
    # Convert time to days (original is in seconds)
    .withColumn(
        "ActiveTimeDays",
        F.when(F.col("IsActive") == True, F.col("ActiveTime") / 86400.0).otherwise(F.lit(0))
    )
    .withColumn(
        "InactiveTimeDays",
        F.when(F.col("IsActive") == False, F.col("InactiveTime") / 86400.0).otherwise(F.lit(0))
    )
)

print("  Transformations applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Select Final Columns

# COMMAND ----------

# Prepare final DataFrame
gold_fact_workers_history = (
    transformed
    .select(
        F.col("DeviceLocationId").alias("WorkersHistoryId"),
        F.col("GeneratedAt").alias("TimestampUTC"),
        "LocalDate",
        "ShiftLocalDate",
        "WorkerId",
        "ProjectId",
        F.col("FloorId").alias("ZoneId"),
        "FloorId",
        "CrewId",
        "WorkshiftId",
        "WorkshiftDetailsId",
        F.col("ActiveTimeDays").alias("ActiveTime"),
        F.col("InactiveTimeDays").alias("InactiveTime"),
        "Latitude",
        "Longitude",
        F.lit(0.0).alias("ErrorDistance"),
        F.col("ActiveSequence").alias("Sequence"),
        F.col("InactiveSequence").alias("SequenceInactive"),
        "TimeCategoryId",
        "LocationAssignmentClassId",
        "ProductiveClassId",
        F.lit(17).alias("ExtSourceId"),
        "CreatedAt",
        F.current_timestamp().alias("_processed_at"),
        F.col("GeneratedAt").alias("_loaded_at")
    )
)

print("Sample output:")
gold_fact_workers_history.show(5, truncate=False)

output_count = gold_fact_workers_history.count()
print(f"\nRecords to write: {output_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Delta Table

# COMMAND ----------

# Check if target table exists
try:
    table_exists = spark.catalog.tableExists(TARGET_TABLE)
except:
    table_exists = False

if table_exists:
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

    print(f"  Merged {output_count:,} records")
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

    print(f"  Created table with {output_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Update Watermark

# COMMAND ----------

# Get max timestamp from processed data
max_loaded_at = gold_fact_workers_history.agg(F.max("_loaded_at")).collect()[0][0]
final_count = spark.table(TARGET_TABLE).count()

update_watermark("gold_fact_workers_history", max_loaded_at, final_count)
print(f"Updated watermark to: {max_loaded_at}")
print(f"Total records in table: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Show summary
summary = spark.sql(f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT ProjectId) as unique_projects,
        MIN(ShiftLocalDate) as min_date,
        MAX(ShiftLocalDate) as max_date
    FROM {TARGET_TABLE}
""")

display(summary)

# COMMAND ----------

print("Gold Fact Workers History pipeline complete!")
dbutils.notebook.exit("SUCCESS")
