# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Workers History (Enhanced)
# MAGIC
# MAGIC **Purpose:** Transform Silver layer DeviceLocation into enriched FactWorkersHistory
# MAGIC
# MAGIC **Enhancements over simplified version:**
# MAGIC - WorkerId resolution via silver_resource_device temporal join
# MAGIC - CrewId resolution via silver_crew_composition
# MAGIC - Proper dimension joins for enrichment
# MAGIC
# MAGIC **Source:** `wakecap_prod.silver.silver_fact_workers_history`
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_workers_history`
# MAGIC
# MAGIC **Migration Reference:** stg.spDeltaSyncFactWorkersHistory

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

# Source tables
SOURCE_TABLE = f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_fact_workers_history"
RESOURCE_DEVICE_TABLE = f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_resource_device"
CREW_COMPOSITION_TABLE = f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_crew_composition"
PROJECT_TABLE = f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_project"
WORKER_TABLE = f"{TARGET_CATALOG}.{SOURCE_SCHEMA}.silver_worker"

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

required_tables = [
    SOURCE_TABLE,
    RESOURCE_DEVICE_TABLE,
    CREW_COMPOSITION_TABLE,
]

for table in required_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {table}")
    except Exception as e:
        print(f"[WARN] {table} - {str(e)[:50]}")

# Create Gold schema if not exists
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
    source_df = source_df.filter(F.col("ProjectId") == project_filter)
    print(f"  Filtered to ProjectId: {project_filter}")

source_count = source_df.count()
print(f"\nSource records to process: {source_count:,}")

if source_count == 0:
    print("No records to process. Exiting.")
    dbutils.notebook.exit("No records to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Dimension Tables

# COMMAND ----------

print("Loading dimension tables...")

# Load resource_device for WorkerId lookup (temporal join)
resource_device_df = (
    spark.table(RESOURCE_DEVICE_TABLE)
    .select(
        F.col("DeviceId"),
        F.col("WorkerId").alias("RD_WorkerId"),
        F.col("AssignedAt"),
        F.col("UnassignedAt")
    )
)
print(f"  Resource Device records: {resource_device_df.count():,}")

# Load crew composition for CrewId lookup
# Get the latest crew assignment per worker (where DeletedAt is NULL)
crew_composition_df = (
    spark.table(CREW_COMPOSITION_TABLE)
    .filter(F.col("DeletedAt").isNull())
    .select(
        F.col("WorkerId").alias("CC_WorkerId"),
        F.col("CrewId"),
        F.col("ProjectId").alias("CC_ProjectId")
    )
)
print(f"  Crew Composition records: {crew_composition_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Join with Dimensions

# COMMAND ----------

print("Joining with dimension tables...")

# Step 3a: Join with resource_device to get WorkerId
# This is a temporal join - match device assignments valid at the time of observation
enriched_df = (
    source_df
    .join(
        resource_device_df,
        (source_df.DeviceId == resource_device_df.DeviceId) &
        (source_df.GeneratedAt >= resource_device_df.AssignedAt) &
        ((source_df.GeneratedAt < resource_device_df.UnassignedAt) |
         (resource_device_df.UnassignedAt.isNull())),
        "left"
    )
    .drop(resource_device_df.DeviceId)
)

# Count records with WorkerId
worker_match_count = enriched_df.filter(F.col("RD_WorkerId").isNotNull()).count()
print(f"  Records with WorkerId: {worker_match_count:,} ({worker_match_count/source_count*100:.1f}%)")

# Step 3b: Join with crew_composition to get CrewId
enriched_df = (
    enriched_df
    .join(
        crew_composition_df,
        (enriched_df.RD_WorkerId == crew_composition_df.CC_WorkerId) &
        (enriched_df.ProjectId == crew_composition_df.CC_ProjectId),
        "left"
    )
    .drop("CC_WorkerId", "CC_ProjectId")
)

crew_match_count = enriched_df.filter(F.col("CrewId").isNotNull()).count()
print(f"  Records with CrewId: {crew_match_count:,} ({crew_match_count/source_count*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply Transformations

# COMMAND ----------

print("Applying transformations...")

# Parse PointWKT to Latitude/Longitude
# Format: "POINT (longitude latitude)" or "POINT(longitude latitude)"
transformed = (
    enriched_df
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
    # Add local timestamp (UTC for now - timezone conversion requires project timezone data)
    .withColumn("LocalTimestamp", F.col("GeneratedAt"))
    .withColumn("LocalDate", F.to_date(F.col("GeneratedAt")))
    # ShiftLocalDate - use LocalDate for now (proper calculation requires FactWorkersShifts)
    .withColumn("ShiftLocalDate", F.to_date(F.col("GeneratedAt")))
    # Rename WorkerId from join
    .withColumn("WorkerId", F.col("RD_WorkerId"))
    # Add placeholder columns for fields requiring additional processing
    .withColumn("WorkshiftId", F.lit(None).cast("int"))
    .withColumn("WorkshiftDetailsId", F.lit(None).cast("int"))
    .withColumn("TimeCategoryId", F.lit(2))  # 2 = No Shift Defined (default)
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
# MAGIC ## Step 5: Select Final Columns

# COMMAND ----------

# Prepare final DataFrame matching FactWorkersHistory schema
gold_fact_workers_history = (
    transformed
    .select(
        F.col("DeviceLocationId").alias("WorkersHistoryId"),
        F.col("GeneratedAt").alias("TimestampUTC"),
        "LocalDate",
        "ShiftLocalDate",
        "WorkerId",
        F.col("ProjectId").cast("int").alias("ProjectId"),
        F.col("FloorId").alias("ZoneId"),  # ZoneId mapping requires additional lookup
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
# MAGIC ## Step 6: Write to Delta Table

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
# MAGIC ## Step 7: Update Watermark

# COMMAND ----------

# Get max timestamp from processed data
max_loaded_at = gold_fact_workers_history.agg(F.max("_loaded_at")).collect()[0][0]
final_count = spark.table(TARGET_TABLE).count()

update_watermark("gold_fact_workers_history", max_loaded_at, final_count)
print(f"Updated watermark to: {max_loaded_at}")
print(f"Total records in table: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

# Show summary statistics
summary = spark.sql(f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(WorkerId) as records_with_worker,
        COUNT(CrewId) as records_with_crew,
        COUNT(DISTINCT WorkerId) as unique_workers,
        COUNT(DISTINCT ProjectId) as unique_projects,
        MIN(ShiftLocalDate) as min_date,
        MAX(ShiftLocalDate) as max_date
    FROM {TARGET_TABLE}
""")

print("\n" + "=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)
display(summary)

# COMMAND ----------

# Data quality metrics
print("\nData Quality Metrics:")
total = spark.table(TARGET_TABLE).count()
with_worker = spark.table(TARGET_TABLE).filter(F.col("WorkerId").isNotNull()).count()
with_crew = spark.table(TARGET_TABLE).filter(F.col("CrewId").isNotNull()).count()

print(f"  Total records: {total:,}")
print(f"  With WorkerId: {with_worker:,} ({with_worker/total*100:.1f}%)")
print(f"  With CrewId: {with_crew:,} ({with_crew/total*100:.1f}%)")

# COMMAND ----------

print("\nGold Fact Workers History (Enhanced) pipeline complete!")
dbutils.notebook.exit("SUCCESS")
