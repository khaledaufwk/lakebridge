# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Manager Assignments Expanded
# MAGIC
# MAGIC **Equivalent to:** `dbo.vwManagerAssignments_Expanded` view
# MAGIC
# MAGIC **Purpose:** Create an expanded view of manager assignments by joining
# MAGIC crew composition (workers in crews) with crew managers (who manages each crew).
# MAGIC This provides Worker -> Manager relationships through the Crew linkage.
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_crew_manager` - Crew to Manager mappings
# MAGIC - `wakecap_prod.silver.silver_crew_composition` - Worker to Crew mappings
# MAGIC
# MAGIC **Target Table:** `wakecap_prod.silver.silver_manager_assignments_expanded`
# MAGIC
# MAGIC **Schema:**
# MAGIC - ProjectId: STRING
# MAGIC - CrewId: INT
# MAGIC - WorkerId: INT
# MAGIC - ManagerWorkerId: INT
# MAGIC - ValidFrom_ShiftLocalDate: DATE
# MAGIC - ValidTo_ShiftLocalDate: DATE (NULL for current assignments)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and Schema Configuration
TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
MIGRATION_SCHEMA = "migration"

# Source tables
SOURCE_CREW_MANAGER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_manager"
SOURCE_CREW_COMPOSITION = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_manager_assignments_expanded"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

print(f"Source Crew Manager: {SOURCE_CREW_MANAGER}")
print(f"Source Crew Composition: {SOURCE_CREW_COMPOSITION}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")
print(f"Load Mode: {load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check source tables
source_tables = [
    ("Crew Manager", SOURCE_CREW_MANAGER),
    ("Crew Composition", SOURCE_CREW_COMPOSITION),
]

sources_available = True
for name, table in source_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
    except Exception as e:
        print(f"[FATAL] {name} not found: {table}")
        sources_available = False

if not sources_available:
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Ensure target schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print(f"[OK] Target schemas verified")
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
# MAGIC ## Step 1: Load Source Tables

# COMMAND ----------

print("=" * 60)
print("STEP 1: Load Source Tables")
print("=" * 60)

# Load crew manager (who manages each crew)
crew_manager_df = spark.table(SOURCE_CREW_MANAGER)
crew_manager_count = crew_manager_df.count()
print(f"Crew Manager records: {crew_manager_count:,}")

# Load crew composition (workers in each crew)
crew_composition_df = spark.table(SOURCE_CREW_COMPOSITION)
crew_composition_count = crew_composition_df.count()
print(f"Crew Composition records: {crew_composition_count:,}")

# Get watermarks for incremental processing
new_mgr_watermark = crew_manager_df.agg(F.max("UpdatedAt")).collect()[0][0]
if new_mgr_watermark is None:
    new_mgr_watermark = datetime.now()

new_comp_watermark = crew_composition_df.agg(F.max("UpdatedAt")).collect()[0][0]
if new_comp_watermark is None:
    new_comp_watermark = datetime.now()

print(f"Crew Manager max UpdatedAt: {new_mgr_watermark}")
print(f"Crew Composition max UpdatedAt: {new_comp_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare Workers DataFrame
# MAGIC
# MAGIC Get distinct workers from crew composition, excluding deleted records.

# COMMAND ----------

print("=" * 60)
print("STEP 2: Prepare Workers DataFrame")
print("=" * 60)

# Get workers from crew composition (who has a manager)
# Filter out deleted records (DeletedAt is not null means soft-deleted)
workers_df = crew_composition_df.filter(
    F.col("DeletedAt").isNull()
).select(
    F.col("ProjectId"),
    F.col("CrewId"),
    F.col("WorkerId")
).distinct()

workers_count = workers_df.count()
print(f"Active workers in crews: {workers_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Prepare Managers DataFrame
# MAGIC
# MAGIC Get manager assignments from crew_manager table.
# MAGIC Note: silver_crew_manager doesn't have DeletedAt column, so all records are valid.

# COMMAND ----------

print("=" * 60)
print("STEP 3: Prepare Managers DataFrame")
print("=" * 60)

# CrewManager links CrewId to ManagerId (ManagerId is a WorkerId who manages the crew)
# Note: silver_crew_manager doesn't have DeletedAt column
managers_df = crew_manager_df.select(
    F.col("ProjectId"),
    F.col("CrewId"),
    F.col("ManagerId").alias("ManagerWorkerId"),
    F.col("EffectiveDate").alias("ValidFrom_ShiftLocalDate"),
    F.col("UpdatedAt")
).withColumn(
    "ValidTo_ShiftLocalDate", F.lit(None).cast("date")  # Current assignments are open-ended
)

managers_count = managers_df.count()
print(f"Manager assignments: {managers_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Expanded Assignments
# MAGIC
# MAGIC Join workers with managers through CrewId to create
# MAGIC Worker -> Manager relationships (equivalent to vwManagerAssignments_Expanded).

# COMMAND ----------

print("=" * 60)
print("STEP 4: Create Expanded Assignments")
print("=" * 60)

# Create expanded assignments: Worker -> Manager relationships via Crew
# This is equivalent to dbo.vwManagerAssignments_Expanded
expanded_assignments = workers_df.alias("w").join(
    managers_df.alias("m"),
    (F.col("w.ProjectId") == F.col("m.ProjectId")) &
    (F.col("w.CrewId") == F.col("m.CrewId")),
    "inner"
).select(
    F.col("w.ProjectId"),
    F.col("w.CrewId"),
    F.col("w.WorkerId"),
    F.col("m.ManagerWorkerId"),
    F.col("m.ValidFrom_ShiftLocalDate"),
    F.col("m.ValidTo_ShiftLocalDate"),
    F.col("m.UpdatedAt"),
    F.current_timestamp().alias("_silver_processed_at")
)

expanded_count = expanded_assignments.count()
print(f"Expanded assignments (Worker -> Manager): {expanded_count:,}")

# Show sample
print("\nSample expanded assignments:")
expanded_assignments.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to Target Table

# COMMAND ----------

print("=" * 60)
print("STEP 5: Write to Target Table")
print("=" * 60)

# Check if target exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

if target_exists and load_mode == "incremental":
    print(f"Target exists, performing incremental MERGE...")

    # For incremental, we do a full replace since the view logic is based on current state
    # The source tables themselves handle incremental updates
    expanded_assignments.createOrReplaceTempView("source_expanded")

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING source_expanded AS s
        ON t.ProjectId = s.ProjectId
           AND t.CrewId = s.CrewId
           AND t.WorkerId = s.WorkerId
           AND t.ManagerWorkerId = s.ManagerWorkerId
           AND t.ValidFrom_ShiftLocalDate = s.ValidFrom_ShiftLocalDate

        WHEN MATCHED AND (
            NOT (t.ValidTo_ShiftLocalDate <=> s.ValidTo_ShiftLocalDate) OR
            t.UpdatedAt < s.UpdatedAt
        )
        THEN UPDATE SET
            t.ValidTo_ShiftLocalDate = s.ValidTo_ShiftLocalDate,
            t.UpdatedAt = s.UpdatedAt,
            t._silver_processed_at = s._silver_processed_at

        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)
    spark.catalog.dropTempView("source_expanded")
    print("  MERGE completed")

else:
    print(f"Creating/replacing table: {TARGET_TABLE}")

    (expanded_assignments
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TARGET_TABLE)
    )
    print(f"  Table created with {expanded_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Update Watermarks

# COMMAND ----------

print("=" * 60)
print("STEP 6: Update Watermarks")
print("=" * 60)

final_count = spark.table(TARGET_TABLE).count()

# Use the max of both source watermarks
combined_watermark = max(new_mgr_watermark, new_comp_watermark)
update_watermark("silver_manager_assignments_expanded", combined_watermark, final_count)

print(f"Updated watermark to: {combined_watermark}")
print(f"Total records: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 60)
print("VALIDATION")
print("=" * 60)

# Row counts
print(f"\nRow Counts:")
print(f"  Source crew_manager: {crew_manager_count:,}")
print(f"  Source crew_composition: {crew_composition_count:,}")
print(f"  Active workers: {workers_count:,}")
print(f"  Target expanded: {final_count:,}")

# Check data quality
print(f"\nData Quality Check:")
quality_check = spark.sql(f"""
    SELECT
        COUNT(*) as total,
        COUNT(DISTINCT ProjectId) as projects,
        COUNT(DISTINCT WorkerId) as workers,
        COUNT(DISTINCT ManagerWorkerId) as managers,
        SUM(CASE WHEN WorkerId = ManagerWorkerId THEN 1 ELSE 0 END) as self_managed
    FROM {TARGET_TABLE}
""")
display(quality_check)

# Sample output
print(f"\nSample Output:")
display(spark.table(TARGET_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("SILVER MANAGER ASSIGNMENTS EXPANDED - COMPLETE")
print("=" * 60)
print(f"""
Processing Summary:
  Load Mode:              {load_mode}
  Source Crew Manager:    {crew_manager_count:,} records
  Source Crew Composition: {crew_composition_count:,} records
  Active Workers:         {workers_count:,}

Target Table:
  {TARGET_TABLE}
  Total rows: {final_count:,}

Equivalent to: dbo.vwManagerAssignments_Expanded
Purpose: Maps Worker -> Manager via Crew relationships
""")

dbutils.notebook.exit(f"SUCCESS: {final_count} records")
