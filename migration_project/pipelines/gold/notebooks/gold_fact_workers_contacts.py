# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Workers Contacts (Optimized)
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateFactWorkersContacts_ByRule` (951 lines)
# MAGIC
# MAGIC **Purpose:** Calculate worker contact/proximity metrics based on contact tracing rules.
# MAGIC Workers are considered in contact when they are on the same floor/zone within the
# MAGIC distance threshold specified by the ContactTracingRule.
# MAGIC
# MAGIC **Performance Optimizations:**
# MAGIC - Time-bucket approach for equi-joins (avoids expensive non-equi timestamp comparisons)
# MAGIC - Removed intermediate .count() calls that trigger full recomputation
# MAGIC - Adjacent bucket checking for boundary cases
# MAGIC - Lazy evaluation until final MERGE
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.gold.gold_fact_workers_history` (worker location history)
# MAGIC - `wakecap_prod.silver.silver_contact_tracing_rule` (proximity rules)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_fact_workers_contacts`

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
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables
SOURCE_WORKERS_HISTORY = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_workers_history"
SOURCE_CONTACT_RULE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_contact_tracing_rule"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_fact_workers_contacts"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID for this notebook
EXT_SOURCE_ID = 17  # TimescaleDB source

# Float comparison tolerance
FLOAT_TOLERANCE = 0.00001

# Contact time window in seconds (workers must be within X seconds to be considered in contact)
CONTACT_TIME_WINDOW_SECONDS = 180  # 3 minutes

# Time bucket size (should match or be smaller than CONTACT_TIME_WINDOW_SECONDS)
TIME_BUCKET_SECONDS = 180  # 3-minute buckets

print(f"Source Workers History: {SOURCE_WORKERS_HISTORY}")
print(f"Target: {TARGET_TABLE}")
print(f"Time bucket: {TIME_BUCKET_SECONDS} seconds")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "7", "Lookback Days")
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

# Check required source table exists (without counting - expensive!)
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_WORKERS_HISTORY} LIMIT 1").collect()
    print(f"[OK] Workers History exists: {SOURCE_WORKERS_HISTORY}")
except Exception as e:
    print(f"[ERROR] Workers History not found: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

# Check optional contact tracing rule table
has_rules_table = False
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_CONTACT_RULE} LIMIT 1").collect()
    has_rules_table = True
    print(f"[OK] Contact Tracing Rule exists")
except:
    print(f"[WARN] Contact Tracing Rule not found, using default")

# Ensure target schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")

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
    except:
        pass
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
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        print(f"  Warning: Could not update watermark: {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Contact Tracing Rules

# COMMAND ----------

print("=" * 60)
print("STEP 1: Load Contact Tracing Rules")
print("=" * 60)

# Load or create default rules
if has_rules_table:
    rules_df = spark.table(SOURCE_CONTACT_RULE) \
        .filter(F.col("Enabled") == True) \
        .select(
            F.col("ContactTracingRuleID"),
            F.col("ProjectId"),
            F.col("DateFrom"),
            F.col("DateTo"),
            F.col("Distance"),
        )

    if project_filter:
        rules_df = rules_df.filter(F.col("ProjectId") == project_filter)

    # Check if any rules exist (cheap operation on small table)
    if rules_df.limit(1).count() == 0:
        has_rules_table = False

if not has_rules_table:
    print("[INFO] Using default rule: 2m distance, all projects")
    default_schema = StructType([
        StructField("ContactTracingRuleID", IntegerType(), False),
        StructField("ProjectId", StringType(), True),
        StructField("DateFrom", TimestampType(), True),
        StructField("DateTo", TimestampType(), True),
        StructField("Distance", DoubleType(), True),
    ])
    rules_df = spark.createDataFrame([
        (0, None, datetime(2020, 1, 1), None, 2.0)
    ], schema=default_schema)

print("[OK] Rules loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Worker History with Time Buckets

# COMMAND ----------

print("=" * 60)
print("STEP 2: Load Worker History with Time Buckets")
print("=" * 60)

# Determine date range
if load_mode == "full":
    min_date = datetime(2020, 1, 1).date()
    max_date = datetime.now().date()
else:
    last_watermark = get_watermark("gold_fact_workers_contacts")
    print(f"Last watermark: {last_watermark}")
    max_date = datetime.now().date()
    min_date = max_date - timedelta(days=lookback_days)

print(f"Processing date range: {min_date} to {max_date}")

# Load worker history with TIME BUCKETS for efficient equi-joins
# This is the key optimization - bucket timestamps so we can use hash joins
history_df = spark.table(SOURCE_WORKERS_HISTORY) \
    .filter(F.col("ShiftLocalDate").between(min_date, max_date)) \
    .filter(F.col("WorkerId").isNotNull()) \
    .filter(F.col("FloorId").isNotNull()) \
    .select(
        F.col("ProjectId"),
        F.col("WorkerId"),
        F.col("ShiftLocalDate"),
        F.col("TimestampUTC"),
        F.col("FloorId"),
        F.col("ZoneId"),
        F.col("ActiveTime"),
        F.col("Latitude").alias("LocationX"),
        F.col("Longitude").alias("LocationY")
    ) \
    .withColumn(
        "TimeBucket",
        F.floor(F.unix_timestamp("TimestampUTC") / TIME_BUCKET_SECONDS)
    )

if project_filter:
    history_df = history_df.filter(F.col("ProjectId") == project_filter)

# DON'T count here - it's expensive! Just verify data exists
sample_exists = history_df.limit(1).count() > 0
if not sample_exists:
    print("[WARN] No worker history records in date range")
    dbutils.notebook.exit("NO_RECORDS_TO_PROCESS")

print("[OK] Worker history loaded with time buckets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Calculate Worker Contacts (Optimized with Time Buckets)

# COMMAND ----------

print("=" * 60)
print("STEP 3: Calculate Worker Contacts (Time-Bucket Optimized)")
print("=" * 60)

# OPTIMIZATION: Use time buckets for equi-joins instead of non-equi timestamp comparison
# This allows Spark to use efficient hash joins instead of cartesian products

# We need to check SAME bucket AND ADJACENT buckets (for boundary cases)
# Worker in bucket N could contact worker in bucket N or N+1

# Alias for self-join
w0 = history_df.alias("w0")
w1 = history_df.alias("w1")

# Join condition for SAME time bucket (most contacts)
same_bucket_conditions = [
    F.col("w0.ProjectId").cast("string") == F.col("w1.ProjectId").cast("string"),
    F.col("w0.ShiftLocalDate") == F.col("w1.ShiftLocalDate"),
    F.col("w0.FloorId").cast("string") == F.col("w1.FloorId").cast("string"),
    F.col("w0.TimeBucket") == F.col("w1.TimeBucket"),  # EQUI-JOIN on time bucket!
    F.col("w0.WorkerId").cast("string") < F.col("w1.WorkerId").cast("string"),
]

# Join condition for ADJACENT time bucket (boundary cases)
adjacent_bucket_conditions = [
    F.col("w0.ProjectId").cast("string") == F.col("w1.ProjectId").cast("string"),
    F.col("w0.ShiftLocalDate") == F.col("w1.ShiftLocalDate"),
    F.col("w0.FloorId").cast("string") == F.col("w1.FloorId").cast("string"),
    F.col("w0.TimeBucket") == F.col("w1.TimeBucket") - 1,  # Adjacent bucket
    F.col("w0.WorkerId").cast("string") < F.col("w1.WorkerId").cast("string"),
    # For adjacent buckets, verify timestamps are actually within window
    F.abs(F.unix_timestamp("w0.TimestampUTC") - F.unix_timestamp("w1.TimestampUTC")) <= CONTACT_TIME_WINDOW_SECONDS
]

# Select columns we need to avoid duplicate column names after self-join
def select_contact_columns(df):
    """Select and rename columns from self-join to avoid duplicates."""
    return df.select(
        F.col("w0.ProjectId").alias("w0_ProjectId"),
        F.col("w0.WorkerId").alias("w0_WorkerId"),
        F.col("w0.ShiftLocalDate").alias("w0_ShiftLocalDate"),
        F.col("w0.TimestampUTC").alias("w0_TimestampUTC"),
        F.col("w0.FloorId").alias("w0_FloorId"),
        F.col("w0.ZoneId").alias("w0_ZoneId"),
        F.col("w0.ActiveTime").alias("w0_ActiveTime"),
        F.col("w0.LocationX").alias("w0_LocationX"),
        F.col("w0.LocationY").alias("w0_LocationY"),
        F.col("w1.WorkerId").alias("w1_WorkerId"),
        F.col("w1.ActiveTime").alias("w1_ActiveTime"),
        F.col("w1.LocationX").alias("w1_LocationX"),
        F.col("w1.LocationY").alias("w1_LocationY"),
    )

# Perform both joins and union
print("  Joining same-bucket contacts...")
contacts_same = select_contact_columns(w0.join(w1, same_bucket_conditions, "inner"))

print("  Joining adjacent-bucket contacts...")
contacts_adjacent = select_contact_columns(w0.join(w1, adjacent_bucket_conditions, "inner"))

# Union results
contacts_raw = contacts_same.unionByName(contacts_adjacent)

# Calculate distance between workers (Euclidean on lat/lon - approximate)
contacts_with_distance = contacts_raw.withColumn(
    "Distance",
    F.when(
        F.col("w0_LocationX").isNotNull() & F.col("w1_LocationX").isNotNull(),
        F.sqrt(
            F.pow(F.col("w0_LocationX") - F.col("w1_LocationX"), 2) +
            F.pow(F.col("w0_LocationY") - F.col("w1_LocationY"), 2)
        ) * 111000  # Approximate meters per degree at equator
    ).otherwise(F.lit(None))
)

print("[OK] Contact pairs calculated with time-bucket optimization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply Contact Tracing Rules

# COMMAND ----------

print("=" * 60)
print("STEP 4: Apply Contact Tracing Rules")
print("=" * 60)

# Broadcast small rules table for efficient join
rules_broadcast = F.broadcast(rules_df)

# Apply rules filter (using renamed columns from self-join)
contacts_with_rules = contacts_with_distance.alias("c").crossJoin(
    rules_broadcast.alias("r")
).filter(
    # Project filter (NULL means all projects)
    (F.col("r.ProjectId").isNull() |
     (F.col("c.w0_ProjectId").cast("string") == F.col("r.ProjectId").cast("string"))) &
    # Date filter
    (F.col("c.w0_ShiftLocalDate") >= F.col("r.DateFrom")) &
    (F.col("r.DateTo").isNull() | (F.col("c.w0_ShiftLocalDate") <= F.col("r.DateTo"))) &
    # Distance filter
    (F.col("c.Distance").isNull() | F.col("r.Distance").isNull() |
     (F.col("c.Distance") <= F.col("r.Distance")))
)

print("[OK] Rules applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Aggregate Contact Metrics

# COMMAND ----------

print("=" * 60)
print("STEP 5: Aggregate Contact Metrics")
print("=" * 60)

# Aggregate contacts - this is where the real computation happens
# Using renamed columns (w0_ProjectId instead of w0.ProjectId)
contacts_aggregated = contacts_with_rules.groupBy(
    F.col("r.ContactTracingRuleID").alias("ContactTracingRuleID"),
    F.col("c.w0_ProjectId").alias("ProjectID"),
    F.col("c.w0_WorkerId").alias("WorkerID0"),
    F.col("c.w0_ShiftLocalDate").alias("LocalDate"),
    F.col("c.w1_WorkerId").alias("WorkerID"),
    F.col("c.w0_FloorId").alias("FloorID"),
    F.col("c.w0_ZoneId").alias("ZoneID")
).agg(
    F.min("c.w0_TimestampUTC").alias("FirstInteractionUTC"),
    F.count("*").alias("Interactions"),
    F.sum(
        F.coalesce(F.col("c.w0_ActiveTime"), F.lit(0)) +
        F.coalesce(F.col("c.w1_ActiveTime"), F.lit(0))
    ).alias("InteractionDuration"),
    F.avg("c.Distance").alias("AvgDistanceMeters")
)

# Add metadata
contacts_final = contacts_aggregated \
    .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID)) \
    .withColumn("WatermarkUTC", F.current_timestamp())

print("[OK] Aggregation defined (lazy - will execute during MERGE)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Ensure Target Table Exists

# COMMAND ----------

print("=" * 60)
print("STEP 6: Ensure Target Table Exists")
print("=" * 60)

# Target table schema - STRING IDs for UUID support
target_schema = """
    FactWorkersContactID BIGINT GENERATED ALWAYS AS IDENTITY,
    ContactTracingRuleID INT NOT NULL,
    ProjectID STRING NOT NULL,
    WorkerID0 STRING NOT NULL,
    LocalDate DATE NOT NULL,
    WorkerID STRING NOT NULL,
    FloorID STRING NOT NULL,
    FirstInteractionUTC TIMESTAMP NOT NULL,
    Interactions INT,
    InteractionDuration DOUBLE,
    AvgDistanceMeters DOUBLE,
    ZoneID STRING,
    ExtSourceID INT NOT NULL,
    WatermarkUTC TIMESTAMP,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
"""

# Check if table needs recreation (INT vs STRING schema)
table_needs_recreate = False
try:
    schema_df = spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    project_id_type = schema_df.filter("col_name = 'ProjectID'").select("data_type").collect()
    if project_id_type and project_id_type[0][0].upper() == "INT":
        print(f"[WARN] Table exists with INT schema, recreating with STRING")
        table_needs_recreate = True
    else:
        print(f"[OK] Target table exists: {TARGET_TABLE}")
except:
    table_needs_recreate = True

if table_needs_recreate:
    print(f"[INFO] Creating target table...")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    spark.sql(f"""
        CREATE TABLE {TARGET_TABLE} (
            {target_schema}
        )
        USING DELTA
        CLUSTER BY (ProjectID, LocalDate)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    print(f"[OK] Created target table: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Execute MERGE (Single Computation Point)

# COMMAND ----------

print("=" * 60)
print("STEP 7: Execute MERGE")
print("=" * 60)

# Create temp view for source
contacts_final.createOrReplaceTempView("contacts_source")

# Get count BEFORE merge (from existing table - cheap)
target_before = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Target rows before: {target_before:,}")

# Execute MERGE - this is where ALL the computation happens
# The entire DAG executes here in one optimized pass
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING contacts_source AS s
ON t.ContactTracingRuleID = s.ContactTracingRuleID
   AND t.ProjectID = s.ProjectID
   AND t.WorkerID0 = s.WorkerID0
   AND t.LocalDate = s.LocalDate
   AND t.WorkerID = s.WorkerID
   AND t.FloorID = s.FloorID

WHEN MATCHED AND (
    t.Interactions != s.Interactions OR
    ABS(COALESCE(t.InteractionDuration, 0) - COALESCE(s.InteractionDuration, 0)) > {FLOAT_TOLERANCE} OR
    ABS(COALESCE(t.AvgDistanceMeters, 0) - COALESCE(s.AvgDistanceMeters, 0)) > {FLOAT_TOLERANCE}
)
THEN UPDATE SET
    t.FirstInteractionUTC = s.FirstInteractionUTC,
    t.Interactions = s.Interactions,
    t.InteractionDuration = s.InteractionDuration,
    t.AvgDistanceMeters = s.AvgDistanceMeters,
    t.ZoneID = s.ZoneID,
    t.ExtSourceID = s.ExtSourceID,
    t.WatermarkUTC = current_timestamp(),
    t.UpdatedAt = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
    ContactTracingRuleID, ProjectID, WorkerID0, LocalDate, WorkerID, FloorID,
    FirstInteractionUTC, Interactions, InteractionDuration, AvgDistanceMeters,
    ZoneID, ExtSourceID, WatermarkUTC, CreatedAt
)
VALUES (
    s.ContactTracingRuleID, s.ProjectID, s.WorkerID0, s.LocalDate, s.WorkerID, s.FloorID,
    s.FirstInteractionUTC, s.Interactions, s.InteractionDuration, s.AvgDistanceMeters,
    s.ZoneID, s.ExtSourceID, current_timestamp(), current_timestamp()
)
"""

print("Executing MERGE (this triggers all computation)...")
spark.sql(merge_sql)

# Get counts AFTER merge
target_after = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"[OK] MERGE completed")
print(f"Target rows after: {target_after:,}")
print(f"Net change: {target_after - target_before:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Update Watermark

# COMMAND ----------

print("=" * 60)
print("STEP 8: Update Watermark")
print("=" * 60)

new_watermark = datetime.now()
update_watermark("gold_fact_workers_contacts", new_watermark, target_after)
print(f"Watermark updated to: {new_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_WORKERS_HISTORY}")
print(f"Target: {TARGET_TABLE}")
print(f"")
print(f"Date range: {min_date} to {max_date}")
print(f"Mode: {load_mode}")
print(f"Time bucket: {TIME_BUCKET_SECONDS} seconds")
print(f"")
print(f"Target table:")
print(f"  - Rows before: {target_before:,}")
print(f"  - Rows after: {target_after:,}")
print(f"  - Net change: {target_after - target_before:,}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: rows={target_after}")
