# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Dimension Project (DeltaSync)
# MAGIC
# MAGIC **Converted from:** `stg.spDeltaSyncDimProject` (198 lines)
# MAGIC
# MAGIC **Purpose:** Sync project dimension from TimescaleDB source (wc2023_Project)
# MAGIC to the silver Project dimension table with dimension resolution.
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - ROW_NUMBER deduplication on source Id
# MAGIC - LEFT JOIN dimension lookups (TimeZoneMapping, Organization) with fnExtSourceIDAlias
# MAGIC - Calculated column: TRIM(Name) for project name
# MAGIC - MERGE with change detection on all comparable columns
# MAGIC - Post-MERGE UPDATE to fill NULL dimension IDs
# MAGIC
# MAGIC **Source Table:** `wakecap_prod.bronze.wc2023_project` (stg.wc2023_Project)
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC - `wakecap_prod.silver.silver_timezone_mapping` (dbo.TimeZoneMapping)
# MAGIC - `wakecap_prod.silver.silver_organization` (dbo.Organization)
# MAGIC
# MAGIC **Target:** `wakecap_prod.silver.silver_project`
# MAGIC **Watermarks:** `wakecap_prod.migration._gold_watermarks`
# MAGIC
# MAGIC **ExtSourceID:** 14

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
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
MIGRATION_SCHEMA = "migration"

# Source table (TimescaleDB)
SOURCE_PROJECT = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_project"

# Dimension tables for lookups
DIM_TIMEZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_timezone_mapping"
DIM_ORGANIZATION = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_organization"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID
EXT_SOURCE_ID = 14

print(f"Source: {SOURCE_PROJECT}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")
print(f"Load Mode: {load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def fn_ext_source_id_alias(col_name):
    """PySpark equivalent of stg.fnExtSourceIDAlias(ExtSourceID)."""
    return F.when(F.col(col_name).isin(15, 18), F.lit(15)) \
            .when(F.col(col_name).isin(1, 2, 10, 14, 21), F.lit(1)) \
            .otherwise(F.col(col_name))

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
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check source
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_PROJECT} LIMIT 0")
    print(f"[OK] Source: {SOURCE_PROJECT}")
except:
    dbutils.notebook.exit("SOURCE_NOT_FOUND")

# Check dimension tables (optional)
available_dims = {}
for name, table in [("TimeZone", DIM_TIMEZONE), ("Organization", DIM_ORGANIZATION)]:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name}: {table}")
        available_dims[name] = True
    except:
        print(f"[WARN] {name} not found")
        available_dims[name] = False

# Ensure schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare Dimension Lookups

# COMMAND ----------

print("=" * 60)
print("STEP 1: Dimension Lookups")
print("=" * 60)

# TimeZone mapping (IANA -> Windows timezone)
if available_dims["TimeZone"]:
    tz_lookup_df = spark.table(DIM_TIMEZONE) \
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("IANATimeZone").orderBy(F.lit(1))
        )) \
        .filter(F.col("_rn") == 1) \
        .select(
            F.col("WindowsTimeZone").alias("dim_WindowsTimeZone"),
            F.col("IANATimeZone").alias("dim_IANATimeZone")
        )
    print(f"TimeZone mapping: {tz_lookup_df.count()} records")
else:
    tz_lookup_df = None

# Organization dimension lookup
if available_dims["Organization"]:
    org_lookup_df = spark.table(DIM_ORGANIZATION) \
        .withColumn("_ext_alias", fn_ext_source_id_alias("ExtSourceID")) \
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("ExtOrganizationID", "_ext_alias").orderBy(F.lit(1))
        )) \
        .filter(F.col("_rn") == 1) \
        .filter(F.col("_ext_alias") == fn_ext_source_id_alias(F.lit(EXT_SOURCE_ID))) \
        .select(
            F.col("OrganizationID").alias("dim_OrganizationID"),
            F.col("ExtOrganizationID").alias("dim_ExtOrganizationID")
        )
    print(f"Organization lookup: {org_lookup_df.count()} records")
else:
    org_lookup_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Source DataFrame

# COMMAND ----------

print("=" * 60)
print("STEP 2: Build Source")
print("=" * 60)

# Load source
source_df = spark.table(SOURCE_PROJECT)
source_count = source_df.count()
print(f"Source records: {source_count}")

# Add calculated columns and ExtSourceID
source_calc = source_df \
    .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID)) \
    .withColumn("Name2", F.trim(F.col("Name")))

# ROW_NUMBER deduplication
dedup_window = Window.partitionBy("Id").orderBy(F.lit(1))
source_dedup = source_calc \
    .withColumn("RN", F.row_number().over(dedup_window)) \
    .filter(F.col("RN") == 1) \
    .drop("RN")

# Join with TimeZone mapping
if tz_lookup_df is not None:
    source_with_tz = source_dedup.alias("s").join(
        tz_lookup_df.alias("tz"),
        F.col("s.TimeZone") == F.col("tz.dim_IANATimeZone"),
        "left"
    ).select("s.*", F.col("tz.dim_WindowsTimeZone").alias("WindowsTimeZone"))
else:
    source_with_tz = source_dedup.withColumn("WindowsTimeZone", F.lit(None).cast("string"))

# Join with Organization dimension
if org_lookup_df is not None:
    source_final = source_with_tz.alias("s").join(
        org_lookup_df.alias("org"),
        F.col("s.TenantId") == F.col("org.dim_ExtOrganizationID"),
        "left"
    ).select("s.*", F.col("org.dim_OrganizationID").alias("OrganizationID"))
else:
    source_final = source_with_tz.withColumn("OrganizationID", F.lit(None).cast("int"))

source_final.cache()
final_count = source_final.count()
print(f"After joins: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create/Update Target Table

# COMMAND ----------

print("=" * 60)
print("STEP 3: Ensure Target Table")
print("=" * 60)

target_schema = """
    ProjectID BIGINT GENERATED ALWAYS AS IDENTITY,
    ExtProjectID STRING,
    ExtSourceID INT,
    Project STRING,
    Status STRING,
    ProjectType STRING,
    ExtProjectTypeID STRING,
    ContractType STRING,
    ExtContractTypeID STRING,
    TimeZoneName STRING,
    IANATimeZoneName STRING,
    DateFormat STRING,
    StartDate TIMESTAMP,
    IncludeMissedReadings BOOLEAN,
    AttendanceThreshold INT,
    UndeliveredThreshold INT,
    DefaultTracingDistance DOUBLE,
    ZoneBufferDistance DOUBLE,
    ExtCustomerExpectedHoursID STRING,
    ExtWakecapWorkingHoursID STRING,
    OrganizationID INT,
    ExtOrganizationID STRING,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP,
    WatermarkUTC TIMESTAMP DEFAULT current_timestamp(),
    _silver_processed_at TIMESTAMP DEFAULT current_timestamp()
"""

try:
    spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    print(f"[OK] Target exists: {TARGET_TABLE}")
except:
    print(f"[INFO] Creating target...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            {target_schema}
        )
        USING DELTA
        CLUSTER BY (ExtSourceID, ExtProjectID)
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)

try:
    target_before = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0
print(f"Target rows before: {target_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Execute MERGE

# COMMAND ----------

print("=" * 60)
print("STEP 4: Execute MERGE")
print("=" * 60)

source_final.createOrReplaceTempView("project_source")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING project_source AS s
ON t.ExtProjectID = s.Id
   AND CASE WHEN t.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE t.ExtSourceID END
     = CASE WHEN s.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE s.ExtSourceID END

WHEN MATCHED AND (
    t.AttendanceThreshold IS DISTINCT FROM s.AttendanceThreshold OR
    t.ContractType IS DISTINCT FROM s.ContractType OR
    t.ExtContractTypeID IS DISTINCT FROM s.ContractTypeId OR
    t.ExtCustomerExpectedHoursID IS DISTINCT FROM s.CustomerExpectedHoursId OR
    t.DateFormat IS DISTINCT FROM s.DateFormat OR
    t.DefaultTracingDistance IS DISTINCT FROM s.DefaultTracingDistance OR
    t.IncludeMissedReadings IS DISTINCT FROM s.IncludingMissedReading OR
    t.Project IS DISTINCT FROM s.Name2 OR
    t.OrganizationID IS DISTINCT FROM s.OrganizationID OR
    t.ProjectType IS DISTINCT FROM s.ProjectType OR
    t.ExtProjectTypeID IS DISTINCT FROM s.ProjectTypeId OR
    t.StartDate IS DISTINCT FROM s.StartDate OR
    t.Status IS DISTINCT FROM s.Status OR
    t.ExtOrganizationID IS DISTINCT FROM s.TenantId OR
    t.IANATimeZoneName IS DISTINCT FROM s.TimeZone OR
    t.UndeliveredThreshold IS DISTINCT FROM s.UndeliveredThreshold OR
    t.ExtWakecapWorkingHoursID IS DISTINCT FROM s.WakecapWorkingHoursId OR
    t.TimeZoneName IS DISTINCT FROM s.WindowsTimeZone OR
    t.ZoneBufferDistance IS DISTINCT FROM s.ZoneBufferDistance
)
THEN UPDATE SET
    t.WatermarkUTC = current_timestamp(),
    t._silver_processed_at = current_timestamp(),
    t.AttendanceThreshold = s.AttendanceThreshold,
    t.ContractType = s.ContractType,
    t.ExtContractTypeID = s.ContractTypeId,
    t.CreatedAt = s.CreatedAt,
    t.ExtCustomerExpectedHoursID = s.CustomerExpectedHoursId,
    t.DateFormat = s.DateFormat,
    t.DefaultTracingDistance = s.DefaultTracingDistance,
    t.ExtSourceID = s.ExtSourceID,
    t.IncludeMissedReadings = s.IncludingMissedReading,
    t.Project = s.Name2,
    t.OrganizationID = s.OrganizationID,
    t.ProjectType = s.ProjectType,
    t.ExtProjectTypeID = s.ProjectTypeId,
    t.StartDate = s.StartDate,
    t.Status = s.Status,
    t.ExtOrganizationID = s.TenantId,
    t.IANATimeZoneName = s.TimeZone,
    t.UndeliveredThreshold = s.UndeliveredThreshold,
    t.UpdatedAt = s.UpdatedAt,
    t.ExtWakecapWorkingHoursID = s.WakecapWorkingHoursId,
    t.TimeZoneName = s.WindowsTimeZone,
    t.ZoneBufferDistance = s.ZoneBufferDistance

WHEN NOT MATCHED THEN INSERT (
    AttendanceThreshold, ContractType, ExtContractTypeID, CreatedAt, ExtCustomerExpectedHoursID,
    DateFormat, DefaultTracingDistance, ExtSourceID, ExtProjectID, IncludeMissedReadings,
    Project, OrganizationID, ProjectType, ExtProjectTypeID, StartDate, Status,
    ExtOrganizationID, IANATimeZoneName, UndeliveredThreshold, UpdatedAt,
    ExtWakecapWorkingHoursID, TimeZoneName, ZoneBufferDistance
)
VALUES (
    s.AttendanceThreshold, s.ContractType, s.ContractTypeId, s.CreatedAt, s.CustomerExpectedHoursId,
    s.DateFormat, s.DefaultTracingDistance, s.ExtSourceID, s.Id, s.IncludingMissedReading,
    s.Name2, s.OrganizationID, s.ProjectType, s.ProjectTypeId, s.StartDate, s.Status,
    s.TenantId, s.TimeZone, s.UndeliveredThreshold, s.UpdatedAt,
    s.WakecapWorkingHoursId, s.WindowsTimeZone, s.ZoneBufferDistance
)
"""

spark.sql(merge_sql)
print("[OK] MERGE completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Update Watermark and Summary

# COMMAND ----------

source_final.unpersist()

target_after = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
inserted = target_after - target_before

update_watermark("wc2023_Project", datetime.now(), target_after)

print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Source: {source_count} | After joins: {final_count}")
print(f"Target before: {target_before} | After: {target_after} | Inserted: {inserted}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: processed={final_count}, target={target_after}")
