# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Delta Sync Fact Observations
# MAGIC
# MAGIC **Converted from:** `stg.spDeltaSyncFactObservations` (585 lines)
# MAGIC
# MAGIC **Purpose:** Delta sync from TimescaleDB observations to FactObservations with 12 dimension lookups and stalled record handling.
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - TEMP_TABLE (`#batch`) -> Spark DataFrame with cache()
# MAGIC - MERGE INTO with complex change detection -> DeltaTable.merge()
# MAGIC - ROW_NUMBER() OVER (PARTITION BY...) -> PySpark Window functions
# MAGIC - Custom function `stg.fnExtSourceIDAlias()` -> Inline CASE expression
# MAGIC - Two-phase stalled record handling -> Separate DataFrames with merge logic
# MAGIC - 12 dimension table INNER/LEFT joins -> Sequential DataFrame joins with broadcast
# MAGIC
# MAGIC **Source:** `wakecap_prod.raw.timescale_observations`
# MAGIC **Target:** `wakecap_prod.gold.fact_observations`
# MAGIC **Stalled:** `wakecap_prod.silver.silver_observations_stalled`

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
RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source and target tables
SOURCE_TABLE = f"{TARGET_CATALOG}.{RAW_SCHEMA}.observation_observation"
STALLED_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observations_stalled"
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.fact_observations"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# Dimension tables
DIM_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project_dw"
DIM_OBSERVATION_DISCRIMINATOR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_discriminator"
DIM_OBSERVATION_SOURCE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_source"
DIM_OBSERVATION_TYPE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_type"
DIM_OBSERVATION_SEVERITY = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_severity"
DIM_OBSERVATION_STATUS = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_status"
DIM_OBSERVATION_CLINIC_VIOLATION_STATUS = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_clinic_violation_status"
DIM_ORGANIZATION = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_organization"
DIM_FLOOR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_floor"
DIM_ZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_zone"
DIM_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
DIM_DEVICE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_device"

# Constant for ExtSourceID
EXT_SOURCE_ID = 19  # TimescaleDB observations source

# Float comparison tolerance for MERGE change detection
FLOAT_TOLERANCE = 0.00001

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Stalled: {STALLED_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "7", "Lookback Days for Incremental")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")
dbutils.widgets.text("batch_size", "0", "Batch Size (0=unlimited)")

load_mode = dbutils.widgets.get("load_mode")
lookback_days = int(dbutils.widgets.get("lookback_days"))
project_filter = dbutils.widgets.get("project_id")
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Load Mode: {load_mode}")
print(f"Lookback Days: {lookback_days}")
print(f"Project Filter: {project_filter if project_filter else 'None'}")
print(f"Batch Size: {batch_size if batch_size > 0 else 'Unlimited'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check source table
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} LIMIT 0")
    print(f"[OK] Source table exists: {SOURCE_TABLE}")
except Exception as e:
    print(f"[FATAL] Source table not found: {SOURCE_TABLE}")
    dbutils.notebook.exit(f"SOURCE_TABLE_NOT_FOUND: {SOURCE_TABLE}")

# Check required dimension tables (INNER joins)
required_dims = [
    ("Project", DIM_PROJECT),
    ("ObservationDiscriminator", DIM_OBSERVATION_DISCRIMINATOR),
    ("ObservationSource", DIM_OBSERVATION_SOURCE),
    ("ObservationType", DIM_OBSERVATION_TYPE),
]

for name, table in required_dims:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} dimension exists: {table}")
    except Exception as e:
        print(f"[FATAL] Required dimension not found: {table}")
        dbutils.notebook.exit(f"DIMENSION_NOT_FOUND: {table}")

# Check optional dimension tables (LEFT joins)
optional_dims = [
    ("ObservationSeverity", DIM_OBSERVATION_SEVERITY),
    ("ObservationStatus", DIM_OBSERVATION_STATUS),
    ("ObservationClinicViolationStatus", DIM_OBSERVATION_CLINIC_VIOLATION_STATUS),
    ("Organization", DIM_ORGANIZATION),
    ("Floor", DIM_FLOOR),
    ("Zone", DIM_ZONE),
    ("Worker", DIM_WORKER),
    ("Device", DIM_DEVICE),
]

for name, table in optional_dims:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} dimension exists: {table}")
    except Exception as e:
        print(f"[WARN] Optional dimension not found: {table}")

# Ensure target schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print(f"[OK] Target schemas verified")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def ext_source_id_alias(ext_source_id_col):
    """
    Equivalent of stg.fnExtSourceIDAlias() function.
    Groups ExtSourceID values:
    - 14, 15, 16, 17, 18, 19 -> 15
    - 1, 2, 11, 12 -> 2
    - Otherwise -> original value
    """
    return (
        F.when(F.col(ext_source_id_col).isin(14, 15, 16, 17, 18, 19), F.lit(15))
         .when(F.col(ext_source_id_col).isin(1, 2, 11, 12), F.lit(2))
         .otherwise(F.col(ext_source_id_col))
    )


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


def update_watermark(table_name, watermark_value, row_count, metrics=None):
    """Update watermark after successful processing."""
    try:
        metrics_str = ""
        if metrics:
            metrics_str = f"""
                inserted_count = {metrics.get('inserted', 0)},
                updated_count = {metrics.get('updated', 0)},
                stalled_count = {metrics.get('stalled', 0)},
                recovered_count = {metrics.get('recovered', 0)},
            """

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


def float_changed(target_col, source_col):
    """
    Check if float column has changed beyond tolerance.
    Handles NULL comparisons properly.
    """
    return (
        (F.abs(F.col(f"t.{target_col}") - F.col(f"s.{source_col}")) > FLOAT_TOLERANCE) |
        (F.col(f"t.{target_col}").isNull() & F.col(f"s.{source_col}").isNotNull()) |
        (F.col(f"t.{target_col}").isNotNull() & F.col(f"s.{source_col}").isNull())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Dimension Tables with Deduplication
# MAGIC
# MAGIC Each dimension is loaded with ROW_NUMBER deduplication, matching the original SQL:
# MAGIC ```sql
# MAGIC ROW_NUMBER() OVER (PARTITION BY ExtProjectID, stg.fnExtSourceIDAlias(ExtSourceID) ORDER BY (SELECT NULL)) rn
# MAGIC ```

# COMMAND ----------

print("Loading dimension tables with deduplication...")

# ExtSourceIDAlias for the target (EXT_SOURCE_ID = 19 -> alias = 15)
TARGET_EXT_SOURCE_ALIAS = 15

def load_dimension_with_dedup(table_path, partition_col, id_col, match_col=None, extra_cols=None):
    """
    Load dimension table with ROW_NUMBER deduplication.
    Returns DataFrame with rn=1 filter and ExtSourceIDAlias matching.
    """
    if match_col is None:
        match_col = partition_col

    df = spark.table(table_path)

    # Add ExtSourceIDAlias column
    df = df.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))

    # Filter to matching source alias
    df = df.filter(F.col("ExtSourceIDAlias") == TARGET_EXT_SOURCE_ALIAS)

    # Apply ROW_NUMBER for deduplication
    window = Window.partitionBy(partition_col, "ExtSourceIDAlias").orderBy(F.lit(1))
    df = df.withColumn("rn", F.row_number().over(window))
    df = df.filter(F.col("rn") == 1).drop("rn")

    # Select only needed columns
    select_cols = [match_col, id_col]
    if extra_cols:
        select_cols.extend(extra_cols)

    return df.select(*select_cols)


# 1. Project (INNER) - Match on ExtProjectID (UUID string)
# Note: silver_project_dw from SQL Server DW has columns: ProjectID, ExtProjectID (UUID), ExtSourceID
dim_project = load_dimension_with_dedup(
    DIM_PROJECT,
    partition_col="ExtProjectID",
    id_col="ProjectID",
    match_col="ExtProjectID"
).withColumnRenamed("ProjectID", "ProjectID2")
print(f"  Project dimension: {dim_project.count()} rows")

# 2. ObservationDiscriminator (INNER) - Match on ObservationDiscriminator
dim_discriminator = load_dimension_with_dedup(
    DIM_OBSERVATION_DISCRIMINATOR,
    partition_col="ObservationDiscriminator",
    id_col="ObservationDiscriminatorID",
    match_col="ObservationDiscriminator"
)
print(f"  ObservationDiscriminator dimension: {dim_discriminator.count()} rows")

# 3. ObservationSource (INNER) - Match on ObservationSource
dim_source = load_dimension_with_dedup(
    DIM_OBSERVATION_SOURCE,
    partition_col="ObservationSource",
    id_col="ObservationSourceID",
    match_col="ObservationSource"
)
print(f"  ObservationSource dimension: {dim_source.count()} rows")

# 4. ObservationType (INNER) - Match on ObservationType
dim_type = load_dimension_with_dedup(
    DIM_OBSERVATION_TYPE,
    partition_col="ObservationType",
    id_col="ObservationTypeID",
    match_col="ObservationType"
)
print(f"  ObservationType dimension: {dim_type.count()} rows")

# 5. ObservationSeverity (LEFT) - Match on ObservationSeverity
try:
    dim_severity = load_dimension_with_dedup(
        DIM_OBSERVATION_SEVERITY,
        partition_col="ObservationSeverity",
        id_col="ObservationSeverityID",
        match_col="ObservationSeverity"
    )
    print(f"  ObservationSeverity dimension: {dim_severity.count()} rows")
except Exception as e:
    dim_severity = None
    print(f"  ObservationSeverity dimension: Not available - {str(e)[:50]}")

# 6. ObservationStatus (LEFT) - Match on ObservationStatus
try:
    dim_status = load_dimension_with_dedup(
        DIM_OBSERVATION_STATUS,
        partition_col="ObservationStatus",
        id_col="ObservationStatusID",
        match_col="ObservationStatus"
    )
    print(f"  ObservationStatus dimension: {dim_status.count()} rows")
except Exception as e:
    dim_status = None
    print(f"  ObservationStatus dimension: Not available - {str(e)[:50]}")

# 7. ObservationClinicViolationStatus (LEFT) - Match on ObservationClinicViolationStatus
try:
    dim_clinic_status = load_dimension_with_dedup(
        DIM_OBSERVATION_CLINIC_VIOLATION_STATUS,
        partition_col="ObservationClinicViolationStatus",
        id_col="ObservationClinicViolationStatusID",
        match_col="ObservationClinicViolationStatus"
    )
    print(f"  ObservationClinicViolationStatus dimension: {dim_clinic_status.count()} rows")
except Exception as e:
    dim_clinic_status = None
    print(f"  ObservationClinicViolationStatus dimension: Not available - {str(e)[:50]}")

# 8. Company/Organization (LEFT) - Match on ExtCompanyID
try:
    dim_company = load_dimension_with_dedup(
        DIM_ORGANIZATION,
        partition_col="ExtCompanyID",
        id_col="CompanyID",
        match_col="ExtCompanyID"
    ).withColumnRenamed("CompanyID", "CompanyID2")
    print(f"  Organization dimension: {dim_company.count()} rows")
except Exception as e:
    dim_company = None
    print(f"  Organization dimension: Not available - {str(e)[:50]}")

# 9. Floor (LEFT) - Match on ExtSpaceID
try:
    dim_floor = load_dimension_with_dedup(
        DIM_FLOOR,
        partition_col="ExtSpaceID",
        id_col="FloorID",
        match_col="ExtSpaceID"
    )
    print(f"  Floor dimension: {dim_floor.count()} rows")
except Exception as e:
    dim_floor = None
    print(f"  Floor dimension: Not available - {str(e)[:50]}")

# 10. Zone (LEFT) - Match on ExtZoneID
try:
    dim_zone = load_dimension_with_dedup(
        DIM_ZONE,
        partition_col="ExtZoneID",
        id_col="ZoneID",
        match_col="ExtZoneID"
    ).withColumnRenamed("ZoneID", "ZoneID2")
    print(f"  Zone dimension: {dim_zone.count()} rows")
except Exception as e:
    dim_zone = None
    print(f"  Zone dimension: Not available - {str(e)[:50]}")

# 11. Worker (LEFT) - Match on ExtWorkerID
try:
    dim_worker = load_dimension_with_dedup(
        DIM_WORKER,
        partition_col="ExtWorkerID",
        id_col="WorkerID",
        match_col="ExtWorkerID"
    ).withColumnRenamed("WorkerID", "WorkerID2")
    print(f"  Worker dimension: {dim_worker.count()} rows")
except Exception as e:
    dim_worker = None
    print(f"  Worker dimension: Not available - {str(e)[:50]}")

# 12. Device (LEFT) - Match on ExtDeviceID
try:
    dim_device = load_dimension_with_dedup(
        DIM_DEVICE,
        partition_col="ExtDeviceID",
        id_col="DeviceID",
        match_col="ExtDeviceID"
    ).withColumnRenamed("DeviceID", "DeviceID2")
    print(f"  Device dimension: {dim_device.count()} rows")
except Exception as e:
    dim_device = None
    print(f"  Device dimension: Not available - {str(e)[:50]}")

print("\nDimension tables loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Stalled Table if Not Exists

# COMMAND ----------

# Check if stalled table exists, create if not
stalled_table_exists = spark.catalog.tableExists(STALLED_TABLE)

if not stalled_table_exists:
    print(f"Creating stalled records table: {STALLED_TABLE}")

    # Create empty stalled table with matching schema
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {STALLED_TABLE} (
            Id INT NOT NULL,
            ExternalId STRING,
            ProjectId INT,
            Source STRING,
            Type STRING,
            GeneratedAt TIMESTAMP,
            Description STRING,
            PackageId INT,
            CompanyId INT,
            Severity STRING,
            Viewed BOOLEAN,
            Discriminator STRING,
            PlateNumber STRING,
            Speed DOUBLE,
            SpeedLimit DOUBLE,
            MediaUrl STRING,
            Inaccurate BOOLEAN,
            Latitude DOUBLE,
            Longitude DOUBLE,
            SpaceId INT,
            ZoneId INT,
            ResourceId INT,
            ZoneEntryTime TIMESTAMP,
            ZoneExitTime TIMESTAMP,
            DurationExceeded DOUBLE,
            CreatedAt TIMESTAMP,
            UpdatedAt TIMESTAMP,
            DeletedAt TIMESTAMP,
            DeviceId INT,
            Status STRING,
            OpenedAt TIMESTAMP,
            ClosedAt TIMESTAMP,
            FalseAlarm BOOLEAN,
            ViewedAt TIMESTAMP,
            CameraName STRING,
            PermitNumber STRING,
            StopWorkNotice BOOLEAN,
            SerialNo STRING,
            Threshold DOUBLE,
            ClinicViolationStatus STRING,
            RemainingLeaveDays INT,
            ParentId INT,
            _stalled_at TIMESTAMP,
            _stall_reason STRING
        )
        USING DELTA
    """)
    print(f"  Stalled table created")
else:
    print(f"Stalled table exists: {STALLED_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Recover Previously Stalled Records
# MAGIC
# MAGIC Re-process stalled records where all required dimensions can now be resolved.

# COMMAND ----------

print("=" * 60)
print("PHASE 1: Recover Stalled Records")
print("=" * 60)

recovered_count = 0

try:
    stalled_df = spark.table(STALLED_TABLE)
    stalled_count = stalled_df.count()
    print(f"Stalled records to check: {stalled_count}")

    if stalled_count > 0:
        # Join stalled records with all dimensions
        recoverable = stalled_df.alias("ta")

        # INNER joins (required dimensions)
        recoverable = recoverable.join(
            dim_project.alias("t1"),
            F.lower(F.col("ta.ProjectId")) == F.lower(F.col("t1.ExtProjectID")),
            "inner"
        )
        recoverable = recoverable.join(
            dim_discriminator.alias("t2"),
            F.col("ta.Discriminator") == F.col("t2.ObservationDiscriminator"),
            "inner"
        )
        recoverable = recoverable.join(
            dim_source.alias("t3"),
            F.col("ta.Source") == F.col("t3.ObservationSource"),
            "inner"
        )
        recoverable = recoverable.join(
            dim_type.alias("t4"),
            F.col("ta.Type") == F.col("t4.ObservationType"),
            "inner"
        )

        # LEFT joins (optional dimensions) with resolution check
        # Severity
        if dim_severity is not None:
            recoverable = recoverable.join(
                dim_severity.alias("t5"),
                F.col("ta.Severity") == F.col("t5.ObservationSeverity"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t5.ObservationSeverity").isNotNull() | F.col("ta.Severity").isNull()
            )
        else:
            recoverable = recoverable.withColumn("ObservationSeverityID", F.lit(None).cast("int"))

        # Status
        if dim_status is not None:
            recoverable = recoverable.join(
                dim_status.alias("t6"),
                F.col("ta.Status") == F.col("t6.ObservationStatus"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t6.ObservationStatus").isNotNull() | F.col("ta.Status").isNull()
            )
        else:
            recoverable = recoverable.withColumn("ObservationStatusID", F.lit(None).cast("int"))

        # ClinicViolationStatus
        if dim_clinic_status is not None:
            recoverable = recoverable.join(
                dim_clinic_status.alias("t7"),
                F.col("ta.ClinicViolationStatus") == F.col("t7.ObservationClinicViolationStatus"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t7.ObservationClinicViolationStatus").isNotNull() | F.col("ta.ClinicViolationStatus").isNull()
            )
        else:
            recoverable = recoverable.withColumn("ObservationClinicViolationStatusID", F.lit(None).cast("int"))

        # Company
        if dim_company is not None:
            recoverable = recoverable.join(
                dim_company.alias("t8"),
                F.col("ta.CompanyId") == F.col("t8.ExtCompanyID"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t8.ExtCompanyID").isNotNull() | F.col("ta.CompanyId").isNull()
            )
        else:
            recoverable = recoverable.withColumn("CompanyID2", F.lit(None).cast("int"))

        # Floor
        if dim_floor is not None:
            recoverable = recoverable.join(
                dim_floor.alias("t9"),
                F.col("ta.SpaceId") == F.col("t9.ExtSpaceID"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t9.ExtSpaceID").isNotNull() | F.col("ta.SpaceId").isNull()
            )
        else:
            recoverable = recoverable.withColumn("FloorID", F.lit(None).cast("int"))

        # Zone
        if dim_zone is not None:
            recoverable = recoverable.join(
                dim_zone.alias("t10"),
                F.col("ta.ZoneId") == F.col("t10.ExtZoneID"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t10.ExtZoneID").isNotNull() | F.col("ta.ZoneId").isNull()
            )
        else:
            recoverable = recoverable.withColumn("ZoneID2", F.lit(None).cast("int"))

        # Worker
        if dim_worker is not None:
            recoverable = recoverable.join(
                dim_worker.alias("t11"),
                F.col("ta.ResourceId") == F.col("t11.ExtWorkerID"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t11.ExtWorkerID").isNotNull() | F.col("ta.ResourceId").isNull()
            )
        else:
            recoverable = recoverable.withColumn("WorkerID2", F.lit(None).cast("int"))

        # Device
        if dim_device is not None:
            recoverable = recoverable.join(
                dim_device.alias("t12"),
                F.col("ta.DeviceId") == F.col("t12.ExtDeviceID"),
                "left"
            )
            recoverable = recoverable.filter(
                F.col("t12.ExtDeviceID").isNotNull() | F.col("ta.DeviceId").isNull()
            )
        else:
            recoverable = recoverable.withColumn("DeviceID2", F.lit(None).cast("int"))

        recovered_count = recoverable.count()
        print(f"Recoverable stalled records: {recovered_count}")

        if recovered_count > 0:
            # Store IDs for later deletion from stalled table
            recovered_ids = recoverable.select("ta.Id").distinct()
            recovered_ids.createOrReplaceTempView("recovered_stalled_ids")
            print(f"  Stored {recovered_count} IDs for recovery")
    else:
        print("No stalled records to recover")

except Exception as e:
    print(f"Warning: Could not process stalled records: {str(e)[:100]}")
    recovered_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Build Staging DataFrame
# MAGIC
# MAGIC Equivalent to `#batch` temp table with all dimension lookups.

# COMMAND ----------

print("=" * 60)
print("PHASE 2: Build Staging DataFrame")
print("=" * 60)

# Load source observations
print("Loading source observations...")
source_df = spark.table(SOURCE_TABLE)

# Apply incremental filter if in incremental mode
if load_mode == "incremental":
    last_watermark = get_watermark("fact_observations")
    cutoff_date = datetime.now() - timedelta(days=lookback_days)

    # Use the later of watermark or cutoff_date
    if last_watermark and last_watermark > cutoff_date:
        filter_date = last_watermark
    else:
        filter_date = cutoff_date

    source_df = source_df.filter(F.col("UpdatedAt") >= filter_date)
    print(f"  Incremental filter: UpdatedAt >= {filter_date}")
else:
    print("  Full load mode - processing all records")

# Apply optional project filter
if project_filter:
    source_df = source_df.filter(F.col("ProjectId") == int(project_filter))
    print(f"  Filtered to ProjectId: {project_filter}")

# Apply batch size limit if specified
if batch_size > 0:
    source_df = source_df.limit(batch_size)
    print(f"  Limited to batch size: {batch_size}")

source_count = source_df.count()
print(f"Source records to process: {source_count:,}")

if source_count == 0:
    print("No records to process. Exiting.")
    dbutils.notebook.exit("No records to process")

# COMMAND ----------

# Alias source DataFrame for joins
batch_df = source_df.alias("ta")

# Add ExtSourceID constant and ViewedAt2 computed column
batch_df = batch_df.withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID))
batch_df = batch_df.withColumn(
    "ViewedAt2",
    F.when(
        F.col("ViewedAt").isNull() & (F.col("Viewed") == True),
        F.to_timestamp(F.lit("1900-01-01"))
    ).otherwise(F.col("ViewedAt"))
)

# Cast columns to correct types (source has STRING where we need INT/DOUBLE)
# Note: ProjectId is a UUID string - no casting needed, join on string directly

# Threshold: STRING -> DOUBLE
batch_df = batch_df.withColumn("Threshold_dbl", F.col("Threshold").cast("double"))

# Parse Latitude/Longitude from LocationWKT if available, otherwise NULL
# LocationWKT format: "POINT(longitude latitude)" or NULL
batch_df = batch_df.withColumn(
    "Latitude",
    F.when(
        F.col("LocationWKT").isNotNull() & F.col("LocationWKT").startswith("POINT"),
        F.regexp_extract(F.col("LocationWKT"), r"POINT\s*\(\s*[\d.-]+\s+([\d.-]+)\s*\)", 1).cast("double")
    ).otherwise(F.lit(None).cast("double"))
)
batch_df = batch_df.withColumn(
    "Longitude",
    F.when(
        F.col("LocationWKT").isNotNull() & F.col("LocationWKT").startswith("POINT"),
        F.regexp_extract(F.col("LocationWKT"), r"POINT\s*\(\s*([\d.-]+)\s+[\d.-]+\s*\)", 1).cast("double")
    ).otherwise(F.lit(None).cast("double"))
)

# INNER join with Project dimension (required)
# Note: ProjectId is UUID STRING in both source and dimension - join directly
batch_df = batch_df.join(
    F.broadcast(dim_project).alias("t1"),
    F.lower(F.col("ta.ProjectId")) == F.lower(F.col("t1.ExtProjectID")),
    "inner"
)
print(f"After Project join: {batch_df.count():,} records")

# INNER join with Discriminator dimension (required)
batch_df = batch_df.join(
    F.broadcast(dim_discriminator).alias("t2"),
    F.col("ta.Discriminator") == F.col("t2.ObservationDiscriminator"),
    "inner"
)
print(f"After Discriminator join: {batch_df.count():,} records")

# INNER join with Source dimension (required)
batch_df = batch_df.join(
    F.broadcast(dim_source).alias("t3"),
    F.col("ta.Source") == F.col("t3.ObservationSource"),
    "inner"
)
print(f"After Source join: {batch_df.count():,} records")

# INNER join with Type dimension (required)
batch_df = batch_df.join(
    F.broadcast(dim_type).alias("t4"),
    F.col("ta.Type") == F.col("t4.ObservationType"),
    "inner"
)
print(f"After Type join: {batch_df.count():,} records")

# LEFT join with Severity dimension
if dim_severity is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_severity).alias("t5"),
        F.col("ta.Severity") == F.col("t5.ObservationSeverity"),
        "left"
    )
    # Resolution check: dimension found OR source was null
    batch_df = batch_df.filter(
        F.col("t5.ObservationSeverity").isNotNull() | F.col("ta.Severity").isNull()
    )
else:
    batch_df = batch_df.withColumn("ObservationSeverityID", F.lit(None).cast("int"))

# LEFT join with Status dimension
if dim_status is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_status).alias("t6"),
        F.col("ta.Status") == F.col("t6.ObservationStatus"),
        "left"
    )
    batch_df = batch_df.filter(
        F.col("t6.ObservationStatus").isNotNull() | F.col("ta.Status").isNull()
    )
else:
    batch_df = batch_df.withColumn("ObservationStatusID", F.lit(None).cast("int"))

# LEFT join with ClinicViolationStatus dimension
if dim_clinic_status is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_clinic_status).alias("t7"),
        F.col("ta.ClinicViolationStatus") == F.col("t7.ObservationClinicViolationStatus"),
        "left"
    )
    batch_df = batch_df.filter(
        F.col("t7.ObservationClinicViolationStatus").isNotNull() | F.col("ta.ClinicViolationStatus").isNull()
    )
else:
    batch_df = batch_df.withColumn("ObservationClinicViolationStatusID", F.lit(None).cast("int"))

# LEFT join with Company dimension
if dim_company is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_company).alias("t8"),
        F.col("ta.CompanyId") == F.col("t8.ExtCompanyID"),
        "left"
    )
    batch_df = batch_df.filter(
        F.col("t8.ExtCompanyID").isNotNull() | F.col("ta.CompanyId").isNull()
    )
else:
    batch_df = batch_df.withColumn("CompanyID2", F.lit(None).cast("int"))

# LEFT join with Floor dimension
if dim_floor is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_floor).alias("t9"),
        F.col("ta.SpaceId") == F.col("t9.ExtSpaceID"),
        "left"
    )
    batch_df = batch_df.filter(
        F.col("t9.ExtSpaceID").isNotNull() | F.col("ta.SpaceId").isNull()
    )
else:
    batch_df = batch_df.withColumn("FloorID", F.lit(None).cast("int"))

# LEFT join with Zone dimension
if dim_zone is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_zone).alias("t10"),
        F.col("ta.ZoneId") == F.col("t10.ExtZoneID"),
        "left"
    )
    batch_df = batch_df.filter(
        F.col("t10.ExtZoneID").isNotNull() | F.col("ta.ZoneId").isNull()
    )
else:
    batch_df = batch_df.withColumn("ZoneID2", F.lit(None).cast("int"))

# LEFT join with Worker dimension
if dim_worker is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_worker).alias("t11"),
        F.col("ta.ResourceId") == F.col("t11.ExtWorkerID"),
        "left"
    )
    batch_df = batch_df.filter(
        F.col("t11.ExtWorkerID").isNotNull() | F.col("ta.ResourceId").isNull()
    )
else:
    batch_df = batch_df.withColumn("WorkerID2", F.lit(None).cast("int"))

# LEFT join with Device dimension
if dim_device is not None:
    batch_df = batch_df.join(
        F.broadcast(dim_device).alias("t12"),
        F.col("ta.DeviceId") == F.col("t12.ExtDeviceID"),
        "left"
    )
    batch_df = batch_df.filter(
        F.col("t12.ExtDeviceID").isNotNull() | F.col("ta.DeviceId").isNull()
    )
else:
    batch_df = batch_df.withColumn("DeviceID2", F.lit(None).cast("int"))

# Add ROW_NUMBER for deduplication on Id (observation ID)
window = Window.partitionBy("ta.Id").orderBy(F.lit(1))
batch_df = batch_df.withColumn("RN", F.row_number().over(window))

# COMMAND ----------

# Cache the batch DataFrame for reuse
batch_df = batch_df.cache()
batch_count = batch_df.count()
print(f"Batch DataFrame ready: {batch_count:,} records (before dedup)")

# Filter to RN=1 for unique records
batch_unique = batch_df.filter(F.col("RN") == 1)
unique_count = batch_unique.count()
print(f"Unique records (RN=1): {unique_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: MERGE into FactObservations
# MAGIC
# MAGIC Use DeltaTable.merge() with:
# MAGIC - Match on ExtObservationID and ExtSourceIDAlias
# MAGIC - Update condition checking all columns (with float tolerance)
# MAGIC - WatermarkUTC = current_timestamp() on update

# COMMAND ----------

print("=" * 60)
print("PHASE 3: MERGE into FactObservations")
print("=" * 60)

# Prepare the merge source DataFrame with proper column names
merge_source = batch_unique.select(
    F.col("ta.Id").alias("ExtObservationID"),
    F.col("ExtSourceID"),
    F.col("ta.GeneratedAt").alias("TimestampUTC"),
    F.col("t1.ProjectID2").alias("ProjectID"),
    F.col("t2.ObservationDiscriminatorID"),
    F.col("t3.ObservationSourceID"),
    F.col("t4.ObservationTypeID"),
    F.coalesce(F.col("t5.ObservationSeverityID"), F.lit(None).cast("int")).alias("ObservationSeverityID") if dim_severity is not None else F.lit(None).cast("int").alias("ObservationSeverityID"),
    F.coalesce(F.col("t6.ObservationStatusID"), F.lit(None).cast("int")).alias("ObservationStatusID") if dim_status is not None else F.lit(None).cast("int").alias("ObservationStatusID"),
    F.coalesce(F.col("t7.ObservationClinicViolationStatusID"), F.lit(None).cast("int")).alias("ObservationClinicViolationStatusID") if dim_clinic_status is not None else F.lit(None).cast("int").alias("ObservationClinicViolationStatusID"),
    F.coalesce(F.col("t8.CompanyID2"), F.lit(None).cast("int")).alias("CompanyID") if dim_company is not None else F.lit(None).cast("int").alias("CompanyID"),
    F.coalesce(F.col("t9.FloorID"), F.lit(None).cast("int")).alias("FloorID") if dim_floor is not None else F.lit(None).cast("int").alias("FloorID"),
    F.coalesce(F.col("t10.ZoneID2"), F.lit(None).cast("int")).alias("ZoneID") if dim_zone is not None else F.lit(None).cast("int").alias("ZoneID"),
    F.coalesce(F.col("t11.WorkerID2"), F.lit(None).cast("int")).alias("WorkerID") if dim_worker is not None else F.lit(None).cast("int").alias("WorkerID"),
    F.coalesce(F.col("t12.DeviceID2"), F.lit(None).cast("int")).alias("DeviceID") if dim_device is not None else F.lit(None).cast("int").alias("DeviceID"),
    F.col("ta.Inaccurate").alias("IsInaccurate"),
    F.col("ta.FalseAlarm").alias("IsFalseAlarm"),
    F.col("ta.Description"),
    F.col("ta.CameraName"),
    F.col("ta.SerialNo").alias("SerialNumber"),
    F.col("ta.PermitNumber"),
    F.col("ta.StopWorkNotice"),
    F.col("ta.RemainingLeaveDays"),
    F.col("Threshold_dbl").alias("Threshold"),
    F.col("Latitude"),
    F.col("Longitude"),
    F.col("ta.PlateNumber"),
    F.col("ta.Speed"),
    F.col("ta.SpeedLimit"),
    F.col("ta.ZoneEntryTime").alias("ZoneEntryTimeUTC"),
    F.col("ta.ZoneExitTime").alias("ZoneExitTimeUTC"),
    F.col("ta.DurationExceeded"),
    F.col("ta.OpenedAt").alias("OpenedAtUTC"),
    F.col("ViewedAt2").alias("ViewedAtUTC"),
    F.col("ta.ClosedAt").alias("ClosedAtUTC"),
    F.col("ta.MediaUrl").alias("MediaURL"),
    F.col("ta.CreatedAt"),
    F.col("ta.UpdatedAt"),
    F.col("ta.DeletedAt"),
    F.col("ta.PackageId").alias("ExtPackageID"),
    F.col("ta.ParentId").alias("ExtParentID"),
    F.current_timestamp().alias("WatermarkUTC")
)

# Add ExtSourceIDAlias for merge condition
merge_source = merge_source.withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))

print(f"Merge source prepared: {merge_source.count():,} records")

# COMMAND ----------

# Check if target table exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

if target_exists:
    print(f"Merging into existing table: {TARGET_TABLE}")

    delta_target = DeltaTable.forName(spark, TARGET_TABLE)

    # Build the merge with complex change detection
    # Using SQL-based merge for better control over the condition
    merge_source.createOrReplaceTempView("merge_source_view")

    # Add ExtSourceIDAlias to target for matching
    # Execute MERGE with all change detection conditions
    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING merge_source_view AS s
        ON s.ExtObservationID = t.ExtObservationID
           AND s.ExtSourceIDAlias = CASE
                                        WHEN t.ExtSourceID IN (14, 15, 16, 17, 18, 19) THEN 15
                                        WHEN t.ExtSourceID IN (1, 2, 11, 12) THEN 2
                                        ELSE t.ExtSourceID
                                    END
        WHEN MATCHED AND (
            (t.CameraName IS DISTINCT FROM s.CameraName) OR
            (t.ClosedAtUTC IS DISTINCT FROM s.ClosedAtUTC) OR
            (t.CompanyID IS DISTINCT FROM s.CompanyID) OR
            (t.CreatedAt IS DISTINCT FROM s.CreatedAt) OR
            (t.DeletedAt IS DISTINCT FROM s.DeletedAt) OR
            (t.Description IS DISTINCT FROM s.Description) OR
            (t.DeviceID IS DISTINCT FROM s.DeviceID) OR
            (ABS(COALESCE(t.DurationExceeded, 0) - COALESCE(s.DurationExceeded, 0)) > {FLOAT_TOLERANCE} OR (t.DurationExceeded IS NULL AND s.DurationExceeded IS NOT NULL) OR (t.DurationExceeded IS NOT NULL AND s.DurationExceeded IS NULL)) OR
            (t.IsFalseAlarm IS DISTINCT FROM s.IsFalseAlarm) OR
            (t.FloorID IS DISTINCT FROM s.FloorID) OR
            (t.TimestampUTC IS DISTINCT FROM s.TimestampUTC) OR
            (t.IsInaccurate IS DISTINCT FROM s.IsInaccurate) OR
            (t.Latitude IS DISTINCT FROM s.Latitude) OR
            (t.Longitude IS DISTINCT FROM s.Longitude) OR
            (t.MediaURL IS DISTINCT FROM s.MediaURL) OR
            (t.ObservationClinicViolationStatusID IS DISTINCT FROM s.ObservationClinicViolationStatusID) OR
            (t.ObservationDiscriminatorID IS DISTINCT FROM s.ObservationDiscriminatorID) OR
            (t.ObservationSeverityID IS DISTINCT FROM s.ObservationSeverityID) OR
            (t.ObservationSourceID IS DISTINCT FROM s.ObservationSourceID) OR
            (t.ObservationStatusID IS DISTINCT FROM s.ObservationStatusID) OR
            (t.ObservationTypeID IS DISTINCT FROM s.ObservationTypeID) OR
            (t.OpenedAtUTC IS DISTINCT FROM s.OpenedAtUTC) OR
            (t.ExtPackageID IS DISTINCT FROM s.ExtPackageID) OR
            (t.ExtParentID IS DISTINCT FROM s.ExtParentID) OR
            (t.PermitNumber IS DISTINCT FROM s.PermitNumber) OR
            (t.PlateNumber IS DISTINCT FROM s.PlateNumber) OR
            (t.ProjectID IS DISTINCT FROM s.ProjectID) OR
            (t.RemainingLeaveDays IS DISTINCT FROM s.RemainingLeaveDays) OR
            (ABS(COALESCE(t.Speed, 0) - COALESCE(s.Speed, 0)) > {FLOAT_TOLERANCE} OR (t.Speed IS NULL AND s.Speed IS NOT NULL) OR (t.Speed IS NOT NULL AND s.Speed IS NULL)) OR
            (ABS(COALESCE(t.SpeedLimit, 0) - COALESCE(s.SpeedLimit, 0)) > {FLOAT_TOLERANCE} OR (t.SpeedLimit IS NULL AND s.SpeedLimit IS NOT NULL) OR (t.SpeedLimit IS NOT NULL AND s.SpeedLimit IS NULL)) OR
            (t.StopWorkNotice IS DISTINCT FROM s.StopWorkNotice) OR
            (ABS(COALESCE(t.Threshold, 0) - COALESCE(s.Threshold, 0)) > {FLOAT_TOLERANCE} OR (t.Threshold IS NULL AND s.Threshold IS NOT NULL) OR (t.Threshold IS NOT NULL AND s.Threshold IS NULL)) OR
            (t.UpdatedAt IS DISTINCT FROM s.UpdatedAt) OR
            (t.ViewedAtUTC IS DISTINCT FROM s.ViewedAtUTC) OR
            (t.WorkerID IS DISTINCT FROM s.WorkerID) OR
            (t.ZoneEntryTimeUTC IS DISTINCT FROM s.ZoneEntryTimeUTC) OR
            (t.ZoneExitTimeUTC IS DISTINCT FROM s.ZoneExitTimeUTC) OR
            (t.ZoneID IS DISTINCT FROM s.ZoneID)
        )
        THEN UPDATE SET
            t.WatermarkUTC = current_timestamp(),
            t.CameraName = s.CameraName,
            t.ClosedAtUTC = s.ClosedAtUTC,
            t.CompanyID = s.CompanyID,
            t.CreatedAt = s.CreatedAt,
            t.DeletedAt = s.DeletedAt,
            t.Description = s.Description,
            t.DeviceID = s.DeviceID,
            t.DurationExceeded = s.DurationExceeded,
            t.ExtSourceID = s.ExtSourceID,
            t.IsFalseAlarm = s.IsFalseAlarm,
            t.FloorID = s.FloorID,
            t.TimestampUTC = s.TimestampUTC,
            t.IsInaccurate = s.IsInaccurate,
            t.Latitude = s.Latitude,
            t.Longitude = s.Longitude,
            t.MediaURL = s.MediaURL,
            t.ObservationClinicViolationStatusID = s.ObservationClinicViolationStatusID,
            t.ObservationDiscriminatorID = s.ObservationDiscriminatorID,
            t.ObservationSeverityID = s.ObservationSeverityID,
            t.ObservationSourceID = s.ObservationSourceID,
            t.ObservationStatusID = s.ObservationStatusID,
            t.ObservationTypeID = s.ObservationTypeID,
            t.OpenedAtUTC = s.OpenedAtUTC,
            t.ExtPackageID = s.ExtPackageID,
            t.ExtParentID = s.ExtParentID,
            t.PermitNumber = s.PermitNumber,
            t.PlateNumber = s.PlateNumber,
            t.ProjectID = s.ProjectID,
            t.RemainingLeaveDays = s.RemainingLeaveDays,
            t.SerialNumber = s.SerialNumber,
            t.Speed = s.Speed,
            t.SpeedLimit = s.SpeedLimit,
            t.StopWorkNotice = s.StopWorkNotice,
            t.Threshold = s.Threshold,
            t.UpdatedAt = s.UpdatedAt,
            t.ViewedAtUTC = s.ViewedAtUTC,
            t.WorkerID = s.WorkerID,
            t.ZoneEntryTimeUTC = s.ZoneEntryTimeUTC,
            t.ZoneExitTimeUTC = s.ZoneExitTimeUTC,
            t.ZoneID = s.ZoneID
        WHEN NOT MATCHED THEN INSERT (
            CameraName, ClosedAtUTC, CompanyID, CreatedAt, DeletedAt, Description,
            DeviceID, DurationExceeded, ExtSourceID, IsFalseAlarm, FloorID,
            TimestampUTC, ExtObservationID, IsInaccurate, Latitude, Longitude,
            MediaURL, ObservationClinicViolationStatusID, ObservationDiscriminatorID,
            ObservationSeverityID, ObservationSourceID, ObservationStatusID,
            ObservationTypeID, OpenedAtUTC, ExtPackageID, ExtParentID, PermitNumber,
            PlateNumber, ProjectID, RemainingLeaveDays, SerialNumber, Speed, SpeedLimit,
            StopWorkNotice, Threshold, UpdatedAt, ViewedAtUTC, WorkerID,
            ZoneEntryTimeUTC, ZoneExitTimeUTC, ZoneID, WatermarkUTC
        ) VALUES (
            s.CameraName, s.ClosedAtUTC, s.CompanyID, s.CreatedAt, s.DeletedAt, s.Description,
            s.DeviceID, s.DurationExceeded, s.ExtSourceID, s.IsFalseAlarm, s.FloorID,
            s.TimestampUTC, s.ExtObservationID, s.IsInaccurate, s.Latitude, s.Longitude,
            s.MediaURL, s.ObservationClinicViolationStatusID, s.ObservationDiscriminatorID,
            s.ObservationSeverityID, s.ObservationSourceID, s.ObservationStatusID,
            s.ObservationTypeID, s.OpenedAtUTC, s.ExtPackageID, s.ExtParentID, s.PermitNumber,
            s.PlateNumber, s.ProjectID, s.RemainingLeaveDays, s.SerialNumber, s.Speed, s.SpeedLimit,
            s.StopWorkNotice, s.Threshold, s.UpdatedAt, s.ViewedAtUTC, s.WorkerID,
            s.ZoneEntryTimeUTC, s.ZoneExitTimeUTC, s.ZoneID, current_timestamp()
        )
    """

    spark.sql(merge_sql)
    print("MERGE completed successfully")

else:
    print(f"Creating new table: {TARGET_TABLE}")

    # Drop ExtSourceIDAlias before writing (not needed in target)
    merge_source_final = merge_source.drop("ExtSourceIDAlias")

    (merge_source_final
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TARGET_TABLE)
    )
    print(f"Created table with {merge_source_final.count():,} records")

# Clean up temp view
spark.catalog.dropTempView("merge_source_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Handle Stalled Records
# MAGIC
# MAGIC - Delete recovered stalled records that were successfully processed
# MAGIC - Insert new stalled records that couldn't be resolved

# COMMAND ----------

print("=" * 60)
print("PHASE 4: Handle Stalled Records")
print("=" * 60)

# Step 4a: Delete recovered stalled records
if recovered_count > 0:
    try:
        # Get the processed IDs from batch_unique
        processed_ids = batch_unique.select(F.col("ta.Id").alias("Id")).distinct()

        # Delete from stalled where Id is in processed
        processed_ids.createOrReplaceTempView("processed_ids_view")

        delete_sql = f"""
            DELETE FROM {STALLED_TABLE}
            WHERE Id IN (SELECT Id FROM processed_ids_view)
        """
        spark.sql(delete_sql)

        spark.catalog.dropTempView("processed_ids_view")
        print(f"  Deleted {recovered_count} recovered records from stalled table")
    except Exception as e:
        print(f"  Warning: Could not delete recovered records: {str(e)[:100]}")

# Step 4b: Find new stalled records (source records that didn't make it to batch)
print("\nChecking for new stalled records...")

# Get all source IDs
source_ids = source_df.select(F.col("Id")).distinct()

# Get processed IDs (RN=1 from batch)
batch_ids = batch_unique.select(F.col("ta.Id").alias("Id")).distinct()

# Find IDs that are in source but not in batch (stalled)
new_stalled_ids = source_ids.join(batch_ids, "Id", "left_anti")
new_stalled_count = new_stalled_ids.count()

print(f"New stalled records: {new_stalled_count}")

if new_stalled_count > 0:
    # Get full records for stalled
    new_stalled_df = source_df.join(new_stalled_ids, "Id", "inner")

    # Add computed columns (same as batch_df)
    # ProjectId: STRING (UUID) -> INT (will be NULL since UUIDs can't cast to INT)
    # Using try_cast semantics via expr to handle gracefully
    new_stalled_df = new_stalled_df.withColumn("ProjectId", F.expr("try_cast(ProjectId as INT)"))

    # Threshold: STRING -> DOUBLE
    new_stalled_df = new_stalled_df.withColumn("Threshold", F.col("Threshold").cast("double"))

    # Parse Latitude/Longitude from LocationWKT if available
    new_stalled_df = new_stalled_df.withColumn(
        "Latitude",
        F.when(
            F.col("LocationWKT").isNotNull() & F.col("LocationWKT").startswith("POINT"),
            F.regexp_extract(F.col("LocationWKT"), r"POINT\s*\(\s*[\d.-]+\s+([\d.-]+)\s*\)", 1).cast("double")
        ).otherwise(F.lit(None).cast("double"))
    )
    new_stalled_df = new_stalled_df.withColumn(
        "Longitude",
        F.when(
            F.col("LocationWKT").isNotNull() & F.col("LocationWKT").startswith("POINT"),
            F.regexp_extract(F.col("LocationWKT"), r"POINT\s*\(\s*([\d.-]+)\s+[\d.-]+\s*\)", 1).cast("double")
        ).otherwise(F.lit(None).cast("double"))
    )

    # Add stalled metadata
    new_stalled_df = (
        new_stalled_df
        .withColumn("_stalled_at", F.current_timestamp())
        .withColumn("_stall_reason", F.lit("Dimension lookup failed"))
    )

    # Select columns matching stalled table schema
    stalled_cols = [
        "Id", "ExternalId", "ProjectId", "Source", "Type", "GeneratedAt",
        "Description", "PackageId", "CompanyId", "Severity", "Viewed",
        "Discriminator", "PlateNumber", "Speed", "SpeedLimit", "MediaUrl",
        "Inaccurate", "Latitude", "Longitude", "SpaceId", "ZoneId",
        "ResourceId", "ZoneEntryTime", "ZoneExitTime", "DurationExceeded",
        "CreatedAt", "UpdatedAt", "DeletedAt", "DeviceId", "Status",
        "OpenedAt", "ClosedAt", "FalseAlarm", "ViewedAt", "CameraName",
        "PermitNumber", "StopWorkNotice", "SerialNo", "Threshold",
        "ClinicViolationStatus", "RemainingLeaveDays", "ParentId",
        "_stalled_at", "_stall_reason"
    ]

    # Only include columns that exist
    available_cols = [c for c in stalled_cols if c in new_stalled_df.columns]
    new_stalled_final = new_stalled_df.select(*available_cols)

    # Merge new stalled records
    new_stalled_final.createOrReplaceTempView("new_stalled_view")

    stalled_merge_sql = f"""
        MERGE INTO {STALLED_TABLE} AS t
        USING new_stalled_view AS s
        ON t.Id = s.Id
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(stalled_merge_sql)

    spark.catalog.dropTempView("new_stalled_view")
    print(f"  Inserted {new_stalled_count} new stalled records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Update Watermark

# COMMAND ----------

# Get max timestamp from processed data
max_loaded_at = merge_source.agg(F.max("UpdatedAt")).collect()[0][0]
if max_loaded_at is None:
    max_loaded_at = datetime.now()

final_count = spark.table(TARGET_TABLE).count()

# Prepare metrics
metrics = {
    'processed': unique_count,
    'stalled': new_stalled_count if 'new_stalled_count' in dir() else 0,
    'recovered': recovered_count
}

update_watermark("fact_observations", max_loaded_at, final_count, metrics)
print(f"Updated watermark to: {max_loaded_at}")
print(f"Total records in target table: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 60)
print("VALIDATION")
print("=" * 60)

# Row counts
source_total = spark.table(SOURCE_TABLE).count()
target_total = spark.table(TARGET_TABLE).filter(F.col("ExtSourceID") == EXT_SOURCE_ID).count()
stalled_total = spark.table(STALLED_TABLE).count()

print(f"\nRow Counts:")
print(f"  Source (timescale_observations):     {source_total:,}")
print(f"  Target (fact_observations, ExtSourceID={EXT_SOURCE_ID}): {target_total:,}")
print(f"  Stalled:                              {stalled_total:,}")
print(f"  Target + Stalled:                     {target_total + stalled_total:,}")

# Check for nulls in required dimensions
print(f"\nData Quality Check - Required Dimension Nulls:")
quality_check = spark.sql(f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN ProjectID IS NULL THEN 1 ELSE 0 END) as null_project,
        SUM(CASE WHEN ObservationDiscriminatorID IS NULL THEN 1 ELSE 0 END) as null_discriminator,
        SUM(CASE WHEN ObservationSourceID IS NULL THEN 1 ELSE 0 END) as null_source,
        SUM(CASE WHEN ObservationTypeID IS NULL THEN 1 ELSE 0 END) as null_type
    FROM {TARGET_TABLE}
    WHERE ExtSourceID = {EXT_SOURCE_ID}
""")
display(quality_check)

# Check for duplicates
print(f"\nDuplicate Check:")
dup_check = spark.sql(f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT ExtObservationID) as distinct_obs
    FROM {TARGET_TABLE}
    WHERE ExtSourceID = {EXT_SOURCE_ID}
""")
dup_result = dup_check.collect()[0]
if dup_result['total_rows'] == dup_result['distinct_obs']:
    print(f"  [OK] No duplicates: {dup_result['total_rows']:,} total = {dup_result['distinct_obs']:,} distinct")
else:
    print(f"  [WARN] Duplicates found: {dup_result['total_rows']:,} total vs {dup_result['distinct_obs']:,} distinct")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Clean up cached DataFrame
batch_df.unpersist()

# Final summary
print("=" * 60)
print("DELTA SYNC FACT OBSERVATIONS - COMPLETE")
print("=" * 60)
print(f"""
Processing Summary:
  Source records processed:  {source_count:,}
  Dimension lookups:         12 tables
  Records merged:            {unique_count:,}
  Records stalled:           {new_stalled_count if 'new_stalled_count' in dir() else 0:,}
  Records recovered:         {recovered_count:,}

Target Table:
  {TARGET_TABLE}
  Total rows: {final_count:,}

Stalled Table:
  {STALLED_TABLE}
  Total rows: {stalled_total:,}
""")

dbutils.notebook.exit("SUCCESS")
