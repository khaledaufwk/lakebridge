# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Dimension Worker (DeltaSync)
# MAGIC
# MAGIC **Converted from:** `stg.spDeltaSyncDimWorker` (228 lines)
# MAGIC
# MAGIC **Purpose:** Sync worker dimension from TimescaleDB source (wc2023_People)
# MAGIC to the silver Worker dimension table with dimension resolution.
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - ROW_NUMBER deduplication on source Id
# MAGIC - LEFT JOIN dimension lookups (Project, Trade, Company) with fnExtSourceIDAlias
# MAGIC - Calculated columns with LEFT(TRIM(...), length) for string truncation
# MAGIC - MERGE with change detection on all comparable columns
# MAGIC - Post-MERGE UPDATE to fill NULL dimension IDs from newly resolved dimensions
# MAGIC
# MAGIC **Source Table:** `wakecap_prod.bronze.wc2023_people` (stg.wc2023_People)
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC - `wakecap_prod.silver.silver_project` (dbo.Project)
# MAGIC - `wakecap_prod.silver.silver_trade` (dbo.Trade)
# MAGIC - `wakecap_prod.silver.silver_company` (dbo.Company)
# MAGIC
# MAGIC **Target:** `wakecap_prod.silver.silver_worker`
# MAGIC **Watermarks:** `wakecap_prod.migration._gold_watermarks`
# MAGIC
# MAGIC **ExtSourceID:** 15 (ExtSourceIDAlias: 15)

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
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
MIGRATION_SCHEMA = "migration"

# Source table (TimescaleDB)
SOURCE_PEOPLE = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_people"

# Dimension tables for lookups
DIM_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"
DIM_TRADE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_trade"
DIM_COMPANY = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_company"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID
EXT_SOURCE_ID = 15
EXT_SOURCE_ID_ALIAS = 15

print(f"Source: {SOURCE_PEOPLE}")
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

# Check source table
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_PEOPLE} LIMIT 0")
    print(f"[OK] Source exists: {SOURCE_PEOPLE}")
except Exception as e:
    print(f"[ERROR] Source not found: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_NOT_FOUND")

# Check dimension tables (optional - LEFT JOIN)
dim_tables = {
    "Project": DIM_PROJECT,
    "Trade": DIM_TRADE,
    "Company": DIM_COMPANY,
}

available_dims = {}
for name, table in dim_tables.items():
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} dimension exists: {table}")
        available_dims[name] = True
    except:
        print(f"[WARN] {name} dimension not found (will use NULL for lookups)")
        available_dims[name] = False

# Ensure target schema exists
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


def fn_ext_source_id_alias(col_name):
    """PySpark equivalent of stg.fnExtSourceIDAlias(ExtSourceID)."""
    return F.when(F.col(col_name).isin(15, 18), F.lit(15)) \
            .when(F.col(col_name).isin(1, 2, 10, 14, 21), F.lit(1)) \
            .otherwise(F.col(col_name))


def left_trim(col_name, max_length):
    """PySpark equivalent of LEFT(TRIM(column), length)."""
    return F.substring(F.trim(F.col(col_name)), 1, max_length)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare Dimension Lookups

# COMMAND ----------

print("=" * 60)
print("STEP 1: Prepare Dimension Lookups")
print("=" * 60)

# Project dimension lookup
if available_dims["Project"]:
    project_lookup_df = spark.table(DIM_PROJECT) \
        .withColumn("_ext_source_alias", fn_ext_source_id_alias("ExtSourceID")) \
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("ExtProjectID", "_ext_source_alias").orderBy(F.lit(1))
        )) \
        .filter(F.col("_rn") == 1) \
        .filter(F.col("_ext_source_alias") == EXT_SOURCE_ID_ALIAS) \
        .select(
            F.col("ProjectID").alias("dim_ProjectID"),
            F.col("ExtProjectID").alias("dim_ExtProjectID")
        )
    print(f"Project dimension: {project_lookup_df.count()} records")
else:
    project_lookup_df = None

# Trade dimension lookup
if available_dims["Trade"]:
    trade_lookup_df = spark.table(DIM_TRADE) \
        .withColumn("_ext_source_alias", fn_ext_source_id_alias("ExtSourceID")) \
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("ExtTradeID", "_ext_source_alias").orderBy(F.lit(1))
        )) \
        .filter(F.col("_rn") == 1) \
        .filter(F.col("_ext_source_alias") == EXT_SOURCE_ID_ALIAS) \
        .select(
            F.col("TradeID").alias("dim_TradeID"),
            F.col("ExtTradeID").alias("dim_ExtTradeID")
        )
    print(f"Trade dimension: {trade_lookup_df.count()} records")
else:
    trade_lookup_df = None

# Company dimension lookup
if available_dims["Company"]:
    company_lookup_df = spark.table(DIM_COMPANY) \
        .withColumn("_ext_source_alias", fn_ext_source_id_alias("ExtSourceID")) \
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("ExtCompanyID", "_ext_source_alias").orderBy(F.lit(1))
        )) \
        .filter(F.col("_rn") == 1) \
        .filter(F.col("_ext_source_alias") == EXT_SOURCE_ID_ALIAS) \
        .select(
            F.col("CompanyID").alias("dim_CompanyID"),
            F.col("ExtCompanyID").alias("dim_ExtCompanyID")
        )
    print(f"Company dimension: {company_lookup_df.count()} records")
else:
    company_lookup_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Source DataFrame

# COMMAND ----------

print("=" * 60)
print("STEP 2: Build Source DataFrame")
print("=" * 60)

# Load source data
source_df = spark.table(SOURCE_PEOPLE)
source_count = source_df.count()
print(f"Source records: {source_count}")

# Apply calculated columns (LEFT(TRIM(...), length))
# Original: Worker=LEFT(TRIM(PeopleCode),50), WorkerName=LEFT(TRIM(Name),100), etc.
source_with_calc = source_df \
    .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID)) \
    .withColumn("Worker", left_trim("PeopleCode", 50)) \
    .withColumn("WorkerName", left_trim("Name", 100)) \
    .withColumn("MobileNumber", left_trim("Mobile", 50)) \
    .withColumn("Email2", left_trim("Email", 50)) \
    .withColumn("ImageURL", left_trim("Picture", 1024)) \
    .withColumn("Color", left_trim("HelmetColor", 10)) \
    .withColumn("Address2", left_trim("Address", 100)) \
    .withColumn("Nationality2", left_trim("Nationality", 100))

# ROW_NUMBER deduplication on Id
# Original: PARTITION BY Id ORDER BY (SELECT NULL)
dedup_window = Window.partitionBy("Id").orderBy(F.lit(1))
source_dedup = source_with_calc \
    .withColumn("RN", F.row_number().over(dedup_window)) \
    .filter(F.col("RN") == 1) \
    .drop("RN")

# COMMAND ----------

# Join with Project dimension (LEFT JOIN)
if project_lookup_df is not None:
    source_with_project = source_dedup.alias("src").join(
        project_lookup_df.alias("proj"),
        F.col("src.ProjectId") == F.col("proj.dim_ExtProjectID"),
        "left"
    ).select(
        "src.*",
        F.col("proj.dim_ProjectID").alias("ProjectID2")
    )
else:
    source_with_project = source_dedup.withColumn("ProjectID2", F.lit(None).cast("int"))

# Join with Trade dimension (LEFT JOIN)
if trade_lookup_df is not None:
    source_with_trade = source_with_project.alias("src").join(
        trade_lookup_df.alias("trade"),
        F.col("src.TradeId") == F.col("trade.dim_ExtTradeID"),
        "left"
    ).select(
        "src.*",
        F.col("trade.dim_TradeID").alias("TradeID2")
    )
else:
    source_with_trade = source_with_project.withColumn("TradeID2", F.lit(None).cast("int"))

# Join with Company dimension (LEFT JOIN)
if company_lookup_df is not None:
    source_final = source_with_trade.alias("src").join(
        company_lookup_df.alias("comp"),
        F.col("src.CompanyId") == F.col("comp.dim_ExtCompanyID"),
        "left"
    ).select(
        "src.*",
        F.col("comp.dim_CompanyID").alias("CompanyID2")
    )
else:
    source_final = source_with_trade.withColumn("CompanyID2", F.lit(None).cast("int"))

source_final.cache()
final_count = source_final.count()
print(f"Source after dimension joins: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create/Update Target Table

# COMMAND ----------

print("=" * 60)
print("STEP 3: Ensure Target Table Exists")
print("=" * 60)

target_schema = """
    WorkerID BIGINT GENERATED ALWAYS AS IDENTITY,
    ExtWorkerID STRING,
    ExtSourceID INT,
    ExtProjectID STRING,
    ProjectID INT,
    ExtCompanyID STRING,
    CompanyID INT,
    ExtTradeID STRING,
    TradeID INT,
    ExtTitleID STRING,
    ExtDepartmentID STRING,
    Worker STRING,
    WorkerName STRING,
    MobileNumber STRING,
    Email STRING,
    ImageURL STRING,
    Color STRING,
    Address STRING,
    Nationality STRING,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP,
    DeletedAt TIMESTAMP,
    WatermarkUTC TIMESTAMP DEFAULT current_timestamp(),
    _silver_processed_at TIMESTAMP DEFAULT current_timestamp()
"""

try:
    spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    print(f"[OK] Target table exists: {TARGET_TABLE}")
except:
    print(f"[INFO] Creating target table...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            {target_schema}
        )
        USING DELTA
        CLUSTER BY (ExtSourceID, ExtWorkerID)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    print(f"[OK] Created target table: {TARGET_TABLE}")

# Get target row count before merge
try:
    target_before = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0
print(f"Target rows before MERGE: {target_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Execute MERGE

# COMMAND ----------

print("=" * 60)
print("STEP 4: Execute MERGE")
print("=" * 60)

# Create temp view for source
source_final.createOrReplaceTempView("worker_source")

# Execute MERGE
# Original match: s.Id = t.ExtWorkerID AND fnExtSourceIDAlias(s.ExtSourceID) = fnExtSourceIDAlias(t.ExtSourceID)
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING worker_source AS s
ON t.ExtWorkerID = s.Id
   AND CASE WHEN t.ExtSourceID IN (15, 18) THEN 15 WHEN t.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE t.ExtSourceID END
     = CASE WHEN s.ExtSourceID IN (15, 18) THEN 15 WHEN s.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE s.ExtSourceID END

WHEN MATCHED AND (
    t.Address IS DISTINCT FROM s.Address2 OR
    t.Color IS DISTINCT FROM s.Color OR
    t.ExtCompanyID IS DISTINCT FROM s.CompanyId OR
    t.CompanyID IS DISTINCT FROM s.CompanyID2 OR
    t.DeletedAt IS DISTINCT FROM s.DeletedAt OR
    t.ExtDepartmentID IS DISTINCT FROM s.DepartmentId OR
    t.Email IS DISTINCT FROM s.Email2 OR
    t.ImageURL IS DISTINCT FROM s.ImageURL OR
    t.MobileNumber IS DISTINCT FROM s.MobileNumber OR
    t.Nationality IS DISTINCT FROM s.Nationality2 OR
    t.ExtProjectID IS DISTINCT FROM s.ProjectId OR
    t.ProjectID IS DISTINCT FROM s.ProjectID2 OR
    t.ExtTitleID IS DISTINCT FROM s.TitleId OR
    t.ExtTradeID IS DISTINCT FROM s.TradeId OR
    t.TradeID IS DISTINCT FROM s.TradeID2 OR
    t.Worker IS DISTINCT FROM s.Worker OR
    t.WorkerName IS DISTINCT FROM s.WorkerName
)
THEN UPDATE SET
    t.WatermarkUTC = current_timestamp(),
    t._silver_processed_at = current_timestamp(),
    t.Address = s.Address2,
    t.Color = s.Color,
    t.ExtCompanyID = s.CompanyId,
    t.CompanyID = s.CompanyID2,
    t.CreatedAt = s.CreatedAt,
    t.DeletedAt = s.DeletedAt,
    t.ExtDepartmentID = s.DepartmentId,
    t.Email = s.Email2,
    t.ExtSourceID = s.ExtSourceID,
    t.ImageURL = s.ImageURL,
    t.MobileNumber = s.MobileNumber,
    t.Nationality = s.Nationality2,
    t.ExtProjectID = s.ProjectId,
    t.ProjectID = s.ProjectID2,
    t.ExtTitleID = s.TitleId,
    t.ExtTradeID = s.TradeId,
    t.TradeID = s.TradeID2,
    t.UpdatedAt = s.UpdatedAt,
    t.Worker = s.Worker,
    t.WorkerName = s.WorkerName

WHEN NOT MATCHED THEN INSERT (
    Address, Color, ExtCompanyID, CompanyID, CreatedAt, DeletedAt,
    ExtDepartmentID, Email, ExtSourceID, ExtWorkerID, ImageURL, MobileNumber,
    Nationality, ExtProjectID, ProjectID, ExtTitleID, ExtTradeID, TradeID,
    UpdatedAt, Worker, WorkerName
)
VALUES (
    s.Address2, s.Color, s.CompanyId, s.CompanyID2, s.CreatedAt, s.DeletedAt,
    s.DepartmentId, s.Email2, s.ExtSourceID, s.Id, s.ImageURL, s.MobileNumber,
    s.Nationality2, s.ProjectId, s.ProjectID2, s.TitleId, s.TradeId, s.TradeID2,
    s.UpdatedAt, s.Worker, s.WorkerName
)
"""

spark.sql(merge_sql)
print("[OK] MERGE completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Post-MERGE Update for NULL Dimension IDs
# MAGIC
# MAGIC Original pattern: After main MERGE, update records where dimension IDs became resolvable
# MAGIC (new dimension records may have been inserted between worker insert and now)

# COMMAND ----------

print("=" * 60)
print("STEP 5: Post-MERGE Dimension Resolution")
print("=" * 60)

# Re-resolve dimensions for records with NULL dimension IDs
# Original: UPDATE t SET ProjectID = ISNULL(t.ProjectID, t1.ProjectID), etc.
# WHERE (t.ProjectID IS NULL AND t1.ProjectID IS NOT NULL) OR ...

if project_lookup_df is not None or trade_lookup_df is not None or company_lookup_df is not None:
    # Build the update using a join approach
    target_df = spark.table(TARGET_TABLE)

    # Find records with any NULL dimension ID
    null_dim_filter = (
        (F.col("ProjectID").isNull() & F.col("ExtProjectID").isNotNull()) |
        (F.col("TradeID").isNull() & F.col("ExtTradeID").isNotNull()) |
        (F.col("CompanyID").isNull() & F.col("ExtCompanyID").isNotNull())
    )

    records_to_update = target_df.filter(null_dim_filter)
    update_count = records_to_update.count()

    if update_count > 0:
        print(f"Records with NULL dimension IDs to resolve: {update_count}")

        # Join with dimensions and update
        update_sql = f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING (
            SELECT t.WorkerID, t.ExtProjectID, t.ExtTradeID, t.ExtCompanyID,
                   p.dim_ProjectID, tr.dim_TradeID, c.dim_CompanyID
            FROM {TARGET_TABLE} t
            LEFT JOIN (
                SELECT dim_ProjectID, dim_ExtProjectID FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY dim_ExtProjectID ORDER BY 1) as rn
                    FROM (SELECT ProjectID as dim_ProjectID, ExtProjectID as dim_ExtProjectID FROM {DIM_PROJECT})
                ) WHERE rn = 1
            ) p ON p.dim_ExtProjectID = t.ExtProjectID
            LEFT JOIN (
                SELECT dim_TradeID, dim_ExtTradeID FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY dim_ExtTradeID ORDER BY 1) as rn
                    FROM (SELECT TradeID as dim_TradeID, ExtTradeID as dim_ExtTradeID FROM {DIM_TRADE})
                ) WHERE rn = 1
            ) tr ON tr.dim_ExtTradeID = t.ExtTradeID
            LEFT JOIN (
                SELECT dim_CompanyID, dim_ExtCompanyID FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY dim_ExtCompanyID ORDER BY 1) as rn
                    FROM (SELECT CompanyID as dim_CompanyID, ExtCompanyID as dim_ExtCompanyID FROM {DIM_COMPANY})
                ) WHERE rn = 1
            ) c ON c.dim_ExtCompanyID = t.ExtCompanyID
            WHERE (t.ProjectID IS NULL AND p.dim_ProjectID IS NOT NULL)
               OR (t.TradeID IS NULL AND tr.dim_TradeID IS NOT NULL)
               OR (t.CompanyID IS NULL AND c.dim_CompanyID IS NOT NULL)
        ) s
        ON t.WorkerID = s.WorkerID
        WHEN MATCHED THEN UPDATE SET
            t.ProjectID = COALESCE(t.ProjectID, s.dim_ProjectID),
            t.TradeID = COALESCE(t.TradeID, s.dim_TradeID),
            t.CompanyID = COALESCE(t.CompanyID, s.dim_CompanyID),
            t.WatermarkUTC = current_timestamp()
        """
        try:
            spark.sql(update_sql)
            print("[OK] Post-MERGE dimension resolution completed")
        except Exception as e:
            print(f"[WARN] Post-MERGE update failed: {str(e)[:100]}")
    else:
        print("No records with NULL dimension IDs")
else:
    print("Skipping post-MERGE update (no dimension tables available)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Update Watermark

# COMMAND ----------

print("=" * 60)
print("STEP 6: Update Watermark")
print("=" * 60)

# Get final target count
target_after = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0][0]
inserted = target_after - target_before

# Update watermark
new_watermark = datetime.now()
update_watermark("wc2023_People", new_watermark, target_after)

print(f"Watermark updated to: {new_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Cleanup
source_final.unpersist()

# Print summary
print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_PEOPLE}")
print(f"Target: {TARGET_TABLE}")
print(f"")
print(f"Records processed:")
print(f"  - Source: {source_count}")
print(f"  - After dedup and joins: {final_count}")
print(f"")
print(f"Target table:")
print(f"  - Rows before: {target_before}")
print(f"  - Rows after: {target_after}")
print(f"  - Estimated inserts: {inserted}")
print(f"")
print(f"Mode: {load_mode}")
print("=" * 60)

# Return success
dbutils.notebook.exit(f"SUCCESS: processed={final_count}, target={target_after}")
