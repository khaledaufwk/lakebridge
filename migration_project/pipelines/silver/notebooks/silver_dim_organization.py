# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Dimension Organization (DeltaSync)
# MAGIC
# MAGIC **Converted from:** `stg.spDeltaSyncDimOrganization` (90 lines)
# MAGIC
# MAGIC **Purpose:** Sync organization dimension from TimescaleDB source (wc2023_Organization).
# MAGIC This is the simplest dimension sync - no dimension lookups, just source-to-target MERGE.
# MAGIC
# MAGIC **Pattern:** This notebook serves as the base template for all DeltaSync dimension SPs.
# MAGIC Other dimensions follow the same pattern with additional dimension lookups.
# MAGIC
# MAGIC **Source Table:** `wakecap_prod.bronze.wc2023_organization`
# MAGIC **Target:** `wakecap_prod.silver.silver_organization`
# MAGIC **ExtSourceID:** 14

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# Configuration
TARGET_CATALOG = "wakecap_prod"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
MIGRATION_SCHEMA = "migration"

SOURCE_TABLE = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_organization"
TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_organization"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

EXT_SOURCE_ID = 14

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
load_mode = dbutils.widgets.get("load_mode")

# COMMAND ----------

# Pre-flight check
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} LIMIT 0")
    print(f"[OK] Source exists")
except:
    dbutils.notebook.exit("SOURCE_NOT_FOUND")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")

# COMMAND ----------

# Load and prepare source
source_df = spark.table(SOURCE_TABLE)
source_count = source_df.count()

# Add ExtSourceID and calculated columns
# Original: LEFT(TRIM(LogoUrl), 1024) as ImageURL
source_prepared = source_df \
    .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID)) \
    .withColumn("ImageURL", F.substring(F.trim(F.col("LogoUrl")), 1, 1024))

# ROW_NUMBER deduplication on Id
dedup_window = Window.partitionBy("Id").orderBy(F.lit(1))
source_final = source_prepared \
    .withColumn("RN", F.row_number().over(dedup_window)) \
    .filter(F.col("RN") == 1) \
    .drop("RN")

source_final.cache()
final_count = source_final.count()
print(f"Source: {source_count} | After dedup: {final_count}")

# COMMAND ----------

# Create target table if not exists
target_schema = """
    OrganizationID BIGINT GENERATED ALWAYS AS IDENTITY,
    ExtOrganizationID STRING,
    ExtSourceID INT,
    Organization STRING,
    OrganizationName STRING,
    Status STRING,
    ImageURL STRING,
    TimeZoneName STRING,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP,
    WatermarkUTC TIMESTAMP DEFAULT current_timestamp(),
    _silver_processed_at TIMESTAMP DEFAULT current_timestamp()
"""

try:
    spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    print(f"[OK] Target exists")
except:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({target_schema})
        USING DELTA CLUSTER BY (ExtSourceID, ExtOrganizationID)
    """)

try:
    target_before = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    target_before = 0

# COMMAND ----------

# Execute MERGE
source_final.createOrReplaceTempView("org_source")

merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING org_source AS s
ON t.ExtOrganizationID = s.Id
   AND CASE WHEN t.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE t.ExtSourceID END
     = CASE WHEN s.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE s.ExtSourceID END

WHEN MATCHED AND (
    t.Organization IS DISTINCT FROM s.Name OR
    t.Status IS DISTINCT FROM s.Status OR
    t.ImageURL IS DISTINCT FROM s.ImageURL
)
THEN UPDATE SET
    t.WatermarkUTC = current_timestamp(),
    t._silver_processed_at = current_timestamp(),
    t.Organization = s.Name,
    t.OrganizationName = s.Name,
    t.Status = s.Status,
    t.ImageURL = s.ImageURL,
    t.ExtSourceID = s.ExtSourceID,
    t.CreatedAt = s.CreatedAt,
    t.UpdatedAt = s.UpdatedAt

WHEN NOT MATCHED THEN INSERT (
    ExtOrganizationID, ExtSourceID, Organization, OrganizationName, Status, ImageURL, CreatedAt, UpdatedAt
)
VALUES (s.Id, s.ExtSourceID, s.Name, s.Name, s.Status, s.ImageURL, s.CreatedAt, s.UpdatedAt)
"""

spark.sql(merge_sql)

# COMMAND ----------

# Summary
source_final.unpersist()
target_after = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

print("=" * 60)
print(f"Source: {source_count} | Processed: {final_count}")
print(f"Target: {target_before} -> {target_after} (+{target_after - target_before})")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: {final_count} processed")
