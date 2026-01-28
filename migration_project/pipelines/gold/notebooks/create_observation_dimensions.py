# Databricks notebook source
# MAGIC %md
# MAGIC # Create Observation Dimension Tables
# MAGIC
# MAGIC **Purpose:** Create observation dimension tables from DISTINCT values in observation_observation Bronze table.
# MAGIC These dimension tables are required by delta_sync_fact_observations.py for resolving observation attributes.
# MAGIC
# MAGIC **ADF Equivalent:** SyncDimensionsObservations pipeline
# MAGIC - SyncDimOSource -> ObservationSource
# MAGIC - SyncDimOStatus -> ObservationStatus
# MAGIC - SyncDimOType -> ObservationType
# MAGIC - SyncDimOSeverity -> ObservationSeverity
# MAGIC - SyncDimODiscriminator -> ObservationDiscriminator
# MAGIC - SyncDimOClinicViolationStatus -> ObservationClinicViolationStatus
# MAGIC
# MAGIC **Source:** `wakecap_prod.raw.observation_observation`
# MAGIC **Target:** Silver dimension tables in `wakecap_prod.silver`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"

SOURCE_TABLE = f"{TARGET_CATALOG}.{RAW_SCHEMA}.observation_observation"

# ExtSourceID for observations (matches ADF)
EXT_SOURCE_ID = 19

print(f"Source: {SOURCE_TABLE}")
print(f"Target Schema: {TARGET_CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Ensure target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
print(f"[OK] Target schema verified")

# Check source table
try:
    source_count = spark.table(SOURCE_TABLE).count()
    print(f"[OK] Source table exists: {SOURCE_TABLE}")
    print(f"     Row count: {source_count:,}")
except Exception as e:
    print(f"[ERROR] Source table not found: {SOURCE_TABLE}")
    dbutils.notebook.exit("SOURCE_TABLE_NOT_FOUND")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function

# COMMAND ----------

def create_dimension_from_distinct(source_column, target_table, id_column_name):
    """
    Create a dimension table from DISTINCT values of a source column.

    Matches ADF pattern:
        SELECT DISTINCT LEFT(TRIM("Column"), 50) AS "ObservationXxx", 19 AS "ExtSourceID"
        FROM public."Observation"
        WHERE "Column" IS NOT NULL

    Args:
        source_column: Name of the source column in observation_observation
        target_table: Full path to target dimension table
        id_column_name: Name of the ID column (e.g., ObservationSourceID)

    Returns:
        Record count of created dimension
    """
    print(f"\nCreating {target_table}...")

    # Load source and get distinct values
    source_df = spark.table(SOURCE_TABLE)

    # Get the dimension name (e.g., "ObservationSource" from "Source")
    dimension_name = f"Observation{source_column}" if not source_column.startswith("Observation") else source_column

    # Apply LEFT(TRIM(col), 50) transformation and filter NULLs
    # For Type column, use 100 chars (matches ADF)
    max_length = 100 if source_column == "Type" else 50

    distinct_df = source_df.select(
        F.trim(F.col(source_column)).alias("_raw")
    ).filter(
        F.col("_raw").isNotNull() & (F.trim(F.col("_raw")) != "")
    ).distinct()

    # Add dimension columns
    window = Window.orderBy(F.col("_raw"))

    dim_df = distinct_df.select(
        F.row_number().over(window).alias(id_column_name),
        F.substring(F.col("_raw"), 1, max_length).alias(dimension_name),
        F.lit(EXT_SOURCE_ID).alias("ExtSourceID")
    )

    count = dim_df.count()
    print(f"  Found {count} distinct values")

    # Write to target table (overwrite for idempotency)
    (dim_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(target_table)
    )

    print(f"  [OK] Created {target_table} with {count} rows")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimension Tables

# COMMAND ----------

print("=" * 60)
print("CREATING OBSERVATION DIMENSION TABLES")
print("=" * 60)

results = {}

# 1. ObservationSource
results["source"] = create_dimension_from_distinct(
    source_column="Source",
    target_table=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_source",
    id_column_name="ObservationSourceID"
)

# 2. ObservationStatus
results["status"] = create_dimension_from_distinct(
    source_column="Status",
    target_table=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_status",
    id_column_name="ObservationStatusID"
)

# 3. ObservationType
results["type"] = create_dimension_from_distinct(
    source_column="Type",
    target_table=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_type",
    id_column_name="ObservationTypeID"
)

# 4. ObservationSeverity
results["severity"] = create_dimension_from_distinct(
    source_column="Severity",
    target_table=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_severity",
    id_column_name="ObservationSeverityID"
)

# 5. ObservationDiscriminator
results["discriminator"] = create_dimension_from_distinct(
    source_column="Discriminator",
    target_table=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_discriminator",
    id_column_name="ObservationDiscriminatorID"
)

# 6. ObservationClinicViolationStatus
results["clinic_status"] = create_dimension_from_distinct(
    source_column="ClinicViolationStatus",
    target_table=f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_observation_clinic_violation_status",
    id_column_name="ObservationClinicViolationStatusID"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("DIMENSION CREATION SUMMARY")
print("=" * 60)
print(f"Source: {SOURCE_TABLE}")
print(f"Target Schema: {TARGET_CATALOG}.{SILVER_SCHEMA}")
print(f"ExtSourceID: {EXT_SOURCE_ID}")
print("")
print("Dimensions Created:")
for name, count in results.items():
    print(f"  - {name}: {count} values")
print("")
print(f"Total dimensions: {len(results)}")
print("=" * 60)

dbutils.notebook.exit(f"SUCCESS: created {len(results)} dimension tables")
