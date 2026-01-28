# Databricks notebook source
# MAGIC %md
# MAGIC # ADF Helper Functions
# MAGIC
# MAGIC Common helper functions that replicate ADF logic for use across Gold layer notebooks.
# MAGIC
# MAGIC **Equivalent ADF Operations:**
# MAGIC - MV_ResourceDevice_NoViolation materialized view
# MAGIC - Date range filtering (2000-01-01 to 2100-01-01)
# MAGIC - LinkedUserId to WorkerId resolution

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Date range constants for filtering (matches ADF logic)
DATE_MIN = "2000-01-01"
DATE_MAX = "2100-01-01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## MV_ResourceDevice_NoViolation Equivalent

# COMMAND ----------

def get_resource_device_no_violation(spark, catalog="wakecap_prod", schema="silver"):
    """
    Equivalent to PostgreSQL MV_ResourceDevice_NoViolation materialized view.

    Returns active device-worker assignments for joining with DeviceLocation.

    Original ADF refreshes this view before DeltaCopyAssetLocation:
        REFRESH MATERIALIZED VIEW public."MV_ResourceDevice_NoViolation";

    Usage in ADF DeltaCopyAssetLocation:
        INNER JOIN public."MV_ResourceDevice_NoViolation" da
            ON da."DeviceId" = dl."DeviceId"
            AND dl."GeneratedAt" >= da."AssignedAt"
            AND (dl."GeneratedAt" < da."UnAssignedAt" OR da."UnAssignedAt" IS NULL)

    Args:
        spark: SparkSession
        catalog: Catalog name (default: wakecap_prod)
        schema: Schema name (default: silver)

    Returns:
        DataFrame with columns: DeviceId, WorkerId, ProjectId, AssignedAt, UnassignedAt
    """
    return spark.sql(f"""
        SELECT
            DeviceId,
            WorkerId,
            ProjectId,
            AssignedAt,
            UnassignedAt
        FROM {catalog}.{schema}.silver_resource_device
        WHERE DeletedAt IS NULL
    """)


def join_device_location_with_assignment(device_location_df, resource_device_df):
    """
    Join DeviceLocation with MV_ResourceDevice_NoViolation equivalent.

    Matches ADF logic:
        INNER JOIN ... ON da."DeviceId" = dl."DeviceId"
            AND dl."GeneratedAt" >= da."AssignedAt"
            AND (dl."GeneratedAt" < da."UnAssignedAt" OR da."UnAssignedAt" IS NULL)

    Args:
        device_location_df: DataFrame with DeviceId, GeneratedAt columns
        resource_device_df: Result of get_resource_device_no_violation()

    Returns:
        DataFrame with WorkerId (ResourceId) resolved from device assignment
    """
    return device_location_df.alias("dl").join(
        resource_device_df.alias("da"),
        (F.col("dl.DeviceId") == F.col("da.DeviceId")) &
        (F.col("dl.GeneratedAt") >= F.col("da.AssignedAt")) &
        (
            (F.col("dl.GeneratedAt") < F.col("da.UnassignedAt")) |
            F.col("da.UnassignedAt").isNull()
        ),
        "inner"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Range Filtering

# COMMAND ----------

def filter_valid_date(df, date_column, min_date=DATE_MIN, max_date=DATE_MAX, allow_null=True):
    """
    Filter DataFrame to rows with valid dates in the specified column.

    Matches ADF pattern:
        WHERE "EffectiveDate" BETWEEN '2000-01-01' AND '2100-01-01'

    Args:
        df: Input DataFrame
        date_column: Name of the date column to filter
        min_date: Minimum valid date (default: 2000-01-01)
        max_date: Maximum valid date (default: 2100-01-01)
        allow_null: Whether to keep NULL dates (default: True)

    Returns:
        Filtered DataFrame
    """
    if allow_null:
        return df.filter(
            F.col(date_column).between(min_date, max_date) |
            F.col(date_column).isNull()
        )
    else:
        return df.filter(F.col(date_column).between(min_date, max_date))


def clean_date_column(df, source_column, target_column=None):
    """
    Clean a date column by replacing invalid dates with NULL.

    Matches ADF pattern:
        CASE WHEN "JoinDate" BETWEEN '2000-01-01' AND '2100-01-01'
             THEN "JoinDate" ELSE NULL END AS "JoinDate"

    Args:
        df: Input DataFrame
        source_column: Name of the source date column
        target_column: Name of the target column (default: same as source)

    Returns:
        DataFrame with cleaned date column
    """
    if target_column is None:
        target_column = source_column

    return df.withColumn(
        target_column,
        F.when(
            F.col(source_column).between(DATE_MIN, DATE_MAX),
            F.col(source_column)
        ).otherwise(F.lit(None))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## LinkedUserId Resolution

# COMMAND ----------

def get_worker_by_linked_user_id(spark, catalog="wakecap_prod", schema="silver"):
    """
    Get worker lookup DataFrame for resolving LinkedUserId (user GUID) to WorkerId.

    Used for ApprovedBy resolution in ResourceTimesheet.

    Matches ADF pattern:
        LEFT JOIN (
            SELECT "LinkedUserId", "Id" AS "ApprovedById",
                   ROW_NUMBER() OVER (PARTITION BY "LinkedUserId" ORDER BY NULL) rn
            FROM "People" WHERE "LinkedUserId" IS NOT NULL
        ) p ON rt."ApprovedBy" = p."LinkedUserId" AND p.rn = 1

    Args:
        spark: SparkSession
        catalog: Catalog name (default: wakecap_prod)
        schema: Schema name (default: silver)

    Returns:
        DataFrame with columns: LinkedUserId, WorkerId (deduplicated by LinkedUserId)
    """
    worker_df = spark.table(f"{catalog}.{schema}.silver_worker")

    # Window for deduplication by LinkedUserId
    window = Window.partitionBy("LinkedUserId").orderBy(F.lit(1))

    return worker_df \
        .filter(F.col("LinkedUserId").isNotNull()) \
        .withColumn("_rn", F.row_number().over(window)) \
        .filter(F.col("_rn") == 1) \
        .select(
            F.col("LinkedUserId"),
            F.col("WorkerId")
        )


def resolve_approved_by(df, approved_by_column, worker_lookup_df):
    """
    Resolve ApprovedBy user GUID to WorkerId using LinkedUserId lookup.

    Args:
        df: Input DataFrame with ApprovedBy column
        approved_by_column: Name of the ApprovedBy column (contains user GUID)
        worker_lookup_df: Result of get_worker_by_linked_user_id()

    Returns:
        DataFrame with ApprovedByWorkerId column added
    """
    lookup_aliased = worker_lookup_df.select(
        F.col("LinkedUserId").cast("string").alias("_lookup_linked_user_id"),
        F.col("WorkerId").alias("ApprovedByWorkerId")
    )

    return df.join(
        lookup_aliased,
        F.col(approved_by_column).cast("string") == F.col("_lookup_linked_user_id"),
        "left"
    ).drop("_lookup_linked_user_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples
# MAGIC
# MAGIC ```python
# MAGIC # Import helpers in Gold notebooks
# MAGIC # %run ../udfs/adf_helpers
# MAGIC
# MAGIC # Example 1: Get MV_ResourceDevice_NoViolation equivalent
# MAGIC resource_device_df = get_resource_device_no_violation(spark)
# MAGIC
# MAGIC # Example 2: Filter dates in Silver table
# MAGIC crew_composition = spark.table("wakecap_prod.silver.silver_crew_composition")
# MAGIC crew_composition = filter_valid_date(crew_composition, "EffectiveDate")
# MAGIC
# MAGIC # Example 3: Resolve ApprovedBy to WorkerId
# MAGIC worker_lookup = get_worker_by_linked_user_id(spark)
# MAGIC timesheet_df = resolve_approved_by(timesheet_df, "ApprovedById", worker_lookup)
# MAGIC ```
