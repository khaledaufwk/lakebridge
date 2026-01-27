# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Contact Tracing Rule View
# MAGIC
# MAGIC **Converted from:** `dbo.vwContactTracingRule`
# MAGIC
# MAGIC **Purpose:** Provide contact tracing rules with project configuration.
# MAGIC Defines distance thresholds, time windows, and rule names for contact detection.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Join contact tracing rules with project to get project context
# MAGIC - Provide default rules when silver source not available
# MAGIC - Support project-specific rule overrides
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_contact_tracing_rule` (if available)
# MAGIC - `wakecap_prod.silver.silver_project`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_contact_tracing_rule`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_CONTACT_RULE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_contact_tracing_rule"
SOURCE_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_contact_tracing_rule"

# Default contact tracing parameters
DEFAULT_DISTANCE_THRESHOLD_METERS = 2.0  # 2 meters for close contact
DEFAULT_TIME_WINDOW_SECONDS = 180  # 3 minutes minimum exposure
DEFAULT_MIN_CONTACT_DURATION_SECONDS = 60  # 1 minute minimum

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

has_contact_rule = False
try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_CONTACT_RULE}").collect()[0][0]
    print(f"[OK] Contact Tracing Rule: {cnt:,} rows")
    has_contact_rule = True
except Exception as e:
    print(f"[WARN] Contact Tracing Rule not available: {str(e)[:50]}")
    print("[INFO] Will create default rules for all projects")

has_project = False
try:
    proj_cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_PROJECT}").collect()[0][0]
    print(f"[OK] Project: {proj_cnt:,} rows")
    has_project = True
except Exception as e:
    print(f"[WARN] Project not available: {str(e)[:50]}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

print("=" * 60)
print("BUILD CONTACT TRACING RULE VIEW")
print("=" * 60)

if has_contact_rule:
    # Load from silver source table
    rule_df = spark.table(SOURCE_CONTACT_RULE)
    print(f"Loaded {rule_df.count():,} rules from silver")

    # Join with project for project context
    if has_project:
        project_df = spark.table(SOURCE_PROJECT).select(
            F.col("ProjectId").alias("p_ProjectId"),
            F.col("ProjectName"),
            F.col("ProjectCode")
        )
        rule_df = rule_df.join(
            project_df,
            F.col("ProjectId") == F.col("p_ProjectId"),
            "left"
        ).drop("p_ProjectId")
    else:
        rule_df = rule_df \
            .withColumn("ProjectName", F.lit(None).cast("string")) \
            .withColumn("ProjectCode", F.lit(None).cast("string"))

    # Add computed columns
    rule_df = rule_df \
        .withColumn("IsActive", F.lit(True)) \
        .withColumn("_view_generated_at", F.current_timestamp())

else:
    # Create default rules for each project
    print("Creating default contact tracing rules for all projects...")

    if has_project:
        project_df = spark.table(SOURCE_PROJECT).select(
            F.col("ProjectId"),
            F.col("ProjectName"),
            F.col("ProjectCode")
        )

        # Create default rules - one per project
        rule_df = project_df.select(
            F.monotonically_increasing_id().alias("ContactTracingRuleId"),
            F.col("ProjectId"),
            F.col("ProjectName"),
            F.col("ProjectCode"),
            F.lit("Default Contact Rule").alias("RuleName"),
            F.lit("Standard contact tracing with 2m distance and 3min window").alias("RuleDescription"),
            F.lit(DEFAULT_DISTANCE_THRESHOLD_METERS).cast("double").alias("DistanceThresholdMeters"),
            F.lit(DEFAULT_TIME_WINDOW_SECONDS).cast("int").alias("TimeWindowSeconds"),
            F.lit(DEFAULT_MIN_CONTACT_DURATION_SECONDS).cast("int").alias("MinContactDurationSeconds"),
            F.lit(True).alias("TrackSameFloor"),
            F.lit(True).alias("TrackSameZone"),
            F.lit(True).alias("IsActive"),
            F.current_timestamp().alias("CreatedAt"),
            F.current_timestamp().alias("UpdatedAt"),
            F.current_timestamp().alias("_view_generated_at")
        )
    else:
        # Create a single default rule when no projects available
        default_schema = StructType([
            StructField("ContactTracingRuleId", LongType(), False),
            StructField("ProjectId", StringType(), True),
            StructField("ProjectName", StringType(), True),
            StructField("ProjectCode", StringType(), True),
            StructField("RuleName", StringType(), True),
            StructField("RuleDescription", StringType(), True),
            StructField("DistanceThresholdMeters", DoubleType(), True),
            StructField("TimeWindowSeconds", IntegerType(), True),
            StructField("MinContactDurationSeconds", IntegerType(), True),
            StructField("TrackSameFloor", BooleanType(), True),
            StructField("TrackSameZone", BooleanType(), True),
            StructField("IsActive", BooleanType(), True),
            StructField("CreatedAt", TimestampType(), True),
            StructField("UpdatedAt", TimestampType(), True),
            StructField("_view_generated_at", TimestampType(), True),
        ])

        default_data = [(
            1,
            None,
            None,
            None,
            "Default Contact Rule",
            "Standard contact tracing with 2m distance and 3min window",
            DEFAULT_DISTANCE_THRESHOLD_METERS,
            DEFAULT_TIME_WINDOW_SECONDS,
            DEFAULT_MIN_CONTACT_DURATION_SECONDS,
            True,
            True,
            True,
            datetime.now(),
            datetime.now(),
            datetime.now()
        )]

        rule_df = spark.createDataFrame(default_data, default_schema)

print(f"Final view: {rule_df.count():,} rows")

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

rule_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Rows: {after_count:,}")

# COMMAND ----------

# Display sample for validation
print("\nSample data:")
display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 5"))

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
