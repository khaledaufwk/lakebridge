# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Silver Layer
# MAGIC
# MAGIC Cleaned and validated data with data quality rules.
# MAGIC Reads from Bronze layer tables.
# MAGIC
# MAGIC **Source:** Bronze tables
# MAGIC **Target:** wakecap_prod.migration (Silver tables)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Dimension Tables
# MAGIC Using batch reads from Bronze (JDBC sources don't support streaming).

# COMMAND ----------

@dlt.table(
    name="silver_worker",
    comment="Cleaned worker data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_project", "ProjectID IS NOT NULL")
def silver_worker():
    """Silver table: Cleaned worker data excluding soft-deleted records."""
    return (
        dlt.read("bronze_dbo_Worker")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_project",
    comment="Cleaned project data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "ProjectID IS NOT NULL")
def silver_project():
    """Silver table: Cleaned project data excluding soft-deleted records."""
    return (
        dlt.read("bronze_dbo_Project")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_crew",
    comment="Cleaned crew data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "CrewID IS NOT NULL")
def silver_crew():
    """Silver table: Cleaned crew data excluding soft-deleted records."""
    return (
        dlt.read("bronze_dbo_Crew")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_device",
    comment="Cleaned device data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "DeviceID IS NOT NULL")
def silver_device():
    """Silver table: Cleaned device data excluding soft-deleted records."""
    return (
        dlt.read("bronze_dbo_Device")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_organization",
    comment="Cleaned organization data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "OrganizationID IS NOT NULL")
def silver_organization():
    """Silver table: Cleaned organization data."""
    return (
        dlt.read("bronze_dbo_Organization")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_company",
    comment="Cleaned company data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "CompanyID IS NOT NULL")
def silver_company():
    """Silver table: Cleaned company data."""
    return (
        dlt.read("bronze_dbo_Company")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_trade",
    comment="Cleaned trade data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "TradeID IS NOT NULL")
def silver_trade():
    """Silver table: Cleaned trade data."""
    return (
        dlt.read("bronze_dbo_Trade")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_floor",
    comment="Cleaned floor data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "FloorID IS NOT NULL")
def silver_floor():
    """Silver table: Cleaned floor data."""
    return (
        dlt.read("bronze_dbo_Floor")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_zone",
    comment="Cleaned zone data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "ZoneID IS NOT NULL")
def silver_zone():
    """Silver table: Cleaned zone data."""
    return (
        dlt.read("bronze_dbo_Zone")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_workshift",
    comment="Cleaned workshift data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "WorkshiftID IS NOT NULL")
def silver_workshift():
    """Silver table: Cleaned workshift data."""
    return (
        dlt.read("bronze_dbo_Workshift")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Tables

# COMMAND ----------

@dlt.table(
    name="silver_workers_shifts",
    comment="Cleaned workers shifts fact with data quality checks"
)
@dlt.expect_or_drop("valid_worker", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_project", "ProjectID IS NOT NULL")
@dlt.expect("valid_dates", "ShiftLocalDate IS NOT NULL")
def silver_workers_shifts():
    """Silver table: Cleaned workers shifts data."""
    return (
        dlt.read("bronze_dbo_FactWorkersShifts")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_observations",
    comment="Cleaned observations fact with data quality checks"
)
@dlt.expect_or_drop("valid_id", "ObservationID IS NOT NULL")
def silver_observations():
    """Silver table: Cleaned observations data."""
    return (
        dlt.read("bronze_dbo_FactObservations")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_reported_attendance",
    comment="Cleaned reported attendance fact with data quality checks"
)
@dlt.expect_or_drop("valid_project", "ProjectID IS NOT NULL")
def silver_reported_attendance():
    """Silver table: Cleaned reported attendance data."""
    return (
        dlt.read("bronze_dbo_FactReportedAttendance")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_progress",
    comment="Cleaned progress fact with data quality checks"
)
def silver_progress():
    """Silver table: Cleaned progress data."""
    return (
        dlt.read("bronze_dbo_FactProgress")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )
