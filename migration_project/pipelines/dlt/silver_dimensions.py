# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Silver Layer Dimensions
# MAGIC
# MAGIC This notebook defines DLT tables for the Silver layer dimensions with data quality expectations.
# MAGIC Silver layer cleanses and validates data from Bronze layer.
# MAGIC
# MAGIC **Data Quality Expectations:**
# MAGIC - NOT NULL constraints on primary keys
# MAGIC - Valid foreign key references
# MAGIC - Business rule validations
# MAGIC - Data type validations

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce, trim, upper, lower,
    regexp_replace, to_date, to_timestamp
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Default catalog and schema
CATALOG = "wakecap_prod"
SCHEMA = "migration"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Organization

# COMMAND ----------

@dlt.table(
    name="silver_organization",
    comment="Cleansed organization dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "OrganizationID"
    }
)
@dlt.expect_or_drop("valid_organization_id", "OrganizationID IS NOT NULL")
@dlt.expect_or_drop("valid_organization_name", "Organization IS NOT NULL AND LENGTH(TRIM(Organization)) > 0")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_organization():
    """
    Cleansed organization records.
    """
    return (
        dlt.read("bronze_dbo_Organization")
        .filter(col("DeletedAt").isNull())
        .withColumn("Organization", trim(col("Organization")))
        .withColumn("OrganizationName", trim(col("OrganizationName")))
        .select(
            "OrganizationID",
            "Organization",
            "OrganizationName",
            "TimeZoneName",
            "ExtOrganizationID",
            "CreatedAt",
            "WatermarkUTC",
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Project

# COMMAND ----------

@dlt.table(
    name="silver_project",
    comment="Cleansed project dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID"
    }
)
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect_or_drop("valid_project_name", "Project IS NOT NULL AND LENGTH(TRIM(Project)) > 0")
@dlt.expect("valid_organization_ref", "OrganizationID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_project():
    """
    Cleansed project records with organization reference validation.
    """
    projects = dlt.read("bronze_dbo_Project")
    organizations = dlt.read("silver_organization")

    return (
        projects.alias("p")
        .join(organizations.alias("o"), col("p.OrganizationID") == col("o.OrganizationID"), "left")
        .filter(col("p.DeletedAt").isNull())
        .withColumn("Project", trim(col("p.Project")))
        .withColumn("ProjectName", trim(col("p.ProjectName")))
        .select(
            col("p.ProjectID"),
            col("p.Project"),
            col("p.ProjectName"),
            col("p.OrganizationID"),
            col("o.OrganizationName"),
            col("p.TimeZoneName"),
            col("p.IsActive"),
            col("p.ZoneBufferDistance"),
            col("p.ExtProjectID"),
            col("p.CreatedAt"),
            col("p.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Worker

# COMMAND ----------

@dlt.table(
    name="silver_worker",
    comment="Cleansed worker dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "WorkerID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("valid_worker_code", "Worker IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
@dlt.expect_or_warn("has_worker_name", "WorkerName IS NOT NULL AND LENGTH(TRIM(WorkerName)) > 0")
def silver_worker():
    """
    Cleansed worker records with project reference validation.
    """
    workers = dlt.read("bronze_dbo_Worker")
    projects = dlt.read("silver_project")

    return (
        workers.alias("w")
        .join(projects.alias("p"), col("w.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("w.DeletedAt").isNull())
        .withColumn("Worker", trim(col("w.Worker")))
        .withColumn("WorkerName", trim(col("w.WorkerName")))
        .select(
            col("w.WorkerID"),
            col("w.Worker"),
            col("w.WorkerName"),
            col("w.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("w.Color"),
            col("w.IsActive"),
            col("w.ExtWorkerID"),
            col("w.CreatedAt"),
            col("w.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Crew

# COMMAND ----------

@dlt.table(
    name="silver_crew",
    comment="Cleansed crew dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "CrewID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_crew_id", "CrewID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_crew():
    """
    Cleansed crew records.
    """
    crews = dlt.read("bronze_dbo_Crew")
    projects = dlt.read("silver_project")

    return (
        crews.alias("c")
        .join(projects.alias("p"), col("c.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("c.DeletedAt").isNull())
        .withColumn("Crew", trim(col("c.Crew")))
        .withColumn("CrewName", trim(col("c.CrewName")))
        .select(
            col("c.CrewID"),
            col("c.Crew"),
            col("c.CrewName"),
            col("c.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("c.IsActive"),
            col("c.CreatedAt"),
            col("c.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Trade

# COMMAND ----------

@dlt.table(
    name="silver_trade",
    comment="Cleansed trade/role dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "TradeID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_trade_id", "TradeID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_trade():
    """
    Cleansed trade (worker role) records.
    """
    trades = dlt.read("bronze_dbo_Trade")
    projects = dlt.read("silver_project")

    return (
        trades.alias("t")
        .join(projects.alias("p"), col("t.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("t.DeletedAt").isNull())
        .withColumn("Trade", trim(col("t.Trade")))
        .withColumn("TradeGroup", trim(col("t.TradeGroup")))
        .select(
            col("t.TradeID"),
            col("t.Trade"),
            col("t.TradeGroup"),
            col("t.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("t.ExtTradeID"),
            col("t.CreatedAt"),
            col("t.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Floor

# COMMAND ----------

@dlt.table(
    name="silver_floor",
    comment="Cleansed floor dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "FloorID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_floor_id", "FloorID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_floor():
    """
    Cleansed floor records.
    """
    floors = dlt.read("bronze_dbo_Floor")
    projects = dlt.read("silver_project")

    return (
        floors.alias("f")
        .join(projects.alias("p"), col("f.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("f.DeletedAt").isNull())
        .withColumn("Floor", trim(col("f.Floor")))
        .withColumn("FloorName", trim(col("f.FloorName")))
        .select(
            col("f.FloorID"),
            col("f.Floor"),
            col("f.FloorName"),
            col("f.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("f.FloorNumber"),
            col("f.ExtSpaceID"),
            col("f.CreatedAt"),
            col("f.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Zone

# COMMAND ----------

@dlt.table(
    name="silver_zone",
    comment="Cleansed zone dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ZoneID,FloorID"
    }
)
@dlt.expect_or_drop("valid_zone_id", "ZoneID IS NOT NULL")
@dlt.expect("valid_floor_ref", "FloorID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
@dlt.expect_or_warn("valid_productive_class", "ProductiveClassID IN (1, 2, 3) OR ProductiveClassID IS NULL")
def silver_zone():
    """
    Cleansed zone records with floor reference validation.
    """
    zones = dlt.read("bronze_dbo_Zone")
    floors = dlt.read("silver_floor")

    return (
        zones.alias("z")
        .join(floors.alias("f"), col("z.FloorID") == col("f.FloorID"), "left")
        .filter(col("z.DeletedAt").isNull())
        .withColumn("Zone", trim(col("z.Zone")))
        .withColumn("ZoneName", trim(col("z.ZoneName")))
        .select(
            col("z.ZoneID"),
            col("z.Zone"),
            col("z.ZoneName"),
            col("z.FloorID"),
            col("f.Floor").alias("FloorName"),
            col("f.ProjectID"),
            col("z.ProductiveClassID"),
            when(col("z.ProductiveClassID") == 1, "Direct Productive")
            .when(col("z.ProductiveClassID") == 2, "Indirect Productive")
            .when(col("z.ProductiveClassID") == 3, "Non-Productive")
            .otherwise("Unknown").alias("ProductiveClassName"),
            col("z.ExtSpaceID"),
            col("z.CreatedAt"),
            col("z.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Workshift

# COMMAND ----------

@dlt.table(
    name="silver_workshift",
    comment="Cleansed workshift dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "WorkshiftID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_workshift_id", "WorkshiftID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_workshift():
    """
    Cleansed workshift records.
    """
    workshifts = dlt.read("bronze_dbo_Workshift")
    projects = dlt.read("silver_project")

    return (
        workshifts.alias("ws")
        .join(projects.alias("p"), col("ws.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("ws.DeletedAt").isNull())
        .withColumn("Workshift", trim(col("ws.Workshift")))
        .select(
            col("ws.WorkshiftID"),
            col("ws.Workshift"),
            col("ws.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("ws.IsDefault"),
            col("ws.CreatedAt"),
            col("ws.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Device

# COMMAND ----------

@dlt.table(
    name="silver_device",
    comment="Cleansed device dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "DeviceID"
    }
)
@dlt.expect_or_drop("valid_device_id", "DeviceID IS NOT NULL")
@dlt.expect("valid_device_code", "Device IS NOT NULL AND LENGTH(TRIM(Device)) > 0")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_device():
    """
    Cleansed device records.
    """
    devices = dlt.read("bronze_dbo_Device")

    return (
        devices
        .filter(col("DeletedAt").isNull())
        .withColumn("Device", trim(col("Device")))
        .select(
            "DeviceID",
            "Device",
            "DeviceType",
            "OrganizationID",
            "FirmwareVersion",
            "IsActive",
            "CreatedAt",
            "WatermarkUTC",
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Company

# COMMAND ----------

@dlt.table(
    name="silver_company",
    comment="Cleansed company/contractor dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "CompanyID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_company_id", "CompanyID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_company():
    """
    Cleansed company (contractor) records.
    """
    companies = dlt.read("bronze_dbo_Company")
    projects = dlt.read("silver_project")

    return (
        companies.alias("c")
        .join(projects.alias("p"), col("c.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("c.DeletedAt").isNull())
        .withColumn("Company", trim(col("c.Company")))
        .withColumn("CompanyDescription", trim(col("c.CompanyDescription")))
        .select(
            col("c.CompanyID"),
            col("c.Company"),
            col("c.CompanyDescription"),
            col("c.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("c.ExtCompanyID"),
            col("c.CreatedAt"),
            col("c.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Location Group

# COMMAND ----------

@dlt.table(
    name="silver_location_group",
    comment="Cleansed location group dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "LocationGroupID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_location_group_id", "LocationGroupID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_location_group():
    """
    Cleansed location group records.
    """
    location_groups = dlt.read("bronze_dbo_LocationGroup")
    projects = dlt.read("silver_project")

    return (
        location_groups.alias("lg")
        .join(projects.alias("p"), col("lg.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("lg.DeletedAt").isNull())
        .withColumn("LocationGroup", trim(col("lg.LocationGroup")))
        .select(
            col("lg.LocationGroupID"),
            col("lg.LocationGroup"),
            col("lg.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("lg.CreatedAt"),
            col("lg.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Activity

# COMMAND ----------

@dlt.table(
    name="silver_activity",
    comment="Cleansed activity dimension with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ActivityID,ProjectID"
    }
)
@dlt.expect_or_drop("valid_activity_id", "ActivityID IS NOT NULL")
@dlt.expect("valid_project_ref", "ProjectID IS NOT NULL")
@dlt.expect("not_deleted", "DeletedAt IS NULL")
def silver_activity():
    """
    Cleansed activity records.
    """
    activities = dlt.read("bronze_dbo_Activity")
    projects = dlt.read("silver_project")

    return (
        activities.alias("a")
        .join(projects.alias("p"), col("a.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("a.DeletedAt").isNull())
        .withColumn("Activity", trim(col("a.Activity")))
        .withColumn("ActivityCode", trim(col("a.ActivityCode")))
        .select(
            col("a.ActivityID"),
            col("a.Activity"),
            col("a.ActivityCode"),
            col("a.ProjectID"),
            col("p.Project").alias("ProjectName"),
            col("a.TradeID"),
            col("a.ExtDataGroupID"),
            col("a.CreatedAt"),
            col("a.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )
