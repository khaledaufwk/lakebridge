# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Bronze Layer - All Tables
# MAGIC
# MAGIC This notebook defines all Bronze layer tables for the WakeCapDW migration.
# MAGIC Data is ingested from ADLS Gen2 (populated by Azure Data Factory).
# MAGIC
# MAGIC **Source:** ADLS Gen2 Parquet files
# MAGIC **Target:** wakecap_prod.migration schema
# MAGIC **Tables:** 142 total (24 dimensions, 9 facts, 8 assignments, staging/security)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ADLS Configuration - Update with actual storage account
ADLS_STORAGE_ACCOUNT = spark.conf.get("pipeline.adls_storage_account", "wakecapadls")
ADLS_CONTAINER = "raw"
ADLS_BASE_PATH = f"abfss://{ADLS_CONTAINER}@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/wakecap"

def get_adls_path(table_category, table_name):
    """Get ADLS path for a table."""
    return f"{ADLS_BASE_PATH}/{table_category}/{table_name}/"

def read_adls_table(table_category, table_name):
    """Read a table from ADLS Parquet files (batch mode)."""
    path = get_adls_path(table_category, table_name)
    return spark.read.format("parquet").load(path)

def read_adls_table_streaming(table_category, table_name):
    """Read a table from ADLS using Auto Loader (streaming mode)."""
    path = get_adls_path(table_category, table_name)
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"/checkpoints/{table_name}/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables (24 Tables)

# COMMAND ----------

# Core Dimension Tables

@dlt.table(
    name="bronze_dbo_Worker",
    comment="Raw worker data from ADLS - all employees/workers",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Worker"}
)
def bronze_dbo_Worker():
    return read_adls_table("dimensions", "Worker")

@dlt.table(
    name="bronze_dbo_Project",
    comment="Raw project data from ADLS - construction projects",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Project"}
)
def bronze_dbo_Project():
    return read_adls_table("dimensions", "Project")

@dlt.table(
    name="bronze_dbo_Crew",
    comment="Raw crew data from ADLS - work crews/teams",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Crew"}
)
def bronze_dbo_Crew():
    return read_adls_table("dimensions", "Crew")

@dlt.table(
    name="bronze_dbo_Device",
    comment="Raw device data from ADLS - tracking devices",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Device"}
)
def bronze_dbo_Device():
    return read_adls_table("dimensions", "Device")

@dlt.table(
    name="bronze_dbo_Organization",
    comment="Raw organization data from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Organization"}
)
def bronze_dbo_Organization():
    return read_adls_table("dimensions", "Organization")

@dlt.table(
    name="bronze_dbo_Company",
    comment="Raw company data from ADLS - subcontractors",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Company"}
)
def bronze_dbo_Company():
    return read_adls_table("dimensions", "Company")

@dlt.table(
    name="bronze_dbo_Trade",
    comment="Raw trade data from ADLS - worker trades/skills",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Trade"}
)
def bronze_dbo_Trade():
    return read_adls_table("dimensions", "Trade")

@dlt.table(
    name="bronze_dbo_Floor",
    comment="Raw floor data from ADLS - building floors",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Floor"}
)
def bronze_dbo_Floor():
    return read_adls_table("dimensions", "Floor")

@dlt.table(
    name="bronze_dbo_Zone",
    comment="Raw zone data from ADLS - work zones",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Zone"}
)
def bronze_dbo_Zone():
    return read_adls_table("dimensions", "Zone")

@dlt.table(
    name="bronze_dbo_Workshift",
    comment="Raw workshift data from ADLS - shift schedules",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Workshift"}
)
def bronze_dbo_Workshift():
    return read_adls_table("dimensions", "Workshift")

# COMMAND ----------

# Additional Dimension Tables

@dlt.table(
    name="bronze_dbo_Activity",
    comment="Raw activity data from ADLS - activity types",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Activity"}
)
def bronze_dbo_Activity():
    return read_adls_table("dimensions", "Activity")

@dlt.table(
    name="bronze_dbo_Department",
    comment="Raw department data from ADLS - worker departments",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Department"}
)
def bronze_dbo_Department():
    return read_adls_table("dimensions", "Department")

@dlt.table(
    name="bronze_dbo_Task",
    comment="Raw task data from ADLS - task definitions",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Task"}
)
def bronze_dbo_Task():
    return read_adls_table("dimensions", "Task")

@dlt.table(
    name="bronze_dbo_Title",
    comment="Raw title data from ADLS - worker titles",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.Title"}
)
def bronze_dbo_Title():
    return read_adls_table("dimensions", "Title")

@dlt.table(
    name="bronze_dbo_WorkerStatus",
    comment="Raw worker status data from ADLS - status codes",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.WorkerStatus"}
)
def bronze_dbo_WorkerStatus():
    return read_adls_table("dimensions", "WorkerStatus")

@dlt.table(
    name="bronze_dbo_WorkshiftDetails",
    comment="Raw workshift details from ADLS - shift time windows",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.WorkshiftDetails"}
)
def bronze_dbo_WorkshiftDetails():
    return read_adls_table("dimensions", "WorkshiftDetails")

@dlt.table(
    name="bronze_dbo_LocationGroup",
    comment="Raw location group data from ADLS - location groupings",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.LocationGroup"}
)
def bronze_dbo_LocationGroup():
    return read_adls_table("dimensions", "LocationGroup")

@dlt.table(
    name="bronze_dbo_ObservationSource",
    comment="Raw observation source data from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ObservationSource"}
)
def bronze_dbo_ObservationSource():
    return read_adls_table("dimensions", "ObservationSource")

@dlt.table(
    name="bronze_dbo_ObservationType",
    comment="Raw observation type data from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ObservationType"}
)
def bronze_dbo_ObservationType():
    return read_adls_table("dimensions", "ObservationType")

@dlt.table(
    name="bronze_dbo_ObservationStatus",
    comment="Raw observation status data from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ObservationStatus"}
)
def bronze_dbo_ObservationStatus():
    return read_adls_table("dimensions", "ObservationStatus")

@dlt.table(
    name="bronze_dbo_LocationAssignmentClass",
    comment="Raw location assignment class data from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.LocationAssignmentClass"}
)
def bronze_dbo_LocationAssignmentClass():
    return read_adls_table("dimensions", "LocationAssignmentClass")

@dlt.table(
    name="bronze_dbo_ContactTracingRule",
    comment="Raw contact tracing rule data from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ContactTracingRule"}
)
def bronze_dbo_ContactTracingRule():
    return read_adls_table("dimensions", "ContactTracingRule")

@dlt.table(
    name="bronze_dbo_ProductiveClass",
    comment="Raw productive class data from ADLS - productivity codes",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ProductiveClass"}
)
def bronze_dbo_ProductiveClass():
    return read_adls_table("dimensions", "ProductiveClass")

@dlt.table(
    name="bronze_dbo_ShiftTimeCategory",
    comment="Raw shift time category data from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ShiftTimeCategory"}
)
def bronze_dbo_ShiftTimeCategory():
    return read_adls_table("dimensions", "ShiftTimeCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables (9 Tables)

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactObservations",
    comment="Raw observations fact from ADLS - safety and activity observations",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactObservations"}
)
def bronze_dbo_FactObservations():
    return read_adls_table("facts", "FactObservations")

@dlt.table(
    name="bronze_dbo_FactWorkersHistory",
    comment="Raw workers history fact from ADLS - worker location history",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactWorkersHistory"}
)
def bronze_dbo_FactWorkersHistory():
    return read_adls_table("facts", "FactWorkersHistory")

@dlt.table(
    name="bronze_dbo_FactWorkersShifts",
    comment="Raw workers shifts fact from ADLS - daily shift records",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactWorkersShifts"}
)
def bronze_dbo_FactWorkersShifts():
    return read_adls_table("facts", "FactWorkersShifts")

@dlt.table(
    name="bronze_dbo_FactWorkersShiftsCombined",
    comment="Raw combined workers shifts fact from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactWorkersShiftsCombined"}
)
def bronze_dbo_FactWorkersShiftsCombined():
    return read_adls_table("facts", "FactWorkersShiftsCombined")

@dlt.table(
    name="bronze_dbo_FactReportedAttendance",
    comment="Raw reported attendance fact from ADLS - manual attendance records",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactReportedAttendance"}
)
def bronze_dbo_FactReportedAttendance():
    return read_adls_table("facts", "FactReportedAttendance")

@dlt.table(
    name="bronze_dbo_FactProgress",
    comment="Raw progress fact from ADLS - task/activity progress",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactProgress"}
)
def bronze_dbo_FactProgress():
    return read_adls_table("facts", "FactProgress")

@dlt.table(
    name="bronze_dbo_FactWorkersContacts",
    comment="Raw workers contacts fact from ADLS - contact tracing data",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactWorkersContacts"}
)
def bronze_dbo_FactWorkersContacts():
    return read_adls_table("facts", "FactWorkersContacts")

@dlt.table(
    name="bronze_dbo_FactWorkersTasks",
    comment="Raw workers tasks fact from ADLS - task assignments",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactWorkersTasks"}
)
def bronze_dbo_FactWorkersTasks():
    return read_adls_table("facts", "FactWorkersTasks")

@dlt.table(
    name="bronze_dbo_FactWeatherObservations",
    comment="Raw weather observations fact from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.FactWeatherObservations"}
)
def bronze_dbo_FactWeatherObservations():
    return read_adls_table("facts", "FactWeatherObservations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assignment Tables (8 Tables)

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_CrewAssignments",
    comment="Raw crew assignments from ADLS - worker to crew mapping",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.CrewAssignments"}
)
def bronze_dbo_CrewAssignments():
    return read_adls_table("assignments", "CrewAssignments")

@dlt.table(
    name="bronze_dbo_DeviceAssignments",
    comment="Raw device assignments from ADLS - worker to device mapping",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.DeviceAssignments"}
)
def bronze_dbo_DeviceAssignments():
    return read_adls_table("assignments", "DeviceAssignments")

@dlt.table(
    name="bronze_dbo_WorkshiftAssignments",
    comment="Raw workshift assignments from ADLS - worker to shift mapping",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.WorkshiftAssignments"}
)
def bronze_dbo_WorkshiftAssignments():
    return read_adls_table("assignments", "WorkshiftAssignments")

@dlt.table(
    name="bronze_dbo_TradeAssignments",
    comment="Raw trade assignments from ADLS - worker to trade mapping",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.TradeAssignments"}
)
def bronze_dbo_TradeAssignments():
    return read_adls_table("assignments", "TradeAssignments")

@dlt.table(
    name="bronze_dbo_ManagerAssignments",
    comment="Raw manager assignments from ADLS - worker to manager hierarchy",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ManagerAssignments"}
)
def bronze_dbo_ManagerAssignments():
    return read_adls_table("assignments", "ManagerAssignments")

@dlt.table(
    name="bronze_dbo_ManagerAssignmentSnapshots",
    comment="Raw manager assignment snapshots from ADLS - point-in-time snapshots",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.ManagerAssignmentSnapshots"}
)
def bronze_dbo_ManagerAssignmentSnapshots():
    return read_adls_table("assignments", "ManagerAssignmentSnapshots")

@dlt.table(
    name="bronze_dbo_LocationGroupAssignments",
    comment="Raw location group assignments from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.LocationGroupAssignments"}
)
def bronze_dbo_LocationGroupAssignments():
    return read_adls_table("assignments", "LocationGroupAssignments")

@dlt.table(
    name="bronze_dbo_WorkerLocationAssignments",
    comment="Raw worker location assignments from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "dbo.WorkerLocationAssignments"}
)
def bronze_dbo_WorkerLocationAssignments():
    return read_adls_table("assignments", "WorkerLocationAssignments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security Tables

# COMMAND ----------

@dlt.table(
    name="bronze_security_UserPermissions",
    comment="Raw user permissions from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "security.UserPermissions"}
)
def bronze_security_UserPermissions():
    return read_adls_table("security", "UserPermissions")

@dlt.table(
    name="bronze_security_UserPermissionsProject",
    comment="Raw project-level permissions from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "security.UserPermissionsProject"}
)
def bronze_security_UserPermissionsProject():
    return read_adls_table("security", "UserPermissionsProject")

@dlt.table(
    name="bronze_security_UserPermissionsOrganization",
    comment="Raw organization-level permissions from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "security.UserPermissionsOrganization"}
)
def bronze_security_UserPermissionsOrganization():
    return read_adls_table("security", "UserPermissionsOrganization")

@dlt.table(
    name="bronze_security_UserPermissionsCompany",
    comment="Raw company-level permissions from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "security.UserPermissionsCompany"}
)
def bronze_security_UserPermissionsCompany():
    return read_adls_table("security", "UserPermissionsCompany")

@dlt.table(
    name="bronze_security_UserPermissionsCrew",
    comment="Raw crew-level permissions from ADLS",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "security.UserPermissionsCrew"}
)
def bronze_security_UserPermissionsCrew():
    return read_adls_table("security", "UserPermissionsCrew")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Staging Tables

# COMMAND ----------

@dlt.table(
    name="bronze_stg_SyncState",
    comment="Raw sync state from ADLS - ETL sync tracking",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "stg.SyncState"}
)
def bronze_stg_SyncState():
    return read_adls_table("staging", "SyncState")

@dlt.table(
    name="bronze_stg_ImpactedAreaLog",
    comment="Raw impacted area log from ADLS - ETL logging",
    table_properties={"quality": "bronze", "source": "adls", "source_table": "stg.ImpactedAreaLog"}
)
def bronze_stg_ImpactedAreaLog():
    return read_adls_table("staging", "ImpactedAreaLog")
