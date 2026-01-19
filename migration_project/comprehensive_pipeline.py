# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Complete Migration Pipeline
# MAGIC
# MAGIC This Delta Live Tables pipeline migrates ALL data from SQL Server (WakeCapDW) to Databricks.
# MAGIC
# MAGIC **Source:** Azure SQL Server - WakeCapDW_20251215
# MAGIC **Target:** Databricks Unity Catalog
# MAGIC **Generated:** 2026-01-19
# MAGIC
# MAGIC ## Architecture
# MAGIC - **Bronze Layer:** Raw ingestion from SQL Server (36 tables)
# MAGIC - **Silver Layer:** Cleaned and validated data (13 tables)
# MAGIC - **Gold Layer:** Business aggregates and views (16 views)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
CATALOG = "wakecap_prod"
SCHEMA = "migration"
SECRET_SCOPE = "wakecap_migration"

# JDBC connection settings
def get_jdbc_config():
    """Get JDBC configuration from secrets."""
    return {
        "url": dbutils.secrets.get(SECRET_SCOPE, "sqlserver_jdbc_url"),
        "user": dbutils.secrets.get(SECRET_SCOPE, "sqlserver_user"),
        "password": dbutils.secrets.get(SECRET_SCOPE, "sqlserver_password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

def read_sql_server_table(table_name, schema_name="dbo"):
    """Read a table from SQL Server using JDBC."""
    jdbc_config = get_jdbc_config()
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_config["url"])
        .option("dbtable", f"[{schema_name}].[{table_name}]")
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .option("driver", jdbc_config["driver"])
        .option("fetchsize", "10000")
        .load()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Dimension Tables

# COMMAND ----------

# Core Dimension Tables

@dlt.table(
    name="bronze_dbo_Worker",
    comment="Raw worker data from SQL Server dbo.Worker",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Worker"
    }
)
def bronze_dbo_Worker():
    """Bronze table: Worker dimension - all employees/workers in the system."""
    return read_sql_server_table("Worker", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Project",
    comment="Raw project data from SQL Server dbo.Project",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Project"
    }
)
def bronze_dbo_Project():
    """Bronze table: Project dimension - construction projects."""
    return read_sql_server_table("Project", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Crew",
    comment="Raw crew data from SQL Server dbo.Crew",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Crew"
    }
)
def bronze_dbo_Crew():
    """Bronze table: Crew dimension - work crews/teams."""
    return read_sql_server_table("Crew", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Device",
    comment="Raw device data from SQL Server dbo.Device",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Device"
    }
)
def bronze_dbo_Device():
    """Bronze table: Device dimension - tracking devices."""
    return read_sql_server_table("Device", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Organization",
    comment="Raw organization data from SQL Server dbo.Organization",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Organization"
    }
)
def bronze_dbo_Organization():
    """Bronze table: Organization dimension."""
    return read_sql_server_table("Organization", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Company",
    comment="Raw company data from SQL Server dbo.Company",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Company"
    }
)
def bronze_dbo_Company():
    """Bronze table: Company dimension - subcontractors/companies."""
    return read_sql_server_table("Company", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Trade",
    comment="Raw trade data from SQL Server dbo.Trade",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Trade"
    }
)
def bronze_dbo_Trade():
    """Bronze table: Trade dimension - worker trades/skills."""
    return read_sql_server_table("Trade", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Floor",
    comment="Raw floor data from SQL Server dbo.Floor",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Floor"
    }
)
def bronze_dbo_Floor():
    """Bronze table: Floor dimension - building floors."""
    return read_sql_server_table("Floor", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Zone",
    comment="Raw zone data from SQL Server dbo.Zone",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Zone"
    }
)
def bronze_dbo_Zone():
    """Bronze table: Zone dimension - work zones."""
    return read_sql_server_table("Zone", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Workshift",
    comment="Raw workshift data from SQL Server dbo.Workshift",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Workshift"
    }
)
def bronze_dbo_Workshift():
    """Bronze table: Workshift dimension - shift schedules."""
    return read_sql_server_table("Workshift", "dbo")

# COMMAND ----------

# NEW: Additional Dimension Tables

@dlt.table(
    name="bronze_dbo_WorkshiftDetails",
    comment="Raw workshift details from SQL Server dbo.WorkshiftDetails",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.WorkshiftDetails"
    }
)
def bronze_dbo_WorkshiftDetails():
    """Bronze table: Workshift details - detailed shift configuration."""
    return read_sql_server_table("WorkshiftDetails", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Activity",
    comment="Raw activity data from SQL Server dbo.Activity",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Activity"
    }
)
def bronze_dbo_Activity():
    """Bronze table: Activity dimension - work activities."""
    return read_sql_server_table("Activity", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_LocationGroup",
    comment="Raw location group data from SQL Server dbo.LocationGroup",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.LocationGroup"
    }
)
def bronze_dbo_LocationGroup():
    """Bronze table: Location group dimension - groupings of locations."""
    return read_sql_server_table("LocationGroup", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Department",
    comment="Raw department data from SQL Server dbo.Department",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Department"
    }
)
def bronze_dbo_Department():
    """Bronze table: Department dimension - organizational departments."""
    return read_sql_server_table("Department", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Title",
    comment="Raw title data from SQL Server dbo.Title",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.Title"
    }
)
def bronze_dbo_Title():
    """Bronze table: Title dimension - job titles."""
    return read_sql_server_table("Title", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_ObservationSource",
    comment="Raw observation source data from SQL Server dbo.ObservationSource",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.ObservationSource"
    }
)
def bronze_dbo_ObservationSource():
    """Bronze table: Observation source dimension - sources of observations."""
    return read_sql_server_table("ObservationSource", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Fact Tables

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactWorkersHistory",
    comment="Raw workers history fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactWorkersHistory"
    }
)
def bronze_dbo_FactWorkersHistory():
    """Bronze table: Workers history - tracking worker location history."""
    return read_sql_server_table("FactWorkersHistory", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactWorkersShifts",
    comment="Raw workers shifts fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactWorkersShifts"
    }
)
def bronze_dbo_FactWorkersShifts():
    """Bronze table: Workers shifts - daily shift records."""
    return read_sql_server_table("FactWorkersShifts", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactObservations",
    comment="Raw observations fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactObservations"
    }
)
def bronze_dbo_FactObservations():
    """Bronze table: Observations - safety and activity observations."""
    return read_sql_server_table("FactObservations", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactReportedAttendance",
    comment="Raw reported attendance fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactReportedAttendance"
    }
)
def bronze_dbo_FactReportedAttendance():
    """Bronze table: Reported attendance - manual attendance records."""
    return read_sql_server_table("FactReportedAttendance", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactProgress",
    comment="Raw progress fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactProgress"
    }
)
def bronze_dbo_FactProgress():
    """Bronze table: Progress - task/activity progress tracking."""
    return read_sql_server_table("FactProgress", "dbo")

# COMMAND ----------

# NEW: Additional Fact Tables

@dlt.table(
    name="bronze_dbo_FactWorkersShiftsCombined",
    comment="Raw workers shifts combined fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactWorkersShiftsCombined"
    }
)
def bronze_dbo_FactWorkersShiftsCombined():
    """Bronze table: Workers shifts combined - aggregated shift data across workshifts."""
    return read_sql_server_table("FactWorkersShiftsCombined", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactWeatherObservations",
    comment="Raw weather observations fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactWeatherObservations"
    }
)
def bronze_dbo_FactWeatherObservations():
    """Bronze table: Weather observations - weather data for projects."""
    return read_sql_server_table("FactWeatherObservations", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactWorkersContacts",
    comment="Raw workers contacts fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactWorkersContacts"
    }
)
def bronze_dbo_FactWorkersContacts():
    """Bronze table: Workers contacts - contact tracing data."""
    return read_sql_server_table("FactWorkersContacts", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactWorkersTasks",
    comment="Raw workers tasks fact from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.FactWorkersTasks"
    }
)
def bronze_dbo_FactWorkersTasks():
    """Bronze table: Workers tasks - task assignments and progress."""
    return read_sql_server_table("FactWorkersTasks", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Assignment Tables

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_CrewAssignments",
    comment="Raw crew assignments from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.CrewAssignments"
    }
)
def bronze_dbo_CrewAssignments():
    """Bronze table: Crew assignments - worker to crew mapping."""
    return read_sql_server_table("CrewAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_DeviceAssignments",
    comment="Raw device assignments from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.DeviceAssignments"
    }
)
def bronze_dbo_DeviceAssignments():
    """Bronze table: Device assignments - worker to device mapping."""
    return read_sql_server_table("DeviceAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_WorkshiftAssignments",
    comment="Raw workshift assignments from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.WorkshiftAssignments"
    }
)
def bronze_dbo_WorkshiftAssignments():
    """Bronze table: Workshift assignments - worker to shift mapping."""
    return read_sql_server_table("WorkshiftAssignments", "dbo")

# COMMAND ----------

# NEW: Additional Assignment Tables

@dlt.table(
    name="bronze_dbo_TradeAssignments",
    comment="Raw trade assignments from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.TradeAssignments"
    }
)
def bronze_dbo_TradeAssignments():
    """Bronze table: Trade assignments - worker to trade mapping."""
    return read_sql_server_table("TradeAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_ManagerAssignments",
    comment="Raw manager assignments from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.ManagerAssignments"
    }
)
def bronze_dbo_ManagerAssignments():
    """Bronze table: Manager assignments - worker to manager mapping."""
    return read_sql_server_table("ManagerAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_ManagerAssignmentSnapshots",
    comment="Raw manager assignment snapshots from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.ManagerAssignmentSnapshots"
    }
)
def bronze_dbo_ManagerAssignmentSnapshots():
    """Bronze table: Manager assignment snapshots - historical manager assignments."""
    return read_sql_server_table("ManagerAssignmentSnapshots", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_LocationGroupAssignments",
    comment="Raw location group assignments from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.LocationGroupAssignments"
    }
)
def bronze_dbo_LocationGroupAssignments():
    """Bronze table: Location group assignments - worker to location group mapping."""
    return read_sql_server_table("LocationGroupAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_WorkerLocationAssignments",
    comment="Raw worker location assignments from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.WorkerLocationAssignments"
    }
)
def bronze_dbo_WorkerLocationAssignments():
    """Bronze table: Worker location assignments - worker to location mapping."""
    return read_sql_server_table("WorkerLocationAssignments", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Other Tables

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_ContactTracingRule",
    comment="Raw contact tracing rule from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.ContactTracingRule"
    }
)
def bronze_dbo_ContactTracingRule():
    """Bronze table: Contact tracing rules - rules for contact tracing."""
    return read_sql_server_table("ContactTracingRule", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_SyncState",
    comment="Raw sync state from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.SyncState"
    }
)
def bronze_dbo_SyncState():
    """Bronze table: Sync state - data synchronization state tracking."""
    return read_sql_server_table("SyncState", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_ImpactedAreaLog",
    comment="Raw impacted area log from SQL Server",
    table_properties={
        "quality": "bronze",
        "source": "sqlserver",
        "source_table": "dbo.ImpactedAreaLog"
    }
)
def bronze_dbo_ImpactedAreaLog():
    """Bronze table: Impacted area log - area change tracking."""
    return read_sql_server_table("ImpactedAreaLog", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned Data
# MAGIC These tables contain validated and cleaned data with data quality rules.

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

@dlt.table(
    name="silver_activity",
    comment="Cleaned activity data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "ActivityID IS NOT NULL")
def silver_activity():
    """Silver table: Cleaned activity data."""
    return (
        dlt.read("bronze_dbo_Activity")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_department",
    comment="Cleaned department data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "DepartmentID IS NOT NULL")
def silver_department():
    """Silver table: Cleaned department data."""
    return (
        dlt.read("bronze_dbo_Department")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_location_group",
    comment="Cleaned location group data with data quality checks"
)
@dlt.expect_or_drop("valid_id", "LocationGroupID IS NOT NULL")
def silver_location_group():
    """Silver table: Cleaned location group data."""
    return (
        dlt.read("bronze_dbo_LocationGroup")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

# Silver Fact Tables

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
    name="silver_workers_shifts_combined",
    comment="Cleaned workers shifts combined fact with data quality checks"
)
@dlt.expect_or_drop("valid_worker", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_project", "ProjectID IS NOT NULL")
def silver_workers_shifts_combined():
    """Silver table: Cleaned workers shifts combined data."""
    return (
        dlt.read("bronze_dbo_FactWorkersShiftsCombined")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_workers_history",
    comment="Cleaned workers history fact with data quality checks"
)
@dlt.expect_or_drop("valid_worker", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_project", "ProjectID IS NOT NULL")
def silver_workers_history():
    """Silver table: Cleaned workers history data."""
    return (
        dlt.read("bronze_dbo_FactWorkersHistory")
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
    name="silver_workers_contacts",
    comment="Cleaned workers contacts fact with data quality checks"
)
@dlt.expect_or_drop("valid_worker", "WorkerID IS NOT NULL")
def silver_workers_contacts():
    """Silver table: Cleaned workers contacts data."""
    return (
        dlt.read("bronze_dbo_FactWorkersContacts")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("sqlserver"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Views & Aggregates
# MAGIC These views provide business-ready data for reporting and analytics.

# COMMAND ----------

@dlt.table(
    name="gold_worker_summary",
    comment="Worker summary with current assignments"
)
def gold_worker_summary():
    """Gold table: Worker summary with device and crew info."""
    workers = dlt.read("silver_worker")
    crews = dlt.read("silver_crew")
    devices = dlt.read("silver_device")

    # Get active crew assignments (CrewAssignments uses DeleteFlag, not ValidTo)
    crew_assignments = (
        dlt.read("bronze_dbo_CrewAssignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    # Get active device assignments (DeviceAssignments has ValidTo)
    device_assignments = (
        dlt.read("bronze_dbo_DeviceAssignments")
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        workers
        .join(crew_assignments, ["WorkerID", "ProjectID"], "left")
        .join(crews.select("CrewID", "Crew", "CrewName"), "CrewID", "left")
        .join(device_assignments, ["WorkerID", "ProjectID"], "left")
        .join(devices.select("DeviceID", "Device", "DeviceName"), "DeviceID", "left")
        .select(
            workers["WorkerID"],
            workers["Worker"],
            workers["WorkerName"],
            workers["ProjectID"],
            col("Crew"),
            col("CrewName"),
            col("Device"),
            col("DeviceName"),
            workers["_ingested_at"]
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_daily_attendance",
    comment="Daily attendance aggregates by project and shift date"
)
def gold_daily_attendance():
    """Gold table: Daily attendance summary based on actual shift activity."""
    shifts = dlt.read("silver_workers_shifts")

    return (
        shifts
        .groupBy("ProjectID", "ShiftLocalDate")
        .agg(
            countDistinct("WorkerID").alias("total_workers"),
            sum("ActiveTimeDuringShift").alias("total_active_time"),
            avg("ActiveTimeDuringShift").alias("avg_active_time"),
            sum("InactiveTimeDuringShift").alias("total_inactive_time"),
            avg("Readings").alias("avg_readings"),
            sum("DistanceTravelled").alias("total_distance")
        )
        .withColumn("_calculated_at", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="gold_project_summary",
    comment="Project summary with worker counts"
)
def gold_project_summary():
    """Gold table: Project summary statistics."""
    projects = dlt.read("silver_project")
    workers = dlt.read("silver_worker")

    worker_counts = (
        workers
        .groupBy("ProjectID")
        .agg(countDistinct("WorkerID").alias("active_workers"))
    )

    return (
        projects
        .join(worker_counts, "ProjectID", "left")
        .select(
            projects["ProjectID"],
            projects["Project"],
            projects["ProjectName"],
            coalesce(col("active_workers"), lit(0)).alias("active_workers"),
            projects["CreatedAt"],
            projects["_ingested_at"]
        )
    )

# COMMAND ----------

# NEW: Additional Gold Views

@dlt.table(
    name="gold_crew_assignments",
    comment="Active crew assignments view"
)
def gold_crew_assignments():
    """Gold view: Active crew assignments with worker and crew details."""
    workers = dlt.read("silver_worker")
    crews = dlt.read("silver_crew")
    crew_assignments = (
        dlt.read("bronze_dbo_CrewAssignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        crew_assignments
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "inner")
        .join(crews.select("CrewID", "Crew", "CrewName"), "CrewID", "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("CrewID"),
            col("Crew"),
            col("CrewName"),
            col("ValidFrom"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_trade_assignments",
    comment="Active trade assignments view"
)
def gold_trade_assignments():
    """Gold view: Active trade assignments with worker and trade details."""
    workers = dlt.read("silver_worker")
    trades = dlt.read("silver_trade")
    trade_assignments = (
        dlt.read("bronze_dbo_TradeAssignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        trade_assignments
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "inner")
        .join(trades.select("TradeID", "Trade", "TradeName"), "TradeID", "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("TradeID"),
            col("Trade"),
            col("TradeName"),
            col("ValidFrom"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_workshift_assignments",
    comment="Active workshift assignments view"
)
def gold_workshift_assignments():
    """Gold view: Active workshift assignments with worker and shift details."""
    workers = dlt.read("silver_worker")
    workshifts = dlt.read("silver_workshift")
    workshift_assignments = (
        dlt.read("bronze_dbo_WorkshiftAssignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        workshift_assignments
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "inner")
        .join(workshifts.select("WorkshiftID", "Workshift", "WorkshiftName"), "WorkshiftID", "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("WorkshiftID"),
            col("Workshift"),
            col("WorkshiftName"),
            col("ValidFrom"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_device_assignments",
    comment="Active device assignments view"
)
def gold_device_assignments():
    """Gold view: Active device assignments with worker and device details."""
    workers = dlt.read("silver_worker")
    devices = dlt.read("silver_device")
    device_assignments = (
        dlt.read("bronze_dbo_DeviceAssignments")
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        device_assignments
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "inner")
        .join(devices.select("DeviceID", "Device", "DeviceName"), "DeviceID", "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("DeviceID"),
            col("Device"),
            col("DeviceName"),
            col("ValidFrom"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_manager_assignments",
    comment="Active manager assignments view"
)
def gold_manager_assignments():
    """Gold view: Active manager assignments."""
    workers = dlt.read("silver_worker")
    manager_assignments = (
        dlt.read("bronze_dbo_ManagerAssignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    # Self-join to get manager details
    managers = workers.select(
        col("WorkerID").alias("ManagerWorkerID"),
        col("Worker").alias("ManagerWorker"),
        col("WorkerName").alias("ManagerName")
    )

    return (
        manager_assignments
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "inner")
        .join(managers,
              manager_assignments["ManagerID"] == managers["ManagerWorkerID"], "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("ManagerID"),
            col("ManagerWorker"),
            col("ManagerName"),
            col("ValidFrom"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_location_group_assignments",
    comment="Active location group assignments view"
)
def gold_location_group_assignments():
    """Gold view: Active location group assignments."""
    workers = dlt.read("silver_worker")
    location_groups = dlt.read("silver_location_group")
    lg_assignments = (
        dlt.read("bronze_dbo_LocationGroupAssignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        lg_assignments
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "inner")
        .join(location_groups.select("LocationGroupID", "LocationGroup", "LocationGroupName"),
              "LocationGroupID", "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("LocationGroupID"),
            col("LocationGroup"),
            col("LocationGroupName"),
            col("ValidFrom"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_floor_zone_hierarchy",
    comment="Floor and zone hierarchy view"
)
def gold_floor_zone_hierarchy():
    """Gold view: Floor and zone hierarchy with project."""
    projects = dlt.read("silver_project")
    floors = dlt.read("silver_floor")
    zones = dlt.read("silver_zone")

    return (
        zones
        .join(floors.select("FloorID", "Floor", "FloorName", "ProjectID"), "FloorID", "left")
        .join(projects.select("ProjectID", "Project", "ProjectName"), "ProjectID", "left")
        .select(
            col("ProjectID"),
            col("Project"),
            col("ProjectName"),
            col("FloorID"),
            col("Floor"),
            col("FloorName"),
            col("ZoneID"),
            col("Zone"),
            col("ZoneName"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_workshift_schedule",
    comment="Workshift schedule with details"
)
def gold_workshift_schedule():
    """Gold view: Workshift schedule with shift times and days."""
    workshifts = dlt.read("silver_workshift")
    workshift_details = dlt.read("bronze_dbo_WorkshiftDetails")
    projects = dlt.read("silver_project")

    return (
        workshifts
        .join(workshift_details, "WorkshiftID", "left")
        .join(projects.select("ProjectID", "Project", "ProjectName"), "ProjectID", "left")
        .select(
            col("WorkshiftID"),
            col("Workshift"),
            col("WorkshiftName"),
            col("ProjectID"),
            col("Project"),
            col("ProjectName"),
            col("DayOfWeek"),
            col("StartTime"),
            col("FinishTime"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_weather_by_project",
    comment="Weather observations aggregated by project and date"
)
def gold_weather_by_project():
    """Gold view: Weather observations aggregated by project."""
    weather = dlt.read("bronze_dbo_FactWeatherObservations")
    projects = dlt.read("silver_project")

    return (
        weather
        .join(projects.select("ProjectID", "Project", "ProjectName"), "ProjectID", "left")
        .select(
            col("ProjectID"),
            col("Project"),
            col("ProjectName"),
            col("ObservationDate"),
            col("Temperature"),
            col("Humidity"),
            col("WindSpeed"),
            col("Condition"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_contact_tracing_summary",
    comment="Contact tracing summary by worker"
)
def gold_contact_tracing_summary():
    """Gold view: Contact tracing summary showing worker contacts."""
    contacts = dlt.read("silver_workers_contacts")
    workers = dlt.read("silver_worker")

    return (
        contacts
        .groupBy("WorkerID", "ProjectID", "ContactDate")
        .agg(
            countDistinct("ContactWorkerID").alias("unique_contacts"),
            sum("ContactDuration").alias("total_contact_duration"),
            avg("ContactDistance").alias("avg_contact_distance")
        )
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("ContactDate"),
            col("unique_contacts"),
            col("total_contact_duration"),
            col("avg_contact_distance"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_worker_productivity",
    comment="Worker productivity metrics from combined shifts"
)
def gold_worker_productivity():
    """Gold view: Worker productivity based on shifts combined data."""
    shifts_combined = dlt.read("silver_workers_shifts_combined")
    workers = dlt.read("silver_worker")

    return (
        shifts_combined
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "left")
        .select(
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("ShiftLocalDate"),
            col("TotalActiveTime"),
            col("TotalInactiveTime"),
            col("TotalReadings"),
            col("TotalDistanceTravelled"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_observation_summary",
    comment="Observation summary by project and type"
)
def gold_observation_summary():
    """Gold view: Observation summary aggregated by project and type."""
    observations = dlt.read("silver_observations")
    projects = dlt.read("silver_project")
    sources = dlt.read("bronze_dbo_ObservationSource")

    return (
        observations
        .groupBy("ProjectID", "ObservationSourceID", "ObservationDate")
        .agg(
            count("*").alias("observation_count"),
            countDistinct("WorkerID").alias("workers_observed")
        )
        .join(projects.select("ProjectID", "Project", "ProjectName"), "ProjectID", "left")
        .join(sources.select("ObservationSourceID", "ObservationSource", "ObservationSourceName"),
              "ObservationSourceID", "left")
        .select(
            col("ProjectID"),
            col("Project"),
            col("ProjectName"),
            col("ObservationSourceID"),
            col("ObservationSource"),
            col("ObservationSourceName"),
            col("ObservationDate"),
            col("observation_count"),
            col("workers_observed"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_task_progress",
    comment="Task progress by project and activity"
)
def gold_task_progress():
    """Gold view: Task progress summary by project."""
    tasks = dlt.read("bronze_dbo_FactWorkersTasks")
    workers = dlt.read("silver_worker")
    activities = dlt.read("silver_activity")

    return (
        tasks
        .join(workers.select("WorkerID", "Worker", "WorkerName", "ProjectID"),
              ["WorkerID", "ProjectID"], "left")
        .join(activities.select("ActivityID", "Activity", "ActivityName"), "ActivityID", "left")
        .select(
            col("TaskID"),
            col("WorkerID"),
            col("Worker"),
            col("WorkerName"),
            col("ProjectID"),
            col("ActivityID"),
            col("Activity"),
            col("ActivityName"),
            col("TaskDate"),
            col("PlannedQuantity"),
            col("ActualQuantity"),
            col("CompletionPercentage"),
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary
# MAGIC
# MAGIC ### Tables Created
# MAGIC
# MAGIC | Layer | Count | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | 36 | Raw tables from SQL Server |
# MAGIC | Silver | 18 | Cleaned tables with DQ rules |
# MAGIC | Gold | 16 | Business views and aggregates |
# MAGIC | **Total** | **70** | |
# MAGIC
# MAGIC ### Bronze Tables (36)
# MAGIC - **Dimensions (16):** Worker, Project, Crew, Device, Organization, Company, Trade, Floor, Zone, Workshift, WorkshiftDetails, Activity, LocationGroup, Department, Title, ObservationSource
# MAGIC - **Facts (9):** FactWorkersHistory, FactWorkersShifts, FactObservations, FactReportedAttendance, FactProgress, FactWorkersShiftsCombined, FactWeatherObservations, FactWorkersContacts, FactWorkersTasks
# MAGIC - **Assignments (8):** CrewAssignments, DeviceAssignments, WorkshiftAssignments, TradeAssignments, ManagerAssignments, ManagerAssignmentSnapshots, LocationGroupAssignments, WorkerLocationAssignments
# MAGIC - **Other (3):** ContactTracingRule, SyncState, ImpactedAreaLog
# MAGIC
# MAGIC ### Secret Scope Configuration
# MAGIC
# MAGIC **Required Secrets in `wakecap_migration`:**
# MAGIC - `sqlserver_jdbc_url` - JDBC connection URL
# MAGIC - `sqlserver_user` - SQL Server username
# MAGIC - `sqlserver_password` - SQL Server password
