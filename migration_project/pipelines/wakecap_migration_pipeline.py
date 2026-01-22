# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration Pipeline
# MAGIC 
# MAGIC This Delta Live Tables pipeline migrates data from SQL Server (WakeCapDW) to Databricks.
# MAGIC 
# MAGIC **Source:** Azure SQL Server - WakeCapDW_20251215
# MAGIC **Target:** Databricks Unity Catalog
# MAGIC **Generated:** 2026-01-18
# MAGIC 
# MAGIC ## Architecture
# MAGIC - **Bronze Layer:** Raw ingestion from SQL Server
# MAGIC - **Silver Layer:** Cleaned and validated data
# MAGIC - **Gold Layer:** Business aggregates and views

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
# MAGIC ## Bronze Layer - Raw Tables
# MAGIC These tables contain raw data ingested from SQL Server.

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

# Fact Tables

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

# Assignment Tables

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

# MAGIC %md
# MAGIC ## Pipeline Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Metrics
# MAGIC 
# MAGIC This pipeline includes the following data quality expectations:
# MAGIC 
# MAGIC | Layer | Table | Rule | Action |
# MAGIC |-------|-------|------|--------|
# MAGIC | Silver | worker | WorkerID NOT NULL | Drop |
# MAGIC | Silver | worker | ProjectID NOT NULL | Drop |
# MAGIC | Silver | project | ProjectID NOT NULL | Drop |
# MAGIC | Silver | crew | CrewID NOT NULL | Drop |
# MAGIC | Silver | device | DeviceID NOT NULL | Drop |
# MAGIC | Silver | workers_shifts | ShiftLocalDate NOT NULL | Warn |
# MAGIC 
# MAGIC ### Monitoring
# MAGIC 
# MAGIC Monitor pipeline health using:
# MAGIC - DLT Pipeline UI for run status
# MAGIC - Data quality metrics in pipeline events
# MAGIC - Unity Catalog data lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Reference
# MAGIC 
# MAGIC **Secret Scope:** `wakecap_migration`
# MAGIC 
# MAGIC **Required Secrets:**
# MAGIC - `sqlserver_jdbc_url` - JDBC connection URL
# MAGIC - `sqlserver_user` - SQL Server username
# MAGIC - `sqlserver_password` - SQL Server password
# MAGIC 
# MAGIC **Target:**
# MAGIC - Catalog: `wakecap_prod`
# MAGIC - Schema: `migration`
