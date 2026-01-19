# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Bronze Layer
# MAGIC
# MAGIC Raw ingestion from SQL Server.
# MAGIC
# MAGIC **Source:** Azure SQL Server - WakeCapDW_20251215
# MAGIC **Target:** wakecap_prod.migration (Bronze tables)
# MAGIC
# MAGIC **Note:** FactWorkersHistory is DISABLED due to long refresh times.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
SECRET_SCOPE = "wakecap_migration"

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
        .withColumn("_dlt_load_time", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Worker",
    comment="Raw worker data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Worker():
    return read_sql_server_table("Worker", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Project",
    comment="Raw project data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Project():
    return read_sql_server_table("Project", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Crew",
    comment="Raw crew data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Crew():
    return read_sql_server_table("Crew", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Device",
    comment="Raw device data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Device():
    return read_sql_server_table("Device", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Organization",
    comment="Raw organization data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Organization():
    return read_sql_server_table("Organization", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Company",
    comment="Raw company data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Company():
    return read_sql_server_table("Company", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Trade",
    comment="Raw trade data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Trade():
    return read_sql_server_table("Trade", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Floor",
    comment="Raw floor data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Floor():
    return read_sql_server_table("Floor", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Zone",
    comment="Raw zone data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Zone():
    return read_sql_server_table("Zone", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_Workshift",
    comment="Raw workshift data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_Workshift():
    return read_sql_server_table("Workshift", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables
# MAGIC **Note:** FactWorkersHistory is DISABLED due to long refresh times.

# COMMAND ----------

# DISABLED: FactWorkersHistory - takes too long to refresh
# @dlt.table(name="bronze_dbo_FactWorkersHistory", ...)

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactWorkersShifts",
    comment="Raw workers shifts fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_FactWorkersShifts():
    return read_sql_server_table("FactWorkersShifts", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactObservations",
    comment="Raw observations fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_FactObservations():
    return read_sql_server_table("FactObservations", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactReportedAttendance",
    comment="Raw reported attendance fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_FactReportedAttendance():
    return read_sql_server_table("FactReportedAttendance", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_FactProgress",
    comment="Raw progress fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_FactProgress():
    return read_sql_server_table("FactProgress", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assignment Tables

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_CrewAssignments",
    comment="Raw crew assignments from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_CrewAssignments():
    return read_sql_server_table("CrewAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_DeviceAssignments",
    comment="Raw device assignments from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_DeviceAssignments():
    return read_sql_server_table("DeviceAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_WorkshiftAssignments",
    comment="Raw workshift assignments from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_WorkshiftAssignments():
    return read_sql_server_table("WorkshiftAssignments", "dbo")
