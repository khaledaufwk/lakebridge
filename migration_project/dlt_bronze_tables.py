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
    name="bronze_dbo_worker",
    comment="Raw worker data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_worker():
    return read_sql_server_table("Worker", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_project",
    comment="Raw project data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_project():
    return read_sql_server_table("Project", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_crew",
    comment="Raw crew data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_crew():
    return read_sql_server_table("Crew", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_device",
    comment="Raw device data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_device():
    return read_sql_server_table("Device", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_organization",
    comment="Raw organization data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_organization():
    return read_sql_server_table("Organization", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_company",
    comment="Raw company data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_company():
    return read_sql_server_table("Company", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_trade",
    comment="Raw trade data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_trade():
    return read_sql_server_table("Trade", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_floor",
    comment="Raw floor data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_floor():
    return read_sql_server_table("Floor", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_zone",
    comment="Raw zone data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_zone():
    return read_sql_server_table("Zone", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_workshift",
    comment="Raw workshift data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_workshift():
    return read_sql_server_table("Workshift", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables
# MAGIC **Note:** FactWorkersHistory is DISABLED due to long refresh times.

# COMMAND ----------

# DISABLED: FactWorkersHistory - takes too long to refresh
# @dlt.table(name="bronze_dbo_factworkershistory", ...)

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_factworkersshifts",
    comment="Raw workers shifts fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_factworkersshifts():
    return read_sql_server_table("FactWorkersShifts", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_factobservations",
    comment="Raw observations fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_factobservations():
    return read_sql_server_table("FactObservations", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_factreportedattendance",
    comment="Raw reported attendance fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_factreportedattendance():
    return read_sql_server_table("FactReportedAttendance", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_factprogress",
    comment="Raw progress fact from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_factprogress():
    return read_sql_server_table("FactProgress", "dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assignment Tables

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_crewassignments",
    comment="Raw crew assignments from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_crewassignments():
    return read_sql_server_table("CrewAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_deviceassignments",
    comment="Raw device assignments from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_deviceassignments():
    return read_sql_server_table("DeviceAssignments", "dbo")

# COMMAND ----------

@dlt.table(
    name="bronze_dbo_workshiftassignments",
    comment="Raw workshift assignments from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_dbo_workshiftassignments():
    return read_sql_server_table("WorkshiftAssignments", "dbo")
