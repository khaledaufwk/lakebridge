# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration Orchestrator
# MAGIC
# MAGIC This notebook orchestrates the incremental load of all WakeCapDW tables from SQL Server to Delta Lake.
# MAGIC It calls the `sqlserver_incremental_load` notebook for each table in the configuration.
# MAGIC
# MAGIC ## Usage
# MAGIC - Run with `category` parameter to load specific category (dimensions, assignments, facts, all)
# MAGIC - Run with `table_filter` to load specific table(s)
# MAGIC - Run with `parallel_tasks` to control concurrency

# COMMAND ----------

import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("category", "all", "Category to load: dimensions, assignments, facts, staging, other, all")
category = dbutils.widgets.get("category")

dbutils.widgets.text("table_filter", "", "Comma-separated list of specific tables to load (empty = all)")
table_filter = dbutils.widgets.get("table_filter")
table_filter_list = [t.strip().lower() for t in table_filter.split(',') if t.strip()]

dbutils.widgets.text("parallel_tasks", "4", "Number of parallel notebook runs")
parallel_tasks = int(dbutils.widgets.get("parallel_tasks"))

dbutils.widgets.text("notebook_path", "/Workspace/WakeCapDW/notebooks/sqlserver_incremental_load", "Path to the incremental load notebook")
notebook_path = dbutils.widgets.get("notebook_path")

dbutils.widgets.text("timeout_seconds", "3600", "Timeout per table in seconds")
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))

dbutils.widgets.text("stop_on_error", "0", "Set to 1 to stop on first error")
stop_on_error = dbutils.widgets.get("stop_on_error") == "1"

# COMMAND ----------

# DBTITLE 1,Table Configuration
# Load configuration from JSON file or use embedded config
# The JSON file should be uploaded to DBFS or workspace alongside this notebook

import requests

# Try to load from workspace file first
CONFIG_PATH = "/Workspace/WakeCapDW/config/wakecapdw_tables_complete.json"

try:
    # Try reading from workspace
    config_content = dbutils.fs.head(f"file:{CONFIG_PATH}", 500000)
    TABLE_CONFIG = json.loads(config_content)
    print(f"Loaded configuration from {CONFIG_PATH}")
except Exception as e:
    print(f"Could not load external config ({e}), using embedded configuration")

    # Embedded complete configuration for all 136 tables
    TABLE_CONFIG = {
        "metadata": {
            "source_system": "wakecapdw",
            "source_database": "WakeCapDW_20251215",
            "target_catalog": "wakecap_prod",
            "target_schema": "raw"
        },
        "connection": {
            "secret_scope": "akv-wakecap24",
            "source_driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "source_user": "wakecap_reader",
            "source_pwd_secret_name": "sqlserver-wakecap-password",
            "source_url": "jdbc:sqlserver://wakecap24.database.windows.net:1433"
        },
        "tables": {
            "dimensions": [
                {"source_schema": "dbo", "source_table": "Activity", "primary_key_columns": "ActivityID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Company", "primary_key_columns": "CompanyID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ContactTracingRule", "primary_key_columns": "ContactTracingRuleID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Crew", "primary_key_columns": "CrewID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Department", "primary_key_columns": "DepartmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Device", "primary_key_columns": "DeviceID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ExtSource", "primary_key_columns": "ExtSourceID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "dbo", "source_table": "Floor", "primary_key_columns": "FloorID", "watermark_column": "WatermarkUTC", "is_full_load": False, "has_geometry": True},
                {"source_schema": "dbo", "source_table": "LocationAssignmentClass", "primary_key_columns": "LocationAssignmentClassID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "dbo", "source_table": "LocationGroup", "primary_key_columns": "LocationGroupID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ObservationClinicViolationStatus", "primary_key_columns": "ObservationClinicViolationStatusID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ObservationDiscriminator", "primary_key_columns": "ObservationDiscriminatorID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ObservationSeverity", "primary_key_columns": "ObservationSeverityID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ObservationSource", "primary_key_columns": "ObservationSourceID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ObservationStatus", "primary_key_columns": "ObservationStatusID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ObservationType", "primary_key_columns": "ObservationTypeID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Organization", "primary_key_columns": "OrganizationID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ProductiveClass", "primary_key_columns": "ProductiveClassID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "dbo", "source_table": "Project", "primary_key_columns": "ProjectID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ShiftTimeCategory", "primary_key_columns": "ShiftTimeCategoryID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "dbo", "source_table": "Stats", "primary_key_columns": "StatsID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Task", "primary_key_columns": "TaskID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "TimeZoneMapping", "primary_key_columns": "TimeZoneMappingID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "dbo", "source_table": "Title", "primary_key_columns": "TitleID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Trade", "primary_key_columns": "TradeID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Work", "primary_key_columns": "WorkID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Worker", "primary_key_columns": "WorkerID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "WorkerStatus", "primary_key_columns": "WorkerStatusID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "dbo", "source_table": "Workshift", "primary_key_columns": "WorkshiftID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "WorkshiftDetails", "primary_key_columns": "WorkshiftDetailsID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "Zone", "primary_key_columns": "ZoneID", "watermark_column": "WatermarkUTC", "is_full_load": False, "has_geometry": True},
            ],
            "facts": [
                {"source_schema": "dbo", "source_table": "FactObservations", "primary_key_columns": "FactObservationID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactProgress", "primary_key_columns": "FactProgressID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactReportedAttendance", "primary_key_columns": "FactReportedAttendanceID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactWeatherObservations", "primary_key_columns": "FactWeatherObservationID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactWorkersContacts", "primary_key_columns": "FactWorkersContactID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactWorkersHistory", "primary_key_columns": "FactWorkersHistoryID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactWorkersShifts", "primary_key_columns": "FactWorkersShiftID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactWorkersShiftsCombined", "primary_key_columns": "FactWorkersShiftsCombinedID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "FactWorkersTasks", "primary_key_columns": "FactWorkersTaskID", "watermark_column": "WatermarkUTC", "is_full_load": False},
            ],
            "assignments": [
                {"source_schema": "dbo", "source_table": "CrewAssignments", "primary_key_columns": "CrewAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "DeviceAssignments", "primary_key_columns": "DeviceAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "LocationGroupAssignments", "primary_key_columns": "LocationGroupAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ManagerAssignments", "primary_key_columns": "ManagerAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "ManagerAssignmentSnapshots", "primary_key_columns": "ManagerAssignmentSnapshotID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "TradeAssignments", "primary_key_columns": "TradeAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "WorkerLocationAssignments", "primary_key_columns": "WorkerLocationAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "dbo", "source_table": "WorkshiftAssignments", "primary_key_columns": "WorkshiftAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
            ],
            "staging": [
                {"source_schema": "stg", "source_table": "ImpactedAreaLog", "primary_key_columns": "ImpactedAreaLogID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "stg", "source_table": "SyncState", "primary_key_columns": "SyncStateID", "watermark_column": None, "is_full_load": True},
            ],
            "staging_sources": [
                {"source_schema": "stg", "source_table": "wc2021_worker_activity_assignments", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_asset_location", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Company", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Crew", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_CrewComposition", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_CrewManagerAssignments", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Department", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_LocationGroup", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_LocationGroupZone", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_ManualLocationAssignment", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_node", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_OBS", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_observation_Observations", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_organization", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_People", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Project", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_ResourceDevice", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_ResourceHours", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_ResourceTimesheet", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_ResourceZone", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Space", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Trade", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_weather_station_sensor", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Workshift", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_WorkshiftResourceAssignment", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_WorkshiftSchedule", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "stg", "source_table": "wc2023_Zone", "primary_key_columns": "id", "watermark_column": "WatermarkUTC", "is_full_load": False},
            ],
            "merge": [
                {"source_schema": "mrg", "source_table": "Activity", "primary_key_columns": "ActivityID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Building", "primary_key_columns": "BuildingID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "ContactTracingRule", "primary_key_columns": "ContactTracingRuleID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Contractor", "primary_key_columns": "ContractorID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Crew", "primary_key_columns": "CrewID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "CrewAssignments", "primary_key_columns": "CrewAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "FactReportedAttendance", "primary_key_columns": "FactReportedAttendanceID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "FactWorkersContacts", "primary_key_columns": "FactWorkersContactsID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "FactWorkersHistory", "primary_key_columns": "FactWorkersHistoryID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "FactWorkersShifts", "primary_key_columns": "FactWorkersShiftsID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "FactWorkersShiftsCombined", "primary_key_columns": "FactWorkersShiftsCombinedID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "FactWorkersTasks", "primary_key_columns": "FactWorkersTasksID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Floor", "primary_key_columns": "FloorID", "watermark_column": "WatermarkUTC", "is_full_load": False, "has_geometry": True},
                {"source_schema": "mrg", "source_table": "InventoryAssignments", "primary_key_columns": "InventoryAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "LocationGroup", "primary_key_columns": "LocationGroupID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "LocationGroupAssignments", "primary_key_columns": "LocationGroupAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "ManagerAssignments", "primary_key_columns": "ManagerAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "ManagerAssignmentSnapshots", "primary_key_columns": "ManagerAssignmentSnapshotID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Organization", "primary_key_columns": "OrganizationID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Project", "primary_key_columns": "ProjectID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Task", "primary_key_columns": "TaskID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Worker", "primary_key_columns": "WorkerID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkerLocationAssignments", "primary_key_columns": "WorkerLocationAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkerRole", "primary_key_columns": "WorkerRoleID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkerRoleAssignments", "primary_key_columns": "WorkerRoleAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkerStatus", "primary_key_columns": "WorkerStatusID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkerStatusAssignments", "primary_key_columns": "WorkerStatusAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkShift", "primary_key_columns": "WorkShiftID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkShiftAssignments", "primary_key_columns": "WorkShiftAssignmentID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "WorkShiftDetails", "primary_key_columns": "WorkShiftDetailsID", "watermark_column": "WatermarkUTC", "is_full_load": False},
                {"source_schema": "mrg", "source_table": "Zone", "primary_key_columns": "ZoneID", "watermark_column": "WatermarkUTC", "is_full_load": False, "has_geometry": True},
            ],
            "security": [
                {"source_schema": "security", "source_table": "UserPermissions", "primary_key_columns": "UserPermissionID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "security", "source_table": "UserPermissionsCompany", "primary_key_columns": "UserPermissionsCompanyID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "security", "source_table": "UserPermissionsCrew", "primary_key_columns": "UserPermissionsCrewID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "security", "source_table": "UserPermissionsOrganization", "primary_key_columns": "UserPermissionsOrganizationID", "watermark_column": None, "is_full_load": True},
                {"source_schema": "security", "source_table": "UserPermissionsProject", "primary_key_columns": "UserPermissionsProjectID", "watermark_column": None, "is_full_load": True},
            ]
        }
    }

print(f"Configuration loaded with {sum(len(v) for v in TABLE_CONFIG['tables'].values())} tables")

# COMMAND ----------

# DBTITLE 1,Get Tables to Process
def get_tables_to_process():
    """Get list of tables to process based on category and filter."""
    tables = []

    # All available categories
    all_categories = ["dimensions", "facts", "assignments", "staging", "staging_sources", "merge", "security"]

    categories_to_process = []
    if category == "all":
        categories_to_process = all_categories
    elif category in all_categories:
        categories_to_process = [category]
    else:
        print(f"Warning: Unknown category '{category}'. Using all categories.")
        categories_to_process = all_categories

    for cat in categories_to_process:
        if cat in TABLE_CONFIG["tables"]:
            for table in TABLE_CONFIG["tables"][cat]:
                table_name = table["source_table"].lower()

                # Apply filter if specified
                if table_filter_list and table_name not in table_filter_list:
                    continue

                tables.append({
                    **table,
                    "category": cat
                })

    return tables

tables_to_process = get_tables_to_process()
print(f"Tables to process: {len(tables_to_process)}")
for t in tables_to_process:
    print(f"  - {t['source_schema']}.{t['source_table']}")

# COMMAND ----------

# DBTITLE 1,Run Single Table Load
def run_table_load(table_config):
    """Run the incremental load notebook for a single table."""
    start_time = datetime.now()
    table_name = f"{table_config['source_schema']}.{table_config['source_table']}"

    try:
        # Build notebook parameters
        params = {
            "secret_scope": TABLE_CONFIG["connection"]["secret_scope"],
            "source_driver": TABLE_CONFIG["connection"]["source_driver"],
            "source_user": TABLE_CONFIG["connection"]["source_user"],
            "source_pwd_secret_name": TABLE_CONFIG["connection"]["source_pwd_secret_name"],
            "source_url": TABLE_CONFIG["connection"]["source_url"],
            "source_database": TABLE_CONFIG["metadata"]["source_database"],
            "source_system_ref": TABLE_CONFIG["metadata"]["source_system"],
            "source_schema": table_config["source_schema"],
            "source_dataset_ref": table_config["source_table"],
            "target_catalog": TABLE_CONFIG["metadata"]["target_catalog"],
            "target_schema": TABLE_CONFIG["metadata"]["target_schema"],
            "primary_key_columns": table_config["primary_key_columns"],
            "source_watermark_column": table_config.get("watermark_column") or "WatermarkUTC",
            "is_full_load": "1" if table_config.get("is_full_load", False) else "0",
            "allow_update": "1",
        }

        # Add custom column mapping if present
        if "custom_column_mapping" in table_config:
            if isinstance(table_config["custom_column_mapping"], str):
                params["custom_column_mapping"] = table_config["custom_column_mapping"]
            else:
                params["custom_column_mapping"] = json.dumps(table_config["custom_column_mapping"])

        # Run the notebook
        result = dbutils.notebook.run(notebook_path, timeout_seconds, params)
        result_data = json.loads(result)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            "table": table_name,
            "status": "success",
            "result": result_data,
            "duration_seconds": duration
        }

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            "table": table_name,
            "status": "error",
            "error": str(e),
            "duration_seconds": duration
        }

# COMMAND ----------

# DBTITLE 1,Run All Tables (Sequential or Parallel)
results = []
errors = []

print("=" * 60)
print(f"Starting WakeCapDW Migration - {datetime.now()}")
print(f"Tables to process: {len(tables_to_process)}")
print(f"Parallel tasks: {parallel_tasks}")
print("=" * 60)

if parallel_tasks <= 1:
    # Sequential execution
    for i, table in enumerate(tables_to_process):
        print(f"\n[{i+1}/{len(tables_to_process)}] Processing {table['source_schema']}.{table['source_table']}...")
        result = run_table_load(table)
        results.append(result)

        if result["status"] == "success":
            print(f"  ✓ Success - {result['result'].get('rows_processed', 0)} rows in {result['duration_seconds']:.1f}s")
        else:
            print(f"  ✗ Error: {result['error']}")
            errors.append(result)
            if stop_on_error:
                print("Stopping due to error (stop_on_error=1)")
                break
else:
    # Parallel execution using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=parallel_tasks) as executor:
        future_to_table = {executor.submit(run_table_load, table): table for table in tables_to_process}

        for i, future in enumerate(as_completed(future_to_table)):
            table = future_to_table[future]
            result = future.result()
            results.append(result)

            if result["status"] == "success":
                print(f"[{i+1}/{len(tables_to_process)}] {result['table']} ✓ - {result['result'].get('rows_processed', 0)} rows in {result['duration_seconds']:.1f}s")
            else:
                print(f"[{i+1}/{len(tables_to_process)}] {result['table']} ✗ - {result['error']}")
                errors.append(result)

# COMMAND ----------

# DBTITLE 1,Summary Report
print("\n" + "=" * 60)
print("MIGRATION SUMMARY")
print("=" * 60)

success_count = len([r for r in results if r["status"] == "success"])
error_count = len(errors)
total_rows = sum(r.get("result", {}).get("rows_processed", 0) for r in results if r["status"] == "success")
total_duration = sum(r["duration_seconds"] for r in results)

print(f"Tables processed: {len(results)}")
print(f"Successful: {success_count}")
print(f"Errors: {error_count}")
print(f"Total rows processed: {total_rows:,}")
print(f"Total duration: {total_duration:.1f} seconds")

if errors:
    print("\nFailed tables:")
    for err in errors:
        print(f"  - {err['table']}: {err['error'][:100]}")

# COMMAND ----------

# DBTITLE 1,Results DataFrame
results_df = spark.createDataFrame([
    {
        "table": r["table"],
        "status": r["status"],
        "rows_processed": r.get("result", {}).get("rows_processed", 0) if r["status"] == "success" else 0,
        "total_rows": r.get("result", {}).get("total_rows", 0) if r["status"] == "success" else 0,
        "duration_seconds": r["duration_seconds"],
        "error": r.get("error", "")
    }
    for r in results
])

display(results_df)

# COMMAND ----------

# DBTITLE 1,Exit with Status
exit_result = {
    "status": "success" if error_count == 0 else "partial_success" if success_count > 0 else "failed",
    "tables_processed": len(results),
    "tables_successful": success_count,
    "tables_failed": error_count,
    "total_rows_processed": total_rows,
    "total_duration_seconds": total_duration,
    "errors": [{"table": e["table"], "error": e["error"]} for e in errors]
}

dbutils.notebook.exit(json.dumps(exit_result))
