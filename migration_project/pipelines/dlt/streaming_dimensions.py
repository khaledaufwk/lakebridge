# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Streaming Dimension Tables (CDC)
# MAGIC
# MAGIC This notebook defines DLT streaming tables with Change Data Capture (CDC)
# MAGIC for dimension tables. Replaces spDeltaSync* stored procedures.
# MAGIC
# MAGIC **Converted Procedures:**
# MAGIC - stg.spDeltaSyncDimWorker
# MAGIC - stg.spDeltaSyncDimProject
# MAGIC - stg.spDeltaSyncDimCrew
# MAGIC - stg.spDeltaSyncDimDevice
# MAGIC - stg.spDeltaSyncDimTrade
# MAGIC - stg.spDeltaSyncDimOrganization
# MAGIC - stg.spDeltaSyncDimCompany
# MAGIC - stg.spDeltaSyncDimDepartment
# MAGIC - stg.spDeltaSyncDimFloor
# MAGIC - stg.spDeltaSyncDimZone
# MAGIC - stg.spDeltaSyncDimWorkshift
# MAGIC - stg.spDeltaSyncDimWorkshiftDetails
# MAGIC - stg.spDeltaSyncDimActivity
# MAGIC - stg.spDeltaSyncDimLocationGroup
# MAGIC - stg.spDeltaSyncDimTitle
# MAGIC
# MAGIC **Pattern:** Uses DLT APPLY CHANGES for SCD Type 2 tracking

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, lit, coalesce

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# ADLS Configuration
ADLS_STORAGE_ACCOUNT = spark.conf.get("pipeline.adls_storage_account", "wakecapadls")
ADLS_CONTAINER = "raw"
ADLS_BASE_PATH = f"abfss://{ADLS_CONTAINER}@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/wakecap"

def get_adls_path(table_category, table_name):
    """Get ADLS path for a table."""
    return f"{ADLS_BASE_PATH}/{table_category}/{table_name}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worker CDC
# MAGIC Converted from: stg.spDeltaSyncDimWorker (227 lines)

# COMMAND ----------

@dlt.table(
    name="staging_worker",
    comment="Streaming staging table for Worker CDC changes",
    table_properties={"quality": "staging"}
)
def staging_worker():
    """Capture CDC changes from ADLS for Worker dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_worker/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Worker"))
        .withColumn("_load_time", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

dlt.create_streaming_table(
    name="worker_cdc",
    comment="Worker dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="worker_cdc",
    source="staging_worker",
    keys=["WorkerID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=[
        "WorkerName", "Worker", "ProjectID", "CompanyID", "TradeID",
        "TitleID", "DepartmentID", "WorkerStatusID", "DeletedAt"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project CDC
# MAGIC Converted from: stg.spDeltaSyncDimProject (198 lines)

# COMMAND ----------

@dlt.table(
    name="staging_project",
    comment="Streaming staging table for Project CDC changes",
    table_properties={"quality": "staging"}
)
def staging_project():
    """Capture CDC changes from ADLS for Project dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_project/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Project"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="project_cdc",
    comment="Project dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="project_cdc",
    source="staging_project",
    keys=["ProjectID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=[
        "ProjectName", "Project", "OrganizationID", "TimeZoneID",
        "IsActive", "DeletedAt"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crew CDC
# MAGIC Converted from: stg.spDeltaSyncDimCrew (151 lines)

# COMMAND ----------

@dlt.table(
    name="staging_crew",
    comment="Streaming staging table for Crew CDC changes",
    table_properties={"quality": "staging"}
)
def staging_crew():
    """Capture CDC changes from ADLS for Crew dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_crew/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Crew"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="crew_cdc",
    comment="Crew dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="crew_cdc",
    source="staging_crew",
    keys=["CrewID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["CrewName", "Crew", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Device CDC
# MAGIC Converted from: stg.spDeltaSyncDimDevice (149 lines)

# COMMAND ----------

@dlt.table(
    name="staging_device",
    comment="Streaming staging table for Device CDC changes",
    table_properties={"quality": "staging"}
)
def staging_device():
    """Capture CDC changes from ADLS for Device dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_device/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Device"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="device_cdc",
    comment="Device dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="device_cdc",
    source="staging_device",
    keys=["DeviceID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["DeviceName", "Device", "DeviceModelID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trade CDC
# MAGIC Converted from: stg.spDeltaSyncDimTrade (132 lines)

# COMMAND ----------

@dlt.table(
    name="staging_trade",
    comment="Streaming staging table for Trade CDC changes",
    table_properties={"quality": "staging"}
)
def staging_trade():
    """Capture CDC changes from ADLS for Trade dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_trade/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Trade"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="trade_cdc",
    comment="Trade dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="trade_cdc",
    source="staging_trade",
    keys=["TradeID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["TradeName", "Trade", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organization CDC
# MAGIC Converted from: stg.spDeltaSyncDimOrganization (90 lines)

# COMMAND ----------

@dlt.table(
    name="staging_organization",
    comment="Streaming staging table for Organization CDC changes",
    table_properties={"quality": "staging"}
)
def staging_organization():
    """Capture CDC changes from ADLS for Organization dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_organization/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Organization"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="organization_cdc",
    comment="Organization dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="organization_cdc",
    source="staging_organization",
    keys=["OrganizationID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["OrganizationName", "Organization", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Company CDC
# MAGIC Converted from: stg.spDeltaSyncDimCompany (126 lines)

# COMMAND ----------

@dlt.table(
    name="staging_company",
    comment="Streaming staging table for Company CDC changes",
    table_properties={"quality": "staging"}
)
def staging_company():
    """Capture CDC changes from ADLS for Company dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_company/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Company"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="company_cdc",
    comment="Company dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="company_cdc",
    source="staging_company",
    keys=["CompanyID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["CompanyName", "Company", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Department CDC
# MAGIC Converted from: stg.spDeltaSyncDimDepartment

# COMMAND ----------

@dlt.table(
    name="staging_department",
    comment="Streaming staging table for Department CDC changes",
    table_properties={"quality": "staging"}
)
def staging_department():
    """Capture CDC changes from ADLS for Department dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_department/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Department"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="department_cdc",
    comment="Department dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="department_cdc",
    source="staging_department",
    keys=["DepartmentID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["DepartmentName", "Department", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Floor CDC
# MAGIC Converted from: stg.spDeltaSyncDimFloor (271 lines) - includes SPATIAL

# COMMAND ----------

@dlt.table(
    name="staging_floor",
    comment="Streaming staging table for Floor CDC changes",
    table_properties={"quality": "staging"}
)
def staging_floor():
    """Capture CDC changes from ADLS for Floor dimension.

    Note: Spatial/Geography columns are cast to WKT strings during ADF extraction.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_floor/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Floor"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="floor_cdc",
    comment="Floor dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="floor_cdc",
    source="staging_floor",
    keys=["FloorID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["FloorName", "Floor", "ProjectID", "SortOrder", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zone CDC
# MAGIC Converted from: stg.spDeltaSyncDimZone (321 lines) - includes SPATIAL

# COMMAND ----------

@dlt.table(
    name="staging_zone",
    comment="Streaming staging table for Zone CDC changes",
    table_properties={"quality": "staging"}
)
def staging_zone():
    """Capture CDC changes from ADLS for Zone dimension.

    Note: Spatial/Geography columns are cast to WKT strings during ADF extraction.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_zone/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Zone"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="zone_cdc",
    comment="Zone dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="zone_cdc",
    source="staging_zone",
    keys=["ZoneID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["ZoneName", "Zone", "FloorID", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workshift CDC
# MAGIC Converted from: stg.spDeltaSyncDimWorkshift

# COMMAND ----------

@dlt.table(
    name="staging_workshift",
    comment="Streaming staging table for Workshift CDC changes",
    table_properties={"quality": "staging"}
)
def staging_workshift():
    """Capture CDC changes from ADLS for Workshift dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_workshift/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Workshift"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="workshift_cdc",
    comment="Workshift dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="workshift_cdc",
    source="staging_workshift",
    keys=["WorkshiftID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["WorkshiftName", "Workshift", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workshift Details CDC
# MAGIC Converted from: stg.spDeltaSyncDimWorkshiftDetails (331 lines)

# COMMAND ----------

@dlt.table(
    name="staging_workshift_details",
    comment="Streaming staging table for WorkshiftDetails CDC changes",
    table_properties={"quality": "staging"}
)
def staging_workshift_details():
    """Capture CDC changes from ADLS for WorkshiftDetails dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_workshift_details/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "WorkshiftDetails"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="workshift_details_cdc",
    comment="WorkshiftDetails dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="workshift_details_cdc",
    source="staging_workshift_details",
    keys=["WorkshiftDetailsID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=[
        "WorkshiftID", "DayOfWeek", "StartTime", "EndTime",
        "IsActive", "DeletedAt"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Activity CDC
# MAGIC Converted from: stg.spDeltaSyncDimActivity

# COMMAND ----------

@dlt.table(
    name="staging_activity",
    comment="Streaming staging table for Activity CDC changes",
    table_properties={"quality": "staging"}
)
def staging_activity():
    """Capture CDC changes from ADLS for Activity dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_activity/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Activity"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="activity_cdc",
    comment="Activity dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="activity_cdc",
    source="staging_activity",
    keys=["ActivityID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["ActivityName", "Activity", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Location Group CDC
# MAGIC Converted from: stg.spDeltaSyncDimLocationGroup

# COMMAND ----------

@dlt.table(
    name="staging_location_group",
    comment="Streaming staging table for LocationGroup CDC changes",
    table_properties={"quality": "staging"}
)
def staging_location_group():
    """Capture CDC changes from ADLS for LocationGroup dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_location_group/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "LocationGroup"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="location_group_cdc",
    comment="LocationGroup dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="location_group_cdc",
    source="staging_location_group",
    keys=["LocationGroupID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["LocationGroupName", "LocationGroup", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Title CDC
# MAGIC Converted from: stg.spDeltaSyncDimTitle

# COMMAND ----------

@dlt.table(
    name="staging_title",
    comment="Streaming staging table for Title CDC changes",
    table_properties={"quality": "staging"}
)
def staging_title():
    """Capture CDC changes from ADLS for Title dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_title/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "Title"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="title_cdc",
    comment="Title dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="title_cdc",
    source="staging_title",
    keys=["TitleID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["TitleName", "Title", "ProjectID", "DeletedAt"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observation Source CDC
# MAGIC Converted from: stg.spDeltaSyncDimObservationSource

# COMMAND ----------

@dlt.table(
    name="staging_observation_source",
    comment="Streaming staging table for ObservationSource CDC changes",
    table_properties={"quality": "staging"}
)
def staging_observation_source():
    """Capture CDC changes from ADLS for ObservationSource dimension."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_observation_source/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("dimensions", "ObservationSource"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="observation_source_cdc",
    comment="ObservationSource dimension with SCD Type 2 history tracking"
)

dlt.apply_changes(
    target="observation_source_cdc",
    source="staging_observation_source",
    keys=["ObservationSourceID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["ObservationSourceName", "ObservationSource"]
)
