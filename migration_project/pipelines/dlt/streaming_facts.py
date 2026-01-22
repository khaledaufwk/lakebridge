# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Streaming Fact Tables (CDC)
# MAGIC
# MAGIC This notebook defines DLT streaming tables with Change Data Capture (CDC)
# MAGIC for fact tables. Replaces spDeltaSyncFact* stored procedures.
# MAGIC
# MAGIC **Converted Procedures:**
# MAGIC - stg.spDeltaSyncFactObservations (1165 lines)
# MAGIC - stg.spDeltaSyncFactWorkersHistory (1561 lines)
# MAGIC - stg.spDeltaSyncFactWeatherObservations (481 lines)
# MAGIC - stg.spDeltaSyncFactProgress
# MAGIC - stg.spDeltaSyncFactReportedAttendance_ResourceTimesheet
# MAGIC - stg.spDeltaSyncFactReportedAttendance_ResourceHours
# MAGIC
# MAGIC **Pattern:** Uses DLT APPLY CHANGES for incremental fact loading

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, lit, to_date

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
# MAGIC ## Fact Observations CDC
# MAGIC Converted from: stg.spDeltaSyncFactObservations (1165 lines)
# MAGIC
# MAGIC Original patterns: TEMP_TABLE, MERGE, WINDOW_FUNCTION, CTE

# COMMAND ----------

@dlt.table(
    name="staging_observations",
    comment="Streaming staging table for FactObservations CDC changes",
    table_properties={"quality": "staging"}
)
def staging_observations():
    """
    Capture CDC changes from ADLS for FactObservations.

    Note: This is the largest fact table (~50M rows).
    Use maxFilesPerTrigger to control ingestion rate.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_observations/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", "100")  # Control batch size
        .load(get_adls_path("facts", "FactObservations"))
        .withColumn("_load_time", current_timestamp())
        .withColumn("_observation_date", to_date(col("ObservationTime")))
    )

dlt.create_streaming_table(
    name="observations_cdc",
    comment="FactObservations with incremental CDC updates",
    partition_cols=["_observation_date"]  # Partition by date for query performance
)

dlt.apply_changes(
    target="observations_cdc",
    source="staging_observations",
    keys=["ObservationID"],
    sequence_by="_load_time",
    stored_as_scd_type=1  # SCD Type 1 for facts - just update
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Workers History CDC
# MAGIC Converted from: stg.spDeltaSyncFactWorkersHistory (1561 lines)
# MAGIC
# MAGIC Original patterns: TEMP_TABLE, SPATIAL, WINDOW_FUNCTION, CTE

# COMMAND ----------

@dlt.table(
    name="staging_workers_history",
    comment="Streaming staging table for FactWorkersHistory CDC changes",
    table_properties={"quality": "staging"}
)
def staging_workers_history():
    """
    Capture CDC changes from ADLS for FactWorkersHistory.

    Note: SPATIAL columns (LocationID based on geography) are pre-computed
    during ADF extraction or require spatial UDFs for runtime calculation.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_workers_history/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", "100")
        .load(get_adls_path("facts", "FactWorkersHistory"))
        .withColumn("_load_time", current_timestamp())
        .withColumn("_record_date", to_date(col("RecordTime")))
    )

dlt.create_streaming_table(
    name="workers_history_cdc",
    comment="FactWorkersHistory with incremental CDC updates",
    partition_cols=["_record_date"]
)

dlt.apply_changes(
    target="workers_history_cdc",
    source="staging_workers_history",
    keys=["WorkersHistoryID"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Weather Observations CDC
# MAGIC Converted from: stg.spDeltaSyncFactWeatherObservations (481 lines)
# MAGIC
# MAGIC Original patterns: TEMP_TABLE, MERGE, WINDOW_FUNCTION, CTE

# COMMAND ----------

@dlt.table(
    name="staging_weather_observations",
    comment="Streaming staging table for FactWeatherObservations CDC changes",
    table_properties={"quality": "staging"}
)
def staging_weather_observations():
    """Capture CDC changes from ADLS for FactWeatherObservations."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_weather_observations/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("facts", "FactWeatherObservations"))
        .withColumn("_load_time", current_timestamp())
        .withColumn("_observation_date", to_date(col("ObservationTime")))
    )

dlt.create_streaming_table(
    name="weather_observations_cdc",
    comment="FactWeatherObservations with incremental CDC updates",
    partition_cols=["_observation_date"]
)

dlt.apply_changes(
    target="weather_observations_cdc",
    source="staging_weather_observations",
    keys=["WeatherObservationID"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Progress CDC
# MAGIC Converted from: stg.spDeltaSyncFactProgress

# COMMAND ----------

@dlt.table(
    name="staging_progress",
    comment="Streaming staging table for FactProgress CDC changes",
    table_properties={"quality": "staging"}
)
def staging_progress():
    """Capture CDC changes from ADLS for FactProgress."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_progress/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("facts", "FactProgress"))
        .withColumn("_load_time", current_timestamp())
        .withColumn("_progress_date", to_date(col("ProgressDate")))
    )

dlt.create_streaming_table(
    name="progress_cdc",
    comment="FactProgress with incremental CDC updates",
    partition_cols=["_progress_date"]
)

dlt.apply_changes(
    target="progress_cdc",
    source="staging_progress",
    keys=["ProgressID"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Reported Attendance - Timesheet CDC
# MAGIC Converted from: stg.spDeltaSyncFactReportedAttendance_ResourceTimesheet

# COMMAND ----------

@dlt.table(
    name="staging_reported_attendance_timesheet",
    comment="Streaming staging for FactReportedAttendance from timesheet source",
    table_properties={"quality": "staging"}
)
def staging_reported_attendance_timesheet():
    """Capture CDC changes from ADLS for FactReportedAttendance (timesheet source)."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_attendance_timesheet/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("facts", "FactReportedAttendance"))
        .filter(col("SourceType") == "TIMESHEET")
        .withColumn("_load_time", current_timestamp())
        .withColumn("_report_date", to_date(col("ReportDate")))
    )

dlt.create_streaming_table(
    name="reported_attendance_timesheet_cdc",
    comment="FactReportedAttendance (timesheet) with incremental CDC updates",
    partition_cols=["_report_date"]
)

dlt.apply_changes(
    target="reported_attendance_timesheet_cdc",
    source="staging_reported_attendance_timesheet",
    keys=["ReportedAttendanceID"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Reported Attendance - Resource Hours CDC
# MAGIC Converted from: stg.spDeltaSyncFactReportedAttendance_ResourceHours

# COMMAND ----------

@dlt.table(
    name="staging_reported_attendance_hours",
    comment="Streaming staging for FactReportedAttendance from resource hours source",
    table_properties={"quality": "staging"}
)
def staging_reported_attendance_hours():
    """Capture CDC changes from ADLS for FactReportedAttendance (resource hours source)."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_attendance_hours/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("facts", "FactReportedAttendance"))
        .filter(col("SourceType") == "RESOURCE_HOURS")
        .withColumn("_load_time", current_timestamp())
        .withColumn("_report_date", to_date(col("ReportDate")))
    )

dlt.create_streaming_table(
    name="reported_attendance_hours_cdc",
    comment="FactReportedAttendance (resource hours) with incremental CDC updates",
    partition_cols=["_report_date"]
)

dlt.apply_changes(
    target="reported_attendance_hours_cdc",
    source="staging_reported_attendance_hours",
    keys=["ReportedAttendanceID"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Workers Shifts CDC

# COMMAND ----------

@dlt.table(
    name="staging_workers_shifts",
    comment="Streaming staging table for FactWorkersShifts CDC changes",
    table_properties={"quality": "staging"}
)
def staging_workers_shifts():
    """Capture CDC changes from ADLS for FactWorkersShifts."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_workers_shifts/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("facts", "FactWorkersShifts"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    name="workers_shifts_cdc",
    comment="FactWorkersShifts with incremental CDC updates",
    partition_cols=["ShiftLocalDate"]
)

dlt.apply_changes(
    target="workers_shifts_cdc",
    source="staging_workers_shifts",
    keys=["WorkerID", "ProjectID", "ShiftLocalDate"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Workers Contacts CDC

# COMMAND ----------

@dlt.table(
    name="staging_workers_contacts",
    comment="Streaming staging table for FactWorkersContacts CDC changes",
    table_properties={"quality": "staging"}
)
def staging_workers_contacts():
    """Capture CDC changes from ADLS for FactWorkersContacts."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_workers_contacts/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", "100")  # Large table
        .load(get_adls_path("facts", "FactWorkersContacts"))
        .withColumn("_load_time", current_timestamp())
        .withColumn("_contact_date", to_date(col("ContactTime")))
    )

dlt.create_streaming_table(
    name="workers_contacts_cdc",
    comment="FactWorkersContacts with incremental CDC updates",
    partition_cols=["_contact_date"]
)

dlt.apply_changes(
    target="workers_contacts_cdc",
    source="staging_workers_contacts",
    keys=["WorkersContactID"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Workers Tasks CDC

# COMMAND ----------

@dlt.table(
    name="staging_workers_tasks",
    comment="Streaming staging table for FactWorkersTasks CDC changes",
    table_properties={"quality": "staging"}
)
def staging_workers_tasks():
    """Capture CDC changes from ADLS for FactWorkersTasks."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/staging_workers_tasks/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(get_adls_path("facts", "FactWorkersTasks"))
        .withColumn("_load_time", current_timestamp())
        .withColumn("_task_date", to_date(col("TaskDate")))
    )

dlt.create_streaming_table(
    name="workers_tasks_cdc",
    comment="FactWorkersTasks with incremental CDC updates",
    partition_cols=["_task_date"]
)

dlt.apply_changes(
    target="workers_tasks_cdc",
    source="staging_workers_tasks",
    keys=["WorkersTaskID"],
    sequence_by="_load_time",
    stored_as_scd_type=1
)
