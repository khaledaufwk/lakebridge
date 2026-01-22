# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Silver Layer Facts
# MAGIC
# MAGIC This notebook defines DLT tables for the Silver layer fact tables with data quality expectations.
# MAGIC Silver layer cleanses and validates data from Bronze layer.
# MAGIC
# MAGIC **Data Quality Expectations:**
# MAGIC - Primary key validation
# MAGIC - Foreign key reference checks
# MAGIC - Range validations (e.g., ActiveTime >= 0)
# MAGIC - Business rule validations

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce, abs as spark_abs,
    to_date, to_timestamp, year, month, dayofweek
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Workers History

# COMMAND ----------

@dlt.table(
    name="silver_fact_workers_history",
    comment="Cleansed worker location history with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,WorkerID,LocalDate"
    },
    partition_cols=["LocalDate"]
)
@dlt.expect_or_drop("valid_timestamp", "TimestampUTC IS NOT NULL")
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect("valid_local_date", "LocalDate IS NOT NULL")
@dlt.expect("non_negative_active_time", "ActiveTime >= 0 OR ActiveTime IS NULL")
@dlt.expect("non_negative_inactive_time", "InactiveTime >= 0 OR InactiveTime IS NULL")
@dlt.expect_or_warn("valid_location", "Latitude IS NOT NULL AND Longitude IS NOT NULL")
@dlt.expect_or_warn("valid_latitude", "Latitude BETWEEN -90 AND 90 OR Latitude IS NULL")
@dlt.expect_or_warn("valid_longitude", "Longitude BETWEEN -180 AND 180 OR Longitude IS NULL")
def silver_fact_workers_history():
    """
    Cleansed worker location history records.
    Primary grain: ProjectID + WorkerID + TimestampUTC
    """
    fwh = dlt.read("bronze_dbo_FactWorkersHistory")
    workers = dlt.read("silver_worker")

    return (
        fwh.alias("f")
        .join(
            workers.alias("w").select("WorkerID", "ProjectID", "WorkerName"),
            (col("f.ProjectID") == col("w.ProjectID")) & (col("f.WorkerID") == col("w.WorkerID")),
            "left"
        )
        .select(
            col("f.TimestampUTC"),
            col("f.LocalDate"),
            col("f.ShiftLocalDate"),
            col("f.ProjectID"),
            col("f.WorkerID"),
            col("w.WorkerName"),
            col("f.FloorID"),
            col("f.ZoneID"),
            col("f.Latitude"),
            col("f.Longitude"),
            col("f.ErrorDistance"),
            col("f.ActiveTime"),
            col("f.InactiveTime"),
            col("f.ProductiveClassID"),
            col("f.LocationAssignmentClassID"),
            col("f.TimeCategoryID"),
            col("f.Sequence"),
            col("f.SequenceInactive"),
            col("f.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Workers Shifts

# COMMAND ----------

@dlt.table(
    name="silver_fact_workers_shifts",
    comment="Cleansed worker shift records with data quality",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,WorkerID,ShiftLocalDate"
    },
    partition_cols=["ShiftLocalDate"]
)
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_shift_date", "ShiftLocalDate IS NOT NULL")
@dlt.expect("non_negative_active_time", "ActiveTime >= 0 OR ActiveTime IS NULL")
@dlt.expect("non_negative_inactive_time", "InactiveTime >= 0 OR InactiveTime IS NULL")
@dlt.expect("non_negative_readings", "Readings >= 0 OR Readings IS NULL")
@dlt.expect_or_warn("valid_shift_duration", "FinishAtUTC >= StartAtUTC OR StartAtUTC IS NULL OR FinishAtUTC IS NULL")
@dlt.expect_or_warn("reasonable_active_time", "ActiveTime <= 1.5 OR ActiveTime IS NULL")  # Max ~36 hours as day fraction
def silver_fact_workers_shifts():
    """
    Cleansed worker shift records.
    Primary grain: ProjectID + WorkerID + StartAtUTC
    """
    shifts = dlt.read("bronze_dbo_FactWorkersShifts")
    workers = dlt.read("silver_worker")

    return (
        shifts.alias("s")
        .join(
            workers.alias("w").select("WorkerID", "ProjectID", "WorkerName"),
            (col("s.ProjectID") == col("w.ProjectID")) & (col("s.WorkerID") == col("w.WorkerID")),
            "left"
        )
        .select(
            col("s.ProjectID"),
            col("s.WorkerID"),
            col("w.WorkerName"),
            col("s.ShiftLocalDate"),
            col("s.StartAtUTC"),
            col("s.FinishAtUTC"),
            col("s.ActiveTime"),
            col("s.ActiveTimeBeforeShift"),
            col("s.ActiveTimeDuringShift"),
            col("s.ActiveTimeAfterShift"),
            col("s.FirstDirectProductiveUTC"),
            col("s.LastDirectProductiveUTC"),
            col("s.FirstAssignedLocationUTC"),
            col("s.LastAssignedLocationUTC"),
            col("s.Readings"),
            col("s.ReadingsMissedInactive"),
            col("s.ReadingsMissedInactiveDuringShift"),
            col("s.ReadingsMissedActive"),
            col("s.ReadingsMissedActiveDuringShift"),
            col("s.InactiveTime"),
            col("s.InactiveTimeDuringShift"),
            col("s.InactiveTimeBeforeShift"),
            col("s.InactiveTimeAfterShift"),
            col("s.ActiveTimeInDirectProductive"),
            col("s.ActiveTimeDuringShiftInDirectProductive"),
            col("s.ActiveTimeBeforeShiftInDirectProductive"),
            col("s.ActiveTimeAfterShiftInDirectProductive"),
            col("s.ActiveTimeInIndirectProductive"),
            col("s.ActiveTimeDuringShiftInIndirectProductive"),
            col("s.ActiveTimeBeforeShiftInIndirectProductive"),
            col("s.ActiveTimeAfterShiftInIndirectProductive"),
            col("s.InactiveTimeInDirectProductive"),
            col("s.InactiveTimeDuringShiftInDirectProductive"),
            col("s.InactiveTimeBeforeShiftInDirectProductive"),
            col("s.InactiveTimeAfterShiftInDirectProductive"),
            col("s.InactiveTimeInIndirectProductive"),
            col("s.InactiveTimeDuringShiftInIndirectProductive"),
            col("s.InactiveTimeBeforeShiftInIndirectProductive"),
            col("s.InactiveTimeAfterShiftInIndirectProductive"),
            col("s.ActiveTimeInAssignedLocation"),
            col("s.ActiveTimeDuringShiftInAssignedLocation"),
            col("s.ActiveTimeBeforeShiftInAssignedLocation"),
            col("s.ActiveTimeAfterShiftInAssignedLocation"),
            col("s.InactiveTimeInAssignedLocation"),
            col("s.InactiveTimeDuringShiftInAssignedLocation"),
            col("s.InactiveTimeBeforeShiftInAssignedLocation"),
            col("s.InactiveTimeAfterShiftInAssignedLocation"),
            col("s.ExtSourceID"),
            col("s.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Workers Shifts Combined

# COMMAND ----------

@dlt.table(
    name="silver_fact_workers_shifts_combined",
    comment="Cleansed combined shifts with attendance data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,WorkerID,ShiftLocalDate"
    },
    partition_cols=["ShiftLocalDate"]
)
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_shift_date", "ShiftLocalDate IS NOT NULL")
@dlt.expect("has_data", "StartAtUTC IS NOT NULL OR ReportedTime IS NOT NULL")
@dlt.expect("non_negative_reported_time", "ReportedTime >= 0 OR ReportedTime IS NULL")
def silver_fact_workers_shifts_combined():
    """
    Cleansed combined shift and attendance records.
    Includes records with shifts only, attendance only, or both.
    """
    combined = dlt.read("bronze_dbo_FactWorkersShiftsCombined")
    workers = dlt.read("silver_worker")
    crews = dlt.read("silver_crew")
    trades = dlt.read("silver_trade")

    return (
        combined.alias("c")
        .join(
            workers.alias("w").select("WorkerID", col("ProjectID").alias("w_ProjectID"), "WorkerName"),
            (col("c.ProjectID") == col("w.w_ProjectID")) & (col("c.WorkerID") == col("w.WorkerID")),
            "left"
        )
        .join(
            crews.alias("cr").select("CrewID", "Crew"),
            col("c.CrewID") == col("cr.CrewID"),
            "left"
        )
        .join(
            trades.alias("t").select("TradeID", "Trade"),
            col("c.TradeID") == col("t.TradeID"),
            "left"
        )
        .select(
            col("c.ProjectID"),
            col("c.WorkerID"),
            col("w.WorkerName"),
            col("c.ShiftLocalDate"),
            col("c.StartAtUTC"),
            col("c.FinishAtUTC"),
            col("c.CrewID"),
            col("cr.Crew").alias("CrewName"),
            col("c.TradeID"),
            col("t.Trade").alias("TradeName"),
            col("c.WorkshiftID"),
            col("c.WorkshiftDetailsID"),
            col("c.Segment"),
            col("c.ActiveTime"),
            col("c.ActiveTimeBeforeShift"),
            col("c.ActiveTimeDuringShift"),
            col("c.ActiveTimeAfterShift"),
            col("c.ReportedTime"),
            col("c.InventoryAssignedOn"),
            col("c.InventoryUnassignedOn"),
            col("c.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Reported Attendance

# COMMAND ----------

@dlt.table(
    name="silver_fact_reported_attendance",
    comment="Cleansed reported attendance records",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,WorkerID,ShiftLocalDate"
    },
    partition_cols=["ShiftLocalDate"]
)
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_shift_date", "ShiftLocalDate IS NOT NULL")
@dlt.expect("non_negative_reported_time", "ReportedTime >= 0 OR ReportedTime IS NULL")
@dlt.expect_or_warn("reasonable_reported_time", "ReportedTime <= 24 OR ReportedTime IS NULL")  # Max 24 hours per day
def silver_fact_reported_attendance():
    """
    Cleansed reported attendance records.
    """
    attendance = dlt.read("bronze_dbo_FactReportedAttendance")
    workers = dlt.read("silver_worker")

    return (
        attendance.alias("a")
        .join(
            workers.alias("w").select("WorkerID", col("ProjectID").alias("w_ProjectID"), "WorkerName"),
            (col("a.ProjectID") == col("w.w_ProjectID")) & (col("a.WorkerID") == col("w.WorkerID")),
            "left"
        )
        .select(
            col("a.ProjectID"),
            col("a.WorkerID"),
            col("w.WorkerName"),
            col("a.ShiftLocalDate"),
            col("a.ReportedTime"),
            col("a.SourceType"),
            col("a.ManagerReportedAttendance"),
            col("a.CustomerReportedAttendance"),
            col("a.ExtSourceID"),
            col("a.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Workers Contacts

# COMMAND ----------

@dlt.table(
    name="silver_fact_workers_contacts",
    comment="Cleansed worker contact records for tracing",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,LocalDate"
    },
    partition_cols=["LocalDate"]
)
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect_or_drop("valid_worker0_id", "WorkerID0 IS NOT NULL")
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_local_date", "LocalDate IS NOT NULL")
@dlt.expect("different_workers", "WorkerID0 != WorkerID")
@dlt.expect("non_negative_interactions", "Interactions >= 0 OR Interactions IS NULL")
@dlt.expect("non_negative_duration", "InteractionDuration >= 0 OR InteractionDuration IS NULL")
@dlt.expect_or_warn("valid_distance", "AvgDistanceMeters >= 0 OR AvgDistanceMeters IS NULL")
def silver_fact_workers_contacts():
    """
    Cleansed worker contact records for contact tracing.
    """
    contacts = dlt.read("bronze_dbo_FactWorkersContacts")
    workers = dlt.read("silver_worker")

    return (
        contacts.alias("c")
        .join(
            workers.alias("w0").select(
                col("WorkerID"),
                col("ProjectID").alias("w0_ProjectID"),
                col("WorkerName").alias("Worker0Name")
            ),
            (col("c.ProjectID") == col("w0.w0_ProjectID")) & (col("c.WorkerID0") == col("w0.WorkerID")),
            "left"
        )
        .join(
            workers.alias("w1").select(
                col("WorkerID"),
                col("ProjectID").alias("w1_ProjectID"),
                col("WorkerName")
            ),
            (col("c.ProjectID") == col("w1.w1_ProjectID")) & (col("c.WorkerID") == col("w1.WorkerID")),
            "left"
        )
        .select(
            col("c.ContactTracingRuleID"),
            col("c.ProjectID"),
            col("c.LocalDate"),
            col("c.WorkerID0"),
            col("w0.Worker0Name"),
            col("c.WorkerID"),
            col("w1.WorkerName"),
            col("c.FloorID"),
            col("c.ZoneID"),
            col("c.FirstInteractionUTC"),
            col("c.Interactions"),
            col("c.InteractionDuration"),
            col("c.AvgDistanceMeters"),
            col("c.ExtSourceID"),
            col("c.WatermarkUTC"),
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Observations

# COMMAND ----------

@dlt.table(
    name="silver_fact_observations",
    comment="Cleansed observation records",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,ObservationTime"
    },
    partition_cols=["ObservationDate"]
)
@dlt.expect_or_drop("valid_observation_id", "ObservationID IS NOT NULL")
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect("valid_observation_time", "ObservationTime IS NOT NULL")
def silver_fact_observations():
    """
    Cleansed observation records.
    """
    observations = dlt.read("bronze_dbo_FactObservations")

    return (
        observations
        .withColumn("ObservationDate", to_date(col("ObservationTime")))
        .select(
            "ObservationID",
            "ProjectID",
            "ObservationTime",
            "ObservationDate",
            "ObserverWorkerID",
            "FloorID",
            "ZoneID",
            "ActivityID",
            "WorkerCount",
            "Notes",
            "ExtSourceID",
            "WatermarkUTC",
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Progress

# COMMAND ----------

@dlt.table(
    name="silver_fact_progress",
    comment="Cleansed progress records",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,ProgressDate"
    },
    partition_cols=["ProgressDate"]
)
@dlt.expect_or_drop("valid_progress_id", "ProgressID IS NOT NULL")
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect("valid_progress_date", "ProgressDate IS NOT NULL")
@dlt.expect("non_negative_progress", "ProgressPercent >= 0 OR ProgressPercent IS NULL")
@dlt.expect_or_warn("valid_progress_range", "ProgressPercent <= 100 OR ProgressPercent IS NULL")
def silver_fact_progress():
    """
    Cleansed progress records.
    """
    progress = dlt.read("bronze_dbo_FactProgress")

    return (
        progress
        .select(
            "ProgressID",
            "ProjectID",
            "ProgressDate",
            "ActivityID",
            "FloorID",
            "ZoneID",
            "ProgressPercent",
            "Notes",
            "ExtSourceID",
            "WatermarkUTC",
            current_timestamp().alias("_silver_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Fact Weather Observations

# COMMAND ----------

@dlt.table(
    name="silver_fact_weather_observations",
    comment="Cleansed weather observation records",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "ProjectID,ObservationTime"
    },
    partition_cols=["ObservationDate"]
)
@dlt.expect_or_drop("valid_weather_id", "WeatherObservationID IS NOT NULL")
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect("valid_observation_time", "ObservationTime IS NOT NULL")
@dlt.expect_or_warn("valid_temperature", "Temperature BETWEEN -60 AND 60 OR Temperature IS NULL")
@dlt.expect_or_warn("valid_humidity", "Humidity BETWEEN 0 AND 100 OR Humidity IS NULL")
def silver_fact_weather_observations():
    """
    Cleansed weather observation records.
    """
    weather = dlt.read("bronze_dbo_FactWeatherObservations")

    return (
        weather
        .withColumn("ObservationDate", to_date(col("ObservationTime")))
        .select(
            "WeatherObservationID",
            "ProjectID",
            "ObservationTime",
            "ObservationDate",
            "Temperature",
            "Humidity",
            "WindSpeed",
            "WeatherCondition",
            "ExtSourceID",
            "WatermarkUTC",
            current_timestamp().alias("_silver_processed_at")
        )
    )
