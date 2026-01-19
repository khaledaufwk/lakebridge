# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Gold Layer
# MAGIC
# MAGIC Business views and aggregates for reporting and analytics.
# MAGIC Reads from Bronze and Silver layer tables.
# MAGIC
# MAGIC **Source:** Bronze/Silver tables
# MAGIC **Target:** wakecap_prod.migration (Gold views)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Views

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

    crew_assignments = (
        dlt.read("bronze_dbo_crewassignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    device_assignments = (
        dlt.read("bronze_dbo_deviceassignments")
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

# MAGIC %md
# MAGIC ## Assignment Views

# COMMAND ----------

@dlt.table(
    name="gold_crew_assignments",
    comment="Active crew assignments view"
)
def gold_crew_assignments():
    """Gold view: Active crew assignments with worker and crew details."""
    workers = dlt.read("silver_worker")
    crews = dlt.read("silver_crew")
    crew_assignments = (
        dlt.read("bronze_dbo_crewassignments")
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
    name="gold_device_assignments",
    comment="Active device assignments view"
)
def gold_device_assignments():
    """Gold view: Active device assignments with worker and device details."""
    workers = dlt.read("silver_worker")
    devices = dlt.read("silver_device")
    device_assignments = (
        dlt.read("bronze_dbo_deviceassignments")
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
    name="gold_workshift_assignments",
    comment="Active workshift assignments view"
)
def gold_workshift_assignments():
    """Gold view: Active workshift assignments with worker and shift details."""
    workers = dlt.read("silver_worker")
    workshifts = dlt.read("silver_workshift")
    workshift_assignments = (
        dlt.read("bronze_dbo_workshiftassignments")
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

# MAGIC %md
# MAGIC ## Reference Views

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

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Category | Count |
# MAGIC |----------|-------|
# MAGIC | Summary Views | 3 |
# MAGIC | Assignment Views | 3 |
# MAGIC | Reference Views | 1 |
# MAGIC | **Total Gold Views** | **7** |
