# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Batch Calculation Tables
# MAGIC
# MAGIC This notebook defines DLT batch tables for calculation procedures
# MAGIC that don't require CURSOR or complex DYNAMIC SQL.
# MAGIC
# MAGIC **Converted Procedures:**
# MAGIC - stg.spCalculateFactReportedAttendance (215 lines)
# MAGIC - stg.spCalculateFactProgress (112 lines)
# MAGIC - stg.spCalculateProjectActiveFlag (36 lines)
# MAGIC - stg.spCalculateLocationGroupAssignments (153 lines)
# MAGIC - stg.spCalculateManagerAssignments (151 lines)
# MAGIC - stg.spCalculateManagerAssignmentSnapshots (527 lines)
# MAGIC - stg.spCalculateCrewAssignments
# MAGIC - stg.spCalculateDeviceAssignments
# MAGIC - stg.spCalculateTradeAssignments
# MAGIC - stg.spCalculateWorkshiftAssignments

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, coalesce, sum, count, avg, min, max,
    countDistinct, datediff, to_date, expr, row_number, lead, lag
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Reported Attendance
# MAGIC Converted from: stg.spCalculateFactReportedAttendance (215 lines)
# MAGIC
# MAGIC Original patterns: MERGE, CTE

# COMMAND ----------

@dlt.table(
    name="calc_reported_attendance",
    comment="Calculated reported attendance aggregates. Converted from stg.spCalculateFactReportedAttendance"
)
def calc_reported_attendance():
    """
    Calculate reported attendance aggregates from timesheet data.

    Aggregates attendance hours by worker, project, and date.
    """
    # Read source data
    attendance = dlt.read("bronze_dbo_FactReportedAttendance")
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    projects = dlt.read("bronze_dbo_Project").filter(col("DeletedAt").isNull())

    return (
        attendance
        .filter(col("IsDeleted") != True)
        .join(
            workers.select("WorkerID", "WorkerName", "Worker"),
            "WorkerID",
            "inner"
        )
        .join(
            projects.select("ProjectID", "ProjectName", "Project"),
            "ProjectID",
            "inner"
        )
        .groupBy(
            "ProjectID",
            "Project",
            "ProjectName",
            "WorkerID",
            "Worker",
            "WorkerName",
            "ReportDate"
        )
        .agg(
            sum("ReportedHours").alias("TotalReportedHours"),
            sum(when(col("IsOvertime") == True, col("ReportedHours")).otherwise(0)).alias("TotalOvertimeHours"),
            sum(when(col("IsOvertime") != True, col("ReportedHours")).otherwise(0)).alias("TotalRegularHours"),
            count("*").alias("EntryCount"),
            min("ReportedStartTime").alias("FirstReportedStart"),
            max("ReportedEndTime").alias("LastReportedEnd")
        )
        .withColumn("_calculated_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Progress
# MAGIC Converted from: stg.spCalculateFactProgress (112 lines)
# MAGIC
# MAGIC Original patterns: MERGE, CTE

# COMMAND ----------

@dlt.table(
    name="calc_progress",
    comment="Calculated progress aggregates. Converted from stg.spCalculateFactProgress"
)
def calc_progress():
    """
    Calculate task progress aggregates.

    Aggregates progress by project, task, and date.
    """
    progress = dlt.read("bronze_dbo_FactProgress")
    tasks = dlt.read("bronze_dbo_Task").filter(col("DeletedAt").isNull())
    projects = dlt.read("bronze_dbo_Project").filter(col("DeletedAt").isNull())

    return (
        progress
        .join(
            tasks.select("TaskID", "TaskName", "Task"),
            "TaskID",
            "left"
        )
        .join(
            projects.select("ProjectID", "ProjectName", "Project"),
            "ProjectID",
            "inner"
        )
        .groupBy(
            "ProjectID",
            "Project",
            "ProjectName",
            "TaskID",
            "Task",
            "TaskName",
            "ProgressDate"
        )
        .agg(
            sum("ProgressValue").alias("TotalProgress"),
            sum("PlannedValue").alias("TotalPlanned"),
            count("*").alias("EntryCount"),
            avg("ProgressValue").alias("AvgProgress")
        )
        .withColumn(
            "ProgressPercentage",
            when(col("TotalPlanned") > 0,
                 (col("TotalProgress") / col("TotalPlanned") * 100)
            ).otherwise(0)
        )
        .withColumn("_calculated_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Project Active Flag
# MAGIC Converted from: stg.spCalculateProjectActiveFlag (36 lines)
# MAGIC
# MAGIC Original patterns: CTE

# COMMAND ----------

@dlt.table(
    name="calc_project_active",
    comment="Calculated project active flag. Converted from stg.spCalculateProjectActiveFlag"
)
def calc_project_active():
    """
    Calculate project active status based on recent activity.

    A project is active if it has observations or attendance in the last 30 days.
    """
    projects = dlt.read("bronze_dbo_Project").filter(col("DeletedAt").isNull())
    observations = dlt.read("bronze_dbo_FactObservations")
    attendance = dlt.read("bronze_dbo_FactReportedAttendance")

    # Get latest observation date per project
    latest_obs = (
        observations
        .groupBy("ProjectID")
        .agg(max("ObservationTime").alias("LatestObservation"))
    )

    # Get latest attendance date per project
    latest_att = (
        attendance
        .groupBy("ProjectID")
        .agg(max("ReportDate").alias("LatestAttendance"))
    )

    return (
        projects
        .join(latest_obs, "ProjectID", "left")
        .join(latest_att, "ProjectID", "left")
        .withColumn(
            "LatestActivity",
            coalesce(
                when(col("LatestObservation") > col("LatestAttendance"), col("LatestObservation"))
                    .otherwise(col("LatestAttendance")),
                col("LatestObservation"),
                col("LatestAttendance")
            )
        )
        .withColumn(
            "DaysSinceActivity",
            when(col("LatestActivity").isNotNull(),
                 datediff(current_timestamp(), col("LatestActivity"))
            ).otherwise(9999)
        )
        .withColumn(
            "IsActiveCalculated",
            col("DaysSinceActivity") <= 30
        )
        .select(
            "ProjectID",
            "Project",
            "ProjectName",
            "IsActive",
            "IsActiveCalculated",
            "LatestActivity",
            "DaysSinceActivity",
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Location Group Assignments
# MAGIC Converted from: stg.spCalculateLocationGroupAssignments (153 lines)
# MAGIC
# MAGIC Original patterns: TEMP_TABLE, MERGE

# COMMAND ----------

@dlt.table(
    name="calc_location_group_assignments",
    comment="Calculated location group assignments. Converted from stg.spCalculateLocationGroupAssignments"
)
def calc_location_group_assignments():
    """
    Calculate active location group assignments.

    Joins workers with their location group assignments and validates.
    """
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    location_groups = dlt.read("bronze_dbo_LocationGroup").filter(col("DeletedAt").isNull())
    assignments = dlt.read("bronze_dbo_LocationGroupAssignments")

    # Get active assignments
    active_assignments = (
        assignments
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        active_assignments
        .join(
            workers.select("WorkerID", "WorkerName", "Worker", "ProjectID"),
            "WorkerID",
            "inner"
        )
        .join(
            location_groups.select(
                "LocationGroupID",
                "LocationGroupName",
                "LocationGroup",
                col("ProjectID").alias("LG_ProjectID")
            ),
            "LocationGroupID",
            "inner"
        )
        # Validate project matches
        .filter(col("ProjectID") == col("LG_ProjectID"))
        .select(
            "WorkerID",
            "Worker",
            "WorkerName",
            "ProjectID",
            "LocationGroupID",
            "LocationGroup",
            "LocationGroupName",
            "ValidFrom",
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Manager Assignments
# MAGIC Converted from: stg.spCalculateManagerAssignments (151 lines)
# MAGIC
# MAGIC Original patterns: TEMP_TABLE, MERGE

# COMMAND ----------

@dlt.table(
    name="calc_manager_assignments",
    comment="Calculated manager assignments. Converted from stg.spCalculateManagerAssignments"
)
def calc_manager_assignments():
    """
    Calculate active manager assignments with manager details.
    """
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    assignments = dlt.read("bronze_dbo_ManagerAssignments")

    # Get active assignments
    active_assignments = (
        assignments
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    # Join with worker for employee details
    worker_details = workers.select(
        col("WorkerID"),
        col("Worker"),
        col("WorkerName"),
        col("ProjectID")
    )

    # Join with worker for manager details
    manager_details = workers.select(
        col("WorkerID").alias("ManagerWorkerID"),
        col("Worker").alias("ManagerCode"),
        col("WorkerName").alias("ManagerName")
    )

    return (
        active_assignments
        .join(worker_details, "WorkerID", "inner")
        .join(manager_details, "ManagerWorkerID", "left")
        .select(
            "WorkerID",
            "Worker",
            "WorkerName",
            "ProjectID",
            "ManagerWorkerID",
            "ManagerCode",
            "ManagerName",
            "ValidFrom",
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Manager Assignment Snapshots
# MAGIC Converted from: stg.spCalculateManagerAssignmentSnapshots (527 lines)
# MAGIC
# MAGIC Original patterns: TEMP_TABLE, MERGE, PIVOT

# COMMAND ----------

@dlt.table(
    name="calc_manager_snapshots",
    comment="Calculated manager assignment snapshots. Converted from stg.spCalculateManagerAssignmentSnapshots"
)
def calc_manager_snapshots():
    """
    Calculate manager assignment snapshots for point-in-time reporting.

    Creates a snapshot of manager relationships for each worker at regular intervals.
    """
    assignments = dlt.read("bronze_dbo_ManagerAssignments")
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())

    # Window to detect changes
    change_window = Window.partitionBy("WorkerID", "ProjectID").orderBy("ValidFrom")

    # Get all assignment changes with previous values
    assignment_history = (
        assignments
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
        .withColumn("PrevManagerID", lag("ManagerWorkerID").over(change_window))
        .withColumn("IsChange", col("ManagerWorkerID") != col("PrevManagerID"))
    )

    # Join with worker names
    manager_names = workers.select(
        col("WorkerID").alias("ManagerWorkerID"),
        col("WorkerName").alias("ManagerName")
    )

    return (
        assignment_history
        .join(manager_names, "ManagerWorkerID", "left")
        .select(
            "WorkerID",
            "ProjectID",
            "ManagerWorkerID",
            "ManagerName",
            "ValidFrom",
            "ValidTo",
            "IsChange",
            current_timestamp().alias("_snapshot_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Crew Assignments
# MAGIC Converted from: stg.spCalculateCrewAssignments

# COMMAND ----------

@dlt.table(
    name="calc_crew_assignments",
    comment="Calculated active crew assignments. Converted from stg.spCalculateCrewAssignments"
)
def calc_crew_assignments():
    """Calculate active crew assignments with crew details."""
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    crews = dlt.read("bronze_dbo_Crew").filter(col("DeletedAt").isNull())
    assignments = dlt.read("bronze_dbo_CrewAssignments")

    active_assignments = (
        assignments
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        active_assignments
        .join(
            workers.select("WorkerID", "WorkerName", "Worker"),
            "WorkerID",
            "inner"
        )
        .join(
            crews.select("CrewID", "CrewName", "Crew", "ProjectID"),
            "CrewID",
            "inner"
        )
        .select(
            "WorkerID",
            "Worker",
            "WorkerName",
            "CrewID",
            "Crew",
            "CrewName",
            "ProjectID",
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Device Assignments
# MAGIC Converted from: stg.spCalculateDeviceAssignments

# COMMAND ----------

@dlt.table(
    name="calc_device_assignments",
    comment="Calculated active device assignments. Converted from stg.spCalculateDeviceAssignments"
)
def calc_device_assignments():
    """Calculate active device assignments with device details."""
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    devices = dlt.read("bronze_dbo_Device").filter(col("DeletedAt").isNull())
    assignments = dlt.read("bronze_dbo_DeviceAssignments")

    active_assignments = (
        assignments
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        active_assignments
        .join(
            workers.select("WorkerID", "WorkerName", "Worker", "ProjectID"),
            "WorkerID",
            "inner"
        )
        .join(
            devices.select("DeviceID", "DeviceName", "Device"),
            "DeviceID",
            "inner"
        )
        .select(
            "WorkerID",
            "Worker",
            "WorkerName",
            "ProjectID",
            "DeviceID",
            "Device",
            "DeviceName",
            "ValidFrom",
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Trade Assignments
# MAGIC Converted from: stg.spCalculateTradeAssignments

# COMMAND ----------

@dlt.table(
    name="calc_trade_assignments",
    comment="Calculated active trade assignments. Converted from stg.spCalculateTradeAssignments"
)
def calc_trade_assignments():
    """Calculate active trade assignments with trade details."""
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    trades = dlt.read("bronze_dbo_Trade").filter(col("DeletedAt").isNull())
    assignments = dlt.read("bronze_dbo_TradeAssignments")

    active_assignments = (
        assignments
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        active_assignments
        .join(
            workers.select("WorkerID", "WorkerName", "Worker"),
            "WorkerID",
            "inner"
        )
        .join(
            trades.select("TradeID", "TradeName", "Trade", "ProjectID"),
            "TradeID",
            "inner"
        )
        .select(
            "WorkerID",
            "Worker",
            "WorkerName",
            "TradeID",
            "Trade",
            "TradeName",
            "ProjectID",
            "ValidFrom",
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated Workshift Assignments
# MAGIC Converted from: stg.spCalculateWorkshiftAssignments

# COMMAND ----------

@dlt.table(
    name="calc_workshift_assignments",
    comment="Calculated active workshift assignments. Converted from stg.spCalculateWorkshiftAssignments"
)
def calc_workshift_assignments():
    """Calculate active workshift assignments with shift details."""
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    workshifts = dlt.read("bronze_dbo_Workshift").filter(col("DeletedAt").isNull())
    assignments = dlt.read("bronze_dbo_WorkshiftAssignments")

    active_assignments = (
        assignments
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
    )

    return (
        active_assignments
        .join(
            workers.select("WorkerID", "WorkerName", "Worker"),
            "WorkerID",
            "inner"
        )
        .join(
            workshifts.select("WorkshiftID", "WorkshiftName", "Workshift", "ProjectID"),
            "WorkshiftID",
            "inner"
        )
        .select(
            "WorkerID",
            "Worker",
            "WorkerName",
            "WorkshiftID",
            "Workshift",
            "WorkshiftName",
            "ProjectID",
            "ValidFrom",
            current_timestamp().alias("_calculated_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worker Assignment Summary
# MAGIC Combined view of all worker assignments

# COMMAND ----------

@dlt.table(
    name="calc_worker_assignments_summary",
    comment="Combined summary of all worker assignments"
)
def calc_worker_assignments_summary():
    """
    Combined summary of all active assignments for each worker.

    Joins crew, device, trade, workshift, and manager assignments.
    """
    workers = dlt.read("bronze_dbo_Worker").filter(col("DeletedAt").isNull())
    crew_assign = dlt.read("calc_crew_assignments")
    device_assign = dlt.read("calc_device_assignments")
    trade_assign = dlt.read("calc_trade_assignments")
    workshift_assign = dlt.read("calc_workshift_assignments")
    manager_assign = dlt.read("calc_manager_assignments")

    return (
        workers
        .select("WorkerID", "Worker", "WorkerName", "ProjectID")
        .join(
            crew_assign.select(
                "WorkerID",
                col("CrewID"),
                col("Crew").alias("CrewCode"),
                col("CrewName")
            ),
            "WorkerID",
            "left"
        )
        .join(
            device_assign.select(
                "WorkerID",
                col("DeviceID"),
                col("Device").alias("DeviceCode"),
                col("DeviceName")
            ),
            "WorkerID",
            "left"
        )
        .join(
            trade_assign.select(
                "WorkerID",
                col("TradeID"),
                col("Trade").alias("TradeCode"),
                col("TradeName")
            ),
            "WorkerID",
            "left"
        )
        .join(
            workshift_assign.select(
                "WorkerID",
                col("WorkshiftID"),
                col("Workshift").alias("WorkshiftCode"),
                col("WorkshiftName")
            ),
            "WorkerID",
            "left"
        )
        .join(
            manager_assign.select(
                "WorkerID",
                col("ManagerWorkerID"),
                col("ManagerCode"),
                col("ManagerName")
            ),
            "WorkerID",
            "left"
        )
        .withColumn("_calculated_at", current_timestamp())
    )
