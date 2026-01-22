# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Gold Layer Views
# MAGIC
# MAGIC This notebook defines DLT views for the Gold layer.
# MAGIC Gold layer provides business-ready aggregated views and denormalized tables.
# MAGIC
# MAGIC **Converted Views (24):**
# MAGIC - dbo.vwCrewAssignments
# MAGIC - dbo.vwTradeAssignments
# MAGIC - dbo.vwWorkshiftAssignments
# MAGIC - dbo.vwDeviceAssignment_Continuous
# MAGIC - dbo.vwFactReportedAttendance
# MAGIC - dbo.vwFloor
# MAGIC - dbo.vwZone
# MAGIC - dbo.vwProject
# MAGIC - dbo.vwContactTracingRule
# MAGIC - dbo.vwWorkshiftDetails_DaysOfWeek
# MAGIC - dbo.vwWorkshiftDetails_SpecialDates
# MAGIC - dbo.vwLocationGroupAssignments
# MAGIC - Analytics views

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce, row_number,
    lead, lag, to_date, date_add, date_sub, datediff, dayofweek,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    min as spark_min, max as spark_max, first, last,
    concat, concat_ws, expr
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assignment Views
# MAGIC These views provide point-in-time assignment lookups with validity periods.

# COMMAND ----------

@dlt.view(
    name="gold_crew_assignments",
    comment="Active crew assignments with validity periods. Replaces dbo.vwCrewAssignments"
)
def gold_crew_assignments():
    """
    Crew assignments with ShiftLocalDate validity for point-in-time lookups.
    Includes computed ValidFrom_ShiftLocalDate and ValidTo_ShiftLocalDate.
    """
    crew_assignments = dlt.read("bronze_dbo_CrewAssignments")
    workers = dlt.read("bronze_dbo_Worker")

    window_spec = Window.partitionBy("ProjectID", "WorkerID").orderBy("ValidFrom")

    return (
        crew_assignments
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
        .withColumn("ValidTo_ShiftLocalDate",
            lead("ValidFrom", 1).over(window_spec))
        .withColumn("ValidFrom_ShiftLocalDate", col("ValidFrom"))
        .select(
            "CrewAssignmentID",
            "ProjectID",
            "WorkerID",
            "CrewID",
            "ValidFrom",
            "ValidTo",
            "ValidFrom_ShiftLocalDate",
            "ValidTo_ShiftLocalDate",
            "WatermarkUTC"
        )
    )


@dlt.view(
    name="gold_trade_assignments",
    comment="Active trade assignments with validity periods. Replaces dbo.vwTradeAssignments"
)
def gold_trade_assignments():
    """
    Trade (role) assignments with ShiftLocalDate validity.
    """
    trade_assignments = dlt.read("bronze_dbo_TradeAssignments")

    window_spec = Window.partitionBy("ProjectID", "WorkerID").orderBy("ValidFrom")

    return (
        trade_assignments
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
        .withColumn("ValidTo_ShiftLocalDate",
            lead("ValidFrom", 1).over(window_spec))
        .withColumn("ValidFrom_ShiftLocalDate", col("ValidFrom"))
        .select(
            "TradeAssignmentID",
            "ProjectID",
            "WorkerID",
            "TradeID",
            "ValidFrom",
            "ValidTo",
            "ValidFrom_ShiftLocalDate",
            "ValidTo_ShiftLocalDate",
            "WatermarkUTC"
        )
    )


@dlt.view(
    name="gold_workshift_assignments",
    comment="Active workshift assignments with validity periods. Replaces dbo.vwWorkshiftAssignments"
)
def gold_workshift_assignments():
    """
    Workshift assignments with ShiftLocalDate validity.
    """
    workshift_assignments = dlt.read("bronze_dbo_WorkshiftAssignments")

    window_spec = Window.partitionBy("ProjectID", "WorkerID").orderBy("ValidFrom")

    return (
        workshift_assignments
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
        .withColumn("ValidTo_ShiftLocalDate",
            lead("ValidFrom", 1).over(window_spec))
        .withColumn("ValidFrom_ShiftLocalDate", col("ValidFrom"))
        .select(
            "WorkshiftAssignmentID",
            "ProjectID",
            "WorkerID",
            "WorkshiftID",
            "ValidFrom",
            "ValidTo",
            "ValidFrom_ShiftLocalDate",
            "ValidTo_ShiftLocalDate",
            "WatermarkUTC"
        )
    )


@dlt.view(
    name="gold_device_assignment_continuous",
    comment="Continuous device assignments (gaps < 15 days merged). Replaces dbo.vwDeviceAssignment_Continuous"
)
def gold_device_assignment_continuous():
    """
    Device assignments with short gaps (<15 days) merged into continuous periods.
    Used for determining expected work days.
    """
    device_assignments = dlt.read("bronze_dbo_DeviceAssignments")

    # Get next assignment for gap detection
    window_spec = Window.partitionBy("ProjectID", "WorkerID").orderBy("ValidFrom")

    with_next = (
        device_assignments
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
        .withColumn("NextValidFrom",
            lead("ValidFrom", 1).over(window_spec))
        .withColumn("GapDays",
            datediff(col("NextValidFrom"), coalesce(col("ValidTo"), col("NextValidFrom"))))
        .withColumn("NewPeriod",
            when(
                (col("GapDays") > 15) | col("GapDays").isNull(),
                1
            ))
    )

    # Assign period groups
    with_groups = (
        with_next
        .withColumn("PeriodGroup",
            spark_sum("NewPeriod").over(
                window_spec.rowsBetween(Window.unboundedPreceding, 0)
            ))
    )

    # Aggregate continuous periods
    return (
        with_groups
        .groupBy("ProjectID", "WorkerID", "PeriodGroup")
        .agg(
            spark_min("ValidFrom").alias("ValidFrom"),
            spark_max(coalesce(col("ValidTo"), current_timestamp())).alias("ValidTo"),
            spark_max("WatermarkUTC").alias("WatermarkUTC")
        )
        .drop("PeriodGroup")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Views

# COMMAND ----------

@dlt.view(
    name="gold_project",
    comment="Active projects with organization info. Replaces dbo.vwProject"
)
def gold_project():
    """
    Projects with organization name and timezone.
    """
    projects = dlt.read("bronze_dbo_Project")
    organizations = dlt.read("bronze_dbo_Organization")

    return (
        projects.alias("p")
        .join(organizations.alias("o"), col("p.OrganizationID") == col("o.OrganizationID"), "left")
        .filter(col("p.DeletedAt").isNull())
        .select(
            col("p.ProjectID"),
            col("p.Project"),
            col("p.ProjectName"),
            col("p.OrganizationID"),
            col("o.Organization").alias("OrganizationName"),
            col("p.TimeZoneName"),
            col("p.IsActive"),
            col("p.ZoneBufferDistance"),
            col("p.ExtProjectID"),
            col("p.WatermarkUTC")
        )
    )


@dlt.view(
    name="gold_floor",
    comment="Floors with project info. Replaces dbo.vwFloor"
)
def gold_floor():
    """
    Floors with project and organization context.
    """
    floors = dlt.read("bronze_dbo_Floor")
    projects = dlt.read("gold_project")

    return (
        floors.alias("f")
        .join(projects.alias("p"), col("f.ProjectID") == col("p.ProjectID"), "left")
        .filter(col("f.DeletedAt").isNull())
        .select(
            col("f.FloorID"),
            col("f.Floor"),
            col("f.FloorName"),
            col("f.ProjectID"),
            col("p.Project"),
            col("p.OrganizationID"),
            col("f.FloorNumber"),
            col("f.ExtSpaceID"),
            col("f.WatermarkUTC")
        )
    )


@dlt.view(
    name="gold_zone",
    comment="Zones with floor and project info. Replaces dbo.vwZone"
)
def gold_zone():
    """
    Zones with floor, project, and organization context.
    """
    zones = dlt.read("bronze_dbo_Zone")
    floors = dlt.read("gold_floor")

    return (
        zones.alias("z")
        .join(floors.alias("f"), col("z.FloorID") == col("f.FloorID"), "left")
        .filter(col("z.DeletedAt").isNull())
        .select(
            col("z.ZoneID"),
            col("z.Zone"),
            col("z.ZoneName"),
            col("z.FloorID"),
            col("f.Floor"),
            col("f.ProjectID"),
            col("f.OrganizationID"),
            col("z.ProductiveClassID"),
            col("z.ExtSpaceID"),
            col("z.WatermarkUTC")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workshift Views

# COMMAND ----------

@dlt.view(
    name="gold_workshift_details_dow",
    comment="Workshift details by day of week. Replaces dbo.vwWorkshiftDetails_DaysOfWeek"
)
def gold_workshift_details_dow():
    """
    Workshift details for recurring schedules (by day of week).
    """
    workshift_details = dlt.read("bronze_dbo_WorkshiftDetails")
    workshifts = dlt.read("bronze_dbo_Workshift")

    return (
        workshift_details.alias("wsd")
        .filter(col("DayOfWeek").isNotNull())
        .filter((col("wsd.DeletedAt").isNull()))
        .join(workshifts.alias("ws"), col("wsd.WorkshiftID") == col("ws.WorkshiftID"), "left")
        .select(
            col("wsd.WorkshiftDetailsID"),
            col("wsd.WorkshiftID"),
            col("ws.Workshift"),
            col("ws.ProjectID"),
            col("wsd.DayOfWeek"),
            col("wsd.StartTime"),
            col("wsd.EndTime"),
            col("wsd.BreakDuration"),
            col("wsd.WatermarkUTC")
        )
    )


@dlt.view(
    name="gold_workshift_details_dates",
    comment="Workshift details for specific dates. Replaces dbo.vwWorkshiftDetails_SpecialDates"
)
def gold_workshift_details_dates():
    """
    Workshift details for specific dates (holidays, special schedules).
    """
    workshift_details = dlt.read("bronze_dbo_WorkshiftDetails")
    workshifts = dlt.read("bronze_dbo_Workshift")

    return (
        workshift_details.alias("wsd")
        .filter(col("Date").isNotNull())
        .filter((col("wsd.DeletedAt").isNull()))
        .join(workshifts.alias("ws"), col("wsd.WorkshiftID") == col("ws.WorkshiftID"), "left")
        .select(
            col("wsd.WorkshiftDetailsID"),
            col("wsd.WorkshiftID"),
            col("ws.Workshift"),
            col("ws.ProjectID"),
            col("wsd.Date"),
            col("wsd.StartTime"),
            col("wsd.EndTime"),
            col("wsd.BreakDuration"),
            col("wsd.WatermarkUTC")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Views

# COMMAND ----------

@dlt.view(
    name="gold_fact_reported_attendance",
    comment="Reported attendance with aggregated hours. Replaces dbo.vwFactReportedAttendance"
)
def gold_fact_reported_attendance():
    """
    Reported attendance combining timesheet and resource hours sources.
    """
    fact_reported_attendance = dlt.read("bronze_dbo_FactReportedAttendance")

    return (
        fact_reported_attendance
        .groupBy("ProjectID", "WorkerID", "ShiftLocalDate")
        .agg(
            spark_sum("ReportedTime").alias("ReportedTime"),
            spark_max("ManagerReportedAttendance").alias("ManagerReportedAttendance"),
            spark_max("CustomerReportedAttendance").alias("CustomerReportedAttendance"),
            spark_max("WatermarkUTC").alias("WatermarkUTC")
        )
    )


@dlt.view(
    name="gold_contact_tracing_rule",
    comment="Contact tracing rules with project info. Replaces dbo.vwContactTracingRule"
)
def gold_contact_tracing_rule():
    """
    Contact tracing rules with computed identifier.
    """
    rules = dlt.read("bronze_dbo_ContactTracingRule")
    projects = dlt.read("gold_project")

    return (
        rules.alias("ctr")
        .filter(col("Enabled") == True)
        .join(projects.alias("p"), col("ctr.ProjectID") == col("p.ProjectID"), "left")
        .withColumn("ContactTracingRule",
            concat_ws("_",
                col("p.Project"),
                col("ctr.Distance").cast("string"),
                col("ctr.DateFrom").cast("string")
            ))
        .select(
            col("ctr.ContactTracingRuleID"),
            col("ContactTracingRule"),
            col("ctr.ProjectID"),
            col("p.Project"),
            col("ctr.Distance"),
            col("ctr.DateFrom"),
            col("ctr.DateTo"),
            col("ctr.WorkerID"),
            col("ctr.CrewID"),
            col("ctr.TracePerFloor"),
            col("ctr.Enabled"),
            col("ctr.EvaluatedUTC"),
            col("ctr.WatermarkUTC")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Location Group Views

# COMMAND ----------

@dlt.view(
    name="gold_location_group_assignments",
    comment="Location group to zone/floor mappings. Replaces dbo.vwLocationGroupAssignments"
)
def gold_location_group_assignments():
    """
    Active location group assignments mapping location groups to zones/floors.
    """
    assignments = dlt.read("bronze_dbo_LocationGroupAssignments")

    return (
        assignments
        .filter(col("ValidTo").isNull())
        .select(
            "LocationGroupAssignmentID",
            "ProjectID",
            "LocationGroupID",
            "FloorID",
            "ZoneID",
            "ValidFrom",
            "ValidTo",
            "WatermarkUTC"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics Views

# COMMAND ----------

@dlt.view(
    name="gold_worker_daily_summary",
    comment="Daily summary metrics per worker"
)
def gold_worker_daily_summary():
    """
    Aggregated daily metrics for each worker combining shifts and attendance.
    """
    shifts = dlt.read("silver_fact_workers_shifts")
    attendance = dlt.read("gold_fact_reported_attendance")
    workers = dlt.read("bronze_dbo_Worker")
    projects = dlt.read("gold_project")

    return (
        shifts.alias("s")
        .join(
            attendance.alias("a"),
            (col("s.ProjectID") == col("a.ProjectID")) &
            (col("s.WorkerID") == col("a.WorkerID")) &
            (col("s.ShiftLocalDate") == col("a.ShiftLocalDate")),
            "full_outer"
        )
        .join(
            workers.alias("w"),
            coalesce(col("s.WorkerID"), col("a.WorkerID")) == col("w.WorkerID"),
            "left"
        )
        .join(
            projects.alias("p"),
            coalesce(col("s.ProjectID"), col("a.ProjectID")) == col("p.ProjectID"),
            "left"
        )
        .select(
            coalesce(col("s.ProjectID"), col("a.ProjectID")).alias("ProjectID"),
            col("p.ProjectName"),
            coalesce(col("s.WorkerID"), col("a.WorkerID")).alias("WorkerID"),
            col("w.WorkerName"),
            coalesce(col("s.ShiftLocalDate"), col("a.ShiftLocalDate")).alias("ShiftLocalDate"),
            col("s.ActiveTime"),
            col("s.ActiveTimeDuringShift"),
            col("s.InactiveTime"),
            col("s.Readings"),
            col("a.ReportedTime"),
            # Calculated metrics
            when(
                col("s.ActiveTime").isNotNull() & (col("s.ActiveTime") > 0),
                col("s.ActiveTimeDuringShift") / col("s.ActiveTime")
            ).alias("DuringShiftRatio"),
            when(
                col("a.ReportedTime").isNotNull() & (col("a.ReportedTime") > 0) &
                col("s.ActiveTime").isNotNull(),
                col("s.ActiveTime") / col("a.ReportedTime")
            ).alias("ActiveVsReportedRatio")
        )
    )


@dlt.view(
    name="gold_project_daily_summary",
    comment="Daily summary metrics per project"
)
def gold_project_daily_summary():
    """
    Aggregated daily metrics at project level.
    """
    worker_summary = dlt.read("gold_worker_daily_summary")

    return (
        worker_summary
        .groupBy("ProjectID", "ProjectName", "ShiftLocalDate")
        .agg(
            spark_count("WorkerID").alias("WorkerCount"),
            spark_sum("ActiveTime").alias("TotalActiveTime"),
            spark_sum("ActiveTimeDuringShift").alias("TotalActiveTimeDuringShift"),
            spark_sum("ReportedTime").alias("TotalReportedTime"),
            spark_avg("DuringShiftRatio").alias("AvgDuringShiftRatio"),
            spark_avg("ActiveVsReportedRatio").alias("AvgActiveVsReportedRatio"),
            spark_sum("Readings").alias("TotalReadings")
        )
    )


@dlt.view(
    name="gold_worker_productivity",
    comment="Worker productivity metrics with location classification"
)
def gold_worker_productivity():
    """
    Worker productivity analysis combining shift data with productive/assigned location time.
    """
    shifts = dlt.read("silver_fact_workers_shifts")
    workers = dlt.read("bronze_dbo_Worker")
    crews = dlt.read("bronze_dbo_Crew")
    trades = dlt.read("bronze_dbo_Trade")
    crew_assignments = dlt.read("gold_crew_assignments")
    trade_assignments = dlt.read("gold_trade_assignments")

    # Join shifts with current assignments
    with_assignments = (
        shifts.alias("s")
        .join(
            crew_assignments.alias("ca"),
            (col("s.ProjectID") == col("ca.ProjectID")) &
            (col("s.WorkerID") == col("ca.WorkerID")) &
            (col("s.ShiftLocalDate") >= col("ca.ValidFrom_ShiftLocalDate")) &
            ((col("s.ShiftLocalDate") < col("ca.ValidTo_ShiftLocalDate")) |
             col("ca.ValidTo_ShiftLocalDate").isNull()),
            "left"
        )
        .join(
            trade_assignments.alias("ta"),
            (col("s.ProjectID") == col("ta.ProjectID")) &
            (col("s.WorkerID") == col("ta.WorkerID")) &
            (col("s.ShiftLocalDate") >= col("ta.ValidFrom_ShiftLocalDate")) &
            ((col("s.ShiftLocalDate") < col("ta.ValidTo_ShiftLocalDate")) |
             col("ta.ValidTo_ShiftLocalDate").isNull()),
            "left"
        )
        .join(workers.alias("w"), col("s.WorkerID") == col("w.WorkerID"), "left")
        .join(crews.alias("c"), col("ca.CrewID") == col("c.CrewID"), "left")
        .join(trades.alias("t"), col("ta.TradeID") == col("t.TradeID"), "left")
    )

    return (
        with_assignments
        .select(
            col("s.ProjectID"),
            col("s.WorkerID"),
            col("w.WorkerName"),
            col("ca.CrewID"),
            col("c.Crew").alias("CrewName"),
            col("ta.TradeID"),
            col("t.Trade").alias("TradeName"),
            col("s.ShiftLocalDate"),
            col("s.ActiveTime"),
            col("s.ActiveTimeDuringShift"),
            col("s.ActiveTimeInDirectProductive"),
            col("s.ActiveTimeInIndirectProductive"),
            col("s.ActiveTimeInAssignedLocation"),
            # Productivity ratios
            when(
                col("s.ActiveTime") > 0,
                col("s.ActiveTimeInDirectProductive") / col("s.ActiveTime")
            ).alias("DirectProductiveRatio"),
            when(
                col("s.ActiveTime") > 0,
                (col("s.ActiveTimeInDirectProductive") + col("s.ActiveTimeInIndirectProductive")) / col("s.ActiveTime")
            ).alias("TotalProductiveRatio"),
            when(
                col("s.ActiveTime") > 0,
                col("s.ActiveTimeInAssignedLocation") / col("s.ActiveTime")
            ).alias("AssignedLocationRatio")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contact Tracing Views

# COMMAND ----------

@dlt.view(
    name="gold_contact_network",
    comment="Worker contact network for tracing analysis"
)
def gold_contact_network():
    """
    Contact network showing worker interactions.
    """
    contacts = dlt.read("bronze_dbo_FactWorkersContacts")
    workers = dlt.read("bronze_dbo_Worker")
    rules = dlt.read("gold_contact_tracing_rule")

    return (
        contacts.alias("c")
        .join(rules.alias("r"), col("c.ContactTracingRuleID") == col("r.ContactTracingRuleID"), "left")
        .join(
            workers.alias("w0").select(
                col("WorkerID"),
                col("WorkerName").alias("Worker0Name")
            ),
            col("c.WorkerID0") == col("w0.WorkerID"),
            "left"
        )
        .join(
            workers.alias("w1").select(
                col("WorkerID"),
                col("WorkerName").alias("WorkerName")
            ),
            col("c.WorkerID") == col("w1.WorkerID"),
            "left"
        )
        .select(
            col("c.ProjectID"),
            col("c.LocalDate"),
            col("c.ContactTracingRuleID"),
            col("r.ContactTracingRule"),
            col("r.Distance"),
            col("c.WorkerID0"),
            col("Worker0Name"),
            col("c.WorkerID"),
            col("WorkerName"),
            col("c.FloorID"),
            col("c.ZoneID"),
            col("c.Interactions"),
            col("c.InteractionDuration"),
            col("c.AvgDistanceMeters"),
            col("c.FirstInteractionUTC")
        )
    )
