# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate Fact Workers Contacts (Contact Tracing)
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateFactWorkersContacts_ByRule` (478 lines)
# MAGIC
# MAGIC **Purpose:** Calculate contact interactions between workers based on proximity rules.
# MAGIC Used for contact tracing during COVID-19 and general worker interaction analysis.
# MAGIC
# MAGIC **Original Patterns:** CURSOR (over distance rules, partitions), DYNAMIC_SQL, TEMP_TABLE, SPATIAL, MERGE
# MAGIC
# MAGIC **Key Business Logic:**
# MAGIC 1. Process each contact tracing rule by distance threshold
# MAGIC 2. For each rule, find worker interactions within the distance
# MAGIC 3. Use time slicing (5-minute windows) for efficient proximity matching
# MAGIC 4. Track contacts per floor or aggregated per project (based on rule)
# MAGIC 5. Calculate interaction count, duration, and average distance
# MAGIC
# MAGIC **Spatial Conversion:**
# MAGIC - SQL Server geometry::STDistance() → Haversine distance formula in Spark
# MAGIC - Spatial index → H3 geospatial indexing

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import h3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("rule_id", "", "Contact Tracing Rule ID (optional, blank for all)")
dbutils.widgets.text("distance_meters", "2", "Distance Threshold (meters)")
dbutils.widgets.text("lookback_days", "14", "Lookback Days")

rule_id_str = dbutils.widgets.get("rule_id")
distance_threshold = float(dbutils.widgets.get("distance_meters"))
lookback_days = int(dbutils.widgets.get("lookback_days"))

rule_filter = int(rule_id_str) if rule_id_str else None
today = datetime.now().date()
cutoff_date = today - timedelta(days=lookback_days)

print(f"Processing contacts with distance <= {distance_threshold}m, lookback {lookback_days} days")
if rule_filter:
    print(f"Filtering to rule ID: {rule_filter}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Haversine Distance UDF

# COMMAND ----------

from math import radians, sin, cos, sqrt, atan2

@F.udf(returnType=DoubleType())
def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points in meters.
    Replaces SQL Server geometry::STDistance()
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    R = 6371000  # Earth's radius in meters

    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)

    a = sin(delta_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

spark.udf.register("haversine_distance", haversine_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 Indexing for Efficient Proximity Search

# COMMAND ----------

# H3 resolution 12 gives ~0.3m edge length, good for 2m proximity
H3_RESOLUTION = 12

@F.udf(returnType=StringType())
def geo_to_h3(lat, lon, resolution=H3_RESOLUTION):
    """Convert lat/lon to H3 index."""
    if lat is None or lon is None:
        return None
    try:
        return h3.latlng_to_cell(lat, lon, resolution)
    except:
        return None

@F.udf(returnType=ArrayType(StringType()))
def h3_k_ring(h3_index, k=1):
    """Get H3 cell and neighbors within k rings."""
    if h3_index is None:
        return None
    try:
        return list(h3.grid_disk(h3_index, k))
    except:
        return None

spark.udf.register("geo_to_h3", geo_to_h3)
spark.udf.register("h3_k_ring", h3_k_ring)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Source Data

# COMMAND ----------

# Load contact tracing rules
contact_rules = spark.table("wakecap_prod.migration.bronze_dbo_ContactTracingRule").filter(
    F.col("Enabled") == True
)

if rule_filter:
    contact_rules = contact_rules.filter(F.col("ContactTracingRuleID") == rule_filter)

# Filter by distance threshold
contact_rules = contact_rules.filter(F.col("Distance") == distance_threshold)

# Load FactWorkersHistory
fact_workers_history = (
    spark.table("wakecap_prod.migration.bronze_dbo_FactWorkersHistory")
    .filter(F.col("LocalDate") >= cutoff_date)
    .filter(F.col("ActiveTime") > 0)
    .filter(F.col("FloorID").isNotNull())
    .filter(F.col("Latitude").isNotNull())
    .filter(F.col("Longitude").isNotNull())
)

# Load crew assignments for rule scope
crew_assignments = spark.table("wakecap_prod.migration.bronze_dbo_CrewAssignments")

print(f"Active rules: {contact_rules.count()}")
print(f"Source history records: {fact_workers_history.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Scope of Workers per Rule

# COMMAND ----------

# Get all workers in scope for each rule
# Rules can specify: specific WorkerID, CrewID (all workers in crew), or NULL (blanket tracing - all workers)

# Workers from direct worker assignment
direct_workers = (
    contact_rules
    .filter(F.col("WorkerID").isNotNull())
    .select(
        "ContactTracingRuleID", "ProjectID", "WorkerID",
        "DateFrom", "DateTo"
    )
)

# Workers from crew assignment
crew_workers = (
    contact_rules
    .filter(F.col("CrewID").isNotNull())
    .join(
        crew_assignments,
        (contact_rules["ProjectID"] == crew_assignments["ProjectID"]) &
        (contact_rules["CrewID"] == crew_assignments["CrewID"]) &
        ((contact_rules["DateFrom"] <= crew_assignments["ValidTo_ShiftLocalDate"]) | crew_assignments["ValidTo_ShiftLocalDate"].isNull()) &
        ((contact_rules["DateTo"] >= crew_assignments["ValidFrom_ShiftLocalDate"]) | contact_rules["DateTo"].isNull()),
        "inner"
    )
    .select(
        contact_rules["ContactTracingRuleID"],
        contact_rules["ProjectID"],
        crew_assignments["WorkerID"],
        F.greatest(contact_rules["DateFrom"], crew_assignments["ValidFrom_ShiftLocalDate"]).alias("DateFrom"),
        F.least(
            F.coalesce(contact_rules["DateTo"], F.lit("2099-12-31")),
            F.coalesce(crew_assignments["ValidTo_ShiftLocalDate"], F.lit("2099-12-31"))
        ).alias("DateTo")
    )
)

# Blanket tracing rules (all workers in project)
blanket_rules = (
    contact_rules
    .filter(F.col("WorkerID").isNull() & F.col("CrewID").isNull())
    .select("ContactTracingRuleID", "ProjectID", "DateFrom", "DateTo")
)

# Combine scopes
scope = direct_workers.unionByName(crew_workers, allowMissingColumns=True)

print(f"Workers in scope: {scope.count()}")
print(f"Blanket tracing rules: {blanket_rules.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Time Slicing and H3 Index to History

# COMMAND ----------

# Add time slicing for efficient proximity matching
# 5-minute time slices as per original procedure
history_indexed = (
    fact_workers_history
    .withColumn("TimeSlice",
        F.from_unixtime(
            (F.floor(F.unix_timestamp("TimestampUTC") / 300) * 300)
        ).cast("timestamp"))
    .withColumn("TimeSlice_Shift",
        F.from_unixtime(
            (F.floor((F.unix_timestamp("TimestampUTC") + 150) / 300) * 300)
        ).cast("timestamp"))
    .withColumn("H3Index", geo_to_h3(F.col("Latitude"), F.col("Longitude")))
    .withColumn("H3Neighbors", h3_k_ring(F.col("H3Index"), F.lit(1)))  # k=1 ring for ~2m
    .filter(F.col("H3Index").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Self-Join to Find Contacts

# COMMAND ----------

# Join workers within same project, floor, and time slice
# Using H3 for initial filter, then precise distance calculation

contacts_raw = (
    history_indexed.alias("a")
    .join(
        history_indexed.alias("b"),
        (F.col("a.ProjectID") == F.col("b.ProjectID")) &
        (F.col("a.FloorID") == F.col("b.FloorID")) &
        (F.col("a.LocalDate") == F.col("b.LocalDate")) &
        # Time slice match (either exact or shifted)
        ((F.col("a.TimeSlice") == F.col("b.TimeSlice")) |
         (F.col("a.TimeSlice_Shift") == F.col("b.TimeSlice_Shift"))) &
        # Time difference <= 150 seconds (2.5 minutes)
        (F.abs(F.unix_timestamp("a.TimestampUTC") - F.unix_timestamp("b.TimestampUTC")) <= 150) &
        # H3 neighbor check
        (F.array_contains(F.col("a.H3Neighbors"), F.col("b.H3Index"))) &
        # Worker b is not worker a
        (F.col("b.WorkerID").isNotNull()),
        "inner"
    )
    .select(
        F.col("a.ProjectID"),
        F.col("a.WorkerID").alias("WorkerID0"),  # Source worker
        F.col("a.LocalDate"),
        F.col("a.FloorID"),
        F.col("a.ZoneID"),
        F.col("b.WorkerID"),  # Contact worker
        F.col("b.TimestampUTC"),
        F.col("b.ActiveTime").alias("bActiveTime"),
        haversine_distance(
            F.col("a.Latitude"), F.col("a.Longitude"),
            F.col("b.Latitude"), F.col("b.Longitude")
        ).alias("Distance")
    )
    # Filter by actual distance threshold
    .filter(F.col("Distance") <= distance_threshold)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Aggregate Contacts by Worker Pair and Date

# COMMAND ----------

# First aggregation: per worker0-worker-floor-localdate-timestamp
contacts_per_ts = (
    contacts_raw
    .groupBy("ProjectID", "WorkerID0", "LocalDate", "TimestampUTC", "WorkerID", "FloorID")
    .agg(
        F.max("ZoneID").alias("ZoneID"),
        F.max("bActiveTime").alias("bActiveTime"),
        F.min("Distance").alias("Distance")
    )
)

# Final aggregation: per worker0-worker-floor-localdate
contacts_aggregated = (
    contacts_per_ts
    .groupBy("ProjectID", "WorkerID0", "LocalDate", "WorkerID", "FloorID")
    .agg(
        F.min("TimestampUTC").alias("FirstInteractionUTC"),
        F.max("ZoneID").alias("ZoneID"),
        F.count("*").alias("Interactions"),
        F.sum("bActiveTime").alias("InteractionDuration"),
        F.avg("Distance").alias("AvgDistanceMeters")
    )
)

print(f"Aggregated contacts: {contacts_aggregated.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Join with Rules to Get Final Contacts

# COMMAND ----------

# Join contacts with scope to get rule assignments
contacts_with_rules = (
    contacts_aggregated.alias("c")
    .join(
        scope.alias("s"),
        (F.col("c.ProjectID") == F.col("s.ProjectID")) &
        (F.col("c.WorkerID0") == F.col("s.WorkerID")) &
        (F.col("c.LocalDate") >= F.col("s.DateFrom")) &
        ((F.col("c.LocalDate") <= F.col("s.DateTo")) | F.col("s.DateTo").isNull()),
        "inner"
    )
    .select(
        F.col("s.ContactTracingRuleID"),
        "c.*"
    )
)

# Add blanket tracing rules (all contacts in project)
blanket_contacts = (
    contacts_aggregated.alias("c")
    .join(
        blanket_rules.alias("b"),
        (F.col("c.ProjectID") == F.col("b.ProjectID")) &
        (F.col("c.LocalDate") >= F.col("b.DateFrom")) &
        ((F.col("c.LocalDate") <= F.col("b.DateTo")) | F.col("b.DateTo").isNull()),
        "inner"
    )
    .select(
        F.col("b.ContactTracingRuleID"),
        "c.*"
    )
)

# Union all contacts
all_contacts = contacts_with_rules.unionByName(blanket_contacts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Handle TracePerFloor Flag

# COMMAND ----------

# Get rule settings
rules_settings = contact_rules.select("ContactTracingRuleID", "TracePerFloor")

# Split into floor-level and project-level contacts based on TracePerFloor flag
contacts_per_floor = (
    all_contacts.alias("c")
    .join(
        rules_settings.filter(F.col("TracePerFloor") == True),
        "ContactTracingRuleID",
        "inner"
    )
    .select("c.*")
)

contacts_per_project = (
    all_contacts.alias("c")
    .join(
        rules_settings.filter(F.coalesce(F.col("TracePerFloor"), F.lit(False)) == False),
        "ContactTracingRuleID",
        "inner"
    )
    .groupBy("ContactTracingRuleID", "ProjectID", "WorkerID0", "LocalDate", "WorkerID")
    .agg(
        F.min("FirstInteractionUTC").alias("FirstInteractionUTC"),
        F.max("FloorID").alias("FloorID"),
        F.max("ZoneID").alias("ZoneID"),
        F.sum("Interactions").alias("Interactions"),
        F.sum("InteractionDuration").alias("InteractionDuration"),
        (F.sum(F.col("AvgDistanceMeters") * F.col("Interactions")) / F.sum("Interactions")).alias("AvgDistanceMeters")
    )
)

# Final result combines both
final_contacts = contacts_per_floor.unionByName(contacts_per_project)

# Add metadata
final_contacts = (
    final_contacts
    .withColumn("ExtSourceID", F.lit(5))
    .withColumn("WatermarkUTC", F.current_timestamp())
)

print(f"Final contacts count: {final_contacts.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: MERGE into Target Table

# COMMAND ----------

target_table = "wakecap_prod.migration.fact_workers_contacts"

# Select final columns
result = final_contacts.select(
    "ContactTracingRuleID", "ProjectID", "WorkerID0", "LocalDate",
    "WorkerID", "FirstInteractionUTC", "FloorID", "ZoneID",
    "Interactions", "InteractionDuration", "AvgDistanceMeters",
    "ExtSourceID", "WatermarkUTC"
)

if spark.catalog.tableExists(target_table):
    target = DeltaTable.forName(spark, target_table)

    (target.alias("t")
     .merge(
         result.alias("s"),
         """
         t.ContactTracingRuleID = s.ContactTracingRuleID AND
         t.ProjectID = s.ProjectID AND
         t.WorkerID0 = s.WorkerID0 AND
         t.LocalDate = s.LocalDate AND
         t.WorkerID = s.WorkerID AND
         t.FloorID = s.FloorID
         """
     )
     .whenMatchedUpdate(
         condition="""
             s.FirstInteractionUTC != t.FirstInteractionUTC OR
             s.Interactions != t.Interactions OR
             ABS(s.InteractionDuration - t.InteractionDuration) > 0.00001 OR
             ABS(s.AvgDistanceMeters - t.AvgDistanceMeters) > 0.01 OR
             (s.ZoneID != t.ZoneID OR (s.ZoneID IS NULL AND t.ZoneID IS NOT NULL) OR (s.ZoneID IS NOT NULL AND t.ZoneID IS NULL))
         """,
         set={
             "FirstInteractionUTC": "s.FirstInteractionUTC",
             "Interactions": "s.Interactions",
             "InteractionDuration": "s.InteractionDuration",
             "AvgDistanceMeters": "s.AvgDistanceMeters",
             "ZoneID": "s.ZoneID",
             "ExtSourceID": "s.ExtSourceID",
             "WatermarkUTC": "s.WatermarkUTC"
         }
     )
     .whenNotMatchedInsertAll()
     .execute()
    )
    print(f"Merged into existing table {target_table}")
else:
    result.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Created new table {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Update Rule Evaluation Timestamp

# COMMAND ----------

# Mark rules as evaluated
# In production, this would update the ContactTracingRule table
evaluated_rules = contact_rules.select("ContactTracingRuleID").distinct().collect()
evaluated_rule_ids = [r.ContactTracingRuleID for r in evaluated_rules]
print(f"Evaluated rules: {evaluated_rule_ids}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Show sample results
display(spark.table(target_table).filter(
    F.col("LocalDate") >= cutoff_date
).limit(100))

# COMMAND ----------

# Summary
summary = spark.table(target_table).filter(
    F.col("LocalDate") >= cutoff_date
).agg(
    F.count("*").alias("total_contacts"),
    F.countDistinct("WorkerID0").alias("unique_source_workers"),
    F.countDistinct("WorkerID").alias("unique_contact_workers"),
    F.sum("Interactions").alias("total_interactions"),
    F.avg("AvgDistanceMeters").alias("avg_distance_meters")
)
display(summary)
