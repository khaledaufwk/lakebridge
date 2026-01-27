# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Manager Assignments Expanded View
# MAGIC
# MAGIC **Converted from:** `dbo.vwManagerAssignments_Expanded`
# MAGIC
# MAGIC **Purpose:** Provide expanded manager assignments that show the relationship between
# MAGIC managers and all workers in their assigned crews. Creates one row per manager-worker
# MAGIC pair for each crew assignment period.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Join manager assignments with crew composition (workers in crew)
# MAGIC - Calculate effective date ranges (intersection of manager and worker assignment periods)
# MAGIC - Use LEAD for ValidTo calculation
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_crew_manager`
# MAGIC - `wakecap_prod.silver.silver_crew_composition`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_manager_assignments_expanded`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_CREW_MGR = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_manager"
SOURCE_CREW_COMP = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_manager_assignments_expanded"

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

all_ok = True
for name, table in [("Crew Manager", SOURCE_CREW_MGR), ("Crew Composition", SOURCE_CREW_COMP)]:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
        print(f"[OK] {name}: {cnt:,} rows")
    except Exception as e:
        print(f"[ERROR] {name}: {str(e)[:50]}")
        all_ok = False

if not all_ok:
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

has_worker = False
try:
    spark.sql(f"SELECT 1 FROM {SOURCE_WORKER} LIMIT 0")
    has_worker = True
    print(f"[OK] Worker available")
except:
    print(f"[WARN] Worker not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

print("=" * 60)
print("BUILD EXPANDED MANAGER ASSIGNMENTS VIEW")
print("=" * 60)

# Load manager assignments (uses EffectiveDate)
mgr_df = spark.table(SOURCE_CREW_MGR).select(
    F.col("ProjectId").alias("mgr_ProjectId"),
    F.col("CrewId").alias("mgr_CrewId"),
    F.col("ManagerId").alias("ManagerWorkerId"),  # ManagerId is the worker ID of the manager
    F.col("EffectiveDate").alias("mgr_ValidFrom"),
    F.col("CreatedAt").alias("mgr_CreatedAt")
)

print(f"Manager assignments: {mgr_df.count():,}")

# Load crew composition (worker-crew assignments, uses CreatedAt)
comp_df = spark.table(SOURCE_CREW_COMP).select(
    F.col("ProjectId").alias("comp_ProjectId"),
    F.col("CrewId").alias("comp_CrewId"),
    F.col("WorkerId").alias("WorkerWorkerId"),
    F.col("CreatedAt").alias("comp_ValidFrom"),
    F.col("DeletedAt").alias("comp_ValidTo")
)

print(f"Crew composition: {comp_df.count():,}")

# Join manager with crew members
# Match on ProjectId and CrewId
expanded_df = mgr_df.join(
    comp_df,
    (F.col("mgr_ProjectId") == F.col("comp_ProjectId")) &
    (F.col("mgr_CrewId") == F.col("comp_CrewId")),
    "inner"
)

# Calculate effective date range (intersection)
# Use coalesce with far-future date for NULL handling
expanded_df = expanded_df.withColumn(
    "EffectiveFrom",
    F.greatest(F.col("mgr_ValidFrom"), F.col("comp_ValidFrom"))
).withColumn(
    "EffectiveTo",
    F.least(
        F.coalesce(F.col("comp_ValidTo"), F.lit("9999-12-31").cast("timestamp")),
        F.lit("9999-12-31").cast("timestamp")
    )
).withColumn(
    "EffectiveTo",
    F.when(F.col("EffectiveTo") == F.lit("9999-12-31").cast("timestamp"), F.lit(None)).otherwise(F.col("EffectiveTo"))
)

# Select final columns
expanded_df = expanded_df.select(
    F.col("mgr_ProjectId").alias("ProjectId"),
    F.col("mgr_CrewId").alias("CrewId"),
    F.col("ManagerWorkerId"),
    F.col("WorkerWorkerId"),
    F.col("EffectiveFrom").alias("ValidFrom"),
    F.col("EffectiveTo").alias("ValidTo")
)

# Filter out self-references (manager managing themselves)
expanded_df = expanded_df.filter(F.col("ManagerWorkerId") != F.col("WorkerWorkerId"))

print(f"Expanded assignments (before dedup): {expanded_df.count():,}")

# COMMAND ----------

# Calculate ValidTo using LEAD for precise boundaries
assign_window = Window.partitionBy("ProjectId", "ManagerWorkerId", "WorkerWorkerId").orderBy("ValidFrom")

expanded_df = expanded_df.withColumn(
    "ValidTo_Calculated",
    F.lead("ValidFrom").over(assign_window)
).withColumn(
    "ValidTo_Final",
    F.coalesce(F.col("ValidTo"), F.col("ValidTo_Calculated"))
)

# Add worker names (silver_worker has WorkerName column)
if has_worker:
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId"),
        F.col("WorkerName")
    )

    # Join for manager name
    expanded_df = expanded_df.join(
        worker_df.select(
            F.col("WorkerId").alias("m_id"),
            F.col("WorkerName").alias("ManagerName")
        ),
        F.col("ManagerWorkerId") == F.col("m_id"),
        "left"
    ).drop("m_id")

    # Join for worker name
    expanded_df = expanded_df.join(
        worker_df.select(
            F.col("WorkerId").alias("w_id"),
            F.col("WorkerName").alias("ManagedWorkerName")
        ),
        F.col("WorkerWorkerId") == F.col("w_id"),
        "left"
    ).drop("w_id")
else:
    expanded_df = expanded_df \
        .withColumn("ManagerName", F.lit(None).cast("string")) \
        .withColumn("ManagedWorkerName", F.lit(None).cast("string"))

# Add computed columns
expanded_df = expanded_df \
    .withColumn("IsCurrent", F.when(F.col("ValidTo_Final").isNull(), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("AssignmentDurationDays",
        F.when(F.col("ValidTo_Final").isNotNull(),
               F.datediff(F.col("ValidTo_Final"), F.col("ValidFrom")))
    ) \
    .withColumn("_view_generated_at", F.current_timestamp()) \
    .drop("ValidTo_Calculated")

print(f"Final view: {expanded_df.count():,} rows")

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

expanded_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Rows: {after_count:,}")

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
