# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Worker Properties History View
# MAGIC
# MAGIC **Converted from:** `dbo.vwWorkerTitleDepartmentAssignments` / `dbo.vwWorkerPropertiesHistory`
# MAGIC
# MAGIC **Purpose:** Track changes to worker properties (title, department, trade) over time.
# MAGIC Uses assignment tables to build a historical view of worker attribute changes.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - Combine worker with their title/department/trade assignments
# MAGIC - Use LEAD to calculate ValidTo from next assignment's ValidFrom
# MAGIC - Track property changes over time
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC - `wakecap_prod.silver.silver_people_title`
# MAGIC - `wakecap_prod.silver.silver_department`
# MAGIC - `wakecap_prod.silver.silver_trade`
# MAGIC - `wakecap_prod.silver.silver_crew_composition` (for crew history)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_worker_properties_history`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
SOURCE_TITLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_people_title"
SOURCE_DEPARTMENT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_department"
SOURCE_TRADE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_trade"
SOURCE_CREW_COMP = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_worker_properties_history"

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_WORKER}").collect()[0][0]
    print(f"[OK] Worker: {cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Worker: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

opt_status = {}
for name, table in [("Title", SOURCE_TITLE), ("Department", SOURCE_DEPARTMENT),
                     ("Trade", SOURCE_TRADE), ("CrewComposition", SOURCE_CREW_COMP)]:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        opt_status[name] = True
        print(f"[OK] {name} available")
    except:
        opt_status[name] = False
        print(f"[WARN] {name} not available")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

print("=" * 60)
print("BUILD WORKER PROPERTIES HISTORY VIEW")
print("=" * 60)

# Load base worker data
worker_df = spark.table(SOURCE_WORKER)
print(f"Base workers: {worker_df.count():,}")

# Build property history from crew composition (tracks worker-crew assignments over time)
# This gives us temporal changes to worker assignments
if opt_status.get("CrewComposition"):
    crew_hist_df = spark.table(SOURCE_CREW_COMP).select(
        F.col("WorkerId"),
        F.col("ProjectId"),
        F.col("CrewId"),
        F.col("TradeId"),
        F.col("CreatedAt").alias("ValidFrom"),
        F.col("DeletedAt").alias("ValidTo")
    ).filter(F.col("DeletedAt").isNull())

    # Use LEAD to calculate ValidTo for continuous ranges
    assign_window = Window.partitionBy("ProjectId", "WorkerId").orderBy("ValidFrom")

    crew_hist_df = crew_hist_df.withColumn(
        "ValidTo_Calculated",
        F.lead("ValidFrom").over(assign_window)
    ).withColumn(
        "ValidTo_Final",
        F.coalesce(F.col("ValidTo"), F.col("ValidTo_Calculated"))
    )

    print(f"Crew history records: {crew_hist_df.count():,}")
else:
    crew_hist_df = None
    print("[WARN] Crew composition not available, skipping crew history")

# COMMAND ----------

# Join worker with dimension tables
result_df = worker_df.alias("w")

# Add Title details
if opt_status.get("Title"):
    title_df = spark.table(SOURCE_TITLE).select(
        F.col("PeopleTitleId").alias("t_TitleId"),
        F.col("TitleName"),
        F.col("TitleCode")
    )
    result_df = result_df.join(
        title_df,
        F.col("w.TitleId") == F.col("t_TitleId"),
        "left"
    ).drop("t_TitleId")
else:
    result_df = result_df \
        .withColumn("TitleName", F.lit(None).cast("string")) \
        .withColumn("TitleCode", F.lit(None).cast("string"))

# Add Department details
if opt_status.get("Department"):
    dept_df = spark.table(SOURCE_DEPARTMENT).select(
        F.col("Id").alias("d_DeptId"),
        F.col("DepartmentName"),
        F.col("DepartmentCode")
    )
    result_df = result_df.join(
        dept_df,
        F.col("w.DepartmentId") == F.col("d_DeptId"),
        "left"
    ).drop("d_DeptId")
else:
    result_df = result_df \
        .withColumn("DepartmentName", F.lit(None).cast("string")) \
        .withColumn("DepartmentCode", F.lit(None).cast("string"))

# Add Trade details
if opt_status.get("Trade"):
    trade_df = spark.table(SOURCE_TRADE).select(
        F.col("TradeId").alias("tr_TradeId"),
        F.col("TradeName"),
        F.col("TradeCode")
    )
    result_df = result_df.join(
        trade_df,
        F.col("w.TradeId") == F.col("tr_TradeId"),
        "left"
    ).drop("tr_TradeId")
else:
    result_df = result_df \
        .withColumn("TradeName", F.lit(None).cast("string")) \
        .withColumn("TradeCode", F.lit(None).cast("string"))

# COMMAND ----------

# Build the final view combining worker properties with temporal information
# Use CreatedAt/UpdatedAt as ValidFrom, and calculate ValidTo using row-level changes

# For workers with crew history, join to get temporal records
if crew_hist_df is not None:
    # Get worker base info (non-temporal columns)
    worker_base = result_df.select(
        F.col("w.WorkerId"),
        F.col("w.WorkerCode"),
        F.col("w.WorkerName"),
        F.col("w.ProjectId"),
        F.col("TitleName"),
        F.col("TitleCode"),
        F.col("DepartmentName"),
        F.col("DepartmentCode"),
        F.col("TradeName").alias("WorkerTradeName"),
        F.col("TradeCode").alias("WorkerTradeCode"),
        F.col("w.Nationality"),
        F.col("w.HelmetColor")
    )

    # Join with crew history to get temporal records with crew/trade assignments
    props_history = worker_base.alias("wb").join(
        crew_hist_df.alias("ch"),
        F.col("wb.WorkerId") == F.col("ch.WorkerId"),
        "left"
    )

    # Add trade from crew composition (may differ from worker's default trade)
    if opt_status.get("Trade"):
        props_history = props_history.join(
            trade_df.alias("ct"),
            F.col("ch.TradeId") == F.col("ct.tr_TradeId"),
            "left"
        ).withColumn(
            "AssignedTradeName",
            F.coalesce(F.col("ct.TradeName"), F.col("wb.WorkerTradeName"))
        ).withColumn(
            "AssignedTradeCode",
            F.coalesce(F.col("ct.TradeCode"), F.col("wb.WorkerTradeCode"))
        ).drop("ct.tr_TradeId")
    else:
        props_history = props_history \
            .withColumn("AssignedTradeName", F.col("wb.WorkerTradeName")) \
            .withColumn("AssignedTradeCode", F.col("wb.WorkerTradeCode"))

    # Select final columns
    props_history = props_history.select(
        F.col("wb.WorkerId"),
        F.col("wb.WorkerCode"),
        F.col("wb.WorkerName"),
        F.coalesce(F.col("ch.ProjectId"), F.col("wb.ProjectId")).alias("ProjectId"),
        F.col("ch.CrewId"),
        F.col("wb.TitleName"),
        F.col("wb.TitleCode"),
        F.col("wb.DepartmentName"),
        F.col("wb.DepartmentCode"),
        F.col("AssignedTradeName"),
        F.col("AssignedTradeCode"),
        F.col("wb.Nationality"),
        F.col("wb.HelmetColor"),
        F.col("ch.ValidFrom"),
        F.col("ch.ValidTo_Final").alias("ValidTo")
    )
else:
    # Fallback: use worker created/updated as temporal markers
    props_history = result_df.select(
        F.col("w.WorkerId"),
        F.col("w.WorkerCode"),
        F.col("w.WorkerName"),
        F.col("w.ProjectId"),
        F.lit(None).cast("string").alias("CrewId"),
        F.col("TitleName"),
        F.col("TitleCode"),
        F.col("DepartmentName"),
        F.col("DepartmentCode"),
        F.col("TradeName").alias("AssignedTradeName"),
        F.col("TradeCode").alias("AssignedTradeCode"),
        F.col("w.Nationality"),
        F.col("w.HelmetColor"),
        F.col("w.CreatedAt").alias("ValidFrom"),
        F.lit(None).cast("timestamp").alias("ValidTo")
    )

# COMMAND ----------

# Add computed columns
props_history = props_history \
    .withColumn("IsCurrent", F.when(F.col("ValidTo").isNull(), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("AssignmentDurationDays",
        F.when(F.col("ValidTo").isNotNull(),
               F.datediff(F.col("ValidTo"), F.col("ValidFrom")))
    ) \
    .withColumn("_view_generated_at", F.current_timestamp())

# Deduplicate if needed (keep latest per worker+validfrom combination)
dedup_window = Window.partitionBy("WorkerId", "ValidFrom").orderBy(F.col("ValidTo").desc_nulls_first())
props_history = props_history \
    .withColumn("_rn", F.row_number().over(dedup_window)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn")

print(f"Final view: {props_history.count():,} rows")

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

props_history.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Rows: {after_count:,}")

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
