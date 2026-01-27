# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Trade Assignments View
# MAGIC
# MAGIC **Converted from:** `dbo.vwTradeAssignments`
# MAGIC
# MAGIC **Purpose:** Provide trade assignments with calculated ValidTo using LEAD window function.
# MAGIC Trade assignments come from crew_composition which includes TradeId.
# MAGIC
# MAGIC **Key Logic:**
# MAGIC - LEAD(ValidFrom) OVER (PARTITION BY ProjectID, WorkerID ORDER BY ValidFrom) as ValidTo
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_crew_composition` (contains TradeId)
# MAGIC - `wakecap_prod.silver.silver_trade`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_vw_trade_assignments`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Trade assignments come from crew_composition which has TradeId
SOURCE_CREW_COMP = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"
SOURCE_TRADE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_trade"
SOURCE_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"

TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_vw_trade_assignments"

# COMMAND ----------

dbutils.widgets.dropdown("load_mode", "full", ["incremental", "full"], "Load Mode")

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

try:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_CREW_COMP}").collect()[0][0]
    print(f"[OK] Crew Composition (trade source): {cnt:,} rows")
except Exception as e:
    print(f"[ERROR] Crew Composition: {str(e)[:50]}")
    dbutils.notebook.exit("SOURCE_TABLES_MISSING")

opt_status = {}
for name, table in [("Trade", SOURCE_TRADE), ("Worker", SOURCE_WORKER)]:
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
print("BUILD TRADE ASSIGNMENTS VIEW")
print("=" * 60)

# Load crew composition (which contains TradeId)
# Note: silver_crew_composition uses CreatedAt, not ValidFrom
assign_df = spark.table(SOURCE_CREW_COMP).filter(F.col("TradeId").isNotNull())
print(f"Base assignments (with TradeId): {assign_df.count():,}")

# Rename CreatedAt to ValidFrom for consistency
assign_df = assign_df.withColumn("ValidFrom", F.col("CreatedAt"))

# Calculate ValidTo using LEAD
assign_window = Window.partitionBy("ProjectId", "WorkerId").orderBy("ValidFrom")

assign_df = assign_df.withColumn(
    "ValidTo_Calculated",
    F.lead("ValidFrom").over(assign_window)
)

# Use DeletedAt if set, otherwise use calculated ValidTo
assign_df = assign_df.withColumn(
    "ValidTo_Final",
    F.coalesce(F.col("DeletedAt"), F.col("ValidTo_Calculated"))
)

# Add trade details (silver_trade has TradeName column)
if opt_status.get("Trade"):
    trade_df = spark.table(SOURCE_TRADE).select(
        F.col("TradeId").alias("t_TradeId"),
        F.col("TradeName"),
        F.col("TradeCode")
    )
    assign_df = assign_df.join(trade_df, F.col("TradeId") == F.col("t_TradeId"), "left").drop("t_TradeId")
else:
    assign_df = assign_df \
        .withColumn("TradeName", F.lit(None).cast("string")) \
        .withColumn("TradeCode", F.lit(None).cast("string"))

# Add worker details (silver_worker has WorkerName column)
if opt_status.get("Worker"):
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId").alias("w_WorkerId"),
        F.col("WorkerName")
    )
    assign_df = assign_df.join(worker_df, F.col("WorkerId") == F.col("w_WorkerId"), "left").drop("w_WorkerId")
else:
    assign_df = assign_df.withColumn("WorkerName", F.lit(None).cast("string"))

# Add computed columns
assign_df = assign_df \
    .withColumn("IsCurrent", F.when(F.col("ValidTo_Final").isNull(), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("AssignmentDurationDays",
        F.when(F.col("ValidTo_Final").isNotNull(),
               F.datediff(F.col("ValidTo_Final"), F.col("ValidFrom")))
    ) \
    .withColumn("_view_generated_at", F.current_timestamp()) \
    .drop("ValidTo_Calculated")

print(f"Final view: {assign_df.count():,} rows")

# COMMAND ----------

print("=" * 60)
print("WRITE TO TARGET")
print("=" * 60)

try:
    before_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
except:
    before_count = 0

assign_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)

after_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
print(f"Rows: {after_count:,}")

dbutils.notebook.exit(f"SUCCESS: rows={after_count}")
