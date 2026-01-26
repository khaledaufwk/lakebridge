# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Manager Assignment Snapshots
# MAGIC
# MAGIC **Converted from:** `stg.spCalculateManagerAssignmentSnapshots` (266 lines)
# MAGIC
# MAGIC **Purpose:** Calculate flattened manager hierarchy snapshots with time-sliced intervals.
# MAGIC For each worker, determines all managers at each level (0-12) for each time boundary period.
# MAGIC
# MAGIC **Original Patterns:**
# MAGIC - RECURSIVE CTE -> Iterative loop with DataFrame accumulation
# MAGIC - TEMP_TABLE (`#ca`, `#ca2`, `#unpivoted`, `#boundaries`, `#result`) -> Spark DataFrames with cache()
# MAGIC - PIVOT -> PySpark pivot()
# MAGIC - MERGE with INSERT/UPDATE/DELETE -> Two-phase DeltaTable merge
# MAGIC - Date interval intersection (GREATEST/LEAST) -> greatest()/least() functions
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `wakecap_prod.silver.silver_manager_assignments_expanded` (dbo.vwManagerAssignments_Expanded)
# MAGIC - `wakecap_prod.silver.silver_crew_composition` (dbo.vwCrewAssignments)
# MAGIC
# MAGIC **Target:** `wakecap_prod.gold.gold_manager_assignment_snapshots`
# MAGIC **Watermarks:** `wakecap_prod.migration._gold_watermarks`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and Schema Configuration
TARGET_CATALOG = "wakecap_prod"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MIGRATION_SCHEMA = "migration"

# Source tables (Silver layer equivalents of the views)
# Note: vwManagerAssignments_Expanded -> silver_crew_manager joined with silver_crew_composition
# In this simplified model, we use silver_crew_manager as the manager assignments source
SOURCE_MANAGER_ASSIGNMENTS = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_manager"
SOURCE_CREW_COMPOSITION = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"

# Target table
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.gold_manager_assignment_snapshots"
WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# Maximum hierarchy level (matches original SP: Level < 12)
MAX_HIERARCHY_LEVEL = 12

print(f"Source Manager Assignments: {SOURCE_MANAGER_ASSIGNMENTS}")
print(f"Source Crew Composition: {SOURCE_CREW_COMPOSITION}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")

load_mode = dbutils.widgets.get("load_mode")
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {load_mode}")
print(f"Project Filter: {project_filter if project_filter else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check source tables
source_tables = [
    ("Manager Assignments", SOURCE_MANAGER_ASSIGNMENTS),
    ("Crew Composition", SOURCE_CREW_COMPOSITION),
]

for name, table in source_tables:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 0")
        print(f"[OK] {name} exists: {table}")
    except Exception as e:
        print(f"[WARN] {name} not found: {table} - {str(e)[:50]}")

# Ensure target schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print(f"[OK] Target schemas verified")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_watermark(table_name):
    """Get last processed watermark for incremental loading."""
    try:
        result = spark.sql(f"""
            SELECT last_watermark_value
            FROM {WATERMARK_TABLE}
            WHERE table_name = '{table_name}'
        """).collect()
        if result and result[0][0]:
            return result[0][0]
    except Exception as e:
        print(f"  Watermark lookup failed: {str(e)[:50]}")
    return datetime(1900, 1, 1)


def update_watermark(table_name, watermark_value, row_count, metrics=None):
    """Update watermark after successful processing."""
    try:
        spark.sql(f"""
            MERGE INTO {WATERMARK_TABLE} AS target
            USING (SELECT '{table_name}' as table_name,
                          CAST('{watermark_value}' AS TIMESTAMP) as last_watermark_value,
                          {row_count} as row_count,
                          current_timestamp() as last_processed_at,
                          current_timestamp() as updated_at) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET
                target.last_watermark_value = source.last_watermark_value,
                target.row_count = source.row_count,
                target.last_processed_at = source.last_processed_at,
                target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        print(f"  Warning: Could not update watermark: {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Calculate Impacted Area
# MAGIC
# MAGIC Identify which workers/projects have changed since last watermark.
# MAGIC This matches the original `@ImpactedArea` table variable.

# COMMAND ----------

print("=" * 60)
print("STEP 1: Calculate Impacted Area")
print("=" * 60)

# Get watermarks
last_mgr_watermark = get_watermark("sql_CalculateManagerAssignmentSnapshots")
last_crew_watermark = get_watermark("sql_CrewAssignments[tracked for mgr snapshots]")

print(f"Last Manager Watermark: {last_mgr_watermark}")
print(f"Last Crew Watermark: {last_crew_watermark}")

# Load manager assignments
try:
    manager_assignments_df = spark.table(SOURCE_MANAGER_ASSIGNMENTS)
    print(f"Loaded manager assignments: {manager_assignments_df.count()} rows")
except Exception as e:
    print(f"[FATAL] Cannot load manager assignments: {str(e)}")
    dbutils.notebook.exit(f"SOURCE_NOT_FOUND: {SOURCE_MANAGER_ASSIGNMENTS}")

# Get new watermark
new_mgr_watermark = manager_assignments_df.agg(F.max("UpdatedAt")).collect()[0][0]
if new_mgr_watermark is None:
    new_mgr_watermark = datetime.now()
print(f"New Manager Watermark: {new_mgr_watermark}")

# Load crew composition
try:
    crew_composition_df = spark.table(SOURCE_CREW_COMPOSITION)
    new_crew_watermark = crew_composition_df.agg(F.max("UpdatedAt")).collect()[0][0]
    if new_crew_watermark is None:
        new_crew_watermark = datetime.now()
    print(f"New Crew Watermark: {new_crew_watermark}")
except Exception as e:
    print(f"[WARN] Cannot load crew composition: {str(e)}")
    crew_composition_df = None
    new_crew_watermark = datetime.now()

# COMMAND ----------

# Calculate impacted area based on incremental logic
if load_mode == "incremental":
    # Filter manager assignments by watermark
    # Original: WatermarkUTC > @LastWatermark AND (WatermarkUTC <= @NewWatermark OR @NewWatermark IS NULL)
    mgr_changes = manager_assignments_df.filter(
        (F.col("UpdatedAt") > F.lit(last_mgr_watermark)) &
        (F.col("UpdatedAt") <= F.lit(new_mgr_watermark))
    ).select(
        F.col("ProjectId"),
        F.col("ManagerId").alias("WorkerId"),  # In crew_manager, ManagerId is the worker being assigned as manager
        F.col("EffectiveDate").alias("ValidFrom_ShiftLocalDate"),
        F.lit(None).cast("date").alias("ValidTo_ShiftLocalDate")  # Open-ended for current managers
    )

    # Filter crew composition changes
    if crew_composition_df is not None:
        crew_changes = crew_composition_df.filter(
            (F.col("UpdatedAt") > F.lit(last_crew_watermark)) &
            (F.col("UpdatedAt") <= F.lit(new_crew_watermark)) &
            (F.col("UpdatedAt") > F.lit(last_mgr_watermark))  # Additional filter from original SP
        ).select(
            F.col("ProjectId"),
            F.col("WorkerId"),
            F.col("CreatedAt").cast("date").alias("ValidFrom_ShiftLocalDate"),
            F.date_add(F.coalesce(F.col("DeletedAt").cast("date"), F.current_date()), 1).alias("ValidTo_ShiftLocalDate")
        )

        # Union changes
        all_changes = mgr_changes.union(crew_changes)
    else:
        all_changes = mgr_changes

    # Group to get impacted area (ProjectID, WorkerID, MinDate, MaxDate)
    impacted_area = all_changes.groupBy("ProjectId", "WorkerId").agg(
        F.min("ValidFrom_ShiftLocalDate").alias("MinLocalDate"),
        F.max("ValidTo_ShiftLocalDate").alias("MaxLocalDate")
    )

    impacted_count = impacted_area.count()
    print(f"Impacted workers/projects: {impacted_count}")

    if impacted_count == 0:
        print("No changes detected. Exiting.")
        dbutils.notebook.exit("No changes to process")
else:
    # Full load: all workers are impacted
    impacted_area = manager_assignments_df.select(
        F.col("ProjectId"),
        F.col("ManagerId").alias("WorkerId")
    ).distinct().withColumn(
        "MinLocalDate", F.lit("1900-01-01").cast("date")
    ).withColumn(
        "MaxLocalDate", F.lit("9999-12-31").cast("date")
    )
    print(f"Full load: {impacted_area.count()} workers/projects")

# Apply project filter if specified
if project_filter:
    impacted_area = impacted_area.filter(F.col("ProjectId") == int(project_filter))
    print(f"Filtered to project_id: {project_filter}")

impacted_area = impacted_area.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Base DataFrames (#ca, #ca2)
# MAGIC
# MAGIC - `#ca`: Manager assignments for impacted workers only
# MAGIC - `#ca2`: All manager assignments for impacted projects (needed for hierarchy walk)

# COMMAND ----------

print("=" * 60)
print("STEP 2: Load Base DataFrames")
print("=" * 60)

# Build the expanded manager assignments view
# Original view joins CrewManager with CrewComposition to get worker -> manager relationships
# For now, we'll create a simplified version using crew_manager and crew_composition

# Get workers from crew composition (who has a manager)
workers_df = crew_composition_df.filter(
    F.col("DeletedAt").isNull()
).select(
    F.col("ProjectId"),
    F.col("CrewId"),
    F.col("WorkerId")
).distinct()

# Join with managers
# CrewManager links CrewId to ManagerId (ManagerId is a WorkerId who manages the crew)
managers_df = manager_assignments_df.filter(
    F.col("DeletedAt").isNull()
).select(
    F.col("ProjectId"),
    F.col("CrewId"),
    F.col("ManagerId").alias("ManagerWorkerId"),
    F.col("EffectiveDate").alias("ValidFrom_ShiftLocalDate")
).withColumn(
    "ValidTo_ShiftLocalDate", F.lit(None).cast("date")  # Current assignments are open-ended
)

# Create expanded assignments: Worker -> Manager relationships via Crew
expanded_assignments = workers_df.alias("w").join(
    managers_df.alias("m"),
    (F.col("w.ProjectId") == F.col("m.ProjectId")) &
    (F.col("w.CrewId") == F.col("m.CrewId")),
    "inner"
).select(
    F.col("w.ProjectId"),
    F.col("w.CrewId"),
    F.col("w.WorkerId"),
    F.col("m.ManagerWorkerId"),
    F.col("m.ValidFrom_ShiftLocalDate"),
    F.col("m.ValidTo_ShiftLocalDate")
)

print(f"Expanded assignments: {expanded_assignments.count()} rows")

# #ca: Manager assignments for impacted workers
# Original: SELECT mae.* FROM dbo.vwManagerAssignments_Expanded mae
#           INNER JOIN @ImpactedArea ia ON mae.ProjectID = ia.ProjectID AND mae.WorkerID = ia.WorkerID
ca_df = expanded_assignments.alias("mae").join(
    impacted_area.alias("ia"),
    (F.col("mae.ProjectId") == F.col("ia.ProjectId")) &
    (F.col("mae.WorkerId") == F.col("ia.WorkerId")),
    "inner"
).select(
    F.col("mae.ProjectId"),
    F.col("mae.CrewId"),
    F.col("mae.WorkerId"),
    F.col("mae.ManagerWorkerId"),
    F.col("mae.ValidFrom_ShiftLocalDate"),
    F.col("mae.ValidTo_ShiftLocalDate")
)

ca_df = ca_df.cache()
print(f"#ca (impacted workers): {ca_df.count()} rows")

# #ca2: All manager assignments for impacted projects
# Original: SELECT mae.* FROM dbo.vwManagerAssignments_Expanded mae
#           WHERE ProjectID IN (SELECT DISTINCT ProjectID FROM @ImpactedArea)
impacted_projects = impacted_area.select("ProjectId").distinct()

ca2_df = expanded_assignments.alias("mae").join(
    impacted_projects.alias("ip"),
    F.col("mae.ProjectId") == F.col("ip.ProjectId"),
    "inner"
).select(
    F.col("mae.ProjectId"),
    F.col("mae.CrewId"),
    F.col("mae.WorkerId"),
    F.col("mae.ManagerWorkerId"),
    F.col("mae.ValidFrom_ShiftLocalDate"),
    F.col("mae.ValidTo_ShiftLocalDate")
)

ca2_df = ca2_df.cache()
print(f"#ca2 (all project assignments): {ca2_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Iterative Hierarchy Walk (Recursive CTE Replacement)
# MAGIC
# MAGIC Walk the manager hierarchy up to 12 levels using iterative approach.
# MAGIC
# MAGIC Original recursive CTE:
# MAGIC ```sql
# MAGIC WITH MyCTE AS (
# MAGIC     -- Anchor: Level 1 - direct managers
# MAGIC     SELECT ProjectID, WorkerID, ManagerWorkerID, ValidFrom, ValidTo, 1 AS Level
# MAGIC     FROM #ca WHERE WorkerID <> ManagerWorkerID
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     -- Recursive: Walk up hierarchy
# MAGIC     SELECT t2.ProjectID, M.WorkerID, t2.ManagerWorkerID,
# MAGIC            GREATEST(t2.ValidFrom, m.ValidFrom),
# MAGIC            LEAST(t2.ValidTo, m.ValidTo),
# MAGIC            M.Level + 1
# MAGIC     FROM #ca2 t2
# MAGIC     JOIN MyCTE M ON t2.WorkerID = M.ManagerWorkerID
# MAGIC         AND (date intervals overlap)
# MAGIC     WHERE M.WorkerID <> t2.ManagerWorkerID AND Level < 12
# MAGIC )
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("STEP 3: Iterative Hierarchy Walk")
print("=" * 60)

# Level 1: Direct managers (anchor of recursive CTE)
# Original: WHERE WorkerID <> ManagerWorkerID
level1 = ca_df.filter(
    F.col("WorkerId") != F.col("ManagerWorkerId")
).select(
    F.col("ProjectId"),
    F.col("WorkerId"),
    F.col("ManagerWorkerId"),
    F.col("ValidFrom_ShiftLocalDate"),
    F.col("ValidTo_ShiftLocalDate"),
    F.lit(1).alias("Level")
)

print(f"Level 1 (direct managers): {level1.count()} rows")

# Accumulate all levels
all_levels = level1
current_level = level1

# Iterate for levels 2 through MAX_HIERARCHY_LEVEL
for level_num in range(2, MAX_HIERARCHY_LEVEL + 1):
    # Join current level with ca2 to find next level managers
    # Original join conditions:
    # t2.WorkerID = M.ManagerWorkerID (the manager becomes the worker to find their manager)
    # AND (t2.ValidTo > m.ValidFrom OR t2.ValidTo IS NULL)
    # AND (m.ValidTo > t2.ValidFrom OR m.ValidTo IS NULL)

    next_level = current_level.alias("m").join(
        ca2_df.alias("t2"),
        (F.col("t2.WorkerId") == F.col("m.ManagerWorkerId")) &
        # Date interval overlap check
        (F.col("t2.ValidTo_ShiftLocalDate").isNull() |
         (F.col("t2.ValidTo_ShiftLocalDate") > F.col("m.ValidFrom_ShiftLocalDate"))) &
        (F.col("m.ValidTo_ShiftLocalDate").isNull() |
         (F.col("m.ValidTo_ShiftLocalDate") > F.col("t2.ValidFrom_ShiftLocalDate"))),
        "inner"
    ).filter(
        # Cycle prevention: M.WorkerID <> t2.ManagerWorkerID AND t2.WorkerID <> t2.ManagerWorkerID
        (F.col("m.WorkerId") != F.col("t2.ManagerWorkerId")) &
        (F.col("t2.WorkerId") != F.col("t2.ManagerWorkerId"))
    ).select(
        F.col("t2.ProjectId"),
        F.col("m.WorkerId"),  # Keep original worker from lower level
        F.col("t2.ManagerWorkerId"),  # Get their manager's manager
        # Date interval intersection: GREATEST(t2.ValidFrom, m.ValidFrom)
        F.greatest(
            F.col("t2.ValidFrom_ShiftLocalDate"),
            F.col("m.ValidFrom_ShiftLocalDate")
        ).alias("ValidFrom_ShiftLocalDate"),
        # LEAST with NULL handling: CASE WHEN t2.ValidTo < m.ValidTo OR m.ValidTo IS NULL THEN t2.ValidTo ELSE m.ValidTo END
        F.when(
            F.col("m.ValidTo_ShiftLocalDate").isNull(),
            F.col("t2.ValidTo_ShiftLocalDate")
        ).when(
            F.col("t2.ValidTo_ShiftLocalDate").isNull(),
            F.col("m.ValidTo_ShiftLocalDate")
        ).otherwise(
            F.least(F.col("t2.ValidTo_ShiftLocalDate"), F.col("m.ValidTo_ShiftLocalDate"))
        ).alias("ValidTo_ShiftLocalDate"),
        F.lit(level_num).alias("Level")
    )

    next_count = next_level.count()
    print(f"Level {level_num}: {next_count} rows")

    if next_count == 0:
        break

    # Union with accumulated levels
    all_levels = all_levels.union(next_level)
    current_level = next_level

# Filter to only include records with non-null managers (matches original: WHERE ManagerWorkerID IS NOT NULL)
unpivoted_df = all_levels.filter(F.col("ManagerWorkerId").isNotNull())
unpivoted_df = unpivoted_df.cache()

total_unpivoted = unpivoted_df.count()
print(f"\nTotal unpivoted rows: {total_unpivoted}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Calculate Boundary Dates
# MAGIC
# MAGIC Extract unique ValidFrom/ValidTo dates and calculate LEAD(Boundary) for BoundaryNext.
# MAGIC
# MAGIC Original:
# MAGIC ```sql
# MAGIC SELECT ProjectID, WorkerID, Boundary,
# MAGIC        LEAD(Boundary) OVER (PARTITION BY WorkerID ORDER BY Boundary) as BoundaryNext
# MAGIC INTO #boundaries
# MAGIC FROM (
# MAGIC     SELECT ProjectID, WorkerID, ValidFrom_ShiftLocalDate as Boundary FROM #unpivoted GROUP BY ...
# MAGIC     UNION
# MAGIC     SELECT ProjectID, WorkerID, ValidTo_ShiftLocalDate as Boundary FROM #unpivoted WHERE ValidTo IS NOT NULL GROUP BY ...
# MAGIC )
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("STEP 4: Calculate Boundary Dates")
print("=" * 60)

# Get all unique boundary dates (ValidFrom dates)
boundaries_from = unpivoted_df.select(
    "ProjectId", "WorkerId",
    F.col("ValidFrom_ShiftLocalDate").alias("Boundary")
).filter(
    F.col("Boundary").isNotNull()
).distinct()

# Get all unique boundary dates (ValidTo dates)
boundaries_to = unpivoted_df.filter(
    F.col("ValidTo_ShiftLocalDate").isNotNull()
).select(
    "ProjectId", "WorkerId",
    F.col("ValidTo_ShiftLocalDate").alias("Boundary")
).distinct()

# Union and deduplicate
all_boundaries = boundaries_from.union(boundaries_to).distinct()

# Calculate BoundaryNext using LEAD window function
boundary_window = Window.partitionBy("WorkerId").orderBy("Boundary")

boundaries_df = all_boundaries.withColumn(
    "BoundaryNext",
    F.lead("Boundary").over(boundary_window)
)

boundaries_df = boundaries_df.cache()
print(f"Boundary records: {boundaries_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: PIVOT to Create Manager Columns
# MAGIC
# MAGIC Join boundaries with unpivoted data and pivot to create ManagerWorkerID0-12 columns.
# MAGIC
# MAGIC Original:
# MAGIC ```sql
# MAGIC SELECT ... INTO #result FROM (
# MAGIC     SELECT b.*, ue.ManagerWorkerID, ue.Level,
# MAGIC            MAX(Level) OVER (PARTITION BY WorkerID, Boundary) as Levels
# MAGIC     FROM #boundaries b
# MAGIC     INNER JOIN #unpivoted ue ON b.WorkerID = ue.WorkerID
# MAGIC         AND b.Boundary >= ue.ValidFrom AND (b.Boundary < ue.ValidTo OR ue.ValidTo IS NULL)
# MAGIC     UNION ALL
# MAGIC     -- Level 0: Worker themselves
# MAGIC     SELECT ProjectID, WorkerID, Boundary, BoundaryNext, WorkerID as ManagerWorkerID, 0 as Level
# MAGIC     FROM #boundaries
# MAGIC ) t
# MAGIC PIVOT(MAX(ManagerWorkerID) FOR Level IN ([0], [1], ..., [12]))
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("STEP 5: PIVOT to Create Manager Columns")
print("=" * 60)

# Break loops by keeping only minimum level for duplicate (Worker, Manager, ValidFrom, ValidTo) combinations
level_window = Window.partitionBy(
    "ProjectId", "WorkerId", "ManagerWorkerId", "ValidFrom_ShiftLocalDate", "ValidTo_ShiftLocalDate"
).orderBy("Level")

unpivoted_deduped = unpivoted_df.withColumn(
    "MinLevel",
    F.min("Level").over(level_window)
).filter(
    F.col("Level") == F.col("MinLevel")
).drop("MinLevel")

# Join boundaries with unpivoted data
# Original: b.Boundary >= ue.ValidFrom AND (b.Boundary < ue.ValidTo OR ue.ValidTo IS NULL)
joined_df = boundaries_df.alias("b").join(
    unpivoted_deduped.alias("ue"),
    (F.col("b.WorkerId") == F.col("ue.WorkerId")) &
    (F.col("b.Boundary") >= F.col("ue.ValidFrom_ShiftLocalDate")) &
    (F.col("ue.ValidTo_ShiftLocalDate").isNull() | (F.col("b.Boundary") < F.col("ue.ValidTo_ShiftLocalDate"))),
    "inner"
).select(
    F.col("b.ProjectId"),
    F.col("b.WorkerId"),
    F.col("b.Boundary"),
    F.col("b.BoundaryNext"),
    F.col("ue.ManagerWorkerId"),
    F.col("ue.Level")
)

# Add Level 0: Worker themselves
level0_df = boundaries_df.select(
    F.col("ProjectId"),
    F.col("WorkerId"),
    F.col("Boundary"),
    F.col("BoundaryNext"),
    F.col("WorkerId").alias("ManagerWorkerId"),
    F.lit(0).alias("Level")
)

# Union with hierarchy levels
pre_pivot = joined_df.union(level0_df)

# Calculate Levels (max level per worker/boundary combination)
levels_window = Window.partitionBy("WorkerId", "Boundary")
pre_pivot = pre_pivot.withColumn(
    "Levels",
    F.max("Level").over(levels_window)
)

print(f"Pre-pivot records: {pre_pivot.count()}")

# COMMAND ----------

# Pivot to create ManagerWorkerID columns
# Original: PIVOT(MAX(ManagerWorkerID) FOR Level IN ([0], [1], ..., [12]))

pivoted_df = pre_pivot.groupBy(
    "ProjectId", "WorkerId", "Boundary", "BoundaryNext", "Levels"
).pivot(
    "Level",
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
).agg(
    F.max("ManagerWorkerId")
)

# Rename pivot columns to match target schema
result_df = pivoted_df.select(
    F.col("ProjectId"),
    F.col("WorkerId"),
    F.col("Levels"),
    F.col("Boundary").alias("ValidFrom_ShiftLocalDate"),
    F.col("BoundaryNext").alias("ValidTo_ShiftLocalDate"),
    F.col("0").alias("ManagerWorkerId0"),
    F.col("1").alias("ManagerWorkerId1"),
    F.col("2").alias("ManagerWorkerId2"),
    F.col("3").alias("ManagerWorkerId3"),
    F.col("4").alias("ManagerWorkerId4"),
    F.col("5").alias("ManagerWorkerId5"),
    F.col("6").alias("ManagerWorkerId6"),
    F.col("7").alias("ManagerWorkerId7"),
    F.col("8").alias("ManagerWorkerId8"),
    F.col("9").alias("ManagerWorkerId9"),
    F.col("10").alias("ManagerWorkerId10"),
    F.col("11").alias("ManagerWorkerId11"),
    F.col("12").alias("ManagerWorkerId12")
).withColumn(
    "WatermarkUTC",
    F.current_timestamp()
)

result_df = result_df.cache()
result_count = result_df.count()
print(f"Result records: {result_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Two-Phase MERGE
# MAGIC
# MAGIC Phase A: MERGE for INSERT/UPDATE (WHEN NOT MATCHED BY SOURCE handled separately)
# MAGIC Phase B: DELETE for NOT MATCHED BY SOURCE (anti-join pattern)
# MAGIC
# MAGIC Original MERGE:
# MAGIC ```sql
# MAGIC WITH CTE AS (
# MAGIC     SELECT mas.* FROM [dbo].[ManagerAssignmentSnapshots] mas
# MAGIC     INNER JOIN @ImpactedArea ia ON mas.ProjectID = ia.ProjectID AND mas.WorkerID = ia.WorkerID
# MAGIC )
# MAGIC MERGE CTE t USING #result s
# MAGIC ON s.ProjectID = t.ProjectID AND s.WorkerID = t.WorkerID AND s.ValidFrom = t.ValidFrom
# MAGIC WHEN MATCHED AND (...columns differ...) THEN UPDATE SET ...
# MAGIC WHEN NOT MATCHED BY TARGET THEN INSERT ...
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("STEP 6: Two-Phase MERGE")
print("=" * 60)

# Check if target table exists
target_exists = spark.catalog.tableExists(TARGET_TABLE)

if target_exists:
    print(f"Target table exists: {TARGET_TABLE}")

    # Phase A: MERGE for INSERT/UPDATE
    print("\nPhase A: MERGE INSERT/UPDATE...")

    # Create temp view for merge source
    result_df.createOrReplaceTempView("merge_source_view")

    # Scope the merge to only impacted workers (matches CTE in original)
    impacted_area.createOrReplaceTempView("impacted_area_view")

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING (
            SELECT s.*
            FROM merge_source_view s
            INNER JOIN impacted_area_view ia
            ON s.ProjectId = ia.ProjectId AND s.WorkerId = ia.WorkerId
        ) AS s
        ON s.ProjectId = t.ProjectId
           AND s.WorkerId = t.WorkerId
           AND s.ValidFrom_ShiftLocalDate = t.ValidFrom_ShiftLocalDate
        WHEN MATCHED AND (
            NOT (s.ManagerWorkerId0 <=> t.ManagerWorkerId0) OR
            NOT (s.ManagerWorkerId1 <=> t.ManagerWorkerId1) OR
            NOT (s.ManagerWorkerId2 <=> t.ManagerWorkerId2) OR
            NOT (s.ManagerWorkerId3 <=> t.ManagerWorkerId3) OR
            NOT (s.ManagerWorkerId4 <=> t.ManagerWorkerId4) OR
            NOT (s.ManagerWorkerId5 <=> t.ManagerWorkerId5) OR
            NOT (s.ManagerWorkerId6 <=> t.ManagerWorkerId6) OR
            NOT (s.ManagerWorkerId7 <=> t.ManagerWorkerId7) OR
            NOT (s.ManagerWorkerId8 <=> t.ManagerWorkerId8) OR
            NOT (s.ManagerWorkerId9 <=> t.ManagerWorkerId9) OR
            NOT (s.ManagerWorkerId10 <=> t.ManagerWorkerId10) OR
            NOT (s.ManagerWorkerId11 <=> t.ManagerWorkerId11) OR
            NOT (s.ManagerWorkerId12 <=> t.ManagerWorkerId12) OR
            NOT (s.Levels <=> t.Levels)
        )
        THEN UPDATE SET
            t.Levels = s.Levels,
            t.ValidTo_ShiftLocalDate = s.ValidTo_ShiftLocalDate,
            t.ManagerWorkerId0 = s.ManagerWorkerId0,
            t.ManagerWorkerId1 = s.ManagerWorkerId1,
            t.ManagerWorkerId2 = s.ManagerWorkerId2,
            t.ManagerWorkerId3 = s.ManagerWorkerId3,
            t.ManagerWorkerId4 = s.ManagerWorkerId4,
            t.ManagerWorkerId5 = s.ManagerWorkerId5,
            t.ManagerWorkerId6 = s.ManagerWorkerId6,
            t.ManagerWorkerId7 = s.ManagerWorkerId7,
            t.ManagerWorkerId8 = s.ManagerWorkerId8,
            t.ManagerWorkerId9 = s.ManagerWorkerId9,
            t.ManagerWorkerId10 = s.ManagerWorkerId10,
            t.ManagerWorkerId11 = s.ManagerWorkerId11,
            t.ManagerWorkerId12 = s.ManagerWorkerId12,
            t.WatermarkUTC = s.WatermarkUTC
        WHEN NOT MATCHED THEN INSERT (
            ProjectId, WorkerId, ValidFrom_ShiftLocalDate, ValidTo_ShiftLocalDate, Levels,
            ManagerWorkerId0, ManagerWorkerId1, ManagerWorkerId2, ManagerWorkerId3,
            ManagerWorkerId4, ManagerWorkerId5, ManagerWorkerId6, ManagerWorkerId7,
            ManagerWorkerId8, ManagerWorkerId9, ManagerWorkerId10, ManagerWorkerId11,
            ManagerWorkerId12, WatermarkUTC
        ) VALUES (
            s.ProjectId, s.WorkerId, s.ValidFrom_ShiftLocalDate, s.ValidTo_ShiftLocalDate, s.Levels,
            s.ManagerWorkerId0, s.ManagerWorkerId1, s.ManagerWorkerId2, s.ManagerWorkerId3,
            s.ManagerWorkerId4, s.ManagerWorkerId5, s.ManagerWorkerId6, s.ManagerWorkerId7,
            s.ManagerWorkerId8, s.ManagerWorkerId9, s.ManagerWorkerId10, s.ManagerWorkerId11,
            s.ManagerWorkerId12, s.WatermarkUTC
        )
    """

    spark.sql(merge_sql)
    print("  Phase A completed")

    # Phase B: DELETE for NOT MATCHED BY SOURCE (anti-join pattern)
    print("\nPhase B: DELETE NOT MATCHED BY SOURCE...")

    # Find records in target that are NOT in source for impacted workers
    delete_sql = f"""
        DELETE FROM {TARGET_TABLE}
        WHERE (ProjectId, WorkerId, ValidFrom_ShiftLocalDate) IN (
            SELECT t.ProjectId, t.WorkerId, t.ValidFrom_ShiftLocalDate
            FROM {TARGET_TABLE} t
            INNER JOIN impacted_area_view ia ON t.ProjectId = ia.ProjectId AND t.WorkerId = ia.WorkerId
            LEFT JOIN merge_source_view s ON t.ProjectId = s.ProjectId
                AND t.WorkerId = s.WorkerId
                AND t.ValidFrom_ShiftLocalDate = s.ValidFrom_ShiftLocalDate
            WHERE s.ProjectId IS NULL
        )
    """

    spark.sql(delete_sql)
    print("  Phase B completed")

    # Clean up temp views
    spark.catalog.dropTempView("merge_source_view")
    spark.catalog.dropTempView("impacted_area_view")

else:
    print(f"Creating new table: {TARGET_TABLE}")

    (result_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("ProjectId")
     .saveAsTable(TARGET_TABLE)
    )
    print(f"  Created table with {result_count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Update Watermarks

# COMMAND ----------

print("=" * 60)
print("STEP 7: Update Watermarks")
print("=" * 60)

# Update watermarks
final_count = spark.table(TARGET_TABLE).count()

update_watermark("sql_CalculateManagerAssignmentSnapshots", new_mgr_watermark, final_count)
update_watermark("sql_CrewAssignments[tracked for mgr snapshots]", new_crew_watermark, final_count)

print(f"Updated Manager Watermark to: {new_mgr_watermark}")
print(f"Updated Crew Watermark to: {new_crew_watermark}")
print(f"Total records in target: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 60)
print("VALIDATION")
print("=" * 60)

# Row counts
target_total = spark.table(TARGET_TABLE).count()
print(f"\nRow Counts:")
print(f"  Target (gold_manager_assignment_snapshots): {target_total:,}")

# Check for NULL primary key fields
print(f"\nData Quality Check - Required Field Nulls:")
quality_check = spark.sql(f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN ProjectId IS NULL THEN 1 ELSE 0 END) as null_project,
        SUM(CASE WHEN WorkerId IS NULL THEN 1 ELSE 0 END) as null_worker,
        SUM(CASE WHEN ValidFrom_ShiftLocalDate IS NULL THEN 1 ELSE 0 END) as null_valid_from
    FROM {TARGET_TABLE}
""")
display(quality_check)

# Level distribution
print(f"\nHierarchy Level Distribution:")
level_dist = spark.sql(f"""
    SELECT Levels, COUNT(*) as count
    FROM {TARGET_TABLE}
    GROUP BY Levels
    ORDER BY Levels
""")
display(level_dist)

# Sample output
print(f"\nSample Output:")
display(spark.table(TARGET_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Clean up cached DataFrames
ca_df.unpersist()
ca2_df.unpersist()
unpivoted_df.unpersist()
boundaries_df.unpersist()
result_df.unpersist()
impacted_area.unpersist()

# Final summary
print("=" * 60)
print("MANAGER ASSIGNMENT SNAPSHOTS - COMPLETE")
print("=" * 60)
print(f"""
Processing Summary:
  Load Mode:                {load_mode}
  Impacted Workers:         {impacted_area.count() if 'impacted_area' in dir() else 'N/A'}
  Unpivoted Records:        {total_unpivoted if 'total_unpivoted' in dir() else 'N/A'}
  Result Records:           {result_count if 'result_count' in dir() else 'N/A'}

Target Table:
  {TARGET_TABLE}
  Total rows: {final_count:,}

Watermarks Updated:
  - sql_CalculateManagerAssignmentSnapshots: {new_mgr_watermark}
  - sql_CrewAssignments[tracked for mgr snapshots]: {new_crew_watermark}

Converted from: stg.spCalculateManagerAssignmentSnapshots (266 lines)
Patterns: RECURSIVE CTE -> Iterative loop, PIVOT, Two-phase MERGE
""")

dbutils.notebook.exit("SUCCESS")
