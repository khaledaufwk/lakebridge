# Implementation Plan: stg.spCalculateFactProgress Migration

## SP Analysis Summary

### Source Stored Procedure
- **File**: `migration_project/source_sql/stored_procedures/stg.spCalculateFactProgress.sql`
- **Line Count**: 113 lines
- **Complexity**: 5/10 (MEDIUM)
- **Purpose**: Delta sync from TimescaleDB ResourceTimesheet to FactProgress with Worker/Project dimension lookups

### Related SP
- **stg.spDeltaSyncFactProgress**: Syncs from `wc2023_ResourceApprovedHoursSegment` to proxy table (not needed for Databricks - we use direct source)

### Detected Patterns and Conversion Approach

| Pattern | Description | Conversion |
|---------|-------------|------------|
| **MERGE** | Full MERGE with UPDATE/INSERT/DELETE | `DeltaTable.merge()` |
| **DIMENSION_LOOKUP** | Worker (INNER), Project (INNER) | DataFrame broadcast joins |
| **ROW_NUMBER** | Deduplication on composite key | PySpark Window functions |
| **CALCULATED_COLUMNS** | ApprovedTime, Overtime (sec->days) | DataFrame withColumn |
| **FLOAT_TOLERANCE** | 0.00001 comparison | SQL ABS() in merge condition |
| **SOFT_DELETE** | DeleteFlag pattern | MERGE whenNotMatchedBySource |
| **fnExtSourceIDAlias** | Source ID grouping | Inline CASE expression |

### Input Tables

| SQL Server Table | Databricks Table | Join Type | Purpose |
|------------------|------------------|-----------|---------|
| `stg.wc2023_ResourceTimesheet_full` | `wakecap_prod.raw.timescale_resourcetimesheet` | Source | Raw timesheet data |
| `dbo.Worker` | `wakecap_prod.silver.silver_worker` | INNER | WorkerID resolution |
| `dbo.Project` | `wakecap_prod.silver.silver_project_dw` | INNER | ProjectID resolution |

### Output Table

| SQL Server Table | Databricks Table | Primary Key |
|------------------|------------------|-------------|
| `dbo.FactProgress` | `wakecap_prod.gold.fact_progress` | (ProjectID, ActivityID, ShiftLocalDate, WorkerID) |

---

## Target Table Schema: dbo.FactProgress

```sql
CREATE TABLE dbo.FactProgress (
    ProjectID int NOT NULL,
    ActivityID int NOT NULL,       -- Always -1 for this source
    WorkerID int NOT NULL,
    ShiftLocalDate date NOT NULL,
    ApprovedTime float NULL,       -- Calculated: ApprovedSeconds / 86400
    Overtime float NULL,           -- Calculated: OverTimeSeconds / 86400
    CreatedAt datetime NULL,
    UpdatedAt datetime NULL,
    DeletedAt datetime NULL,
    WatermarkUTC datetime NULL,
    ExtSourceID int NULL,          -- 15 for TimescaleDB
    DeleteFlag bit NULL
);
-- Primary Key: (ProjectID, ActivityID, ShiftLocalDate, WorkerID)
```

---

## Conversion Steps

### Step 1: Configuration and Setup

```python
# Catalog configuration
TARGET_CATALOG = "wakecap_prod"
RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Tables
SOURCE_TABLE = f"{TARGET_CATALOG}.{RAW_SCHEMA}.timescale_resourcetimesheet"
DIM_WORKER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"
DIM_PROJECT = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project_dw"
TARGET_TABLE = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.fact_progress"

# Constants
EXT_SOURCE_ID = 15  # TimescaleDB
ACTIVITY_ID = -1    # Constant for ResourceTimesheet source
FLOAT_TOLERANCE = 0.00001
```

### Step 2: Load Dimension Tables with Deduplication

```python
def ext_source_id_alias(ext_source_id):
    """fnExtSourceIDAlias equivalent"""
    return F.when(F.col(ext_source_id).isin(14,15,16,17,18,19), 15) \
            .when(F.col(ext_source_id).isin(1,2,11,12), 2) \
            .otherwise(F.col(ext_source_id))

# Worker dimension with ROW_NUMBER dedup
window = Window.partitionBy("ExtWorkerID", ext_source_id_alias("ExtSourceID")).orderBy(F.lit(1))
dim_worker = (spark.table(DIM_WORKER)
    .withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
    .filter(F.col("ExtSourceIDAlias") == 15)  # Target source alias
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .select("WorkerID", "ExtWorkerID"))

# Project dimension with ROW_NUMBER dedup
window = Window.partitionBy("ExtProjectID", ext_source_id_alias("ExtSourceID")).orderBy(F.lit(1))
dim_project = (spark.table(DIM_PROJECT)
    .withColumn("ExtSourceIDAlias", ext_source_id_alias("ExtSourceID"))
    .filter(F.col("ExtSourceIDAlias") == 15)
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .select("ProjectID", "ExtProjectID"))
```

### Step 3: Build Source DataFrame

```python
# Load source with calculated columns
source_df = (spark.table(SOURCE_TABLE)
    .filter(F.col("DeletedAt").isNull())  # Only non-deleted
    .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID))
    .withColumn("ActivityID", F.lit(ACTIVITY_ID))
    .withColumn("ApprovedTime", F.col("ApprovedSeconds") / 86400.0)
    .withColumn("Overtime", F.col("OverTimeSeconds") / 86400.0)
)

# Join dimensions (INNER)
source_df = source_df.join(
    F.broadcast(dim_worker),
    source_df.ResourceId == dim_worker.ExtWorkerID,
    "inner"
)
source_df = source_df.join(
    F.broadcast(dim_project).alias("p"),
    source_df.ProjectId == F.col("p.ExtProjectID"),
    "inner"
).withColumn("ProjectID2", F.col("p.ProjectID"))

# Deduplication with ROW_NUMBER
window = Window.partitionBy("ResourceId", "Day", "ProjectId", "ActivityID").orderBy(F.desc("Id"))
source_df = source_df.withColumn("RN", F.row_number().over(window))
source_df = source_df.filter(F.col("RN") == 1)
```

### Step 4: MERGE into FactProgress

```python
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS t
USING source_view AS s
ON s.WorkerID = t.WorkerID
   AND s.Day = t.ShiftLocalDate
   AND s.ProjectID2 = t.ProjectID
   AND s.ActivityID = t.ActivityID
   AND CASE WHEN s.ExtSourceID IN (14,15,16,17,18,19) THEN 15
            WHEN s.ExtSourceID IN (1,2,11,12) THEN 2
            ELSE s.ExtSourceID END
     = CASE WHEN t.ExtSourceID IN (14,15,16,17,18,19) THEN 15
            WHEN t.ExtSourceID IN (1,2,11,12) THEN 2
            ELSE t.ExtSourceID END
WHEN MATCHED AND (
    (ABS(COALESCE(t.ApprovedTime,0) - COALESCE(s.ApprovedTime,0)) > {FLOAT_TOLERANCE}) OR
    (ABS(COALESCE(t.Overtime,0) - COALESCE(s.Overtime,0)) > {FLOAT_TOLERANCE}) OR
    (t.ApprovedTime IS NULL AND s.ApprovedTime IS NOT NULL) OR
    (t.ApprovedTime IS NOT NULL AND s.ApprovedTime IS NULL) OR
    (t.Overtime IS NULL AND s.Overtime IS NOT NULL) OR
    (t.Overtime IS NOT NULL AND s.Overtime IS NULL)
)
THEN UPDATE SET
    t.WatermarkUTC = current_timestamp(),
    t.ActivityID = s.ActivityID,
    t.ApprovedTime = s.ApprovedTime,
    t.ExtSourceID = s.ExtSourceID,
    t.Overtime = s.Overtime
WHEN NOT MATCHED BY TARGET
THEN INSERT (ActivityID, ApprovedTime, ShiftLocalDate, ExtSourceID, Overtime, ProjectID, WorkerID, WatermarkUTC)
VALUES (s.ActivityID, s.ApprovedTime, s.Day, s.ExtSourceID, s.Overtime, s.ProjectID2, s.WorkerID, current_timestamp())
WHEN NOT MATCHED BY SOURCE AND t.ExtSourceID = 15
THEN UPDATE SET t.DeleteFlag = 1
"""
```

### Step 5: Cleanup Soft-Deleted Records

```python
spark.sql(f"DELETE FROM {TARGET_TABLE} WHERE DeleteFlag = 1")
```

---

## Expected Output File

**Path:** `migration_project/pipelines/gold/notebooks/delta_sync_fact_progress.py`

---

## Validation Commands

### Row Count Comparison
```sql
-- SQL Server
SELECT COUNT(*) FROM dbo.FactProgress WHERE ExtSourceID = 15;

-- Databricks
SELECT COUNT(*) FROM wakecap_prod.gold.fact_progress WHERE ExtSourceID = 15;
```

### Sum Comparison
```sql
-- SQL Server
SELECT SUM(ApprovedTime), SUM(Overtime) FROM dbo.FactProgress WHERE ExtSourceID = 15;

-- Databricks
SELECT SUM(ApprovedTime), SUM(Overtime) FROM wakecap_prod.gold.fact_progress WHERE ExtSourceID = 15;
```

### Distinct Workers/Projects
```sql
SELECT
    COUNT(DISTINCT WorkerID) as workers,
    COUNT(DISTINCT ProjectID) as projects
FROM wakecap_prod.gold.fact_progress
WHERE ExtSourceID = 15;
```

---

## Acceptance Criteria

| Criteria | Target | Validation |
|----------|--------|------------|
| Row count match | >= 99% | Count comparison |
| ApprovedTime sum match | >= 99.9% | Sum comparison with tolerance |
| Overtime sum match | >= 99.9% | Sum comparison with tolerance |
| No NULL in required keys | 0 nulls | Data quality check |
| No duplicates | 0 | Distinct count check |

---

## Dependencies

### Required Tables
- `wakecap_prod.raw.timescale_resourcetimesheet` (or similar source)
- `wakecap_prod.silver.silver_worker`
- `wakecap_prod.silver.silver_project_dw`

---

## Notes

1. **ActivityID = -1**: This source always uses -1 for ActivityID (unlike ApprovedHoursSegment which has actual Activity)
2. **Soft Delete Pattern**: Original uses DeleteFlag, we preserve this pattern
3. **fnExtSourceIDAlias**: Groups sources (14-19)->15, (1-12)->2 for cross-source matching
4. **Float Tolerance**: Use 0.00001 for ApprovedTime/Overtime comparisons
