# Stored Procedure Migration Workflow Reference

## Overview

This document provides reference material for the ADW_SP_plan_build_review_fix skill, including T-SQL to Databricks conversion patterns, common issues, and validation approaches.

## T-SQL to Databricks Conversion Patterns

### 1. CURSOR Pattern

**T-SQL (Source):**
```sql
DECLARE @WorkerId INT, @Timestamp DATETIME
DECLARE worker_cursor CURSOR FOR
    SELECT WorkerId, TimestampUTC
    FROM FactWorkersHistory
    ORDER BY WorkerId, TimestampUTC

OPEN worker_cursor
FETCH NEXT FROM worker_cursor INTO @WorkerId, @Timestamp

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Process row, compare with previous
    ...
    FETCH NEXT FROM worker_cursor INTO @WorkerId, @Timestamp
END

CLOSE worker_cursor
DEALLOCATE worker_cursor
```

**Databricks (Target):**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, row_number

# Define window
worker_window = Window.partitionBy("WorkerId").orderBy("TimestampUTC")

# Apply window functions instead of cursor iteration
df = (
    spark.table("silver.fact_workers_history")
    .withColumn("prev_timestamp", lag("TimestampUTC").over(worker_window))
    .withColumn("next_timestamp", lead("TimestampUTC").over(worker_window))
    .withColumn("row_num", row_number().over(worker_window))
    .withColumn("gap_seconds",
        (col("TimestampUTC").cast("long") - col("prev_timestamp").cast("long"))
    )
)
```

### 2. TEMP_TABLE Pattern

**T-SQL (Source):**
```sql
CREATE TABLE #TempWorkers (
    WorkerId INT,
    ProjectId INT,
    TotalHours DECIMAL(10,2)
)

INSERT INTO #TempWorkers
SELECT WorkerId, ProjectId, SUM(Hours)
FROM WorkerHours
GROUP BY WorkerId, ProjectId

SELECT * FROM #TempWorkers
WHERE TotalHours > 8

DROP TABLE #TempWorkers
```

**Databricks (Target):**
```python
# Option 1: Temporary View (scoped to session)
df_temp = (
    spark.table("silver.worker_hours")
    .groupBy("WorkerId", "ProjectId")
    .agg(F.sum("Hours").alias("TotalHours"))
)
df_temp.createOrReplaceTempView("temp_workers")

result = spark.sql("""
    SELECT * FROM temp_workers WHERE TotalHours > 8
""")

# Option 2: Cache DataFrame (for multiple uses)
df_temp.cache()
result = df_temp.filter(col("TotalHours") > 8)
df_temp.unpersist()

# Option 3: CTE in SQL
result = spark.sql("""
    WITH temp_workers AS (
        SELECT WorkerId, ProjectId, SUM(Hours) as TotalHours
        FROM silver.worker_hours
        GROUP BY WorkerId, ProjectId
    )
    SELECT * FROM temp_workers WHERE TotalHours > 8
""")
```

### 3. MERGE Pattern

**T-SQL (Source):**
```sql
MERGE INTO FactWorkersShifts AS target
USING (SELECT * FROM #StagedData) AS source
ON target.WorkerId = source.WorkerId
   AND target.ShiftDate = source.ShiftDate
WHEN MATCHED THEN
    UPDATE SET
        target.Hours = source.Hours,
        target.UpdatedAt = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (WorkerId, ShiftDate, Hours, CreatedAt)
    VALUES (source.WorkerId, source.ShiftDate, source.Hours, GETDATE());
```

**Databricks (Target):**
```python
from delta.tables import DeltaTable

# Load target Delta table
target = DeltaTable.forName(spark, "gold.fact_workers_shifts")

# Load source data
source = spark.table("staged_data")

# Perform merge
(
    target.alias("target")
    .merge(source.alias("source"),
           "target.WorkerId = source.WorkerId AND target.ShiftDate = source.ShiftDate")
    .whenMatchedUpdate(set={
        "Hours": col("source.Hours"),
        "UpdatedAt": current_timestamp()
    })
    .whenNotMatchedInsert(values={
        "WorkerId": col("source.WorkerId"),
        "ShiftDate": col("source.ShiftDate"),
        "Hours": col("source.Hours"),
        "CreatedAt": current_timestamp()
    })
    .execute()
)
```

### 4. SPATIAL Pattern

**T-SQL (Source):**
```sql
DECLARE @Point1 geography = geography::Point(@Lat1, @Lon1, 4326)
DECLARE @Point2 geography = geography::Point(@Lat2, @Lon2, 4326)

SELECT @Point1.STDistance(@Point2) AS DistanceMeters
```

**Databricks (Target):**
```python
import h3
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType
import math

# Haversine distance UDF
@udf(returnType=DoubleType())
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate great-circle distance in meters."""
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None

    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c

# H3 indexing for efficient spatial joins
@udf(returnType=StringType())
def lat_lon_to_h3(lat, lon, resolution=9):
    """Convert lat/lon to H3 index."""
    if lat is None or lon is None:
        return None
    return h3.geo_to_h3(lat, lon, resolution)

# Register UDFs
spark.udf.register("haversine_distance", haversine_distance)
spark.udf.register("lat_lon_to_h3", lat_lon_to_h3)

# Usage
df = df.withColumn("distance_meters",
    haversine_distance(col("lat1"), col("lon1"), col("lat2"), col("lon2"))
)
```

### 5. DYNAMIC_SQL Pattern

**T-SQL (Source):**
```sql
DECLARE @SQL NVARCHAR(MAX)
SET @SQL = 'SELECT * FROM ' + @TableName + ' WHERE ProjectId = ' + CAST(@ProjectId AS VARCHAR)
EXEC sp_executesql @SQL
```

**Databricks (Target):**
```python
# Option 1: Parameterized query (preferred for security)
table_name = dbutils.widgets.get("table_name")
project_id = int(dbutils.widgets.get("project_id"))

# Validate table name against allowlist
ALLOWED_TABLES = ["silver.workers", "silver.projects", "silver.zones"]
if table_name not in ALLOWED_TABLES:
    raise ValueError(f"Invalid table: {table_name}")

df = spark.table(table_name).filter(col("ProjectId") == project_id)

# Option 2: Format string (use only with validated inputs)
query = f"SELECT * FROM {table_name} WHERE ProjectId = {project_id}"
df = spark.sql(query)
```

## Data Type Mappings

| T-SQL Type | Databricks Type | Notes |
|------------|-----------------|-------|
| `VARCHAR(n)` | `STRING` | No length limit in Spark |
| `NVARCHAR(n)` | `STRING` | UTF-8 by default |
| `NVARCHAR(MAX)` | `STRING` | No special handling needed |
| `INT` | `INT` | Same |
| `BIGINT` | `BIGINT` | Same |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Preserve precision |
| `MONEY` | `DECIMAL(19,4)` | Fixed precision |
| `FLOAT` | `DOUBLE` | 64-bit float |
| `REAL` | `FLOAT` | 32-bit float |
| `BIT` | `BOOLEAN` | 0/1 -> false/true |
| `DATETIME` | `TIMESTAMP` | Millisecond precision |
| `DATETIME2` | `TIMESTAMP` | Up to nanosecond |
| `DATE` | `DATE` | Same |
| `TIME` | `STRING` | No TIME type in Spark |
| `UNIQUEIDENTIFIER` | `STRING` | Store as string |
| `GEOGRAPHY` | `STRING` | Store as WKT |
| `GEOMETRY` | `STRING` | Store as WKT |
| `VARBINARY` | `BINARY` | Same |
| `XML` | `STRING` | Store as string |

## Function Mappings

| T-SQL Function | Databricks Equivalent |
|----------------|----------------------|
| `GETDATE()` | `current_timestamp()` |
| `GETUTCDATE()` | `current_timestamp()` (Spark uses UTC) |
| `ISNULL(a, b)` | `coalesce(a, b)` |
| `LEN(s)` | `length(s)` |
| `CHARINDEX(a, b)` | `locate(a, b)` |
| `DATEDIFF(day, a, b)` | `datediff(b, a)` |
| `DATEADD(day, n, d)` | `date_add(d, n)` |
| `CONVERT(type, val)` | `cast(val as type)` |
| `CAST(val AS type)` | `cast(val as type)` |
| `LEFT(s, n)` | `left(s, n)` |
| `RIGHT(s, n)` | `right(s, n)` |
| `SUBSTRING(s, start, len)` | `substring(s, start, len)` |
| `UPPER(s)` | `upper(s)` |
| `LOWER(s)` | `lower(s)` |
| `LTRIM(s)` | `ltrim(s)` |
| `RTRIM(s)` | `rtrim(s)` |
| `REPLACE(s, a, b)` | `replace(s, a, b)` |
| `STUFF(s, start, len, new)` | `concat(left(s, start-1), new, substring(s, start+len))` |
| `ROUND(n, d)` | `round(n, d)` |
| `ABS(n)` | `abs(n)` |
| `CEILING(n)` | `ceil(n)` |
| `FLOOR(n)` | `floor(n)` |
| `POWER(a, b)` | `pow(a, b)` |
| `SQUARE(n)` | `pow(n, 2)` |
| `SQRT(n)` | `sqrt(n)` |
| `@@ROWCOUNT` | Use DataFrame `.count()` |
| `@@IDENTITY` | Not applicable (use generated columns) |
| `NEWID()` | `uuid()` |
| `ROW_NUMBER()` | `row_number()` |
| `RANK()` | `rank()` |
| `DENSE_RANK()` | `dense_rank()` |
| `LAG()` | `lag()` |
| `LEAD()` | `lead()` |
| `FIRST_VALUE()` | `first()` |
| `LAST_VALUE()` | `last()` |

## Common Validation Queries

### Row Count Comparison
```sql
-- Source (SQL Server)
SELECT COUNT(*) FROM dbo.FactWorkersHistory

-- Target (Databricks)
SELECT COUNT(*) FROM wakecap_prod.silver.silver_fact_workers_history
```

### Aggregate Comparison
```sql
-- Source
SELECT ProjectId, COUNT(*) as cnt, SUM(Hours) as total_hours
FROM dbo.FactWorkersShifts
GROUP BY ProjectId
ORDER BY ProjectId

-- Target
SELECT ProjectId, COUNT(*) as cnt, SUM(Hours) as total_hours
FROM wakecap_prod.gold.gold_fact_workers_shifts
GROUP BY ProjectId
ORDER BY ProjectId
```

### Sample Record Comparison
```python
# Get sample from source
sample_ids = source_df.select("WorkerId").limit(10).collect()
sample_id_list = [r.WorkerId for r in sample_ids]

# Compare with target
source_sample = source_df.filter(col("WorkerId").isin(sample_id_list))
target_sample = target_df.filter(col("WorkerId").isin(sample_id_list))

# Check if data matches
diff = source_sample.exceptAll(target_sample)
if diff.count() > 0:
    print("MISMATCH FOUND:")
    diff.show()
else:
    print("MATCH: All sample records match")
```

## Complexity Scoring

| Pattern | Complexity Score | Conversion Approach |
|---------|-----------------|---------------------|
| Simple SELECT | 1 | Direct transpilation |
| JOIN (2-3 tables) | 2 | Direct transpilation |
| CTE | 2 | Direct transpilation |
| Window Functions | 3 | Direct transpilation |
| MERGE | 3 | DeltaTable.merge() |
| PIVOT/UNPIVOT | 4 | pivot()/unpivot() functions |
| CURSOR | 5 | Window functions + careful analysis |
| TEMP_TABLE (1-2) | 3 | Temp views or cache |
| TEMP_TABLE (3+) | 5 | Consider notebook cells |
| DYNAMIC_SQL | 5 | Manual review required |
| SPATIAL | 6 | H3 + UDFs, manual review |
| Recursive CTE | 6 | GraphX or iterative approach |
| TRANSACTION | 4 | Delta Lake handles ACID |

**Total Score Interpretation:**
- 1-5: Simple - Auto-convert with high confidence
- 6-10: Medium - Auto-convert with manual review
- 11-15: Complex - Significant manual work expected
- 16+: Very Complex - Consider breaking into multiple notebooks

## WakeCap-Specific Table Mappings

### Bronze Layer (Raw)
```
wakecap_prod.raw.timescale_company        <- TimescaleDB Company table
wakecap_prod.raw.timescale_people         <- TimescaleDB People table
wakecap_prod.raw.timescale_zone           <- TimescaleDB Zone table
wakecap_prod.raw.timescale_space          <- TimescaleDB Space table (Floor)
wakecap_prod.raw.timescale_devicelocation <- TimescaleDB DeviceLocation
```

### Silver Layer (Cleaned)
```
wakecap_prod.silver.silver_organization   <- Filtered Company (Type='organization')
wakecap_prod.silver.silver_project        <- Filtered Company (Type='project')
wakecap_prod.silver.silver_worker         <- People table
wakecap_prod.silver.silver_floor          <- Space table
wakecap_prod.silver.silver_zone           <- Zone table
wakecap_prod.silver.silver_crew           <- Crew table
wakecap_prod.silver.silver_device         <- AvlDevice table
```

### Gold Layer (Aggregated)
```
wakecap_prod.gold.gold_fact_workers_history    <- SP output
wakecap_prod.gold.gold_fact_workers_shifts     <- SP output
wakecap_prod.gold.gold_fact_observations       <- SP output
wakecap_prod.gold.gold_worker_daily_summary    <- View
```

## Stored Procedure Priority List

Based on the migration assessment, here are the SPs by priority:

### High Priority (Critical Business Logic)
1. `stg.spCalculateFactWorkersShifts` (1639 lines) - CURSOR, TEMP_TABLE
2. `stg.spDeltaSyncFactWorkersHistory` (1561 lines) - TEMP_TABLE, SPATIAL
3. `stg.spDeltaSyncFactObservations` (1165 lines) - TEMP_TABLE, MERGE
4. `stg.spCalculateFactWorkersContacts_ByRule` (951 lines) - CURSOR, DYNAMIC_SQL
5. `mrg.spMergeOldData` (903 lines) - CURSOR, SPATIAL

### Medium Priority (Staging)
- `stg.spStageWorkers` - MERGE pattern
- `stg.spStageProjects` - MERGE pattern
- `stg.spStageCrews` - MERGE pattern
- `stg.spDeltaSync*` series - CDC patterns

### Lower Priority (Admin/Maintenance)
- `dbo.spRebuildIndex*` - Not needed (Delta handles)
- `dbo.spMaintainPartitions*` - Not needed (Delta handles)
- `dbo.spCleanup*` - Replace with VACUUM/OPTIMIZE
