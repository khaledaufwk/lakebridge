# WakeCapDW Migration Plan: Stored Procedures, Views & Functions

**Created:** 2026-01-19
**Last Updated:** 2026-01-20
**Target:** Databricks Unity Catalog (wakecap_prod.migration)
**Development Approach:** Claude-Driven Development

---

## Raw Zone Data Source

**Primary Source:** TimescaleDB (connection details in `credentials_template.yml`)
**Loading Method:** Incremental (new records only via watermark-based extraction)

The Raw Zone (Bronze Layer) is populated incrementally from TimescaleDB using watermark columns to track and load only new records. This ensures efficient data loading without full table scans.

See `MIGRATION_PLAN.md` Phase 0 for detailed implementation.

---

## Executive Summary

| Object Type | Total | Transpiled | Pending | Complexity |
|-------------|-------|------------|---------|------------|
| Views | 34 | 33 (97%) | 1 | LOW |
| Functions | 23 | 0 (0%) | 23 | MEDIUM-HIGH |
| Stored Procedures | 70 | 0 (0%) | 70 | HIGH |

**Key Challenge Patterns:**
- CURSOR logic (10 procedures) → Convert to set-based operations
- SPATIAL/Geography (18 objects) → H3 library or Python UDFs
- DYNAMIC SQL (5 procedures) → Parameterized SQL templates
- TEMP TABLES (33 procedures) → Delta temp views or CTEs
- Security predicates (4 functions) → Unity Catalog row filters

---

## Claude-Driven Development Approach

This migration will use **Claude Code as the primary developer** to convert all stored procedures, views, and functions. The workflow is designed for iterative, Claude-assisted development.

### Development Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  1. USER: Points Claude to source SQL file                      │
│     "Convert migration_project/source_sql/stored_procedures/    │
│      stg.spCalculateFactWorkersShifts.sql to Databricks"        │
├─────────────────────────────────────────────────────────────────┤
│  2. CLAUDE: Reads source SQL, analyzes patterns                 │
│     - Identifies CURSOR, TEMP_TABLE, MERGE patterns             │
│     - Determines target format (DLT/Notebook/UDF)               │
├─────────────────────────────────────────────────────────────────┤
│  3. CLAUDE: Generates Databricks-compatible code                │
│     - Converts T-SQL to Spark SQL/Python                        │
│     - Replaces cursors with DataFrame operations                │
│     - Creates DLT table definitions or Python notebooks         │
├─────────────────────────────────────────────────────────────────┤
│  4. CLAUDE: Writes output to target location                    │
│     - DLT tables → migration_project/pipelines/dlt/             │
│     - Notebooks → migration_project/pipelines/notebooks/        │
│     - UDFs → migration_project/pipelines/udfs/                  │
├─────────────────────────────────────────────────────────────────┤
│  5. CLAUDE: Updates tracking in MIGRATION_STATUS.md             │
│     - Marks procedure as CONVERTED                              │
│     - Notes any manual review needed                            │
└─────────────────────────────────────────────────────────────────┘
```

### Claude Commands for Migration

| Command | Description |
|---------|-------------|
| `/build migration_project/plan.md` | Execute full migration plan |
| `Convert <file> to Databricks` | Convert single SQL object |
| `Analyze <file> complexity` | Get conversion strategy recommendation |
| `Generate UDF for <function>` | Create Python UDF from SQL function |
| `Create DLT table for <procedure>` | Convert procedure to DLT table |

### Batch Conversion Prompts

**Convert all DeltaSync procedures:**
```
Read all stored procedures in migration_project/source_sql/stored_procedures/
that start with "spDeltaSync" and convert them to DLT streaming tables.
Write output to migration_project/pipelines/dlt/delta_sync_tables.py
```

**Convert all spatial functions:**
```
Read all functions in migration_project/source_sql/functions/ that use
geography/geometry types. Convert to Python UDFs using H3 and Shapely.
Write output to migration_project/pipelines/udfs/spatial_udfs.py
```

**Convert all views to Gold layer:**
```
Read all transpiled views in migration_project/transpiled/views/ and
generate DLT table definitions for the Gold layer.
Write output to migration_project/pipelines/dlt/gold_views.py
```

---

## Phase 1: Views Migration (Claude-Driven)

### Status: 97% Complete

All 33 business views have been transpiled to Databricks SQL. Only system view skipped.

### 1.1 Deploy Transpiled Views to Gold Layer

**Claude Prompt:**
```
Read all view files in migration_project/transpiled/views/ and generate
a DLT Gold layer notebook. Output to migration_project/pipelines/dlt/gold_views.py
```

**Action:** Add all transpiled views to DLT pipeline as Gold layer tables.

| Priority | View | Lines | Target |
|----------|------|-------|--------|
| HIGH | vwFactWorkersShifts_Ext | 487 | gold_vwFactWorkersShifts_Ext |
| HIGH | vwFactWorkersShiftsCombined_Ext | 487 | gold_vwFactWorkersShiftsCombined_Ext |
| HIGH | vwWorker | 64 | gold_vwWorker |
| HIGH | vwFactWorkersHistory | 29 | gold_vwFactWorkersHistory |
| MEDIUM | vwFloor | 96 | gold_vwFloor (spatial handling needed) |
| MEDIUM | vwZone | 27 | gold_vwZone (spatial handling needed) |
| MEDIUM | vwDeviceAssignment_Continuous | 44 | gold_vwDeviceAssignment_Continuous |
| MEDIUM | vwManagerAssignments_Expanded | 66 | gold_vwManagerAssignments_Expanded |
| LOW | All remaining 25 views | - | gold_{view_name} |

### 1.2 Views with Spatial Data (Special Handling)

| View | Issue | Resolution |
|------|-------|------------|
| vwFloor | Geography column | Exclude or convert to WKT string |
| vwZone | Geography column | Exclude or convert to WKT string |

**Temporary Solution:** Cast geography to string for initial migration:
```sql
CAST(Geometry AS STRING) AS GeometryWKT
```

### 1.3 Merge Schema Views (mrg.*)

| View | Purpose | Action |
|------|---------|--------|
| mrg.vwFactReportedAttendance | Merge replication | Convert to standard view |
| mrg.vwProject | Merge replication | Convert to standard view |
| mrg.vwZone | Merge replication | Convert to standard view |
| mrg.vwFloor | Merge replication | Convert to standard view |
| mrg.vwWorkShiftDetails | Merge replication | Convert to standard view |
| mrg.vwContactTracingRule | Merge replication | Convert to standard view |

**Note:** mrg schema views may not be needed if using ADF for replication.

---

## Phase 2: Functions Migration (Claude-Driven)

### Claude Batch Prompts for Functions

**Prompt 1: Simple Functions (Native Databricks)**
```
Convert these SQL Server functions to Databricks SQL UDFs:
- migration_project/source_sql/functions/dbo.fnStripNonNumerics.sql
- migration_project/source_sql/functions/dbo.fnExtractPattern.sql
- migration_project/source_sql/functions/dbo.fnAtTimeZone.sql
- migration_project/source_sql/functions/stg.fnCalcTimeCategory.sql

Use native Databricks functions (regexp_replace, regexp_extract, from_utc_timestamp).
Output to migration_project/pipelines/udfs/simple_udfs.sql
```

**Prompt 2: Spatial Functions (Python UDFs)**
```
Convert all spatial/geography functions in migration_project/source_sql/functions/
to Python UDFs using H3 and Shapely libraries. Functions to convert:
- dbo.fnGeometry2SVG, dbo.fnGeometry2JSON, dbo.fnGeoPointShiftScale
- dbo.fnCalcDistanceNearby, dbo.fnFixGeographyOrder
- stg.fnNearestNeighbor_3Ordered

Output to migration_project/pipelines/udfs/spatial_udfs.py
Include cluster init script for installing h3-py and shapely.
```

**Prompt 3: Security Predicates (Unity Catalog Row Filters)**
```
Analyze security predicate functions in migration_project/source_sql/functions/:
- security.fn_OrganizationPredicate.sql
- security.fn_ProjectPredicate.sql
- security.fn_UserPredicate.sql

Generate Unity Catalog row filter SQL statements.
Output to migration_project/pipelines/security/row_filters.sql
```

### 2.1 Function Categories & Conversion Strategy

| Category | Count | Strategy | Effort |
|----------|-------|----------|--------|
| String/Pattern | 2 | Native Spark functions | LOW |
| Time/Timezone | 2 | Native Spark functions | LOW |
| Source Mapping | 1 | Lookup table | LOW |
| Category Calculation | 1 | SQL CASE expression | LOW |
| Spatial/Geometry | 8 | Python UDFs + H3 | HIGH |
| Hierarchical | 2 | Python UDFs | MEDIUM |
| SVG/JSON Generation | 3 | Python UDFs | MEDIUM |
| Security Predicates | 4 | Unity Catalog Row Filters | HIGH |

---

### 2.2 LOW Effort Functions (Native Databricks)

#### fnStripNonNumerics → regexp_replace
```sql
-- Original: dbo.fnStripNonNumerics(@input)
-- Databricks:
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_strip_non_numerics(input STRING)
RETURNS STRING
RETURN regexp_replace(input, '[^0-9]', '');
```

#### fnExtractPattern → regexp_extract
```sql
-- Original: dbo.fnExtractPattern(@input, @pattern)
-- Databricks:
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_extract_pattern(input STRING, pattern STRING)
RETURNS STRING
RETURN regexp_extract(input, pattern, 0);
```

#### fnAtTimeZone → from_utc_timestamp
```sql
-- Original: dbo.fnAtTimeZone(@datetime, @timezone)
-- Databricks:
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_at_timezone(dt TIMESTAMP, tz STRING)
RETURNS TIMESTAMP
RETURN from_utc_timestamp(dt, tz);
```

#### fnCalcTimeCategory → SQL CASE
```sql
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_calc_time_category(hour_of_day INT)
RETURNS INT
RETURN CASE
    WHEN hour_of_day BETWEEN 6 AND 11 THEN 1   -- Morning
    WHEN hour_of_day BETWEEN 12 AND 17 THEN 2  -- Afternoon
    WHEN hour_of_day BETWEEN 18 AND 21 THEN 3  -- Evening
    ELSE 4                                      -- Night
END;
```

#### fnExtSourceIDAlias → Lookup or simple UDF
```sql
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_ext_source_id_alias(source_id STRING)
RETURNS STRING
RETURN COALESCE(source_id, 'UNKNOWN');
```

---

### 2.3 MEDIUM Effort Functions (Python UDFs)

#### fnManagersByLevel (Hierarchical)
```python
# Register as Python UDF for hierarchical manager traversal
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType

@udf(returnType=ArrayType(StructType([
    StructField("ManagerID", IntegerType()),
    StructField("Level", IntegerType()),
    StructField("ManagerName", StringType())
])))
def fn_managers_by_level(worker_id, max_level=10):
    """Traverse manager hierarchy up to max_level."""
    # Implementation requires access to manager assignments table
    # Best implemented as a DLT table with recursive logic
    pass
```

**Recommendation:** Convert to DLT table that pre-computes hierarchy:
```python
@dlt.table(name="manager_hierarchy")
def manager_hierarchy():
    """Pre-computed manager hierarchy for all workers."""
    # Use Spark GraphX or iterative joins for hierarchy
    pass
```

#### fnProjectAutoSVG / fnProjectAutoJSON
```python
# Python UDF for SVG generation
@udf(returnType=StringType())
def fn_project_auto_svg(floors_json, zones_json, scale=1.0):
    """Generate SVG from floor and zone geometries."""
    import json
    # Parse geometries and generate SVG path elements
    # Implementation depends on geometry format
    return "<svg>...</svg>"
```

---

### 2.4 HIGH Effort Functions (Spatial - H3 Library)

#### Install H3 Library
```python
# In Databricks notebook or cluster init script
%pip install h3-py
```

#### fnCalcDistanceNearby → H3 + Haversine
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import h3
from math import radians, sin, cos, sqrt, atan2

@udf(returnType=DoubleType())
def fn_calc_distance_nearby(lat1, lon1, lat2, lon2):
    """Calculate distance in meters using Haversine formula."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = radians(lat1), radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)

    a = sin(delta_phi/2)**2 + cos(phi1) * cos(phi2) * sin(delta_lambda/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))

    return R * c

# Register for SQL use
spark.udf.register("fn_calc_distance_nearby", fn_calc_distance_nearby)
```

#### fnGeometry2SVG → Shapely UDF
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from shapely import wkt
from shapely.geometry import mapping

@udf(returnType=StringType())
def fn_geometry_to_svg(wkt_string, width=100, height=100):
    """Convert WKT geometry to SVG path."""
    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        bounds = geom.bounds
        # Generate SVG path from geometry coordinates
        # ... implementation
        return f'<svg viewBox="{bounds}">{path}</svg>'
    except:
        return None

spark.udf.register("fn_geometry_to_svg", fn_geometry_to_svg)
```

#### fnGeoPointShiftScale
```python
@udf(returnType=StringType())
def fn_geo_point_shift_scale(wkt_string, shift_x, shift_y, scale):
    """Transform coordinates by shifting and scaling."""
    from shapely import wkt
    from shapely.affinity import translate, scale as shapely_scale

    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        geom = translate(geom, xoff=shift_x, yoff=shift_y)
        geom = shapely_scale(geom, xfact=scale, yfact=scale)
        return geom.wkt
    except:
        return None

spark.udf.register("fn_geo_point_shift_scale", fn_geo_point_shift_scale)
```

---

### 2.5 Security Predicate Functions → Unity Catalog Row Filters

#### Current SQL Server RLS Functions:
- `security.fn_OrganizationPredicate` - Filter by organization
- `security.fn_ProjectPredicate` - Filter by project
- `security.fn_ProjectPredicateEx` - Extended project filter
- `security.fn_UserPredicate` - Filter by user

#### Unity Catalog Row Filter Implementation:

```sql
-- Step 1: Create a user-organization mapping table
CREATE TABLE wakecap_prod.security.user_organization_access (
    user_email STRING,
    organization_id INT,
    access_level STRING
);

-- Step 2: Create row filter function
CREATE OR REPLACE FUNCTION wakecap_prod.security.organization_filter(org_id INT)
RETURNS BOOLEAN
RETURN EXISTS (
    SELECT 1 FROM wakecap_prod.security.user_organization_access
    WHERE user_email = current_user()
    AND organization_id = org_id
);

-- Step 3: Apply row filter to table
ALTER TABLE wakecap_prod.migration.silver_worker
SET ROW FILTER wakecap_prod.security.organization_filter ON (OrganizationID);
```

**Alternative:** Implement in application layer if Unity Catalog row filters are too limiting.

---

## Phase 3: Stored Procedures Migration (Claude-Driven)

### Claude Batch Prompts for Stored Procedures

**Prompt 1: DeltaSync Procedures → DLT Streaming Tables**
```
Convert all spDeltaSync* stored procedures in migration_project/source_sql/stored_procedures/
to DLT streaming tables using APPLY CHANGES INTO pattern.

Procedures to convert (27 total):
- stg.spDeltaSyncDimWorker, stg.spDeltaSyncDimProject, stg.spDeltaSyncDimCrew
- stg.spDeltaSyncDimDevice, stg.spDeltaSyncFactObservations, etc.

Source data comes from ADLS Parquet files at:
abfss://raw@<storage>.dfs.core.windows.net/wakecap/{dimensions|facts|assignments}/

Output to migration_project/pipelines/dlt/streaming_tables.py
Use cloudFiles Auto Loader for streaming ingestion.
```

**Prompt 2: Calculate Procedures → DLT Batch Tables**
```
Convert these calculation procedures to DLT batch tables:
- migration_project/source_sql/stored_procedures/stg.spCalculateFactReportedAttendance.sql
- migration_project/source_sql/stored_procedures/stg.spCalculateFactProgress.sql
- migration_project/source_sql/stored_procedures/stg.spCalculateLocationGroupAssignments.sql
- migration_project/source_sql/stored_procedures/stg.spCalculateManagerAssignments.sql

Replace MERGE statements with DLT table definitions.
Replace temp tables with CTEs or intermediate DLT tables.
Output to migration_project/pipelines/dlt/batch_calculations.py
```

**Prompt 3: Complex CURSOR Procedures → Python Notebooks**
```
Convert these complex stored procedures with CURSOR logic to Python notebooks:
- migration_project/source_sql/stored_procedures/stg.spCalculateFactWorkersShifts.sql (821 lines)
- migration_project/source_sql/stored_procedures/stg.spCalculateFactWorkersContacts_ByRule.sql (477 lines)
- migration_project/source_sql/stored_procedures/mrg.spMergeOldData.sql (453 lines)

Replace all CURSOR iterations with Spark DataFrame operations using window functions.
Replace temp tables with DataFrame variables or temp views.
Replace DYNAMIC SQL with parameterized queries.

Output each as separate notebook:
- migration_project/pipelines/notebooks/calc_fact_workers_shifts.py
- migration_project/pipelines/notebooks/calc_worker_contacts.py
- migration_project/pipelines/notebooks/merge_old_data.py
```

**Prompt 4: Assignment Procedures → DLT Tables**
```
Convert all assignment-related stored procedures to DLT tables:
- stg.spDeltaSyncCrewAssignments, stg.spDeltaSyncDeviceAssignments
- stg.spDeltaSyncWorkshiftAssignments, stg.spDeltaSyncManagerAssignments
- stg.spCalculateWorkerLocationAssignments

Output to migration_project/pipelines/dlt/assignment_tables.py
```

### 3.1 Conversion Strategy by Pattern

| Pattern | Count | Target | Approach |
|---------|-------|--------|----------|
| Simple MERGE | 27 | DLT APPLY CHANGES | Use CDC pattern |
| CTE-based transforms | 45 | DLT Tables | Direct conversion |
| TEMP TABLE usage | 33 | Delta temp views | Convert #tables to views |
| CURSOR loops | 10 | Python DataFrame ops | Rewrite as set-based |
| DYNAMIC SQL | 5 | Parameterized notebooks | Security review required |
| SPATIAL operations | 8 | Python UDFs + H3 | Use spatial UDFs |

### 3.2 Procedure Classification

#### Tier 1: DLT Streaming Tables (Delta Sync Procedures)
Convert to DLT with `APPLY CHANGES INTO` for incremental loads.

| Procedure | Lines | Target |
|-----------|-------|--------|
| stg.spDeltaSyncDimWorker | 227 | streaming_worker |
| stg.spDeltaSyncDimProject | 198 | streaming_project |
| stg.spDeltaSyncDimCrew | 151 | streaming_crew |
| stg.spDeltaSyncDimDevice | 149 | streaming_device |
| stg.spDeltaSyncDimTrade | 132 | streaming_trade |
| stg.spDeltaSyncDimOrganization | 90 | streaming_organization |
| stg.spDeltaSyncDimCompany | 126 | streaming_company |
| stg.spDeltaSyncFactObservations | 584 | streaming_fact_observations |
| stg.spDeltaSyncFactWorkersHistory | 782 | streaming_fact_workers_history |
| + 18 more DeltaSync procedures | | |

**DLT Pattern:**
```python
import dlt
from pyspark.sql.functions import *

@dlt.table(name="streaming_worker")
def streaming_worker():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("abfss://raw@storage.dfs.core.windows.net/wakecap/dimensions/Worker/")
    )

dlt.create_streaming_table("silver_worker_cdc")

dlt.apply_changes(
    target="silver_worker_cdc",
    source="streaming_worker",
    keys=["WorkerID"],
    sequence_by="ModifiedAt",
    stored_as_scd_type=2
)
```

#### Tier 2: DLT Batch Tables (Calculate/Stage Procedures)
Convert to DLT batch tables with transformations.

| Procedure | Lines | Patterns | Target |
|-----------|-------|----------|--------|
| stg.spCalculateFactReportedAttendance | 215 | MERGE, CTE | dlt_fact_reported_attendance |
| stg.spCalculateFactProgress | 112 | MERGE, CTE | dlt_fact_progress |
| stg.spCalculateProjectActiveFlag | 36 | CTE | dlt_project_active_flag |
| stg.spCalculateLocationGroupAssignments | 153 | TEMP_TABLE, MERGE | dlt_location_group_assignments |
| stg.spCalculateManagerAssignments | 151 | TEMP_TABLE, MERGE | dlt_manager_assignments |

#### Tier 3: Python Notebooks (Complex Procedures)
Procedures with CURSOR, DYNAMIC SQL, or complex logic.

| Procedure | Lines | Patterns | Target |
|-----------|-------|----------|--------|
| stg.spCalculateFactWorkersShifts | 821 | CURSOR, TEMP_TABLE | notebook_calc_workers_shifts |
| stg.spCalculateFactWorkersShiftsCombined | 645 | TEMP_TABLE, MERGE | notebook_calc_shifts_combined |
| stg.spCalculateFactWorkersContacts_ByRule | 477 | CURSOR, DYNAMIC_SQL | notebook_calc_contacts |
| mrg.spMergeOldData | 453 | CURSOR, SPATIAL | notebook_merge_old_data |
| stg.spWorkersHistory_UpdateAssignments_3 | 553 | TEMP_TABLE | notebook_update_assignments |

#### Tier 4: Skip or Replace (Admin Procedures)
Procedures not needed in Databricks.

| Procedure | Reason | Replacement |
|-----------|--------|-------------|
| dbo.spMaintainIndexes | Delta handles optimization | OPTIMIZE command |
| dbo.spMaintainPartitions | Delta auto-partitions | Z-ORDER command |
| dbo.spUpdateDBStats | Not needed | Built-in stats |
| stg.spCleanupLog | Replace with retention | VACUUM command |

---

### 3.3 Detailed Conversion: Top 5 Critical Procedures

#### 3.3.1 stg.spCalculateFactWorkersShifts (821 lines)

**Current Logic:**
1. Uses CURSOR to iterate through workers
2. Creates multiple temp tables for intermediate results
3. Calculates shift metrics (active time, inactive time, distance)
4. Merges results into FactWorkersShifts

**Conversion Approach:**
```python
# notebook: calc_fact_workers_shifts.py

def calculate_fact_workers_shifts(spark, run_date):
    """
    Convert cursor-based shift calculation to DataFrame operations.
    """
    # Step 1: Load source data
    observations = spark.table("wakecap_prod.migration.bronze_dbo_FactObservations")
    workers = spark.table("wakecap_prod.migration.silver_worker")
    workshifts = spark.table("wakecap_prod.migration.bronze_dbo_WorkshiftAssignments")

    # Step 2: Calculate shift boundaries (replaces cursor loop)
    from pyspark.sql.window import Window

    worker_window = Window.partitionBy("WorkerID", "ProjectID", "ShiftLocalDate").orderBy("ObservationTime")

    shift_metrics = (
        observations
        .filter(col("ShiftLocalDate") == run_date)
        .withColumn("prev_time", lag("ObservationTime").over(worker_window))
        .withColumn("time_gap",
            unix_timestamp("ObservationTime") - unix_timestamp("prev_time"))
        .withColumn("is_active", col("time_gap") <= 300)  # 5 min threshold
        .groupBy("WorkerID", "ProjectID", "ShiftLocalDate")
        .agg(
            min("ObservationTime").alias("FirstObservation"),
            max("ObservationTime").alias("LastObservation"),
            sum(when(col("is_active"), col("time_gap")).otherwise(0)).alias("ActiveTimeDuringShift"),
            sum(when(~col("is_active"), col("time_gap")).otherwise(0)).alias("InactiveTimeDuringShift"),
            count("*").alias("Readings"),
            sum("Distance").alias("DistanceTravelled")
        )
    )

    # Step 3: Merge into target table
    shift_metrics.createOrReplaceTempView("shift_updates")

    spark.sql("""
        MERGE INTO wakecap_prod.migration.fact_workers_shifts AS target
        USING shift_updates AS source
        ON target.WorkerID = source.WorkerID
           AND target.ProjectID = source.ProjectID
           AND target.ShiftLocalDate = source.ShiftLocalDate
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    return shift_metrics.count()
```

#### 3.3.2 stg.spDeltaSyncFactWorkersHistory (782 lines)

**Current Logic:**
1. Loads changed records from staging
2. Uses spatial functions for location assignment
3. Merges with SCD Type 2 logic

**Conversion Approach:** DLT with APPLY CHANGES
```python
import dlt
from pyspark.sql.functions import *

# Source: streaming from ADLS
@dlt.table(name="staging_workers_history")
def staging_workers_history():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("abfss://raw@storage.dfs.core.windows.net/wakecap/facts/FactWorkersHistory/")
        .withColumn("_load_time", current_timestamp())
    )

# Apply CDC changes
dlt.create_streaming_table("fact_workers_history_cdc")

dlt.apply_changes(
    target="fact_workers_history_cdc",
    source="staging_workers_history",
    keys=["WorkerHistoryID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["LocationID", "ZoneID", "FloorID", "StatusID"]
)
```

#### 3.3.3 stg.spDeltaSyncFactObservations (584 lines)

**Conversion:** Similar to FactWorkersHistory - use DLT APPLY CHANGES.

#### 3.3.4 stg.spCalculateFactWorkersContacts_ByRule (477 lines)

**Current Logic:**
1. CURSOR iterates through contact rules
2. DYNAMIC SQL builds queries based on rule type
3. Calculates worker contacts per rule

**Conversion Approach:** Parameterized notebook with rule engine
```python
# notebook: calc_worker_contacts.py

def calculate_contacts_by_rule(spark, rule_id):
    """Process contact tracing for a specific rule."""

    # Load rule configuration
    rule = spark.table("wakecap_prod.migration.contact_tracing_rules").filter(col("RuleID") == rule_id).first()

    # Build query based on rule type
    if rule.RuleType == "PROXIMITY":
        contacts = calculate_proximity_contacts(spark, rule)
    elif rule.RuleType == "ZONE":
        contacts = calculate_zone_contacts(spark, rule)
    elif rule.RuleType == "MANAGER":
        contacts = calculate_manager_contacts(spark, rule)

    return contacts

def calculate_proximity_contacts(spark, rule):
    """Calculate contacts based on spatial proximity."""
    from pyspark.sql.functions import udf

    # Use H3 for efficient spatial joins
    observations = spark.table("wakecap_prod.migration.bronze_dbo_FactObservations")

    # Add H3 index for spatial bucketing
    observations = observations.withColumn(
        "h3_index",
        h3_lat_lng_to_cell(col("Latitude"), col("Longitude"), lit(rule.H3Resolution))
    )

    # Self-join on H3 index for potential contacts
    contacts = (
        observations.alias("a")
        .join(observations.alias("b"),
              (col("a.h3_index") == col("b.h3_index")) &
              (col("a.WorkerID") < col("b.WorkerID")) &
              (abs(unix_timestamp("a.ObservationTime") - unix_timestamp("b.ObservationTime")) <= rule.TimeThresholdSeconds)
        )
        .select(
            col("a.WorkerID").alias("WorkerID1"),
            col("b.WorkerID").alias("WorkerID2"),
            col("a.ObservationTime").alias("ContactTime"),
            col("a.ProjectID")
        )
    )

    return contacts
```

#### 3.3.5 mrg.spMergeOldData (453 lines)

**Assessment:** This appears to be a one-time migration procedure for historical data. May not need ongoing conversion.

**Recommendation:**
1. Run original procedure on SQL Server to completion
2. Extract final result to ADLS via ADF
3. Load into Databricks as historical baseline

---

### 3.4 Procedure Migration Schedule

| Week | Procedures | Count | Deliverable |
|------|------------|-------|-------------|
| 3 | DeltaSync Dimension procs | 15 | DLT streaming tables |
| 4 | DeltaSync Fact procs | 8 | DLT streaming tables |
| 5 | Calculate procs (simple) | 10 | DLT batch tables |
| 6 | Calculate procs (complex) | 5 | Python notebooks |
| 7 | Assignment procs | 12 | DLT tables |
| 8 | Remaining + testing | 20 | Mixed |

---

## Phase 4: Integration & Testing (Week 7-8)

### 4.1 Testing Checklist

| Test | Scope | Method |
|------|-------|--------|
| Row count validation | All tables | Compare source vs target counts |
| Data type validation | All columns | Schema comparison |
| Null handling | Nullable columns | Null count comparison |
| Referential integrity | FK relationships | Join validation |
| Business logic | Key calculations | Sample record comparison |
| Performance | Large tables | Query timing |

### 4.2 Validation Queries

```sql
-- Row count comparison
SELECT 'Worker' as table_name,
       (SELECT COUNT(*) FROM wakecap_prod.migration.silver_worker) as databricks_count,
       142567 as sqlserver_count  -- From source system

UNION ALL

SELECT 'FactObservations',
       (SELECT COUNT(*) FROM wakecap_prod.migration.bronze_dbo_FactObservations),
       52847392
-- ... repeat for all tables
```

### 4.3 Reconciliation Report Template

```python
def generate_reconciliation_report():
    """Generate migration validation report."""
    report = {
        "tables": [],
        "views": [],
        "functions": [],
        "procedures": []
    }

    for table in tables_to_validate:
        source_count = get_source_count(table)
        target_count = spark.table(f"wakecap_prod.migration.{table}").count()

        report["tables"].append({
            "name": table,
            "source_count": source_count,
            "target_count": target_count,
            "match": source_count == target_count,
            "difference": abs(source_count - target_count)
        })

    return report
```

---

## Step by Step Tasks (Claude Execution)

Execute these tasks in order. Each task should be completed before moving to the next.

### Task 1: Create Output Directory Structure
```
Create the following directories:
- migration_project/pipelines/dlt/
- migration_project/pipelines/notebooks/
- migration_project/pipelines/udfs/
- migration_project/pipelines/security/
```

### Task 2: Convert Simple Functions to SQL UDFs
```
Read and convert these functions to Databricks SQL UDFs:
1. migration_project/source_sql/functions/dbo.fnStripNonNumerics.sql
2. migration_project/source_sql/functions/dbo.fnExtractPattern.sql
3. migration_project/source_sql/functions/dbo.fnAtTimeZone.sql
4. migration_project/source_sql/functions/stg.fnCalcTimeCategory.sql
5. migration_project/source_sql/functions/stg.fnExtSourceIDAlias.sql

Output: migration_project/pipelines/udfs/simple_udfs.sql
```

### Task 3: Convert Spatial Functions to Python UDFs
```
Read and convert these spatial functions to Python UDFs:
1. migration_project/source_sql/functions/dbo.fnCalcDistanceNearby.sql
2. migration_project/source_sql/functions/dbo.fnGeometry2SVG.sql
3. migration_project/source_sql/functions/dbo.fnGeometry2JSON.sql
4. migration_project/source_sql/functions/dbo.fnGeoPointShiftScale.sql
5. migration_project/source_sql/functions/dbo.fnFixGeographyOrder.sql
6. migration_project/source_sql/functions/stg.fnNearestNeighbor_3Ordered.sql

Use H3 library for spatial indexing and Shapely for geometry operations.
Output: migration_project/pipelines/udfs/spatial_udfs.py
```

### Task 4: Convert Hierarchical Functions to Python UDFs
```
Read and convert these hierarchical functions:
1. migration_project/source_sql/functions/dbo.fnManagersByLevel.sql
2. migration_project/source_sql/functions/dbo.fnManagersByLevelSlicedIntervals.sql

Output: migration_project/pipelines/udfs/hierarchy_udfs.py
```

### Task 5: Convert Security Predicates to Row Filters
```
Read and convert these security predicate functions to Unity Catalog row filters:
1. migration_project/source_sql/functions/security.fn_OrganizationPredicate.sql
2. migration_project/source_sql/functions/security.fn_ProjectPredicate.sql
3. migration_project/source_sql/functions/security.fn_ProjectPredicateEx.sql
4. migration_project/source_sql/functions/security.fn_UserPredicate.sql

Output: migration_project/pipelines/security/row_filters.sql
```

### Task 6: Convert All Views to Gold Layer DLT
```
Read all transpiled views from migration_project/transpiled/views/
Generate DLT table definitions for the Gold layer.
Output: migration_project/pipelines/dlt/gold_views.py
```

### Task 7: Convert DeltaSync Dimension Procedures
```
Read and convert these spDeltaSync* dimension procedures to DLT streaming tables:
- stg.spDeltaSyncDimWorker.sql
- stg.spDeltaSyncDimProject.sql
- stg.spDeltaSyncDimCrew.sql
- stg.spDeltaSyncDimDevice.sql
- stg.spDeltaSyncDimTrade.sql
- stg.spDeltaSyncDimOrganization.sql
- stg.spDeltaSyncDimCompany.sql
- stg.spDeltaSyncDimDepartment.sql
- stg.spDeltaSyncDimFloor.sql
- stg.spDeltaSyncDimZone.sql
- stg.spDeltaSyncDimWorkshift.sql
- stg.spDeltaSyncDimActivity.sql
- stg.spDeltaSyncDimLocationGroup.sql
- stg.spDeltaSyncDimObservationSource.sql
- stg.spDeltaSyncDimTitle.sql

Use APPLY CHANGES INTO pattern with Auto Loader from ADLS.
Output: migration_project/pipelines/dlt/streaming_dimensions.py
```

### Task 8: Convert DeltaSync Fact Procedures
```
Read and convert these spDeltaSync* fact procedures:
- stg.spDeltaSyncFactObservations.sql
- stg.spDeltaSyncFactWorkersHistory.sql (SPATIAL - use spatial UDFs)
- stg.spDeltaSyncFactWeatherObservations.sql
- stg.spDeltaSyncFactProgress.sql
- stg.spDeltaSyncFactReportedAttendance_ResourceTimesheet.sql
- stg.spDeltaSyncFactReportedAttendance_ResourceHours.sql

Output: migration_project/pipelines/dlt/streaming_facts.py
```

### Task 9: Convert Assignment Procedures
```
Read and convert these assignment procedures:
- stg.spDeltaSyncCrewAssignments.sql
- stg.spDeltaSyncDeviceAssignments.sql
- stg.spDeltaSyncWorkshiftAssignments.sql
- stg.spDeltaSyncManagerAssignments.sql
- stg.spDeltaSyncWorkerLocationAssignments.sql
- stg.spDeltaSyncLocationGroupAssignments.sql
- stg.spDeltaSyncCrewManagerAssignments.sql
- stg.spDeltaSyncWorkerStatusAssignments.sql
- stg.spDeltaSyncPermissions.sql

Output: migration_project/pipelines/dlt/streaming_assignments.py
```

### Task 10: Convert Simple Calculate Procedures to DLT Batch
```
Read and convert these calculation procedures (no CURSOR):
- stg.spCalculateFactReportedAttendance.sql
- stg.spCalculateFactProgress.sql
- stg.spCalculateProjectActiveFlag.sql
- stg.spCalculateLocationGroupAssignments.sql
- stg.spCalculateManagerAssignments.sql
- stg.spCalculateManagerAssignmentSnapshots.sql

Output: migration_project/pipelines/dlt/batch_calculations.py
```

### Task 11: Convert Complex CURSOR Procedures to Notebooks
```
Read and convert these complex procedures (with CURSOR) to Python notebooks:

1. stg.spCalculateFactWorkersShifts.sql (821 lines, CURSOR, TEMP_TABLE)
   Output: migration_project/pipelines/notebooks/calc_fact_workers_shifts.py

2. stg.spCalculateFactWorkersShiftsCombined.sql (645 lines, TEMP_TABLE, MERGE)
   Output: migration_project/pipelines/notebooks/calc_fact_workers_shifts_combined.py

3. stg.spCalculateFactWorkersContacts_ByRule.sql (477 lines, CURSOR, DYNAMIC_SQL)
   Output: migration_project/pipelines/notebooks/calc_worker_contacts.py

4. stg.spWorkersHistory_UpdateAssignments_3_LocationClass.sql (553 lines, TEMP_TABLE)
   Output: migration_project/pipelines/notebooks/update_workers_history_assignments.py

Replace CURSOR with DataFrame window operations.
Replace TEMP_TABLE with createOrReplaceTempView or CTEs.
Replace DYNAMIC_SQL with parameterized queries.
```

### Task 12: Update Bronze Layer to Read from TimescaleDB (Incremental)
```
Update migration_project/pipelines/wakecap_migration_pipeline.py:
- Configure JDBC connection to TimescaleDB using credentials_template.yml
- Implement watermark-based incremental loading (new records only)
- Track last loaded watermark in _watermarks Delta table
- Query only records where watermark_column > last_watermark
- Append new records to Raw Zone Delta tables

Configuration: migration_project/credentials_template.yml (timescaledb section)
Output: migration_project/pipelines/wakecap_migration_pipeline.py (updated)

Incremental Loading Pattern:
1. Read last_watermark from _watermarks table for each source table
2. Build query: SELECT * FROM table WHERE created_at > last_watermark
3. Load results via JDBC with PostgreSQL driver
4. Append to Delta table with _loaded_at timestamp
5. Update _watermarks with new max watermark value
```

### Task 13: Create Master Pipeline Notebook
```
Create a master DLT pipeline notebook that imports all components:
- Bronze layer tables (from ADLS)
- Silver layer tables (with data quality)
- Gold layer views
- Streaming CDC tables

Output: migration_project/pipelines/master_pipeline.py
```

### Task 14: Update Migration Status
```
Update migration_project/MIGRATION_STATUS.md:
- Mark converted functions as COMPLETED
- Mark converted procedures as COMPLETED
- Update phase progress percentages
- Add conversion statistics
```

### Task 15: Validate All Generated Code
```
For each generated Python file:
1. Check syntax is valid: python -m py_compile <file>
2. Verify all imports are available
3. Ensure DLT decorators are correct
4. Check for missing table references

Report any issues found.
```

---

## Acceptance Criteria

- [ ] All 23 functions converted (5 SQL UDFs, 8 spatial Python UDFs, 2 hierarchy UDFs, 4 security predicates, 4 other)
- [ ] All 34 views converted to Gold layer DLT tables
- [ ] All 27 DeltaSync procedures converted to streaming tables
- [ ] All 10 simple Calculate procedures converted to batch DLT tables
- [ ] All 5 complex CURSOR procedures converted to Python notebooks
- [ ] Bronze layer updated to read from ADLS instead of JDBC
- [ ] MIGRATION_STATUS.md updated with completion status
- [ ] All generated Python files pass syntax validation

---

## Validation Commands

```bash
# Validate Python syntax for all generated files
python -m py_compile migration_project/pipelines/dlt/*.py
python -m py_compile migration_project/pipelines/notebooks/*.py
python -m py_compile migration_project/pipelines/udfs/*.py

# Count converted objects
echo "Functions converted:"
grep -c "CREATE.*FUNCTION\|@udf" migration_project/pipelines/udfs/*.sql migration_project/pipelines/udfs/*.py

echo "DLT tables defined:"
grep -c "@dlt.table" migration_project/pipelines/dlt/*.py

echo "Notebooks created:"
ls -la migration_project/pipelines/notebooks/*.py | wc -l
```

---

## Appendix A: Function Conversion Reference

| Original Function | Databricks Equivalent | Status |
|-------------------|----------------------|--------|
| dbo.fnStripNonNumerics | regexp_replace(col, '[^0-9]', '') | TODO |
| dbo.fnExtractPattern | regexp_extract(col, pattern, 0) | TODO |
| dbo.fnAtTimeZone | from_utc_timestamp(col, tz) | TODO |
| stg.fnCalcTimeCategory | SQL CASE expression | TODO |
| dbo.fnCalcDistanceNearby | Python UDF (Haversine) | TODO |
| dbo.fnGeometry2SVG | Python UDF (Shapely) | TODO |
| dbo.fnManagersByLevel | DLT table (hierarchy) | TODO |
| security.fn_*Predicate | Unity Catalog row filter | TODO |

---

## Appendix B: DLT Pipeline Structure

```
wakecap_migration_pipeline/
├── bronze/
│   ├── bronze_dimensions.py      # All dimension tables from TimescaleDB (incremental)
│   ├── bronze_facts.py           # All fact tables from TimescaleDB (incremental)
│   └── bronze_assignments.py     # Assignment tables from TimescaleDB (incremental)
├── silver/
│   ├── silver_dimensions.py      # Cleaned dimensions with DQ
│   ├── silver_facts.py           # Cleaned facts with DQ
│   └── silver_cdc.py             # CDC streaming tables
├── gold/
│   ├── gold_views.py             # Business views (transpiled)
│   └── gold_aggregates.py        # Calculated aggregates
└── utils/
    ├── spatial_udfs.py           # H3 and geometry UDFs
    ├── hierarchy_udfs.py         # Manager hierarchy UDFs
    └── security_filters.py       # Row filter functions
```

---

## Appendix C: Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Spatial function accuracy | Medium | High | Validate against source samples |
| Cursor conversion bugs | Medium | High | Unit test each conversion |
| Performance degradation | Low | High | Benchmark before/after |
| Security gap (RLS) | Medium | High | Test all access scenarios |
| Data loss in CDC | Low | Critical | Validate row counts daily |

---

*Last Updated: 2026-01-19*
