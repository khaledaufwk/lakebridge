# Plan: WakeCapDW Migration - Remaining Artifacts

## Task Description
Complete the migration of all remaining SQL Server artifacts (stored procedures, functions, views, and tables) from WakeCapDW_20251215 to Databricks Unity Catalog. This plan covers 70 stored procedures, 23 user-defined functions, 125+ additional bronze tables, and 24+ gold layer views that have not yet been migrated.

## Objective
When this plan is complete:
- All 142 source tables will be ingested into the Bronze layer via ADLS
- All 70 stored procedures will be converted to DLT tables or Python notebooks
- All 23 functions will be converted to SQL UDFs or Python UDFs
- All 34 views will be deployed as Gold layer tables
- Data quality expectations will be enforced across all Silver layer tables
- Security predicates will be implemented as Unity Catalog row filters
- Full data reconciliation will validate migration completeness

## Problem Statement
The WakeCapDW migration is 65% complete with tables and views transpiled, but stored procedures (0%) and functions (0%) require manual conversion. The current pipeline uses JDBC which is blocked by firewall issues. The architecture must shift to ADF → ADLS → Databricks DLT.

## Solution Approach
1. Migrate data ingestion from JDBC to ADLS-based Auto Loader
2. Convert stored procedures based on their complexity patterns:
   - Simple MERGE → DLT APPLY CHANGES
   - CTE-based transforms → DLT batch tables
   - CURSOR/DYNAMIC_SQL → Python notebooks
3. Convert functions to Databricks-native equivalents (SQL UDFs, Python UDFs, or Unity Catalog row filters)
4. Deploy all transpiled views as Gold layer DLT tables
5. Validate with row counts and business logic checks

## Source System Analysis
- **Database**: WakeCapDW_20251215
- **Server**: wakecap24.database.windows.net
- **Object Count**: 269 total (142 tables, 34 views, 70 procedures, 23 functions)
- **Complexity Indicators**:
  - CURSOR (10 procedures) - HIGH
  - SPATIAL/Geography (18 objects) - HIGH
  - DYNAMIC_SQL (5 procedures) - HIGH
  - TEMP_TABLE (33 procedures) - MEDIUM
  - MERGE (51 procedures) - LOW
  - CTE (269 occurrences) - LOW
- **Objects Requiring Manual Work**:
  - 70 stored procedures (all require conversion)
  - 23 functions (all require conversion)
  - 8 spatial functions (require H3/Shapely)
  - 4 security predicates (require Unity Catalog RLS)
  - 5 complex CURSOR procedures (require complete rewrite)

## Target Architecture
- **Catalog**: wakecap_prod
- **Schema**: migration
- **Pipeline Type**: Delta Live Tables (DLT)
- **Medallion Layers**: Bronze (raw from ADLS) → Silver (cleaned with DQ) → Gold (business views)
- **Data Source**: ADLS Gen2 (populated by Azure Data Factory from SQL Server)

## Relevant Files
Use these files to complete the task:

### Source SQL Files
- `migration_project/source_sql/stored_procedures/` - 70 stored procedure definitions
- `migration_project/source_sql/functions/` - 23 function definitions
- `migration_project/source_sql/views/` - 34 view definitions
- `migration_project/source_sql/tables/` - 142 table definitions

### Transpiled Files
- `migration_project/transpiled/views/` - 33 transpiled views (Databricks SQL)
- `migration_project/transpiled/tables/` - 142 transpiled table DDL

### Existing Pipeline
- `migration_project/pipelines/wakecap_migration_pipeline.py` - Current DLT pipeline (17 bronze, 6 silver, 3 gold)
- `migration_project/pipelines/pipeline_config.json` - Pipeline configuration

### Assessment Documents
- `migration_project/assessment/migration_assessment.md` - Complexity analysis
- `migration_project/MIGRATION_STATUS.md` - Current status tracking
- `migration_project/plan.md` - Previous planning document

### New Files to Create
- `migration_project/pipelines/dlt/bronze_all_tables.py` - All 142 bronze tables
- `migration_project/pipelines/dlt/silver_dimensions.py` - Cleaned dimension tables
- `migration_project/pipelines/dlt/silver_facts.py` - Cleaned fact tables
- `migration_project/pipelines/dlt/gold_views.py` - All 34 business views
- `migration_project/pipelines/dlt/streaming_dimensions.py` - CDC for dimensions
- `migration_project/pipelines/dlt/streaming_facts.py` - CDC for facts
- `migration_project/pipelines/dlt/batch_calculations.py` - Batch calculation procedures
- `migration_project/pipelines/notebooks/calc_fact_workers_shifts.py` - Complex procedure conversion
- `migration_project/pipelines/notebooks/calc_worker_contacts.py` - Complex procedure conversion
- `migration_project/pipelines/udfs/simple_udfs.sql` - SQL UDF definitions
- `migration_project/pipelines/udfs/spatial_udfs.py` - Python spatial UDFs
- `migration_project/pipelines/security/row_filters.sql` - Unity Catalog RLS

---

## Implementation Phases

### Phase 1: Infrastructure Setup
Set up the data ingestion infrastructure using ADF and ADLS.

**Tasks:**
1. Create ADLS landing zone structure
2. Configure Unity Catalog external location for ADLS
3. Update DLT pipeline to read from ADLS instead of JDBC
4. Create directory structure for new pipeline files

### Phase 2: Complete Bronze Layer (125 Additional Tables)
Add all remaining tables to the Bronze layer.

**Tables Currently Missing from Bronze:**
- Reference tables: Activity, Department, Task, Title, WorkerStatus, etc.
- Assignment tables: TradeAssignments, ManagerAssignments, LocationGroupAssignments, etc.
- Fact tables: FactWorkersContacts, FactWorkersTasks, FactWeatherObservations, etc.
- Staging tables: SyncState, ImpactedAreaLog
- Merge tables: mrg.* tables (if needed for historical data)

### Phase 3: Function Conversion
Convert all 23 user-defined functions to Databricks equivalents.

### Phase 4: Stored Procedure Conversion
Convert all 70 stored procedures based on complexity tier.

### Phase 5: Gold Layer Completion
Deploy all 34 business views as Gold layer tables.

### Phase 6: Silver Layer Enhancement
Add data quality expectations to all dimension and fact tables.

### Phase 7: Validation and Reconciliation
Validate data completeness and accuracy.

---

## Step by Step Tasks

IMPORTANT: Execute every step in order, top to bottom.

---

### PHASE 1: INFRASTRUCTURE SETUP

### 1.1 Create Output Directory Structure
Create the directory structure for all new pipeline files.

```
migration_project/pipelines/
├── dlt/                      # Delta Live Tables definitions
│   ├── bronze_all_tables.py  # All 142 bronze tables
│   ├── silver_dimensions.py  # Cleaned dimensions
│   ├── silver_facts.py       # Cleaned facts
│   ├── gold_views.py         # Business views
│   ├── streaming_dimensions.py # CDC dimensions
│   ├── streaming_facts.py    # CDC facts
│   └── batch_calculations.py # Batch calculations
├── notebooks/                 # Python notebooks for complex procedures
│   ├── calc_fact_workers_shifts.py
│   ├── calc_fact_workers_shifts_combined.py
│   ├── calc_worker_contacts.py
│   ├── merge_old_data.py
│   └── update_workers_history.py
├── udfs/                      # User-defined functions
│   ├── simple_udfs.sql       # SQL UDFs
│   ├── spatial_udfs.py       # Python spatial UDFs
│   └── hierarchy_udfs.py     # Manager hierarchy UDFs
└── security/                  # Security configurations
    └── row_filters.sql       # Unity Catalog row filters
```

**Actions:**
- Create directories: `mkdir -p migration_project/pipelines/dlt migration_project/pipelines/notebooks migration_project/pipelines/udfs migration_project/pipelines/security`
- Verify structure exists

---

### 1.2 Configure ADLS External Location
Create Unity Catalog external location for ADLS access.

**Prerequisites:**
- ADLS Gen2 storage account exists
- Service principal with Storage Blob Data Contributor role
- Storage credential created in Unity Catalog

**SQL to execute in Databricks:**
```sql
-- Create storage credential (if not exists)
CREATE STORAGE CREDENTIAL IF NOT EXISTS wakecap_adls_credential
WITH (
    AZURE_MANAGED_IDENTITY = '<managed-identity-resource-id>'
);

-- Create external location
CREATE EXTERNAL LOCATION IF NOT EXISTS wakecap_raw
URL 'abfss://raw@<storage-account>.dfs.core.windows.net/wakecap/'
WITH (STORAGE CREDENTIAL wakecap_adls_credential);

-- Grant access
GRANT READ FILES ON EXTERNAL LOCATION wakecap_raw TO `data-engineers`;
```

**Note:** Replace `<storage-account>` and `<managed-identity-resource-id>` with actual values from Azure portal.

---

### 1.3 Create ADLS Landing Zone Structure
Define the folder structure for ADF to write data.

**Target Structure:**
```
abfss://raw@<storage>.dfs.core.windows.net/wakecap/
├── dimensions/
│   ├── Worker/
│   ├── Project/
│   ├── Crew/
│   ├── Company/
│   ├── Organization/
│   ├── Device/
│   ├── Floor/
│   ├── Zone/
│   ├── Trade/
│   ├── Department/
│   ├── Activity/
│   ├── Task/
│   ├── Title/
│   ├── WorkerStatus/
│   ├── Workshift/
│   ├── WorkshiftDetails/
│   ├── LocationGroup/
│   ├── ObservationSource/
│   └── ObservationType/
├── facts/
│   ├── FactObservations/
│   ├── FactWorkersHistory/
│   ├── FactWorkersShifts/
│   ├── FactWorkersShiftsCombined/
│   ├── FactReportedAttendance/
│   ├── FactProgress/
│   ├── FactWorkersContacts/
│   ├── FactWorkersTasks/
│   └── FactWeatherObservations/
├── assignments/
│   ├── CrewAssignments/
│   ├── DeviceAssignments/
│   ├── WorkshiftAssignments/
│   ├── TradeAssignments/
│   ├── ManagerAssignments/
│   ├── LocationGroupAssignments/
│   ├── WorkerLocationAssignments/
│   └── ManagerAssignmentSnapshots/
├── security/
│   ├── UserPermissions/
│   ├── UserPermissionsProject/
│   └── UserPermissionsOrganization/
└── staging/
    ├── SyncState/
    └── ImpactedAreaLog/
```

**ADF Configuration:**
- Each table should output to its corresponding folder in Parquet format
- Partition large fact tables by date (e.g., FactObservations partitioned by ObservationDate)
- Use incremental extraction for fact tables (delta based on ModifiedAt column)

---

### 1.4 Update Pipeline Configuration for ADLS
Modify the DLT pipeline to read from ADLS instead of JDBC.

**File:** `migration_project/pipelines/dlt/bronze_all_tables.py`

**Key Changes:**
1. Replace `read_sql_server_table()` with `read_adls_table()`
2. Use cloudFiles Auto Loader for streaming ingestion
3. Remove JDBC configuration and secrets dependency

**New helper function:**
```python
def get_adls_path(table_category, table_name):
    """Get ADLS path for a table."""
    base_path = "abfss://raw@<storage>.dfs.core.windows.net/wakecap"
    return f"{base_path}/{table_category}/{table_name}/"

def read_adls_table(table_category, table_name, streaming=False):
    """Read a table from ADLS Parquet files."""
    path = get_adls_path(table_category, table_name)
    if streaming:
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"/checkpoints/{table_name}/_schema")
            .load(path)
        )
    else:
        return spark.read.format("parquet").load(path)
```

---

### PHASE 2: COMPLETE BRONZE LAYER

### 2.1 Create Bronze Layer for All Dimension Tables (24 Tables)
Generate DLT definitions for all dimension tables.

**File:** `migration_project/pipelines/dlt/bronze_dimensions.py`

**Dimension Tables to Add:**
| Table | Status | Notes |
|-------|--------|-------|
| Worker | EXISTS | Already in pipeline |
| Project | EXISTS | Already in pipeline |
| Crew | EXISTS | Already in pipeline |
| Device | EXISTS | Already in pipeline |
| Organization | EXISTS | Already in pipeline |
| Company | EXISTS | Already in pipeline |
| Trade | EXISTS | Already in pipeline |
| Floor | EXISTS | Already in pipeline |
| Zone | EXISTS | Already in pipeline |
| Workshift | EXISTS | Already in pipeline |
| Activity | ADD | Activity types |
| Department | ADD | Worker departments |
| Task | ADD | Task definitions |
| Title | ADD | Worker titles |
| WorkerStatus | ADD | Worker status codes |
| WorkshiftDetails | ADD | Shift time windows |
| LocationGroup | ADD | Location groupings |
| ObservationSource | ADD | Observation sources |
| ObservationType | ADD | Observation types |
| ObservationStatus | ADD | Status codes |
| LocationAssignmentClass | ADD | Location classes |
| ContactTracingRule | ADD | Contact rules |
| ProductiveClass | ADD | Productivity codes |
| ShiftTimeCategory | ADD | Time categories |

**Template for Each Table:**
```python
@dlt.table(
    name="bronze_dbo_{TableName}",
    comment="Raw {TableName} data from ADLS",
    table_properties={
        "quality": "bronze",
        "source": "adls",
        "source_table": "dbo.{TableName}"
    }
)
def bronze_dbo_{TableName}():
    """Bronze table: {Description}"""
    return read_adls_table("dimensions", "{TableName}")
```

---

### 2.2 Create Bronze Layer for All Fact Tables (9 Tables)
Generate DLT definitions for all fact tables.

**File:** `migration_project/pipelines/dlt/bronze_facts.py`

**Fact Tables:**
| Table | Status | Row Estimate | Partitioning |
|-------|--------|--------------|--------------|
| FactObservations | EXISTS | ~50M | By ObservationDate |
| FactWorkersHistory | EXISTS | ~10M | By RecordDate |
| FactWorkersShifts | EXISTS | ~5M | By ShiftLocalDate |
| FactReportedAttendance | EXISTS | ~2M | By ReportDate |
| FactProgress | EXISTS | ~1M | By ProgressDate |
| FactWorkersShiftsCombined | ADD | ~5M | By ShiftLocalDate |
| FactWorkersContacts | ADD | ~20M | By ContactDate |
| FactWorkersTasks | ADD | ~500K | By TaskDate |
| FactWeatherObservations | ADD | ~100K | By ObservationDate |

**Template for Large Fact Tables (with streaming):**
```python
@dlt.table(
    name="bronze_dbo_FactWorkersContacts",
    comment="Raw worker contacts fact from ADLS",
    table_properties={
        "quality": "bronze",
        "source": "adls",
        "source_table": "dbo.FactWorkersContacts"
    }
)
def bronze_dbo_FactWorkersContacts():
    """Bronze table: Worker contact tracing data."""
    return read_adls_table("facts", "FactWorkersContacts", streaming=True)
```

---

### 2.3 Create Bronze Layer for Assignment Tables (8 Tables)
Generate DLT definitions for assignment tables.

**File:** `migration_project/pipelines/dlt/bronze_assignments.py`

**Assignment Tables:**
| Table | Status | Notes |
|-------|--------|-------|
| CrewAssignments | EXISTS | Worker-Crew mapping |
| DeviceAssignments | EXISTS | Worker-Device mapping |
| WorkshiftAssignments | EXISTS | Worker-Shift mapping |
| TradeAssignments | ADD | Worker-Trade mapping |
| ManagerAssignments | ADD | Worker-Manager hierarchy |
| ManagerAssignmentSnapshots | ADD | Point-in-time snapshots |
| LocationGroupAssignments | ADD | Worker-LocationGroup mapping |
| WorkerLocationAssignments | ADD | Worker-Location mapping |

---

### 2.4 Create Bronze Layer for Staging/Security Tables (5 Tables)
Generate DLT definitions for staging and security tables.

**Security Tables:**
| Table | Notes |
|-------|-------|
| security.UserPermissions | User access permissions |
| security.UserPermissionsProject | Project-level permissions |
| security.UserPermissionsOrganization | Org-level permissions |
| security.UserPermissionsCompany | Company-level permissions |
| security.UserPermissionsCrew | Crew-level permissions |

**Staging Tables:**
| Table | Notes |
|-------|-------|
| stg.SyncState | Sync tracking |
| stg.ImpactedAreaLog | ETL logging |

---

### PHASE 3: FUNCTION CONVERSION

### 3.1 Convert Simple String/Pattern Functions to SQL UDFs
Convert functions that have direct Databricks SQL equivalents.

**File:** `migration_project/pipelines/udfs/simple_udfs.sql`

**Source Files to Read:**
- `migration_project/source_sql/functions/dbo.fnStripNonNumerics.sql`
- `migration_project/source_sql/functions/dbo.fnExtractPattern.sql`
- `migration_project/source_sql/functions/dbo.fnAtTimeZone.sql`
- `migration_project/source_sql/functions/stg.fnCalcTimeCategory.sql`
- `migration_project/source_sql/functions/stg.fnExtSourceIDAlias.sql`

**Conversions:**

| Original Function | Databricks SQL UDF |
|-------------------|-------------------|
| `dbo.fnStripNonNumerics(@input)` | `regexp_replace(input, '[^0-9]', '')` |
| `dbo.fnExtractPattern(@input, @pattern)` | `regexp_extract(input, pattern, 0)` |
| `dbo.fnAtTimeZone(@dt, @tz)` | `from_utc_timestamp(dt, tz)` |
| `stg.fnCalcTimeCategory(@hour)` | SQL CASE expression |
| `stg.fnExtSourceIDAlias(@source)` | Simple COALESCE lookup |

**SQL UDF Definitions:**
```sql
-- fn_strip_non_numerics: Remove non-numeric characters
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_strip_non_numerics(input STRING)
RETURNS STRING
LANGUAGE SQL
RETURN regexp_replace(input, '[^0-9]', '');

-- fn_extract_pattern: Extract pattern from string
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_extract_pattern(input STRING, pattern STRING)
RETURNS STRING
LANGUAGE SQL
RETURN regexp_extract(input, pattern, 0);

-- fn_at_timezone: Convert UTC to local timezone
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_at_timezone(dt TIMESTAMP, tz STRING)
RETURNS TIMESTAMP
LANGUAGE SQL
RETURN from_utc_timestamp(dt, tz);

-- fn_calc_time_category: Categorize hour of day
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_calc_time_category(hour_of_day INT)
RETURNS INT
LANGUAGE SQL
RETURN CASE
    WHEN hour_of_day BETWEEN 6 AND 11 THEN 1   -- Morning
    WHEN hour_of_day BETWEEN 12 AND 17 THEN 2  -- Afternoon
    WHEN hour_of_day BETWEEN 18 AND 21 THEN 3  -- Evening
    ELSE 4                                      -- Night
END;

-- fn_ext_source_id_alias: Get source alias
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_ext_source_id_alias(source_id STRING)
RETURNS STRING
LANGUAGE SQL
RETURN COALESCE(source_id, 'UNKNOWN');
```

---

### 3.2 Convert Spatial Functions to Python UDFs
Convert geography/geometry functions using H3 and Shapely libraries.

**File:** `migration_project/pipelines/udfs/spatial_udfs.py`

**Source Files to Read:**
- `migration_project/source_sql/functions/dbo.fnCalcDistanceNearby.sql`
- `migration_project/source_sql/functions/dbo.fnGeometry2SVG.sql`
- `migration_project/source_sql/functions/dbo.fnGeometry2JSON.sql`
- `migration_project/source_sql/functions/dbo.fnGeoPointShiftScale.sql`
- `migration_project/source_sql/functions/dbo.fnGeoPointShiftScale_iso3D.sql`
- `migration_project/source_sql/functions/dbo.fnGeometry2SVG_iso3D.sql`
- `migration_project/source_sql/functions/dbo.fnFixGeographyOrder.sql`
- `migration_project/source_sql/functions/stg.fnNearestNeighbor_3Ordered.sql`

**Steps:**
1. Read each source SQL function to understand the logic
2. Create Python UDF equivalents using H3 for indexing and Shapely for geometry operations
3. Register UDFs for both Python and SQL access

**Python UDF Template:**
```python
# Databricks notebook source
# MAGIC %pip install h3 shapely

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, DoubleType, ArrayType, StructType, StructField, IntegerType
import h3
from shapely import wkt
from shapely.geometry import Point, mapping
from shapely.affinity import translate, scale as shapely_scale
from math import radians, sin, cos, sqrt, atan2
import json

# COMMAND ----------

@udf(returnType=DoubleType())
def fn_calc_distance_nearby(lat1, lon1, lat2, lon2):
    """Calculate distance in meters between two points using Haversine formula.

    Equivalent to: dbo.fnCalcDistanceNearby
    """
    if None in (lat1, lon1, lat2, lon2):
        return None
    R = 6371000  # Earth radius in meters
    phi1, phi2 = radians(lat1), radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)

    a = sin(delta_phi/2)**2 + cos(phi1) * cos(phi2) * sin(delta_lambda/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))

    return R * c

spark.udf.register("fn_calc_distance_nearby", fn_calc_distance_nearby)

# COMMAND ----------

@udf(returnType=StringType())
def fn_geometry_to_svg(wkt_string, width=100, height=100):
    """Convert WKT geometry to SVG path.

    Equivalent to: dbo.fnGeometry2SVG
    """
    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        bounds = geom.bounds  # (minx, miny, maxx, maxy)

        # Calculate scaling
        geom_width = bounds[2] - bounds[0]
        geom_height = bounds[3] - bounds[1]
        scale_x = width / geom_width if geom_width > 0 else 1
        scale_y = height / geom_height if geom_height > 0 else 1
        scale = min(scale_x, scale_y)

        # Generate SVG path
        if geom.geom_type == 'Polygon':
            coords = list(geom.exterior.coords)
            path_data = "M " + " L ".join([f"{(x - bounds[0]) * scale},{(bounds[3] - y) * scale}" for x, y in coords]) + " Z"
        elif geom.geom_type == 'Point':
            x, y = geom.x, geom.y
            path_data = f"M {(x - bounds[0]) * scale},{(bounds[3] - y) * scale} m -5,0 a 5,5 0 1,0 10,0 a 5,5 0 1,0 -10,0"
        else:
            return None

        return f'<svg viewBox="0 0 {width} {height}"><path d="{path_data}" fill="none" stroke="black"/></svg>'
    except Exception:
        return None

spark.udf.register("fn_geometry_to_svg", fn_geometry_to_svg)

# COMMAND ----------

@udf(returnType=StringType())
def fn_geometry_to_json(wkt_string):
    """Convert WKT geometry to GeoJSON.

    Equivalent to: dbo.fnGeometry2JSON
    """
    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        return json.dumps(mapping(geom))
    except Exception:
        return None

spark.udf.register("fn_geometry_to_json", fn_geometry_to_json)

# COMMAND ----------

@udf(returnType=StringType())
def fn_geo_point_shift_scale(wkt_string, shift_x, shift_y, scale_factor):
    """Transform geometry by shifting and scaling.

    Equivalent to: dbo.fnGeoPointShiftScale
    """
    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        geom = translate(geom, xoff=shift_x or 0, yoff=shift_y or 0)
        if scale_factor and scale_factor != 1:
            geom = shapely_scale(geom, xfact=scale_factor, yfact=scale_factor, origin='centroid')
        return geom.wkt
    except Exception:
        return None

spark.udf.register("fn_geo_point_shift_scale", fn_geo_point_shift_scale)

# COMMAND ----------

@udf(returnType=StringType())
def fn_fix_geography_order(wkt_string):
    """Fix polygon ring ordering for geography type.

    Equivalent to: dbo.fnFixGeographyOrder
    SQL Server geography requires counter-clockwise exterior rings.
    """
    if wkt_string is None:
        return None
    try:
        geom = wkt.loads(wkt_string)
        if geom.geom_type == 'Polygon':
            from shapely.geometry import Polygon
            # Ensure exterior is counter-clockwise
            if not geom.exterior.is_ccw:
                geom = Polygon(list(geom.exterior.coords)[::-1],
                              [list(interior.coords)[::-1] for interior in geom.interiors])
        return geom.wkt
    except Exception:
        return None

spark.udf.register("fn_fix_geography_order", fn_fix_geography_order)

# COMMAND ----------

@udf(returnType=ArrayType(StructType([
    StructField("neighbor_id", IntegerType()),
    StructField("distance", DoubleType()),
    StructField("rank", IntegerType())
])))
def fn_nearest_neighbor_3_ordered(target_lat, target_lon, neighbor_ids, neighbor_lats, neighbor_lons):
    """Find 3 nearest neighbors ordered by distance.

    Equivalent to: stg.fnNearestNeighbor_3Ordered
    """
    if None in (target_lat, target_lon) or not neighbor_ids:
        return None

    R = 6371000  # Earth radius in meters
    distances = []

    for i, (nid, nlat, nlon) in enumerate(zip(neighbor_ids, neighbor_lats, neighbor_lons)):
        if nlat is not None and nlon is not None:
            phi1, phi2 = radians(target_lat), radians(nlat)
            delta_phi = radians(nlat - target_lat)
            delta_lambda = radians(nlon - target_lon)

            a = sin(delta_phi/2)**2 + cos(phi1) * cos(phi2) * sin(delta_lambda/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            distance = R * c

            distances.append({"neighbor_id": nid, "distance": distance})

    # Sort by distance and take top 3
    distances.sort(key=lambda x: x["distance"])
    return [{"neighbor_id": d["neighbor_id"], "distance": d["distance"], "rank": i+1}
            for i, d in enumerate(distances[:3])]

spark.udf.register("fn_nearest_neighbor_3_ordered", fn_nearest_neighbor_3_ordered)
```

---

### 3.3 Convert Hierarchical Functions to Python UDFs
Convert manager hierarchy traversal functions.

**File:** `migration_project/pipelines/udfs/hierarchy_udfs.py`

**Source Files to Read:**
- `migration_project/source_sql/functions/dbo.fnManagersByLevel.sql`
- `migration_project/source_sql/functions/dbo.fnManagersByLevelSlicedIntervals.sql`

**Approach:**
These functions require access to the ManagerAssignments table. Best implemented as:
1. Pre-computed DLT table that builds the hierarchy
2. Python UDF for runtime traversal (less efficient)

**Recommendation:** Create a DLT table `manager_hierarchy` that pre-computes all manager paths:

```python
@dlt.table(name="manager_hierarchy")
def manager_hierarchy():
    """Pre-computed manager hierarchy for all workers.

    Replaces: dbo.fnManagersByLevel, dbo.fnManagersByLevelSlicedIntervals
    """
    manager_assignments = dlt.read("bronze_dbo_ManagerAssignments")

    # Use GraphFrames or iterative approach to build hierarchy
    # Start with direct managers (level 1)
    hierarchy = (
        manager_assignments
        .filter(col("ValidTo").isNull())
        .select(
            col("WorkerID"),
            col("ProjectID"),
            col("ManagerWorkerID").alias("ManagerID_L1"),
            lit(1).alias("Level")
        )
    )

    # Iterate to build higher levels (up to 10)
    for level in range(2, 11):
        next_level = (
            hierarchy.filter(col("Level") == level - 1)
            .join(
                manager_assignments.filter(col("ValidTo").isNull()),
                (hierarchy[f"ManagerID_L{level-1}"] == manager_assignments["WorkerID"]) &
                (hierarchy["ProjectID"] == manager_assignments["ProjectID"]),
                "left"
            )
            .withColumn(f"ManagerID_L{level}", col("ManagerWorkerID"))
            .withColumn("Level", lit(level))
        )
        hierarchy = hierarchy.union(next_level)

    return hierarchy
```

---

### 3.4 Convert Security Predicate Functions to Row Filters
Convert SQL Server RLS predicates to Unity Catalog row filters.

**File:** `migration_project/pipelines/security/row_filters.sql`

**Source Files to Read:**
- `migration_project/source_sql/functions/security.fn_OrganizationPredicate.sql`
- `migration_project/source_sql/functions/security.fn_ProjectPredicate.sql`
- `migration_project/source_sql/functions/security.fn_ProjectPredicateEx.sql`
- `migration_project/source_sql/functions/security.fn_UserPredicate.sql`

**Steps:**
1. Read each source function to understand the permission logic
2. Create user-permission mapping tables in Unity Catalog
3. Create row filter functions
4. Apply filters to relevant tables

**Row Filter Implementation:**
```sql
-- Step 1: Create permission mapping tables (if not from Bronze)
CREATE TABLE IF NOT EXISTS wakecap_prod.security.user_organization_access (
    user_email STRING,
    organization_id INT,
    access_level STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wakecap_prod.security.user_project_access (
    user_email STRING,
    project_id INT,
    access_level STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
);

-- Step 2: Create row filter function for organizations
CREATE OR REPLACE FUNCTION wakecap_prod.security.organization_filter(org_id INT)
RETURNS BOOLEAN
LANGUAGE SQL
RETURN (
    -- Allow if user has explicit access
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_organization_access
        WHERE user_email = current_user()
        AND organization_id = org_id
        AND (valid_to IS NULL OR valid_to > current_timestamp())
    )
    OR
    -- Allow if user is admin (has access to all)
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_organization_access
        WHERE user_email = current_user()
        AND organization_id IS NULL  -- NULL means all organizations
        AND access_level = 'ADMIN'
    )
);

-- Step 3: Create row filter function for projects
CREATE OR REPLACE FUNCTION wakecap_prod.security.project_filter(project_id INT)
RETURNS BOOLEAN
LANGUAGE SQL
RETURN (
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_project_access
        WHERE user_email = current_user()
        AND project_id = project_filter.project_id
        AND (valid_to IS NULL OR valid_to > current_timestamp())
    )
    OR
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_project_access
        WHERE user_email = current_user()
        AND project_id IS NULL  -- NULL means all projects
    )
);

-- Step 4: Apply row filters to tables
ALTER TABLE wakecap_prod.migration.silver_worker
SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

ALTER TABLE wakecap_prod.migration.silver_project
SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

ALTER TABLE wakecap_prod.migration.silver_crew
SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

-- Add more tables as needed
```

---

### 3.5 Convert SVG/JSON Generation Functions
Convert project visualization functions.

**Source Files to Read:**
- `migration_project/source_sql/functions/dbo.fnProjectAutoSVG.sql`
- `migration_project/source_sql/functions/dbo.fnProjectAutoJSON.sql`
- `migration_project/source_sql/functions/dbo.fnLocationAutoSVG.sql`

**Approach:** These are complex functions that generate visualization output. Best implemented as Python UDFs that read from the Floor and Zone tables.

```python
@udf(returnType=StringType())
def fn_project_auto_svg(project_id, floors_json, zones_json, scale=1.0):
    """Generate SVG visualization for a project.

    Equivalent to: dbo.fnProjectAutoSVG
    """
    import json

    if not floors_json:
        return None

    floors = json.loads(floors_json) if isinstance(floors_json, str) else floors_json
    zones = json.loads(zones_json) if zones_json else []

    svg_parts = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 1000">']

    # Draw floors
    for floor in floors:
        if floor.get('GeometryWKT'):
            geom = wkt.loads(floor['GeometryWKT'])
            # Generate path...
            svg_parts.append(f'<g class="floor" data-floor-id="{floor.get("FloorID")}">')
            # Add floor geometry
            svg_parts.append('</g>')

    # Draw zones
    for zone in zones:
        if zone.get('GeometryWKT'):
            svg_parts.append(f'<g class="zone" data-zone-id="{zone.get("ZoneID")}">')
            # Add zone geometry
            svg_parts.append('</g>')

    svg_parts.append('</svg>')
    return '\n'.join(svg_parts)

spark.udf.register("fn_project_auto_svg", fn_project_auto_svg)
```

---

### PHASE 4: STORED PROCEDURE CONVERSION

### 4.1 Convert DeltaSync Dimension Procedures to DLT Streaming Tables
Convert incremental sync procedures for dimension tables.

**File:** `migration_project/pipelines/dlt/streaming_dimensions.py`

**Procedures to Convert (15):**

| Procedure | Lines | Target DLT Table |
|-----------|-------|------------------|
| stg.spDeltaSyncDimWorker.sql | 227 | streaming_worker |
| stg.spDeltaSyncDimProject.sql | 198 | streaming_project |
| stg.spDeltaSyncDimCrew.sql | 151 | streaming_crew |
| stg.spDeltaSyncDimDevice.sql | 149 | streaming_device |
| stg.spDeltaSyncDimTrade.sql | 132 | streaming_trade |
| stg.spDeltaSyncDimOrganization.sql | 90 | streaming_organization |
| stg.spDeltaSyncDimCompany.sql | 126 | streaming_company |
| stg.spDeltaSyncDimDepartment.sql | ~100 | streaming_department |
| stg.spDeltaSyncDimFloor.sql | 271 | streaming_floor |
| stg.spDeltaSyncDimZone.sql | 321 | streaming_zone |
| stg.spDeltaSyncDimWorkshift.sql | ~150 | streaming_workshift |
| stg.spDeltaSyncDimWorkshiftDetails.sql | 331 | streaming_workshift_details |
| stg.spDeltaSyncDimActivity.sql | ~100 | streaming_activity |
| stg.spDeltaSyncDimLocationGroup.sql | ~100 | streaming_location_group |
| stg.spDeltaSyncDimTitle.sql | ~80 | streaming_title |

**Steps for Each Procedure:**
1. Read the source SQL file to understand the MERGE logic
2. Identify the primary key column(s)
3. Identify the sequence column (usually ModifiedAt or a timestamp)
4. Create DLT streaming table with APPLY CHANGES INTO

**Template for DeltaSync Conversion:**
```python
# Read source procedure
# migration_project/source_sql/stored_procedures/stg.spDeltaSyncDimWorker.sql

@dlt.table(
    name="staging_worker",
    comment="Streaming staging table for Worker changes"
)
def staging_worker():
    """Staging table: Capture CDC changes from ADLS."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/worker/_schema")
        .load(get_adls_path("dimensions", "Worker"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table("worker_cdc")

dlt.apply_changes(
    target="worker_cdc",
    source="staging_worker",
    keys=["WorkerID"],
    sequence_by="_load_time",
    stored_as_scd_type=2,
    track_history_column_list=["WorkerName", "ProjectID", "CompanyID", "TradeID"]
)
```

---

### 4.2 Convert DeltaSync Fact Procedures to DLT Streaming Tables
Convert incremental sync procedures for fact tables.

**File:** `migration_project/pipelines/dlt/streaming_facts.py`

**Procedures to Convert (6):**

| Procedure | Lines | Patterns | Target DLT Table |
|-----------|-------|----------|------------------|
| stg.spDeltaSyncFactObservations.sql | 1165 | TEMP_TABLE, MERGE, WINDOW | streaming_observations |
| stg.spDeltaSyncFactWorkersHistory.sql | 1561 | TEMP_TABLE, SPATIAL | streaming_workers_history |
| stg.spDeltaSyncFactWeatherObservations.sql | 481 | TEMP_TABLE, MERGE | streaming_weather |
| stg.spDeltaSyncFactProgress.sql | ~200 | MERGE | streaming_progress |
| stg.spDeltaSyncFactReportedAttendance_ResourceTimesheet.sql | ~300 | MERGE | streaming_attendance_timesheet |
| stg.spDeltaSyncFactReportedAttendance_ResourceHours.sql | ~300 | MERGE | streaming_attendance_hours |

**Steps:**
1. Read each source procedure
2. Analyze the MERGE conditions and transformations
3. Create streaming DLT with appropriate APPLY CHANGES configuration
4. Handle SPATIAL columns by casting to WKT strings in the source

**Example for FactObservations:**
```python
@dlt.table(name="staging_observations")
def staging_observations():
    """Staging: Capture observation changes from ADLS."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/checkpoints/observations/_schema")
        .option("cloudFiles.maxFilesPerTrigger", "1000")  # Control ingestion rate
        .load(get_adls_path("facts", "FactObservations"))
        .withColumn("_load_time", current_timestamp())
    )

dlt.create_streaming_table(
    "observations_cdc",
    partition_cols=["ObservationDate"]  # Partition by date for performance
)

dlt.apply_changes(
    target="observations_cdc",
    source="staging_observations",
    keys=["ObservationID"],
    sequence_by="_load_time",
    stored_as_scd_type=1  # SCD Type 1 - overwrite for facts
)
```

---

### 4.3 Convert Assignment DeltaSync Procedures
Convert assignment table sync procedures.

**File:** `migration_project/pipelines/dlt/streaming_assignments.py`

**Procedures to Convert (9):**

| Procedure | Target DLT Table |
|-----------|------------------|
| stg.spDeltaSyncCrewAssignments.sql | streaming_crew_assignments |
| stg.spDeltaSyncDeviceAssignments.sql | streaming_device_assignments |
| stg.spDeltaSyncWorkshiftAssignments.sql | streaming_workshift_assignments |
| stg.spDeltaSyncManagerAssignments.sql | streaming_manager_assignments |
| stg.spDeltaSyncWorkerLocationAssignments.sql | streaming_worker_location_assignments |
| stg.spDeltaSyncLocationGroupAssignments.sql | streaming_location_group_assignments |
| stg.spDeltaSyncCrewManagerAssignments.sql | streaming_crew_manager_assignments |
| stg.spDeltaSyncWorkerStatusAssignments.sql | streaming_worker_status_assignments |
| stg.spDeltaSyncPermissions.sql | streaming_permissions |

---

### 4.4 Convert Simple Calculate Procedures to DLT Batch Tables
Convert calculation procedures without CURSOR or DYNAMIC SQL.

**File:** `migration_project/pipelines/dlt/batch_calculations.py`

**Procedures to Convert (6):**

| Procedure | Lines | Patterns | Target DLT Table |
|-----------|-------|----------|------------------|
| stg.spCalculateFactReportedAttendance.sql | 215 | MERGE, CTE | calc_reported_attendance |
| stg.spCalculateFactProgress.sql | 112 | MERGE, CTE | calc_progress |
| stg.spCalculateProjectActiveFlag.sql | 36 | CTE | calc_project_active |
| stg.spCalculateLocationGroupAssignments.sql | 153 | TEMP_TABLE, MERGE | calc_location_groups |
| stg.spCalculateManagerAssignments.sql | 151 | TEMP_TABLE, MERGE | calc_manager_assignments |
| stg.spCalculateManagerAssignmentSnapshots.sql | 527 | TEMP_TABLE, PIVOT | calc_manager_snapshots |

**Steps for Each:**
1. Read source procedure
2. Convert TEMP TABLES to CTEs or intermediate DLT tables
3. Convert MERGE to DLT table definition
4. Replace T-SQL functions with Databricks equivalents

**Example - spCalculateFactReportedAttendance:**
```python
@dlt.table(
    name="calc_reported_attendance",
    comment="Calculated reported attendance aggregates"
)
def calc_reported_attendance():
    """Calculate reported attendance from timesheet data.

    Converted from: stg.spCalculateFactReportedAttendance
    """
    # Read source data
    timesheet = dlt.read("bronze_dbo_FactReportedAttendance")
    workers = dlt.read("silver_worker")
    projects = dlt.read("silver_project")

    # Apply calculation logic (convert from original procedure)
    return (
        timesheet
        .join(workers, ["WorkerID", "ProjectID"], "inner")
        .join(projects, "ProjectID", "inner")
        .groupBy(
            "ProjectID",
            "WorkerID",
            "ReportDate"
        )
        .agg(
            sum("ReportedHours").alias("TotalReportedHours"),
            sum("ReportedOvertimeHours").alias("TotalOvertimeHours"),
            count("*").alias("EntryCount")
        )
        .withColumn("_calculated_at", current_timestamp())
    )
```

---

### 4.5 Convert Complex CURSOR Procedures to Python Notebooks
Convert procedures with CURSOR, DYNAMIC_SQL, or complex logic.

**Target Directory:** `migration_project/pipelines/notebooks/`

**Procedures to Convert (5):**

| Procedure | Lines | Patterns | Notebook |
|-----------|-------|----------|----------|
| stg.spCalculateFactWorkersShifts.sql | 1639 | CURSOR, TEMP_TABLE | calc_fact_workers_shifts.py |
| stg.spCalculateFactWorkersShiftsCombined.sql | 1287 | TEMP_TABLE, MERGE, WINDOW | calc_fact_workers_shifts_combined.py |
| stg.spCalculateFactWorkersContacts_ByRule.sql | 951 | CURSOR, DYNAMIC_SQL | calc_worker_contacts.py |
| mrg.spMergeOldData.sql | 903 | CURSOR, SPATIAL | merge_old_data.py |
| stg.spWorkersHistory_UpdateAssignments_3_LocationClass.sql | 553 | TEMP_TABLE | update_workers_history.py |

**Conversion Approach for CURSOR Procedures:**
1. Identify what the cursor iterates over
2. Replace cursor loop with DataFrame window functions or groupBy/agg operations
3. Replace TEMP TABLES with DataFrame variables or temp views
4. Replace DYNAMIC SQL with parameterized SQL templates

---

#### 4.5.1 Convert stg.spCalculateFactWorkersShifts (Notebook)

**File:** `migration_project/pipelines/notebooks/calc_fact_workers_shifts.py`

**Source:** `migration_project/source_sql/stored_procedures/stg.spCalculateFactWorkersShifts.sql`

**Steps:**
1. Read the source procedure (1639 lines)
2. Identify the CURSOR iteration pattern - iterates over workers/dates
3. Convert to window functions for shift boundary detection
4. Use DataFrame groupBy for aggregations

**Notebook Template:**
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate Fact Workers Shifts
# MAGIC
# MAGIC Converted from: stg.spCalculateFactWorkersShifts
# MAGIC
# MAGIC **Purpose:** Calculate daily shift metrics for each worker based on observations.
# MAGIC
# MAGIC **Original Logic:**
# MAGIC 1. CURSOR iterates through workers by project and date
# MAGIC 2. For each worker-date, calculates shift start/end times
# MAGIC 3. Calculates active/inactive time based on observation gaps
# MAGIC 4. Calculates distance traveled between observations
# MAGIC 5. MERGEs results into FactWorkersShifts table

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

def calculate_fact_workers_shifts(run_date=None):
    """
    Calculate worker shift metrics for a given date.

    Args:
        run_date: Date to calculate shifts for (default: yesterday)

    Returns:
        DataFrame with shift metrics
    """

    if run_date is None:
        run_date = date_sub(current_date(), 1)

    # Load source data
    observations = spark.table("wakecap_prod.migration.bronze_dbo_FactObservations")
    workers = spark.table("wakecap_prod.migration.silver_worker")
    workshifts = spark.table("wakecap_prod.migration.bronze_dbo_WorkshiftAssignments")

    # Define window for calculating time gaps between observations
    worker_window = (
        Window
        .partitionBy("WorkerID", "ProjectID", "ShiftLocalDate")
        .orderBy("ObservationTime")
    )

    # Step 1: Filter observations for the run date
    daily_observations = (
        observations
        .filter(col("ShiftLocalDate") == run_date)
        .filter(col("WorkerID").isNotNull())
    )

    # Step 2: Calculate time gaps and activity flags (replaces CURSOR logic)
    observations_with_gaps = (
        daily_observations
        .withColumn("prev_time", lag("ObservationTime").over(worker_window))
        .withColumn("prev_lat", lag("Latitude").over(worker_window))
        .withColumn("prev_lon", lag("Longitude").over(worker_window))
        .withColumn(
            "time_gap_seconds",
            when(col("prev_time").isNotNull(),
                 unix_timestamp("ObservationTime") - unix_timestamp("prev_time")
            ).otherwise(0)
        )
        .withColumn(
            "is_active",
            col("time_gap_seconds") <= 300  # 5 minute threshold
        )
        .withColumn(
            "distance_meters",
            when(
                col("prev_lat").isNotNull() & col("prev_lon").isNotNull(),
                # Haversine formula approximation
                sqrt(
                    pow((col("Latitude") - col("prev_lat")) * 111320, 2) +
                    pow((col("Longitude") - col("prev_lon")) * 111320 * cos(radians(col("Latitude"))), 2)
                )
            ).otherwise(0)
        )
    )

    # Step 3: Aggregate by worker-project-date (replaces CURSOR aggregation)
    shift_metrics = (
        observations_with_gaps
        .groupBy("WorkerID", "ProjectID", "ShiftLocalDate")
        .agg(
            min("ObservationTime").alias("FirstObservation"),
            max("ObservationTime").alias("LastObservation"),
            (
                unix_timestamp(max("ObservationTime")) -
                unix_timestamp(min("ObservationTime"))
            ).alias("TotalShiftDuration"),
            sum(
                when(col("is_active"), col("time_gap_seconds")).otherwise(0)
            ).alias("ActiveTimeDuringShift"),
            sum(
                when(~col("is_active") & (col("time_gap_seconds") < 3600),
                     col("time_gap_seconds")).otherwise(0)
            ).alias("InactiveTimeDuringShift"),
            count("*").alias("Readings"),
            sum("distance_meters").alias("DistanceTravelled"),
            first("CrewID").alias("CrewID"),
            first("FloorID").alias("FloorID"),
            first("ZoneID").alias("ZoneID")
        )
        .withColumn("_calculated_at", current_timestamp())
    )

    return shift_metrics

# COMMAND ----------

# Step 4: MERGE into target table (replaces MERGE statement)
def merge_shift_metrics(shift_metrics):
    """Merge calculated metrics into FactWorkersShifts table."""

    target_table = "wakecap_prod.migration.fact_workers_shifts"

    # Check if target exists
    if spark.catalog.tableExists(target_table):
        target = DeltaTable.forName(spark, target_table)

        (target.alias("target")
         .merge(
             shift_metrics.alias("source"),
             """
             target.WorkerID = source.WorkerID AND
             target.ProjectID = source.ProjectID AND
             target.ShiftLocalDate = source.ShiftLocalDate
             """
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute()
        )
    else:
        shift_metrics.write.format("delta").saveAsTable(target_table)

# COMMAND ----------

# Execute calculation
if __name__ == "__main__":
    # Run for specific date or default (yesterday)
    run_date = dbutils.widgets.get("run_date") if "run_date" in dbutils.widgets else None

    shift_metrics = calculate_fact_workers_shifts(run_date)
    display(shift_metrics.limit(100))

    # Merge results
    merge_shift_metrics(shift_metrics)
    print(f"Processed {shift_metrics.count()} shift records")
```

---

#### 4.5.2 Convert stg.spCalculateFactWorkersContacts_ByRule (Notebook)

**File:** `migration_project/pipelines/notebooks/calc_worker_contacts.py`

**Source:** `migration_project/source_sql/stored_procedures/stg.spCalculateFactWorkersContacts_ByRule.sql`

**Steps:**
1. Read source procedure (951 lines, CURSOR, DYNAMIC_SQL)
2. Replace CURSOR over rules with DataFrame iteration
3. Replace DYNAMIC SQL with parameterized query builder
4. Use H3 for efficient spatial proximity detection

**Key Conversion:**
- Original uses CURSOR to iterate through ContactTracingRules
- Each rule has different conditions (proximity distance, zone overlap, etc.)
- DYNAMIC SQL builds query based on rule type

**Notebook Template:**
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate Worker Contacts by Rule
# MAGIC
# MAGIC Converted from: stg.spCalculateFactWorkersContacts_ByRule
# MAGIC
# MAGIC **Purpose:** Identify worker contacts based on configurable rules.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import h3

# COMMAND ----------

def get_active_rules():
    """Load active contact tracing rules."""
    return (
        spark.table("wakecap_prod.migration.bronze_dbo_ContactTracingRule")
        .filter(col("IsActive") == True)
        .filter(col("DeletedAt").isNull())
        .collect()
    )

# COMMAND ----------

def calculate_proximity_contacts(rule, run_date):
    """
    Calculate contacts based on spatial proximity.

    Replaces DYNAMIC SQL for PROXIMITY rule type.
    """
    observations = (
        spark.table("wakecap_prod.migration.bronze_dbo_FactObservations")
        .filter(col("ShiftLocalDate") == run_date)
        .filter(col("Latitude").isNotNull())
        .filter(col("Longitude").isNotNull())
    )

    # Add H3 index for efficient spatial bucketing
    h3_resolution = 9  # ~174m hexagons

    @udf(returnType=StringType())
    def lat_lng_to_h3(lat, lng):
        if lat is None or lng is None:
            return None
        return h3.geo_to_h3(lat, lng, h3_resolution)

    observations_h3 = observations.withColumn(
        "h3_index",
        lat_lng_to_h3(col("Latitude"), col("Longitude"))
    )

    # Self-join on H3 index for potential contacts
    contacts = (
        observations_h3.alias("a")
        .join(
            observations_h3.alias("b"),
            (col("a.h3_index") == col("b.h3_index")) &
            (col("a.WorkerID") < col("b.WorkerID")) &  # Avoid duplicates
            (col("a.ProjectID") == col("b.ProjectID")) &
            (abs(unix_timestamp(col("a.ObservationTime")) -
                 unix_timestamp(col("b.ObservationTime"))) <= rule.TimeThresholdSeconds)
        )
        .select(
            col("a.WorkerID").alias("WorkerID1"),
            col("b.WorkerID").alias("WorkerID2"),
            col("a.ObservationTime").alias("ContactTime"),
            col("a.ProjectID"),
            lit(rule.RuleID).alias("RuleID"),
            lit("PROXIMITY").alias("ContactType")
        )
    )

    return contacts

# COMMAND ----------

def calculate_zone_contacts(rule, run_date):
    """
    Calculate contacts based on shared zone occupancy.

    Replaces DYNAMIC SQL for ZONE rule type.
    """
    observations = (
        spark.table("wakecap_prod.migration.bronze_dbo_FactObservations")
        .filter(col("ShiftLocalDate") == run_date)
        .filter(col("ZoneID").isNotNull())
    )

    # Workers in same zone within time threshold
    contacts = (
        observations.alias("a")
        .join(
            observations.alias("b"),
            (col("a.ZoneID") == col("b.ZoneID")) &
            (col("a.WorkerID") < col("b.WorkerID")) &
            (col("a.ProjectID") == col("b.ProjectID")) &
            (abs(unix_timestamp(col("a.ObservationTime")) -
                 unix_timestamp(col("b.ObservationTime"))) <= rule.TimeThresholdSeconds)
        )
        .select(
            col("a.WorkerID").alias("WorkerID1"),
            col("b.WorkerID").alias("WorkerID2"),
            col("a.ObservationTime").alias("ContactTime"),
            col("a.ProjectID"),
            col("a.ZoneID"),
            lit(rule.RuleID).alias("RuleID"),
            lit("ZONE").alias("ContactType")
        )
    )

    return contacts

# COMMAND ----------

def calculate_contacts_by_rule(run_date):
    """
    Main function - replaces CURSOR over rules.
    """
    rules = get_active_rules()
    all_contacts = None

    for rule in rules:
        if rule.RuleType == "PROXIMITY":
            contacts = calculate_proximity_contacts(rule, run_date)
        elif rule.RuleType == "ZONE":
            contacts = calculate_zone_contacts(rule, run_date)
        elif rule.RuleType == "MANAGER":
            contacts = calculate_manager_contacts(rule, run_date)
        else:
            continue

        if all_contacts is None:
            all_contacts = contacts
        else:
            all_contacts = all_contacts.union(contacts)

    return all_contacts.distinct() if all_contacts else None

# COMMAND ----------

# Execute
if __name__ == "__main__":
    run_date = dbutils.widgets.get("run_date") if "run_date" in dbutils.widgets else date_sub(current_date(), 1)

    contacts = calculate_contacts_by_rule(run_date)
    if contacts:
        display(contacts.limit(100))

        # Write to target table
        contacts.write.format("delta").mode("append").saveAsTable(
            "wakecap_prod.migration.fact_workers_contacts"
        )
```

---

#### 4.5.3 Convert stg.spCalculateFactWorkersShiftsCombined (Notebook)

**File:** `migration_project/pipelines/notebooks/calc_fact_workers_shifts_combined.py`

**Source:** `migration_project/source_sql/stored_procedures/stg.spCalculateFactWorkersShiftsCombined.sql`

**Steps:**
1. Read source (1287 lines, TEMP_TABLE, MERGE, WINDOW)
2. Replace temp tables with DataFrame variables
3. Convert window functions to PySpark equivalents
4. Combine shifts across multiple workshifts

---

#### 4.5.4 Convert mrg.spMergeOldData (Notebook)

**File:** `migration_project/pipelines/notebooks/merge_old_data.py`

**Source:** `migration_project/source_sql/stored_procedures/mrg.spMergeOldData.sql`

**Assessment:** This is a one-time migration procedure for historical data. Consider:
1. Running the original on SQL Server to completion
2. Extracting the final result via ADF
3. Loading into Databricks as a historical baseline

**If conversion is required:**
- Replace CURSOR with batch processing
- Handle SPATIAL columns with Python UDFs
- Use Delta Lake transactions for consistency

---

#### 4.5.5 Convert stg.spWorkersHistory_UpdateAssignments_3_LocationClass (Notebook)

**File:** `migration_project/pipelines/notebooks/update_workers_history.py`

**Source:** `migration_project/source_sql/stored_procedures/stg.spWorkersHistory_UpdateAssignments_3_LocationClass.sql`

---

### 4.6 Skip or Replace Admin Procedures
These procedures are not needed in Databricks or have simpler alternatives.

| Original Procedure | Databricks Replacement |
|-------------------|----------------------|
| dbo.spMaintainIndexes | `OPTIMIZE table ZORDER BY (columns)` |
| dbo.spMaintainPartitions | Delta handles automatically |
| dbo.spMaintainPartitions_Site | Delta handles automatically |
| dbo.spUpdateDBStats | Not needed - Delta auto-computes |
| stg.spCleanupLog | `VACUUM table RETAIN 168 HOURS` |
| stg.spResetSyncState | Create simple cleanup SQL |
| stg.spResetDatabase | Not needed - use DROP/CREATE |

---

### PHASE 5: GOLD LAYER COMPLETION

### 5.1 Deploy All Transpiled Views as Gold Layer Tables
Convert all 34 transpiled views to DLT Gold layer tables.

**File:** `migration_project/pipelines/dlt/gold_views.py`

**Steps:**
1. Read each transpiled view from `migration_project/transpiled/views/`
2. Convert VIEW definition to DLT @dlt.table decorator
3. Replace table references with dlt.read() calls
4. Handle spatial columns (cast to WKT string)

**Views to Add (24 not yet in pipeline):**

| View | Priority | Spatial Handling |
|------|----------|------------------|
| vwFactWorkersShifts_Ext | HIGH | None |
| vwFactWorkersShiftsCombined_Ext | HIGH | None |
| vwFactWorkersShiftsCombined_Ext2 | MEDIUM | None |
| vwFactWorkersHistory | HIGH | None |
| vwFactReportedAttendance | MEDIUM | None |
| vwFactProgress | MEDIUM | None |
| vwFactWeatherObservations | LOW | None |
| vwFloor | MEDIUM | Cast Geography to WKT |
| vwZone | MEDIUM | Cast Geography to WKT |
| vwContactTracingRule | MEDIUM | None |
| vwDate | LOW | None |
| vwDeviceAssignment_Continuous | MEDIUM | None |
| vwLocationGroupAssignments | MEDIUM | None |
| vwManagerAssignments | MEDIUM | None |
| vwManagerAssignments_Continuous | MEDIUM | None |
| vwManagerAssignments_Expanded | MEDIUM | None |
| vwManagerAssignmentSnapshots | MEDIUM | None |
| vwSyncState | LOW | None |
| vwTradeAssignments | MEDIUM | None |
| vwWorkerPropertiesHistory | MEDIUM | None |
| vwWorkerTitleDepartmentAssignments | MEDIUM | None |
| vwWorkshiftDetails | MEDIUM | None |
| vwWorkshiftDetails_DaysOfWeek | LOW | None |
| vwWorkshiftDetails_SpecialDates | LOW | None |

**Template for View Conversion:**
```python
# Read source: migration_project/transpiled/views/dbo.vwFactWorkersShifts_Ext.sql

@dlt.table(
    name="gold_vwFactWorkersShifts_Ext",
    comment="Extended worker shifts view with derived metrics"
)
def gold_vwFactWorkersShifts_Ext():
    """Gold view: Worker shifts with extended calculations.

    Converted from: dbo.vwFactWorkersShifts_Ext
    """
    shifts = dlt.read("silver_workers_shifts")
    workers = dlt.read("silver_worker")
    projects = dlt.read("silver_project")
    crews = dlt.read("silver_crew")

    return (
        shifts
        .join(workers, ["WorkerID", "ProjectID"], "left")
        .join(projects, "ProjectID", "left")
        .join(crews, "CrewID", "left")
        .select(
            # Select columns matching original view definition
            shifts["*"],
            workers["WorkerName"],
            workers["Worker"],
            projects["ProjectName"],
            projects["Project"],
            crews["CrewName"],
            crews["Crew"],
            # Calculated fields from original view
            (col("ActiveTimeDuringShift") / 3600).alias("ActiveHours"),
            (col("InactiveTimeDuringShift") / 3600).alias("InactiveHours"),
            (col("DistanceTravelled") / 1000).alias("DistanceKm")
        )
    )
```

---

### 5.2 Handle Views with Spatial Data
Special handling for vwFloor and vwZone which have Geography columns.

**Source Views:**
- `migration_project/transpiled/views/dbo.vwFloor.sql`
- `migration_project/transpiled/views/dbo.vwZone.sql`

**Approach:**
1. Cast Geography column to WKT string during bronze ingestion
2. Or exclude Geography column from Gold view if not needed

```python
@dlt.table(name="gold_vwFloor")
def gold_vwFloor():
    """Gold view: Floor with geometry as WKT string."""
    floors = dlt.read("bronze_dbo_Floor")

    return (
        floors
        .withColumn(
            "GeometryWKT",
            # If source has Geography column, cast to string
            # Note: This may already be handled in ADF extraction
            col("Geometry").cast("string") if "Geometry" in floors.columns else lit(None)
        )
        .drop("Geometry")  # Drop binary geography
    )
```

---

### 5.3 Handle Merge Schema Views (mrg.*)
Evaluate whether mrg.* views are needed.

**Views:**
- mrg.vwContactTracingRule
- mrg.vwFactReportedAttendance
- mrg.vwFloor
- mrg.vwProject
- mrg.vwZone
- mrg.vwWorkShiftDetails

**Assessment:**
- If mrg.* tables are used for historical data replication, extract via ADF
- If not needed post-migration, skip these views
- If needed, create as Gold views referencing mrg.* Bronze tables

---

### PHASE 6: SILVER LAYER ENHANCEMENT

### 6.1 Add Data Quality Expectations to All Silver Tables
Enhance Silver layer with comprehensive data quality checks.

**File:** Update `migration_project/pipelines/dlt/silver_dimensions.py`

**Data Quality Rules by Table:**

| Table | Expectation | Action |
|-------|-------------|--------|
| silver_worker | WorkerID NOT NULL | DROP |
| silver_worker | ProjectID NOT NULL | DROP |
| silver_worker | Worker (code) NOT NULL | WARN |
| silver_project | ProjectID NOT NULL | DROP |
| silver_project | Project (code) NOT NULL | WARN |
| silver_project | OrganizationID NOT NULL | DROP |
| silver_crew | CrewID NOT NULL | DROP |
| silver_crew | ProjectID NOT NULL | DROP |
| silver_device | DeviceID NOT NULL | DROP |
| silver_device | Device (code) NOT NULL | WARN |
| silver_organization | OrganizationID NOT NULL | DROP |
| silver_company | CompanyID NOT NULL | DROP |
| silver_fact_observations | ObservationID NOT NULL | DROP |
| silver_fact_observations | ObservationTime NOT NULL | DROP |
| silver_fact_observations | WorkerID NOT NULL | WARN |
| silver_fact_workers_shifts | WorkerID NOT NULL | DROP |
| silver_fact_workers_shifts | ShiftLocalDate NOT NULL | DROP |
| silver_fact_workers_shifts | ActiveTimeDuringShift >= 0 | WARN |

**Template:**
```python
@dlt.table(name="silver_worker")
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
@dlt.expect("has_worker_code", "Worker IS NOT NULL")
@dlt.expect("valid_name", "LENGTH(WorkerName) > 0")
def silver_worker():
    """Silver: Cleaned worker data with quality checks."""
    return (
        dlt.read("bronze_dbo_Worker")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("wakecap"))
    )
```

---

### 6.2 Add Silver Layer for All Remaining Dimensions
Create Silver tables for dimensions not yet included.

**Tables to Add:**
- silver_activity
- silver_department
- silver_task
- silver_title
- silver_worker_status
- silver_workshift_details
- silver_location_group
- silver_observation_type
- silver_observation_source

---

### 6.3 Add Silver Layer for Fact Tables
Create Silver tables for all fact tables with appropriate quality checks.

**Tables:**
- silver_fact_observations
- silver_fact_workers_history
- silver_fact_workers_shifts
- silver_fact_workers_shifts_combined
- silver_fact_reported_attendance
- silver_fact_progress
- silver_fact_workers_contacts
- silver_fact_workers_tasks
- silver_fact_weather_observations

---

### PHASE 7: VALIDATION AND RECONCILIATION

### 7.1 Create Validation Notebook
Create a notebook to validate migration completeness.

**File:** `migration_project/pipelines/notebooks/validation_reconciliation.py`

**Validation Checks:**
1. Row count comparison (source vs target)
2. Data type verification
3. Null count comparison
4. Primary key uniqueness
5. Foreign key integrity
6. Business logic validation (sample record comparison)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Migration Validation & Reconciliation

# COMMAND ----------

def validate_row_counts():
    """Compare row counts between source and target."""

    expected_counts = {
        # From source system (populated during extraction)
        "Worker": 50000,
        "Project": 500,
        "Crew": 5000,
        "FactObservations": 52000000,
        "FactWorkersShifts": 5000000,
        # ... add all tables
    }

    results = []
    for table, expected in expected_counts.items():
        actual = spark.table(f"wakecap_prod.migration.bronze_dbo_{table}").count()
        match = actual == expected
        results.append({
            "table": table,
            "expected": expected,
            "actual": actual,
            "match": match,
            "difference": abs(expected - actual)
        })

    return spark.createDataFrame(results)

# COMMAND ----------

def validate_primary_keys():
    """Check primary key uniqueness."""

    pk_definitions = {
        "bronze_dbo_Worker": ["WorkerID"],
        "bronze_dbo_Project": ["ProjectID"],
        "bronze_dbo_FactObservations": ["ObservationID"],
        # ... add all tables
    }

    results = []
    for table, pk_cols in pk_definitions.items():
        df = spark.table(f"wakecap_prod.migration.{table}")
        total = df.count()
        distinct = df.select(pk_cols).distinct().count()

        results.append({
            "table": table,
            "pk_columns": ",".join(pk_cols),
            "total_rows": total,
            "distinct_keys": distinct,
            "has_duplicates": total != distinct
        })

    return spark.createDataFrame(results)

# COMMAND ----------

def validate_foreign_keys():
    """Check referential integrity."""

    fk_checks = [
        ("bronze_dbo_Worker", "ProjectID", "bronze_dbo_Project", "ProjectID"),
        ("bronze_dbo_Worker", "CompanyID", "bronze_dbo_Company", "CompanyID"),
        ("bronze_dbo_Crew", "ProjectID", "bronze_dbo_Project", "ProjectID"),
        # ... add all FK relationships
    ]

    results = []
    for child_table, child_col, parent_table, parent_col in fk_checks:
        child = spark.table(f"wakecap_prod.migration.{child_table}")
        parent = spark.table(f"wakecap_prod.migration.{parent_table}")

        orphans = (
            child.select(child_col).distinct()
            .join(
                parent.select(parent_col).distinct(),
                child[child_col] == parent[parent_col],
                "left_anti"
            )
            .count()
        )

        results.append({
            "child_table": child_table,
            "child_column": child_col,
            "parent_table": parent_table,
            "parent_column": parent_col,
            "orphan_count": orphans
        })

    return spark.createDataFrame(results)

# COMMAND ----------

# Run all validations
row_count_results = validate_row_counts()
display(row_count_results.filter(col("match") == False))

pk_results = validate_primary_keys()
display(pk_results.filter(col("has_duplicates") == True))

fk_results = validate_foreign_keys()
display(fk_results.filter(col("orphan_count") > 0))
```

---

### 7.2 Update Migration Status Document
Update MIGRATION_STATUS.md with completion status.

**Actions:**
1. Mark each converted object as COMPLETED
2. Update phase progress percentages
3. Add conversion statistics
4. Document any issues or known limitations

---

### 7.3 Generate Final Migration Report
Create comprehensive migration completion report.

**Report Contents:**
- Total objects migrated by type
- Data quality metrics
- Row count reconciliation summary
- Known issues and limitations
- Performance benchmarks
- Maintenance recommendations

---

## Testing Strategy

### Unit Testing
- Test each UDF with sample inputs
- Verify SQL UDF output matches original T-SQL function output
- Test Python UDFs with edge cases (nulls, empty strings, invalid geometry)

### Integration Testing
- Run full DLT pipeline in development mode
- Verify all tables populate correctly
- Check data quality expectation results
- Validate row counts match source

### Business Logic Testing
- Compare sample records between source and target
- Verify calculated fields produce same results
- Test edge cases (workers with no observations, projects with no crews)

### Performance Testing
- Benchmark query performance on Gold layer views
- Compare Databricks query times to SQL Server baselines
- Identify queries requiring optimization (Z-ORDER, partitioning)

---

## Acceptance Criteria

- [ ] All 142 tables ingested into Bronze layer from ADLS
- [ ] All 70 stored procedures converted:
  - [ ] 30 DeltaSync → DLT streaming tables
  - [ ] 6 simple Calculate → DLT batch tables
  - [ ] 5 complex CURSOR → Python notebooks
  - [ ] 6 admin → Replaced with Delta commands
  - [ ] 23 remaining → Categorized and converted
- [ ] All 23 functions converted:
  - [ ] 5 simple → SQL UDFs
  - [ ] 8 spatial → Python UDFs with H3/Shapely
  - [ ] 2 hierarchical → DLT table + UDF
  - [ ] 4 security → Unity Catalog row filters
  - [ ] 4 SVG/JSON → Python UDFs
- [ ] All 34 views deployed as Gold layer tables
- [ ] All Silver tables have data quality expectations
- [ ] Row count reconciliation passes (>99% match)
- [ ] Primary key validation passes (no duplicates)
- [ ] Foreign key validation passes (<1% orphans)
- [ ] MIGRATION_STATUS.md updated with 100% completion

---

## Validation Commands

Execute these commands to validate the task is complete:

```bash
# Validate Python syntax for all generated files
python -m py_compile migration_project/pipelines/dlt/*.py
python -m py_compile migration_project/pipelines/notebooks/*.py
python -m py_compile migration_project/pipelines/udfs/*.py

# Count DLT tables defined
grep -c "@dlt.table" migration_project/pipelines/dlt/*.py

# Count UDFs defined
grep -c "CREATE.*FUNCTION\|@udf" migration_project/pipelines/udfs/*.sql migration_project/pipelines/udfs/*.py

# Count notebooks created
ls -la migration_project/pipelines/notebooks/*.py | wc -l

# Verify all source procedures accounted for
ls migration_project/source_sql/stored_procedures/*.sql | wc -l  # Should be 70

# Verify all source functions accounted for
ls migration_project/source_sql/functions/*.sql | wc -l  # Should be 23
```

**Databricks Validation Queries:**
```sql
-- Count Bronze tables
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'migration' AND table_name LIKE 'bronze_%';

-- Count Silver tables
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'migration' AND table_name LIKE 'silver_%';

-- Count Gold tables
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'migration' AND table_name LIKE 'gold_%';

-- Check UDFs created
SELECT * FROM information_schema.routines
WHERE routine_schema = 'migration';

-- Verify row counts for key tables
SELECT 'Worker' as table_name, COUNT(*) as row_count FROM wakecap_prod.migration.bronze_dbo_Worker
UNION ALL
SELECT 'Project', COUNT(*) FROM wakecap_prod.migration.bronze_dbo_Project
UNION ALL
SELECT 'FactObservations', COUNT(*) FROM wakecap_prod.migration.bronze_dbo_FactObservations;
```

---

## Notes

### Dependencies
- Azure Data Factory pipelines must be created to extract data to ADLS
- ADLS external location must be configured in Unity Catalog
- H3 and Shapely libraries must be available on Databricks cluster
- Secret scope must have ADLS storage credentials

### Known Limitations
1. Spatial functions use approximations (Haversine vs. geodetic calculations)
2. Unity Catalog row filters may have performance impact on large tables
3. Some complex procedures may require iterative refinement after testing
4. Historical data from mrg schema may need special handling

### Recommended Cluster Configuration
```
Cluster Type: Serverless (to avoid VM quota issues)
Photon: Enabled
Libraries:
  - h3-py
  - shapely
```

### Monitoring Recommendations
- Set up DLT pipeline alerts for data quality failures
- Monitor Delta table storage growth
- Track query performance on Gold layer views
- Set up data freshness alerts

---

*Last Updated: 2026-01-19*
*Plan Generated By: Claude-Driven Development*
