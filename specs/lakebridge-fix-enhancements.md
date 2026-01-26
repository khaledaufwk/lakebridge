# Lakebridge Fix Skill - Enhancement Requirements

**Document Version:** 1.0
**Created:** 2026-01-25
**Purpose:** Define enhancements needed to support automated stored procedure migration fixes

---

## Executive Summary

The current `lakebridge-fix` skill provides infrastructure fixes (schema, secrets, pipeline configuration) and basic notebook format fixes. However, it lacks the ability to automatically convert T-SQL patterns to Databricks equivalents or generate complete notebook code from stored procedures.

This document specifies enhancements to enable:
1. Automated T-SQL pattern conversion
2. Notebook generation from stored procedures
3. CURSOR to Window function transformation
4. MERGE statement generation
5. Dependency resolution and creation
6. Validation notebook generation

---

## Current State Assessment

### Existing Capabilities (fixer.py)

| Feature | Location | Description |
|---------|----------|-------------|
| Fix Status | `FixStatus` enum | SUCCESS, FAILED, SKIPPED, MANUAL_REQUIRED |
| Fix Actions | `FixAction` dataclass | Track individual fixes |
| Fix Result | `FixResult` dataclass | Aggregate fix report |
| Missing Schema | `fix_missing_schema()` | Create Unity Catalog schema |
| Missing Scope | `fix_missing_secret_scope()` | Create Databricks secret scope |
| Missing Secrets | `fix_missing_secrets()` | Set SQL Server credentials |
| Notebook Format | `fix_notebook_format()` | Add header, DLT import |
| Ambiguous Columns | `fix_ambiguous_columns()` | Prefix column names |
| Serverless Flag | `fix_serverless_flag()` | Change serverless=True |
| Pipeline Serverless | `fix_pipeline_serverless()` | Recreate pipeline |

### Missing Capabilities

| Gap | Impact | Priority |
|-----|--------|----------|
| SP conversion templates | Manual conversion each time | HIGH |
| CURSOR→Window transformation | Most complex pattern unfixed | HIGH |
| MERGE generation | Manual Delta MERGE writing | HIGH |
| Temp table conversion | Manual createOrReplaceTempView | MEDIUM |
| Spatial function stubs | No H3/UDF generation | MEDIUM |
| Dependency creation | Missing tables not created | MEDIUM |
| Validation notebook generation | No automated testing | MEDIUM |

---

## Enhancement Requirements

### REQ-F1: Stored Procedure Converter Class

**Purpose:** Main orchestrator for converting T-SQL stored procedures to Databricks notebooks.

**Specification:**

```python
# Add new file: sp_converter.py

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import re

@dataclass
class ConversionResult:
    """Result of stored procedure conversion."""
    procedure_name: str
    source_path: str
    target_path: str
    notebook_content: str
    patterns_converted: List[str]
    patterns_manual: List[str]  # Patterns requiring manual review
    warnings: List[str]
    success: bool

class SPConverter:
    """
    Convert T-SQL stored procedures to Databricks notebooks.

    Usage:
        converter = SPConverter()
        result = converter.convert(
            source_sql=sql_content,
            procedure_name="stg.spCalculateFactWorkersShifts",
            target_catalog="wakecap_prod",
            target_schema="gold"
        )

        if result.success:
            Path("notebooks/").mkdir(exist_ok=True)
            Path(result.target_path).write_text(result.notebook_content)
    """

    def __init__(self):
        self._pattern_handlers = {
            "CURSOR": self._convert_cursor,
            "TEMP_TABLE": self._convert_temp_table,
            "MERGE": self._convert_merge,
            "DYNAMIC_SQL": self._convert_dynamic_sql,
            "SPATIAL": self._convert_spatial,
            "WHILE_LOOP": self._convert_while_loop,
            "TRANSACTION": self._convert_transaction,
        }

    def convert(
        self,
        source_sql: str,
        procedure_name: str,
        target_catalog: str = "wakecap_prod",
        target_schema: str = "gold",
        source_tables_mapping: Optional[Dict[str, str]] = None,
    ) -> ConversionResult:
        """
        Convert a stored procedure to a Databricks notebook.

        Args:
            source_sql: T-SQL stored procedure content
            procedure_name: Full procedure name (e.g., "stg.spCalculateFactWorkersShifts")
            target_catalog: Databricks catalog name
            target_schema: Target schema for output tables
            source_tables_mapping: Optional mapping of source tables to Databricks tables

        Returns:
            ConversionResult with notebook content and conversion metadata
        """

    def _detect_patterns(self, source_sql: str) -> Dict[str, List[Dict]]:
        """Detect all T-SQL patterns in source."""

    def _generate_header(
        self,
        procedure_name: str,
        source_sql: str,
        patterns: List[str]
    ) -> str:
        """Generate notebook header with metadata."""

    def _generate_config_section(
        self,
        procedure_name: str,
        target_catalog: str,
        target_schema: str
    ) -> str:
        """Generate configuration and widget parameters section."""

    def _convert_cursor(self, cursor_sql: str, context: Dict) -> str:
        """Convert CURSOR pattern to Window functions."""

    def _convert_temp_table(self, temp_sql: str, context: Dict) -> str:
        """Convert temp table to createOrReplaceTempView."""

    def _convert_merge(self, merge_sql: str, context: Dict) -> str:
        """Convert MERGE statement to Delta MERGE."""

    def _convert_dynamic_sql(self, dynamic_sql: str, context: Dict) -> str:
        """Convert dynamic SQL to parameterized query."""

    def _convert_spatial(self, spatial_sql: str, context: Dict) -> str:
        """Generate spatial function stub with H3/UDF."""

    def _convert_while_loop(self, while_sql: str, context: Dict) -> str:
        """Convert WHILE loop to DataFrame operations."""

    def _convert_transaction(self, tran_sql: str, context: Dict) -> str:
        """Convert transaction to Delta ACID comment."""
```

---

### REQ-F2: CURSOR to Window Function Converter

**Purpose:** Convert T-SQL CURSOR patterns to equivalent Spark Window functions.

**Specification:**

```python
# Add to sp_converter.py

class CursorConverter:
    """
    Convert T-SQL CURSOR patterns to Spark Window functions.

    Handles common cursor patterns:
    1. Simple iteration with FETCH NEXT
    2. Running totals and cumulative calculations
    3. Previous/next row comparisons
    4. Gap detection (session identification)
    5. Partition-based processing
    """

    # Common cursor patterns and their Window equivalents
    CURSOR_PATTERNS = {
        "running_total": {
            "detect": r"SET\s+@\w+\s*=\s*@\w+\s*\+",
            "template": """
# Running total conversion
window_spec = Window.partitionBy({partition_cols}).orderBy({order_cols})
result_df = source_df.withColumn(
    "{result_col}",
    F.sum("{source_col}").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
)
"""
        },
        "previous_row": {
            "detect": r"LAG|@prev\w+",
            "template": """
# Previous row comparison
window_spec = Window.partitionBy({partition_cols}).orderBy({order_cols})
result_df = source_df.withColumn(
    "prev_{col}",
    F.lag("{col}", 1).over(window_spec)
)
"""
        },
        "next_row": {
            "detect": r"LEAD|@next\w+",
            "template": """
# Next row comparison
window_spec = Window.partitionBy({partition_cols}).orderBy({order_cols})
result_df = source_df.withColumn(
    "next_{col}",
    F.lead("{col}", 1).over(window_spec)
)
"""
        },
        "row_number": {
            "detect": r"@row\w*\s*=\s*@row\w*\s*\+\s*1|ROW_NUMBER",
            "template": """
# Row numbering
window_spec = Window.partitionBy({partition_cols}).orderBy({order_cols})
result_df = source_df.withColumn(
    "row_num",
    F.row_number().over(window_spec)
)
"""
        },
        "gap_detection": {
            "detect": r"DATEDIFF|date.*diff|gap|island",
            "template": """
# Gap/Island detection (session identification)
window_spec = Window.partitionBy({partition_cols}).orderBy({order_cols})

# Calculate time difference from previous row
df_with_gap = source_df.withColumn(
    "prev_timestamp",
    F.lag("{timestamp_col}").over(window_spec)
).withColumn(
    "gap_seconds",
    F.unix_timestamp("{timestamp_col}") - F.unix_timestamp("prev_timestamp")
)

# Identify new sessions where gap exceeds threshold
df_with_sessions = df_with_gap.withColumn(
    "new_session",
    F.when(F.col("gap_seconds") > {gap_threshold}, 1).otherwise(0)
).withColumn(
    "session_id",
    F.sum("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow))
)
"""
        },
    }

    def convert(
        self,
        cursor_sql: str,
        context: Dict
    ) -> Tuple[str, List[str]]:
        """
        Convert CURSOR SQL to Window function code.

        Args:
            cursor_sql: T-SQL containing CURSOR definition and loop
            context: Dict with partition_cols, order_cols, etc.

        Returns:
            Tuple of (converted_code, warnings)
        """

    def extract_cursor_definition(self, sql: str) -> Dict:
        """
        Extract cursor definition details.

        Returns:
            {
                "cursor_name": str,
                "select_sql": str,
                "partition_cols": List[str],
                "order_cols": List[str],
                "fetch_variables": List[str],
            }
        """

    def extract_loop_body(self, sql: str, cursor_name: str) -> str:
        """Extract the WHILE @@FETCH_STATUS = 0 loop body."""

    def detect_cursor_pattern(self, loop_body: str) -> str:
        """Detect which cursor pattern is being used."""

    def generate_window_code(
        self,
        pattern: str,
        partition_cols: List[str],
        order_cols: List[str],
        **kwargs
    ) -> str:
        """Generate Window function code for detected pattern."""
```

**Example Conversion:**

```sql
-- Source T-SQL CURSOR
DECLARE @WorkerID INT, @PrevTimestamp DATETIME, @SessionID INT = 0
DECLARE worker_cursor CURSOR FOR
    SELECT WorkerID, TimestampUTC
    FROM FactWorkersHistory
    ORDER BY WorkerID, TimestampUTC

OPEN worker_cursor
FETCH NEXT FROM worker_cursor INTO @WorkerID, @Timestamp

WHILE @@FETCH_STATUS = 0
BEGIN
    IF DATEDIFF(MINUTE, @PrevTimestamp, @Timestamp) > 300
        SET @SessionID = @SessionID + 1

    UPDATE FactWorkersHistory
    SET SessionID = @SessionID
    WHERE CURRENT OF worker_cursor

    SET @PrevTimestamp = @Timestamp
    FETCH NEXT FROM worker_cursor INTO @WorkerID, @Timestamp
END
```

```python
# Converted Spark Window Function
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
worker_window = Window.partitionBy("WorkerID").orderBy("TimestampUTC")

# Gap detection for session identification
result_df = (
    spark.table("wakecap_prod.silver.silver_fact_workers_history")
    .withColumn("prev_timestamp", F.lag("TimestampUTC").over(worker_window))
    .withColumn(
        "gap_minutes",
        (F.unix_timestamp("TimestampUTC") - F.unix_timestamp("prev_timestamp")) / 60
    )
    .withColumn(
        "new_session",
        F.when(F.col("gap_minutes") > 300, 1).otherwise(0)
    )
    .withColumn(
        "SessionID",
        F.sum("new_session").over(
            worker_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
)
```

---

### REQ-F3: MERGE Statement Generator

**Purpose:** Convert T-SQL MERGE statements to Delta Lake MERGE operations.

**Specification:**

```python
# Add to sp_converter.py

class MergeConverter:
    """
    Convert T-SQL MERGE statements to Delta Lake MERGE.

    Handles:
    1. WHEN MATCHED THEN UPDATE
    2. WHEN NOT MATCHED THEN INSERT
    3. WHEN NOT MATCHED BY SOURCE THEN DELETE
    4. Multiple WHEN clauses with conditions
    """

    def convert(
        self,
        merge_sql: str,
        target_table: str,
        context: Dict
    ) -> str:
        """
        Convert T-SQL MERGE to Delta MERGE.

        Args:
            merge_sql: T-SQL MERGE statement
            target_table: Databricks target table name
            context: Additional context (catalog, schema)

        Returns:
            Python code for Delta MERGE operation
        """

    def extract_merge_components(self, merge_sql: str) -> Dict:
        """
        Extract MERGE statement components.

        Returns:
            {
                "target_table": str,
                "target_alias": str,
                "source_table": str,
                "source_alias": str,
                "join_condition": str,
                "when_matched": [{"condition": str, "action": str, "set_clause": str}],
                "when_not_matched": [{"condition": str, "values": Dict}],
                "when_not_matched_by_source": [{"condition": str, "action": str}],
            }
        """

    def generate_delta_merge(
        self,
        components: Dict,
        target_table: str
    ) -> str:
        """Generate Delta MERGE Python code."""

    MERGE_TEMPLATE = '''
# Delta MERGE operation
# Converted from T-SQL MERGE statement
from delta.tables import DeltaTable

# Get target Delta table
target_table = DeltaTable.forName(spark, "{target_table}")

# Define merge condition
merge_condition = """{merge_condition}"""

# Execute MERGE
(
    target_table.alias("{target_alias}")
    .merge(
        source_df.alias("{source_alias}"),
        merge_condition
    )
    {when_matched_clause}
    {when_not_matched_clause}
    {when_not_matched_by_source_clause}
    .execute()
)

print(f"MERGE completed: {{source_df.count()}} source rows processed")
'''

    WHEN_MATCHED_TEMPLATE = '''
    .whenMatchedUpdate(
        condition={condition},
        set={{
            {set_assignments}
        }}
    )'''

    WHEN_NOT_MATCHED_TEMPLATE = '''
    .whenNotMatchedInsert(
        condition={condition},
        values={{
            {insert_values}
        }}
    )'''

    WHEN_NOT_MATCHED_BY_SOURCE_TEMPLATE = '''
    .whenNotMatchedBySourceDelete(
        condition={condition}
    )'''
```

**Example Conversion:**

```sql
-- Source T-SQL MERGE
MERGE INTO dbo.FactWorkersShifts AS target
USING #CalculatedShifts AS source
ON target.ProjectID = source.ProjectID
   AND target.WorkerID = source.WorkerID
   AND target.ShiftLocalDate = source.ShiftLocalDate

WHEN MATCHED AND source.UpdatedAt > target.UpdatedAt THEN
    UPDATE SET
        target.ActiveTime = source.ActiveTime,
        target.InactiveTime = source.InactiveTime,
        target.UpdatedAt = source.UpdatedAt

WHEN NOT MATCHED THEN
    INSERT (ProjectID, WorkerID, ShiftLocalDate, ActiveTime, InactiveTime, CreatedAt)
    VALUES (source.ProjectID, source.WorkerID, source.ShiftLocalDate,
            source.ActiveTime, source.InactiveTime, GETUTCDATE());
```

```python
# Converted Delta MERGE
from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "wakecap_prod.gold.gold_fact_workers_shifts")

merge_condition = """
    target.ProjectID = source.ProjectID
    AND target.WorkerID = source.WorkerID
    AND target.ShiftLocalDate = source.ShiftLocalDate
"""

(
    target_table.alias("target")
    .merge(
        calculated_shifts_df.alias("source"),
        merge_condition
    )
    .whenMatchedUpdate(
        condition="source.UpdatedAt > target.UpdatedAt",
        set={
            "ActiveTime": "source.ActiveTime",
            "InactiveTime": "source.InactiveTime",
            "UpdatedAt": "source.UpdatedAt",
        }
    )
    .whenNotMatchedInsert(
        values={
            "ProjectID": "source.ProjectID",
            "WorkerID": "source.WorkerID",
            "ShiftLocalDate": "source.ShiftLocalDate",
            "ActiveTime": "source.ActiveTime",
            "InactiveTime": "source.InactiveTime",
            "CreatedAt": "current_timestamp()",
        }
    )
    .execute()
)
```

---

### REQ-F4: Temp Table Converter

**Purpose:** Convert T-SQL temporary tables to Spark temp views or cached DataFrames.

**Specification:**

```python
# Add to sp_converter.py

class TempTableConverter:
    """
    Convert T-SQL temporary tables to Spark equivalents.

    Mappings:
    - #TempTable → createOrReplaceTempView() for single-use
    - #TempTable (reused) → cache() + createOrReplaceTempView()
    - ##GlobalTemp → Delta table with session prefix
    """

    def convert(
        self,
        temp_sql: str,
        temp_name: str,
        usage_count: int
    ) -> str:
        """
        Convert temp table to Spark equivalent.

        Args:
            temp_sql: SQL creating/using temp table
            temp_name: Temp table name (e.g., "#Results")
            usage_count: Number of times temp table is referenced

        Returns:
            Python code for temp view creation
        """

    def detect_temp_tables(self, sql: str) -> List[Dict]:
        """
        Find all temp table definitions and usages.

        Returns:
            [{"name": "#TableName", "create_sql": str, "usage_count": int}]
        """

    def extract_select_into(self, sql: str, temp_name: str) -> str:
        """Extract SELECT INTO #temp statement."""

    def extract_create_table(self, sql: str, temp_name: str) -> Dict:
        """Extract CREATE TABLE #temp with columns."""

    TEMP_VIEW_TEMPLATE = '''
# Temp table {temp_name} → Spark temp view
{df_name} = (
    spark.sql("""
        {select_sql}
    """)
)
{cache_line}
{df_name}.createOrReplaceTempView("{view_name}")
print(f"Created temp view {view_name}: {{{df_name}.count()}} rows")
'''

    CACHED_DF_TEMPLATE = '''
# Temp table {temp_name} → Cached DataFrame (used {usage_count} times)
{df_name} = (
    spark.sql("""
        {select_sql}
    """)
).cache()

# Materialize cache
{df_name}.count()
print(f"Cached {df_name}: {{{df_name}.count()}} rows")
'''
```

---

### REQ-F5: Spatial Function Stub Generator

**Purpose:** Generate placeholder code for spatial functions that require H3 or custom UDFs.

**Specification:**

```python
# Add to sp_converter.py

class SpatialConverter:
    """
    Generate stubs for T-SQL spatial functions.

    Provides templates for:
    - geography::Point() → (lat, lon) columns + H3 index
    - STDistance() → Haversine UDF
    - STContains() → H3 containment check
    - STIntersects() → H3 ring intersection
    """

    SPATIAL_MAPPINGS = {
        "geography::Point": {
            "description": "Create point from lat/lon",
            "template": '''
# geography::Point conversion
# Original: geography::Point(@lon, @lat, 4326)
# Note: SQL Server uses (lon, lat) order, Spark typically uses (lat, lon)

from pyspark.sql import functions as F

df_with_point = source_df.withColumn(
    "latitude", F.col("{lat_col}").cast("double")
).withColumn(
    "longitude", F.col("{lon_col}").cast("double")
)

# Optional: Add H3 index for efficient spatial queries
# Requires: %pip install h3-pyspark
# from h3_pyspark import geo_to_h3
# df_with_h3 = df_with_point.withColumn(
#     "h3_index",
#     geo_to_h3("latitude", "longitude", F.lit(9))  # Resolution 9 ~174m
# )
'''
        },
        "STDistance": {
            "description": "Calculate distance between points",
            "template": '''
# STDistance conversion → Haversine formula
# Original: point1.STDistance(point2)

from pyspark.sql.functions import udf, col, radians, sin, cos, sqrt, atan2
from pyspark.sql.types import DoubleType
import math

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

    a = math.sin(delta_phi/2)**2 + \\
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c

# Register UDF for SQL usage
spark.udf.register("haversine_distance", haversine_distance)

# Usage:
# df.withColumn("distance_m", haversine_distance(
#     col("lat1"), col("lon1"), col("lat2"), col("lon2")
# ))
'''
        },
        "STContains": {
            "description": "Check if polygon contains point",
            "template": '''
# STContains conversion → H3 containment or Shapely
# Original: polygon.STContains(point)

# Option 1: H3-based (fast, approximate)
# Requires h3-pyspark library
# from h3_pyspark import geo_to_h3, polyfill
#
# zone_h3_indices = polyfill(zone_polygon_wkt, resolution=9)
# df_with_zone = df.withColumn(
#     "in_zone",
#     F.col("h3_index").isin(zone_h3_indices)
# )

# Option 2: Shapely-based (precise, slower)
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from shapely import wkt
from shapely.geometry import Point

@udf(returnType=BooleanType())
def point_in_polygon(lat, lon, polygon_wkt):
    """Check if point is inside polygon."""
    if any(v is None for v in [lat, lon, polygon_wkt]):
        return None
    try:
        point = Point(lon, lat)  # Shapely uses (x, y) = (lon, lat)
        polygon = wkt.loads(polygon_wkt)
        return polygon.contains(point)
    except:
        return None

spark.udf.register("point_in_polygon", point_in_polygon)
'''
        },
    }

    def convert(self, spatial_sql: str, context: Dict) -> str:
        """
        Generate spatial function conversion code.

        Returns Python code with comments indicating manual review needed.
        """

    def detect_spatial_functions(self, sql: str) -> List[Dict]:
        """
        Detect spatial function usages.

        Returns:
            [{"function": str, "line": int, "context": str}]
        """

    def generate_h3_setup(self) -> str:
        """Generate H3 library setup code."""
        return '''
# H3 Spatial Library Setup
# Run this cell first to install H3 if not already installed
# %pip install h3 h3-pyspark

import h3
from h3_pyspark import geo_to_h3, h3_to_geo_boundary

# Verify H3 is working
test_h3 = h3.geo_to_h3(37.7749, -122.4194, 9)
print(f"H3 test index: {test_h3}")
'''
```

---

### REQ-F6: Dependency Resolver and Creator

**Purpose:** Automatically resolve and create missing dependencies before running converted procedures.

**Specification:**

```python
# Add new file: dependency_resolver.py

from dataclasses import dataclass
from typing import List, Dict, Optional
from databricks.sdk import WorkspaceClient

@dataclass
class DependencyAction:
    """Action taken to resolve a dependency."""
    dependency_type: str  # "table", "view", "function", "procedure"
    source_name: str
    target_name: str
    action: str  # "exists", "created", "skipped", "failed"
    details: Optional[str] = None

class DependencyResolver:
    """
    Resolve and create missing dependencies for converted stored procedures.

    Capabilities:
    1. Check if tables exist in Databricks
    2. Create missing schemas
    3. Register missing UDFs
    4. Verify procedure dependencies are converted
    """

    def __init__(
        self,
        host: str,
        token: str,
        catalog: str,
        default_schema: str = "silver"
    ):
        self.client = WorkspaceClient(host=host, token=token)
        self.catalog = catalog
        self.default_schema = default_schema
        self.table_mapping = self._load_table_mapping()

    def _load_table_mapping(self) -> Dict[str, str]:
        """
        Load mapping of source tables to Databricks tables.

        Returns:
            {"dbo.Worker": "wakecap_prod.silver.silver_worker", ...}
        """

    def resolve_all(
        self,
        dependencies: List[Dict],
        auto_create: bool = False
    ) -> List[DependencyAction]:
        """
        Resolve all dependencies.

        Args:
            dependencies: List from DependencyTracker.extract_dependencies()
            auto_create: If True, create missing tables/views

        Returns:
            List of actions taken for each dependency
        """

    def check_table_exists(self, table_name: str) -> bool:
        """Check if table exists in Databricks."""

    def check_function_exists(self, function_name: str) -> bool:
        """Check if UDF is registered."""

    def create_placeholder_table(
        self,
        source_table: str,
        target_table: str,
        schema: Dict[str, str]
    ) -> DependencyAction:
        """
        Create empty placeholder table for testing.

        Used when source data not yet migrated but we need table to exist.
        """

    def register_udf(
        self,
        udf_name: str,
        udf_code: str,
        return_type: str
    ) -> DependencyAction:
        """Register a Python UDF in the catalog."""

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get column names and types for a table."""

    def generate_resolution_report(
        self,
        actions: List[DependencyAction]
    ) -> str:
        """Generate markdown report of resolution actions."""

    PLACEHOLDER_TABLE_SQL = '''
-- Placeholder table for {source_table}
-- Created by dependency resolver - populate with actual data

CREATE TABLE IF NOT EXISTS {target_table} (
    {column_definitions}
)
USING DELTA
COMMENT 'Placeholder - source: {source_table}'
TBLPROPERTIES (
    'placeholder' = 'true',
    'source_table' = '{source_table}'
);
'''
```

---

### REQ-F7: Validation Notebook Generator

**Purpose:** Automatically generate validation notebooks to verify conversion correctness.

**Specification:**

```python
# Add new file: validation_generator.py

class ValidationNotebookGenerator:
    """
    Generate validation notebooks for converted stored procedures.

    Creates notebooks that:
    1. Compare row counts between source and target
    2. Compare sample records
    3. Verify aggregations match
    4. Check data type consistency
    5. Validate business rules
    """

    def generate(
        self,
        procedure_name: str,
        source_tables: List[str],
        target_table: str,
        key_columns: List[str],
        aggregation_columns: List[str],
        sample_size: int = 100
    ) -> str:
        """
        Generate validation notebook content.

        Returns:
            Complete Databricks notebook as string
        """

    VALIDATION_TEMPLATE = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Validation: {procedure_name}
# MAGIC
# MAGIC **Purpose:** Validate conversion of `{procedure_name}` to Databricks
# MAGIC
# MAGIC **Comparisons:**
# MAGIC - Row counts
# MAGIC - Sample record matching
# MAGIC - Aggregation verification
# MAGIC - Data type validation

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Source (SQL Server via JDBC or reference table)
SOURCE_TABLE = "{source_reference_table}"

# Target (Databricks Delta table)
TARGET_TABLE = "{target_table}"

# Key columns for matching
KEY_COLUMNS = {key_columns}

# Aggregation columns to verify
AGG_COLUMNS = {aggregation_columns}

# Sample size for detailed comparison
SAMPLE_SIZE = {sample_size}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Count Comparison

# COMMAND ----------

source_count = spark.table(SOURCE_TABLE).count()
target_count = spark.table(TARGET_TABLE).count()

count_match = source_count == target_count
count_diff = abs(source_count - target_count)
count_pct = (count_diff / source_count * 100) if source_count > 0 else 0

print(f"Source count: {{source_count:,}}")
print(f"Target count: {{target_count:,}}")
print(f"Difference:   {{count_diff:,}} ({{count_pct:.2f}}%)")
print(f"Status:       {{'PASS' if count_match else 'FAIL'}}")

assert count_pct < 1, f"Row count difference >1%: {{count_pct:.2f}}%"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregation Comparison

# COMMAND ----------

def compare_aggregations(source_df, target_df, agg_cols):
    """Compare SUM, COUNT, AVG for specified columns."""
    results = []

    for col_name in agg_cols:
        source_agg = source_df.agg(
            F.sum(col_name).alias("sum"),
            F.count(col_name).alias("count"),
            F.avg(col_name).alias("avg")
        ).collect()[0]

        target_agg = target_df.agg(
            F.sum(col_name).alias("sum"),
            F.count(col_name).alias("count"),
            F.avg(col_name).alias("avg")
        ).collect()[0]

        for metric in ["sum", "count", "avg"]:
            source_val = source_agg[metric] or 0
            target_val = target_agg[metric] or 0
            diff = abs(source_val - target_val)
            pct = (diff / source_val * 100) if source_val != 0 else 0

            results.append({{
                "column": col_name,
                "metric": metric,
                "source": source_val,
                "target": target_val,
                "diff": diff,
                "diff_pct": pct,
                "status": "PASS" if pct < 0.01 else "FAIL"
            }})

    return results

source_df = spark.table(SOURCE_TABLE)
target_df = spark.table(TARGET_TABLE)

agg_results = compare_aggregations(source_df, target_df, AGG_COLUMNS)

# Display results
agg_df = spark.createDataFrame(agg_results)
display(agg_df)

# Check for failures
failures = [r for r in agg_results if r["status"] == "FAIL"]
assert len(failures) == 0, f"Aggregation mismatches: {{failures}}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sample Record Comparison

# COMMAND ----------

# Get sample keys from source
sample_keys = (
    source_df
    .select(KEY_COLUMNS)
    .distinct()
    .limit(SAMPLE_SIZE)
)

# Join source and target on keys
comparison_df = (
    source_df.alias("src")
    .join(
        target_df.alias("tgt"),
        on=KEY_COLUMNS,
        how="inner"
    )
    .join(sample_keys, on=KEY_COLUMNS, how="inner")
)

# Check if all samples matched
matched_count = comparison_df.count()
print(f"Sample records matched: {{matched_count}} / {{SAMPLE_SIZE}}")
assert matched_count == SAMPLE_SIZE, f"Only {{matched_count}} of {{SAMPLE_SIZE}} samples matched"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation Summary

# COMMAND ----------

validation_results = {{
    "procedure": "{procedure_name}",
    "timestamp": datetime.now().isoformat(),
    "row_count_match": count_match,
    "row_count_diff_pct": count_pct,
    "aggregation_failures": len(failures),
    "sample_match_rate": matched_count / SAMPLE_SIZE,
    "overall_status": "PASS" if (count_match and len(failures) == 0) else "FAIL"
}}

print("=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)
for key, value in validation_results.items():
    print(f"{{key}}: {{value}}")
print("=" * 60)

# Exit with status
dbutils.notebook.exit(validation_results["overall_status"])
'''
```

---

### REQ-F8: Complete Notebook Generator

**Purpose:** Generate complete, ready-to-run Databricks notebooks from stored procedures.

**Specification:**

```python
# Add to sp_converter.py

class NotebookGenerator:
    """
    Generate complete Databricks notebooks from converted components.

    Combines:
    - Header with metadata
    - Configuration section
    - Helper functions
    - Main transformation logic
    - Write/MERGE operations
    - Summary and logging
    """

    def generate(
        self,
        procedure_name: str,
        source_sql: str,
        converted_sections: Dict[str, str],
        target_catalog: str,
        target_schema: str,
        target_table: str
    ) -> str:
        """
        Generate complete notebook.

        Args:
            procedure_name: Original procedure name
            source_sql: Original T-SQL for reference
            converted_sections: Dict of section_name -> converted code
            target_catalog: Databricks catalog
            target_schema: Target schema
            target_table: Target table name

        Returns:
            Complete notebook content as string
        """

    NOTEBOOK_STRUCTURE = [
        ("header", "Header and metadata"),
        ("config", "Configuration and widgets"),
        ("helpers", "Helper functions"),
        ("source_load", "Load source data"),
        ("transformations", "Main transformations"),
        ("write", "Write results"),
        ("summary", "Execution summary"),
    ]

    HEADER_TEMPLATE = '''# Databricks notebook source
# MAGIC %md
# MAGIC # {title}
# MAGIC
# MAGIC **Converted from:** `{procedure_name}` ({line_count} lines)
# MAGIC
# MAGIC **Purpose:** {purpose}
# MAGIC
# MAGIC **Original Patterns:** {patterns}
# MAGIC
# MAGIC **Conversion Approach:**
# MAGIC {conversion_notes}
# MAGIC
# MAGIC **Key Business Logic:**
# MAGIC {business_logic}
'''

    CONFIG_TEMPLATE = '''# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and Schema
TARGET_CATALOG = "{catalog}"
TARGET_SCHEMA = "{schema}"
MIGRATION_SCHEMA = "migration"

# Target table
TARGET_TABLE = f"{{TARGET_CATALOG}}.{{TARGET_SCHEMA}}.{target_table}"
WATERMARK_TABLE = f"{{TARGET_CATALOG}}.{{MIGRATION_SCHEMA}}._gold_watermarks"

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "7", "Lookback Days")
dbutils.widgets.text("project_id", "", "Project ID (optional)")

load_mode = dbutils.widgets.get("load_mode")
lookback_days = int(dbutils.widgets.get("lookback_days"))
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {{load_mode}}")
print(f"Lookback Days: {{lookback_days}}")
'''

    SUMMARY_TEMPLATE = '''# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Summary

# COMMAND ----------

# Final summary
end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

summary = {{
    "procedure": "{procedure_name}",
    "load_mode": load_mode,
    "start_time": start_time.isoformat(),
    "end_time": end_time.isoformat(),
    "duration_seconds": duration,
    "rows_processed": rows_processed,
    "status": "SUCCESS"
}}

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
for key, value in summary.items():
    print(f"{{key}}: {{value}}")
print("=" * 60)

# Update watermark
update_watermark(TARGET_TABLE, end_time, rows_processed)

# Exit with success
dbutils.notebook.exit("SUCCESS")
'''
```

---

## File Changes Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `fixer.py` | Modify | Add SP-related imports and integration |
| `sp_converter.py` | New | Main SP conversion orchestrator |
| `cursor_converter.py` | New | CURSOR to Window function conversion |
| `merge_converter.py` | New | MERGE statement generation |
| `temp_converter.py` | New | Temp table conversion |
| `spatial_converter.py` | New | Spatial function stubs |
| `dependency_resolver.py` | New | Dependency resolution and creation |
| `validation_generator.py` | New | Validation notebook generation |
| `SKILL.md` | Modify | Add SP conversion workflow |

---

## Acceptance Criteria

1. **SP Conversion:** `SPConverter.convert()` produces runnable notebook from any T-SQL procedure
2. **CURSOR Conversion:** `CursorConverter.convert()` detects pattern and generates Window function code
3. **MERGE Generation:** `MergeConverter.convert()` produces valid Delta MERGE from T-SQL MERGE
4. **Temp Tables:** `TempTableConverter.convert()` handles #temp and ##temp patterns
5. **Spatial Stubs:** `SpatialConverter.convert()` generates H3/UDF placeholder code
6. **Dependencies:** `DependencyResolver.resolve_all()` identifies and optionally creates missing tables
7. **Validation:** `ValidationNotebookGenerator.generate()` creates executable validation notebook

---

## Testing Plan

| Test Case | Input | Expected Output |
|-----------|-------|-----------------|
| Convert CURSOR | `stg.spCalculateFactWorkersShifts.sql` | Notebook with Window functions |
| Convert MERGE | `stg.spDeltaSyncFactWorkersHistory.sql` | Delta MERGE code |
| Convert Spatial | Procedure with `geography::Point` | H3/Haversine stubs |
| Resolve Dependencies | Any converted notebook | All tables resolved or flagged |
| Generate Validation | Converted notebook | Runnable validation notebook |
| End-to-End | Any source procedure | Complete, deployable notebook |

---

## Integration with Review Skill

The fix skill enhancements integrate with review skill enhancements:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         MIGRATION WORKFLOW                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Source SQL ──► REVIEW SKILL ──► Issues Report                      │
│       │              │                  │                            │
│       │              │                  │                            │
│       │              ▼                  ▼                            │
│       │         Pattern Detection   Dependency List                  │
│       │         Complexity Score    Missing Tables                   │
│       │         Checklist           Missing UDFs                     │
│       │                                                              │
│       │                                                              │
│       └──────────────────► FIX SKILL                                │
│                                │                                     │
│                                ▼                                     │
│                          ┌─────────────┐                            │
│                          │ SP Converter │                            │
│                          └─────────────┘                            │
│                                │                                     │
│                    ┌───────────┼───────────┐                        │
│                    ▼           ▼           ▼                        │
│              ┌─────────┐ ┌─────────┐ ┌─────────┐                   │
│              │ CURSOR  │ │  MERGE  │ │ SPATIAL │                   │
│              │Converter│ │Converter│ │Converter│                   │
│              └─────────┘ └─────────┘ └─────────┘                   │
│                    │           │           │                        │
│                    └───────────┼───────────┘                        │
│                                ▼                                     │
│                          ┌─────────────┐                            │
│                          │  Notebook   │                            │
│                          │  Generator  │                            │
│                          └─────────────┘                            │
│                                │                                     │
│                                ▼                                     │
│                    ┌───────────────────────┐                        │
│                    │ Dependency Resolver   │                        │
│                    │ (Create missing deps) │                        │
│                    └───────────────────────┘                        │
│                                │                                     │
│                                ▼                                     │
│                    ┌───────────────────────┐                        │
│                    │ Validation Generator  │                        │
│                    │ (Create test notebook)│                        │
│                    └───────────────────────┘                        │
│                                │                                     │
│                                ▼                                     │
│                         DEPLOY & TEST                               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

*Document maintained by: Migration Team*
*Last updated: 2026-01-25*
