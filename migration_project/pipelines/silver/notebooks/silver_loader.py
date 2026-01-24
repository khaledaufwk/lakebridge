# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Loader - WakeCapDW
# MAGIC
# MAGIC Self-contained notebook for transforming Bronze layer tables into Silver layer.
# MAGIC All code is inline - no external module imports from /Workspace.
# MAGIC
# MAGIC **Features:**
# MAGIC - 78 Silver tables from TimescaleDB Bronze layer
# MAGIC - 3-tier data quality validation (critical/business/advisory)
# MAGIC - Watermark-based incremental loading from Bronze `_loaded_at`
# MAGIC - Processing groups for dependency ordering
# MAGIC - MERGE INTO for upsert operations
# MAGIC
# MAGIC **Databricks Expert Guidelines Applied:**
# MAGIC - Explicit StructType schema for createDataFrame (CANNOT_DETERMINE_TYPE fix)
# MAGIC - Timestamp-based run IDs (currentRunId() not accessible in Python)
# MAGIC - PyYAML pre-installed - no pip install needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Schema Definitions

# COMMAND ----------

from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import yaml

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce, trim, upper, lower,
    regexp_replace, to_date, to_timestamp, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
SOURCE_CATALOG = "wakecap_prod"
SOURCE_SCHEMA = "raw"
TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "silver"
WATERMARK_TABLE = f"{TARGET_CATALOG}.migration._silver_watermarks"
REGISTRY_PATH = "/Workspace/migration_project/pipelines/silver/config/silver_tables.yml"

# Ensure target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Source: {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters

# COMMAND ----------

# Create widgets for parameterized runs
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("table_filter", "ALL", [
    "ALL",
    "independent_dimensions",
    "organization",
    "project_dependent",
    "project_children",
    "zone_dependent",
    "assignments",
    "facts",
    "history"
], "Processing Group")
dbutils.widgets.text("specific_tables", "", "Specific Tables (comma-separated)")
dbutils.widgets.text("max_tables", "0", "Max Tables (0=all)")

# Get widget values
load_mode = dbutils.widgets.get("load_mode")
table_filter = dbutils.widgets.get("table_filter")
specific_tables_str = dbutils.widgets.get("specific_tables").strip()
max_tables = int(dbutils.widgets.get("max_tables"))

force_full_load = (load_mode == "full")
group_filter = None if table_filter == "ALL" else table_filter
specific_tables = [t.strip() for t in specific_tables_str.split(",") if t.strip()] if specific_tables_str else []

print(f"Load Mode: {load_mode}")
print(f"Processing Group Filter: {table_filter}")
print(f"Specific Tables: {specific_tables if specific_tables else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run ID (timestamp-based, currentRunId() not accessible in Python)

# COMMAND ----------

# Generate run ID from timestamp (currentRunId() is not accessible in Python notebooks)
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_START_TIME = datetime.now()
print(f"Run ID: {RUN_ID}")
print(f"Start Time: {RUN_START_TIME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Structures

# COMMAND ----------

class LoadStatus(Enum):
    """Load operation status"""
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TableConfig:
    """Configuration for a single Silver table"""
    name: str
    source_bronze_table: str
    primary_key_columns: List[str]
    processing_group: str
    transformation_type: str
    columns: List[Dict[str, Any]]
    expectations: Dict[str, List[str]]
    source_filter: Optional[str] = None
    fk_validations: Optional[List[Dict[str, Any]]] = None
    has_geometry: bool = False
    geometry_columns: Optional[List[str]] = None
    batch_size: int = 100000
    comment: Optional[str] = None


@dataclass
class LoadResult:
    """Result of a table load operation"""
    table_name: str
    source_bronze_table: str
    processing_group: str
    status: LoadStatus
    rows_input: int = 0
    rows_output: int = 0
    rows_dropped_critical: int = 0
    rows_flagged_business: int = 0
    rows_warned_advisory: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    previous_watermark: Optional[datetime] = None
    new_watermark: Optional[datetime] = None


# CRITICAL: Explicit schema for createDataFrame to avoid CANNOT_DETERMINE_TYPE error
SUMMARY_SCHEMA = StructType([
    StructField("table", StringType(), True),
    StructField("source", StringType(), True),
    StructField("group", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rows_input", LongType(), True),
    StructField("rows_output", LongType(), True),
    StructField("rows_dropped", LongType(), True),
    StructField("rows_flagged", LongType(), True),
    StructField("rows_warned", LongType(), True),
    StructField("duration_sec", DoubleType(), True),
    StructField("error", StringType(), True)  # May be all NULL - needs explicit type
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark Manager

# COMMAND ----------

class WatermarkManager:
    """Manages watermarks for incremental loading"""

    def __init__(self, spark: SparkSession, watermark_table: str):
        self.spark = spark
        self.watermark_table = watermark_table
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create watermark table if it doesn't exist"""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.watermark_table} (
                table_name STRING NOT NULL,
                source_bronze_table STRING,
                processing_group STRING,
                last_bronze_watermark TIMESTAMP,
                last_load_status STRING,
                last_load_row_count BIGINT,
                rows_input BIGINT,
                rows_dropped_critical BIGINT,
                rows_flagged_business BIGINT,
                rows_warned_advisory BIGINT,
                last_error_message STRING,
                last_load_start_time TIMESTAMP,
                last_load_end_time TIMESTAMP,
                last_load_duration_seconds DOUBLE,
                pipeline_run_id STRING,
                updated_at TIMESTAMP
            )
            USING DELTA
        """)

    def get_last_watermark(self, table_name: str) -> Optional[datetime]:
        """Get the last successful watermark for a table"""
        try:
            result = self.spark.sql(f"""
                SELECT last_bronze_watermark
                FROM {self.watermark_table}
                WHERE table_name = '{table_name}'
                  AND last_load_status = 'success'
            """).collect()
            return result[0].last_bronze_watermark if result else None
        except Exception:
            return None

    def update_watermark(
        self,
        table_name: str,
        source_bronze_table: str,
        processing_group: str,
        bronze_watermark: Optional[datetime],
        status: str,
        rows_input: int,
        rows_output: int,
        rows_dropped: int,
        rows_flagged: int,
        rows_warned: int,
        start_time: datetime,
        end_time: datetime,
        run_id: str,
        error_message: Optional[str] = None
    ):
        """Update watermark using MERGE"""
        duration = (end_time - start_time).total_seconds()

        # Use MERGE to upsert watermark record
        self.spark.sql(f"""
            MERGE INTO {self.watermark_table} AS target
            USING (
                SELECT
                    '{table_name}' AS table_name,
                    '{source_bronze_table}' AS source_bronze_table,
                    '{processing_group}' AS processing_group,
                    {'TIMESTAMP' + repr(str(bronze_watermark)) if bronze_watermark else 'NULL'} AS last_bronze_watermark,
                    '{status}' AS last_load_status,
                    {rows_output} AS last_load_row_count,
                    {rows_input} AS rows_input,
                    {rows_dropped} AS rows_dropped_critical,
                    {rows_flagged} AS rows_flagged_business,
                    {rows_warned} AS rows_warned_advisory,
                    {repr(error_message[:500]) if error_message else 'NULL'} AS last_error_message,
                    TIMESTAMP'{start_time}' AS last_load_start_time,
                    TIMESTAMP'{end_time}' AS last_load_end_time,
                    {duration} AS last_load_duration_seconds,
                    '{run_id}' AS pipeline_run_id,
                    current_timestamp() AS updated_at
            ) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validator

# COMMAND ----------

class DataQualityValidator:
    """
    Applies 3-tier data quality validation:
    - critical: Filter out failing rows (expect_or_drop equivalent)
    - business: Log violations but keep rows (expect equivalent)
    - advisory: Warn only (expect_or_warn equivalent)
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate(
        self,
        df: DataFrame,
        expectations: Dict[str, List[str]],
        table_name: str
    ) -> Tuple[DataFrame, Dict[str, int]]:
        """
        Apply data quality validation and return filtered DataFrame with metrics.

        Returns:
            Tuple of (filtered_df, metrics_dict)
        """
        metrics = {
            'input_rows': df.count(),
            'dropped_critical': 0,
            'flagged_business': 0,
            'warned_advisory': 0
        }

        # Critical expectations - filter out failing rows
        for constraint in expectations.get('critical', []):
            try:
                failing_count = df.filter(f"NOT ({constraint})").count()
                metrics['dropped_critical'] += failing_count
                if failing_count > 0:
                    print(f"  [CRITICAL] {table_name}: {failing_count} rows dropped for: {constraint}")
                df = df.filter(constraint)
            except Exception as e:
                print(f"  [WARNING] Failed to apply critical constraint '{constraint}': {e}")

        # Business expectations - log violations but keep rows
        for constraint in expectations.get('business', []):
            try:
                failing_count = df.filter(f"NOT ({constraint})").count()
                metrics['flagged_business'] += failing_count
                if failing_count > 0:
                    print(f"  [BUSINESS] {table_name}: {failing_count} rows violate: {constraint}")
            except Exception as e:
                print(f"  [WARNING] Failed to check business constraint '{constraint}': {e}")

        # Advisory expectations - warn only
        for constraint in expectations.get('advisory', []):
            try:
                failing_count = df.filter(f"NOT ({constraint})").count()
                metrics['warned_advisory'] += failing_count
                if failing_count > 0:
                    print(f"  [ADVISORY] {table_name}: {failing_count} rows warn: {constraint}")
            except Exception as e:
                print(f"  [WARNING] Failed to check advisory constraint '{constraint}': {e}")

        return df, metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Transformer

# COMMAND ----------

class SilverTableTransformer:
    """Transforms Bronze tables into Silver tables"""

    def __init__(
        self,
        spark: SparkSession,
        source_catalog: str,
        source_schema: str,
        target_catalog: str,
        target_schema: str
    ):
        self.spark = spark
        self.source_catalog = source_catalog
        self.source_schema = source_schema
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.validator = DataQualityValidator(spark)

    def get_bronze_table_path(self, table_name: str) -> str:
        """Get fully qualified Bronze table path"""
        return f"{self.source_catalog}.{self.source_schema}.{table_name}"

    def get_silver_table_path(self, table_name: str) -> str:
        """Get fully qualified Silver table path"""
        return f"{self.target_catalog}.{self.target_schema}.{table_name}"

    def read_bronze_incremental(
        self,
        bronze_table: str,
        last_watermark: Optional[datetime],
        source_filter: Optional[str] = None
    ) -> Tuple[DataFrame, Optional[datetime]]:
        """
        Read Bronze table incrementally based on _loaded_at watermark.

        Returns:
            Tuple of (DataFrame, max_watermark)
        """
        bronze_path = self.get_bronze_table_path(bronze_table)

        # Read base data
        df = self.spark.table(bronze_path)

        # Apply source filter if specified (e.g., Type = 'organization')
        if source_filter:
            df = df.filter(source_filter)

        # Apply incremental filter based on watermark
        if last_watermark:
            df = df.filter(col("_loaded_at") > lit(last_watermark))

        # Get max watermark from this batch
        max_wm_row = df.agg(spark_max("_loaded_at").alias("max_wm")).collect()
        new_watermark = max_wm_row[0].max_wm if max_wm_row and max_wm_row[0].max_wm else None

        return df, new_watermark

    def apply_column_transformations(
        self,
        df: DataFrame,
        columns: List[Dict[str, Any]]
    ) -> DataFrame:
        """Apply column mappings and transformations"""
        select_exprs = []

        for col_def in columns:
            source_col = col_def['source']
            target_col = col_def.get('target', source_col)
            transform = col_def.get('transform')

            # Build column expression
            if transform == 'TRIM':
                expr = trim(col(source_col)).alias(target_col)
            elif transform == 'UPPER':
                expr = upper(trim(col(source_col))).alias(target_col)
            elif transform == 'LOWER':
                expr = lower(trim(col(source_col))).alias(target_col)
            else:
                expr = col(source_col).alias(target_col)

            select_exprs.append(expr)

        # Add standard Silver metadata columns
        select_exprs.append(current_timestamp().alias("_silver_processed_at"))
        select_exprs.append(col("_loaded_at").alias("_bronze_loaded_at"))

        return df.select(*select_exprs)

    def apply_soft_delete_filter(self, df: DataFrame) -> DataFrame:
        """Filter out soft-deleted records (DeletedAt IS NULL)"""
        if "DeletedAt" in df.columns:
            return df.filter(col("DeletedAt").isNull())
        return df

    def merge_into_silver(
        self,
        source_df: DataFrame,
        target_table: str,
        primary_key_columns: List[str]
    ) -> int:
        """
        Merge source data into Silver table using MERGE INTO.

        Returns:
            Number of rows in source DataFrame
        """
        target_path = self.get_silver_table_path(target_table)
        row_count = source_df.count()

        if row_count == 0:
            print(f"  No rows to merge for {target_table}")
            return 0

        # Check if target table exists
        table_exists = self.spark.catalog.tableExists(target_path)

        if not table_exists:
            # Create table with first batch
            print(f"  Creating new table: {target_path}")
            source_df.write.format("delta").mode("overwrite").saveAsTable(target_path)
        else:
            # Build merge condition from primary key columns
            merge_condition = " AND ".join([
                f"target.{pk} = source.{pk}" for pk in primary_key_columns
            ])

            # Create temp view for merge
            temp_view = f"silver_merge_temp_{target_table.replace('silver_', '')}"
            source_df.createOrReplaceTempView(temp_view)

            # Get column list for update/insert
            columns = source_df.columns
            update_set = ", ".join([f"target.{c} = source.{c}" for c in columns])
            insert_cols = ", ".join(columns)
            insert_vals = ", ".join([f"source.{c}" for c in columns])

            # Execute MERGE
            self.spark.sql(f"""
                MERGE INTO {target_path} AS target
                USING {temp_view} AS source
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)

            # Clean up temp view
            self.spark.catalog.dropTempView(temp_view)

        return row_count

    def transform_table(
        self,
        config: TableConfig,
        last_watermark: Optional[datetime],
        force_full: bool = False
    ) -> LoadResult:
        """
        Transform a single Bronze table into Silver.

        Returns:
            LoadResult with metrics
        """
        start_time = datetime.now()
        result = LoadResult(
            table_name=config.name,
            source_bronze_table=config.source_bronze_table,
            processing_group=config.processing_group,
            status=LoadStatus.FAILED,
            previous_watermark=last_watermark
        )

        try:
            print(f"\n{'='*60}")
            print(f"Processing: {config.name}")
            print(f"  Source: {config.source_bronze_table}")
            print(f"  Group: {config.processing_group}")
            print(f"  Type: {config.transformation_type}")

            # Use None watermark for full load
            effective_watermark = None if force_full else last_watermark
            if effective_watermark:
                print(f"  Watermark: {effective_watermark}")
            else:
                print(f"  Mode: Full load")

            # Read Bronze data incrementally
            df, new_watermark = self.read_bronze_incremental(
                config.source_bronze_table,
                effective_watermark,
                config.source_filter
            )
            result.new_watermark = new_watermark

            # Check if there's any data to process
            row_count = df.count()
            result.rows_input = row_count

            if row_count == 0:
                print(f"  No new data to process")
                result.status = LoadStatus.SKIPPED
                result.duration_seconds = (datetime.now() - start_time).total_seconds()
                return result

            print(f"  Input rows: {row_count:,}")

            # Apply soft delete filter if applicable
            if config.transformation_type in ['soft_delete_filter', 'filter_transform']:
                df = self.apply_soft_delete_filter(df)
                after_soft_delete = df.count()
                if after_soft_delete < row_count:
                    print(f"  After soft-delete filter: {after_soft_delete:,}")

            # Apply data quality validation
            df, dq_metrics = self.validator.validate(df, config.expectations, config.name)
            result.rows_dropped_critical = dq_metrics['dropped_critical']
            result.rows_flagged_business = dq_metrics['flagged_business']
            result.rows_warned_advisory = dq_metrics['warned_advisory']

            # Apply column transformations
            df = self.apply_column_transformations(df, config.columns)

            # Merge into Silver table
            result.rows_output = self.merge_into_silver(
                df,
                config.name,
                config.primary_key_columns
            )

            print(f"  Output rows: {result.rows_output:,}")
            result.status = LoadStatus.SUCCESS

        except Exception as e:
            result.error_message = str(e)
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        print(f"  Duration: {result.duration_seconds:.2f}s")
        return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registry Loader

# COMMAND ----------

def load_registry(registry_path: str) -> Dict[str, Any]:
    """Load the Silver table registry from YAML"""
    with open(registry_path, 'r') as f:
        return yaml.safe_load(f)


def get_tables_for_group(
    registry: Dict[str, Any],
    group_filter: Optional[str] = None,
    specific_tables: Optional[List[str]] = None
) -> List[TableConfig]:
    """
    Get table configurations filtered by processing group or specific table names.

    Returns:
        List of TableConfig objects in processing order
    """
    tables_config = registry.get('tables', [])
    processing_groups = registry.get('processing_groups', [])

    # Build group order map
    group_order = {g['group']: g['order'] for g in processing_groups}

    # Parse table configs
    table_configs = []
    for t in tables_config:
        config = TableConfig(
            name=t['name'],
            source_bronze_table=t['source_bronze_table'],
            primary_key_columns=t['primary_key_columns'],
            processing_group=t['processing_group'],
            transformation_type=t.get('transformation_type', 'pass_through'),
            columns=t.get('columns', []),
            expectations=t.get('expectations', {}),
            source_filter=t.get('source_filter'),
            fk_validations=t.get('fk_validations'),
            has_geometry=t.get('has_geometry', False),
            geometry_columns=t.get('geometry_columns'),
            batch_size=t.get('batch_size', 100000),
            comment=t.get('comment')
        )
        table_configs.append(config)

    # Filter by specific tables if provided
    if specific_tables:
        table_configs = [t for t in table_configs if t.name in specific_tables]

    # Filter by group if provided
    if group_filter:
        table_configs = [t for t in table_configs if t.processing_group == group_filter]

    # Sort by processing group order
    table_configs.sort(key=lambda t: group_order.get(t.processing_group, 999))

    return table_configs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Loop

# COMMAND ----------

# Initialize components
watermark_mgr = WatermarkManager(spark, WATERMARK_TABLE)
transformer = SilverTableTransformer(
    spark=spark,
    source_catalog=SOURCE_CATALOG,
    source_schema=SOURCE_SCHEMA,
    target_catalog=TARGET_CATALOG,
    target_schema=TARGET_SCHEMA
)

# Load registry
print(f"Loading registry from: {REGISTRY_PATH}")
registry = load_registry(REGISTRY_PATH)
print(f"Registry version: {registry.get('registry_version', 'unknown')}")

# Get tables to process
table_configs = get_tables_for_group(registry, group_filter, specific_tables if specific_tables else None)

# Apply max_tables limit
if max_tables > 0:
    table_configs = table_configs[:max_tables]

print(f"Tables to process: {len(table_configs)}")

# Process tables in order
results: List[LoadResult] = []

for config in table_configs:
    # Get last watermark for this table
    last_wm = watermark_mgr.get_last_watermark(config.name)

    # Transform table
    result = transformer.transform_table(config, last_wm, force_full_load)
    results.append(result)

    # Update watermark
    watermark_mgr.update_watermark(
        table_name=result.table_name,
        source_bronze_table=result.source_bronze_table,
        processing_group=result.processing_group,
        bronze_watermark=result.new_watermark,
        status=result.status.value,
        rows_input=result.rows_input,
        rows_output=result.rows_output,
        rows_dropped=result.rows_dropped_critical,
        rows_flagged=result.rows_flagged_business,
        rows_warned=result.rows_warned_advisory,
        start_time=RUN_START_TIME,
        end_time=datetime.now(),
        run_id=RUN_ID,
        error_message=result.error_message
    )

print(f"\n{'='*60}")
print(f"Completed processing {len(results)} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

# Create summary DataFrame with explicit schema to avoid CANNOT_DETERMINE_TYPE
from pyspark.sql import Row

if results:
    summary_data = [
        Row(
            table=r.table_name,
            source=r.source_bronze_table,
            group=r.processing_group,
            status=r.status.value,
            rows_input=r.rows_input,
            rows_output=r.rows_output,
            rows_dropped=r.rows_dropped_critical,
            rows_flagged=r.rows_flagged_business,
            rows_warned=r.rows_warned_advisory,
            duration_sec=round(r.duration_seconds, 2),
            error=r.error_message[:80] if r.error_message else None
        )
        for r in results
    ]
    summary_df = spark.createDataFrame(summary_data, schema=SUMMARY_SCHEMA)
    display(summary_df.orderBy("group", "table"))
else:
    print("No tables were processed - results list is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistics

# COMMAND ----------

# Calculate statistics
success_count = sum(1 for r in results if r.status == LoadStatus.SUCCESS)
failed_count = sum(1 for r in results if r.status == LoadStatus.FAILED)
skipped_count = sum(1 for r in results if r.status == LoadStatus.SKIPPED)
total_rows_input = sum(r.rows_input for r in results)
total_rows_output = sum(r.rows_output for r in results)
total_dropped = sum(r.rows_dropped_critical for r in results)
total_flagged = sum(r.rows_flagged_business for r in results)
total_warned = sum(r.rows_warned_advisory for r in results)
total_duration = sum(r.duration_seconds for r in results)

print(f"""
{'='*70}
                      SILVER LAYER LOAD STATISTICS
{'='*70}

  Status Summary:
    Success:   {success_count:>4}
    Failed:    {failed_count:>4}
    Skipped:   {skipped_count:>4}
    ───────────────
    Total:     {len(results):>4}

  Row Counts:
    Input (from Bronze):      {total_rows_input:>15,}
    Output (to Silver):       {total_rows_output:>15,}
    Dropped (critical DQ):    {total_dropped:>15,}
    Flagged (business DQ):    {total_flagged:>15,}
    Warned (advisory DQ):     {total_warned:>15,}

  Performance:
    Total Duration:           {total_duration:>12.2f} sec ({total_duration/60:.1f} min)
    Avg Throughput:           {total_rows_output/total_duration if total_duration > 0 else 0:>12,.0f} rows/sec

{'='*70}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group Breakdown

# COMMAND ----------

# Group breakdown
from collections import defaultdict

group_stats = defaultdict(lambda: {"count": 0, "success": 0, "failed": 0, "skipped": 0, "rows": 0})

for r in results:
    grp = r.processing_group
    group_stats[grp]["count"] += 1
    group_stats[grp]["rows"] += r.rows_output
    if r.status == LoadStatus.SUCCESS:
        group_stats[grp]["success"] += 1
    elif r.status == LoadStatus.FAILED:
        group_stats[grp]["failed"] += 1
    else:
        group_stats[grp]["skipped"] += 1

print("Group Breakdown:")
print("-" * 60)
for grp, stats in sorted(group_stats.items()):
    print(f"  {grp}:")
    print(f"    Tables: {stats['count']} ({stats['success']} success, {stats['failed']} failed, {stats['skipped']} skipped)")
    print(f"    Rows:   {stats['rows']:,}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failure Analysis

# COMMAND ----------

# Check for failures
failures = [r for r in results if r.status == LoadStatus.FAILED]

if failures:
    print("="*70)
    print("                      FAILED TABLES")
    print("="*70)
    for f in failures:
        print(f"\n  Table: {f.table_name}")
        print(f"  Source: {f.source_bronze_table}")
        print(f"  Group: {f.processing_group}")
        print(f"  Error: {f.error_message[:100] if f.error_message else 'Unknown'}...")
    print("="*70)
    print(f"\nWARNING: {len(failures)} tables failed to load")
else:
    print("All tables loaded successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Sample Tables

# COMMAND ----------

# Verify sample Silver tables
sample_tables = [
    "silver_organization",
    "silver_project",
    "silver_worker",
    "silver_zone",
    "silver_device"
]

print("Verifying sample Silver tables:")
print("-" * 60)

for table_name in sample_tables:
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}"
    try:
        if spark.catalog.tableExists(target_table):
            count = spark.table(target_table).count()
            print(f"  {table_name}: {count:,} rows")
        else:
            print(f"  {table_name}: Not created yet")
    except Exception as e:
        print(f"  {table_name}: Error - {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermark Summary

# COMMAND ----------

# Show watermark status
print("Watermark Table Summary:")
print("-" * 60)

try:
    wm_df = spark.sql(f"""
        SELECT
            table_name,
            source_bronze_table,
            processing_group,
            last_load_status,
            last_load_row_count,
            rows_dropped_critical,
            rows_flagged_business,
            last_bronze_watermark,
            last_load_end_time
        FROM {WATERMARK_TABLE}
        ORDER BY processing_group, table_name
    """)
    display(wm_df)
except Exception as e:
    print(f"Could not query watermark table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit

# COMMAND ----------

# Final summary
end_time = datetime.now()
total_run_duration = (end_time - RUN_START_TIME).total_seconds()

print(f"End Time: {end_time}")
print(f"Total Run Duration: {total_run_duration:.1f}s ({total_run_duration/60:.1f} min)")

# Exit message
if failed_count > 0:
    exit_message = f"PARTIAL: Loaded {success_count}/{len(results)} tables ({total_rows_output:,} rows). {failed_count} failed."
else:
    exit_message = f"SUCCESS: Loaded {success_count} tables ({total_rows_output:,} rows) in {total_run_duration:.1f}s"

print(f"\n{exit_message}")
dbutils.notebook.exit(exit_message)
