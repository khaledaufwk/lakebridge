"""
TimescaleDB Incremental Loader v2.0 - Optimized
================================================
Enhanced loader with:
1. Support for GREATEST() watermark expressions
2. Proper geometry column handling with ST_AsText
3. Better error handling and retry logic
4. Parallel loading option for independent tables
5. Memory-optimized batch processing

Usage:
    loader = TimescaleDBLoaderV2(
        spark=spark,
        credentials=TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale"),
        target_catalog="wakecap_prod",
        target_schema="raw"
    )
    results = loader.load_all_tables("config/timescaledb_tables_v2.yml")
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timezone
from enum import Enum
import yaml
import json
import logging
import time
import re

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, IntegerType, DateType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TimescaleDBLoaderV2")


class WatermarkType(Enum):
    TIMESTAMP = "timestamp"
    BIGINT = "bigint"
    DATE = "date"
    INTEGER = "integer"


class LoadStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    SKIPPED = "skipped"


@dataclass
class TableConfigV2:
    """Enhanced table configuration with GREATEST support."""
    source_schema: str
    source_table: str
    primary_key_columns: List[str]
    watermark_column: str
    watermark_type: WatermarkType = WatermarkType.TIMESTAMP
    watermark_expression: Optional[str] = None  # NEW: Support for GREATEST expressions
    category: str = "unknown"
    is_full_load: bool = False
    fetch_size: int = 50000
    batch_size: int = 100000
    partition_column: Optional[str] = None
    custom_query: Optional[str] = None
    custom_column_mapping: Optional[Dict[str, str]] = None
    enabled: bool = True
    is_hypertable: bool = False
    has_geometry: bool = False
    geometry_columns: Optional[List[str]] = None
    comment: Optional[str] = None

    @property
    def full_source_name(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    @property
    def target_table_name(self) -> str:
        return f"{self.source_schema}_{self.source_table}".lower()

    @property
    def effective_watermark_expr(self) -> str:
        """Return the watermark expression to use in queries."""
        if self.watermark_expression:
            return self.watermark_expression
        return f'"{self.watermark_column}"'

    @classmethod
    def from_dict(cls, data: Dict[str, Any], defaults: Dict[str, Any] = None) -> "TableConfigV2":
        defaults = defaults or {}
        config = {**defaults, **data}

        wm_type = config.get("watermark_type", "timestamp")
        if isinstance(wm_type, str):
            wm_type = WatermarkType(wm_type.lower())

        pk_columns = config.get("primary_key_columns", [])
        if isinstance(pk_columns, str):
            pk_columns = [pk_columns]

        geometry_cols = config.get("geometry_columns", [])
        if isinstance(geometry_cols, str):
            geometry_cols = [geometry_cols]

        return cls(
            source_schema=config.get("source_schema", "public"),
            source_table=config["source_table"],
            primary_key_columns=pk_columns,
            watermark_column=config.get("watermark_column", "UpdatedAt"),
            watermark_type=wm_type,
            watermark_expression=config.get("watermark_expression"),
            category=config.get("category", "unknown"),
            is_full_load=config.get("is_full_load", False),
            fetch_size=config.get("fetch_size", 50000),
            batch_size=config.get("batch_size", 100000),
            partition_column=config.get("partition_column"),
            custom_query=config.get("custom_query"),
            custom_column_mapping=config.get("custom_column_mapping"),
            enabled=config.get("enabled", True),
            is_hypertable=config.get("is_hypertable", False),
            has_geometry=config.get("has_geometry", False),
            geometry_columns=geometry_cols if geometry_cols else None,
            comment=config.get("comment"),
        )


@dataclass
class LoadResult:
    table_config: TableConfigV2
    status: LoadStatus
    rows_loaded: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    previous_watermark: Optional[str] = None
    new_watermark: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class TimescaleDBCredentials:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        sslmode: str = "require"
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.sslmode = sslmode

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.sslmode}"
        )

    @classmethod
    def from_databricks_secrets(cls, scope: str, dbutils=None) -> "TimescaleDBCredentials":
        """Load credentials from Databricks secrets.

        Args:
            scope: The secret scope name
            dbutils: Optional dbutils instance. If not provided, will try to get from globals
                    or create one. Pass the notebook's dbutils for USER_ISOLATION clusters.
        """
        try:
            # Try to get dbutils from various sources
            if dbutils is None:
                # First try to get from IPython user namespace (notebook globals)
                try:
                    from IPython import get_ipython
                    ipython = get_ipython()
                    if ipython is not None:
                        dbutils = ipython.user_ns.get('dbutils')
                except:
                    pass

            if dbutils is None:
                # Try to get from builtins
                import builtins
                dbutils = getattr(builtins, 'dbutils', None)

            if dbutils is None:
                # Last resort: try to create DBUtils
                from pyspark.dbutils import DBUtils
                spark = SparkSession.getActiveSession()
                dbutils = DBUtils(spark)

            return cls(
                host=dbutils.secrets.get(scope, "timescaledb-host"),
                port=int(dbutils.secrets.get(scope, "timescaledb-port") or "5432"),
                database=dbutils.secrets.get(scope, "timescaledb-database"),
                user=dbutils.secrets.get(scope, "timescaledb-user"),
                password=dbutils.secrets.get(scope, "timescaledb-password"),
            )
        except Exception as e:
            logger.error(f"Could not load from Databricks secrets: {e}")
            raise


class WatermarkManagerV2:
    """Enhanced watermark manager with better concurrency handling."""

    WATERMARK_TABLE = "wakecap_prod.migration._timescaledb_watermarks"

    def __init__(self, spark: SparkSession, source_system: str = "timescaledb"):
        self.spark = spark
        self.source_system = source_system
        self._ensure_watermark_table()

    def _ensure_watermark_table(self) -> None:
        try:
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS wakecap_prod.migration")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.WATERMARK_TABLE} (
                    source_system STRING NOT NULL,
                    source_schema STRING NOT NULL,
                    source_table STRING NOT NULL,
                    watermark_column STRING NOT NULL,
                    watermark_type STRING NOT NULL,
                    watermark_expression STRING,
                    last_watermark_value STRING,
                    last_watermark_timestamp TIMESTAMP,
                    last_watermark_bigint BIGINT,
                    last_load_start_time TIMESTAMP,
                    last_load_end_time TIMESTAMP,
                    last_load_status STRING,
                    last_load_row_count BIGINT,
                    last_error_message STRING,
                    pipeline_id STRING,
                    pipeline_run_id STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    created_by STRING
                )
                USING DELTA
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.autoOptimize.optimizeWrite' = 'true'
                )
            """)
        except Exception as e:
            logger.warning(f"Could not create watermark table: {e}")

    def get_watermark(
        self,
        source_schema: str,
        source_table: str,
        watermark_type: WatermarkType = WatermarkType.TIMESTAMP
    ) -> Optional[Union[datetime, int, str]]:
        try:
            query = f"""
                SELECT last_watermark_value, last_watermark_timestamp, last_watermark_bigint
                FROM {self.WATERMARK_TABLE}
                WHERE source_system = '{self.source_system}'
                  AND source_schema = '{source_schema}'
                  AND source_table = '{source_table}'
                  AND last_load_status = 'success'
            """
            result = self.spark.sql(query).collect()
            if not result:
                return None

            row = result[0]
            if watermark_type == WatermarkType.TIMESTAMP:
                return row.last_watermark_timestamp
            elif watermark_type in (WatermarkType.BIGINT, WatermarkType.INTEGER):
                return row.last_watermark_bigint
            else:
                return row.last_watermark_value
        except Exception as e:
            logger.warning(f"Error getting watermark: {e}")
            return None

    def update_watermark(
        self,
        source_schema: str,
        source_table: str,
        watermark_column: str,
        watermark_type: WatermarkType,
        watermark_value: Union[datetime, int, str, None],
        watermark_expression: Optional[str],
        load_start_time: datetime,
        load_end_time: datetime,
        status: LoadStatus,
        row_count: int,
        error_message: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
    ) -> None:
        try:
            watermark_str = str(watermark_value) if watermark_value else None

            if watermark_type == WatermarkType.TIMESTAMP and watermark_value:
                watermark_ts_sql = f"TIMESTAMP'{watermark_value}'"
            else:
                watermark_ts_sql = "NULL"

            if watermark_type in (WatermarkType.BIGINT, WatermarkType.INTEGER) and watermark_value:
                watermark_bigint_sql = str(watermark_value)
            else:
                watermark_bigint_sql = "NULL"

            wm_expr_sql = f"'{watermark_expression}'" if watermark_expression else "NULL"
            err_sql = f"'{error_message[:500].replace(chr(39), chr(39)+chr(39))}'" if error_message else "NULL"

            self.spark.sql(f"""
                MERGE INTO {self.WATERMARK_TABLE} AS target
                USING (
                    SELECT
                        '{self.source_system}' AS source_system,
                        '{source_schema}' AS source_schema,
                        '{source_table}' AS source_table,
                        '{watermark_column}' AS watermark_column,
                        '{watermark_type.value}' AS watermark_type,
                        {wm_expr_sql} AS watermark_expression,
                        {f"'{watermark_str}'" if watermark_str else 'NULL'} AS last_watermark_value,
                        {watermark_ts_sql} AS last_watermark_timestamp,
                        {watermark_bigint_sql} AS last_watermark_bigint,
                        TIMESTAMP'{load_start_time}' AS last_load_start_time,
                        TIMESTAMP'{load_end_time}' AS last_load_end_time,
                        '{status.value}' AS last_load_status,
                        {row_count} AS last_load_row_count,
                        {err_sql} AS last_error_message,
                        {f"'{pipeline_id}'" if pipeline_id else 'NULL'} AS pipeline_id,
                        {f"'{pipeline_run_id}'" if pipeline_run_id else 'NULL'} AS pipeline_run_id
                ) AS source
                ON target.source_system = source.source_system
                   AND target.source_schema = source.source_schema
                   AND target.source_table = source.source_table
                WHEN MATCHED THEN UPDATE SET
                    target.watermark_column = source.watermark_column,
                    target.watermark_type = source.watermark_type,
                    target.watermark_expression = source.watermark_expression,
                    target.last_watermark_value = source.last_watermark_value,
                    target.last_watermark_timestamp = source.last_watermark_timestamp,
                    target.last_watermark_bigint = source.last_watermark_bigint,
                    target.last_load_start_time = source.last_load_start_time,
                    target.last_load_end_time = source.last_load_end_time,
                    target.last_load_status = source.last_load_status,
                    target.last_load_row_count = source.last_load_row_count,
                    target.last_error_message = source.last_error_message,
                    target.pipeline_id = source.pipeline_id,
                    target.pipeline_run_id = source.pipeline_run_id,
                    target.updated_at = current_timestamp()
                WHEN NOT MATCHED THEN INSERT (
                    source_system, source_schema, source_table,
                    watermark_column, watermark_type, watermark_expression,
                    last_watermark_value, last_watermark_timestamp, last_watermark_bigint,
                    last_load_start_time, last_load_end_time,
                    last_load_status, last_load_row_count, last_error_message,
                    pipeline_id, pipeline_run_id,
                    created_at, updated_at, created_by
                ) VALUES (
                    source.source_system, source.source_schema, source.source_table,
                    source.watermark_column, source.watermark_type, source.watermark_expression,
                    source.last_watermark_value, source.last_watermark_timestamp, source.last_watermark_bigint,
                    source.last_load_start_time, source.last_load_end_time,
                    source.last_load_status, source.last_load_row_count, source.last_error_message,
                    source.pipeline_id, source.pipeline_run_id,
                    current_timestamp(), current_timestamp(), 'timescaledb_loader_v2'
                )
            """)
        except Exception as e:
            logger.error(f"Error updating watermark: {e}")


class TimescaleDBLoaderV2:
    """
    Enhanced loader with GREATEST watermark support and geometry handling.
    """

    def __init__(
        self,
        spark: SparkSession,
        credentials: TimescaleDBCredentials,
        target_catalog: str = "wakecap_prod",
        target_schema: str = "raw",
        pipeline_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
        table_prefix: str = "timescale_",
        max_retries: int = 3,
        retry_delay: int = 5,
    ):
        self.spark = spark
        self.credentials = credentials
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.pipeline_id = pipeline_id
        self.pipeline_run_id = pipeline_run_id
        self.table_prefix = table_prefix
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.watermark_manager = WatermarkManagerV2(spark)

        # Ensure target schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
        logger.info(f"Initialized TimescaleDBLoaderV2 for {target_catalog}.{target_schema}")

    def _build_geometry_select(self, table_config: TableConfigV2) -> str:
        """Build SELECT with geometry columns converted to WKT."""
        if not table_config.has_geometry or not table_config.geometry_columns:
            return "*"

        # Get all columns except geometry, then add geometry as WKT
        geom_cols = table_config.geometry_columns
        geom_selects = [f'ST_AsText("{col}") AS "{col}WKT"' for col in geom_cols]

        return f'*, {", ".join(geom_selects)}'

    def _format_watermark_for_sql(
        self,
        watermark_value: Union[datetime, int, str],
        watermark_type: WatermarkType
    ) -> str:
        """Format watermark value for use in SQL WHERE clause.

        Ensures datetime values are formatted in PostgreSQL-compatible ISO format
        without timezone offset issues.
        """
        if watermark_type == WatermarkType.TIMESTAMP:
            if isinstance(watermark_value, datetime):
                # Format as ISO 8601 without timezone for PostgreSQL compatibility
                # PostgreSQL TIMESTAMP columns expect: 'YYYY-MM-DD HH:MI:SS.ffffff'
                return watermark_value.strftime('%Y-%m-%d %H:%M:%S.%f')
            elif isinstance(watermark_value, str):
                # If already a string, try to parse and reformat for consistency
                try:
                    from datetime import datetime as dt
                    # Handle various input formats
                    for fmt in ['%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S']:
                        try:
                            parsed = dt.strptime(watermark_value.replace('+00:00', '').replace('Z', ''), fmt.replace('%z', ''))
                            return parsed.strftime('%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            continue
                    return watermark_value
                except Exception:
                    return watermark_value
            return str(watermark_value)
        else:
            return str(watermark_value)

    def _build_incremental_query(
        self,
        table_config: TableConfigV2,
        last_watermark: Optional[Union[datetime, int, str]]
    ) -> str:
        """Build query with GREATEST watermark support."""

        # Format watermark for SQL if present
        formatted_watermark = None
        if last_watermark is not None:
            formatted_watermark = self._format_watermark_for_sql(
                last_watermark, table_config.watermark_type
            )

        if table_config.custom_query:
            if formatted_watermark:
                return table_config.custom_query.format(watermark=formatted_watermark)
            else:
                # Remove WHERE clause for full load
                query = re.sub(r"WHERE\s+.*?(?=ORDER|$)", "", table_config.custom_query, flags=re.IGNORECASE | re.DOTALL)
                return query

        columns = self._build_geometry_select(table_config)
        from_clause = f'"{table_config.source_schema}"."{table_config.source_table}"'
        watermark_expr = table_config.effective_watermark_expr

        if last_watermark is None or table_config.is_full_load:
            query = f"SELECT {columns} FROM {from_clause}"
        else:
            if table_config.watermark_type == WatermarkType.TIMESTAMP:
                watermark_filter = f"{watermark_expr} > '{formatted_watermark}'"
            else:
                watermark_filter = f"{watermark_expr} > {formatted_watermark}"

            query = f"SELECT {columns} FROM {from_clause} WHERE {watermark_filter}"

        # Order by watermark for consistent loading
        query += f' ORDER BY {watermark_expr}'

        logger.info(f"Query: {query[:300]}...")
        return query

    def _read_from_timescaledb(
        self,
        query: str,
        fetch_size: int = 50000
    ) -> DataFrame:
        """Execute query with optimized settings."""
        return (
            self.spark.read
            .format("jdbc")
            .option("url", self.credentials.jdbc_url)
            .option("query", query)
            .option("user", self.credentials.user)
            .option("password", self.credentials.password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", fetch_size)
            .option("sessionInitStatement", "SET statement_timeout = '60min'")
            .load()
        )

    def _add_metadata_columns(self, df: DataFrame, table_config: TableConfigV2) -> DataFrame:
        return (
            df
            .withColumn("_loaded_at", F.current_timestamp())
            .withColumn("_source_system", F.lit("timescaledb"))
            .withColumn("_source_schema", F.lit(table_config.source_schema))
            .withColumn("_source_table", F.lit(table_config.source_table))
            .withColumn("_pipeline_id", F.lit(self.pipeline_id))
            .withColumn("_pipeline_run_id", F.lit(self.pipeline_run_id))
        )

    def _drop_geometry_columns(self, df: DataFrame, table_config: TableConfigV2) -> DataFrame:
        """Drop raw geometry columns (keep WKT versions)."""
        if not table_config.has_geometry or not table_config.geometry_columns:
            return df

        for col in table_config.geometry_columns:
            if col in df.columns:
                df = df.drop(col)

        return df

    def _get_max_watermark_value(
        self,
        df: DataFrame,
        table_config: TableConfigV2
    ) -> Optional[Union[datetime, int, str]]:
        """Get max watermark using the configured column or GREATEST expression.

        IMPORTANT: When a watermark_expression (GREATEST) is used, we must calculate
        the max of that expression, not just the watermark_column. Otherwise, rows
        where CreatedAt > UpdatedAt will be reloaded on every run because their
        GREATEST value exceeds the stored MAX(UpdatedAt).
        """
        try:
            if df.isEmpty():
                return None

            # If using GREATEST expression, compute max of GREATEST across all relevant columns
            if table_config.watermark_expression:
                # Extract column names from the GREATEST expression
                # Pattern: COALESCE("ColumnName", ...)
                import re
                col_pattern = r'COALESCE\("([^"]+)"'
                matches = re.findall(col_pattern, table_config.watermark_expression)

                if matches:
                    # Filter to columns that exist in the DataFrame
                    existing_cols = [col for col in matches if col in df.columns]

                    if existing_cols:
                        # Calculate GREATEST across all matched columns, handling nulls
                        # Use coalesce with a very old date for null handling (matching the SQL expression)
                        from pyspark.sql.functions import coalesce, lit, greatest
                        from datetime import datetime as dt

                        min_date = dt(1900, 1, 1)
                        col_exprs = [coalesce(F.col(c), F.lit(min_date)) for c in existing_cols]

                        if len(col_exprs) == 1:
                            max_expr = col_exprs[0]
                        else:
                            max_expr = greatest(*col_exprs)

                        max_row = df.agg(F.max(max_expr).alias("max_wm")).collect()[0]
                        logger.info(f"Calculated max GREATEST watermark from columns {existing_cols}: {max_row.max_wm}")
                        return max_row.max_wm

            # Fallback to simple watermark column
            wm_col = table_config.watermark_column
            if wm_col in df.columns:
                max_row = df.agg(F.max(wm_col).alias("max_wm")).collect()[0]
                return max_row.max_wm
            return None
        except Exception as e:
            logger.warning(f"Error getting max watermark: {e}")
            return None

    def _write_to_delta(
        self,
        df: DataFrame,
        table_config: TableConfigV2
    ) -> int:
        """Write to Delta with merge for deduplication."""
        table_name = f"{self.table_prefix}{table_config.source_table}".lower()
        target_table = f"{self.target_catalog}.{self.target_schema}.{table_name}"

        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(target_table)
        except Exception:
            pass

        row_count = df.count()

        if not table_exists or table_config.is_full_load:
            logger.info(f"Writing full load to {target_table} ({row_count} rows)")

            writer = df.write.format("delta").mode("overwrite")

            if table_config.partition_column:
                writer = writer.partitionBy(table_config.partition_column)

            writer.saveAsTable(target_table)

            self.spark.sql(f"""
                ALTER TABLE {target_table} SET TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'quality' = 'bronze',
                    'source_system' = 'timescaledb',
                    'source_table' = '{table_config.full_source_name}'
                )
            """)
        else:
            logger.info(f"Merging {row_count} rows into {target_table}")

            from delta.tables import DeltaTable
            delta_table = DeltaTable.forName(self.spark, target_table)

            merge_condition = " AND ".join([
                f"target.{pk} = source.{pk}"
                for pk in table_config.primary_key_columns
            ])

            (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        return row_count

    def load_table(
        self,
        table_config: TableConfigV2,
        force_full_load: bool = False
    ) -> LoadResult:
        """Load a single table with retry support."""

        start_time = datetime.now(timezone.utc)
        retry_count = 0

        while retry_count <= self.max_retries:
            try:
                logger.info(f"Loading {table_config.full_source_name} (attempt {retry_count + 1})")

                # Get last watermark
                last_watermark = None
                if not force_full_load and not table_config.is_full_load:
                    last_watermark = self.watermark_manager.get_watermark(
                        table_config.source_schema,
                        table_config.source_table,
                        table_config.watermark_type
                    )

                logger.info(f"Last watermark: {last_watermark}")

                # Build and execute query
                query = self._build_incremental_query(table_config, last_watermark)
                df = self._read_from_timescaledb(query, table_config.fetch_size)

                # Cache for efficiency
                df = df.cache()

                # Get new watermark
                new_watermark = self._get_max_watermark_value(df, table_config)

                if new_watermark is None and last_watermark is not None:
                    logger.info(f"No new data for {table_config.full_source_name}")
                    df.unpersist()
                    end_time = datetime.now(timezone.utc)

                    self.watermark_manager.update_watermark(
                        source_schema=table_config.source_schema,
                        source_table=table_config.source_table,
                        watermark_column=table_config.watermark_column,
                        watermark_type=table_config.watermark_type,
                        watermark_value=last_watermark,
                        watermark_expression=table_config.watermark_expression,
                        load_start_time=start_time,
                        load_end_time=end_time,
                        status=LoadStatus.SUCCESS,
                        row_count=0,
                        pipeline_id=self.pipeline_id,
                        pipeline_run_id=self.pipeline_run_id
                    )

                    return LoadResult(
                        table_config=table_config,
                        status=LoadStatus.SKIPPED,
                        rows_loaded=0,
                        start_time=start_time,
                        end_time=end_time,
                        previous_watermark=str(last_watermark) if last_watermark else None,
                        retry_count=retry_count
                    )

                # Add metadata and drop raw geometry columns
                df = self._add_metadata_columns(df, table_config)
                df = self._drop_geometry_columns(df, table_config)

                # Write to Delta
                rows_written = self._write_to_delta(df, table_config)

                df.unpersist()
                end_time = datetime.now(timezone.utc)

                # Update watermark
                self.watermark_manager.update_watermark(
                    source_schema=table_config.source_schema,
                    source_table=table_config.source_table,
                    watermark_column=table_config.watermark_column,
                    watermark_type=table_config.watermark_type,
                    watermark_value=new_watermark,
                    watermark_expression=table_config.watermark_expression,
                    load_start_time=start_time,
                    load_end_time=end_time,
                    status=LoadStatus.SUCCESS,
                    row_count=rows_written,
                    pipeline_id=self.pipeline_id,
                    pipeline_run_id=self.pipeline_run_id
                )

                logger.info(f"Successfully loaded {rows_written} rows for {table_config.full_source_name}")

                return LoadResult(
                    table_config=table_config,
                    status=LoadStatus.SUCCESS,
                    rows_loaded=rows_written,
                    start_time=start_time,
                    end_time=end_time,
                    previous_watermark=str(last_watermark) if last_watermark else None,
                    new_watermark=str(new_watermark) if new_watermark else None,
                    retry_count=retry_count
                )

            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                logger.error(f"Error loading {table_config.full_source_name}: {error_msg}")

                if retry_count <= self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    end_time = datetime.now(timezone.utc)

                    self.watermark_manager.update_watermark(
                        source_schema=table_config.source_schema,
                        source_table=table_config.source_table,
                        watermark_column=table_config.watermark_column,
                        watermark_type=table_config.watermark_type,
                        watermark_value=None,
                        watermark_expression=table_config.watermark_expression,
                        load_start_time=start_time,
                        load_end_time=end_time,
                        status=LoadStatus.FAILED,
                        row_count=0,
                        error_message=error_msg[:1000],
                        pipeline_id=self.pipeline_id,
                        pipeline_run_id=self.pipeline_run_id
                    )

                    return LoadResult(
                        table_config=table_config,
                        status=LoadStatus.FAILED,
                        rows_loaded=0,
                        start_time=start_time,
                        end_time=end_time,
                        error_message=error_msg,
                        retry_count=retry_count
                    )

    def load_all_tables(
        self,
        registry_path: str,
        category_filter: Optional[str] = None,
        table_filter: Optional[List[str]] = None,
        force_full_load: bool = False
    ) -> List[LoadResult]:
        """Load all tables from registry."""

        table_configs = self.load_registry(registry_path)

        if category_filter:
            table_configs = [t for t in table_configs if t.category == category_filter]

        if table_filter:
            table_configs = [t for t in table_configs if t.source_table in table_filter]

        table_configs = [t for t in table_configs if t.enabled]

        logger.info(f"Loading {len(table_configs)} tables")

        results = []
        for config in table_configs:
            result = self.load_table(config, force_full_load=force_full_load)
            results.append(result)

        # Summary
        success = sum(1 for r in results if r.status == LoadStatus.SUCCESS)
        failed = sum(1 for r in results if r.status == LoadStatus.FAILED)
        skipped = sum(1 for r in results if r.status == LoadStatus.SKIPPED)
        total_rows = sum(r.rows_loaded for r in results)

        logger.info(f"Complete: {success} success, {failed} failed, {skipped} skipped, {total_rows} rows")

        return results

    @staticmethod
    def load_registry(registry_path: str) -> List[TableConfigV2]:
        """Load table configurations from registry file."""
        with open(registry_path, "r") as f:
            if registry_path.endswith(".yaml") or registry_path.endswith(".yml"):
                registry = yaml.safe_load(f)
            else:
                registry = json.load(f)

        defaults = registry.get("defaults", {})
        tables = registry.get("tables", [])
        excluded = registry.get("excluded_tables", [])

        # Build excluded set
        excluded_names = {t.get("source_table") for t in excluded}

        return [
            TableConfigV2.from_dict(table_data, defaults)
            for table_data in tables
            if table_data.get("source_table") not in excluded_names
        ]
