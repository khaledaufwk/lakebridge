"""
TimescaleDB Incremental Loader for Databricks
==============================================
Parametrized reusable template for loading tables from TimescaleDB
into Databricks raw zone with watermark-based incremental loading.

Usage:
    # Initialize loader
    loader = TimescaleDBLoader(
        spark=spark,
        credentials=TimescaleDBCredentials.from_yaml("credentials_template.yml"),
        target_catalog="wakecap_prod",
        target_schema="raw_timescaledb"
    )

    # Load a single table
    result = loader.load_table(table_config)

    # Load all tables from registry
    results = loader.load_all_tables("config/timescaledb_tables.yml")
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timezone
from enum import Enum
import yaml
import json
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, IntegerType, DateType
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TimescaleDBLoader")


class WatermarkType(Enum):
    """Supported watermark column types."""
    TIMESTAMP = "timestamp"
    BIGINT = "bigint"
    DATE = "date"
    INTEGER = "integer"


class LoadStatus(Enum):
    """Load operation status."""
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    SKIPPED = "skipped"


@dataclass
class TableConfig:
    """Configuration for a single table to be loaded."""
    source_schema: str
    source_table: str
    primary_key_columns: List[str]
    watermark_column: str
    watermark_type: WatermarkType = WatermarkType.TIMESTAMP
    category: str = "unknown"
    is_full_load: bool = False
    fetch_size: int = 10000
    batch_size: int = 100000
    partition_column: Optional[str] = None
    custom_query: Optional[str] = None
    custom_column_mapping: Optional[Dict[str, str]] = None
    enabled: bool = True
    is_hypertable: bool = False
    chunk_time_interval: Optional[str] = None
    comment: Optional[str] = None

    @property
    def full_source_name(self) -> str:
        """Return fully qualified source table name."""
        return f"{self.source_schema}.{self.source_table}"

    @property
    def primary_key_str(self) -> str:
        """Return comma-separated primary key columns."""
        return ",".join(self.primary_key_columns)

    @property
    def target_table_name(self) -> str:
        """Return target table name (lowercase, underscores)."""
        return f"{self.source_schema}_{self.source_table}".lower()

    @classmethod
    def from_dict(cls, data: Dict[str, Any], defaults: Dict[str, Any] = None) -> "TableConfig":
        """Create TableConfig from dictionary with defaults."""
        defaults = defaults or {}

        # Merge with defaults
        config = {**defaults, **data}

        # Convert watermark_type string to enum
        wm_type = config.get("watermark_type", "timestamp")
        if isinstance(wm_type, str):
            wm_type = WatermarkType(wm_type.lower())

        # Handle primary_key_columns as list or string
        pk_columns = config.get("primary_key_columns", [])
        if isinstance(pk_columns, str):
            pk_columns = [pk_columns]

        return cls(
            source_schema=config["source_schema"],
            source_table=config["source_table"],
            primary_key_columns=pk_columns,
            watermark_column=config.get("watermark_column", "created_at"),
            watermark_type=wm_type,
            category=config.get("category", "unknown"),
            is_full_load=config.get("is_full_load", False),
            fetch_size=config.get("fetch_size", 10000),
            batch_size=config.get("batch_size", 100000),
            partition_column=config.get("partition_column"),
            custom_query=config.get("custom_query"),
            custom_column_mapping=config.get("custom_column_mapping"),
            enabled=config.get("enabled", True),
            is_hypertable=config.get("is_hypertable", False),
            chunk_time_interval=config.get("chunk_time_interval"),
            comment=config.get("comment"),
        )


@dataclass
class LoadResult:
    """Result of a table load operation."""
    table_config: TableConfig
    status: LoadStatus
    rows_loaded: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    previous_watermark: Optional[str] = None
    new_watermark: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate load duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class TimescaleDBCredentials:
    """Manages TimescaleDB connection credentials."""

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
        """Generate JDBC connection URL."""
        return (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.sslmode}"
        )

    @property
    def connection_properties(self) -> Dict[str, str]:
        """Return JDBC connection properties."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
            "sslmode": self.sslmode,
        }

    @classmethod
    def from_yaml(cls, yaml_path: str, key: str = "timescaledb") -> "TimescaleDBCredentials":
        """Load credentials from YAML file."""
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f)

        creds = config[key]
        return cls(
            host=creds["host"],
            port=creds.get("port", 5432),
            database=creds["database"],
            user=creds["user"],
            password=creds["password"],
            sslmode=creds.get("sslmode", "require"),
        )

    @classmethod
    def from_databricks_secrets(
        cls,
        scope: str,
        host_key: str = "timescaledb-host",
        port_key: str = "timescaledb-port",
        database_key: str = "timescaledb-database",
        user_key: str = "timescaledb-user",
        password_key: str = "timescaledb-password",
    ) -> "TimescaleDBCredentials":
        """Load credentials from Databricks secret scope."""
        try:
            from pyspark.dbutils import DBUtils
            spark = SparkSession.getActiveSession()
            dbutils = DBUtils(spark)

            return cls(
                host=dbutils.secrets.get(scope, host_key),
                port=int(dbutils.secrets.get(scope, port_key) or "5432"),
                database=dbutils.secrets.get(scope, database_key),
                user=dbutils.secrets.get(scope, user_key),
                password=dbutils.secrets.get(scope, password_key),
            )
        except Exception as e:
            logger.warning(f"Could not load from Databricks secrets: {e}")
            raise


class WatermarkManager:
    """Manages watermark state for incremental loads."""

    WATERMARK_TABLE = "wakecap_prod.migration._timescaledb_watermarks"

    def __init__(self, spark: SparkSession, source_system: str = "timescaledb"):
        self.spark = spark
        self.source_system = source_system
        self._ensure_watermark_table()

    def _ensure_watermark_table(self) -> None:
        """Create watermark table if it doesn't exist."""
        try:
            # First ensure the schema exists
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS wakecap_prod.migration COMMENT 'Migration tracking and metadata'")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.WATERMARK_TABLE} (
                    source_system STRING NOT NULL,
                    source_schema STRING NOT NULL,
                    source_table STRING NOT NULL,
                    watermark_column STRING NOT NULL,
                    watermark_type STRING NOT NULL,
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
            logger.info(f"Watermark table {self.WATERMARK_TABLE} ready")
        except Exception as e:
            logger.warning(f"Could not create watermark table: {e}")

    def get_watermark(
        self,
        source_schema: str,
        source_table: str,
        watermark_type: WatermarkType = WatermarkType.TIMESTAMP
    ) -> Optional[Union[datetime, int, str]]:
        """
        Retrieve the last watermark value for a table.

        Returns None if no watermark exists (indicating full load needed).
        """
        try:
            query = f"""
                SELECT
                    last_watermark_value,
                    last_watermark_timestamp,
                    last_watermark_bigint
                FROM {self.WATERMARK_TABLE}
                WHERE source_system = '{self.source_system}'
                  AND source_schema = '{source_schema}'
                  AND source_table = '{source_table}'
                  AND last_load_status = 'success'
            """

            result = self.spark.sql(query).collect()

            if not result:
                logger.info(f"No watermark found for {source_schema}.{source_table}")
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
        load_start_time: datetime,
        load_end_time: datetime,
        status: LoadStatus,
        row_count: int,
        error_message: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
    ) -> None:
        """Update or insert watermark record for a table."""
        try:
            # Convert watermark value to appropriate types
            watermark_str = str(watermark_value) if watermark_value else None

            # Format timestamp for SQL
            if watermark_type == WatermarkType.TIMESTAMP and watermark_value:
                watermark_ts_sql = f"TIMESTAMP'{watermark_value}'"
            else:
                watermark_ts_sql = "NULL"

            # Format bigint
            if watermark_type in (WatermarkType.BIGINT, WatermarkType.INTEGER) and watermark_value:
                watermark_bigint_sql = str(watermark_value)
            else:
                watermark_bigint_sql = "NULL"

            # Use MERGE for upsert
            self.spark.sql(f"""
                MERGE INTO {self.WATERMARK_TABLE} AS target
                USING (
                    SELECT
                        '{self.source_system}' AS source_system,
                        '{source_schema}' AS source_schema,
                        '{source_table}' AS source_table,
                        '{watermark_column}' AS watermark_column,
                        '{watermark_type.value}' AS watermark_type,
                        {f"'{watermark_str}'" if watermark_str else 'NULL'} AS last_watermark_value,
                        {watermark_ts_sql} AS last_watermark_timestamp,
                        {watermark_bigint_sql} AS last_watermark_bigint,
                        TIMESTAMP'{load_start_time}' AS last_load_start_time,
                        TIMESTAMP'{load_end_time}' AS last_load_end_time,
                        '{status.value}' AS last_load_status,
                        {row_count} AS last_load_row_count,
                        {f"'{error_message[:500]}'" if error_message else 'NULL'} AS last_error_message,
                        {f"'{pipeline_id}'" if pipeline_id else 'NULL'} AS pipeline_id,
                        {f"'{pipeline_run_id}'" if pipeline_run_id else 'NULL'} AS pipeline_run_id
                ) AS source
                ON target.source_system = source.source_system
                   AND target.source_schema = source.source_schema
                   AND target.source_table = source.source_table
                WHEN MATCHED THEN UPDATE SET
                    target.watermark_column = source.watermark_column,
                    target.watermark_type = source.watermark_type,
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
                    watermark_column, watermark_type,
                    last_watermark_value, last_watermark_timestamp, last_watermark_bigint,
                    last_load_start_time, last_load_end_time,
                    last_load_status, last_load_row_count, last_error_message,
                    pipeline_id, pipeline_run_id,
                    created_at, updated_at, created_by
                ) VALUES (
                    source.source_system, source.source_schema, source.source_table,
                    source.watermark_column, source.watermark_type,
                    source.last_watermark_value, source.last_watermark_timestamp, source.last_watermark_bigint,
                    source.last_load_start_time, source.last_load_end_time,
                    source.last_load_status, source.last_load_row_count, source.last_error_message,
                    source.pipeline_id, source.pipeline_run_id,
                    current_timestamp(), current_timestamp(), 'timescaledb_loader'
                )
            """)
            logger.info(f"Updated watermark for {source_schema}.{source_table}")
        except Exception as e:
            logger.error(f"Error updating watermark: {e}")

    def mark_running(
        self,
        source_schema: str,
        source_table: str,
        pipeline_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
    ) -> None:
        """Mark a table load as currently running."""
        try:
            self.spark.sql(f"""
                MERGE INTO {self.WATERMARK_TABLE} AS target
                USING (
                    SELECT
                        '{self.source_system}' AS source_system,
                        '{source_schema}' AS source_schema,
                        '{source_table}' AS source_table
                ) AS source
                ON target.source_system = source.source_system
                   AND target.source_schema = source.source_schema
                   AND target.source_table = source.source_table
                WHEN MATCHED THEN UPDATE SET
                    target.last_load_status = 'running',
                    target.last_load_start_time = current_timestamp(),
                    target.pipeline_id = {f"'{pipeline_id}'" if pipeline_id else 'NULL'},
                    target.pipeline_run_id = {f"'{pipeline_run_id}'" if pipeline_run_id else 'NULL'},
                    target.updated_at = current_timestamp()
                WHEN NOT MATCHED THEN INSERT (
                    source_system, source_schema, source_table,
                    watermark_column, watermark_type,
                    last_load_status, last_load_start_time,
                    pipeline_id, pipeline_run_id,
                    created_at, updated_at, created_by
                ) VALUES (
                    source.source_system, source.source_schema, source.source_table,
                    'unknown', 'timestamp',
                    'running', current_timestamp(),
                    {f"'{pipeline_id}'" if pipeline_id else 'NULL'},
                    {f"'{pipeline_run_id}'" if pipeline_run_id else 'NULL'},
                    current_timestamp(), current_timestamp(), 'timescaledb_loader'
                )
            """)
        except Exception as e:
            logger.warning(f"Error marking running: {e}")


class TimescaleDBLoader:
    """
    Parametrized loader for incremental data loading from TimescaleDB to Databricks.

    This loader implements watermark-based incremental extraction, reading only
    new/changed records based on a timestamp or ID column.

    Usage:
        loader = TimescaleDBLoader(
            spark=spark,
            credentials=TimescaleDBCredentials.from_yaml("credentials.yml"),
            target_catalog="wakecap_prod",
            target_schema="raw_timescaledb"
        )

        # Load a single table
        result = loader.load_table(table_config)

        # Load all tables from registry
        results = loader.load_all_tables("config/timescaledb_tables.yml")
    """

    def __init__(
        self,
        spark: SparkSession,
        credentials: TimescaleDBCredentials,
        target_catalog: str = "wakecap_prod",
        target_schema: str = "raw_timescaledb",
        pipeline_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
        table_prefix: str = "",
    ):
        self.spark = spark
        self.credentials = credentials
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.pipeline_id = pipeline_id
        self.pipeline_run_id = pipeline_run_id
        self.table_prefix = table_prefix
        self.watermark_manager = WatermarkManager(spark)

        logger.info(f"Initialized TimescaleDBLoader for {target_catalog}.{target_schema} (prefix={table_prefix})")

    def _build_column_list(self, table_config: TableConfig) -> str:
        """Build column list with any custom column mappings."""
        if not table_config.custom_column_mapping:
            return "*"

        # For custom mappings, we need to list columns explicitly
        # This would require querying the table schema first
        # For simplicity, return * and handle in query
        return "*"

    def _build_incremental_query(
        self,
        table_config: TableConfig,
        last_watermark: Optional[Union[datetime, int, str]]
    ) -> str:
        """Build the SQL query for incremental extraction."""

        # Use custom query if provided
        if table_config.custom_query:
            if last_watermark:
                return table_config.custom_query.format(watermark=last_watermark)
            else:
                # Remove watermark filter for full load
                return table_config.custom_query.replace(
                    f"WHERE {table_config.watermark_column} > '{{watermark}}'", ""
                )

        # Build standard query
        columns = self._build_column_list(table_config)
        from_clause = f'"{table_config.source_schema}"."{table_config.source_table}"'

        if last_watermark is None or table_config.is_full_load:
            # Full load - no watermark filter
            query = f"SELECT {columns} FROM {from_clause}"
        else:
            # Incremental load with watermark filter
            wm_col = table_config.watermark_column

            if table_config.watermark_type == WatermarkType.TIMESTAMP:
                watermark_filter = f'"{wm_col}" > \'{last_watermark}\''
            elif table_config.watermark_type == WatermarkType.DATE:
                watermark_filter = f'"{wm_col}" > \'{last_watermark}\''
            else:
                watermark_filter = f'"{wm_col}" > {last_watermark}'

            query = f"SELECT {columns} FROM {from_clause} WHERE {watermark_filter}"

        # Add ordering by watermark column for consistent loading
        query += f' ORDER BY "{table_config.watermark_column}"'

        logger.info(f"Built query: {query[:200]}...")
        return query

    def _read_from_timescaledb(
        self,
        query: str,
        fetch_size: int = 10000
    ) -> DataFrame:
        """Execute query against TimescaleDB and return DataFrame."""

        logger.info(f"Reading from TimescaleDB with fetchsize={fetch_size}")

        df = (
            self.spark.read
            .format("jdbc")
            .option("url", self.credentials.jdbc_url)
            .option("query", query)
            .option("user", self.credentials.user)
            .option("password", self.credentials.password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", fetch_size)
            .option("sessionInitStatement", "SET statement_timeout = '30min'")
            .load()
        )

        return df

    def _add_metadata_columns(
        self,
        df: DataFrame,
        table_config: TableConfig
    ) -> DataFrame:
        """Add metadata columns to the DataFrame."""

        return (
            df
            .withColumn("_loaded_at", F.current_timestamp())
            .withColumn("_source_system", F.lit("timescaledb"))
            .withColumn("_source_schema", F.lit(table_config.source_schema))
            .withColumn("_source_table", F.lit(table_config.source_table))
            .withColumn("_pipeline_id", F.lit(self.pipeline_id))
            .withColumn("_pipeline_run_id", F.lit(self.pipeline_run_id))
        )

    def _get_max_watermark(
        self,
        df: DataFrame,
        watermark_column: str,
        watermark_type: WatermarkType
    ) -> Optional[Union[datetime, int, str]]:
        """Extract the maximum watermark value from the DataFrame."""

        try:
            if df.isEmpty():
                return None

            max_row = df.agg(F.max(watermark_column).alias("max_watermark")).collect()[0]
            return max_row.max_watermark
        except Exception as e:
            logger.warning(f"Error getting max watermark: {e}")
            return None

    def _write_to_delta(
        self,
        df: DataFrame,
        table_config: TableConfig,
        mode: str = "append"
    ) -> int:
        """Write DataFrame to Delta table with merge for deduplication."""

        # Build target table name with optional prefix
        table_name = f"{self.table_prefix}{table_config.source_table}".lower()
        target_table = f"{self.target_catalog}.{self.target_schema}.{table_name}"

        # Check if table exists
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(target_table)
        except Exception:
            pass

        row_count = df.count()

        if not table_exists or table_config.is_full_load:
            # Create or overwrite table
            logger.info(f"Writing full load to {target_table} ({row_count} rows)")

            writer = df.write.format("delta").mode("overwrite")

            # Add partitioning if configured
            if table_config.partition_column:
                writer = writer.partitionBy(table_config.partition_column)

            writer.saveAsTable(target_table)

            # Set table properties
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
            # Merge for incremental load with deduplication
            logger.info(f"Merging incremental data to {target_table} ({row_count} rows)")

            from delta.tables import DeltaTable
            delta_table = DeltaTable.forName(self.spark, target_table)

            # Build merge condition from primary keys
            merge_condition = " AND ".join([
                f"target.{pk} = source.{pk}"
                for pk in table_config.primary_key_columns
            ])

            # Perform merge (upsert)
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
        table_config: TableConfig,
        force_full_load: bool = False
    ) -> LoadResult:
        """
        Load a single table from TimescaleDB to Databricks.

        Args:
            table_config: Configuration for the table to load
            force_full_load: Force full load regardless of existing watermark

        Returns:
            LoadResult with status and statistics
        """

        start_time = datetime.now(timezone.utc)

        logger.info(f"Starting load for {table_config.full_source_name}")

        # Mark as running in watermark table
        self.watermark_manager.mark_running(
            table_config.source_schema,
            table_config.source_table,
            self.pipeline_id,
            self.pipeline_run_id
        )

        try:
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

            # Cache the DataFrame to avoid re-reading from JDBC
            # This is more efficient than calling count() which would read all data twice
            df = df.cache()

            # Get new watermark before adding metadata (this triggers a read but caches the data)
            new_watermark = self._get_max_watermark(
                df,
                table_config.watermark_column,
                table_config.watermark_type
            )

            # Check if we have data by checking if watermark exists
            # This is more efficient than count() as the data is already cached
            if new_watermark is None:
                logger.info(f"No new data for {table_config.full_source_name}")
                end_time = datetime.now(timezone.utc)
                df.unpersist()

                self.watermark_manager.update_watermark(
                    source_schema=table_config.source_schema,
                    source_table=table_config.source_table,
                    watermark_column=table_config.watermark_column,
                    watermark_type=table_config.watermark_type,
                    watermark_value=last_watermark,
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
                    previous_watermark=str(last_watermark) if last_watermark else None
                )

            # Add metadata columns
            df = self._add_metadata_columns(df, table_config)

            # Write to Delta
            rows_written = self._write_to_delta(df, table_config)

            # Clean up cached DataFrame
            try:
                df.unpersist()
            except:
                pass

            end_time = datetime.now(timezone.utc)

            # Update watermark
            self.watermark_manager.update_watermark(
                source_schema=table_config.source_schema,
                source_table=table_config.source_table,
                watermark_column=table_config.watermark_column,
                watermark_type=table_config.watermark_type,
                watermark_value=new_watermark,
                load_start_time=start_time,
                load_end_time=end_time,
                status=LoadStatus.SUCCESS,
                row_count=rows_written,
                pipeline_id=self.pipeline_id,
                pipeline_run_id=self.pipeline_run_id
            )

            logger.info(
                f"Successfully loaded {rows_written} rows for "
                f"{table_config.full_source_name}"
            )

            return LoadResult(
                table_config=table_config,
                status=LoadStatus.SUCCESS,
                rows_loaded=rows_written,
                start_time=start_time,
                end_time=end_time,
                previous_watermark=str(last_watermark) if last_watermark else None,
                new_watermark=str(new_watermark) if new_watermark else None
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            error_msg = str(e)

            logger.error(f"Failed to load {table_config.full_source_name}: {error_msg}")

            # Update watermark with failure
            self.watermark_manager.update_watermark(
                source_schema=table_config.source_schema,
                source_table=table_config.source_table,
                watermark_column=table_config.watermark_column,
                watermark_type=table_config.watermark_type,
                watermark_value=None,
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
                error_message=error_msg
            )

    def load_all_tables(
        self,
        registry_path: str,
        category_filter: Optional[str] = None,
        table_filter: Optional[List[str]] = None,
        force_full_load: bool = False
    ) -> List[LoadResult]:
        """
        Load all tables defined in the registry.

        Args:
            registry_path: Path to YAML/JSON registry file
            category_filter: Only load tables of this category
            table_filter: Only load these specific tables
            force_full_load: Force full load for all tables

        Returns:
            List of LoadResult for each table
        """

        # Load registry
        table_configs = self.load_registry(registry_path)

        # Apply filters
        if category_filter:
            table_configs = [t for t in table_configs if t.category == category_filter]

        if table_filter:
            table_configs = [t for t in table_configs if t.source_table in table_filter]

        # Filter enabled tables only
        table_configs = [t for t in table_configs if t.enabled]

        logger.info(f"Loading {len(table_configs)} tables from registry")

        results = []

        # Sequential loading (safer for JDBC connections)
        for config in table_configs:
            result = self.load_table(config, force_full_load=force_full_load)
            results.append(result)

        # Summary
        success_count = sum(1 for r in results if r.status == LoadStatus.SUCCESS)
        failed_count = sum(1 for r in results if r.status == LoadStatus.FAILED)
        skipped_count = sum(1 for r in results if r.status == LoadStatus.SKIPPED)
        total_rows = sum(r.rows_loaded for r in results)

        logger.info(
            f"Load complete: {success_count} success, {failed_count} failed, "
            f"{skipped_count} skipped, {total_rows} total rows"
        )

        return results

    @staticmethod
    def load_registry(registry_path: str) -> List[TableConfig]:
        """Load table configurations from registry file."""

        with open(registry_path, "r") as f:
            if registry_path.endswith(".yaml") or registry_path.endswith(".yml"):
                registry = yaml.safe_load(f)
            else:
                registry = json.load(f)

        defaults = registry.get("defaults", {})
        tables = registry.get("tables", [])

        return [
            TableConfig.from_dict(table_data, defaults)
            for table_data in tables
        ]


def create_loader_from_config(
    spark: SparkSession,
    credentials_path: str,
    target_catalog: str = "wakecap_prod",
    target_schema: str = "raw_timescaledb"
) -> TimescaleDBLoader:
    """
    Factory function to create a loader from configuration files.

    Args:
        spark: SparkSession
        credentials_path: Path to credentials YAML file
        target_catalog: Target Databricks catalog
        target_schema: Target Databricks schema

    Returns:
        Configured TimescaleDBLoader instance
    """
    credentials = TimescaleDBCredentials.from_yaml(credentials_path)

    return TimescaleDBLoader(
        spark=spark,
        credentials=credentials,
        target_catalog=target_catalog,
        target_schema=target_schema
    )
