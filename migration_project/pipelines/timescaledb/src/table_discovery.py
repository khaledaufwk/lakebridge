# TimescaleDB Table Discovery Utility
# ===================================
# Automatically discovers tables, primary keys, and watermark columns
# from a TimescaleDB database and generates a registry YAML file.

import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class WatermarkType(Enum):
    """Supported watermark column types."""
    TIMESTAMP = "timestamp"
    BIGINT = "bigint"
    DATE = "date"
    INTEGER = "integer"


@dataclass
class DiscoveredTable:
    """Represents a discovered table with its metadata."""
    schema_name: str
    table_name: str
    primary_key_columns: List[str] = field(default_factory=list)
    watermark_column: Optional[str] = None
    watermark_type: Optional[str] = None
    is_hypertable: bool = False
    row_count_estimate: int = 0
    columns: List[Dict[str, Any]] = field(default_factory=list)
    has_geometry: bool = False


class TimescaleDBDiscovery:
    """
    Discovers tables and their metadata from TimescaleDB.

    This utility connects to TimescaleDB and:
    1. Lists all tables in specified schemas
    2. Identifies primary keys for each table
    3. Suggests watermark columns based on naming conventions
    4. Detects TimescaleDB hypertables
    5. Generates a registry YAML file
    """

    # Common watermark column patterns (ordered by priority)
    WATERMARK_PATTERNS = [
        ("WatermarkUTC", WatermarkType.TIMESTAMP),
        ("watermark_utc", WatermarkType.TIMESTAMP),
        ("watermark", WatermarkType.TIMESTAMP),
        ("modified_at", WatermarkType.TIMESTAMP),
        ("updated_at", WatermarkType.TIMESTAMP),
        ("ModifiedAt", WatermarkType.TIMESTAMP),
        ("UpdatedAt", WatermarkType.TIMESTAMP),
        ("created_at", WatermarkType.TIMESTAMP),
        ("CreatedAt", WatermarkType.TIMESTAMP),
        ("last_modified", WatermarkType.TIMESTAMP),
        ("LastModified", WatermarkType.TIMESTAMP),
        ("timestamp", WatermarkType.TIMESTAMP),
        ("Timestamp", WatermarkType.TIMESTAMP),
        ("event_time", WatermarkType.TIMESTAMP),
        ("EventTime", WatermarkType.TIMESTAMP),
    ]

    # Timestamp column types in PostgreSQL
    TIMESTAMP_TYPES = [
        "timestamp without time zone",
        "timestamp with time zone",
        "timestamptz",
        "timestamp",
    ]

    def __init__(self, spark, credentials: Dict[str, Any]):
        """
        Initialize the discovery utility.

        Args:
            spark: SparkSession instance
            credentials: Dict with host, port, database, user, password, sslmode
        """
        self.spark = spark
        self.credentials = credentials
        self._jdbc_url = self._build_jdbc_url()

    def _build_jdbc_url(self) -> str:
        """Build JDBC URL from credentials."""
        host = self.credentials["host"]
        port = self.credentials.get("port", 5432)
        database = self.credentials["database"]
        sslmode = self.credentials.get("sslmode", "require")
        return f"jdbc:postgresql://{host}:{port}/{database}?sslmode={sslmode}"

    def _execute_query(self, query: str):
        """Execute a query against TimescaleDB and return DataFrame."""
        return (
            self.spark.read
            .format("jdbc")
            .option("url", self._jdbc_url)
            .option("query", query)
            .option("user", self.credentials["user"])
            .option("password", self.credentials["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def discover_tables(
        self,
        schemas: List[str] = None,
        exclude_patterns: List[str] = None
    ) -> List[DiscoveredTable]:
        """
        Discover all tables in the specified schemas.

        Args:
            schemas: List of schema names to scan (default: ["public"])
            exclude_patterns: List of table name patterns to exclude

        Returns:
            List of DiscoveredTable objects
        """
        schemas = schemas or ["public"]
        exclude_patterns = exclude_patterns or ["_timescaledb%", "pg_%", "sql_%"]

        # Build exclusion clause
        exclude_clause = " AND ".join([
            f"table_name NOT LIKE '{p}'" for p in exclude_patterns
        ])

        # Query to list tables
        schema_list = ", ".join([f"'{s}'" for s in schemas])
        query = f"""
        SELECT
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema IN ({schema_list})
          AND table_type = 'BASE TABLE'
          AND {exclude_clause}
        ORDER BY table_schema, table_name
        """

        df = self._execute_query(query)
        tables_list = df.collect()

        discovered = []
        for row in tables_list:
            table = DiscoveredTable(
                schema_name=row.table_schema,
                table_name=row.table_name
            )
            discovered.append(table)

        logger.info(f"Discovered {len(discovered)} tables")
        return discovered

    def get_table_columns(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        df = self._execute_query(query)
        columns = []
        for row in df.collect():
            columns.append({
                "name": row.column_name,
                "data_type": row.data_type,
                "is_nullable": row.is_nullable == "YES",
                "default": row.column_default,
                "position": row.ordinal_position
            })
        return columns

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        query = f"""
        SELECT
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = '{schema_name}'
          AND tc.table_name = '{table_name}'
        ORDER BY kcu.ordinal_position
        """

        df = self._execute_query(query)
        return [row.column_name for row in df.collect()]

    def detect_watermark_column(
        self,
        columns: List[Dict[str, Any]]
    ) -> tuple:
        """
        Detect the best watermark column based on naming conventions.

        Args:
            columns: List of column metadata dicts

        Returns:
            Tuple of (column_name, watermark_type) or (None, None)
        """
        column_names = {c["name"]: c["data_type"] for c in columns}

        # Check patterns in order of priority
        for pattern, wm_type in self.WATERMARK_PATTERNS:
            if pattern in column_names:
                return pattern, wm_type.value

        # Fallback: look for any timestamp column
        for col_name, data_type in column_names.items():
            if data_type.lower() in [t.lower() for t in self.TIMESTAMP_TYPES]:
                # Prefer columns with common timestamp names
                if any(kw in col_name.lower() for kw in ["time", "date", "at", "utc"]):
                    return col_name, WatermarkType.TIMESTAMP.value

        return None, None

    def check_hypertable(self, schema_name: str, table_name: str) -> bool:
        """Check if a table is a TimescaleDB hypertable."""
        query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM _timescaledb_catalog.hypertable ht
            JOIN pg_class c ON ht.table_name = c.relname
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = '{schema_name}'
              AND c.relname = '{table_name}'
        ) as is_hypertable
        """
        try:
            df = self._execute_query(query)
            row = df.first()
            return row.is_hypertable if row else False
        except Exception:
            # TimescaleDB catalog may not be accessible
            return False

    def get_row_count_estimate(self, schema_name: str, table_name: str) -> int:
        """Get estimated row count using pg_stat statistics."""
        query = f"""
        SELECT reltuples::bigint as estimate
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = '{schema_name}'
          AND c.relname = '{table_name}'
        """
        try:
            df = self._execute_query(query)
            row = df.first()
            return int(row.estimate) if row and row.estimate > 0 else 0
        except Exception:
            return 0

    def enrich_table_metadata(self, table: DiscoveredTable) -> DiscoveredTable:
        """
        Enrich a discovered table with full metadata.

        Args:
            table: DiscoveredTable with basic info

        Returns:
            DiscoveredTable with all metadata populated
        """
        # Get columns
        table.columns = self.get_table_columns(table.schema_name, table.table_name)

        # Get primary keys
        table.primary_key_columns = self.get_primary_keys(
            table.schema_name, table.table_name
        )

        # Detect watermark column
        wm_col, wm_type = self.detect_watermark_column(table.columns)
        table.watermark_column = wm_col
        table.watermark_type = wm_type

        # Check if hypertable
        table.is_hypertable = self.check_hypertable(
            table.schema_name, table.table_name
        )

        # Get row count estimate
        table.row_count_estimate = self.get_row_count_estimate(
            table.schema_name, table.table_name
        )

        # Check for geometry columns
        table.has_geometry = any(
            c["data_type"] == "USER-DEFINED" or "geometry" in c["data_type"].lower()
            for c in table.columns
        )

        logger.info(
            f"Enriched {table.schema_name}.{table.table_name}: "
            f"PKs={table.primary_key_columns}, "
            f"Watermark={table.watermark_column}, "
            f"Hypertable={table.is_hypertable}"
        )

        return table

    def discover_all(
        self,
        schemas: List[str] = None,
        exclude_patterns: List[str] = None
    ) -> List[DiscoveredTable]:
        """
        Discover and enrich all tables.

        Args:
            schemas: Schemas to scan
            exclude_patterns: Patterns to exclude

        Returns:
            List of fully enriched DiscoveredTable objects
        """
        tables = self.discover_tables(schemas, exclude_patterns)

        enriched = []
        for table in tables:
            try:
                enriched_table = self.enrich_table_metadata(table)
                enriched.append(enriched_table)
            except Exception as e:
                logger.error(
                    f"Failed to enrich {table.schema_name}.{table.table_name}: {e}"
                )
                enriched.append(table)

        return enriched

    def categorize_table(self, table: DiscoveredTable) -> str:
        """Categorize a table based on naming conventions."""
        name_lower = table.table_name.lower()

        if name_lower.startswith("fact"):
            return "facts"
        elif name_lower.startswith("dim"):
            return "dimensions"
        elif "assignment" in name_lower:
            return "assignments"
        elif name_lower in ["organization", "project", "worker", "crew", "trade",
                           "company", "floor", "zone", "device", "workshift"]:
            return "dimensions"
        elif any(kw in name_lower for kw in ["history", "event", "log", "audit"]):
            return "facts"
        else:
            return "other"

    def generate_registry_yaml(
        self,
        tables: List[DiscoveredTable],
        target_catalog: str = "wakecap_prod",
        target_schema: str = "raw_timescaledb",
        output_path: str = None
    ) -> str:
        """
        Generate a registry YAML from discovered tables.

        Args:
            tables: List of enriched DiscoveredTable objects
            target_catalog: Target Databricks catalog
            target_schema: Target schema
            output_path: Optional path to write YAML file

        Returns:
            YAML string
        """
        # Build registry structure
        registry = {
            "registry_version": "1.0",
            "source_system": "timescaledb",
            "target_catalog": target_catalog,
            "target_schema": target_schema,
            "generated_at": None,  # Will be set at runtime
            "defaults": {
                "watermark_column": "WatermarkUTC",
                "watermark_type": "timestamp",
                "is_full_load": False,
                "fetch_size": 10000
            },
            "tables": []
        }

        # Process each table
        for table in tables:
            table_config = {
                "source_schema": table.schema_name,
                "source_table": table.table_name,
                "primary_key_columns": table.primary_key_columns or ["id"],
                "watermark_column": table.watermark_column or "created_at",
                "watermark_type": table.watermark_type or "timestamp",
                "category": self.categorize_table(table),
            }

            # Add optional fields
            if table.is_hypertable:
                table_config["is_hypertable"] = True
                table_config["fetch_size"] = 50000

            if table.has_geometry:
                table_config["has_geometry"] = True

            if table.row_count_estimate > 1000000:
                table_config["is_large_table"] = True
                table_config["fetch_size"] = 100000

            # Add partition column for facts
            if table_config["category"] == "facts":
                # Try to find a date column for partitioning
                date_cols = [
                    c["name"] for c in table.columns
                    if "date" in c["name"].lower() and c["data_type"].lower() == "date"
                ]
                if date_cols:
                    table_config["partition_column"] = date_cols[0]

            registry["tables"].append(table_config)

        # Sort tables by category then name
        registry["tables"].sort(
            key=lambda t: (t["category"], t["source_table"])
        )

        # Generate YAML
        yaml_content = yaml.dump(registry, default_flow_style=False, sort_keys=False)

        # Write to file if path provided
        if output_path:
            with open(output_path, "w") as f:
                f.write(yaml_content)
            logger.info(f"Registry written to {output_path}")

        return yaml_content


def discover_and_generate_registry(
    spark,
    credentials: Dict[str, Any],
    schemas: List[str] = None,
    target_catalog: str = "wakecap_prod",
    target_schema: str = "raw_timescaledb",
    output_path: str = None
) -> str:
    """
    Convenience function to discover tables and generate registry.

    Args:
        spark: SparkSession
        credentials: TimescaleDB credentials dict
        schemas: Schemas to scan
        target_catalog: Target Databricks catalog
        target_schema: Target schema
        output_path: Optional path to write YAML

    Returns:
        YAML string
    """
    discovery = TimescaleDBDiscovery(spark, credentials)
    tables = discovery.discover_all(schemas)
    return discovery.generate_registry_yaml(
        tables,
        target_catalog=target_catalog,
        target_schema=target_schema,
        output_path=output_path
    )
