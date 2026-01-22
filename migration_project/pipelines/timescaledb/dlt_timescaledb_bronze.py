# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Bronze Pipeline - TimescaleDB
# MAGIC
# MAGIC Delta Live Tables pipeline for loading raw data from TimescaleDB into the Bronze layer.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - Source: TimescaleDB (PostgreSQL-based time-series database)
# MAGIC - Target: `wakecap_prod.raw_timescaledb` (Bronze layer)
# MAGIC - Pattern: Parametrized factory function with dynamic table creation
# MAGIC
# MAGIC **Tables Created:** 48 bronze tables
# MAGIC - 26 Dimension tables
# MAGIC - 11 Assignment tables
# MAGIC - 9 Fact tables (hypertables)
# MAGIC - 2 Other tables

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
import yaml
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DLT_TimescaleDB_Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Pipeline configuration
CATALOG = "wakecap_prod"
SCHEMA = "raw_timescaledb"
SECRET_SCOPE = "wakecap-timescale"
REGISTRY_PATH = "/Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables.yml"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credentials Management

# COMMAND ----------

def load_credentials():
    """
    Load TimescaleDB credentials from Databricks secret scope.

    Returns:
        dict: Connection credentials including host, port, database, user, password
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

        return {
            "host": dbutils.secrets.get(SECRET_SCOPE, "timescaledb-host"),
            "port": dbutils.secrets.get(SECRET_SCOPE, "timescaledb-port"),
            "database": dbutils.secrets.get(SECRET_SCOPE, "timescaledb-database"),
            "user": dbutils.secrets.get(SECRET_SCOPE, "timescaledb-user"),
            "password": dbutils.secrets.get(SECRET_SCOPE, "timescaledb-password")
        }
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        raise


def get_jdbc_url(creds):
    """
    Build JDBC URL for TimescaleDB connection.

    Args:
        creds: Dictionary containing host, port, database

    Returns:
        str: JDBC connection URL
    """
    return f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['database']}?sslmode=require"

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC Reader Function

# COMMAND ----------

def read_from_timescaledb(schema, table, fetch_size=10000, custom_columns=None):
    """
    Parametrized function to read a table from TimescaleDB.

    Args:
        schema: Source schema name
        table: Source table name
        fetch_size: JDBC fetch size for batching
        custom_columns: Optional dict of column name -> SQL expression for custom mappings

    Returns:
        DataFrame: Raw data with metadata columns added
    """
    creds = load_credentials()

    # Build column list
    if custom_columns:
        # Handle custom column mappings (e.g., geometry -> ST_AsText(geometry))
        column_exprs = []
        for col_name, col_expr in custom_columns.items():
            column_exprs.append(f'{col_expr} AS "{col_name}"')
        # Add remaining columns as *
        columns = ", ".join(column_exprs) + ", *"
        # Remove duplicates by excluding mapped columns from *
        exclude_cols = list(custom_columns.keys())
        query = f'''
            SELECT {columns}
            FROM "{schema}"."{table}"
        '''
    else:
        query = f'SELECT * FROM "{schema}"."{table}"'

    logger.info(f"Reading {schema}.{table} with fetch_size={fetch_size}")

    df = (spark.read
        .format("jdbc")
        .option("url", get_jdbc_url(creds))
        .option("query", query)
        .option("user", creds["user"])
        .option("password", creds["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", fetch_size)
        .option("sessionInitStatement", "SET statement_timeout = '30min'")
        .load()
    )

    # Add metadata columns
    df = (df
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("_source_system", F.lit("timescaledb"))
        .withColumn("_source_schema", F.lit(schema))
        .withColumn("_source_table", F.lit(table))
        .withColumn("_dlt_pipeline_id", F.lit(spark.conf.get("pipelines.id", "unknown")))
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Factory Function

# COMMAND ----------

def create_bronze_table(
    table_name,
    source_schema,
    source_table,
    primary_keys,
    comment,
    fetch_size=10000,
    partition_col=None,
    custom_columns=None,
    expectations=None
):
    """
    Factory function to create DLT bronze tables dynamically.

    This function creates a DLT table definition that will read from TimescaleDB
    and load into the bronze layer with proper metadata and table properties.

    Args:
        table_name: Target table name (without bronze_ prefix)
        source_schema: Source schema in TimescaleDB
        source_table: Source table in TimescaleDB
        primary_keys: List of primary key columns
        comment: Table description
        fetch_size: JDBC fetch size
        partition_col: Optional partition column for large tables
        custom_columns: Optional dict of custom column mappings
        expectations: Optional list of data quality expectations

    Returns:
        Function: DLT table loader function
    """
    # Build table properties
    table_properties = {
        "quality": "bronze",
        "source_system": "timescaledb",
        "source_schema": source_schema,
        "source_table": source_table,
        "primary_keys": ",".join(primary_keys),
        "pipelines.reset.allowed": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }

    # Define the DLT table
    @dlt.table(
        name=f"bronze_{table_name.lower()}",
        comment=comment,
        table_properties=table_properties,
        partition_cols=[partition_col] if partition_col else None
    )
    def load_table():
        """Load data from TimescaleDB source table."""
        return read_from_timescaledb(
            schema=source_schema,
            table=source_table,
            fetch_size=fetch_size,
            custom_columns=custom_columns
        )

    # Add expectations if provided
    if expectations:
        for exp in expectations:
            load_table = dlt.expect_or_drop(exp["name"], exp["constraint"])(load_table)

    return load_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Table Registry and Create DLT Tables

# COMMAND ----------

def load_table_registry(registry_path):
    """
    Load table definitions from YAML registry file.

    Args:
        registry_path: Path to the YAML registry file

    Returns:
        dict: Registry configuration with defaults and table definitions
    """
    try:
        with open(registry_path, "r") as f:
            registry = yaml.safe_load(f)
        logger.info(f"Loaded {len(registry.get('tables', []))} tables from registry")
        return registry
    except Exception as e:
        logger.error(f"Failed to load registry: {e}")
        raise

# COMMAND ----------

# Load the table registry
registry = load_table_registry(REGISTRY_PATH)
defaults = registry.get("defaults", {})
tables = registry.get("tables", [])

logger.info(f"Creating DLT tables for {len(tables)} source tables")

# COMMAND ----------

# Create DLT tables dynamically from registry
for table_config in tables:
    # Skip disabled tables
    if not table_config.get("enabled", defaults.get("enabled", True)):
        logger.info(f"Skipping disabled table: {table_config['source_table']}")
        continue

    # Merge with defaults
    source_schema = table_config.get("source_schema", "public")
    source_table = table_config["source_table"]
    primary_keys = table_config.get("primary_key_columns", [])
    comment = table_config.get("comment", f"Raw data from TimescaleDB: {source_schema}.{source_table}")
    fetch_size = table_config.get("fetch_size", defaults.get("fetch_size", 10000))
    partition_col = table_config.get("partition_column")
    custom_columns = table_config.get("custom_column_mapping")

    # Build expectations for primary key validation
    expectations = []
    if primary_keys:
        pk = primary_keys[0]
        expectations.append({
            "name": f"valid_{pk.lower()}",
            "constraint": f"`{pk}` IS NOT NULL"
        })

    # Create the DLT table
    try:
        create_bronze_table(
            table_name=source_table,
            source_schema=source_schema,
            source_table=source_table,
            primary_keys=primary_keys,
            comment=comment,
            fetch_size=fetch_size,
            partition_col=partition_col,
            custom_columns=custom_columns,
            expectations=expectations
        )
        logger.info(f"Created DLT table: bronze_{source_table.lower()}")
    except Exception as e:
        logger.error(f"Failed to create table {source_table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

# Log pipeline summary
dimension_count = sum(1 for t in tables if t.get("category") == "dimensions")
assignment_count = sum(1 for t in tables if t.get("category") == "assignments")
fact_count = sum(1 for t in tables if t.get("category") == "facts")
other_count = len(tables) - dimension_count - assignment_count - fact_count

logger.info(f"""
DLT Bronze Pipeline Summary:
============================
Total Tables: {len(tables)}
  - Dimensions: {dimension_count}
  - Assignments: {assignment_count}
  - Facts: {fact_count}
  - Other: {other_count}

Target: {CATALOG}.{SCHEMA}
Source: TimescaleDB
""")
