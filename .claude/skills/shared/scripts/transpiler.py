"""
SQL transpiler for T-SQL to Databricks conversion.

Provides:
- Type mappings
- Function mappings
- SQLGlot integration
- Manual conversion helpers
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from enum import Enum
import re


class ConversionTarget(Enum):
    """Target format for converted SQL."""
    DLT_TABLE = "dlt_table"
    DLT_STREAMING = "dlt_streaming"
    SPARK_SQL = "spark_sql"
    PYTHON_NOTEBOOK = "python_notebook"
    SQL_UDF = "sql_udf"
    PYTHON_UDF = "python_udf"


@dataclass
class TranspilationResult:
    """Result of transpiling SQL code."""
    original: str
    transpiled: str
    target: ConversionTarget
    success: bool
    errors: List[str]
    warnings: List[str]
    manual_review_needed: bool = False

    @property
    def error_count(self) -> int:
        return len(self.errors)


class SQLTranspiler:
    """
    Transpile T-SQL to Databricks SQL/Python.

    Handles:
    - Data type mappings
    - Function conversions
    - SQLGlot transpilation for tables/views
    - Template generation for complex objects

    Usage:
        transpiler = SQLTranspiler()

        # Transpile a view
        result = transpiler.transpile_view(sql_code, "MyView")

        # Generate DLT table code
        code = transpiler.generate_bronze_table("Worker", ["WorkerID", "Name"])
    """

    # Data type mappings: T-SQL -> Databricks
    TYPE_MAPPINGS: Dict[str, str] = {
        # String types
        "NVARCHAR": "STRING",
        "VARCHAR": "STRING",
        "CHAR": "STRING",
        "NCHAR": "STRING",
        "TEXT": "STRING",
        "NTEXT": "STRING",
        # Numeric types
        "INT": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "BIT": "BOOLEAN",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "MONEY": "DECIMAL(19,4)",
        "SMALLMONEY": "DECIMAL(10,4)",
        "FLOAT": "DOUBLE",
        "REAL": "FLOAT",
        # Date/time types
        "DATETIME": "TIMESTAMP",
        "DATETIME2": "TIMESTAMP",
        "SMALLDATETIME": "TIMESTAMP",
        "DATE": "DATE",
        "TIME": "STRING",
        "DATETIMEOFFSET": "TIMESTAMP",
        # Binary types
        "BINARY": "BINARY",
        "VARBINARY": "BINARY",
        "IMAGE": "BINARY",
        # Other types
        "UNIQUEIDENTIFIER": "STRING",
        "XML": "STRING",
        "SQL_VARIANT": "STRING",
        "GEOGRAPHY": "STRING",
        "GEOMETRY": "STRING",
        "HIERARCHYID": "STRING",
    }

    # Function mappings: T-SQL -> Databricks
    FUNCTION_MAPPINGS: Dict[str, str] = {
        "GETDATE()": "CURRENT_TIMESTAMP()",
        "GETUTCDATE()": "CURRENT_TIMESTAMP()",
        "SYSDATETIME()": "CURRENT_TIMESTAMP()",
        "ISNULL(": "COALESCE(",
        "LEN(": "LENGTH(",
        "CHARINDEX(": "LOCATE(",
        "DATEPART(": "EXTRACT(",
        "DATEDIFF(": "DATEDIFF(",
        "DATEADD(": "DATE_ADD(",
        "CONVERT(": "CAST(",
        "NEWID()": "UUID()",
        "@@ROWCOUNT": "/* @@ROWCOUNT not supported */",
        "@@IDENTITY": "/* @@IDENTITY not supported - use IDENTITY column */",
    }

    # Patterns that require manual conversion
    MANUAL_PATTERNS = [
        (r"DECLARE\s+\w+\s+CURSOR", "CURSOR requires conversion to window functions"),
        (r"EXEC\s*\(", "Dynamic SQL requires parameterized queries"),
        (r"SP_EXECUTESQL", "Dynamic SQL requires parameterized queries"),
        (r"geography::", "Spatial functions require H3 or custom UDFs"),
        (r"geometry::", "Spatial functions require H3 or custom UDFs"),
        (r"\.STDistance\(", "Spatial distance requires Haversine UDF"),
        (r"WITH\s+\w+\s+AS\s*\([^)]+UNION\s+ALL", "Recursive CTE requires iterative approach"),
    ]

    def __init__(self):
        """Initialize transpiler."""
        self._sqlglot_available = None

    @property
    def sqlglot_available(self) -> bool:
        """Check if SQLGlot is available."""
        if self._sqlglot_available is None:
            try:
                import sqlglot
                self._sqlglot_available = True
            except ImportError:
                self._sqlglot_available = False
        return self._sqlglot_available

    def map_type(self, tsql_type: str) -> str:
        """
        Map a T-SQL data type to Databricks type.

        Handles types with size specifications like NVARCHAR(MAX), DECIMAL(18,2).
        """
        # Normalize type name
        type_upper = tsql_type.upper().strip()

        # Handle MAX types
        if "MAX" in type_upper:
            base_type = type_upper.split("(")[0]
            return self.TYPE_MAPPINGS.get(base_type, "STRING")

        # Handle sized types like NVARCHAR(50)
        match = re.match(r"(\w+)\s*\(([^)]+)\)", type_upper)
        if match:
            base_type = match.group(1)
            params = match.group(2)

            databricks_type = self.TYPE_MAPPINGS.get(base_type)
            if databricks_type:
                # DECIMAL/NUMERIC keep their precision
                if base_type in ("DECIMAL", "NUMERIC"):
                    return f"DECIMAL({params})"
                return databricks_type

        # Direct mapping
        return self.TYPE_MAPPINGS.get(type_upper, type_upper)

    def apply_function_mappings(self, sql: str) -> str:
        """Apply function mappings to SQL code."""
        result = sql
        for tsql_func, databricks_func in self.FUNCTION_MAPPINGS.items():
            # Case-insensitive replacement
            pattern = re.compile(re.escape(tsql_func), re.IGNORECASE)
            result = pattern.sub(databricks_func, result)
        return result

    def convert_brackets_to_backticks(self, sql: str) -> str:
        """Convert [identifier] to `identifier`."""
        return re.sub(r"\[(\w+)\]", r"`\1`", sql)

    def check_manual_patterns(self, sql: str) -> List[str]:
        """Check for patterns requiring manual conversion."""
        warnings = []
        for pattern, message in self.MANUAL_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE):
                warnings.append(message)
        return warnings

    def transpile_with_sqlglot(
        self,
        sql: str,
        source_dialect: str = "tsql",
        target_dialect: str = "databricks"
    ) -> Tuple[str, List[str]]:
        """
        Transpile SQL using SQLGlot.

        Returns tuple of (transpiled_sql, errors).
        """
        if not self.sqlglot_available:
            return sql, ["SQLGlot not available"]

        import sqlglot

        errors = []
        try:
            transpiled = sqlglot.transpile(
                sql,
                read=source_dialect,
                write=target_dialect,
                pretty=True
            )
            return transpiled[0] if transpiled else sql, errors
        except Exception as e:
            errors.append(f"SQLGlot error: {str(e)}")
            return sql, errors

    def transpile_view(
        self,
        sql: str,
        view_name: str
    ) -> TranspilationResult:
        """
        Transpile a T-SQL view to Databricks.

        Returns TranspilationResult with DLT table code.
        """
        errors = []
        warnings = self.check_manual_patterns(sql)

        # Apply basic transformations
        processed = self.convert_brackets_to_backticks(sql)
        processed = self.apply_function_mappings(processed)

        # Try SQLGlot
        transpiled, sqlglot_errors = self.transpile_with_sqlglot(processed)
        errors.extend(sqlglot_errors)

        # Extract SELECT portion
        select_match = re.search(
            r"AS\s+(SELECT.+)$",
            transpiled,
            re.IGNORECASE | re.DOTALL
        )
        select_sql = select_match.group(1) if select_match else transpiled

        # Generate DLT code
        function_name = self._to_snake_case(view_name)
        dlt_code = f'''@dlt.table(
    name="gold_{function_name}",
    comment="Converted from view: {view_name}"
)
def gold_{function_name}():
    return spark.sql("""
{self._indent(select_sql, 8)}
    """)
'''

        return TranspilationResult(
            original=sql,
            transpiled=dlt_code,
            target=ConversionTarget.DLT_TABLE,
            success=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            manual_review_needed=len(warnings) > 0,
        )

    def transpile_table_ddl(
        self,
        sql: str,
        table_name: str
    ) -> TranspilationResult:
        """Transpile a CREATE TABLE statement."""
        errors = []
        warnings = []

        # Apply transformations
        processed = self.convert_brackets_to_backticks(sql)

        # Convert data types
        for tsql_type, db_type in self.TYPE_MAPPINGS.items():
            pattern = re.compile(rf"\b{tsql_type}\b", re.IGNORECASE)
            processed = pattern.sub(db_type, processed)

        # Handle IDENTITY
        processed = re.sub(
            r"IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)",
            "GENERATED ALWAYS AS IDENTITY",
            processed,
            flags=re.IGNORECASE
        )

        # Try SQLGlot for final cleanup
        transpiled, sqlglot_errors = self.transpile_with_sqlglot(processed)
        errors.extend(sqlglot_errors)

        return TranspilationResult(
            original=sql,
            transpiled=transpiled,
            target=ConversionTarget.SPARK_SQL,
            success=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def generate_bronze_table(
        self,
        table_name: str,
        schema_name: str = "dbo",
        secret_scope: str = "migration_secrets"
    ) -> str:
        """
        Generate bronze layer DLT table code for JDBC ingestion.

        Args:
            table_name: Source table name
            schema_name: Source schema name
            secret_scope: Databricks secret scope name
        """
        function_name = self._to_snake_case(table_name)

        return f'''@dlt.table(
    name="bronze_{function_name}",
    comment="Raw data from [{schema_name}].[{table_name}]",
    table_properties={{"quality": "bronze"}}
)
def bronze_{function_name}():
    return (
        spark.read.format("jdbc")
        .option("url", dbutils.secrets.get("{secret_scope}", "sqlserver_jdbc_url"))
        .option("dbtable", "[{schema_name}].[{table_name}]")
        .option("user", dbutils.secrets.get("{secret_scope}", "sqlserver_user"))
        .option("password", dbutils.secrets.get("{secret_scope}", "sqlserver_password"))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )
'''

    def generate_silver_table(
        self,
        table_name: str,
        pk_column: str,
        has_soft_delete: bool = True,
        delete_column: str = "DeletedAt"
    ) -> str:
        """
        Generate silver layer DLT table code with data quality.

        Args:
            table_name: Table name (will use bronze_{name} as source)
            pk_column: Primary key column for NOT NULL expectation
            has_soft_delete: Whether to filter soft-deleted records
            delete_column: Name of soft delete column
        """
        function_name = self._to_snake_case(table_name)

        filter_line = ""
        if has_soft_delete:
            filter_line = f'\n        .filter(col("{delete_column}").isNull())'

        return f'''@dlt.table(
    name="silver_{function_name}",
    comment="Cleaned and validated {table_name}"
)
@dlt.expect_or_drop("valid_pk", "{pk_column} IS NOT NULL")
def silver_{function_name}():
    return (
        dlt.read("bronze_{function_name}"){filter_line}
        .withColumn("{function_name}_ingested_at", current_timestamp())
        .dropDuplicates(["{pk_column}"])
    )
'''

    def generate_dlt_notebook(
        self,
        tables: List[Dict],
        pipeline_name: str,
        secret_scope: str = "migration_secrets"
    ) -> str:
        """
        Generate a complete DLT pipeline notebook.

        Args:
            tables: List of dicts with 'name', 'schema', 'pk_column' keys
            pipeline_name: Name for the pipeline
            secret_scope: Databricks secret scope name

        Returns:
            Complete Python notebook content.
        """
        bronze_tables = []
        silver_tables = []

        for table in tables:
            bronze_tables.append(self.generate_bronze_table(
                table["name"],
                table.get("schema", "dbo"),
                secret_scope
            ))
            silver_tables.append(self.generate_silver_table(
                table["name"],
                table.get("pk_column", "ID"),
                table.get("has_soft_delete", True)
            ))

        return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # {pipeline_name}
# MAGIC
# MAGIC Generated DLT pipeline for SQL Server migration.
# MAGIC - Bronze layer: Raw JDBC ingestion
# MAGIC - Silver layer: Data quality and soft-delete filtering

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Ingestion

# COMMAND ----------

{self._join_cells(bronze_tables)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned Data

# COMMAND ----------

{self._join_cells(silver_tables)}
'''

    def _to_snake_case(self, name: str) -> str:
        """Convert PascalCase/camelCase to snake_case."""
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def _indent(self, text: str, spaces: int) -> str:
        """Indent text by specified spaces."""
        indent = " " * spaces
        return "\n".join(indent + line for line in text.split("\n"))

    def _join_cells(self, cells: List[str]) -> str:
        """Join code cells with Databricks cell separator."""
        return "\n\n# COMMAND ----------\n\n".join(cells)
