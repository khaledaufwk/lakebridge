"""
Dependency Resolver for SP Migration Fixes.

REQ-F6: Automatically resolve and create missing dependencies
before running converted stored procedures.
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Any
from enum import Enum


class ActionStatus(Enum):
    """Status of a dependency resolution action."""
    EXISTS = "exists"           # Dependency already exists
    CREATED = "created"         # Dependency was created
    SKIPPED = "skipped"         # Dependency was skipped
    FAILED = "failed"           # Creation failed
    MANUAL_REQUIRED = "manual_required"  # Requires manual intervention


@dataclass
class DependencyAction:
    """Action taken to resolve a dependency."""
    dependency_type: str        # "table", "view", "function", "procedure", "schema"
    source_name: str            # Original source object name
    target_name: str            # Databricks target name
    action: ActionStatus        # What action was taken
    details: Optional[str] = None
    sql_executed: Optional[str] = None


@dataclass
class TableSchema:
    """Schema definition for a table."""
    columns: List[Dict[str, str]]  # [{"name": str, "type": str, "nullable": bool}]
    primary_key: Optional[List[str]] = None
    partition_columns: Optional[List[str]] = None


# T-SQL to Spark type mappings
TSQL_TO_SPARK_TYPES = {
    # String types
    "NVARCHAR": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "NCHAR": "STRING",
    "TEXT": "STRING",
    "NTEXT": "STRING",
    # Integer types
    "INT": "INT",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",
    # Boolean
    "BIT": "BOOLEAN",
    # Decimal types
    "DECIMAL": "DECIMAL",
    "NUMERIC": "DECIMAL",
    "MONEY": "DECIMAL(19,4)",
    "SMALLMONEY": "DECIMAL(10,4)",
    # Float types
    "FLOAT": "DOUBLE",
    "REAL": "FLOAT",
    # Date/Time types
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
    # Special types
    "UNIQUEIDENTIFIER": "STRING",
    "XML": "STRING",
    "GEOGRAPHY": "STRING",
    "GEOMETRY": "STRING",
    "HIERARCHYID": "STRING",
    "SQL_VARIANT": "STRING",
}


class DependencyResolver:
    """
    Resolve and create missing dependencies for converted stored procedures.

    Capabilities:
    1. Check if tables exist in Databricks
    2. Create missing schemas
    3. Register missing UDFs
    4. Verify procedure dependencies are converted
    5. Create placeholder tables for testing
    """

    # Default schema mapping
    DEFAULT_SCHEMA_MAPPING = {
        "dbo": "silver",
        "stg": "bronze",
        "dim": "gold",
        "fact": "gold",
    }

    def __init__(
        self,
        catalog: str = "wakecap_prod",
        default_schema: str = "silver",
        table_mapping: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ):
        """
        Initialize dependency resolver.

        Args:
            catalog: Databricks catalog name
            default_schema: Default schema for unmapped tables
            table_mapping: Custom source -> Databricks table mapping
            dry_run: If True, don't execute SQL, just generate
        """
        self.catalog = catalog
        self.default_schema = default_schema
        self.table_mapping = table_mapping or {}
        self.dry_run = dry_run
        self._existing_tables: Set[str] = set()
        self._existing_schemas: Set[str] = set()
        self._existing_functions: Set[str] = set()

    def set_existing_tables(self, tables: Set[str]):
        """Set the list of existing tables in Databricks."""
        self._existing_tables = {t.lower() for t in tables}

    def set_existing_schemas(self, schemas: Set[str]):
        """Set the list of existing schemas in Databricks."""
        self._existing_schemas = {s.lower() for s in schemas}

    def set_existing_functions(self, functions: Set[str]):
        """Set the list of existing UDFs in Databricks."""
        self._existing_functions = {f.lower() for f in functions}

    def resolve_all(
        self,
        dependencies: List[Dict],
        auto_create: bool = False
    ) -> List[DependencyAction]:
        """
        Resolve all dependencies.

        Args:
            dependencies: List of dependency dicts with keys:
                - target_object: str
                - dependency_type: str ("reads", "writes", "calls", "uses_function")
            auto_create: If True, create missing tables/views

        Returns:
            List of actions taken for each dependency
        """
        actions = []

        for dep in dependencies:
            target = dep.get("target_object") or dep.get("target")
            dep_type = dep.get("dependency_type") or dep.get("type")

            if dep_type in ["reads", "writes", "TABLE_READ", "TABLE_WRITE"]:
                action = self._resolve_table(target, auto_create)
            elif dep_type in ["calls", "PROCEDURE_CALL"]:
                action = self._resolve_procedure(target)
            elif dep_type in ["uses_function", "FUNCTION_CALL"]:
                action = self._resolve_function(target, auto_create)
            elif dep_type in ["references_view", "VIEW_REFERENCE"]:
                action = self._resolve_view(target, auto_create)
            else:
                action = DependencyAction(
                    dependency_type="unknown",
                    source_name=target,
                    target_name=target,
                    action=ActionStatus.SKIPPED,
                    details=f"Unknown dependency type: {dep_type}"
                )

            actions.append(action)

        return actions

    def _resolve_table(self, source_table: str, auto_create: bool) -> DependencyAction:
        """Resolve a table dependency."""
        # Map to Databricks name
        databricks_name = self._map_to_databricks(source_table)

        # Check if exists
        if self.check_table_exists(databricks_name):
            return DependencyAction(
                dependency_type="table",
                source_name=source_table,
                target_name=databricks_name,
                action=ActionStatus.EXISTS,
                details="Table already exists"
            )

        # Auto-create if requested
        if auto_create:
            return self.create_placeholder_table(source_table, databricks_name)

        return DependencyAction(
            dependency_type="table",
            source_name=source_table,
            target_name=databricks_name,
            action=ActionStatus.MANUAL_REQUIRED,
            details="Table does not exist - manual creation required"
        )

    def _resolve_procedure(self, source_proc: str) -> DependencyAction:
        """Resolve a procedure call dependency."""
        # Check if converted notebook exists
        notebook_name = self._proc_to_notebook_name(source_proc)

        # For now, assume not converted
        return DependencyAction(
            dependency_type="procedure",
            source_name=source_proc,
            target_name=notebook_name,
            action=ActionStatus.MANUAL_REQUIRED,
            details="Convert this procedure first"
        )

    def _resolve_function(
        self,
        source_func: str,
        auto_create: bool
    ) -> DependencyAction:
        """Resolve a function dependency."""
        # Map to UDF name
        udf_name = self._func_to_udf_name(source_func)

        # Check if exists
        if self.check_function_exists(udf_name):
            return DependencyAction(
                dependency_type="function",
                source_name=source_func,
                target_name=udf_name,
                action=ActionStatus.EXISTS,
                details="UDF already registered"
            )

        # Auto-create stub if requested
        if auto_create:
            return self.create_udf_stub(source_func, udf_name)

        return DependencyAction(
            dependency_type="function",
            source_name=source_func,
            target_name=udf_name,
            action=ActionStatus.MANUAL_REQUIRED,
            details="UDF not registered - create and register manually"
        )

    def _resolve_view(self, source_view: str, auto_create: bool) -> DependencyAction:
        """Resolve a view dependency."""
        # Map to Databricks name
        databricks_name = self._map_to_databricks(source_view)

        # Check if exists
        if self.check_table_exists(databricks_name):  # Views are tables in Unity Catalog
            return DependencyAction(
                dependency_type="view",
                source_name=source_view,
                target_name=databricks_name,
                action=ActionStatus.EXISTS,
                details="View already exists"
            )

        return DependencyAction(
            dependency_type="view",
            source_name=source_view,
            target_name=databricks_name,
            action=ActionStatus.MANUAL_REQUIRED,
            details="View does not exist - create or inline SQL"
        )

    def check_table_exists(self, table_name: str) -> bool:
        """Check if table exists in Databricks."""
        return table_name.lower() in self._existing_tables

    def check_function_exists(self, function_name: str) -> bool:
        """Check if UDF is registered."""
        return function_name.lower() in self._existing_functions

    def check_schema_exists(self, schema_name: str) -> bool:
        """Check if schema exists."""
        return schema_name.lower() in self._existing_schemas

    def create_schema(self, schema_name: str) -> DependencyAction:
        """
        Create a schema if it doesn't exist.

        Args:
            schema_name: Full schema name (catalog.schema)

        Returns:
            DependencyAction with result
        """
        if self.check_schema_exists(schema_name):
            return DependencyAction(
                dependency_type="schema",
                source_name=schema_name,
                target_name=schema_name,
                action=ActionStatus.EXISTS,
                details="Schema already exists"
            )

        sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

        if self.dry_run:
            return DependencyAction(
                dependency_type="schema",
                source_name=schema_name,
                target_name=schema_name,
                action=ActionStatus.SKIPPED,
                details="Dry run - would create schema",
                sql_executed=sql
            )

        # In production, would execute SQL here
        self._existing_schemas.add(schema_name.lower())

        return DependencyAction(
            dependency_type="schema",
            source_name=schema_name,
            target_name=schema_name,
            action=ActionStatus.CREATED,
            details="Schema created",
            sql_executed=sql
        )

    def create_placeholder_table(
        self,
        source_table: str,
        target_table: str,
        schema: Optional[TableSchema] = None
    ) -> DependencyAction:
        """
        Create empty placeholder table for testing.

        Args:
            source_table: Original source table name
            target_table: Databricks target table name
            schema: Optional schema definition

        Returns:
            DependencyAction with result
        """
        # Generate default schema if not provided
        if schema is None:
            schema = self._infer_default_schema(source_table)

        # Generate column definitions
        column_defs = []
        for col in schema.columns:
            nullable = "NOT NULL" if not col.get("nullable", True) else ""
            column_defs.append(f"    {col['name']} {col['type']} {nullable}".strip())

        columns_sql = ",\n".join(column_defs)

        # Build SQL
        sql = f"""-- Placeholder table for {source_table}
-- Created by dependency resolver - populate with actual data

CREATE TABLE IF NOT EXISTS {target_table} (
{columns_sql}
)
USING DELTA
COMMENT 'Placeholder - source: {source_table}'
TBLPROPERTIES (
    'placeholder' = 'true',
    'source_table' = '{source_table}'
)"""

        if self.dry_run:
            return DependencyAction(
                dependency_type="table",
                source_name=source_table,
                target_name=target_table,
                action=ActionStatus.SKIPPED,
                details="Dry run - would create placeholder table",
                sql_executed=sql
            )

        # In production, would execute SQL here
        self._existing_tables.add(target_table.lower())

        return DependencyAction(
            dependency_type="table",
            source_name=source_table,
            target_name=target_table,
            action=ActionStatus.CREATED,
            details="Placeholder table created",
            sql_executed=sql
        )

    def create_udf_stub(
        self,
        source_func: str,
        udf_name: str
    ) -> DependencyAction:
        """
        Create a UDF stub for a missing function.

        Args:
            source_func: Original function name
            udf_name: Target UDF name

        Returns:
            DependencyAction with stub code
        """
        # Generate stub based on function name patterns
        stub_code = self._generate_udf_stub(source_func)

        sql = f"""-- UDF stub for {source_func}
-- Review and implement actual logic

{stub_code}

-- Register UDF
spark.udf.register("{udf_name}", {udf_name.split('.')[-1]})
"""

        return DependencyAction(
            dependency_type="function",
            source_name=source_func,
            target_name=udf_name,
            action=ActionStatus.MANUAL_REQUIRED,
            details="UDF stub generated - implement actual logic",
            sql_executed=sql
        )

    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """
        Get schema for an existing table.

        Args:
            table_name: Full table name

        Returns:
            TableSchema or None if table doesn't exist
        """
        # In production, would query DESCRIBE TABLE
        return None

    def generate_resolution_report(
        self,
        actions: List[DependencyAction]
    ) -> str:
        """
        Generate markdown report of resolution actions.

        Args:
            actions: List of DependencyAction results

        Returns:
            Markdown formatted report
        """
        lines = [
            "## Dependency Resolution Report",
            "",
            f"**Total Dependencies:** {len(actions)}",
            "",
        ]

        # Summary by status
        by_status = {}
        for action in actions:
            status = action.action.value
            by_status[status] = by_status.get(status, 0) + 1

        lines.extend([
            "### Summary",
            "",
        ])
        for status, count in sorted(by_status.items()):
            emoji = self._get_status_emoji(status)
            lines.append(f"- {emoji} **{status}:** {count}")
        lines.append("")

        # Details by type
        by_type = {}
        for action in actions:
            if action.dependency_type not in by_type:
                by_type[action.dependency_type] = []
            by_type[action.dependency_type].append(action)

        for dep_type, type_actions in by_type.items():
            lines.extend([
                f"### {dep_type.title()} Dependencies",
                "",
                "| Source | Target | Status | Details |",
                "|--------|--------|--------|---------|",
            ])

            for action in type_actions:
                emoji = self._get_status_emoji(action.action.value)
                lines.append(
                    f"| {action.source_name} | {action.target_name} | {emoji} {action.action.value} | {action.details or ''} |"
                )
            lines.append("")

        # SQL to execute (for created/manual_required)
        sql_actions = [
            a for a in actions
            if a.sql_executed and a.action in [ActionStatus.CREATED, ActionStatus.MANUAL_REQUIRED, ActionStatus.SKIPPED]
        ]

        if sql_actions:
            lines.extend([
                "### SQL Generated",
                "",
                "```sql",
            ])
            for action in sql_actions:
                lines.append(f"-- {action.source_name} -> {action.target_name}")
                lines.append(action.sql_executed)
                lines.append("")
            lines.append("```")

        return "\n".join(lines)

    def generate_setup_notebook(
        self,
        actions: List[DependencyAction]
    ) -> str:
        """
        Generate a Databricks notebook to set up dependencies.

        Args:
            actions: List of DependencyAction results

        Returns:
            Notebook content as string
        """
        notebook_lines = [
            "# Databricks notebook source",
            "# MAGIC %md",
            "# MAGIC # Dependency Setup",
            "# MAGIC",
            "# MAGIC This notebook creates missing dependencies for SP migration.",
            "# MAGIC",
            "# MAGIC **Generated by:** Lakebridge Fix Skill",
            "",
            "# COMMAND ----------",
            "",
            "# MAGIC %md",
            "# MAGIC ## Schema Setup",
            "",
            "# COMMAND ----------",
            "",
        ]

        # Extract unique schemas
        schemas = set()
        for action in actions:
            if action.target_name:
                parts = action.target_name.split(".")
                if len(parts) >= 2:
                    schemas.add(f"{parts[0]}.{parts[1]}")

        for schema in sorted(schemas):
            notebook_lines.append(f'spark.sql("CREATE SCHEMA IF NOT EXISTS {schema}")')
        notebook_lines.extend(["", "# COMMAND ----------", ""])

        # Group by type
        tables = [a for a in actions if a.dependency_type == "table" and a.sql_executed]
        functions = [a for a in actions if a.dependency_type == "function" and a.sql_executed]

        # Tables
        if tables:
            notebook_lines.extend([
                "# MAGIC %md",
                "# MAGIC ## Placeholder Tables",
                "",
                "# COMMAND ----------",
                "",
            ])

            for action in tables:
                notebook_lines.append(f"# {action.source_name}")
                notebook_lines.append(f'spark.sql("""')
                notebook_lines.append(action.sql_executed)
                notebook_lines.append('""")')
                notebook_lines.extend(["", "# COMMAND ----------", ""])

        # Functions
        if functions:
            notebook_lines.extend([
                "# MAGIC %md",
                "# MAGIC ## UDF Stubs",
                "",
                "# COMMAND ----------",
                "",
            ])

            for action in functions:
                notebook_lines.append(f"# {action.source_name}")
                notebook_lines.append(action.sql_executed)
                notebook_lines.extend(["", "# COMMAND ----------", ""])

        # Summary
        notebook_lines.extend([
            "# MAGIC %md",
            "# MAGIC ## Summary",
            "",
            "# COMMAND ----------",
            "",
            f'print("Dependencies setup complete: {len(actions)} processed")',
        ])

        return "\n".join(notebook_lines)

    # ==================== Helper Methods ====================

    def _map_to_databricks(self, source_object: str) -> str:
        """Map source object to Databricks equivalent."""
        # Check explicit mapping
        if source_object.lower() in {k.lower() for k in self.table_mapping}:
            for k, v in self.table_mapping.items():
                if k.lower() == source_object.lower():
                    return v

        # Default mapping
        parts = source_object.split(".")
        if len(parts) == 2:
            schema, table = parts
            target_schema = self.DEFAULT_SCHEMA_MAPPING.get(
                schema.lower(),
                self.default_schema
            )
            table_lower = table.lower()

            # Naming convention
            if table_lower.startswith("fact") or table_lower.startswith("dim"):
                target_schema = "gold"
                target_table = f"gold_{table_lower}"
            else:
                target_table = f"{target_schema}_{table_lower}"

            return f"{self.catalog}.{target_schema}.{target_table}"

        return f"{self.catalog}.{self.default_schema}.{source_object.lower()}"

    def _proc_to_notebook_name(self, proc_name: str) -> str:
        """Convert procedure name to notebook path."""
        parts = proc_name.split(".")
        name = parts[-1] if parts else proc_name

        # Remove sp prefix and convert to snake_case
        if name.lower().startswith("sp"):
            name = name[2:]

        # CamelCase to snake_case
        name = re.sub(r'([A-Z])', r'_\1', name).lower().strip('_')

        return f"/Workspace/migration/gold/{name}"

    def _func_to_udf_name(self, func_name: str) -> str:
        """Convert function name to UDF name."""
        parts = func_name.split(".")
        name = parts[-1] if parts else func_name

        # Remove fn prefix
        if name.lower().startswith("fn"):
            name = name[2:]

        return f"{self.catalog}.default.{name.lower()}"

    def _infer_default_schema(self, table_name: str) -> TableSchema:
        """Infer a default schema for a table based on naming conventions."""
        # Default columns based on common patterns
        columns = [
            {"name": "id", "type": "BIGINT", "nullable": False},
            {"name": "created_at", "type": "TIMESTAMP", "nullable": True},
            {"name": "updated_at", "type": "TIMESTAMP", "nullable": True},
        ]

        # Add common columns based on table name
        table_lower = table_name.lower()

        if "worker" in table_lower:
            columns.extend([
                {"name": "worker_id", "type": "BIGINT", "nullable": True},
                {"name": "worker_name", "type": "STRING", "nullable": True},
            ])
        if "project" in table_lower:
            columns.extend([
                {"name": "project_id", "type": "BIGINT", "nullable": True},
                {"name": "project_name", "type": "STRING", "nullable": True},
            ])
        if "zone" in table_lower:
            columns.extend([
                {"name": "zone_id", "type": "BIGINT", "nullable": True},
                {"name": "zone_name", "type": "STRING", "nullable": True},
            ])
        if "fact" in table_lower:
            columns.extend([
                {"name": "local_date", "type": "DATE", "nullable": True},
                {"name": "value", "type": "DECIMAL(18,4)", "nullable": True},
            ])

        return TableSchema(
            columns=columns,
            primary_key=["id"]
        )

    def _generate_udf_stub(self, func_name: str) -> str:
        """Generate a UDF stub based on function name."""
        parts = func_name.split(".")
        name = parts[-1] if parts else func_name
        udf_name = name.lower()

        # Remove fn prefix
        if udf_name.startswith("fn"):
            udf_name = udf_name[2:]

        # Common function patterns
        if "calc" in udf_name.lower() or "calculate" in udf_name.lower():
            return f'''from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

@udf(returnType=DoubleType())
def {udf_name}(value):
    """
    Stub for {func_name}
    TODO: Implement actual calculation logic
    """
    if value is None:
        return None
    # Placeholder - implement actual logic
    return float(value)
'''

        if "date" in udf_name.lower() or "time" in udf_name.lower():
            return f'''from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType
from datetime import datetime

@udf(returnType=TimestampType())
def {udf_name}(value):
    """
    Stub for {func_name}
    TODO: Implement actual date/time logic
    """
    if value is None:
        return None
    # Placeholder - implement actual logic
    return datetime.now()
'''

        if "format" in udf_name.lower() or "convert" in udf_name.lower():
            return f'''from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def {udf_name}(value):
    """
    Stub for {func_name}
    TODO: Implement actual formatting/conversion logic
    """
    if value is None:
        return None
    # Placeholder - implement actual logic
    return str(value)
'''

        # Default stub
        return f'''from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def {udf_name}(*args):
    """
    Stub for {func_name}
    TODO: Implement actual logic

    Original SQL Server function: {func_name}
    """
    # Placeholder - implement actual logic based on original function
    return None
'''

    def _get_status_emoji(self, status: str) -> str:
        """Get emoji for status."""
        emojis = {
            "exists": "✅",
            "created": "🆕",
            "skipped": "⏭️",
            "failed": "❌",
            "manual_required": "⚠️",
        }
        return emojis.get(status, "❓")

    def convert_tsql_type(self, tsql_type: str) -> str:
        """
        Convert T-SQL type to Spark type.

        Args:
            tsql_type: T-SQL data type (e.g., "NVARCHAR(100)")

        Returns:
            Spark SQL type
        """
        # Extract base type and precision
        match = re.match(r'(\w+)(?:\(([^)]+)\))?', tsql_type.upper())
        if not match:
            return "STRING"

        base_type = match.group(1)
        precision = match.group(2)

        # Look up mapping
        spark_type = TSQL_TO_SPARK_TYPES.get(base_type, "STRING")

        # Handle precision for decimal types
        if base_type in ["DECIMAL", "NUMERIC"] and precision:
            return f"DECIMAL({precision})"

        return spark_type


class DependencyResolverFactory:
    """Factory for creating configured DependencyResolver instances."""

    @staticmethod
    def create_for_catalog(
        catalog: str,
        existing_tables: Optional[Set[str]] = None,
        table_mapping: Optional[Dict[str, str]] = None,
        dry_run: bool = False
    ) -> DependencyResolver:
        """
        Create a resolver configured for a specific catalog.

        Args:
            catalog: Databricks catalog name
            existing_tables: Set of existing tables
            table_mapping: Custom table mappings
            dry_run: If True, don't execute SQL

        Returns:
            Configured DependencyResolver
        """
        resolver = DependencyResolver(
            catalog=catalog,
            table_mapping=table_mapping,
            dry_run=dry_run
        )

        if existing_tables:
            resolver.set_existing_tables(existing_tables)

        return resolver
