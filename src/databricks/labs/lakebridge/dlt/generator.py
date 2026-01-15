"""
DLT Generator - Generate Delta Live Tables notebooks and definitions.

This module provides tools for generating DLT notebook content from
transpiled SQL code.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any


class DLTTableType(str, Enum):
    """Types of DLT tables."""
    
    STREAMING = "streaming"
    MATERIALIZED = "materialized"
    VIEW = "view"


class DLTDataQuality(str, Enum):
    """Data quality expectation actions."""
    
    WARN = "warn"
    DROP = "drop"
    FAIL = "fail"


@dataclass
class DLTExpectation:
    """Data quality expectation for a DLT table."""
    
    name: str
    constraint: str
    action: DLTDataQuality = DLTDataQuality.WARN
    
    def to_decorator(self) -> str:
        """Generate the @dlt.expect decorator."""
        if self.action == DLTDataQuality.WARN:
            return f'@dlt.expect("{self.name}", "{self.constraint}")'
        elif self.action == DLTDataQuality.DROP:
            return f'@dlt.expect_or_drop("{self.name}", "{self.constraint}")'
        else:
            return f'@dlt.expect_or_fail("{self.name}", "{self.constraint}")'


@dataclass
class DLTTableDefinition:
    """Definition for a DLT table."""
    
    name: str
    sql: str
    comment: str = ""
    table_type: DLTTableType = DLTTableType.MATERIALIZED
    expectations: list[DLTExpectation] = field(default_factory=list)
    partition_cols: list[str] = field(default_factory=list)
    cluster_cols: list[str] = field(default_factory=list)
    table_properties: dict[str, str] = field(default_factory=dict)
    source_file: str | None = None
    
    def to_python(self) -> str:
        """Generate Python code for this table definition."""
        lines = []
        
        # Add expectations as decorators
        for exp in self.expectations:
            lines.append(exp.to_decorator())
        
        # Build table decorator
        decorator_parts = [f'name="{self.name}"']
        if self.comment:
            decorator_parts.append(f'comment="{self.comment}"')
        if self.partition_cols:
            decorator_parts.append(f'partition_cols={self.partition_cols}')
        if self.table_properties:
            props_str = ", ".join(f'"{k}": "{v}"' for k, v in self.table_properties.items())
            decorator_parts.append(f'table_properties={{{props_str}}}')
        
        decorator_args = ", ".join(decorator_parts)
        
        if self.table_type == DLTTableType.STREAMING:
            lines.append(f"@dlt.table({decorator_args})")
            lines.append(f"def {self._safe_name()}():")
            lines.append(f'    """Streaming table: {self.name}"""')
            lines.append(f"    return spark.readStream.format('delta').table('{self._extract_source()}')")
        elif self.table_type == DLTTableType.VIEW:
            lines.append(f"@dlt.view({decorator_args})")
            lines.append(f"def {self._safe_name()}():")
            lines.append(f'    """View: {self.name}"""')
            lines.append(f'    return spark.sql("""{self.sql}""")')
        else:
            lines.append(f"@dlt.table({decorator_args})")
            lines.append(f"def {self._safe_name()}():")
            lines.append(f'    """Materialized table: {self.name}"""')
            lines.append(f'    return spark.sql("""{self.sql}""")')
        
        return "\n".join(lines)
    
    def _safe_name(self) -> str:
        """Convert table name to valid Python function name."""
        name = re.sub(r'[^a-zA-Z0-9_]', '_', self.name.lower())
        if name[0].isdigit():
            name = f"t_{name}"
        return name
    
    def _extract_source(self) -> str:
        """Extract source table from SQL for streaming tables."""
        match = re.search(r'FROM\s+([^\s,;]+)', self.sql, re.IGNORECASE)
        if match:
            return match.group(1)
        return self.name


@dataclass
class DLTViewDefinition(DLTTableDefinition):
    """Definition for a DLT view."""
    
    table_type: DLTTableType = DLTTableType.VIEW


@dataclass
class DLTStreamDefinition(DLTTableDefinition):
    """Definition for a DLT streaming table."""
    
    table_type: DLTTableType = DLTTableType.STREAMING
    source_format: str = "delta"
    source_path: str | None = None


@dataclass
class DLTNotebook:
    """Complete DLT notebook structure."""
    
    name: str
    tables: list[DLTTableDefinition] = field(default_factory=list)
    views: list[DLTViewDefinition] = field(default_factory=list)
    streams: list[DLTStreamDefinition] = field(default_factory=list)
    imports: list[str] = field(default_factory=list)
    catalog: str | None = None
    schema: str | None = None
    description: str = ""
    
    def to_python(self) -> str:
        """Generate complete Python notebook content."""
        lines = []
        
        # Header
        lines.append("# Databricks notebook source")
        lines.append(f"# MAGIC %md")
        lines.append(f"# MAGIC # {self.name}")
        lines.append(f"# MAGIC ")
        lines.append(f"# MAGIC {self.description or 'DLT Pipeline generated by Lakebridge'}")
        lines.append(f"# MAGIC ")
        lines.append(f"# MAGIC Generated: {datetime.now().isoformat()}")
        lines.append("")
        
        # COMMAND separator
        lines.append("# COMMAND ----------")
        lines.append("")
        
        # Imports
        lines.append("import dlt")
        lines.append("from pyspark.sql import functions as F")
        for imp in self.imports:
            lines.append(imp)
        lines.append("")
        
        # Catalog/Schema setup
        if self.catalog:
            lines.append("# COMMAND ----------")
            lines.append("")
            lines.append(f'# Target catalog: {self.catalog}')
            if self.schema:
                lines.append(f'# Target schema: {self.schema}')
            lines.append("")
        
        # Tables
        for table in self.tables:
            lines.append("# COMMAND ----------")
            lines.append("")
            if table.source_file:
                lines.append(f"# Source: {table.source_file}")
            lines.append(table.to_python())
            lines.append("")
        
        # Views
        for view in self.views:
            lines.append("# COMMAND ----------")
            lines.append("")
            if view.source_file:
                lines.append(f"# Source: {view.source_file}")
            lines.append(view.to_python())
            lines.append("")
        
        # Streams
        for stream in self.streams:
            lines.append("# COMMAND ----------")
            lines.append("")
            if stream.source_file:
                lines.append(f"# Source: {stream.source_file}")
            lines.append(stream.to_python())
            lines.append("")
        
        return "\n".join(lines)
    
    def save(self, path: Path) -> None:
        """Save notebook to file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_python())


class DLTGenerator:
    """Generator for DLT notebooks from transpiled SQL.
    
    Usage:
        generator = DLTGenerator(catalog="my_catalog", schema="my_schema")
        notebook = generator.from_sql_files(["path/to/file1.sql", "path/to/file2.sql"])
        notebook.save(Path("output/dlt_pipeline.py"))
    """
    
    def __init__(
        self,
        catalog: str | None = None,
        schema: str | None = None,
        default_table_type: DLTTableType = DLTTableType.MATERIALIZED,
    ):
        """Initialize the DLT generator.
        
        Args:
            catalog: Target Unity Catalog name
            schema: Target schema name
            default_table_type: Default table type for generated tables
        """
        self.catalog = catalog
        self.schema = schema
        self.default_table_type = default_table_type
    
    def from_sql_files(
        self,
        sql_files: list[str | Path],
        notebook_name: str = "migration_pipeline",
        description: str = "",
    ) -> DLTNotebook:
        """Generate DLT notebook from SQL files.
        
        Args:
            sql_files: List of paths to transpiled SQL files
            notebook_name: Name for the generated notebook
            description: Description for the notebook
            
        Returns:
            DLTNotebook ready to be saved or deployed
        """
        notebook = DLTNotebook(
            name=notebook_name,
            catalog=self.catalog,
            schema=self.schema,
            description=description,
        )
        
        for sql_file in sql_files:
            path = Path(sql_file)
            if not path.exists():
                continue
            
            sql_content = path.read_text()
            table_name = path.stem
            
            # Determine table type based on SQL content
            table_type = self._infer_table_type(sql_content)
            
            if table_type == DLTTableType.VIEW:
                notebook.views.append(DLTViewDefinition(
                    name=table_name,
                    sql=sql_content,
                    comment=f"Migrated from {path.name}",
                    source_file=str(path),
                ))
            elif table_type == DLTTableType.STREAMING:
                notebook.streams.append(DLTStreamDefinition(
                    name=table_name,
                    sql=sql_content,
                    comment=f"Migrated from {path.name}",
                    source_file=str(path),
                ))
            else:
                notebook.tables.append(DLTTableDefinition(
                    name=table_name,
                    sql=sql_content,
                    table_type=table_type,
                    comment=f"Migrated from {path.name}",
                    source_file=str(path),
                ))
        
        return notebook
    
    def from_sql_string(
        self,
        sql: str,
        table_name: str,
        table_type: DLTTableType | None = None,
        comment: str = "",
    ) -> DLTTableDefinition:
        """Generate DLT table definition from SQL string.
        
        Args:
            sql: SQL query
            table_name: Name for the table
            table_type: Type of table (inferred if not provided)
            comment: Optional comment
            
        Returns:
            DLTTableDefinition
        """
        if table_type is None:
            table_type = self._infer_table_type(sql)
        
        return DLTTableDefinition(
            name=table_name,
            sql=sql,
            table_type=table_type,
            comment=comment,
        )
    
    def _infer_table_type(self, sql: str) -> DLTTableType:
        """Infer table type from SQL content."""
        sql_upper = sql.upper()
        
        # Check for view indicators
        if "CREATE VIEW" in sql_upper or "CREATE OR REPLACE VIEW" in sql_upper:
            return DLTTableType.VIEW
        
        # Check for streaming indicators
        if "STREAM" in sql_upper or "STREAMING" in sql_upper:
            return DLTTableType.STREAMING
        
        # Check for incremental/CDC patterns
        if "MERGE INTO" in sql_upper or "CDC" in sql_upper:
            return DLTTableType.STREAMING
        
        return self.default_table_type
    
    def add_data_quality(
        self,
        table: DLTTableDefinition,
        expectations: list[tuple[str, str, DLTDataQuality]],
    ) -> DLTTableDefinition:
        """Add data quality expectations to a table.
        
        Args:
            table: Table definition to modify
            expectations: List of (name, constraint, action) tuples
            
        Returns:
            Modified table definition
        """
        for name, constraint, action in expectations:
            table.expectations.append(DLTExpectation(
                name=name,
                constraint=constraint,
                action=action,
            ))
        return table
    
    def generate_expectations_from_schema(
        self,
        table_name: str,
        not_null_columns: list[str] | None = None,
        unique_columns: list[str] | None = None,
        range_constraints: dict[str, tuple[Any, Any]] | None = None,
    ) -> list[DLTExpectation]:
        """Generate common data quality expectations from schema info.
        
        Args:
            table_name: Table name for naming expectations
            not_null_columns: Columns that should not be null
            unique_columns: Columns that should be unique
            range_constraints: Column -> (min, max) constraints
            
        Returns:
            List of DLTExpectation objects
        """
        expectations = []
        
        if not_null_columns:
            for col in not_null_columns:
                expectations.append(DLTExpectation(
                    name=f"{table_name}_{col}_not_null",
                    constraint=f"{col} IS NOT NULL",
                    action=DLTDataQuality.DROP,
                ))
        
        if range_constraints:
            for col, (min_val, max_val) in range_constraints.items():
                expectations.append(DLTExpectation(
                    name=f"{table_name}_{col}_range",
                    constraint=f"{col} BETWEEN {min_val} AND {max_val}",
                    action=DLTDataQuality.WARN,
                ))
        
        return expectations
