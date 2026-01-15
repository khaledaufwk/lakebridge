"""
SQL to DLT Converter - Convert transpiled SQL to DLT pipeline format.

This module provides tools for analyzing transpiled SQL and converting
it to appropriate DLT table definitions with proper dependencies.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from databricks.labs.lakebridge.dlt.generator import (
    DLTGenerator,
    DLTNotebook,
    DLTTableDefinition,
    DLTViewDefinition,
    DLTStreamDefinition,
    DLTTableType,
    DLTExpectation,
    DLTDataQuality,
)

logger = logging.getLogger(__name__)


@dataclass
class SQLObjectInfo:
    """Information extracted from a SQL file."""
    
    file_path: str
    object_name: str
    object_type: str  # TABLE, VIEW, PROCEDURE, FUNCTION
    sql: str
    dependencies: list[str] = field(default_factory=list)
    columns: list[dict[str, Any]] = field(default_factory=list)
    is_incremental: bool = False
    source_system: str | None = None
    
    @property
    def safe_name(self) -> str:
        """Get safe name for DLT function."""
        name = re.sub(r'[^a-zA-Z0-9_]', '_', self.object_name.lower())
        if name[0].isdigit():
            name = f"t_{name}"
        return name


@dataclass
class ConversionResult:
    """Result of SQL to DLT conversion."""
    
    notebook: DLTNotebook
    sql_objects: list[SQLObjectInfo]
    conversion_errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        """Check if conversion was successful."""
        return len(self.conversion_errors) == 0
    
    @property
    def table_count(self) -> int:
        """Total number of tables generated."""
        return len(self.notebook.tables) + len(self.notebook.views) + len(self.notebook.streams)


class SQLToDLTConverter:
    """Convert transpiled SQL files to DLT pipeline format.
    
    This converter analyzes SQL files, extracts table/view definitions,
    determines dependencies, and generates appropriate DLT structures.
    
    Usage:
        converter = SQLToDLTConverter(
            catalog="my_catalog",
            schema="my_schema",
        )
        
        result = converter.convert_directory(
            input_dir="transpiled_sql/",
            notebook_name="migration_pipeline",
        )
        
        if result.success:
            result.notebook.save(Path("output/dlt_pipeline.py"))
    """
    
    # SQL patterns for object detection
    CREATE_TABLE_PATTERN = re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)',
        re.IGNORECASE
    )
    CREATE_VIEW_PATTERN = re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)',
        re.IGNORECASE
    )
    CREATE_PROCEDURE_PATTERN = re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROC|PROCEDURE)\s+([^\s(]+)',
        re.IGNORECASE
    )
    INSERT_PATTERN = re.compile(
        r'INSERT\s+(?:INTO\s+)?([^\s(]+)',
        re.IGNORECASE
    )
    MERGE_PATTERN = re.compile(
        r'MERGE\s+INTO\s+([^\s]+)',
        re.IGNORECASE
    )
    FROM_PATTERN = re.compile(
        r'FROM\s+([^\s,;()]+)',
        re.IGNORECASE
    )
    JOIN_PATTERN = re.compile(
        r'JOIN\s+([^\s,;()]+)',
        re.IGNORECASE
    )
    
    def __init__(
        self,
        catalog: str | None = None,
        schema: str | None = None,
        source_system: str = "mssql",
    ):
        """Initialize the converter.
        
        Args:
            catalog: Target Unity Catalog name
            schema: Target schema name
            source_system: Source database system
        """
        self.catalog = catalog
        self.schema = schema
        self.source_system = source_system
        self.generator = DLTGenerator(catalog=catalog, schema=schema)
    
    def convert_directory(
        self,
        input_dir: str | Path,
        notebook_name: str = "migration_pipeline",
        description: str = "",
        file_pattern: str = "*.sql",
        recursive: bool = True,
    ) -> ConversionResult:
        """Convert all SQL files in a directory to DLT notebook.
        
        Args:
            input_dir: Directory containing transpiled SQL files
            notebook_name: Name for the generated notebook
            description: Description for the notebook
            file_pattern: Glob pattern for SQL files
            recursive: Whether to search recursively
            
        Returns:
            ConversionResult with notebook and metadata
        """
        input_path = Path(input_dir)
        
        if not input_path.exists():
            return ConversionResult(
                notebook=DLTNotebook(name=notebook_name),
                sql_objects=[],
                conversion_errors=[f"Input directory not found: {input_dir}"],
            )
        
        # Find SQL files
        if recursive:
            sql_files = list(input_path.rglob(file_pattern))
        else:
            sql_files = list(input_path.glob(file_pattern))
        
        if not sql_files:
            return ConversionResult(
                notebook=DLTNotebook(name=notebook_name),
                sql_objects=[],
                warnings=[f"No SQL files found matching {file_pattern}"],
            )
        
        logger.info(f"Found {len(sql_files)} SQL files to convert")
        
        # Parse all SQL files
        sql_objects: list[SQLObjectInfo] = []
        errors: list[str] = []
        warnings: list[str] = []
        
        for sql_file in sql_files:
            try:
                obj = self._parse_sql_file(sql_file)
                if obj:
                    sql_objects.append(obj)
            except Exception as e:
                errors.append(f"Failed to parse {sql_file}: {e}")
        
        # Sort by dependencies
        sorted_objects = self._topological_sort(sql_objects)
        
        # Generate DLT notebook
        notebook = DLTNotebook(
            name=notebook_name,
            catalog=self.catalog,
            schema=self.schema,
            description=description or f"DLT pipeline migrated from {self.source_system}",
        )
        
        for obj in sorted_objects:
            try:
                self._add_object_to_notebook(notebook, obj)
            except Exception as e:
                errors.append(f"Failed to convert {obj.object_name}: {e}")
        
        return ConversionResult(
            notebook=notebook,
            sql_objects=sorted_objects,
            conversion_errors=errors,
            warnings=warnings,
        )
    
    def convert_file(
        self,
        sql_file: str | Path,
    ) -> tuple[SQLObjectInfo | None, str | None]:
        """Convert a single SQL file.
        
        Args:
            sql_file: Path to SQL file
            
        Returns:
            Tuple of (SQLObjectInfo, error_message)
        """
        try:
            obj = self._parse_sql_file(Path(sql_file))
            return obj, None
        except Exception as e:
            return None, str(e)
    
    def _parse_sql_file(self, sql_file: Path) -> SQLObjectInfo | None:
        """Parse a SQL file and extract object information."""
        sql = sql_file.read_text()
        
        # Determine object type and name
        object_type, object_name = self._detect_object_type(sql, sql_file.stem)
        
        if not object_name:
            logger.warning(f"Could not determine object name for {sql_file}")
            return None
        
        # Extract dependencies
        dependencies = self._extract_dependencies(sql, object_name)
        
        # Check for incremental patterns
        is_incremental = self._is_incremental(sql)
        
        return SQLObjectInfo(
            file_path=str(sql_file),
            object_name=object_name,
            object_type=object_type,
            sql=sql,
            dependencies=dependencies,
            is_incremental=is_incremental,
            source_system=self.source_system,
        )
    
    def _detect_object_type(
        self,
        sql: str,
        default_name: str,
    ) -> tuple[str, str]:
        """Detect the SQL object type and extract name."""
        # Check for CREATE TABLE
        match = self.CREATE_TABLE_PATTERN.search(sql)
        if match:
            return "TABLE", self._clean_object_name(match.group(1))
        
        # Check for CREATE VIEW
        match = self.CREATE_VIEW_PATTERN.search(sql)
        if match:
            return "VIEW", self._clean_object_name(match.group(1))
        
        # Check for CREATE PROCEDURE
        match = self.CREATE_PROCEDURE_PATTERN.search(sql)
        if match:
            return "PROCEDURE", self._clean_object_name(match.group(1))
        
        # Check for INSERT
        match = self.INSERT_PATTERN.search(sql)
        if match:
            return "TABLE", self._clean_object_name(match.group(1))
        
        # Check for MERGE
        match = self.MERGE_PATTERN.search(sql)
        if match:
            return "TABLE", self._clean_object_name(match.group(1))
        
        # Default: use filename as table name
        return "TABLE", default_name
    
    def _clean_object_name(self, name: str) -> str:
        """Clean up object name (remove schema prefix, brackets, etc.)."""
        # Remove brackets
        name = name.strip('[]"\'`')
        
        # Remove schema prefix
        if '.' in name:
            name = name.split('.')[-1]
        
        return name
    
    def _extract_dependencies(
        self,
        sql: str,
        object_name: str,
    ) -> list[str]:
        """Extract table dependencies from SQL."""
        dependencies = set()
        
        # Find FROM tables
        for match in self.FROM_PATTERN.finditer(sql):
            dep = self._clean_object_name(match.group(1))
            if dep.lower() != object_name.lower():
                dependencies.add(dep)
        
        # Find JOIN tables
        for match in self.JOIN_PATTERN.finditer(sql):
            dep = self._clean_object_name(match.group(1))
            if dep.lower() != object_name.lower():
                dependencies.add(dep)
        
        # Remove common non-table references
        non_tables = {'dual', 'information_schema', 'sys', 'master', 'tempdb'}
        dependencies = {d for d in dependencies if d.lower() not in non_tables}
        
        return list(dependencies)
    
    def _is_incremental(self, sql: str) -> bool:
        """Check if SQL represents incremental/streaming logic."""
        sql_upper = sql.upper()
        
        incremental_patterns = [
            'MERGE INTO',
            'WHEN MATCHED',
            'WHEN NOT MATCHED',
            'CDC',
            'CHANGE_DATA_CAPTURE',
            'STREAMING',
            'INCREMENTAL',
            'WATERMARK',
        ]
        
        return any(pattern in sql_upper for pattern in incremental_patterns)
    
    def _topological_sort(
        self,
        objects: list[SQLObjectInfo],
    ) -> list[SQLObjectInfo]:
        """Sort objects by dependency order."""
        # Build dependency graph
        name_to_obj = {obj.object_name.lower(): obj for obj in objects}
        
        # Simple topological sort
        sorted_objs: list[SQLObjectInfo] = []
        visited: set[str] = set()
        
        def visit(obj: SQLObjectInfo) -> None:
            name = obj.object_name.lower()
            if name in visited:
                return
            
            visited.add(name)
            
            # Visit dependencies first
            for dep in obj.dependencies:
                dep_lower = dep.lower()
                if dep_lower in name_to_obj and dep_lower not in visited:
                    visit(name_to_obj[dep_lower])
            
            sorted_objs.append(obj)
        
        for obj in objects:
            visit(obj)
        
        return sorted_objs
    
    def _add_object_to_notebook(
        self,
        notebook: DLTNotebook,
        obj: SQLObjectInfo,
    ) -> None:
        """Add a SQL object to the DLT notebook."""
        comment = f"Migrated from {self.source_system}: {obj.file_path}"
        
        if obj.object_type == "VIEW":
            notebook.views.append(DLTViewDefinition(
                name=obj.object_name,
                sql=self._clean_sql_for_dlt(obj.sql),
                comment=comment,
                source_file=obj.file_path,
            ))
        
        elif obj.is_incremental:
            notebook.streams.append(DLTStreamDefinition(
                name=obj.object_name,
                sql=self._clean_sql_for_dlt(obj.sql),
                comment=comment,
                source_file=obj.file_path,
            ))
        
        else:
            notebook.tables.append(DLTTableDefinition(
                name=obj.object_name,
                sql=self._clean_sql_for_dlt(obj.sql),
                table_type=DLTTableType.MATERIALIZED,
                comment=comment,
                source_file=obj.file_path,
            ))
    
    def _clean_sql_for_dlt(self, sql: str) -> str:
        """Clean SQL for use in DLT.
        
        Removes CREATE statements and extracts just the query.
        """
        # Remove CREATE TABLE/VIEW statements, keep just the SELECT
        patterns_to_remove = [
            r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^\s(]+\s*(?:\([^)]*\))?\s*AS\s*',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?[^\s(]+\s*AS\s*',
            r'INSERT\s+(?:INTO\s+)?[^\s(]+\s*(?:\([^)]*\))?\s*',
        ]
        
        cleaned = sql
        for pattern in patterns_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        # Trim and clean up
        cleaned = cleaned.strip()
        if cleaned.endswith(';'):
            cleaned = cleaned[:-1]
        
        # If nothing left, use original
        if not cleaned.strip():
            cleaned = sql
        
        return cleaned
    
    def add_data_quality_from_source(
        self,
        notebook: DLTNotebook,
        schema_info: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Add data quality expectations based on source schema info.
        
        Args:
            notebook: DLT notebook to enhance
            schema_info: Dict mapping table names to column info
        """
        for table in notebook.tables:
            table_schema = schema_info.get(table.name, [])
            
            for col_info in table_schema:
                col_name = col_info.get("name")
                is_nullable = col_info.get("nullable", True)
                
                if not is_nullable and col_name:
                    table.expectations.append(DLTExpectation(
                        name=f"{table.name}_{col_name}_not_null",
                        constraint=f"{col_name} IS NOT NULL",
                        action=DLTDataQuality.DROP,
                    ))
