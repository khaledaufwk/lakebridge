"""
Data Type Validator for SP Migration Review.

REQ-R6: Validate that T-SQL data types are correctly mapped to
Spark/Databricks types, detecting precision loss and special handling needs.
"""

import re
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from enum import Enum


class ValidationSeverity(Enum):
    """Severity of data type validation issues."""
    ERROR = "error"         # Incorrect mapping, will cause failures
    WARNING = "warning"     # Potential precision loss or data issues
    INFO = "info"           # FYI, may need special handling


@dataclass
class TypeValidationIssue:
    """A data type validation issue."""
    column_name: str
    source_type: str
    target_type: str
    severity: ValidationSeverity
    message: str
    recommendation: Optional[str] = None


@dataclass
class ColumnTypeInfo:
    """Information about a column's data type."""
    name: str
    base_type: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    max_length: Optional[int] = None
    is_nullable: bool = True
    raw_type: str = ""


# Comprehensive T-SQL to Spark type mappings
TSQL_TO_SPARK_MAPPINGS = {
    # String types
    "NVARCHAR": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "NCHAR": "STRING",
    "TEXT": "STRING",
    "NTEXT": "STRING",
    "SYSNAME": "STRING",

    # Integer types
    "INT": "INT",
    "INTEGER": "INT",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",

    # Boolean
    "BIT": "BOOLEAN",

    # Decimal/Numeric types
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
    "TIME": "STRING",  # Spark doesn't have native TIME
    "DATETIMEOFFSET": "TIMESTAMP",  # Loses timezone info

    # Binary types
    "BINARY": "BINARY",
    "VARBINARY": "BINARY",
    "IMAGE": "BINARY",

    # Special types
    "UNIQUEIDENTIFIER": "STRING",
    "XML": "STRING",
    "GEOGRAPHY": "STRING",  # WKT format
    "GEOMETRY": "STRING",   # WKT format
    "HIERARCHYID": "STRING",
    "SQL_VARIANT": "STRING",
    "TIMESTAMP": "BINARY",  # SQL Server TIMESTAMP is rowversion, not datetime
    "ROWVERSION": "BINARY",
}

# Types that may lose precision
PRECISION_LOSS_TYPES = {
    "MONEY": ("DECIMAL(19,4)", "Full precision preserved with DECIMAL(19,4)"),
    "SMALLMONEY": ("DECIMAL(10,4)", "Full precision preserved with DECIMAL(10,4)"),
    "DATETIME": ("TIMESTAMP", "Precision reduced from 3.33ms to 1ms"),
    "DATETIME2": ("TIMESTAMP", "Precision may be reduced depending on DATETIME2 precision"),
    "DATETIMEOFFSET": ("TIMESTAMP", "Timezone information is lost"),
    "TIME": ("STRING", "No native TIME type in Spark, stored as string HH:mm:ss.SSS"),
    "FLOAT": ("DOUBLE", "Consider if DOUBLE precision is sufficient"),
    "REAL": ("FLOAT", "Consider if FLOAT precision is sufficient"),
}

# Types requiring special handling
SPECIAL_HANDLING_TYPES = {
    "GEOGRAPHY": "Convert to WKT string using STAsText(), use H3 or Shapely for operations",
    "GEOMETRY": "Convert to WKT string using STAsText(), use Shapely for operations",
    "HIERARCHYID": "Convert to string using ToString(), parse hierarchy manually",
    "XML": "Convert to string, use Spark XML functions or UDF for parsing",
    "SQL_VARIANT": "Determine actual type and convert accordingly",
    "TIMESTAMP": "SQL Server TIMESTAMP is rowversion (not datetime), convert to BINARY",
    "UNIQUEIDENTIFIER": "Store as STRING (36 chars), use uuid functions for generation",
}


class DataTypeValidator:
    """
    Validate data type mappings between T-SQL source and Spark target.

    Detects:
    - Incorrect type mappings
    - Potential precision loss
    - Types requiring special handling
    - Nullable mismatches
    """

    def __init__(self):
        self.type_mappings = TSQL_TO_SPARK_MAPPINGS.copy()

    def extract_column_types_from_tsql(self, source_sql: str) -> List[ColumnTypeInfo]:
        """
        Extract column names and types from T-SQL.

        Handles:
        - CREATE TABLE definitions
        - DECLARE statements
        - Stored procedure parameters
        - SELECT with CAST/CONVERT

        Args:
            source_sql: T-SQL source code

        Returns:
            List of ColumnTypeInfo objects
        """
        columns = []

        # Pattern for CREATE TABLE columns
        create_table_pattern = re.compile(
            r'(\[?\w+\]?)\s+(NVARCHAR|VARCHAR|CHAR|NCHAR|INT|BIGINT|SMALLINT|TINYINT|'
            r'DECIMAL|NUMERIC|MONEY|SMALLMONEY|FLOAT|REAL|BIT|'
            r'DATETIME|DATETIME2|SMALLDATETIME|DATE|TIME|DATETIMEOFFSET|'
            r'BINARY|VARBINARY|IMAGE|UNIQUEIDENTIFIER|XML|GEOGRAPHY|GEOMETRY|'
            r'HIERARCHYID|SQL_VARIANT|TIMESTAMP|ROWVERSION|TEXT|NTEXT|SYSNAME)'
            r'(?:\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?'
            r'(?:\s+(NOT\s+NULL|NULL))?',
            re.IGNORECASE
        )

        for match in create_table_pattern.finditer(source_sql):
            col_name = match.group(1).strip("[]")
            base_type = match.group(2).upper()
            precision = int(match.group(3)) if match.group(3) else None
            scale = int(match.group(4)) if match.group(4) else None
            nullable_str = match.group(5)
            is_nullable = nullable_str is None or "NOT" not in nullable_str.upper()

            columns.append(ColumnTypeInfo(
                name=col_name,
                base_type=base_type,
                precision=precision,
                scale=scale,
                max_length=precision if base_type in ["VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "BINARY", "VARBINARY"] else None,
                is_nullable=is_nullable,
                raw_type=match.group(0)
            ))

        # Pattern for stored procedure parameters
        param_pattern = re.compile(
            r'@(\w+)\s+(NVARCHAR|VARCHAR|CHAR|NCHAR|INT|BIGINT|SMALLINT|TINYINT|'
            r'DECIMAL|NUMERIC|MONEY|SMALLMONEY|FLOAT|REAL|BIT|'
            r'DATETIME|DATETIME2|SMALLDATETIME|DATE|TIME|DATETIMEOFFSET|'
            r'BINARY|VARBINARY|UNIQUEIDENTIFIER|XML)'
            r'(?:\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?',
            re.IGNORECASE
        )

        for match in param_pattern.finditer(source_sql):
            col_name = f"@{match.group(1)}"
            base_type = match.group(2).upper()
            precision = int(match.group(3)) if match.group(3) else None
            scale = int(match.group(4)) if match.group(4) else None

            columns.append(ColumnTypeInfo(
                name=col_name,
                base_type=base_type,
                precision=precision,
                scale=scale,
                is_nullable=True,
                raw_type=match.group(0)
            ))

        return columns

    def extract_column_types_from_spark(self, notebook_content: str) -> List[ColumnTypeInfo]:
        """
        Extract column types from Spark/Python notebook.

        Args:
            notebook_content: Databricks notebook content

        Returns:
            List of ColumnTypeInfo objects
        """
        columns = []

        # Pattern for schema definitions
        schema_pattern = re.compile(
            r'StructField\s*\(\s*["\'](\w+)["\']\s*,\s*'
            r'(StringType|IntegerType|LongType|ShortType|ByteType|'
            r'DecimalType|DoubleType|FloatType|BooleanType|'
            r'TimestampType|DateType|BinaryType)'
            r'(?:\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?'
            r'(?:\s*\(\s*\))?'
            r'(?:\s*,\s*(True|False))?',
            re.IGNORECASE
        )

        for match in schema_pattern.finditer(notebook_content):
            col_name = match.group(1)
            spark_type = match.group(2)
            precision = int(match.group(3)) if match.group(3) else None
            scale = int(match.group(4)) if match.group(4) else None
            nullable = match.group(5) != "False" if match.group(5) else True

            columns.append(ColumnTypeInfo(
                name=col_name,
                base_type=self._spark_to_base_type(spark_type),
                precision=precision,
                scale=scale,
                is_nullable=nullable,
                raw_type=match.group(0)
            ))

        # Pattern for .cast() operations
        cast_pattern = re.compile(
            r'\.cast\s*\(\s*["\']?(\w+(?:\(\d+(?:,\s*\d+)?\))?)["\']?\s*\)',
            re.IGNORECASE
        )

        for match in cast_pattern.finditer(notebook_content):
            type_str = match.group(1)
            # Parse type string
            type_match = re.match(r'(\w+)(?:\((\d+)(?:,\s*(\d+))?\))?', type_str)
            if type_match:
                columns.append(ColumnTypeInfo(
                    name=f"cast_{len(columns)}",
                    base_type=type_match.group(1).upper(),
                    precision=int(type_match.group(2)) if type_match.group(2) else None,
                    scale=int(type_match.group(3)) if type_match.group(3) else None,
                    raw_type=match.group(0)
                ))

        return columns

    def validate_type_mapping(
        self,
        source_type: ColumnTypeInfo,
        target_type: Optional[ColumnTypeInfo] = None,
        column_name: str = ""
    ) -> List[TypeValidationIssue]:
        """
        Validate a single type mapping.

        Args:
            source_type: T-SQL column type info
            target_type: Spark column type info (optional)
            column_name: Column name for reporting

        Returns:
            List of validation issues
        """
        issues = []
        col_name = column_name or source_type.name

        # Get expected Spark type
        expected_spark = self._get_expected_spark_type(source_type)

        # Check for special handling types
        if source_type.base_type in SPECIAL_HANDLING_TYPES:
            issues.append(TypeValidationIssue(
                column_name=col_name,
                source_type=source_type.raw_type or source_type.base_type,
                target_type=expected_spark,
                severity=ValidationSeverity.INFO,
                message=f"Type {source_type.base_type} requires special handling",
                recommendation=SPECIAL_HANDLING_TYPES[source_type.base_type]
            ))

        # Check for precision loss
        if source_type.base_type in PRECISION_LOSS_TYPES:
            spark_type, note = PRECISION_LOSS_TYPES[source_type.base_type]
            issues.append(TypeValidationIssue(
                column_name=col_name,
                source_type=source_type.raw_type or source_type.base_type,
                target_type=spark_type,
                severity=ValidationSeverity.WARNING,
                message=f"Potential precision change: {note}",
                recommendation=f"Verify {col_name} values are within acceptable precision"
            ))

        # Check decimal precision
        if source_type.base_type in ["DECIMAL", "NUMERIC"]:
            if source_type.precision and source_type.precision > 38:
                issues.append(TypeValidationIssue(
                    column_name=col_name,
                    source_type=f"DECIMAL({source_type.precision},{source_type.scale})",
                    target_type="DECIMAL(38,x)",
                    severity=ValidationSeverity.ERROR,
                    message=f"Precision {source_type.precision} exceeds Spark maximum of 38",
                    recommendation="Reduce precision or use DOUBLE with loss of precision"
                ))

        # Check varchar max
        if source_type.base_type in ["VARCHAR", "NVARCHAR"]:
            if source_type.max_length and source_type.max_length == -1:  # MAX
                issues.append(TypeValidationIssue(
                    column_name=col_name,
                    source_type=f"{source_type.base_type}(MAX)",
                    target_type="STRING",
                    severity=ValidationSeverity.INFO,
                    message="VARCHAR(MAX)/NVARCHAR(MAX) maps to STRING",
                    recommendation="Verify string length doesn't cause memory issues"
                ))

        # Validate against target if provided
        if target_type:
            if not self._types_compatible(source_type, target_type):
                issues.append(TypeValidationIssue(
                    column_name=col_name,
                    source_type=source_type.raw_type or source_type.base_type,
                    target_type=target_type.raw_type or target_type.base_type,
                    severity=ValidationSeverity.ERROR,
                    message=f"Type mismatch: {source_type.base_type} → {target_type.base_type}",
                    recommendation=f"Expected {expected_spark} for {source_type.base_type}"
                ))

            # Check nullable mismatch
            if not source_type.is_nullable and target_type.is_nullable:
                issues.append(TypeValidationIssue(
                    column_name=col_name,
                    source_type=f"{source_type.base_type} NOT NULL",
                    target_type=f"{target_type.base_type} (nullable)",
                    severity=ValidationSeverity.WARNING,
                    message="NOT NULL constraint not enforced in target",
                    recommendation="Add explicit null check or Delta constraint"
                ))

        return issues

    def validate_all_types(
        self,
        source_sql: str,
        target_notebook: str
    ) -> Dict:
        """
        Validate all type mappings between source and target.

        Args:
            source_sql: T-SQL source code
            target_notebook: Spark notebook content

        Returns:
            Validation report dictionary
        """
        source_columns = self.extract_column_types_from_tsql(source_sql)
        target_columns = self.extract_column_types_from_spark(target_notebook)

        # Build lookup by column name
        target_by_name = {c.name.lower(): c for c in target_columns}

        all_issues = []

        for source_col in source_columns:
            target_col = target_by_name.get(source_col.name.lower())
            issues = self.validate_type_mapping(source_col, target_col)
            all_issues.extend(issues)

        # Categorize issues
        errors = [i for i in all_issues if i.severity == ValidationSeverity.ERROR]
        warnings = [i for i in all_issues if i.severity == ValidationSeverity.WARNING]
        infos = [i for i in all_issues if i.severity == ValidationSeverity.INFO]

        return {
            "source_columns": len(source_columns),
            "target_columns": len(target_columns),
            "issues": all_issues,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "info_count": len(infos),
            "verdict": "FAIL" if errors else "PASS_WITH_WARNINGS" if warnings else "PASS"
        }

    def generate_report(self, validation_result: Dict) -> str:
        """
        Generate markdown report from validation result.

        Args:
            validation_result: Result from validate_all_types()

        Returns:
            Markdown formatted report
        """
        lines = [
            "## Data Type Validation Report",
            "",
            f"**Source Columns:** {validation_result['source_columns']}",
            f"**Target Columns:** {validation_result['target_columns']}",
            f"**Verdict:** {validation_result['verdict']}",
            "",
            "### Summary",
            "",
            f"- Errors: {validation_result['error_count']}",
            f"- Warnings: {validation_result['warning_count']}",
            f"- Info: {validation_result['info_count']}",
            "",
        ]

        issues = validation_result.get("issues", [])

        if issues:
            lines.extend([
                "### Issues",
                "",
                "| Column | Source Type | Target Type | Severity | Message |",
                "|--------|-------------|-------------|----------|---------|",
            ])

            for issue in issues:
                severity_icon = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}.get(
                    issue.severity.value, "❓"
                )
                lines.append(
                    f"| {issue.column_name} | {issue.source_type} | {issue.target_type} | "
                    f"{severity_icon} {issue.severity.value.upper()} | {issue.message} |"
                )

            lines.append("")

            # Recommendations
            recommendations = [i for i in issues if i.recommendation]
            if recommendations:
                lines.extend([
                    "### Recommendations",
                    "",
                ])
                for issue in recommendations:
                    lines.append(f"- **{issue.column_name}**: {issue.recommendation}")
                lines.append("")

        else:
            lines.append("No type validation issues found.")

        return "\n".join(lines)

    def get_type_mapping_table(self) -> str:
        """
        Generate markdown table of all type mappings.

        Returns:
            Markdown formatted mapping table
        """
        lines = [
            "## T-SQL to Spark Type Mapping Reference",
            "",
            "| T-SQL Type | Spark Type | Notes |",
            "|------------|------------|-------|",
        ]

        for tsql_type, spark_type in sorted(TSQL_TO_SPARK_MAPPINGS.items()):
            notes = ""
            if tsql_type in PRECISION_LOSS_TYPES:
                _, note = PRECISION_LOSS_TYPES[tsql_type]
                notes = note
            elif tsql_type in SPECIAL_HANDLING_TYPES:
                notes = "Requires special handling"

            lines.append(f"| {tsql_type} | {spark_type} | {notes} |")

        return "\n".join(lines)

    def _get_expected_spark_type(self, source_type: ColumnTypeInfo) -> str:
        """Get expected Spark type for a T-SQL type."""
        base = source_type.base_type

        if base in ["DECIMAL", "NUMERIC"]:
            if source_type.precision and source_type.scale is not None:
                return f"DECIMAL({source_type.precision},{source_type.scale})"
            elif source_type.precision:
                return f"DECIMAL({source_type.precision})"
            return "DECIMAL(18,0)"

        return self.type_mappings.get(base, "STRING")

    def _spark_to_base_type(self, spark_type: str) -> str:
        """Convert Spark type name to base type."""
        mapping = {
            "StringType": "STRING",
            "IntegerType": "INT",
            "LongType": "BIGINT",
            "ShortType": "SMALLINT",
            "ByteType": "TINYINT",
            "DoubleType": "DOUBLE",
            "FloatType": "FLOAT",
            "BooleanType": "BOOLEAN",
            "TimestampType": "TIMESTAMP",
            "DateType": "DATE",
            "BinaryType": "BINARY",
            "DecimalType": "DECIMAL",
        }
        return mapping.get(spark_type, spark_type.upper())

    def _types_compatible(
        self,
        source: ColumnTypeInfo,
        target: ColumnTypeInfo
    ) -> bool:
        """Check if source and target types are compatible."""
        expected = self._get_expected_spark_type(source)
        actual = target.base_type.upper()

        # Direct match
        if expected.upper().startswith(actual):
            return True

        # Check common compatible mappings
        compatible_pairs = [
            ("STRING", "STRING"),
            ("INT", "INT"),
            ("INT", "BIGINT"),  # Safe upcast
            ("BIGINT", "BIGINT"),
            ("DECIMAL", "DECIMAL"),
            ("DOUBLE", "DOUBLE"),
            ("FLOAT", "FLOAT"),
            ("BOOLEAN", "BOOLEAN"),
            ("TIMESTAMP", "TIMESTAMP"),
            ("DATE", "DATE"),
            ("BINARY", "BINARY"),
        ]

        for src, tgt in compatible_pairs:
            if expected.startswith(src) and actual.startswith(tgt):
                return True

        return False
