"""
Business Logic Comparator for SP Migration Review.

REQ-R3: Extract and compare business logic between T-SQL source
and Databricks notebook target to ensure functional equivalence.
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum


class ComparisonStatus(Enum):
    """Status of business logic comparison."""
    MATCH = "match"
    PARTIAL = "partial"
    MISSING = "missing"
    EXTRA = "extra"


@dataclass
class TableReference:
    """A table reference in SQL."""
    schema: str
    table: str
    alias: Optional[str] = None
    operation: str = "read"  # read, insert, update, delete, merge


@dataclass
class ColumnReference:
    """A column reference in SQL."""
    table: Optional[str]
    column: str
    expression: Optional[str] = None
    is_aggregated: bool = False


@dataclass
class JoinInfo:
    """Information about a JOIN operation."""
    join_type: str  # INNER, LEFT, RIGHT, FULL, CROSS
    left_table: str
    right_table: str
    condition: str


@dataclass
class AggregationInfo:
    """Information about an aggregation."""
    function: str  # SUM, COUNT, AVG, MIN, MAX
    column: str
    alias: Optional[str] = None


@dataclass
class FilterInfo:
    """Information about a filter/WHERE condition."""
    table: Optional[str]
    condition: str
    is_join_condition: bool = False


@dataclass
class BusinessLogic:
    """Extracted business logic from code."""
    input_tables: List[TableReference] = field(default_factory=list)
    output_tables: List[TableReference] = field(default_factory=list)
    columns_read: List[ColumnReference] = field(default_factory=list)
    columns_written: List[ColumnReference] = field(default_factory=list)
    aggregations: List[AggregationInfo] = field(default_factory=list)
    joins: List[JoinInfo] = field(default_factory=list)
    filters: List[FilterInfo] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: List[Tuple[str, str]] = field(default_factory=list)  # (column, direction)
    parameters: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class LogicComparison:
    """Result of comparing source and target business logic."""
    element_type: str
    source_value: str
    target_value: Optional[str]
    status: ComparisonStatus
    risk_tier: str  # BLOCKER, HIGH, MEDIUM, LOW
    message: str


class BusinessLogicExtractor:
    """
    Extract business logic elements from SQL and Python code.

    Analyzes both T-SQL stored procedures and Databricks notebooks
    to extract comparable business logic elements.
    """

    # T-SQL patterns for extraction
    TSQL_PATTERNS = {
        # Table references in FROM/JOIN
        "table_from": re.compile(
            r"FROM\s+(\[?[\w.]+\]?\.)?(\[?[\w]+\]?)\s*(?:AS\s+)?(\w+)?",
            re.IGNORECASE
        ),
        "table_join": re.compile(
            r"(INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+(\[?[\w.]+\]?\.)?(\[?[\w]+\]?)\s*(?:AS\s+)?(\w+)?",
            re.IGNORECASE
        ),
        # Write operations
        "insert_into": re.compile(
            r"INSERT\s+INTO\s+(\[?[\w.]+\]?\.)?(\[?[\w]+\]?)",
            re.IGNORECASE
        ),
        "update_table": re.compile(
            r"UPDATE\s+(\[?[\w.]+\]?\.)?(\[?[\w]+\]?)",
            re.IGNORECASE
        ),
        "delete_from": re.compile(
            r"DELETE\s+FROM\s+(\[?[\w.]+\]?\.)?(\[?[\w]+\]?)",
            re.IGNORECASE
        ),
        "merge_into": re.compile(
            r"MERGE\s+(?:INTO\s+)?(\[?[\w.]+\]?\.)?(\[?[\w]+\]?)\s*(?:AS\s+)?(\w+)?",
            re.IGNORECASE
        ),
        # Aggregations
        "aggregation": re.compile(
            r"\b(SUM|COUNT|AVG|MIN|MAX|STDEV|VAR)\s*\(\s*(?:DISTINCT\s+)?(\[?\w+\]?|\*)\s*\)\s*(?:AS\s+(\w+))?",
            re.IGNORECASE
        ),
        # GROUP BY
        "group_by": re.compile(
            r"GROUP\s+BY\s+([\w\s,.\[\]]+?)(?=\s+(?:HAVING|ORDER|UNION|;|\)|$))",
            re.IGNORECASE | re.DOTALL
        ),
        # ORDER BY
        "order_by": re.compile(
            r"ORDER\s+BY\s+([\w\s,.\[\]]+(?:\s+(?:ASC|DESC))?)(?=\s*(?:OFFSET|;|\)|$))",
            re.IGNORECASE
        ),
        # WHERE conditions
        "where_clause": re.compile(
            r"WHERE\s+(.*?)(?=\s+(?:GROUP|ORDER|UNION|HAVING|;|\)|$))",
            re.IGNORECASE | re.DOTALL
        ),
        # JOIN conditions
        "join_on": re.compile(
            r"ON\s+(.*?)(?=\s+(?:AND\s+\w+\s*=|WHERE|GROUP|ORDER|LEFT|RIGHT|INNER|FULL|CROSS|UNION|;|\)|$))",
            re.IGNORECASE | re.DOTALL
        ),
        # Parameters
        "parameter": re.compile(
            r"@(\w+)\s+(\w+(?:\(\d+(?:,\s*\d+)?\))?)\s*(?:=\s*(\w+|\'[^\']*\'))?",
            re.IGNORECASE
        ),
        # SELECT columns
        "select_columns": re.compile(
            r"SELECT\s+(?:TOP\s+\d+\s+)?(?:DISTINCT\s+)?(.*?)(?=\s+FROM)",
            re.IGNORECASE | re.DOTALL
        ),
    }

    # Spark/Python patterns for extraction
    SPARK_PATTERNS = {
        # Table reads
        "spark_table": re.compile(
            r"spark\.table\s*\(\s*[\"']([^\"']+)[\"']\s*\)",
            re.IGNORECASE
        ),
        "spark_read": re.compile(
            r"spark\.read\s*\..*?\.(?:table|load)\s*\(\s*[\"']([^\"']+)[\"']\s*\)",
            re.IGNORECASE | re.DOTALL
        ),
        # Table writes
        "write_table": re.compile(
            r"\.(?:write|writeTo|saveAsTable)\s*\(?\s*(?:[\"']([^\"']+)[\"'])?\s*\)?",
            re.IGNORECASE
        ),
        "insert_into": re.compile(
            r"\.insertInto\s*\(\s*[\"']([^\"']+)[\"']\s*\)",
            re.IGNORECASE
        ),
        # Delta MERGE
        "delta_merge": re.compile(
            r"DeltaTable\.forName\s*\(\s*spark\s*,\s*[\"']([^\"']+)[\"']\s*\)",
            re.IGNORECASE
        ),
        # Aggregations
        "agg_function": re.compile(
            r"F\.(sum|count|avg|min|max|stddev|variance)\s*\(\s*(?:F\.col\s*\(\s*)?[\"']?(\w+)[\"']?\s*\)?\s*\)",
            re.IGNORECASE
        ),
        # GROUP BY
        "group_by": re.compile(
            r"\.groupBy\s*\(\s*([^)]+)\s*\)",
            re.IGNORECASE
        ),
        # ORDER BY
        "order_by": re.compile(
            r"\.orderBy\s*\(\s*([^)]+)\s*\)",
            re.IGNORECASE
        ),
        # Filters
        "filter": re.compile(
            r"\.(?:filter|where)\s*\(\s*([^)]+)\s*\)",
            re.IGNORECASE
        ),
        # Joins
        "join": re.compile(
            r"\.join\s*\(\s*([^,]+)\s*,\s*(?:on\s*=\s*)?([^,]+)?\s*,\s*(?:how\s*=\s*)?[\"']?(\w+)?[\"']?\s*\)",
            re.IGNORECASE
        ),
        # Widgets/Parameters
        "widget": re.compile(
            r"dbutils\.widgets\.(?:text|dropdown|combobox|multiselect)\s*\(\s*[\"'](\w+)[\"']",
            re.IGNORECASE
        ),
    }

    def extract_from_tsql(self, source_sql: str) -> BusinessLogic:
        """
        Extract business logic elements from T-SQL stored procedure.

        Args:
            source_sql: T-SQL source code

        Returns:
            BusinessLogic with extracted elements
        """
        logic = BusinessLogic()

        # Remove comments for cleaner parsing
        clean_sql = self._remove_sql_comments(source_sql)

        # Extract input tables (FROM, JOIN)
        logic.input_tables = self._extract_tsql_input_tables(clean_sql)

        # Extract output tables (INSERT, UPDATE, DELETE, MERGE)
        logic.output_tables = self._extract_tsql_output_tables(clean_sql)

        # Extract columns (SELECT)
        logic.columns_read = self._extract_tsql_columns(clean_sql)

        # Extract aggregations
        logic.aggregations = self._extract_tsql_aggregations(clean_sql)

        # Extract joins
        logic.joins = self._extract_tsql_joins(clean_sql)

        # Extract filters
        logic.filters = self._extract_tsql_filters(clean_sql)

        # Extract GROUP BY
        logic.group_by = self._extract_tsql_group_by(clean_sql)

        # Extract ORDER BY
        logic.order_by = self._extract_tsql_order_by(clean_sql)

        # Extract parameters
        logic.parameters = self._extract_tsql_parameters(clean_sql)

        return logic

    def extract_from_notebook(self, notebook_content: str) -> BusinessLogic:
        """
        Extract business logic elements from Databricks notebook.

        Args:
            notebook_content: Python notebook content

        Returns:
            BusinessLogic with extracted elements
        """
        logic = BusinessLogic()

        # Remove comments
        clean_code = self._remove_python_comments(notebook_content)

        # Extract input tables
        logic.input_tables = self._extract_spark_input_tables(clean_code)

        # Extract output tables
        logic.output_tables = self._extract_spark_output_tables(clean_code)

        # Extract aggregations
        logic.aggregations = self._extract_spark_aggregations(clean_code)

        # Extract filters
        logic.filters = self._extract_spark_filters(clean_code)

        # Extract group by
        logic.group_by = self._extract_spark_group_by(clean_code)

        # Extract order by
        logic.order_by = self._extract_spark_order_by(clean_code)

        # Extract parameters (widgets)
        logic.parameters = self._extract_spark_parameters(clean_code)

        # Extract joins
        logic.joins = self._extract_spark_joins(clean_code)

        return logic

    def compare_logic(
        self,
        source_logic: BusinessLogic,
        target_logic: BusinessLogic,
        table_mapping: Optional[Dict[str, str]] = None
    ) -> List[LogicComparison]:
        """
        Compare source and target business logic.

        Args:
            source_logic: BusinessLogic from T-SQL source
            target_logic: BusinessLogic from Databricks notebook
            table_mapping: Optional mapping of source -> target table names

        Returns:
            List of LogicComparison results
        """
        comparisons = []

        # Compare input tables
        comparisons.extend(self._compare_tables(
            source_logic.input_tables,
            target_logic.input_tables,
            "input_table",
            table_mapping
        ))

        # Compare output tables
        comparisons.extend(self._compare_tables(
            source_logic.output_tables,
            target_logic.output_tables,
            "output_table",
            table_mapping
        ))

        # Compare aggregations
        comparisons.extend(self._compare_aggregations(
            source_logic.aggregations,
            target_logic.aggregations
        ))

        # Compare joins
        comparisons.extend(self._compare_joins(
            source_logic.joins,
            target_logic.joins
        ))

        # Compare filters
        comparisons.extend(self._compare_filters(
            source_logic.filters,
            target_logic.filters
        ))

        # Compare group by
        comparisons.extend(self._compare_group_by(
            source_logic.group_by,
            target_logic.group_by
        ))

        return comparisons

    # ==================== T-SQL Extraction Helpers ====================

    def _remove_sql_comments(self, sql: str) -> str:
        """Remove SQL comments from code."""
        # Remove single-line comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        # Remove multi-line comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql

    def _extract_tsql_input_tables(self, sql: str) -> List[TableReference]:
        """Extract input tables from T-SQL."""
        tables = []

        # FROM clauses
        for match in self.TSQL_PATTERNS["table_from"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")
            alias = match.group(3)

            # Skip temp tables
            if not table.startswith("#"):
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    alias=alias,
                    operation="read"
                ))

        # JOIN clauses
        for match in self.TSQL_PATTERNS["table_join"].finditer(sql):
            schema = (match.group(2) or "dbo").strip("[].")
            table = match.group(3).strip("[]")
            alias = match.group(4)

            if not table.startswith("#"):
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    alias=alias,
                    operation="read"
                ))

        return self._dedupe_tables(tables)

    def _extract_tsql_output_tables(self, sql: str) -> List[TableReference]:
        """Extract output tables from T-SQL."""
        tables = []

        # INSERT INTO
        for match in self.TSQL_PATTERNS["insert_into"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")
            if not table.startswith("#"):
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    operation="insert"
                ))

        # UPDATE
        for match in self.TSQL_PATTERNS["update_table"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")
            if not table.startswith("#"):
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    operation="update"
                ))

        # DELETE
        for match in self.TSQL_PATTERNS["delete_from"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")
            if not table.startswith("#"):
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    operation="delete"
                ))

        # MERGE INTO
        for match in self.TSQL_PATTERNS["merge_into"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")
            if not table.startswith("#"):
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    alias=match.group(3),
                    operation="merge"
                ))

        return self._dedupe_tables(tables)

    def _extract_tsql_columns(self, sql: str) -> List[ColumnReference]:
        """Extract column references from SELECT."""
        columns = []

        for match in self.TSQL_PATTERNS["select_columns"].finditer(sql):
            select_list = match.group(1)
            # Parse individual columns
            for col_expr in self._split_column_list(select_list):
                col_expr = col_expr.strip()
                if col_expr and col_expr != "*":
                    # Check for aggregation
                    is_agg = bool(re.search(r'\b(SUM|COUNT|AVG|MIN|MAX)\s*\(', col_expr, re.IGNORECASE))

                    # Extract column name and table prefix
                    col_match = re.search(r'(\w+)\.(\w+)|(\w+)\s*(?:AS\s+\w+)?$', col_expr)
                    if col_match:
                        if col_match.group(1):
                            table, column = col_match.group(1), col_match.group(2)
                        else:
                            table, column = None, col_match.group(3)

                        columns.append(ColumnReference(
                            table=table,
                            column=column,
                            expression=col_expr if is_agg else None,
                            is_aggregated=is_agg
                        ))

        return columns

    def _extract_tsql_aggregations(self, sql: str) -> List[AggregationInfo]:
        """Extract aggregation functions from T-SQL."""
        aggregations = []

        for match in self.TSQL_PATTERNS["aggregation"].finditer(sql):
            aggregations.append(AggregationInfo(
                function=match.group(1).upper(),
                column=match.group(2).strip("[]"),
                alias=match.group(3)
            ))

        return aggregations

    def _extract_tsql_joins(self, sql: str) -> List[JoinInfo]:
        """Extract JOIN information from T-SQL."""
        joins = []

        # Find JOIN patterns with their conditions
        join_pattern = re.compile(
            r'(INNER|LEFT\s*(?:OUTER)?|RIGHT\s*(?:OUTER)?|FULL\s*(?:OUTER)?|CROSS)?\s*JOIN\s+(\[?[\w.]+\]?)\s*(?:AS\s+)?(\w+)?\s*ON\s+([^WHERE]+?)(?=\s+(?:LEFT|RIGHT|INNER|FULL|CROSS|WHERE|GROUP|ORDER|UNION|;|\)|$))',
            re.IGNORECASE | re.DOTALL
        )

        for match in join_pattern.finditer(sql):
            join_type = (match.group(1) or "INNER").upper().replace("OUTER", "").strip()
            right_table = match.group(2).strip("[]")
            condition = match.group(4).strip()

            joins.append(JoinInfo(
                join_type=join_type,
                left_table="",  # Hard to determine from regex
                right_table=right_table,
                condition=condition
            ))

        return joins

    def _extract_tsql_filters(self, sql: str) -> List[FilterInfo]:
        """Extract WHERE conditions from T-SQL."""
        filters = []

        for match in self.TSQL_PATTERNS["where_clause"].finditer(sql):
            condition = match.group(1).strip()
            if condition:
                filters.append(FilterInfo(
                    table=None,
                    condition=condition,
                    is_join_condition=False
                ))

        return filters

    def _extract_tsql_group_by(self, sql: str) -> List[str]:
        """Extract GROUP BY columns from T-SQL."""
        group_by = []

        for match in self.TSQL_PATTERNS["group_by"].finditer(sql):
            columns = match.group(1)
            for col in columns.split(","):
                col = col.strip().strip("[]")
                if col:
                    group_by.append(col)

        return group_by

    def _extract_tsql_order_by(self, sql: str) -> List[Tuple[str, str]]:
        """Extract ORDER BY columns from T-SQL."""
        order_by = []

        for match in self.TSQL_PATTERNS["order_by"].finditer(sql):
            columns = match.group(1)
            for col in columns.split(","):
                col = col.strip()
                direction = "ASC"
                if "DESC" in col.upper():
                    direction = "DESC"
                    col = re.sub(r'\s+DESC\s*$', '', col, flags=re.IGNORECASE)
                elif "ASC" in col.upper():
                    col = re.sub(r'\s+ASC\s*$', '', col, flags=re.IGNORECASE)

                col = col.strip().strip("[]")
                if col:
                    order_by.append((col, direction))

        return order_by

    def _extract_tsql_parameters(self, sql: str) -> List[Dict[str, str]]:
        """Extract stored procedure parameters from T-SQL."""
        params = []

        for match in self.TSQL_PATTERNS["parameter"].finditer(sql):
            params.append({
                "name": match.group(1),
                "type": match.group(2),
                "default": match.group(3) or ""
            })

        return params

    # ==================== Spark/Python Extraction Helpers ====================

    def _remove_python_comments(self, code: str) -> str:
        """Remove Python comments from code."""
        # Remove single-line comments
        code = re.sub(r'#.*$', '', code, flags=re.MULTILINE)
        # Remove multi-line strings used as comments
        code = re.sub(r'""".*?"""', '', code, flags=re.DOTALL)
        code = re.sub(r"'''.*?'''", '', code, flags=re.DOTALL)
        return code

    def _extract_spark_input_tables(self, code: str) -> List[TableReference]:
        """Extract input tables from Spark code."""
        tables = []

        # spark.table()
        for match in self.SPARK_PATTERNS["spark_table"].finditer(code):
            table_path = match.group(1)
            parts = table_path.split(".")
            if len(parts) >= 2:
                schema = parts[-2] if len(parts) >= 2 else "default"
                table = parts[-1]
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    operation="read"
                ))

        # spark.read...table/load
        for match in self.SPARK_PATTERNS["spark_read"].finditer(code):
            table_path = match.group(1)
            parts = table_path.split(".")
            schema = parts[-2] if len(parts) >= 2 else "default"
            table = parts[-1]
            tables.append(TableReference(
                schema=schema,
                table=table,
                operation="read"
            ))

        # DeltaTable.forName (for MERGE reads)
        for match in self.SPARK_PATTERNS["delta_merge"].finditer(code):
            table_path = match.group(1)
            parts = table_path.split(".")
            schema = parts[-2] if len(parts) >= 2 else "default"
            table = parts[-1]
            tables.append(TableReference(
                schema=schema,
                table=table,
                operation="read"
            ))

        return self._dedupe_tables(tables)

    def _extract_spark_output_tables(self, code: str) -> List[TableReference]:
        """Extract output tables from Spark code."""
        tables = []

        # .write.saveAsTable or insertInto
        for match in self.SPARK_PATTERNS["write_table"].finditer(code):
            if match.group(1):
                table_path = match.group(1)
                parts = table_path.split(".")
                schema = parts[-2] if len(parts) >= 2 else "default"
                table = parts[-1]
                tables.append(TableReference(
                    schema=schema,
                    table=table,
                    operation="write"
                ))

        for match in self.SPARK_PATTERNS["insert_into"].finditer(code):
            table_path = match.group(1)
            parts = table_path.split(".")
            schema = parts[-2] if len(parts) >= 2 else "default"
            table = parts[-1]
            tables.append(TableReference(
                schema=schema,
                table=table,
                operation="insert"
            ))

        # DeltaTable.forName for MERGE (also output)
        for match in self.SPARK_PATTERNS["delta_merge"].finditer(code):
            table_path = match.group(1)
            parts = table_path.split(".")
            schema = parts[-2] if len(parts) >= 2 else "default"
            table = parts[-1]
            tables.append(TableReference(
                schema=schema,
                table=table,
                operation="merge"
            ))

        return self._dedupe_tables(tables)

    def _extract_spark_aggregations(self, code: str) -> List[AggregationInfo]:
        """Extract aggregations from Spark code."""
        aggregations = []

        for match in self.SPARK_PATTERNS["agg_function"].finditer(code):
            aggregations.append(AggregationInfo(
                function=match.group(1).upper(),
                column=match.group(2)
            ))

        return aggregations

    def _extract_spark_filters(self, code: str) -> List[FilterInfo]:
        """Extract filters from Spark code."""
        filters = []

        for match in self.SPARK_PATTERNS["filter"].finditer(code):
            condition = match.group(1).strip()
            if condition:
                filters.append(FilterInfo(
                    table=None,
                    condition=condition,
                    is_join_condition=False
                ))

        return filters

    def _extract_spark_group_by(self, code: str) -> List[str]:
        """Extract GROUP BY columns from Spark code."""
        group_by = []

        for match in self.SPARK_PATTERNS["group_by"].finditer(code):
            cols_str = match.group(1)
            # Parse column references
            for col in re.findall(r'["\'](\w+)["\']', cols_str):
                group_by.append(col)

        return group_by

    def _extract_spark_order_by(self, code: str) -> List[Tuple[str, str]]:
        """Extract ORDER BY from Spark code."""
        order_by = []

        for match in self.SPARK_PATTERNS["order_by"].finditer(code):
            cols_str = match.group(1)
            # Check for desc() function
            for col in re.findall(r'F\.(?:desc|asc)\s*\(\s*["\'](\w+)["\']\s*\)', cols_str):
                direction = "DESC" if "desc" in cols_str else "ASC"
                order_by.append((col, direction))
            # Simple column references
            for col in re.findall(r'["\'](\w+)["\'](?!\s*\))', cols_str):
                if col not in [c for c, _ in order_by]:
                    order_by.append((col, "ASC"))

        return order_by

    def _extract_spark_parameters(self, code: str) -> List[Dict[str, str]]:
        """Extract widget parameters from Spark code."""
        params = []

        for match in self.SPARK_PATTERNS["widget"].finditer(code):
            params.append({
                "name": match.group(1),
                "type": "string",  # Default type
                "default": ""
            })

        return params

    def _extract_spark_joins(self, code: str) -> List[JoinInfo]:
        """Extract JOIN information from Spark code."""
        joins = []

        for match in self.SPARK_PATTERNS["join"].finditer(code):
            right_df = match.group(1).strip()
            condition = (match.group(2) or "").strip()
            join_type = (match.group(3) or "inner").upper()

            joins.append(JoinInfo(
                join_type=join_type,
                left_table="",
                right_table=right_df,
                condition=condition
            ))

        return joins

    # ==================== Comparison Helpers ====================

    def _compare_tables(
        self,
        source_tables: List[TableReference],
        target_tables: List[TableReference],
        element_type: str,
        table_mapping: Optional[Dict[str, str]] = None
    ) -> List[LogicComparison]:
        """Compare source and target tables."""
        comparisons = []
        table_mapping = table_mapping or {}

        source_set = {f"{t.schema}.{t.table}".lower() for t in source_tables}
        target_set = {f"{t.schema}.{t.table}".lower() for t in target_tables}

        # Check for missing tables in target
        for source_table in source_tables:
            source_key = f"{source_table.schema}.{source_table.table}".lower()
            mapped_key = table_mapping.get(source_key, source_key).lower()

            if mapped_key not in target_set and source_key not in target_set:
                risk = "BLOCKER" if element_type == "output_table" else "HIGH"
                comparisons.append(LogicComparison(
                    element_type=element_type,
                    source_value=f"{source_table.schema}.{source_table.table}",
                    target_value=None,
                    status=ComparisonStatus.MISSING,
                    risk_tier=risk,
                    message=f"Source {element_type} not found in target"
                ))

        # Check for extra tables in target
        for target_table in target_tables:
            target_key = f"{target_table.schema}.{target_table.table}".lower()

            # Check if this corresponds to any source table
            has_source = any(
                target_key == table_mapping.get(f"{t.schema}.{t.table}".lower(), f"{t.schema}.{t.table}".lower()).lower()
                for t in source_tables
            )

            if not has_source:
                comparisons.append(LogicComparison(
                    element_type=element_type,
                    source_value=None,
                    target_value=f"{target_table.schema}.{target_table.table}",
                    status=ComparisonStatus.EXTRA,
                    risk_tier="LOW",
                    message=f"Extra {element_type} in target (may be intentional)"
                ))

        return comparisons

    def _compare_aggregations(
        self,
        source_aggs: List[AggregationInfo],
        target_aggs: List[AggregationInfo]
    ) -> List[LogicComparison]:
        """Compare aggregations between source and target."""
        comparisons = []

        source_funcs = {(a.function, a.column.lower()) for a in source_aggs}
        target_funcs = {(a.function, a.column.lower()) for a in target_aggs}

        # Missing aggregations
        for func, col in source_funcs - target_funcs:
            comparisons.append(LogicComparison(
                element_type="aggregation",
                source_value=f"{func}({col})",
                target_value=None,
                status=ComparisonStatus.MISSING,
                risk_tier="HIGH",
                message=f"Aggregation {func}({col}) not found in target"
            ))

        return comparisons

    def _compare_joins(
        self,
        source_joins: List[JoinInfo],
        target_joins: List[JoinInfo]
    ) -> List[LogicComparison]:
        """Compare JOIN operations."""
        comparisons = []

        # Compare count and types
        source_types = [j.join_type for j in source_joins]
        target_types = [j.join_type for j in target_joins]

        if len(source_joins) != len(target_joins):
            comparisons.append(LogicComparison(
                element_type="join_count",
                source_value=str(len(source_joins)),
                target_value=str(len(target_joins)),
                status=ComparisonStatus.PARTIAL,
                risk_tier="HIGH",
                message=f"Join count mismatch: source has {len(source_joins)}, target has {len(target_joins)}"
            ))

        # Check join types
        for i, source_type in enumerate(source_types):
            if i < len(target_types):
                if source_type != target_types[i]:
                    comparisons.append(LogicComparison(
                        element_type="join_type",
                        source_value=source_type,
                        target_value=target_types[i],
                        status=ComparisonStatus.PARTIAL,
                        risk_tier="HIGH",
                        message=f"Join type mismatch: source is {source_type}, target is {target_types[i]}"
                    ))

        return comparisons

    def _compare_filters(
        self,
        source_filters: List[FilterInfo],
        target_filters: List[FilterInfo]
    ) -> List[LogicComparison]:
        """Compare filter conditions."""
        comparisons = []

        if len(source_filters) > 0 and len(target_filters) == 0:
            comparisons.append(LogicComparison(
                element_type="filter",
                source_value=f"{len(source_filters)} filter(s)",
                target_value="0 filters",
                status=ComparisonStatus.MISSING,
                risk_tier="HIGH",
                message="Source has WHERE conditions but target has none"
            ))

        return comparisons

    def _compare_group_by(
        self,
        source_group: List[str],
        target_group: List[str]
    ) -> List[LogicComparison]:
        """Compare GROUP BY columns."""
        comparisons = []

        source_set = {g.lower() for g in source_group}
        target_set = {g.lower() for g in target_group}

        missing = source_set - target_set
        if missing:
            comparisons.append(LogicComparison(
                element_type="group_by",
                source_value=", ".join(missing),
                target_value=None,
                status=ComparisonStatus.MISSING,
                risk_tier="HIGH",
                message=f"GROUP BY columns missing in target: {', '.join(missing)}"
            ))

        return comparisons

    # ==================== Utility Helpers ====================

    def _dedupe_tables(self, tables: List[TableReference]) -> List[TableReference]:
        """Remove duplicate table references."""
        seen = set()
        result = []
        for t in tables:
            key = (t.schema.lower(), t.table.lower(), t.operation)
            if key not in seen:
                seen.add(key)
                result.append(t)
        return result

    def _split_column_list(self, columns_str: str) -> List[str]:
        """Split a column list, respecting parentheses."""
        result = []
        current = ""
        depth = 0

        for char in columns_str:
            if char == "," and depth == 0:
                if current.strip():
                    result.append(current.strip())
                current = ""
            else:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                current += char

        if current.strip():
            result.append(current.strip())

        return result


class BusinessLogicComparator:
    """
    High-level comparator for stored procedure migration validation.

    Combines BusinessLogicExtractor with additional context
    for comprehensive comparison.
    """

    def __init__(self, table_mapping: Optional[Dict[str, str]] = None):
        """
        Initialize comparator.

        Args:
            table_mapping: Mapping of source tables to Databricks tables
                e.g., {"dbo.Worker": "wakecap_prod.silver.silver_worker"}
        """
        self.extractor = BusinessLogicExtractor()
        self.table_mapping = table_mapping or {}

    def compare(
        self,
        source_sql: str,
        target_notebook: str,
        procedure_name: str = ""
    ) -> Dict:
        """
        Compare source T-SQL with target Databricks notebook.

        Args:
            source_sql: T-SQL stored procedure content
            target_notebook: Databricks notebook content
            procedure_name: Name of the procedure (for reporting)

        Returns:
            Comparison report dictionary
        """
        # Extract logic from both
        source_logic = self.extractor.extract_from_tsql(source_sql)
        target_logic = self.extractor.extract_from_notebook(target_notebook)

        # Compare
        comparisons = self.extractor.compare_logic(
            source_logic,
            target_logic,
            self.table_mapping
        )

        # Calculate scores
        blockers = [c for c in comparisons if c.risk_tier == "BLOCKER"]
        high_risk = [c for c in comparisons if c.risk_tier == "HIGH"]
        medium_risk = [c for c in comparisons if c.risk_tier == "MEDIUM"]
        low_risk = [c for c in comparisons if c.risk_tier == "LOW"]

        # Determine verdict
        if blockers:
            verdict = "FAIL"
        elif high_risk:
            verdict = "NEEDS_REVIEW"
        else:
            verdict = "PASS"

        return {
            "procedure_name": procedure_name,
            "source_logic": {
                "input_tables": len(source_logic.input_tables),
                "output_tables": len(source_logic.output_tables),
                "aggregations": len(source_logic.aggregations),
                "joins": len(source_logic.joins),
                "filters": len(source_logic.filters),
                "group_by": len(source_logic.group_by),
            },
            "target_logic": {
                "input_tables": len(target_logic.input_tables),
                "output_tables": len(target_logic.output_tables),
                "aggregations": len(target_logic.aggregations),
                "joins": len(target_logic.joins),
                "filters": len(target_logic.filters),
                "group_by": len(target_logic.group_by),
            },
            "comparisons": [
                {
                    "type": c.element_type,
                    "source": c.source_value,
                    "target": c.target_value,
                    "status": c.status.value,
                    "risk": c.risk_tier,
                    "message": c.message
                }
                for c in comparisons
            ],
            "summary": {
                "blockers": len(blockers),
                "high_risk": len(high_risk),
                "medium_risk": len(medium_risk),
                "low_risk": len(low_risk),
                "total_issues": len(comparisons),
            },
            "verdict": verdict
        }

    def generate_report(
        self,
        comparison_result: Dict
    ) -> str:
        """
        Generate markdown report from comparison result.

        Args:
            comparison_result: Result from compare()

        Returns:
            Markdown formatted report
        """
        lines = [
            f"## Business Logic Comparison: {comparison_result['procedure_name']}",
            "",
            f"**Verdict:** {comparison_result['verdict']}",
            "",
            "### Logic Summary",
            "",
            "| Metric | Source | Target |",
            "|--------|--------|--------|",
        ]

        for key in comparison_result["source_logic"]:
            source_val = comparison_result["source_logic"][key]
            target_val = comparison_result["target_logic"][key]
            match = "match" if source_val == target_val else "MISMATCH"
            lines.append(f"| {key} | {source_val} | {target_val} ({match}) |")

        lines.extend([
            "",
            "### Issues Found",
            "",
        ])

        if comparison_result["comparisons"]:
            lines.extend([
                "| Type | Source | Target | Risk | Message |",
                "|------|--------|--------|------|---------|",
            ])
            for comp in comparison_result["comparisons"]:
                lines.append(
                    f"| {comp['type']} | {comp['source'] or 'N/A'} | {comp['target'] or 'N/A'} | {comp['risk']} | {comp['message']} |"
                )
        else:
            lines.append("No issues found.")

        lines.extend([
            "",
            "### Summary",
            "",
            f"- Blockers: {comparison_result['summary']['blockers']}",
            f"- High Risk: {comparison_result['summary']['high_risk']}",
            f"- Medium Risk: {comparison_result['summary']['medium_risk']}",
            f"- Low Risk: {comparison_result['summary']['low_risk']}",
            f"- **Total Issues:** {comparison_result['summary']['total_issues']}",
        ])

        return "\n".join(lines)
