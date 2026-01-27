"""
Stored Procedure Converter for Lakebridge.

Converts T-SQL stored procedures to Databricks notebooks.
Includes:
- CURSOR to Window function conversion (Phase 1)
- MERGE statement generation (Phase 1)
- Temp table conversion (Phase 2)
- Spatial function stubs (Phase 2)
- Validation notebook generation (Phase 2)
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime
import re

# Import Phase 2 converters
try:
    from .temp_converter import TempTableConverter, TempTableInfo
    from .spatial_converter import SpatialConverter, SpatialFunctionInfo
    from .validation_generator import ValidationNotebookGenerator, ValidationConfig
    PHASE2_AVAILABLE = True
except ImportError:
    # Fallback for standalone usage
    PHASE2_AVAILABLE = False


@dataclass
class ConversionResult:
    """Result of stored procedure conversion."""
    procedure_name: str
    source_path: Optional[str]
    target_path: str
    notebook_content: str
    patterns_converted: List[str]
    patterns_manual: List[str]
    warnings: List[str]
    success: bool
    conversion_score: float = 0.0
    validation_notebook: Optional[str] = None  # Phase 2: Validation notebook content
    temp_tables_converted: int = 0  # Phase 2: Number of temp tables converted
    spatial_functions_converted: int = 0  # Phase 2: Number of spatial functions handled


class CursorConverter:
    """
    Convert T-SQL CURSOR patterns to Spark Window functions.

    Handles common cursor patterns:
    1. Simple iteration with FETCH NEXT
    2. Running totals and cumulative calculations
    3. Previous/next row comparisons
    4. Gap detection (session identification)
    5. Partition-based processing
    """

    CURSOR_PATTERNS = {
        "running_total": {
            "detect": r"SET\s+@\w+\s*=\s*@\w+\s*\+",
            "description": "Running total/cumulative sum",
        },
        "previous_row": {
            "detect": r"@prev\w+|@last\w+",
            "description": "Previous row comparison",
        },
        "next_row": {
            "detect": r"@next\w+",
            "description": "Next row comparison",
        },
        "row_number": {
            "detect": r"@row\w*\s*=\s*@row\w*\s*\+\s*1",
            "description": "Row numbering",
        },
        "gap_detection": {
            "detect": r"DATEDIFF.*@prev|gap|island|session",
            "description": "Gap/Island detection (session identification)",
        },
        "sequence_detection": {
            "detect": r"@sequence|@seq\w*\s*=",
            "description": "Sequence numbering",
        },
    }

    def extract_cursor_definition(self, sql: str) -> Optional[Dict]:
        """
        Extract cursor definition details.

        Returns:
            Dict with cursor_name, select_sql, partition_cols, order_cols, fetch_variables
        """
        # Match DECLARE cursor pattern
        cursor_pattern = r"DECLARE\s+@?(\w+)\s+CURSOR\s+(?:LOCAL\s+|GLOBAL\s+|FORWARD_ONLY\s+|SCROLL\s+|STATIC\s+|KEYSET\s+|DYNAMIC\s+|FAST_FORWARD\s+|READ_ONLY\s+|SCROLL_LOCKS\s+|OPTIMISTIC\s+)*FOR\s+([\s\S]+?)(?=OPEN\s+@?\1)"

        match = re.search(cursor_pattern, sql, re.IGNORECASE)
        if not match:
            return None

        cursor_name = match.group(1)
        select_sql = match.group(2).strip()

        # Extract ORDER BY columns
        order_match = re.search(r"ORDER\s+BY\s+([\w\s,.\[\]]+?)(?:$|;|\))", select_sql, re.IGNORECASE)
        order_cols = []
        if order_match:
            order_str = order_match.group(1)
            order_cols = [col.strip().split()[-1].replace('[', '').replace(']', '')
                         for col in order_str.split(',')]

        # Extract partition-like columns from WHERE or GROUP BY
        partition_cols = []
        group_match = re.search(r"GROUP\s+BY\s+([\w\s,.\[\]]+?)(?:$|ORDER|HAVING|;)", select_sql, re.IGNORECASE)
        if group_match:
            partition_str = group_match.group(1)
            partition_cols = [col.strip().split('.')[-1].replace('[', '').replace(']', '')
                             for col in partition_str.split(',')]

        # Extract FETCH INTO variables
        fetch_pattern = r"FETCH\s+NEXT\s+FROM\s+@?" + cursor_name + r"\s+INTO\s+([\s\S]+?)(?:$|;|\n)"
        fetch_match = re.search(fetch_pattern, sql, re.IGNORECASE)
        fetch_variables = []
        if fetch_match:
            fetch_str = fetch_match.group(1)
            fetch_variables = [v.strip() for v in fetch_str.split(',')]

        return {
            "cursor_name": cursor_name,
            "select_sql": select_sql,
            "partition_cols": partition_cols,
            "order_cols": order_cols,
            "fetch_variables": fetch_variables,
        }

    def extract_loop_body(self, sql: str, cursor_name: str) -> Optional[str]:
        """Extract the WHILE @@FETCH_STATUS = 0 loop body."""
        # Match WHILE loop
        while_pattern = r"WHILE\s+@@FETCH_STATUS\s*=\s*0\s*BEGIN([\s\S]+?)END\s*(?:;|$)"

        match = re.search(while_pattern, sql, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def detect_cursor_pattern(self, loop_body: str) -> List[str]:
        """Detect which cursor patterns are being used."""
        detected = []
        for pattern_name, pattern_info in self.CURSOR_PATTERNS.items():
            if re.search(pattern_info["detect"], loop_body, re.IGNORECASE):
                detected.append(pattern_name)
        return detected

    def convert(self, cursor_sql: str, context: Dict) -> Tuple[str, List[str]]:
        """
        Convert CURSOR SQL to Window function code.

        Args:
            cursor_sql: T-SQL containing CURSOR definition and loop
            context: Dict with partition_cols, order_cols, source_table, etc.

        Returns:
            Tuple of (converted_code, warnings)
        """
        warnings = []

        # Extract cursor definition
        cursor_def = self.extract_cursor_definition(cursor_sql)
        if not cursor_def:
            warnings.append("Could not parse cursor definition - manual conversion required")
            return self._generate_manual_stub(cursor_sql), warnings

        # Extract loop body
        loop_body = self.extract_loop_body(cursor_sql, cursor_def["cursor_name"])
        if not loop_body:
            warnings.append("Could not extract WHILE loop body - manual conversion required")
            return self._generate_manual_stub(cursor_sql), warnings

        # Detect patterns
        patterns = self.detect_cursor_pattern(loop_body)

        # Get configuration from context
        partition_cols = context.get("partition_cols") or cursor_def["partition_cols"]
        order_cols = context.get("order_cols") or cursor_def["order_cols"]
        source_table = context.get("source_table", "source_df")

        # Generate Window function code based on patterns
        if "gap_detection" in patterns or "sequence_detection" in patterns:
            return self._generate_gap_detection(partition_cols, order_cols, source_table, context), warnings
        elif "running_total" in patterns:
            return self._generate_running_total(partition_cols, order_cols, source_table, context), warnings
        elif "previous_row" in patterns:
            return self._generate_previous_row(partition_cols, order_cols, source_table, context), warnings
        elif "row_number" in patterns:
            return self._generate_row_number(partition_cols, order_cols, source_table, context), warnings
        else:
            warnings.append(f"Unrecognized cursor pattern - generating generic Window stub")
            return self._generate_generic_window(partition_cols, order_cols, source_table, context), warnings

    def _format_cols(self, cols: List[str]) -> str:
        """Format column list for partition/order by."""
        if not cols:
            return '""'
        return ', '.join(f'"{c}"' for c in cols)

    def _generate_gap_detection(self, partition_cols: List[str], order_cols: List[str],
                                 source_table: str, context: Dict) -> str:
        """Generate gap/island detection code (session identification)."""
        timestamp_col = context.get("timestamp_col", order_cols[0] if order_cols else "TimestampUTC")
        gap_threshold = context.get("gap_threshold_seconds", 300)

        partition_str = self._format_cols(partition_cols) if partition_cols else '"ProjectId", "WorkerId"'
        order_str = self._format_cols(order_cols) if order_cols else f'"{timestamp_col}"'

        return f'''
# Gap/Island Detection (Session Identification)
# Converted from CURSOR-based iteration
# Original pattern: Check time difference between consecutive rows

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy({partition_str}).orderBy({order_str})

# Calculate time difference from previous row
df_with_gap = (
    {source_table}
    .withColumn("prev_timestamp", F.lag("{timestamp_col}").over(window_spec))
    .withColumn(
        "gap_seconds",
        F.unix_timestamp(F.col("{timestamp_col}")) - F.unix_timestamp(F.col("prev_timestamp"))
    )
)

# Identify new sessions where gap exceeds threshold ({gap_threshold} seconds)
df_with_sessions = (
    df_with_gap
    .withColumn(
        "new_session",
        F.when(
            (F.col("gap_seconds") > {gap_threshold}) | F.col("prev_timestamp").isNull(),
            1
        ).otherwise(0)
    )
    .withColumn(
        "SessionId",
        F.sum("new_session").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
    .drop("prev_timestamp", "gap_seconds", "new_session")
)

print(f"Session identification complete: {{df_with_sessions.count():,}} rows processed")
'''

    def _generate_running_total(self, partition_cols: List[str], order_cols: List[str],
                                 source_table: str, context: Dict) -> str:
        """Generate running total/cumulative sum code."""
        value_col = context.get("value_col", "Amount")
        result_col = context.get("result_col", "RunningTotal")

        partition_str = self._format_cols(partition_cols) if partition_cols else '"ProjectId"'
        order_str = self._format_cols(order_cols) if order_cols else '"CreatedAt"'

        return f'''
# Running Total / Cumulative Sum
# Converted from CURSOR-based iteration with accumulator variable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification with running sum frame
window_spec = Window.partitionBy({partition_str}).orderBy({order_str})
running_frame = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate running total
result_df = (
    {source_table}
    .withColumn(
        "{result_col}",
        F.sum("{value_col}").over(running_frame)
    )
)

print(f"Running total calculated: {{result_df.count():,}} rows")
'''

    def _generate_previous_row(self, partition_cols: List[str], order_cols: List[str],
                                source_table: str, context: Dict) -> str:
        """Generate previous row comparison code."""
        compare_cols = context.get("compare_cols", order_cols[:1] if order_cols else ["Value"])

        partition_str = self._format_cols(partition_cols) if partition_cols else '"ProjectId"'
        order_str = self._format_cols(order_cols) if order_cols else '"CreatedAt"'

        lag_columns = '\n    '.join([
            f'.withColumn("prev_{col}", F.lag("{col}", 1).over(window_spec))'
            for col in compare_cols
        ])

        return f'''
# Previous Row Comparison
# Converted from CURSOR-based iteration with @prev variables

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy({partition_str}).orderBy({order_str})

# Add previous row values using lag()
result_df = (
    {source_table}
    {lag_columns}
)

print(f"Previous row comparison added: {{result_df.count():,}} rows")
'''

    def _generate_row_number(self, partition_cols: List[str], order_cols: List[str],
                              source_table: str, context: Dict) -> str:
        """Generate row numbering code."""
        partition_str = self._format_cols(partition_cols) if partition_cols else '"ProjectId"'
        order_str = self._format_cols(order_cols) if order_cols else '"CreatedAt"'

        return f'''
# Row Numbering
# Converted from CURSOR-based counter variable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy({partition_str}).orderBy({order_str})

# Add row number
result_df = (
    {source_table}
    .withColumn("RowNum", F.row_number().over(window_spec))
)

print(f"Row numbers assigned: {{result_df.count():,}} rows")
'''

    def _generate_generic_window(self, partition_cols: List[str], order_cols: List[str],
                                  source_table: str, context: Dict) -> str:
        """Generate generic Window function stub."""
        partition_str = self._format_cols(partition_cols) if partition_cols else '"ProjectId"'
        order_str = self._format_cols(order_cols) if order_cols else '"CreatedAt"'

        return f'''
# Window Function Conversion
# TODO: Review and customize based on original CURSOR logic

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy({partition_str}).orderBy({order_str})

# Common window function patterns:
# - F.row_number().over(window_spec) - Sequential numbering
# - F.lag("col", 1).over(window_spec) - Previous row value
# - F.lead("col", 1).over(window_spec) - Next row value
# - F.sum("col").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)) - Running sum
# - F.rank().over(window_spec) - Ranking with gaps
# - F.dense_rank().over(window_spec) - Ranking without gaps

result_df = (
    {source_table}
    .withColumn("row_num", F.row_number().over(window_spec))
    # Add more window columns as needed based on original logic
)

print(f"Window functions applied: {{result_df.count():,}} rows")
'''

    def _generate_manual_stub(self, cursor_sql: str) -> str:
        """Generate stub for manual conversion."""
        # Extract first 200 chars of cursor SQL for reference
        sql_preview = cursor_sql[:200].replace('\n', ' ').replace('  ', ' ')

        return f'''
# CURSOR Conversion Required - Manual Review Needed
#
# Original T-SQL (preview):
# {sql_preview}...
#
# The cursor pattern could not be automatically converted.
# Please review the original T-SQL and implement using:
# - Window functions (lag, lead, row_number, sum over partition)
# - DataFrame operations (.groupBy(), .agg())
# - Collect + Python iteration (for complex logic)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# TODO: Implement cursor logic using Window functions
# window_spec = Window.partitionBy("partition_col").orderBy("order_col")
# result_df = source_df.withColumn("result", F.xxx().over(window_spec))

raise NotImplementedError("CURSOR conversion requires manual implementation")
'''


class MergeConverter:
    """
    Convert T-SQL MERGE statements to Delta Lake MERGE.

    Handles:
    1. WHEN MATCHED THEN UPDATE
    2. WHEN NOT MATCHED THEN INSERT
    3. WHEN NOT MATCHED BY SOURCE THEN DELETE
    4. Multiple WHEN clauses with conditions
    """

    def extract_merge_components(self, merge_sql: str) -> Optional[Dict]:
        """
        Extract MERGE statement components.

        Returns:
            Dict with target_table, target_alias, source_table, source_alias,
            join_condition, when_matched, when_not_matched, when_not_matched_by_source
        """
        # Match main MERGE structure
        merge_pattern = r"MERGE\s+(?:INTO\s+)?(?:\[?(\w+)\]?\.)*\[?(\w+)\]?\s+(?:AS\s+)?(\w+)\s*[\r\n]+\s*USING\s+([\s\S]+?)\s+(?:AS\s+)?(\w+)\s*[\r\n]+\s*ON\s+([\s\S]+?)(?=WHEN)"

        match = re.search(merge_pattern, merge_sql, re.IGNORECASE)
        if not match:
            return None

        target_schema = match.group(1) or ""
        target_table = match.group(2)
        target_alias = match.group(3)
        source_expr = match.group(4).strip()
        source_alias = match.group(5)
        join_condition = match.group(6).strip()

        # Clean join condition
        join_condition = re.sub(r'\s+', ' ', join_condition).strip()

        # Extract WHEN MATCHED clauses
        when_matched = []
        matched_pattern = r"WHEN\s+MATCHED\s+(?:AND\s+([\s\S]+?)\s+)?THEN\s+UPDATE\s+SET\s+([\s\S]+?)(?=WHEN|;|$)"
        for m in re.finditer(matched_pattern, merge_sql, re.IGNORECASE):
            condition = m.group(1).strip() if m.group(1) else None
            set_clause = m.group(2).strip()
            when_matched.append({
                "condition": condition,
                "action": "UPDATE",
                "set_clause": set_clause,
            })

        # Extract WHEN NOT MATCHED clauses (INSERT)
        when_not_matched = []
        not_matched_pattern = r"WHEN\s+NOT\s+MATCHED\s+(?:BY\s+TARGET\s+)?(?:AND\s+([\s\S]+?)\s+)?THEN\s+INSERT\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)"
        for m in re.finditer(not_matched_pattern, merge_sql, re.IGNORECASE):
            condition = m.group(1).strip() if m.group(1) else None
            columns = [c.strip().replace('[', '').replace(']', '') for c in m.group(2).split(',')]
            values = [v.strip() for v in m.group(3).split(',')]
            when_not_matched.append({
                "condition": condition,
                "columns": columns,
                "values": values,
            })

        # Extract WHEN NOT MATCHED BY SOURCE clauses (DELETE)
        when_not_matched_by_source = []
        by_source_pattern = r"WHEN\s+NOT\s+MATCHED\s+BY\s+SOURCE\s+(?:AND\s+([\s\S]+?)\s+)?THEN\s+(DELETE|UPDATE)"
        for m in re.finditer(by_source_pattern, merge_sql, re.IGNORECASE):
            condition = m.group(1).strip() if m.group(1) else None
            action = m.group(2).upper()
            when_not_matched_by_source.append({
                "condition": condition,
                "action": action,
            })

        return {
            "target_schema": target_schema,
            "target_table": target_table,
            "target_alias": target_alias,
            "source_expr": source_expr,
            "source_alias": source_alias,
            "join_condition": join_condition,
            "when_matched": when_matched,
            "when_not_matched": when_not_matched,
            "when_not_matched_by_source": when_not_matched_by_source,
        }

    def convert(self, merge_sql: str, target_table_full: str, source_df_name: str = "source_df") -> Tuple[str, List[str]]:
        """
        Convert T-SQL MERGE to Delta MERGE.

        Args:
            merge_sql: T-SQL MERGE statement
            target_table_full: Full Databricks target table name (catalog.schema.table)
            source_df_name: Name of source DataFrame variable

        Returns:
            Tuple of (Python code for Delta MERGE, warnings)
        """
        warnings = []

        components = self.extract_merge_components(merge_sql)
        if not components:
            warnings.append("Could not parse MERGE statement - manual conversion required")
            return self._generate_manual_stub(merge_sql), warnings

        # Generate Delta MERGE code
        code_lines = [
            "# Delta MERGE Operation",
            "# Converted from T-SQL MERGE statement",
            "",
            "from delta.tables import DeltaTable",
            "",
            f'# Get target Delta table',
            f'target_table = DeltaTable.forName(spark, "{target_table_full}")',
            "",
        ]

        # Build merge condition - convert T-SQL aliases
        join_condition = components["join_condition"]
        join_condition = self._convert_condition(join_condition, components["target_alias"], components["source_alias"])

        code_lines.append(f'# Merge condition')
        code_lines.append(f'merge_condition = """{join_condition}"""')
        code_lines.append("")

        # Start MERGE chain
        code_lines.append("# Execute MERGE")
        code_lines.append("(")
        code_lines.append(f'    target_table.alias("{components["target_alias"]}")')
        code_lines.append(f'    .merge(')
        code_lines.append(f'        {source_df_name}.alias("{components["source_alias"]}"),')
        code_lines.append(f'        merge_condition')
        code_lines.append(f'    )')

        # Add WHEN MATCHED clauses
        for wm in components["when_matched"]:
            condition_str = f'"{self._convert_condition(wm["condition"], components["target_alias"], components["source_alias"])}"' if wm["condition"] else "None"
            set_dict = self._parse_set_clause(wm["set_clause"], components["target_alias"], components["source_alias"])
            code_lines.append(f'    .whenMatchedUpdate(')
            code_lines.append(f'        condition={condition_str},')
            code_lines.append(f'        set={{')
            for col, val in set_dict.items():
                code_lines.append(f'            "{col}": "{val}",')
            code_lines.append(f'        }}')
            code_lines.append(f'    )')

        # Add WHEN NOT MATCHED clauses
        for wnm in components["when_not_matched"]:
            condition_str = f'"{self._convert_condition(wnm["condition"], components["target_alias"], components["source_alias"])}"' if wnm["condition"] else "None"
            values_dict = dict(zip(wnm["columns"], wnm["values"]))
            code_lines.append(f'    .whenNotMatchedInsert(')
            code_lines.append(f'        condition={condition_str},')
            code_lines.append(f'        values={{')
            for col, val in values_dict.items():
                # Convert value references
                val_converted = self._convert_value_ref(val, components["source_alias"])
                code_lines.append(f'            "{col}": "{val_converted}",')
            code_lines.append(f'        }}')
            code_lines.append(f'    )')

        # Add WHEN NOT MATCHED BY SOURCE clauses
        for wnmbs in components["when_not_matched_by_source"]:
            if wnmbs["action"] == "DELETE":
                condition_str = f'"{self._convert_condition(wnmbs["condition"], components["target_alias"], components["source_alias"])}"' if wnmbs["condition"] else "None"
                code_lines.append(f'    .whenNotMatchedBySourceDelete(')
                code_lines.append(f'        condition={condition_str}')
                code_lines.append(f'    )')

        code_lines.append(f'    .execute()')
        code_lines.append(")")
        code_lines.append("")
        code_lines.append(f'print(f"MERGE completed: {{{source_df_name}.count():,}} source rows processed")')

        return "\n".join(code_lines), warnings

    def _convert_condition(self, condition: Optional[str], target_alias: str, source_alias: str) -> str:
        """Convert T-SQL condition to Spark SQL compatible format."""
        if not condition:
            return ""

        # Replace square brackets
        result = condition.replace('[', '').replace(']', '')
        # Ensure aliases are lowercase for consistency
        result = re.sub(rf'\b{target_alias}\b', target_alias.lower(), result, flags=re.IGNORECASE)
        result = re.sub(rf'\b{source_alias}\b', source_alias.lower(), result, flags=re.IGNORECASE)
        return result.strip()

    def _convert_value_ref(self, value: str, source_alias: str) -> str:
        """Convert T-SQL value reference to Spark format."""
        value = value.strip().replace('[', '').replace(']', '')

        # Handle GETUTCDATE() -> current_timestamp()
        value = re.sub(r'GETUTCDATE\s*\(\)', 'current_timestamp()', value, flags=re.IGNORECASE)
        value = re.sub(r'GETDATE\s*\(\)', 'current_timestamp()', value, flags=re.IGNORECASE)

        # Ensure source alias references
        if '.' not in value and not value.endswith('()'):
            # Likely a column reference from source
            value = f"{source_alias.lower()}.{value}"

        return value

    def _parse_set_clause(self, set_clause: str, target_alias: str, source_alias: str) -> Dict[str, str]:
        """Parse SET clause into column -> value mapping."""
        result = {}

        # Split by comma, but be careful of commas inside function calls
        assignments = re.split(r',\s*(?![^()]*\))', set_clause)

        for assignment in assignments:
            if '=' not in assignment:
                continue
            parts = assignment.split('=', 1)
            if len(parts) != 2:
                continue

            col = parts[0].strip().replace('[', '').replace(']', '')
            val = parts[1].strip()

            # Remove target alias prefix from column name
            col = re.sub(rf'^{target_alias}\.', '', col, flags=re.IGNORECASE)

            # Convert value
            val = self._convert_value_ref(val, source_alias)

            result[col] = val

        return result

    def _generate_manual_stub(self, merge_sql: str) -> str:
        """Generate stub for manual conversion."""
        sql_preview = merge_sql[:300].replace('\n', ' ').replace('  ', ' ')

        return f'''
# MERGE Conversion Required - Manual Review Needed
#
# Original T-SQL (preview):
# {sql_preview}...
#
# Please convert manually using Delta Lake MERGE:

from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "catalog.schema.table")

# Example MERGE pattern:
# (
#     target_table.alias("target")
#     .merge(
#         source_df.alias("source"),
#         "target.id = source.id"
#     )
#     .whenMatchedUpdateAll()
#     .whenNotMatchedInsertAll()
#     .execute()
# )

raise NotImplementedError("MERGE conversion requires manual implementation")
'''


class SPConverter:
    """
    Convert T-SQL stored procedures to Databricks notebooks.

    Main orchestrator that coordinates pattern detection and conversion.

    Phase 1 Features:
    - CURSOR to Window function conversion
    - MERGE statement generation

    Phase 2 Features:
    - Temp table to temp view conversion
    - Spatial function stub generation
    - Validation notebook generation
    """

    def __init__(self, generate_validation: bool = True):
        """
        Initialize the SP Converter.

        Args:
            generate_validation: If True, also generate validation notebook (Phase 2)
        """
        # Phase 1 converters
        self.cursor_converter = CursorConverter()
        self.merge_converter = MergeConverter()

        # Phase 2 converters (if available)
        self.generate_validation = generate_validation
        if PHASE2_AVAILABLE:
            self.temp_converter = TempTableConverter()
            self.spatial_converter = SpatialConverter()
            self.validation_generator = ValidationNotebookGenerator()
        else:
            self.temp_converter = None
            self.spatial_converter = None
            self.validation_generator = None

        self._pattern_handlers = {
            "CURSOR": self._convert_cursor,
            "FETCH_CURSOR": self._convert_cursor,
            "TEMP_TABLE": self._convert_temp_table,
            "GLOBAL_TEMP": self._convert_temp_table,
            "MERGE": self._convert_merge,
            "DYNAMIC_SQL": self._convert_dynamic_sql,
            "SPATIAL_GEOGRAPHY": self._convert_spatial,
            "SPATIAL_GEOMETRY": self._convert_spatial,
            "WHILE_LOOP": self._convert_while_loop,
            "TRANSACTION": self._convert_transaction,
        }

    def convert(
        self,
        source_sql: str,
        procedure_name: str,
        target_catalog: str = "wakecap_prod",
        target_schema: str = "gold",
        target_table: Optional[str] = None,
        source_tables_mapping: Optional[Dict[str, str]] = None,
    ) -> ConversionResult:
        """
        Convert a stored procedure to a Databricks notebook.

        Args:
            source_sql: T-SQL stored procedure content
            procedure_name: Full procedure name (e.g., "stg.spCalculateFactWorkersShifts")
            target_catalog: Databricks catalog name
            target_schema: Target schema for output tables
            target_table: Target table name (auto-generated if not provided)
            source_tables_mapping: Optional mapping of source tables to Databricks tables

        Returns:
            ConversionResult with notebook content and conversion metadata
        """
        warnings = []
        patterns_converted = []
        patterns_manual = []

        # Detect patterns
        detected_patterns = self._detect_patterns(source_sql)

        # Generate target table name if not provided
        if not target_table:
            # Convert procedure name to table name
            # e.g., stg.spCalculateFactWorkersShifts -> gold_fact_workers_shifts
            clean_name = procedure_name.split('.')[-1]  # Remove schema
            clean_name = re.sub(r'^sp', '', clean_name, flags=re.IGNORECASE)  # Remove sp prefix
            clean_name = re.sub(r'([A-Z])', r'_\1', clean_name).lower().strip('_')  # CamelCase to snake_case
            target_table = f"gold_{clean_name}"

        # Generate target path
        target_filename = target_table.replace('gold_', '') + ".py"
        target_path = f"notebooks/{target_filename}"

        # Build notebook sections
        sections = []

        # 1. Header
        sections.append(self._generate_header(procedure_name, source_sql, list(detected_patterns.keys())))

        # 2. Configuration
        sections.append(self._generate_config_section(procedure_name, target_catalog, target_schema, target_table))

        # 3. Pre-flight checks
        sections.append(self._generate_preflight_section(target_catalog, target_schema, source_tables_mapping))

        # 4. Helper functions
        sections.append(self._generate_helpers_section())

        # 5. Convert each detected pattern
        context = {
            "target_catalog": target_catalog,
            "target_schema": target_schema,
            "target_table": target_table,
            "source_tables_mapping": source_tables_mapping or {},
        }

        for pattern_name, matches in detected_patterns.items():
            handler = self._pattern_handlers.get(pattern_name)
            if handler:
                try:
                    converted_code, pattern_warnings = handler(source_sql, matches, context)
                    sections.append(converted_code)
                    patterns_converted.append(pattern_name)
                    warnings.extend(pattern_warnings)
                except Exception as e:
                    warnings.append(f"Error converting {pattern_name}: {str(e)}")
                    patterns_manual.append(pattern_name)
            else:
                # Low-risk patterns that don't need specific handling
                if pattern_name not in ["TRANSACTION", "TRY_CATCH", "ROWCOUNT"]:
                    patterns_manual.append(pattern_name)

        # 6. Main transformation placeholder if no patterns converted
        if not patterns_converted:
            sections.append(self._generate_transformation_placeholder(source_sql, context))

        # Phase 2: Enhanced temp table conversion using TempTableConverter
        temp_tables_converted = 0
        if self.temp_converter and "TEMP_TABLE" in detected_patterns:
            try:
                temp_tables = self.temp_converter.detect_temp_tables(source_sql)
                if temp_tables:
                    temp_code, temp_warnings = self.temp_converter.convert_all(source_sql, context)
                    if temp_code:
                        # Insert after helpers section (index 3)
                        sections.insert(4, temp_code)
                        temp_tables_converted = len(temp_tables)
                        warnings.extend(temp_warnings)
            except Exception as e:
                warnings.append(f"Temp table conversion warning: {str(e)}")

        # Phase 2: Enhanced spatial conversion using SpatialConverter
        spatial_functions_converted = 0
        if self.spatial_converter and ("SPATIAL_GEOGRAPHY" in detected_patterns or "SPATIAL_GEOMETRY" in detected_patterns):
            try:
                spatial_funcs = self.spatial_converter.detect_spatial_functions(source_sql)
                if spatial_funcs:
                    spatial_code, spatial_warnings = self.spatial_converter.convert(spatial_funcs, context)
                    if spatial_code:
                        sections.append(spatial_code)
                        spatial_functions_converted = len(spatial_funcs)
                        warnings.extend(spatial_warnings)
            except Exception as e:
                warnings.append(f"Spatial conversion warning: {str(e)}")

        # 7. Write section
        sections.append(self._generate_write_section(target_catalog, target_schema, target_table))

        # 8. Summary section
        sections.append(self._generate_summary_section(procedure_name))

        # Combine all sections
        notebook_content = "\n\n".join(sections)

        # Calculate conversion score
        total_patterns = len(detected_patterns)
        if total_patterns > 0:
            conversion_score = len(patterns_converted) / total_patterns
        else:
            conversion_score = 0.8  # Default score if no patterns detected

        # Phase 2: Generate validation notebook
        validation_notebook = None
        if self.generate_validation and self.validation_generator:
            try:
                source_table = f"{target_catalog}.silver.silver_{target_table.replace('gold_', '')}"
                target_table_full = f"{target_catalog}.{target_schema}.{target_table}"

                validation_notebook = self.validation_generator.generate_from_conversion(
                    procedure_name=procedure_name,
                    source_table=source_table,
                    target_table=target_table_full,
                )
            except Exception as e:
                warnings.append(f"Validation notebook generation warning: {str(e)}")

        return ConversionResult(
            procedure_name=procedure_name,
            source_path=None,
            target_path=target_path,
            notebook_content=notebook_content,
            patterns_converted=patterns_converted,
            patterns_manual=patterns_manual,
            warnings=warnings,
            success=len(patterns_manual) == 0,
            conversion_score=conversion_score,
            validation_notebook=validation_notebook,
            temp_tables_converted=temp_tables_converted,
            spatial_functions_converted=spatial_functions_converted,
        )

    def _detect_patterns(self, source_sql: str) -> Dict[str, List[Dict]]:
        """Detect all T-SQL patterns in source."""
        # Import pattern definitions from analyzer
        from .analyzer_patterns import SP_SOURCE_PATTERNS

        results = {}
        lines = source_sql.split('\n')

        for pattern_name, pattern_info in SP_SOURCE_PATTERNS.items():
            regex = pattern_info[0]
            matches = []
            for match in re.finditer(regex, source_sql, re.IGNORECASE | re.MULTILINE):
                line_num = source_sql[:match.start()].count('\n') + 1
                start_line = max(0, line_num - 4)
                end_line = min(len(lines), line_num + 3)
                context = '\n'.join(lines[start_line:end_line])

                matches.append({
                    "line_number": line_num,
                    "matched_text": match.group(0)[:100],
                    "context": context,
                })

            if matches:
                results[pattern_name] = matches

        return results

    def _generate_header(self, procedure_name: str, source_sql: str, patterns: List[str]) -> str:
        """Generate notebook header with metadata."""
        line_count = len(source_sql.split('\n'))
        patterns_str = ', '.join(patterns) if patterns else 'None detected'

        # Extract procedure description from comments
        desc_match = re.search(r'--\s*Purpose:\s*(.+)', source_sql, re.IGNORECASE)
        purpose = desc_match.group(1).strip() if desc_match else "Converted from T-SQL stored procedure"

        return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # {procedure_name.split('.')[-1]}
# MAGIC
# MAGIC **Converted from:** `{procedure_name}` ({line_count} lines)
# MAGIC
# MAGIC **Purpose:** {purpose}
# MAGIC
# MAGIC **Original Patterns:** {patterns_str}
# MAGIC
# MAGIC **Conversion Date:** {datetime.now().strftime("%Y-%m-%d")}
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Conversion Notes
# MAGIC
# MAGIC This notebook was auto-generated from the original T-SQL stored procedure.
# MAGIC Review sections marked with `TODO` or `MANUAL REVIEW REQUIRED`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

start_time = datetime.now()
rows_processed = 0'''

    def _generate_config_section(self, procedure_name: str, target_catalog: str,
                                  target_schema: str, target_table: str) -> str:
        """Generate configuration and widget parameters section."""
        return f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and Schema Configuration
TARGET_CATALOG = "{target_catalog}"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "{target_schema}"
MIGRATION_SCHEMA = "migration"

# Target table
TARGET_TABLE = f"{{TARGET_CATALOG}}.{{TARGET_SCHEMA}}.{target_table}"
WATERMARK_TABLE = f"{{TARGET_CATALOG}}.{{MIGRATION_SCHEMA}}._gold_watermarks"

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.text("lookback_days", "7", "Lookback Days for Incremental")
dbutils.widgets.text("project_id", "", "Project ID (optional filter)")

load_mode = dbutils.widgets.get("load_mode")
lookback_days = int(dbutils.widgets.get("lookback_days"))
project_filter = dbutils.widgets.get("project_id")

print(f"Load Mode: {{load_mode}}")
print(f"Lookback Days: {{lookback_days}}")
print(f"Project Filter: {{project_filter if project_filter else 'None'}}")'''

    def _generate_preflight_section(self, target_catalog: str, target_schema: str,
                                     source_tables_mapping: Optional[Dict[str, str]]) -> str:
        """Generate pre-flight checks section."""
        return f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check

# COMMAND ----------

print("=" * 60)
print("PRE-FLIGHT CHECK")
print("=" * 60)

# Check target schema exists
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{TARGET_CATALOG}}.{{TARGET_SCHEMA}}")
    print(f"[OK] Target schema exists: {{TARGET_CATALOG}}.{{TARGET_SCHEMA}}")
except Exception as e:
    print(f"[WARN] Could not create/verify schema: {{str(e)[:50]}}")

# Check watermark table
try:
    spark.sql(f"SELECT 1 FROM {{WATERMARK_TABLE}} LIMIT 0")
    print(f"[OK] Watermark table exists: {{WATERMARK_TABLE}}")
except Exception as e:
    print(f"[INFO] Creating watermark table: {{WATERMARK_TABLE}}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {{WATERMARK_TABLE}} (
            table_name STRING,
            last_processed_at TIMESTAMP,
            last_watermark_value TIMESTAMP,
            row_count BIGINT,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)

print("=" * 60)'''

    def _generate_helpers_section(self) -> str:
        """Generate helper functions section."""
        return '''# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_watermark(table_name):
    """Get last processed watermark for incremental loading."""
    try:
        result = spark.sql(f"""
            SELECT last_watermark_value
            FROM {WATERMARK_TABLE}
            WHERE table_name = '{table_name}'
        """).collect()
        if result and result[0][0]:
            return result[0][0]
    except Exception as e:
        print(f"  Watermark lookup failed: {str(e)[:50]}")
    return datetime(1900, 1, 1)


def update_watermark(table_name, watermark_value, row_count):
    """Update watermark after successful processing."""
    try:
        spark.sql(f"""
            MERGE INTO {WATERMARK_TABLE} AS target
            USING (SELECT '{table_name}' as table_name,
                          CAST('{watermark_value}' AS TIMESTAMP) as last_watermark_value,
                          {row_count} as row_count,
                          current_timestamp() as last_processed_at,
                          current_timestamp() as updated_at) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET
                target.last_watermark_value = source.last_watermark_value,
                target.row_count = source.row_count,
                target.last_processed_at = source.last_processed_at,
                target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        print(f"  Warning: Could not update watermark: {str(e)[:50]}")'''

    def _convert_cursor(self, source_sql: str, matches: List[Dict], context: Dict) -> Tuple[str, List[str]]:
        """Convert CURSOR pattern to Window functions."""
        code, warnings = self.cursor_converter.convert(source_sql, context)

        section = f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## CURSOR Conversion
# MAGIC
# MAGIC The original T-SQL used CURSOR for row-by-row processing.
# MAGIC Converted to Spark Window functions for efficient parallel processing.

# COMMAND ----------

{code}'''

        return section, warnings

    def _convert_temp_table(self, source_sql: str, matches: List[Dict], context: Dict) -> Tuple[str, List[str]]:
        """Convert temp table to createOrReplaceTempView using Phase 2 TempTableConverter if available."""
        warnings = []

        # Use Phase 2 converter if available
        if self.temp_converter:
            try:
                temp_tables = self.temp_converter.detect_temp_tables(source_sql)
                if temp_tables:
                    code_sections = []
                    for temp_info in temp_tables[:10]:  # Limit to 10
                        temp_code, temp_warnings = self.temp_converter.convert(temp_info, context)
                        code_sections.append(temp_code)
                        warnings.extend(temp_warnings)

                    if len(temp_tables) > 10:
                        warnings.append(f"Found {len(temp_tables)} temp tables - converting first 10")

                    section = f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporary Table Conversion
# MAGIC
# MAGIC T-SQL temp tables converted to Spark temp views.
# MAGIC Found {len(temp_tables)} temp table(s): {', '.join(t.name for t in temp_tables[:10])}

# COMMAND ----------

{"".join(code_sections)}'''

                    return section, warnings
            except Exception as e:
                warnings.append(f"Phase 2 temp converter error, falling back: {str(e)}")

        # Fallback: Basic temp table detection (Phase 1 behavior)
        temp_tables = re.findall(r'#(\w+)', source_sql)
        unique_temps = list(set(temp_tables))

        code_lines = []
        for temp_name in unique_temps[:5]:  # Limit to first 5
            code_lines.append(f'''
# Temp table #{temp_name} → Spark temp view
# TODO: Populate {temp_name}_df with appropriate query
# {temp_name}_df = spark.sql("""
#     SELECT * FROM source_table WHERE condition
# """)
# {temp_name}_df.createOrReplaceTempView("{temp_name}")
''')

        if len(unique_temps) > 5:
            warnings.append(f"Found {len(unique_temps)} temp tables - showing first 5")

        section = f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporary Table Conversion
# MAGIC
# MAGIC T-SQL temp tables converted to Spark temp views.
# MAGIC Temp tables found: {', '.join(unique_temps[:10])}

# COMMAND ----------

{"".join(code_lines)}'''

        return section, warnings

    def _convert_merge(self, source_sql: str, matches: List[Dict], context: Dict) -> Tuple[str, List[str]]:
        """Convert MERGE statement to Delta MERGE."""
        target_table_full = f"{context['target_catalog']}.{context['target_schema']}.{context['target_table']}"
        code, warnings = self.merge_converter.convert(source_sql, target_table_full)

        section = f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE Operation
# MAGIC
# MAGIC T-SQL MERGE converted to Delta Lake MERGE.

# COMMAND ----------

{code}'''

        return section, warnings

    def _convert_dynamic_sql(self, source_sql: str, matches: List[Dict], context: Dict) -> Tuple[str, List[str]]:
        """Convert dynamic SQL to parameterized query."""
        warnings = ["Dynamic SQL detected - review parameterized queries for correctness"]

        section = '''# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic SQL Conversion
# MAGIC
# MAGIC Dynamic SQL converted to parameterized Spark SQL.
# MAGIC **MANUAL REVIEW REQUIRED** - verify parameter substitution is correct.

# COMMAND ----------

# Dynamic SQL pattern - use f-strings with proper escaping
# Original used EXEC or sp_executesql

# Example parameterized query:
# table_name = "silver_fact_workers_history"
# filter_col = "ProjectId"
# filter_value = 123
#
# query = f"""
#     SELECT *
#     FROM {TARGET_CATALOG}.{SOURCE_SCHEMA}.{table_name}
#     WHERE {filter_col} = {filter_value}
# """
# result_df = spark.sql(query)

# TODO: Implement actual dynamic SQL logic
print("Dynamic SQL conversion - requires manual review")'''

        return section, warnings

    def _convert_spatial(self, source_sql: str, matches: List[Dict], context: Dict) -> Tuple[str, List[str]]:
        """Generate spatial function stub with H3/UDF using Phase 2 SpatialConverter if available."""
        warnings = []

        # Use Phase 2 converter if available
        if self.spatial_converter:
            try:
                spatial_funcs = self.spatial_converter.detect_spatial_functions(source_sql)
                if spatial_funcs:
                    code, conv_warnings = self.spatial_converter.convert(spatial_funcs, context)
                    warnings.extend(conv_warnings)
                    return code, warnings
            except Exception as e:
                warnings.append(f"Phase 2 spatial converter error, falling back: {str(e)}")

        # Fallback: Basic spatial function stubs (Phase 1 behavior)
        warnings.append("Spatial functions detected - H3 or Haversine UDF stubs generated")

        section = '''# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Function Conversion
# MAGIC
# MAGIC T-SQL geography/geometry functions converted to H3 or Haversine UDFs.
# MAGIC **MANUAL REVIEW REQUIRED** - verify spatial logic is correct.

# COMMAND ----------

# Haversine distance UDF for STDistance equivalent
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import math

@udf(returnType=DoubleType())
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate great-circle distance in meters."""
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None

    R = 6371000  # Earth radius in meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + \\
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c

# Register for SQL usage
spark.udf.register("haversine_distance", haversine_distance)

# COMMAND ----------

# WKT Point parsing (for geography::Point equivalent)
# Parse "POINT (longitude latitude)" format

def parse_wkt_point(wkt_string):
    """Parse WKT POINT to (latitude, longitude) tuple."""
    if wkt_string is None:
        return (None, None)
    # Remove "POINT (" prefix and ")" suffix
    coords = wkt_string.replace("POINT (", "").replace("POINT(", "").replace(")", "")
    parts = coords.strip().split()
    if len(parts) >= 2:
        return (float(parts[1]), float(parts[0]))  # lat, lon (reversed from WKT)
    return (None, None)

# Usage example:
# df = df.withColumn("Longitude", F.split(F.trim(F.regexp_replace("PointWKT", "POINT\\\\s*\\\\(|\\\\)", "")), "\\\\s+").getItem(0).cast("double"))
# df = df.withColumn("Latitude", F.split(F.trim(F.regexp_replace("PointWKT", "POINT\\\\s*\\\\(|\\\\)", "")), "\\\\s+").getItem(1).cast("double"))'''

        return section, warnings

    def _convert_while_loop(self, source_sql: str, matches: List[Dict], context: Dict) -> Tuple[str, List[str]]:
        """Convert WHILE loop to DataFrame operations."""
        warnings = ["WHILE loop detected - may need manual conversion if not CURSOR-related"]

        section = '''# COMMAND ----------

# MAGIC %md
# MAGIC ## WHILE Loop Conversion
# MAGIC
# MAGIC T-SQL WHILE loops typically convert to:
# MAGIC - Window functions (for cumulative operations)
# MAGIC - DataFrame operations (for set-based logic)
# MAGIC - Python loops with collect() (for complex logic - use sparingly)

# COMMAND ----------

# If this WHILE loop is for CURSOR iteration, it's handled in the CURSOR section.
# Otherwise, consider these conversion patterns:

# 1. Cumulative/Running calculations:
#    Use Window functions with rowsBetween(unboundedPreceding, currentRow)

# 2. Conditional updates:
#    Use F.when().otherwise() chains

# 3. Complex iterative logic:
#    Consider collecting to driver and using Python, then parallelize result
#    (Only for small datasets - avoid for large tables)

print("WHILE loop conversion - review for appropriate pattern")'''

        return section, warnings

    def _convert_transaction(self, source_sql: str, matches: List[Dict], context: Dict) -> Tuple[str, List[str]]:
        """Convert transaction to Delta ACID comment."""
        return '''# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction Handling
# MAGIC
# MAGIC T-SQL BEGIN TRAN/COMMIT/ROLLBACK converted to Delta Lake ACID semantics.
# MAGIC Delta Lake provides automatic ACID transactions - no explicit transaction control needed.

# COMMAND ----------

# Delta Lake provides ACID transactions automatically:
# - Each DataFrame write is atomic
# - MERGE operations are atomic
# - Failed operations roll back automatically
#
# For multi-table consistency, use Delta Lake's Change Data Feed
# or implement a watermark-based recovery pattern.

print("Transaction handling: Delta Lake ACID semantics (automatic)")''', []

    def _generate_transformation_placeholder(self, source_sql: str, context: Dict) -> str:
        """Generate placeholder for main transformation logic."""
        return '''# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Transformation Logic
# MAGIC
# MAGIC **TODO:** Implement the core business logic from the stored procedure.

# COMMAND ----------

# Load source data
# TODO: Update source table name based on your migration mapping
source_df = spark.table(f"{TARGET_CATALOG}.silver.silver_source_table")

print(f"Source rows: {source_df.count():,}")

# COMMAND ----------

# Apply transformations
# TODO: Implement transformations based on original stored procedure logic

transformed_df = (
    source_df
    # .filter(F.col("condition") == value)
    # .withColumn("new_col", F.expr("expression"))
    # .groupBy("group_cols").agg(F.sum("measure").alias("total"))
)

print(f"Transformed rows: {transformed_df.count():,}")'''

    def _generate_write_section(self, target_catalog: str, target_schema: str, target_table: str) -> str:
        """Generate write/MERGE section."""
        return f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results

# COMMAND ----------

# Check if target table exists
try:
    table_exists = spark.catalog.tableExists(TARGET_TABLE)
except:
    table_exists = False

if table_exists:
    print(f"Merging into existing table: {{TARGET_TABLE}}")

    # Get target table
    delta_table = DeltaTable.forName(spark, TARGET_TABLE)

    # Define merge key - TODO: Update with actual primary key columns
    merge_key = "target.Id = source.Id"

    (delta_table.alias("target")
     .merge(
         result_df.alias("source"),  # TODO: Update DataFrame name
         merge_key
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )

    rows_processed = result_df.count()
    print(f"  Merged {{rows_processed:,}} records")
else:
    print(f"Creating new table: {{TARGET_TABLE}}")

    # TODO: Update DataFrame name and partition column
    (result_df
     .write
     .format("delta")
     .mode("overwrite")
     .partitionBy("ShiftLocalDate")  # TODO: Update partition column
     .option("overwriteSchema", "true")
     .saveAsTable(TARGET_TABLE)
    )

    rows_processed = result_df.count()
    print(f"  Created table with {{rows_processed:,}} records")'''

    def _generate_summary_section(self, procedure_name: str) -> str:
        """Generate execution summary section."""
        return f'''# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Summary

# COMMAND ----------

# Final summary
end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

# Get final count
try:
    final_count = spark.table(TARGET_TABLE).count()
except:
    final_count = rows_processed

# Update watermark
update_watermark(TARGET_TABLE.split(".")[-1], end_time, final_count)

summary = {{
    "procedure": "{procedure_name}",
    "load_mode": load_mode,
    "start_time": start_time.isoformat(),
    "end_time": end_time.isoformat(),
    "duration_seconds": round(duration, 2),
    "rows_processed": rows_processed,
    "final_count": final_count,
    "status": "SUCCESS"
}}

print("=" * 60)
print("EXECUTION SUMMARY")
print("=" * 60)
for key, value in summary.items():
    print(f"{{key}}: {{value}}")
print("=" * 60)

# Exit with success
dbutils.notebook.exit("SUCCESS")'''


# Pattern definitions for import by sp_converter
# These mirror the patterns in analyzer.py for consistency
SP_SOURCE_PATTERNS = {
    "CURSOR": (
        r"DECLARE\s+@?\w+\s+CURSOR\s+(?:LOCAL\s+|GLOBAL\s+|FORWARD_ONLY\s+|SCROLL\s+|STATIC\s+|KEYSET\s+|DYNAMIC\s+|FAST_FORWARD\s+|READ_ONLY\s+|SCROLL_LOCKS\s+|OPTIMISTIC\s+)*FOR",
        "Cursor-based row iteration",
        "Window functions",
    ),
    "FETCH_CURSOR": (
        r"FETCH\s+(?:NEXT|PRIOR|FIRST|LAST|ABSOLUTE|RELATIVE)\s+FROM",
        "Cursor fetch operation",
        "Window functions",
    ),
    "WHILE_LOOP": (
        r"WHILE\s+(?:@\w+|@@\w+|1\s*=\s*1|\()",
        "While loop iteration",
        "DataFrame operations",
    ),
    "TEMP_TABLE": (
        r"(?:INTO\s+#|CREATE\s+TABLE\s+#|INSERT\s+(?:INTO\s+)?#)",
        "Temporary table usage",
        "createOrReplaceTempView()",
    ),
    "MERGE": (
        r"MERGE\s+(?:INTO\s+)?(?:\[?\w+\]?\.)*\[?\w+\]?\s+(?:AS\s+)?\w+\s*[\r\n]+\s*USING",
        "T-SQL MERGE statement",
        "DeltaTable.merge()",
    ),
    "DYNAMIC_SQL": (
        r"(?:EXEC\s*\(|sp_executesql|EXECUTE\s*\()",
        "Dynamic SQL execution",
        "Parameterized f-string",
    ),
    "SPATIAL_GEOGRAPHY": (
        r"geography::(?:Point|STGeomFromText|STGeomFromWKB|Parse)|\.ST(?:Distance|Contains|Intersects|Buffer|AsText)\s*\(",
        "Geography spatial functions",
        "H3/Haversine UDF",
    ),
    "SPATIAL_GEOMETRY": (
        r"geometry::(?:Point|STGeomFromText|Parse)|\.ST(?:Area|Length|Centroid)\s*\(",
        "Geometry spatial functions",
        "Shapely UDF",
    ),
    "TRANSACTION": (
        r"BEGIN\s+TRAN(?:SACTION)?|COMMIT\s+TRAN(?:SACTION)?|ROLLBACK",
        "Explicit transaction control",
        "Delta Lake ACID (automatic)",
    ),
}
