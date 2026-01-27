"""
Temp Table Converter for Lakebridge.

Converts T-SQL temporary tables (#temp, ##global) to Spark equivalents:
- createOrReplaceTempView() for single-use temp tables
- cache() + createOrReplaceTempView() for reused temp tables
- Delta table for global temp tables requiring persistence
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import re


@dataclass
class TempTableInfo:
    """Information about a detected temp table."""
    name: str                    # e.g., "#Results" or "##GlobalResults"
    is_global: bool              # True for ## tables
    create_type: str             # "SELECT_INTO", "CREATE_TABLE", "INSERT"
    create_sql: str              # The SQL that creates the table
    create_line: int             # Line number of creation
    usage_count: int             # Number of times referenced
    usage_lines: List[int]       # Line numbers where used
    columns: Optional[List[Dict]]  # Column definitions if available
    select_sql: Optional[str]    # SELECT statement if SELECT INTO


class TempTableConverter:
    """
    Convert T-SQL temporary tables to Spark equivalents.

    Mappings:
    - #TempTable → createOrReplaceTempView() for single-use
    - #TempTable (reused) → cache() + createOrReplaceTempView()
    - ##GlobalTemp → Delta table with session prefix
    """

    def detect_temp_tables(self, sql: str) -> List[TempTableInfo]:
        """
        Find all temp table definitions and usages.

        Returns:
            List of TempTableInfo objects for each temp table found
        """
        temp_tables = {}
        lines = sql.split('\n')

        # Pattern for SELECT INTO #temp
        select_into_pattern = r"SELECT\s+[\s\S]+?\s+INTO\s+(##+\w+)"

        # Pattern for CREATE TABLE #temp
        create_table_pattern = r"CREATE\s+TABLE\s+(##+\w+)\s*\(([\s\S]+?)\)"

        # Pattern for INSERT INTO #temp
        insert_into_pattern = r"INSERT\s+(?:INTO\s+)?(##+\w+)"

        # Find SELECT INTO patterns
        for match in re.finditer(select_into_pattern, sql, re.IGNORECASE):
            temp_name = match.group(1)
            line_num = sql[:match.start()].count('\n') + 1

            # Extract the full SELECT statement
            # Find the SELECT that precedes INTO
            select_start = sql.rfind('SELECT', 0, match.start())
            if select_start == -1:
                select_start = match.start()

            # Find where the statement ends (next statement or end)
            stmt_end = match.end()
            next_stmt = re.search(r'\n\s*(?:SELECT|INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|DECLARE|SET|IF|WHILE|BEGIN)', sql[stmt_end:], re.IGNORECASE)
            if next_stmt:
                stmt_end = stmt_end + next_stmt.start()

            create_sql = sql[select_start:stmt_end].strip()

            # Extract just the SELECT part (before INTO)
            select_sql = sql[select_start:match.start()].strip()

            if temp_name not in temp_tables:
                temp_tables[temp_name] = TempTableInfo(
                    name=temp_name,
                    is_global=temp_name.startswith('##'),
                    create_type="SELECT_INTO",
                    create_sql=create_sql,
                    create_line=line_num,
                    usage_count=0,
                    usage_lines=[],
                    columns=None,
                    select_sql=select_sql,
                )

        # Find CREATE TABLE patterns
        for match in re.finditer(create_table_pattern, sql, re.IGNORECASE | re.MULTILINE):
            temp_name = match.group(1)
            column_def = match.group(2)
            line_num = sql[:match.start()].count('\n') + 1

            # Parse column definitions
            columns = self._parse_column_definitions(column_def)

            if temp_name not in temp_tables:
                temp_tables[temp_name] = TempTableInfo(
                    name=temp_name,
                    is_global=temp_name.startswith('##'),
                    create_type="CREATE_TABLE",
                    create_sql=match.group(0),
                    create_line=line_num,
                    usage_count=0,
                    usage_lines=[],
                    columns=columns,
                    select_sql=None,
                )

        # Count usages of each temp table
        for temp_name in temp_tables:
            # Escape the # for regex
            escaped_name = temp_name.replace('#', r'\#')
            # Find all references (excluding the creation)
            usage_pattern = rf"(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+{escaped_name}\b"

            for match in re.finditer(usage_pattern, sql, re.IGNORECASE):
                line_num = sql[:match.start()].count('\n') + 1
                # Don't count the creation line
                if line_num != temp_tables[temp_name].create_line:
                    temp_tables[temp_name].usage_count += 1
                    temp_tables[temp_name].usage_lines.append(line_num)

        return list(temp_tables.values())

    def _parse_column_definitions(self, column_def: str) -> List[Dict]:
        """Parse column definitions from CREATE TABLE statement."""
        columns = []

        # Split by comma, but be careful of commas inside parentheses
        parts = re.split(r',\s*(?![^()]*\))', column_def)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Skip constraints
            if part.upper().startswith(('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT')):
                continue

            # Parse column: name type [NULL|NOT NULL] [DEFAULT ...]
            col_match = re.match(r'\[?(\w+)\]?\s+(\w+(?:\([^)]+\))?)', part, re.IGNORECASE)
            if col_match:
                columns.append({
                    'name': col_match.group(1),
                    'type': col_match.group(2).upper(),
                    'nullable': 'NOT NULL' not in part.upper(),
                })

        return columns

    def convert(self, temp_info: TempTableInfo, context: Dict) -> Tuple[str, List[str]]:
        """
        Convert a temp table to Spark equivalent.

        Args:
            temp_info: TempTableInfo object describing the temp table
            context: Dict with catalog, schema, and other context

        Returns:
            Tuple of (converted_code, warnings)
        """
        warnings = []

        # Generate DataFrame name from temp table name
        df_name = self._temp_to_df_name(temp_info.name)
        view_name = temp_info.name.lstrip('#')

        if temp_info.is_global:
            # Global temp table -> Delta table
            return self._convert_global_temp(temp_info, df_name, view_name, context), warnings

        if temp_info.usage_count > 2:
            # Frequently used -> cache
            return self._convert_cached_temp(temp_info, df_name, view_name, context), warnings

        # Standard temp table -> temp view
        return self._convert_standard_temp(temp_info, df_name, view_name, context), warnings

    def _temp_to_df_name(self, temp_name: str) -> str:
        """Convert temp table name to DataFrame variable name."""
        # Remove # prefix and convert to snake_case
        name = temp_name.lstrip('#')
        # Convert CamelCase to snake_case
        name = re.sub(r'([A-Z])', r'_\1', name).lower().strip('_')
        return f"{name}_df"

    def _convert_standard_temp(self, temp_info: TempTableInfo, df_name: str,
                                view_name: str, context: Dict) -> str:
        """Convert standard temp table to temp view."""

        if temp_info.create_type == "SELECT_INTO" and temp_info.select_sql:
            # Convert SELECT INTO to DataFrame
            select_sql = self._convert_select_sql(temp_info.select_sql, context)

            return f'''
# Temp table {temp_info.name} → Spark temp view
# Original: SELECT ... INTO {temp_info.name}
# Usage count: {temp_info.usage_count}

{df_name} = spark.sql("""
{self._indent_sql(select_sql, 4)}
""")

{df_name}.createOrReplaceTempView("{view_name}")
print(f"Created temp view {view_name}: {{{df_name}.count():,}} rows")
'''

        elif temp_info.create_type == "CREATE_TABLE" and temp_info.columns:
            # Convert CREATE TABLE to schema definition
            schema_str = self._columns_to_schema(temp_info.columns)

            return f'''
# Temp table {temp_info.name} → Spark temp view with schema
# Original: CREATE TABLE {temp_info.name} (...)
# Usage count: {temp_info.usage_count}

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType, BooleanType

# Schema definition
{view_name}_schema = StructType([
{schema_str}
])

# Create empty DataFrame with schema (will be populated by INSERT statements)
{df_name} = spark.createDataFrame([], {view_name}_schema)
{df_name}.createOrReplaceTempView("{view_name}")

# TODO: Convert INSERT statements to populate this temp view
# Original INSERT statements should be converted to:
# {df_name} = {df_name}.union(new_data_df)
# {df_name}.createOrReplaceTempView("{view_name}")
'''

        else:
            # Fallback for unrecognized patterns
            return f'''
# Temp table {temp_info.name} → Spark temp view
# Original creation type: {temp_info.create_type}
# Usage count: {temp_info.usage_count}

# TODO: Manually convert the following temp table creation:
# {temp_info.create_sql[:200]}...

{df_name} = spark.sql("""
    -- TODO: Replace with appropriate SELECT statement
    SELECT * FROM source_table WHERE 1=0
""")
{df_name}.createOrReplaceTempView("{view_name}")
'''

    def _convert_cached_temp(self, temp_info: TempTableInfo, df_name: str,
                              view_name: str, context: Dict) -> str:
        """Convert frequently used temp table to cached DataFrame."""

        if temp_info.create_type == "SELECT_INTO" and temp_info.select_sql:
            select_sql = self._convert_select_sql(temp_info.select_sql, context)

            return f'''
# Temp table {temp_info.name} → Cached DataFrame + temp view
# Original: SELECT ... INTO {temp_info.name}
# Usage count: {temp_info.usage_count} (cached for performance)

{df_name} = spark.sql("""
{self._indent_sql(select_sql, 4)}
""").cache()

# Materialize the cache
_cache_count = {df_name}.count()
{df_name}.createOrReplaceTempView("{view_name}")
print(f"Cached temp view {view_name}: {{_cache_count:,}} rows")
'''

        else:
            return f'''
# Temp table {temp_info.name} → Cached DataFrame + temp view
# Original creation type: {temp_info.create_type}
# Usage count: {temp_info.usage_count} (cached for performance)

# TODO: Replace with appropriate query
{df_name} = spark.sql("""
    SELECT * FROM source_table
""").cache()

# Materialize the cache
_cache_count = {df_name}.count()
{df_name}.createOrReplaceTempView("{view_name}")
print(f"Cached temp view {view_name}: {{_cache_count:,}} rows")
'''

    def _convert_global_temp(self, temp_info: TempTableInfo, df_name: str,
                              view_name: str, context: Dict) -> str:
        """Convert global temp table to Delta table."""
        catalog = context.get('target_catalog', 'wakecap_prod')
        schema = context.get('migration_schema', 'migration')

        if temp_info.create_type == "SELECT_INTO" and temp_info.select_sql:
            select_sql = self._convert_select_sql(temp_info.select_sql, context)

            return f'''
# Global temp table {temp_info.name} → Delta table
# Original: SELECT ... INTO {temp_info.name}
# Usage count: {temp_info.usage_count}
# Note: Global temp tables persist across sessions, using Delta table

GLOBAL_TEMP_TABLE = f"{{TARGET_CATALOG}}.{schema}._temp_{view_name}"

{df_name} = spark.sql("""
{self._indent_sql(select_sql, 4)}
""")

# Write to Delta table (overwrite for temp-like behavior)
{df_name}.write.format("delta").mode("overwrite").saveAsTable(GLOBAL_TEMP_TABLE)

# Also create temp view for compatibility
{df_name}.createOrReplaceTempView("{view_name}")
print(f"Created global temp table {{GLOBAL_TEMP_TABLE}}: {{{df_name}.count():,}} rows")
'''

        else:
            return f'''
# Global temp table {temp_info.name} → Delta table
# Original creation type: {temp_info.create_type}
# Usage count: {temp_info.usage_count}

GLOBAL_TEMP_TABLE = f"{{TARGET_CATALOG}}.{schema}._temp_{view_name}"

# TODO: Replace with appropriate query
{df_name} = spark.sql("""
    SELECT * FROM source_table
""")

# Write to Delta table
{df_name}.write.format("delta").mode("overwrite").saveAsTable(GLOBAL_TEMP_TABLE)
{df_name}.createOrReplaceTempView("{view_name}")
print(f"Created global temp table {{GLOBAL_TEMP_TABLE}}: {{{df_name}.count():,}} rows")
'''

    def _convert_select_sql(self, select_sql: str, context: Dict) -> str:
        """Convert T-SQL SELECT to Spark SQL compatible format."""
        result = select_sql

        # Get table mapping
        table_mapping = context.get('source_tables_mapping', {})
        catalog = context.get('target_catalog', 'wakecap_prod')

        # Replace table references
        # Pattern: [schema].[table] or schema.table
        for source, target in table_mapping.items():
            result = re.sub(
                rf'\b{re.escape(source)}\b',
                target,
                result,
                flags=re.IGNORECASE
            )

        # Common T-SQL to Spark SQL conversions
        conversions = [
            # GETUTCDATE() -> current_timestamp()
            (r'GETUTCDATE\s*\(\)', 'current_timestamp()'),
            (r'GETDATE\s*\(\)', 'current_timestamp()'),

            # ISNULL -> COALESCE
            (r'ISNULL\s*\(', 'COALESCE('),

            # TOP N -> LIMIT N (need to move to end)
            # This is complex, leaving as TODO

            # CONVERT(type, value) -> CAST(value AS type)
            (r'CONVERT\s*\(\s*(\w+)\s*,\s*([^)]+)\)', r'CAST(\2 AS \1)'),

            # Square brackets -> nothing (Spark doesn't need them)
            (r'\[(\w+)\]', r'\1'),

            # += -> handled differently in Spark
            # These are typically in UPDATE statements, not SELECT

            # AT TIME ZONE -> This needs special handling
            # For now, add a comment
        ]

        for pattern, replacement in conversions:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

        # Handle AT TIME ZONE conversion
        if 'AT TIME ZONE' in result.upper():
            result = re.sub(
                r"CAST\s*\(\s*(\w+)\s+AT\s+TIME\s+ZONE\s+'UTC'\s+AT\s+TIME\s+ZONE\s+(\w+)\s+as\s+(\w+)\)",
                r"from_utc_timestamp(\1, \2)",
                result,
                flags=re.IGNORECASE
            )

        return result

    def _columns_to_schema(self, columns: List[Dict]) -> str:
        """Convert column definitions to Spark StructType schema."""
        type_mapping = {
            'INT': 'IntegerType()',
            'BIGINT': 'LongType()',
            'SMALLINT': 'ShortType()',
            'TINYINT': 'ByteType()',
            'BIT': 'BooleanType()',
            'FLOAT': 'DoubleType()',
            'REAL': 'FloatType()',
            'DECIMAL': 'DecimalType(18, 2)',
            'NUMERIC': 'DecimalType(18, 2)',
            'MONEY': 'DecimalType(19, 4)',
            'VARCHAR': 'StringType()',
            'NVARCHAR': 'StringType()',
            'CHAR': 'StringType()',
            'NCHAR': 'StringType()',
            'TEXT': 'StringType()',
            'DATETIME': 'TimestampType()',
            'DATETIME2': 'TimestampType()',
            'DATE': 'DateType()',
            'TIME': 'StringType()',
            'UNIQUEIDENTIFIER': 'StringType()',
            'VARBINARY': 'BinaryType()',
            'BINARY': 'BinaryType()',
        }

        schema_lines = []
        for col in columns:
            col_name = col['name']
            col_type = col['type'].split('(')[0].upper()  # Remove size specifier
            spark_type = type_mapping.get(col_type, 'StringType()')
            nullable = 'True' if col.get('nullable', True) else 'False'

            schema_lines.append(f'    StructField("{col_name}", {spark_type}, {nullable}),')

        return '\n'.join(schema_lines)

    def _indent_sql(self, sql: str, spaces: int) -> str:
        """Indent SQL for embedding in Python string."""
        indent = ' ' * spaces
        lines = sql.strip().split('\n')
        return '\n'.join(indent + line for line in lines)

    def convert_all(self, sql: str, context: Dict) -> Tuple[str, List[str]]:
        """
        Convert all temp tables in the SQL to Spark equivalents.

        Returns:
            Tuple of (combined_code, all_warnings)
        """
        temp_tables = self.detect_temp_tables(sql)

        if not temp_tables:
            return "", []

        all_code = []
        all_warnings = []

        # Add section header
        all_code.append('''# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporary Tables
# MAGIC
# MAGIC T-SQL temporary tables converted to Spark temp views.

# COMMAND ----------
''')

        for temp_info in temp_tables:
            code, warnings = self.convert(temp_info, context)
            all_code.append(code)
            all_warnings.extend(warnings)

        return '\n'.join(all_code), all_warnings

    def generate_cleanup_code(self, temp_tables: List[TempTableInfo]) -> str:
        """Generate code to clean up temp views at the end of notebook."""
        if not temp_tables:
            return ""

        cleanup_lines = [
            "# Clean up temporary views",
            "try:",
        ]

        for temp_info in temp_tables:
            view_name = temp_info.name.lstrip('#')
            cleanup_lines.append(f'    spark.catalog.dropTempView("{view_name}")')

        cleanup_lines.extend([
            "except Exception as e:",
            '    print(f"Temp view cleanup warning: {e}")',
        ])

        return '\n'.join(cleanup_lines)
