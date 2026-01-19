"""
SQL Server client for Lakebridge migrations.

Provides operations for:
- Extracting database objects (tables, views, procedures, functions)
- Analyzing SQL complexity
- Testing connectivity
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum


class ObjectType(Enum):
    """SQL Server object types."""
    TABLE = "TABLE"
    VIEW = "VIEW"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    TRIGGER = "TRIGGER"


class ComplexityIndicator(Enum):
    """SQL complexity indicators that affect transpilation."""
    CURSOR = "CURSOR"
    TEMP_TABLE = "TEMP_TABLE"
    DYNAMIC_SQL = "DYNAMIC_SQL"
    SPATIAL = "SPATIAL"
    MERGE = "MERGE"
    PIVOT = "PIVOT"
    CTE = "CTE"
    RECURSIVE_CTE = "RECURSIVE_CTE"
    XML = "XML"
    JSON = "JSON"


@dataclass
class SQLObject:
    """Represents a SQL Server database object."""
    schema_name: str
    object_name: str
    object_type: ObjectType
    definition: Optional[str] = None
    line_count: int = 0
    complexity_indicators: List[ComplexityIndicator] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        """Get fully qualified name."""
        return f"[{self.schema_name}].[{self.object_name}]"

    @property
    def requires_manual_conversion(self) -> bool:
        """Check if object requires manual conversion."""
        manual_indicators = {
            ComplexityIndicator.CURSOR,
            ComplexityIndicator.DYNAMIC_SQL,
            ComplexityIndicator.SPATIAL,
            ComplexityIndicator.RECURSIVE_CTE,
        }
        return bool(set(self.complexity_indicators) & manual_indicators)


@dataclass
class ExtractionResult:
    """Result of extracting SQL objects from a database."""
    tables: List[SQLObject] = field(default_factory=list)
    views: List[SQLObject] = field(default_factory=list)
    procedures: List[SQLObject] = field(default_factory=list)
    functions: List[SQLObject] = field(default_factory=list)

    @property
    def total_count(self) -> int:
        """Total number of objects extracted."""
        return len(self.tables) + len(self.views) + len(self.procedures) + len(self.functions)

    @property
    def manual_conversion_count(self) -> int:
        """Count of objects requiring manual conversion."""
        all_objects = self.views + self.procedures + self.functions
        return sum(1 for obj in all_objects if obj.requires_manual_conversion)

    def summary(self) -> Dict[str, int]:
        """Get summary counts."""
        return {
            "tables": len(self.tables),
            "views": len(self.views),
            "procedures": len(self.procedures),
            "functions": len(self.functions),
            "total": self.total_count,
            "manual_conversion_needed": self.manual_conversion_count,
        }


class SQLServerClient:
    """
    SQL Server client for extracting and analyzing database objects.

    Usage:
        from credentials import CredentialsManager

        creds = CredentialsManager().load()
        client = SQLServerClient(
            server=creds.sqlserver.server,
            database=creds.sqlserver.database,
            user=creds.sqlserver.user,
            password=creds.sqlserver.password
        )

        # Test connection
        if client.test_connection():
            # Extract all objects
            result = client.extract_all()
            print(result.summary())
    """

    # SQL queries for extraction
    TABLES_QUERY = """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS column_count
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name
    """

    VIEWS_QUERY = """
        SELECT
            s.name AS schema_name,
            v.name AS view_name,
            OBJECT_DEFINITION(v.object_id) AS definition
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE v.is_ms_shipped = 0
          AND OBJECT_DEFINITION(v.object_id) IS NOT NULL
        ORDER BY s.name, v.name
    """

    PROCEDURES_QUERY = """
        SELECT
            s.name AS schema_name,
            p.name AS proc_name,
            OBJECT_DEFINITION(p.object_id) AS definition
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE p.is_ms_shipped = 0
          AND OBJECT_DEFINITION(p.object_id) IS NOT NULL
        ORDER BY s.name, p.name
    """

    FUNCTIONS_QUERY = """
        SELECT
            s.name AS schema_name,
            o.name AS func_name,
            o.type_desc AS func_type,
            OBJECT_DEFINITION(o.object_id) AS definition
        FROM sys.objects o
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type IN ('FN', 'IF', 'TF', 'AF')  -- Scalar, Inline, Table, Aggregate
          AND o.is_ms_shipped = 0
          AND OBJECT_DEFINITION(o.object_id) IS NOT NULL
        ORDER BY s.name, o.name
    """

    def __init__(
        self,
        server: str,
        database: str,
        user: str,
        password: str,
        port: int = 1433,
        driver: str = "ODBC Driver 18 for SQL Server",
    ):
        """Initialize SQL Server client."""
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.driver = driver
        self._engine = None

    @property
    def connection_string(self) -> str:
        """Get SQLAlchemy connection URL."""
        from urllib.parse import quote_plus

        params = quote_plus(
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no"
        )
        return f"mssql+pyodbc:///?odbc_connect={params}"

    @property
    def engine(self):
        """Lazy-load SQLAlchemy engine."""
        if self._engine is None:
            from sqlalchemy import create_engine
            self._engine = create_engine(self.connection_string)
        return self._engine

    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _analyze_complexity(self, sql: str) -> List[ComplexityIndicator]:
        """Analyze SQL code for complexity indicators."""
        if not sql:
            return []

        sql_upper = sql.upper()
        indicators = []

        patterns = {
            ComplexityIndicator.CURSOR: ["DECLARE CURSOR", "OPEN CURSOR", "FETCH NEXT"],
            ComplexityIndicator.TEMP_TABLE: ["#", "CREATE TABLE #", "INTO #"],
            ComplexityIndicator.DYNAMIC_SQL: ["EXEC(", "EXECUTE(", "SP_EXECUTESQL"],
            ComplexityIndicator.SPATIAL: ["GEOGRAPHY", "GEOMETRY", "STDISTANCE", "STINTERSECTS"],
            ComplexityIndicator.MERGE: ["MERGE INTO", "MERGE "],
            ComplexityIndicator.PIVOT: ["PIVOT", "UNPIVOT"],
            ComplexityIndicator.CTE: ["WITH ", ";WITH "],
            ComplexityIndicator.XML: [".QUERY(", ".VALUE(", "FOR XML"],
            ComplexityIndicator.JSON: ["JSON_VALUE", "JSON_QUERY", "FOR JSON"],
        }

        for indicator, keywords in patterns.items():
            for keyword in keywords:
                if keyword in sql_upper:
                    indicators.append(indicator)
                    break

        # Check for recursive CTE
        if ComplexityIndicator.CTE in indicators:
            if "UNION ALL" in sql_upper and sql_upper.count("SELECT") > 2:
                indicators.append(ComplexityIndicator.RECURSIVE_CTE)

        return indicators

    def extract_tables(self) -> List[SQLObject]:
        """Extract all user tables."""
        from sqlalchemy import text

        tables = []
        with self.engine.connect() as conn:
            result = conn.execute(text(self.TABLES_QUERY))
            for row in result:
                tables.append(SQLObject(
                    schema_name=row.schema_name,
                    object_name=row.table_name,
                    object_type=ObjectType.TABLE,
                ))
        return tables

    def extract_views(self) -> List[SQLObject]:
        """Extract all user views with definitions."""
        from sqlalchemy import text

        views = []
        with self.engine.connect() as conn:
            result = conn.execute(text(self.VIEWS_QUERY))
            for row in result:
                definition = row.definition or ""
                views.append(SQLObject(
                    schema_name=row.schema_name,
                    object_name=row.view_name,
                    object_type=ObjectType.VIEW,
                    definition=definition,
                    line_count=definition.count("\n") + 1,
                    complexity_indicators=self._analyze_complexity(definition),
                ))
        return views

    def extract_procedures(self) -> List[SQLObject]:
        """Extract all stored procedures with definitions."""
        from sqlalchemy import text

        procedures = []
        with self.engine.connect() as conn:
            result = conn.execute(text(self.PROCEDURES_QUERY))
            for row in result:
                definition = row.definition or ""
                procedures.append(SQLObject(
                    schema_name=row.schema_name,
                    object_name=row.proc_name,
                    object_type=ObjectType.PROCEDURE,
                    definition=definition,
                    line_count=definition.count("\n") + 1,
                    complexity_indicators=self._analyze_complexity(definition),
                ))
        return procedures

    def extract_functions(self) -> List[SQLObject]:
        """Extract all user-defined functions with definitions."""
        from sqlalchemy import text

        functions = []
        with self.engine.connect() as conn:
            result = conn.execute(text(self.FUNCTIONS_QUERY))
            for row in result:
                definition = row.definition or ""
                functions.append(SQLObject(
                    schema_name=row.schema_name,
                    object_name=row.func_name,
                    object_type=ObjectType.FUNCTION,
                    definition=definition,
                    line_count=definition.count("\n") + 1,
                    complexity_indicators=self._analyze_complexity(definition),
                ))
        return functions

    def extract_all(self) -> ExtractionResult:
        """Extract all database objects."""
        return ExtractionResult(
            tables=self.extract_tables(),
            views=self.extract_views(),
            procedures=self.extract_procedures(),
            functions=self.extract_functions(),
        )

    def get_table_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column definitions for a table."""
        from sqlalchemy import text

        query = text("""
            SELECT
                c.name AS column_name,
                t.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                COLUMNPROPERTY(c.object_id, c.name, 'IsComputed') AS is_computed
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            JOIN sys.tables tbl ON c.object_id = tbl.object_id
            JOIN sys.schemas s ON tbl.schema_id = s.schema_id
            WHERE s.name = :schema AND tbl.name = :table
            ORDER BY c.column_id
        """)

        columns = []
        with self.engine.connect() as conn:
            result = conn.execute(query, {"schema": schema, "table": table})
            for row in result:
                columns.append({
                    "name": row.column_name,
                    "type": row.data_type,
                    "max_length": row.max_length,
                    "precision": row.precision,
                    "scale": row.scale,
                    "nullable": row.is_nullable,
                    "identity": row.is_identity,
                    "computed": row.is_computed,
                })
        return columns
