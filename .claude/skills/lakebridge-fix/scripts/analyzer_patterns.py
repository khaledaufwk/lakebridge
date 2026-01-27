"""
Shared pattern definitions for SP analysis and conversion.

These patterns are used by both the review skill (analyzer.py)
and the fix skill (sp_converter.py) to ensure consistency.
"""

from enum import Enum


class RiskTier(Enum):
    """Risk tiers for code review issues."""
    BLOCKER = "BLOCKER"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


# T-SQL source patterns for stored procedure analysis
# Format: pattern_name -> (regex, description, conversion_target, risk_if_unconverted)
SP_SOURCE_PATTERNS = {
    "CURSOR": (
        r"DECLARE\s+@?\w+\s+CURSOR\s+(?:LOCAL\s+|GLOBAL\s+|FORWARD_ONLY\s+|SCROLL\s+|STATIC\s+|KEYSET\s+|DYNAMIC\s+|FAST_FORWARD\s+|READ_ONLY\s+|SCROLL_LOCKS\s+|OPTIMISTIC\s+)*FOR",
        "Cursor-based row iteration",
        "Window functions (lag/lead/row_number)",
        RiskTier.HIGH
    ),
    "WHILE_LOOP": (
        r"WHILE\s+(?:@\w+|@@\w+|1\s*=\s*1|\()",
        "While loop iteration",
        "DataFrame operations or recursive CTE",
        RiskTier.HIGH
    ),
    "TEMP_TABLE": (
        r"(?:INTO\s+#|CREATE\s+TABLE\s+#|INSERT\s+(?:INTO\s+)?#)",
        "Temporary table usage",
        "createOrReplaceTempView() or DataFrame",
        RiskTier.MEDIUM
    ),
    "GLOBAL_TEMP": (
        r"(?:INTO\s+##|CREATE\s+TABLE\s+##)",
        "Global temporary table",
        "Delta table or cached DataFrame",
        RiskTier.MEDIUM
    ),
    "MERGE": (
        r"MERGE\s+(?:INTO\s+)?(?:\[?\w+\]?\.)*\[?\w+\]?\s+(?:AS\s+)?\w+\s*[\r\n]+\s*USING",
        "T-SQL MERGE statement",
        "DeltaTable.merge() or DLT APPLY CHANGES",
        RiskTier.MEDIUM
    ),
    "DYNAMIC_SQL": (
        r"(?:EXEC\s*\(|sp_executesql|EXECUTE\s*\()",
        "Dynamic SQL execution",
        "Parameterized f-string queries",
        RiskTier.HIGH
    ),
    "SPATIAL_GEOGRAPHY": (
        r"geography::(?:Point|STGeomFromText|STGeomFromWKB|Parse)|\.ST(?:Distance|Contains|Intersects|Buffer|AsText)\s*\(",
        "Geography spatial functions",
        "H3 library or Haversine UDF",
        RiskTier.HIGH
    ),
    "SPATIAL_GEOMETRY": (
        r"geometry::(?:Point|STGeomFromText|Parse)|\.ST(?:Area|Length|Centroid)\s*\(",
        "Geometry spatial functions",
        "Shapely UDF",
        RiskTier.HIGH
    ),
    "RECURSIVE_CTE": (
        r"WITH\s+(\w+)\s+AS\s*\([^)]+UNION\s+ALL[^)]+\1",
        "Recursive common table expression",
        "Iterative DataFrame or GraphFrames",
        RiskTier.HIGH
    ),
    "PIVOT": (
        r"PIVOT\s*\(",
        "PIVOT operation",
        "DataFrame pivot()",
        RiskTier.LOW
    ),
    "UNPIVOT": (
        r"UNPIVOT\s*\(",
        "UNPIVOT operation",
        "DataFrame unpivot() or stack()",
        RiskTier.LOW
    ),
    "TRANSACTION": (
        r"BEGIN\s+TRAN(?:SACTION)?|COMMIT\s+TRAN(?:SACTION)?|ROLLBACK",
        "Explicit transaction control",
        "Delta Lake ACID (automatic)",
        RiskTier.LOW
    ),
    "TRY_CATCH": (
        r"BEGIN\s+TRY|BEGIN\s+CATCH",
        "T-SQL error handling",
        "Python try/except",
        RiskTier.LOW
    ),
    "ROWCOUNT": (
        r"@@ROWCOUNT",
        "Row count system variable",
        "df.count() after action",
        RiskTier.LOW
    ),
    "IDENTITY": (
        r"@@IDENTITY|SCOPE_IDENTITY\s*\(\)",
        "Identity value retrieval",
        "Not applicable - use generated keys",
        RiskTier.MEDIUM
    ),
    "FETCH_CURSOR": (
        r"FETCH\s+(?:NEXT|PRIOR|FIRST|LAST|ABSOLUTE|RELATIVE)\s+FROM",
        "Cursor fetch operation",
        "Window functions or collect()",
        RiskTier.HIGH
    ),
    "WAITFOR": (
        r"WAITFOR\s+(?:DELAY|TIME)",
        "Execution delay",
        "time.sleep() if needed, usually remove",
        RiskTier.LOW
    ),
    "CROSS_APPLY": (
        r"(?:CROSS|OUTER)\s+APPLY",
        "APPLY operator (lateral join)",
        "explode() or lateral view",
        RiskTier.MEDIUM
    ),
    "STRING_AGG": (
        r"STRING_AGG\s*\(|FOR\s+XML\s+PATH\s*\(\s*['\"]",
        "String aggregation",
        "collect_list() + concat_ws()",
        RiskTier.LOW
    ),
    "DATE_FUNCTIONS": (
        r"DATEADD\s*\(|DATEDIFF\s*\(|CONVERT\s*\(\s*(?:DATE|DATETIME)",
        "T-SQL date functions",
        "date_add/date_sub/datediff",
        RiskTier.LOW
    ),
}


# Patterns to find in converted Python notebooks that indicate successful conversion
SP_TARGET_PATTERNS = {
    "CURSOR_CONVERTED": (
        r"Window\s*\.\s*partitionBy|\.over\s*\(|F\.lag\s*\(|F\.lead\s*\(|F\.row_number\s*\(",
        "Window function usage indicating CURSOR conversion"
    ),
    "TEMP_TABLE_CONVERTED": (
        r"createOrReplaceTempView\s*\(|\.cache\s*\(\)|\.persist\s*\(",
        "Temp view or cached DataFrame"
    ),
    "MERGE_CONVERTED": (
        r"DeltaTable\.forName|DeltaTable\.forPath|\.merge\s*\(|\.alias\s*\(['\"]target['\"]\)",
        "Delta MERGE operation"
    ),
    "SPATIAL_CONVERTED": (
        r"h3\.|haversine|ST_|shapely|from_wkt|to_wkt|F\.split.*PointWKT|PointWKT_Clean",
        "Spatial library or WKT parsing"
    ),
    "DYNAMIC_SQL_CONVERTED": (
        r"f\"\"\".*\{.*\}.*\"\"\"|f\'.*\{.*\}.*\'|\.format\s*\(|spark\.sql\s*\(",
        "Parameterized query string"
    ),
    "WHILE_CONVERTED": (
        r"\.rdd\.|\bfor\s+\w+\s+in\s+|while\s+|F\.when\s*\(",
        "Iterative or conditional processing"
    ),
    "AGGREGATION_CONVERTED": (
        r"\.groupBy\s*\(|\.agg\s*\(|F\.sum\s*\(|F\.count\s*\(|F\.avg\s*\(",
        "DataFrame aggregation"
    ),
    "CROSS_APPLY_CONVERTED": (
        r"F\.explode\s*\(|lateral\s+view|\.crossJoin\s*\(",
        "Explode or lateral view"
    ),
    "STRING_AGG_CONVERTED": (
        r"F\.collect_list\s*\(|F\.concat_ws\s*\(|F\.array_join\s*\(",
        "String aggregation functions"
    ),
}


# T-SQL to Spark data type mappings
TSQL_TO_SPARK_MAPPINGS = {
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


# Mapping of source patterns to expected target patterns
PATTERN_MAPPING = {
    "CURSOR": "CURSOR_CONVERTED",
    "FETCH_CURSOR": "CURSOR_CONVERTED",
    "WHILE_LOOP": "WHILE_CONVERTED",
    "TEMP_TABLE": "TEMP_TABLE_CONVERTED",
    "GLOBAL_TEMP": "TEMP_TABLE_CONVERTED",
    "MERGE": "MERGE_CONVERTED",
    "SPATIAL_GEOGRAPHY": "SPATIAL_CONVERTED",
    "SPATIAL_GEOMETRY": "SPATIAL_CONVERTED",
    "DYNAMIC_SQL": "DYNAMIC_SQL_CONVERTED",
    "CROSS_APPLY": "CROSS_APPLY_CONVERTED",
    "STRING_AGG": "STRING_AGG_CONVERTED",
}
