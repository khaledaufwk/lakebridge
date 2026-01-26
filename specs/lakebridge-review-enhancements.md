# Lakebridge Review Skill - Enhancement Requirements

**Document Version:** 1.0
**Created:** 2026-01-25
**Purpose:** Define enhancements needed to support automated stored procedure migration review

---

## Executive Summary

The current `lakebridge-review` skill provides general code review capabilities including DLT notebook validation, credential checks, and risk-tiered issue reporting. However, it lacks stored procedure (SP) specific analysis capabilities required to validate T-SQL to Databricks conversions.

This document specifies enhancements to enable:
1. Automated SP complexity analysis
2. Conversion completeness validation
3. Business logic comparison
4. Dependency tracking
5. SP-specific review checklists

---

## Current State Assessment

### Existing Capabilities (analyzer.py)

| Feature | Location | Description |
|---------|----------|-------------|
| Risk Tiers | `RiskTier` enum | BLOCKER, HIGH, MEDIUM, LOW |
| Issue Categories | `IssueCategory` enum | security, performance, migration, etc. |
| Security Patterns | `SECURITY_PATTERNS` | Hardcoded credentials detection |
| Code Quality Patterns | `CODE_QUALITY_PATTERNS` | TODO/FIXME detection |
| Migration Patterns | `MIGRATION_PATTERNS` | serverless=False, column ambiguity |
| DLT Notebook Check | `check_dlt_notebook()` | Format, imports, decorators |
| Deployment Check | `check_migration_deployment()` | Schema, secrets, serverless |

### Missing Capabilities

| Gap | Impact | Priority |
|-----|--------|----------|
| SP complexity scoring | Cannot prioritize conversions | HIGH |
| Pattern conversion validation | Cannot verify all patterns converted | HIGH |
| Business logic comparison | Cannot verify correctness | HIGH |
| Dependency tracking | May miss broken references | MEDIUM |
| Data type validation | Silent data corruption | MEDIUM |
| Test coverage check | No validation assurance | MEDIUM |

---

## Enhancement Requirements

### REQ-R1: Stored Procedure Pattern Detection

**Purpose:** Detect T-SQL patterns in source SQL that require specific conversion approaches.

**Specification:**

```python
# Add to analyzer.py

SP_SOURCE_PATTERNS = {
    # Pattern name: (regex, description, conversion_target, risk_if_unconverted)
    "CURSOR": (
        r"DECLARE\s+@?\w+\s+CURSOR\s+FOR",
        "Cursor-based row iteration",
        "Window functions (lag/lead/row_number)",
        RiskTier.HIGH
    ),
    "WHILE_LOOP": (
        r"WHILE\s+(@\w+|@@\w+)",
        "While loop iteration",
        "DataFrame operations or recursive CTE",
        RiskTier.HIGH
    ),
    "TEMP_TABLE": (
        r"(INTO\s+#|CREATE\s+TABLE\s+#|INSERT\s+INTO\s+#)",
        "Temporary table usage",
        "createOrReplaceTempView() or DataFrame",
        RiskTier.MEDIUM
    ),
    "GLOBAL_TEMP": (
        r"(INTO\s+##|CREATE\s+TABLE\s+##)",
        "Global temporary table",
        "Delta table or cached DataFrame",
        RiskTier.MEDIUM
    ),
    "MERGE": (
        r"MERGE\s+(?:INTO\s+)?\[?\w+\]?\s+(?:AS\s+)?\w+\s+USING",
        "T-SQL MERGE statement",
        "DeltaTable.merge() or DLT APPLY CHANGES",
        RiskTier.MEDIUM
    ),
    "DYNAMIC_SQL": (
        r"(EXEC\s*\(|sp_executesql|EXECUTE\s*\()",
        "Dynamic SQL execution",
        "Parameterized f-string queries",
        RiskTier.HIGH
    ),
    "SPATIAL_GEOGRAPHY": (
        r"geography::\w+|\.STDistance\(|\.STContains\(|\.STIntersects\(",
        "Geography spatial functions",
        "H3 library or Haversine UDF",
        RiskTier.HIGH
    ),
    "SPATIAL_GEOMETRY": (
        r"geometry::\w+|\.STArea\(|\.STLength\(",
        "Geometry spatial functions",
        "Shapely UDF",
        RiskTier.HIGH
    ),
    "RECURSIVE_CTE": (
        r"WITH\s+\w+\s+AS\s*\([^)]+UNION\s+ALL[^)]+\1",
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
        r"BEGIN\s+TRAN|COMMIT\s+TRAN|ROLLBACK",
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
        r"@@IDENTITY|SCOPE_IDENTITY\(\)",
        "Identity value retrieval",
        "Not applicable - use generated keys",
        RiskTier.MEDIUM
    ),
}
```

**New Method:**

```python
def detect_sp_patterns(self, source_sql: str) -> Dict[str, List[Dict]]:
    """
    Detect T-SQL patterns in source stored procedure.

    Returns:
        Dict with pattern names as keys and list of match details as values.
        Each match includes: line_number, matched_text, context (surrounding lines)
    """
```

---

### REQ-R2: Conversion Completeness Validator

**Purpose:** Compare source SP patterns against converted notebook to verify all patterns are addressed.

**Specification:**

```python
# Add to analyzer.py

SP_TARGET_PATTERNS = {
    # Pattern name: (regex to find in Python notebook, description)
    "CURSOR_CONVERTED": (
        r"Window\s*\.\s*partitionBy|\.over\s*\(|lag\s*\(|lead\s*\(|row_number\s*\(",
        "Window function usage indicating CURSOR conversion"
    ),
    "TEMP_TABLE_CONVERTED": (
        r"createOrReplaceTempView\s*\(|\.cache\s*\(\)|\.persist\s*\(",
        "Temp view or cached DataFrame"
    ),
    "MERGE_CONVERTED": (
        r"DeltaTable\.forName|\.merge\s*\(|MERGE\s+INTO|APPLY\s+CHANGES",
        "Delta MERGE or DLT APPLY CHANGES"
    ),
    "SPATIAL_CONVERTED": (
        r"h3\.|haversine|ST_|shapely|from_wkt|to_wkt",
        "Spatial library usage"
    ),
    "DYNAMIC_SQL_CONVERTED": (
        r"f\"\"\".*\{.*\}.*\"\"\"|f\'\'\'.*\{.*\}.*\'\'\'|\.format\s*\(",
        "Parameterized query string"
    ),
}

class ConversionValidator:
    """Validate stored procedure conversion completeness."""

    def validate_conversion(
        self,
        source_sql: str,
        target_notebook: str,
        procedure_name: str
    ) -> List[Issue]:
        """
        Compare source patterns to target implementations.

        Returns list of issues for unconverted or partially converted patterns.
        """

    def generate_conversion_report(
        self,
        source_sql: str,
        target_notebook: str
    ) -> Dict:
        """
        Generate detailed conversion report.

        Returns:
            {
                "procedure_name": str,
                "source_line_count": int,
                "target_line_count": int,
                "patterns_found": [{"name": str, "count": int, "lines": [int]}],
                "patterns_converted": [{"name": str, "evidence": str}],
                "patterns_missing": [{"name": str, "risk": str, "recommendation": str}],
                "conversion_score": float,  # 0.0 to 1.0
                "verdict": "COMPLETE" | "PARTIAL" | "INCOMPLETE"
            }
        """
```

---

### REQ-R3: Business Logic Comparator

**Purpose:** Extract and compare business logic between source and target to ensure functional equivalence.

**Specification:**

```python
# Add new file: analyzer_business_logic.py

class BusinessLogicExtractor:
    """Extract business logic elements from SQL and Python code."""

    def extract_from_tsql(self, source_sql: str) -> Dict:
        """
        Extract business logic elements from T-SQL.

        Returns:
            {
                "input_tables": [{"schema": str, "table": str, "alias": str}],
                "output_tables": [{"schema": str, "table": str, "operation": str}],
                "columns_read": [{"table": str, "column": str}],
                "columns_written": [{"table": str, "column": str, "expression": str}],
                "aggregations": [{"function": str, "column": str, "alias": str}],
                "joins": [{"type": str, "left": str, "right": str, "condition": str}],
                "filters": [{"table": str, "condition": str}],
                "group_by": [str],
                "order_by": [{"column": str, "direction": str}],
                "parameters": [{"name": str, "type": str, "default": str}],
            }
        """

    def extract_from_notebook(self, notebook_content: str) -> Dict:
        """
        Extract business logic elements from Databricks notebook.

        Returns same structure as extract_from_tsql for comparison.
        """

    def compare_logic(
        self,
        source_logic: Dict,
        target_logic: Dict
    ) -> List[Issue]:
        """
        Compare source and target business logic.

        Checks:
        - All input tables are read
        - All output tables are written
        - Column mappings are correct
        - Aggregations match
        - Join logic preserved
        - Filter conditions preserved
        """
```

**Comparison Rules:**

| Element | Validation Rule | Risk if Failed |
|---------|----------------|----------------|
| Input Tables | All source tables read in target | BLOCKER |
| Output Tables | All target tables written | BLOCKER |
| Primary Key | Same PK columns used | BLOCKER |
| Aggregations | Same functions on same columns | HIGH |
| Joins | Same join types and conditions | HIGH |
| Filters | Same WHERE conditions | HIGH |
| Column Mappings | Source→Target mapping documented | MEDIUM |
| Order By | Same ordering (if materialized) | LOW |

---

### REQ-R4: Dependency Tracker

**Purpose:** Track and validate dependencies between stored procedures, tables, views, and functions.

**Specification:**

```python
# Add new file: analyzer_dependencies.py

@dataclass
class Dependency:
    """A single dependency reference."""
    source_object: str      # e.g., "stg.spDeltaSyncFactWorkersHistory"
    target_object: str      # e.g., "dbo.Worker"
    dependency_type: str    # "reads", "writes", "calls", "uses_function"
    is_resolved: bool       # True if target exists in Databricks
    databricks_equivalent: Optional[str]  # e.g., "wakecap_prod.silver.silver_worker"

class DependencyTracker:
    """Track dependencies for stored procedure migrations."""

    def __init__(self, catalog: str, schemas: List[str]):
        """Initialize with target catalog and schemas to check."""

    def extract_dependencies(self, source_sql: str, object_name: str) -> List[Dependency]:
        """
        Extract all dependencies from source SQL.

        Detects:
        - Table reads (FROM, JOIN)
        - Table writes (INSERT, UPDATE, DELETE, MERGE INTO)
        - Procedure calls (EXEC)
        - Function calls (dbo.fn*, stg.fn*)
        - View references
        """

    def resolve_dependencies(
        self,
        dependencies: List[Dependency],
        table_mapping: Dict[str, str]  # source -> databricks mapping
    ) -> List[Dependency]:
        """
        Check if dependencies exist in Databricks.

        Updates is_resolved and databricks_equivalent for each dependency.
        """

    def generate_dependency_report(
        self,
        procedure_name: str,
        dependencies: List[Dependency]
    ) -> str:
        """
        Generate markdown report of dependencies.

        Sections:
        - Resolved Dependencies (tables/views that exist)
        - Unresolved Dependencies (missing - BLOCKERS)
        - Procedure Calls (other SPs that must be converted first)
        - Function Dependencies (UDFs needed)
        """

    def get_conversion_order(
        self,
        procedures: List[str],
        all_dependencies: Dict[str, List[Dependency]]
    ) -> List[str]:
        """
        Determine optimal conversion order based on dependencies.

        Returns procedures sorted so dependencies are converted first.
        """
```

**Dependency Categories:**

| Category | Detection Pattern | Resolution Check |
|----------|------------------|------------------|
| Table Read | `FROM [schema].[table]`, `JOIN [schema].[table]` | Check `SHOW TABLES IN catalog.schema` |
| Table Write | `INSERT INTO`, `UPDATE`, `DELETE FROM`, `MERGE INTO` | Check target table exists |
| Procedure Call | `EXEC [schema].[proc]`, `EXECUTE [schema].[proc]` | Check converted notebook exists |
| Function Call | `[schema].fn*()`, `dbo.fn*()` | Check UDF registered in catalog |
| View Reference | `FROM [schema].[vw*]` | Check view exists or converted |

---

### REQ-R5: SP-Specific Review Checklist Generator

**Purpose:** Generate comprehensive review checklists specific to each stored procedure's patterns.

**Specification:**

```python
# Add to analyzer.py

class SPChecklistGenerator:
    """Generate procedure-specific review checklists."""

    def generate_checklist(
        self,
        procedure_name: str,
        source_sql: str,
        target_notebook: str,
        conversion_report: Dict,
        dependency_report: List[Dependency]
    ) -> str:
        """
        Generate comprehensive markdown checklist for SP review.

        Sections:
        1. Source Analysis
        2. Pattern Conversion Status
        3. Business Logic Validation
        4. Dependency Resolution
        5. Data Quality Rules
        6. Performance Considerations
        7. Testing Requirements
        """
```

**Checklist Template:**

```markdown
## Stored Procedure Review: {procedure_name}

**Source:** `{source_path}`
**Target:** `{target_path}`
**Complexity Score:** {score}/10
**Conversion Status:** {status}

---

### 1. Source Analysis

| Metric | Value |
|--------|-------|
| Lines of Code | {line_count} |
| Patterns Detected | {patterns} |
| Tables Referenced | {table_count} |
| Procedures Called | {proc_count} |
| Functions Used | {func_count} |

---

### 2. Pattern Conversion Status

| Pattern | Found | Converted | Evidence | Status |
|---------|-------|-----------|----------|--------|
| CURSOR | {cursor_found} | {cursor_converted} | {cursor_evidence} | {cursor_status} |
| TEMP_TABLE | {temp_found} | {temp_converted} | {temp_evidence} | {temp_status} |
| MERGE | {merge_found} | {merge_converted} | {merge_evidence} | {merge_status} |
| SPATIAL | {spatial_found} | {spatial_converted} | {spatial_evidence} | {spatial_status} |

---

### 3. Business Logic Validation

#### Input Tables
- [ ] `{table1}` → `{databricks_table1}` - Verified
- [ ] `{table2}` → `{databricks_table2}` - Verified

#### Output Tables
- [ ] `{output_table}` → `{databricks_output}` - Schema matches

#### Key Transformations
- [ ] Aggregation: `{agg_description}` - Logic preserved
- [ ] Join: `{join_description}` - Conditions match
- [ ] Filter: `{filter_description}` - Equivalent

---

### 4. Dependency Resolution

#### Resolved
- [x] `dbo.Worker` → `wakecap_prod.silver.silver_worker`
- [x] `dbo.Project` → `wakecap_prod.silver.silver_project`

#### Unresolved (BLOCKERS)
- [ ] `stg.fnCalcTimeCategory` → **UDF NOT FOUND**
- [ ] `dbo.Zone` → **TABLE NOT FOUND**

---

### 5. Data Quality Rules

- [ ] Primary key NOT NULL: `{pk_columns}`
- [ ] Foreign keys validated: `{fk_columns}`
- [ ] Required fields present: `{required_columns}`
- [ ] Data types match source schema

---

### 6. Performance Considerations

- [ ] Large table ({row_count} rows): Partitioning configured
- [ ] Window functions: Partition columns indexed
- [ ] Joins: Broadcast hint for small tables

---

### 7. Testing Requirements

- [ ] Validation notebook created
- [ ] Row count comparison: Source vs Target
- [ ] Sample record comparison (100 random rows)
- [ ] Aggregation comparison (SUM, COUNT, AVG)
- [ ] Edge case testing (NULL handling, empty sets)

---

### Review Verdict

**Status:** {PASS | FAIL | NEEDS_WORK}

**Blockers:**
{blocker_list}

**Recommendations:**
{recommendation_list}
```

---

### REQ-R6: Data Type Validation

**Purpose:** Validate that T-SQL data types are correctly mapped to Spark/Databricks types.

**Specification:**

```python
# Add to analyzer.py

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
    "TIME": "STRING",  # Note: Spark TIME support limited
    "DATETIMEOFFSET": "TIMESTAMP",

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
}

class DataTypeValidator:
    """Validate data type mappings between source and target."""

    def extract_column_types(self, source_sql: str) -> Dict[str, str]:
        """Extract column names and types from T-SQL."""

    def validate_type_mapping(
        self,
        source_type: str,
        target_type: str,
        column_name: str
    ) -> Optional[Issue]:
        """
        Validate a single type mapping.

        Returns Issue if:
        - Type mapping is incorrect
        - Precision loss possible (e.g., DECIMAL(38,10) → DECIMAL(18,2))
        - Special handling needed (e.g., GEOGRAPHY → STRING needs ST_AsText)
        """

    def check_precision_loss(
        self,
        source_type: str,
        target_type: str
    ) -> Optional[str]:
        """Check for potential precision loss in numeric conversions."""
```

---

### REQ-R7: Enhanced Report Format

**Purpose:** Extend the review report format to include SP-specific sections.

**Specification:**

Add new sections to `ReviewReport.to_markdown()`:

```python
def to_markdown(self) -> str:
    """Generate markdown report with SP-specific sections."""

    # Existing sections...

    # New SP section
    if self.sp_analysis:
        lines.append("## Stored Procedure Analysis")
        lines.append("")
        lines.append(f"**Procedure:** {self.sp_analysis.procedure_name}")
        lines.append(f"**Source Lines:** {self.sp_analysis.source_lines}")
        lines.append(f"**Target Lines:** {self.sp_analysis.target_lines}")
        lines.append(f"**Conversion Score:** {self.sp_analysis.score:.0%}")
        lines.append("")

        # Pattern conversion table
        lines.append("### Pattern Conversion Status")
        lines.append("")
        lines.append("| Pattern | Source | Target | Status |")
        lines.append("|---------|--------|--------|--------|")
        for pattern in self.sp_analysis.patterns:
            status = "CONVERTED" if pattern.converted else "MISSING"
            lines.append(f"| {pattern.name} | {pattern.source_count} | {pattern.target_count} | {status} |")

        # Dependency table
        lines.append("")
        lines.append("### Dependencies")
        lines.append("")
        lines.append("| Object | Type | Status | Databricks Equivalent |")
        lines.append("|--------|------|--------|----------------------|")
        for dep in self.sp_analysis.dependencies:
            status = "RESOLVED" if dep.is_resolved else "**MISSING**"
            lines.append(f"| {dep.target_object} | {dep.dependency_type} | {status} | {dep.databricks_equivalent or 'N/A'} |")
```

---

## File Changes Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `analyzer.py` | Modify | Add SP_SOURCE_PATTERNS, SP_TARGET_PATTERNS, new methods |
| `analyzer_business_logic.py` | New | Business logic extraction and comparison |
| `analyzer_dependencies.py` | New | Dependency tracking and resolution |
| `SKILL.md` | Modify | Add SP review workflow and checklists |
| `review-checklists.md` | Modify | Add SP-specific checklists |

---

## Acceptance Criteria

1. **Pattern Detection:** Running `analyzer.detect_sp_patterns()` on any source T-SQL returns all pattern types with line numbers
2. **Conversion Validation:** Running `validator.validate_conversion()` identifies all unconverted patterns as HIGH/BLOCKER issues
3. **Business Logic:** Running `extractor.compare_logic()` detects missing tables, columns, or transformations
4. **Dependencies:** Running `tracker.extract_dependencies()` identifies all table/proc/function references
5. **Checklist:** Generated checklist covers all patterns found in source
6. **Report:** Review report includes SP-specific sections with conversion score

---

## Testing Plan

| Test Case | Input | Expected Output |
|-----------|-------|-----------------|
| Detect CURSOR | `stg.spCalculateFactWorkersShifts.sql` | 3 CURSOR patterns detected |
| Detect SPATIAL | `stg.spDeltaSyncFactWorkersHistory.sql` | SPATIAL pattern detected |
| Validate Conversion | Source + `calc_fact_workers_shifts.py` | CURSOR converted, score > 0.8 |
| Track Dependencies | `stg.spDeltaSyncFactWorkersHistory.sql` | 15+ table dependencies |
| Generate Checklist | Any procedure | Complete markdown checklist |

---

*Document maintained by: Migration Team*
*Last updated: 2026-01-25*
