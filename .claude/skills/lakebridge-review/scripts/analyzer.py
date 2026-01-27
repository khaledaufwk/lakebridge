"""
Code analysis utilities for Lakebridge skills.

Provides automated analysis patterns for code review.
Enhanced with stored procedure pattern detection for T-SQL to Databricks migration.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum
from datetime import datetime
from pathlib import Path
import re


class RiskTier(Enum):
    """Risk tiers for code review issues."""
    BLOCKER = "BLOCKER"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class IssueCategory(Enum):
    """Categories of issues."""
    SECURITY = "security"
    PERFORMANCE = "performance"
    ERROR_HANDLING = "error_handling"
    CODE_QUALITY = "code_quality"
    TESTING = "testing"
    DOCUMENTATION = "documentation"
    MIGRATION = "migration"
    CONFIGURATION = "configuration"


@dataclass
class Issue:
    """A code review issue."""
    title: str
    description: str
    risk_tier: RiskTier
    category: IssueCategory
    file_path: Optional[str] = None
    line_start: Optional[int] = None
    line_end: Optional[int] = None
    code_snippet: Optional[str] = None
    solutions: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "description": self.description,
            "risk_tier": self.risk_tier.value,
            "category": self.category.value,
            "file_path": self.file_path,
            "lines": f"{self.line_start}-{self.line_end}" if self.line_start else None,
            "solutions": self.solutions,
        }


@dataclass
class ReviewReport:
    """Complete code review report."""
    summary: str
    verdict: str  # "PASS" or "FAIL"
    issues: List[Issue] = field(default_factory=list)
    git_diff_stats: Optional[str] = None
    plan_path: Optional[str] = None
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def blockers(self) -> List[Issue]:
        """Get BLOCKER tier issues."""
        return [i for i in self.issues if i.risk_tier == RiskTier.BLOCKER]

    @property
    def high_risk(self) -> List[Issue]:
        """Get HIGH tier issues."""
        return [i for i in self.issues if i.risk_tier == RiskTier.HIGH]

    @property
    def medium_risk(self) -> List[Issue]:
        """Get MEDIUM tier issues."""
        return [i for i in self.issues if i.risk_tier == RiskTier.MEDIUM]

    @property
    def low_risk(self) -> List[Issue]:
        """Get LOW tier issues."""
        return [i for i in self.issues if i.risk_tier == RiskTier.LOW]

    @property
    def issue_counts(self) -> Dict[str, int]:
        """Get issue counts by tier."""
        return {
            "blocker": len(self.blockers),
            "high": len(self.high_risk),
            "medium": len(self.medium_risk),
            "low": len(self.low_risk),
            "total": len(self.issues),
        }

    def to_markdown(self) -> str:
        """Generate markdown report."""
        lines = [
            "# Code Review Report",
            "",
            f"**Generated**: {self.generated_at.isoformat()}",
        ]

        if self.plan_path:
            lines.append(f"**Plan Reference**: {self.plan_path}")
        if self.git_diff_stats:
            lines.append(f"**Git Diff Summary**: {self.git_diff_stats}")

        lines.append(f"**Verdict**: {'FAIL' if self.blockers else 'PASS'}")
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("## Executive Summary")
        lines.append("")
        lines.append(self.summary)
        lines.append("")
        lines.append("---")
        lines.append("")

        # Quick reference table
        lines.append("## Quick Reference")
        lines.append("")
        lines.append("| # | Description | Risk Level | Solution |")
        lines.append("|---|-------------|------------|----------|")
        for i, issue in enumerate(self.issues, 1):
            solution = issue.solutions[0] if issue.solutions else "See details"
            lines.append(f"| {i} | {issue.title} | {issue.risk_tier.value} | {solution[:40]}... |")
        lines.append("")
        lines.append("---")
        lines.append("")

        # Issues by tier
        for tier, tier_issues, emoji in [
            (RiskTier.BLOCKER, self.blockers, "BLOCKERS (Must Fix Before Merge)"),
            (RiskTier.HIGH, self.high_risk, "HIGH RISK (Should Fix Before Merge)"),
            (RiskTier.MEDIUM, self.medium_risk, "MEDIUM RISK (Fix Soon)"),
            (RiskTier.LOW, self.low_risk, "LOW RISK (Nice to Have)"),
        ]:
            if tier_issues:
                lines.append(f"## {emoji}")
                lines.append("")
                for i, issue in enumerate(tier_issues, 1):
                    lines.append(f"### Issue #{i}: {issue.title}")
                    lines.append("")
                    lines.append(f"**Description**: {issue.description}")
                    lines.append("")
                    if issue.file_path:
                        lines.append("**Location**:")
                        lines.append(f"- File: `{issue.file_path}`")
                        if issue.line_start:
                            lines.append(f"- Lines: `{issue.line_start}-{issue.line_end or issue.line_start}`")
                        lines.append("")
                    if issue.code_snippet:
                        lines.append("**Offending Code**:")
                        lines.append("```")
                        lines.append(issue.code_snippet)
                        lines.append("```")
                        lines.append("")
                    if issue.solutions:
                        lines.append("**Recommended Solutions**:")
                        for j, sol in enumerate(issue.solutions, 1):
                            lines.append(f"{j}. {sol}")
                        lines.append("")
                    lines.append("---")
                    lines.append("")

        # Final verdict
        lines.append("## Final Verdict")
        lines.append("")
        lines.append(f"**Status**: {'FAIL' if self.blockers else 'PASS'}")
        lines.append("")
        if self.blockers:
            lines.append(f"**Reasoning**: {len(self.blockers)} blocker(s) must be fixed before merge.")
        else:
            lines.append("**Reasoning**: No blockers found. Code is ready for merge.")
        lines.append("")

        return "\n".join(lines)

    def save(self, output_dir: str = "app_review/") -> str:
        """Save report to file."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = self.generated_at.strftime("%Y%m%d_%H%M%S")
        filename = f"review_{timestamp}.md"
        filepath = output_path / filename

        filepath.write_text(self.to_markdown(), encoding="utf-8")
        return str(filepath)


class CodeAnalyzer:
    """
    Automated code analysis for review.

    Provides pattern-based detection of common issues.

    Usage:
        analyzer = CodeAnalyzer()

        # Analyze a file
        issues = analyzer.analyze_file("path/to/file.py")

        # Check for anti-patterns
        issues = analyzer.check_patterns(code_content)

        # Migration-specific checks
        issues = analyzer.check_migration_requirements()
    """

    # Patterns that indicate potential issues
    SECURITY_PATTERNS = [
        (r"password\s*=\s*['\"][^'\"]+['\"]", "Hardcoded password", RiskTier.BLOCKER),
        (r"token\s*=\s*['\"][^'\"]+['\"]", "Hardcoded token", RiskTier.BLOCKER),
        (r"secret\s*=\s*['\"][^'\"]+['\"]", "Hardcoded secret", RiskTier.BLOCKER),
        (r"api_key\s*=\s*['\"][^'\"]+['\"]", "Hardcoded API key", RiskTier.BLOCKER),
    ]

    CODE_QUALITY_PATTERNS = [
        (r"#\s*TODO", "TODO comment found", RiskTier.MEDIUM),
        (r"#\s*FIXME", "FIXME comment found", RiskTier.MEDIUM),
        (r"#\s*HACK", "HACK comment found", RiskTier.MEDIUM),
        (r"print\s*\(", "Debug print statement", RiskTier.LOW),
        (r"console\.log\s*\(", "Debug console.log", RiskTier.LOW),
    ]

    MIGRATION_PATTERNS = [
        (r"serverless\s*=\s*False", "serverless=False may cause quota issues", RiskTier.HIGH),
        (r"_ingested_at", "Generic column name may cause AMBIGUOUS_REFERENCE", RiskTier.MEDIUM),
    ]

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

    def __init__(self):
        """Initialize analyzer."""
        pass

    def analyze_content(self, content: str, file_path: Optional[str] = None) -> List[Issue]:
        """
        Analyze code content for issues.

        Returns list of detected issues.
        """
        issues = []

        # Check security patterns
        for pattern, title, tier in self.SECURITY_PATTERNS:
            matches = list(re.finditer(pattern, content, re.IGNORECASE))
            for match in matches:
                line_num = content[:match.start()].count('\n') + 1
                issues.append(Issue(
                    title=title,
                    description=f"Found {title.lower()} in code",
                    risk_tier=tier,
                    category=IssueCategory.SECURITY,
                    file_path=file_path,
                    line_start=line_num,
                    code_snippet=match.group(0)[:50] + "...",
                    solutions=["Use environment variables or secret management"],
                ))

        # Check code quality patterns
        for pattern, title, tier in self.CODE_QUALITY_PATTERNS:
            matches = list(re.finditer(pattern, content))
            for match in matches:
                line_num = content[:match.start()].count('\n') + 1
                issues.append(Issue(
                    title=title,
                    description=f"Found {title.lower()}",
                    risk_tier=tier,
                    category=IssueCategory.CODE_QUALITY,
                    file_path=file_path,
                    line_start=line_num,
                    code_snippet=match.group(0),
                    solutions=["Address or remove before merge"],
                ))

        # Check migration patterns
        for pattern, title, tier in self.MIGRATION_PATTERNS:
            matches = list(re.finditer(pattern, content))
            for match in matches:
                line_num = content[:match.start()].count('\n') + 1
                issues.append(Issue(
                    title=title,
                    description=title,
                    risk_tier=tier,
                    category=IssueCategory.MIGRATION,
                    file_path=file_path,
                    line_start=line_num,
                    code_snippet=match.group(0),
                    solutions=["Use serverless=True", "Use unique column prefixes"],
                ))

        return issues

    def check_dlt_notebook(self, content: str, file_path: Optional[str] = None) -> List[Issue]:
        """
        Check a DLT notebook for common issues.

        Returns list of issues specific to DLT notebooks.
        """
        issues = []

        # Check notebook format
        if not content.startswith("# Databricks notebook source"):
            issues.append(Issue(
                title="Missing notebook header",
                description="First line must be '# Databricks notebook source'",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.MIGRATION,
                file_path=file_path,
                solutions=["Add '# Databricks notebook source' as first line"],
            ))

        # Check for DLT import
        if "import dlt" not in content:
            issues.append(Issue(
                title="Missing DLT import",
                description="Notebook must import dlt module",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.MIGRATION,
                file_path=file_path,
                solutions=["Add 'import dlt' statement"],
            ))

        # Check for table decorators
        if "@dlt.table" not in content:
            issues.append(Issue(
                title="No DLT tables defined",
                description="No @dlt.table decorators found - will cause NO_TABLES_IN_PIPELINE",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.MIGRATION,
                file_path=file_path,
                solutions=["Add @dlt.table decorators to table functions"],
            ))

        # Check for cell separators
        if "# COMMAND ----------" not in content:
            issues.append(Issue(
                title="Missing cell separators",
                description="Notebook should have '# COMMAND ----------' between cells",
                risk_tier=RiskTier.MEDIUM,
                category=IssueCategory.MIGRATION,
                file_path=file_path,
                solutions=["Add cell separators between logical sections"],
            ))

        # Check for ambiguous column names
        ingested_at_matches = re.findall(r'\.withColumn\s*\(\s*["\']_ingested_at["\']', content)
        if len(ingested_at_matches) > 1:
            issues.append(Issue(
                title="Potential AMBIGUOUS_REFERENCE",
                description=f"Found {len(ingested_at_matches)} tables with '_ingested_at' column",
                risk_tier=RiskTier.HIGH,
                category=IssueCategory.MIGRATION,
                file_path=file_path,
                solutions=["Use unique column names like 'worker_ingested_at', 'project_ingested_at'"],
            ))

        return issues

    def check_migration_deployment(
        self,
        has_schema: bool,
        has_secret_scope: bool,
        has_secrets: bool,
        uses_serverless: bool,
    ) -> List[Issue]:
        """
        Check migration deployment requirements.

        Returns list of deployment-related issues.
        """
        issues = []

        if not has_schema:
            issues.append(Issue(
                title="Missing target schema",
                description="Target schema must exist before pipeline runs",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.CONFIGURATION,
                solutions=["Create schema with: w.schemas.create(name=schema, catalog_name=catalog)"],
            ))

        if not has_secret_scope:
            issues.append(Issue(
                title="Missing secret scope",
                description="Secret scope required for SQL Server credentials",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.CONFIGURATION,
                solutions=["Create scope with: w.secrets.create_scope(scope=scope_name)"],
            ))

        if not has_secrets:
            issues.append(Issue(
                title="Missing secrets",
                description="SQL Server secrets not configured",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.CONFIGURATION,
                solutions=["Set sqlserver_jdbc_url, sqlserver_user, sqlserver_password secrets"],
            ))

        if not uses_serverless:
            issues.append(Issue(
                title="Not using serverless compute",
                description="Pipeline may fail with WAITING_FOR_RESOURCES due to VM quota",
                risk_tier=RiskTier.HIGH,
                category=IssueCategory.CONFIGURATION,
                solutions=["Set serverless=True when creating pipeline"],
            ))

        return issues

    def create_report(
        self,
        issues: List[Issue],
        summary: str,
        plan_path: Optional[str] = None,
        git_diff_stats: Optional[str] = None,
    ) -> ReviewReport:
        """Create a review report from issues."""
        has_blockers = any(i.risk_tier == RiskTier.BLOCKER for i in issues)

        return ReviewReport(
            summary=summary,
            verdict="FAIL" if has_blockers else "PASS",
            issues=issues,
            plan_path=plan_path,
            git_diff_stats=git_diff_stats,
        )

    def detect_sp_patterns(self, source_sql: str, file_path: Optional[str] = None) -> Dict[str, List[Dict]]:
        """
        Detect T-SQL patterns in source stored procedure.

        Args:
            source_sql: The T-SQL source code to analyze
            file_path: Optional file path for reporting

        Returns:
            Dict with pattern names as keys and list of match details as values.
            Each match includes: line_number, matched_text, context (surrounding lines)
        """
        results = {}
        lines = source_sql.split('\n')

        for pattern_name, (regex, description, conversion_target, risk_tier) in self.SP_SOURCE_PATTERNS.items():
            matches = []
            for match in re.finditer(regex, source_sql, re.IGNORECASE | re.MULTILINE):
                # Calculate line number
                line_num = source_sql[:match.start()].count('\n') + 1

                # Get context (3 lines before and after)
                start_line = max(0, line_num - 4)
                end_line = min(len(lines), line_num + 3)
                context_lines = lines[start_line:end_line]
                context = '\n'.join(context_lines)

                matches.append({
                    "line_number": line_num,
                    "matched_text": match.group(0)[:100],  # Truncate long matches
                    "context": context,
                    "description": description,
                    "conversion_target": conversion_target,
                    "risk_tier": risk_tier.value,
                })

            if matches:
                results[pattern_name] = matches

        return results

    def analyze_sp_complexity(self, source_sql: str) -> Dict[str, Any]:
        """
        Analyze stored procedure complexity.

        Returns complexity metrics and score (1-10 scale).
        """
        patterns = self.detect_sp_patterns(source_sql)
        lines = source_sql.split('\n')
        non_empty_lines = [l for l in lines if l.strip() and not l.strip().startswith('--')]

        # Count various elements
        pattern_counts = {name: len(matches) for name, matches in patterns.items()}
        total_patterns = sum(pattern_counts.values())

        # Extract table references
        table_refs = re.findall(
            r"(?:FROM|JOIN|INTO|UPDATE|MERGE\s+INTO?)\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?",
            source_sql,
            re.IGNORECASE
        )
        unique_tables = set(f"{t[0]}.{t[1]}" if t[0] else t[1] for t in table_refs)

        # Count function calls
        function_calls = re.findall(r"(\w+)\s*\(", source_sql)
        unique_functions = set(f.upper() for f in function_calls)

        # Calculate complexity score (1-10)
        score = 1
        score += min(3, len(non_empty_lines) // 200)  # Up to 3 points for length
        score += min(2, total_patterns // 3)  # Up to 2 points for pattern complexity
        score += min(2, len(unique_tables) // 5)  # Up to 2 points for table count
        if "CURSOR" in patterns or "SPATIAL_GEOGRAPHY" in patterns:
            score += 2  # High complexity patterns

        return {
            "line_count": len(lines),
            "non_empty_lines": len(non_empty_lines),
            "pattern_counts": pattern_counts,
            "total_patterns": total_patterns,
            "patterns_found": list(patterns.keys()),
            "table_references": list(unique_tables),
            "table_count": len(unique_tables),
            "function_count": len(unique_functions),
            "complexity_score": min(10, score),
            "high_risk_patterns": [
                name for name, matches in patterns.items()
                if self.SP_SOURCE_PATTERNS[name][3] == RiskTier.HIGH
            ],
        }

    def validate_sp_conversion(
        self,
        source_sql: str,
        target_notebook: str,
        procedure_name: str,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None,
    ) -> Tuple[List[Issue], Dict[str, Any]]:
        """
        Validate stored procedure conversion completeness.

        Compares source patterns to target implementations and identifies
        unconverted or partially converted patterns.

        Args:
            source_sql: Original T-SQL source code
            target_notebook: Converted Python notebook content
            procedure_name: Name of the procedure being validated
            source_path: Path to source file (for reporting)
            target_path: Path to target file (for reporting)

        Returns:
            Tuple of (issues_list, conversion_report_dict)
        """
        issues = []

        # Detect patterns in source
        source_patterns = self.detect_sp_patterns(source_sql)
        source_complexity = self.analyze_sp_complexity(source_sql)

        # Detect conversion patterns in target
        target_patterns = {}
        for pattern_name, (regex, description) in self.SP_TARGET_PATTERNS.items():
            matches = list(re.finditer(regex, target_notebook, re.IGNORECASE))
            if matches:
                target_patterns[pattern_name] = len(matches)

        # Map source patterns to expected target patterns
        pattern_mapping = {
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

        # Check each source pattern has corresponding target pattern
        patterns_converted = []
        patterns_missing = []

        for source_pattern, matches in source_patterns.items():
            expected_target = pattern_mapping.get(source_pattern)

            if expected_target:
                if expected_target in target_patterns:
                    patterns_converted.append({
                        "name": source_pattern,
                        "source_count": len(matches),
                        "target_pattern": expected_target,
                        "target_count": target_patterns[expected_target],
                        "status": "CONVERTED",
                    })
                else:
                    # Pattern not converted - create issue
                    pattern_info = self.SP_SOURCE_PATTERNS[source_pattern]
                    risk_tier = pattern_info[3]

                    patterns_missing.append({
                        "name": source_pattern,
                        "source_count": len(matches),
                        "expected_target": expected_target,
                        "risk_tier": risk_tier.value,
                        "recommendation": pattern_info[2],
                        "status": "MISSING",
                    })

                    issues.append(Issue(
                        title=f"Unconverted {source_pattern} pattern",
                        description=f"Found {len(matches)} {pattern_info[1]} in source but no {expected_target} in target",
                        risk_tier=risk_tier,
                        category=IssueCategory.MIGRATION,
                        file_path=target_path,
                        code_snippet=matches[0]["matched_text"] if matches else None,
                        solutions=[
                            f"Convert to: {pattern_info[2]}",
                            f"Expected pattern in target: {self.SP_TARGET_PATTERNS[expected_target][0][:50]}...",
                        ],
                    ))
            else:
                # Low-risk patterns that don't require specific conversion
                patterns_converted.append({
                    "name": source_pattern,
                    "source_count": len(matches),
                    "target_pattern": None,
                    "target_count": None,
                    "status": "LOW_RISK_OK",
                })

        # Calculate conversion score
        total_high_risk = len([p for p in source_patterns.keys()
                               if self.SP_SOURCE_PATTERNS[p][3] in (RiskTier.HIGH, RiskTier.BLOCKER)])
        converted_high_risk = len([p for p in patterns_converted
                                   if p["status"] == "CONVERTED" and
                                   self.SP_SOURCE_PATTERNS[p["name"]][3] in (RiskTier.HIGH, RiskTier.BLOCKER)])

        if total_high_risk > 0:
            conversion_score = converted_high_risk / total_high_risk
        else:
            conversion_score = 1.0 if not patterns_missing else 0.8

        # Determine verdict
        if patterns_missing:
            high_risk_missing = [p for p in patterns_missing if p["risk_tier"] in ("HIGH", "BLOCKER")]
            if high_risk_missing:
                verdict = "INCOMPLETE"
            else:
                verdict = "PARTIAL"
        else:
            verdict = "COMPLETE"

        # Build conversion report
        source_lines = len(source_sql.split('\n'))
        target_lines = len(target_notebook.split('\n'))

        conversion_report = {
            "procedure_name": procedure_name,
            "source_path": source_path,
            "target_path": target_path,
            "source_line_count": source_lines,
            "target_line_count": target_lines,
            "complexity_score": source_complexity["complexity_score"],
            "patterns_found": list(source_patterns.keys()),
            "patterns_converted": patterns_converted,
            "patterns_missing": patterns_missing,
            "conversion_score": conversion_score,
            "verdict": verdict,
            "target_patterns_found": list(target_patterns.keys()),
        }

        return issues, conversion_report

    def generate_sp_review_checklist(
        self,
        procedure_name: str,
        source_sql: str,
        target_notebook: str,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None,
    ) -> str:
        """
        Generate a comprehensive markdown checklist for SP review.

        Args:
            procedure_name: Name of the stored procedure
            source_sql: Original T-SQL source
            target_notebook: Converted Python notebook
            source_path: Path to source file
            target_path: Path to target file

        Returns:
            Markdown-formatted review checklist
        """
        issues, report = self.validate_sp_conversion(
            source_sql, target_notebook, procedure_name, source_path, target_path
        )
        complexity = self.analyze_sp_complexity(source_sql)

        lines = [
            f"## Stored Procedure Review: {procedure_name}",
            "",
            f"**Source:** `{source_path or 'N/A'}`",
            f"**Target:** `{target_path or 'N/A'}`",
            f"**Complexity Score:** {complexity['complexity_score']}/10",
            f"**Conversion Status:** {report['verdict']}",
            f"**Conversion Score:** {report['conversion_score']:.0%}",
            "",
            "---",
            "",
            "### 1. Source Analysis",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Lines of Code | {complexity['line_count']} |",
            f"| Non-Empty Lines | {complexity['non_empty_lines']} |",
            f"| Patterns Detected | {', '.join(complexity['patterns_found']) or 'None'} |",
            f"| Tables Referenced | {complexity['table_count']} |",
            f"| High-Risk Patterns | {', '.join(complexity['high_risk_patterns']) or 'None'} |",
            "",
            "---",
            "",
            "### 2. Pattern Conversion Status",
            "",
            "| Pattern | Found | Converted | Status |",
            "|---------|-------|-----------|--------|",
        ]

        # Add pattern rows
        all_patterns = report['patterns_converted'] + report['patterns_missing']
        for p in all_patterns:
            status_emoji = "CONVERTED" if p['status'] == 'CONVERTED' else "**MISSING**"
            target_count = p.get('target_count', '-')
            lines.append(f"| {p['name']} | {p['source_count']} | {target_count} | {status_emoji} |")

        lines.extend([
            "",
            "---",
            "",
            "### 3. Tables Referenced",
            "",
        ])

        for table in complexity['table_references'][:20]:  # Limit to 20
            lines.append(f"- [ ] `{table}` → Databricks equivalent verified")

        lines.extend([
            "",
            "---",
            "",
            "### 4. Conversion Issues",
            "",
        ])

        if issues:
            for issue in issues:
                lines.append(f"- **{issue.risk_tier.value}**: {issue.title}")
                lines.append(f"  - {issue.description}")
                if issue.solutions:
                    lines.append(f"  - Solution: {issue.solutions[0]}")
        else:
            lines.append("No conversion issues detected.")

        lines.extend([
            "",
            "---",
            "",
            "### 5. Testing Requirements",
            "",
            "- [ ] Validation notebook created",
            "- [ ] Row count comparison: Source vs Target",
            "- [ ] Sample record comparison (100 random rows)",
            "- [ ] Aggregation comparison (SUM, COUNT, AVG)",
            "- [ ] Edge case testing (NULL handling, empty sets)",
            "",
            "---",
            "",
            "### Review Verdict",
            "",
            f"**Status:** {report['verdict']}",
            "",
        ])

        if issues:
            blocker_count = len([i for i in issues if i.risk_tier == RiskTier.BLOCKER])
            high_count = len([i for i in issues if i.risk_tier == RiskTier.HIGH])
            lines.append(f"**Issues:** {blocker_count} BLOCKER, {high_count} HIGH, {len(issues) - blocker_count - high_count} OTHER")
        else:
            lines.append("**Issues:** None - Ready for deployment")

        return "\n".join(lines)


class ConversionValidator:
    """
    Validate stored procedure conversion completeness (REQ-R2).

    Provides comprehensive validation that all source patterns have been
    properly converted in the target notebook.
    """

    def __init__(self):
        self.analyzer = CodeAnalyzer()

    def validate(
        self,
        source_sql: str,
        target_notebook: str,
        procedure_name: str,
        source_path: Optional[str] = None,
        target_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validate conversion completeness.

        Args:
            source_sql: Original T-SQL stored procedure
            target_notebook: Converted Databricks notebook
            procedure_name: Name of the procedure
            source_path: Path to source file
            target_path: Path to target file

        Returns:
            Comprehensive validation report dictionary
        """
        # Get issues and conversion report from analyzer
        issues, conversion_report = self.analyzer.validate_sp_conversion(
            source_sql, target_notebook, procedure_name, source_path, target_path
        )

        # Analyze complexity
        complexity = self.analyzer.analyze_sp_complexity(source_sql)

        # Check notebook structure
        notebook_issues = self._validate_notebook_structure(target_notebook)
        issues.extend(notebook_issues)

        # Check for required sections
        section_issues = self._validate_required_sections(target_notebook)
        issues.extend(section_issues)

        # Check for common mistakes
        mistake_issues = self._check_common_mistakes(target_notebook, source_sql)
        issues.extend(mistake_issues)

        # Calculate final score
        final_score = self._calculate_final_score(
            conversion_report['conversion_score'],
            len(issues),
            len([i for i in issues if i.risk_tier == RiskTier.BLOCKER]),
            len([i for i in issues if i.risk_tier == RiskTier.HIGH]),
        )

        # Determine overall verdict
        if any(i.risk_tier == RiskTier.BLOCKER for i in issues):
            verdict = "FAIL"
        elif any(i.risk_tier == RiskTier.HIGH for i in issues):
            verdict = "NEEDS_WORK"
        elif conversion_report['verdict'] == "INCOMPLETE":
            verdict = "NEEDS_WORK"
        else:
            verdict = "PASS"

        return {
            "procedure_name": procedure_name,
            "source_path": source_path,
            "target_path": target_path,
            "complexity": complexity,
            "conversion_report": conversion_report,
            "issues": [i.to_dict() for i in issues],
            "issue_counts": {
                "blocker": len([i for i in issues if i.risk_tier == RiskTier.BLOCKER]),
                "high": len([i for i in issues if i.risk_tier == RiskTier.HIGH]),
                "medium": len([i for i in issues if i.risk_tier == RiskTier.MEDIUM]),
                "low": len([i for i in issues if i.risk_tier == RiskTier.LOW]),
                "total": len(issues),
            },
            "final_score": final_score,
            "verdict": verdict,
        }

    def _validate_notebook_structure(self, notebook: str) -> List[Issue]:
        """Validate notebook has correct structure."""
        issues = []

        # Check for notebook header
        if not notebook.startswith("# Databricks notebook source"):
            issues.append(Issue(
                title="Missing notebook header",
                description="Notebook must start with '# Databricks notebook source'",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.MIGRATION,
                solutions=["Add '# Databricks notebook source' as the first line"],
            ))

        # Check for cell separators
        if notebook.count("# COMMAND ----------") < 3:
            issues.append(Issue(
                title="Insufficient cell separators",
                description="Notebook should have multiple cells for organization",
                risk_tier=RiskTier.MEDIUM,
                category=IssueCategory.CODE_QUALITY,
                solutions=["Add '# COMMAND ----------' between logical sections"],
            ))

        # Check for markdown documentation
        if notebook.count("# MAGIC %md") < 2:
            issues.append(Issue(
                title="Insufficient documentation",
                description="Notebook should have markdown cells explaining the logic",
                risk_tier=RiskTier.LOW,
                category=IssueCategory.DOCUMENTATION,
                solutions=["Add markdown cells to explain each section"],
            ))

        return issues

    def _validate_required_sections(self, notebook: str) -> List[Issue]:
        """Validate required sections exist in notebook."""
        issues = []
        notebook_lower = notebook.lower()

        required_sections = [
            ("configuration", "Configuration section", RiskTier.HIGH),
            ("target_catalog", "Target catalog definition", RiskTier.HIGH),
            ("watermark", "Watermark handling", RiskTier.MEDIUM),
        ]

        for section, description, risk in required_sections:
            if section not in notebook_lower:
                issues.append(Issue(
                    title=f"Missing {description}",
                    description=f"Notebook should include {description}",
                    risk_tier=risk,
                    category=IssueCategory.MIGRATION,
                    solutions=[f"Add {description} section"],
                ))

        # Check for exit statement
        if "dbutils.notebook.exit" not in notebook:
            issues.append(Issue(
                title="Missing notebook exit",
                description="Notebook should call dbutils.notebook.exit() with status",
                risk_tier=RiskTier.MEDIUM,
                category=IssueCategory.MIGRATION,
                solutions=["Add dbutils.notebook.exit('SUCCESS') at the end"],
            ))

        return issues

    def _check_common_mistakes(self, notebook: str, source_sql: str) -> List[Issue]:
        """Check for common conversion mistakes."""
        issues = []

        # Check for TODO comments that might indicate incomplete conversion
        todo_count = len(re.findall(r'#\s*TODO', notebook, re.IGNORECASE))
        if todo_count > 5:
            issues.append(Issue(
                title=f"Many TODO comments ({todo_count})",
                description="Notebook has many TODO items that may need attention",
                risk_tier=RiskTier.MEDIUM,
                category=IssueCategory.CODE_QUALITY,
                solutions=["Review and address TODO items"],
            ))

        # Check for hardcoded values that should be parameters
        if re.search(r'ProjectId\s*==?\s*\d+', notebook, re.IGNORECASE):
            issues.append(Issue(
                title="Hardcoded ProjectId",
                description="ProjectId appears to be hardcoded instead of parameterized",
                risk_tier=RiskTier.MEDIUM,
                category=IssueCategory.CODE_QUALITY,
                solutions=["Use widget parameter for ProjectId filter"],
            ))

        # Check for print statements that should be removed
        debug_prints = len(re.findall(r'print\s*\([\'"]DEBUG', notebook))
        if debug_prints > 0:
            issues.append(Issue(
                title=f"Debug print statements ({debug_prints})",
                description="Notebook contains DEBUG print statements",
                risk_tier=RiskTier.LOW,
                category=IssueCategory.CODE_QUALITY,
                solutions=["Remove or comment out debug print statements"],
            ))

        # Check for NotImplementedError
        if "NotImplementedError" in notebook:
            issues.append(Issue(
                title="NotImplementedError in code",
                description="Notebook contains NotImplementedError - incomplete conversion",
                risk_tier=RiskTier.BLOCKER,
                category=IssueCategory.MIGRATION,
                solutions=["Implement the missing functionality"],
            ))

        # Check if source has tables that aren't referenced in target
        source_tables = set(re.findall(r'FROM\s+\[?(\w+)\]?\.\[?(\w+)\]?', source_sql, re.IGNORECASE))
        for schema, table in list(source_tables)[:10]:  # Check first 10
            # Look for silver_ or raw_ equivalents
            if not re.search(rf'(?:silver_|raw_|{table})', notebook, re.IGNORECASE):
                issues.append(Issue(
                    title=f"Table {schema}.{table} may not be referenced",
                    description=f"Source table {schema}.{table} may not have corresponding reference in notebook",
                    risk_tier=RiskTier.MEDIUM,
                    category=IssueCategory.MIGRATION,
                    solutions=[f"Verify {schema}.{table} is mapped to correct Databricks table"],
                ))

        return issues

    def _calculate_final_score(
        self,
        conversion_score: float,
        total_issues: int,
        blocker_count: int,
        high_count: int,
    ) -> float:
        """Calculate final validation score (0.0 to 1.0)."""
        if blocker_count > 0:
            return 0.0

        # Start with conversion score
        score = conversion_score

        # Deduct for issues
        score -= high_count * 0.1
        score -= (total_issues - high_count - blocker_count) * 0.02

        return max(0.0, min(1.0, score))

    def generate_report(self, validation_result: Dict[str, Any]) -> str:
        """Generate markdown report from validation result."""
        lines = [
            "# Conversion Validation Report",
            "",
            f"**Procedure:** `{validation_result['procedure_name']}`",
            f"**Verdict:** {validation_result['verdict']}",
            f"**Final Score:** {validation_result['final_score']:.0%}",
            "",
            "---",
            "",
            "## Summary",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Complexity Score | {validation_result['complexity']['complexity_score']}/10 |",
            f"| Conversion Score | {validation_result['conversion_report']['conversion_score']:.0%} |",
            f"| Total Issues | {validation_result['issue_counts']['total']} |",
            f"| Blockers | {validation_result['issue_counts']['blocker']} |",
            f"| High Risk | {validation_result['issue_counts']['high']} |",
            "",
            "---",
            "",
            "## Pattern Conversion",
            "",
            "| Pattern | Status |",
            "|---------|--------|",
        ]

        for p in validation_result['conversion_report'].get('patterns_converted', []):
            lines.append(f"| {p['name']} | CONVERTED |")
        for p in validation_result['conversion_report'].get('patterns_missing', []):
            lines.append(f"| {p['name']} | **MISSING** |")

        lines.extend([
            "",
            "---",
            "",
            "## Issues",
            "",
        ])

        issues_by_tier = {}
        for issue in validation_result['issues']:
            tier = issue['risk_tier']
            if tier not in issues_by_tier:
                issues_by_tier[tier] = []
            issues_by_tier[tier].append(issue)

        for tier in ['BLOCKER', 'HIGH', 'MEDIUM', 'LOW']:
            if tier in issues_by_tier:
                lines.append(f"### {tier}")
                lines.append("")
                for issue in issues_by_tier[tier]:
                    lines.append(f"- **{issue['title']}**")
                    lines.append(f"  - {issue['description']}")
                    if issue.get('solutions'):
                        lines.append(f"  - Solution: {issue['solutions'][0]}")
                lines.append("")

        if not validation_result['issues']:
            lines.append("No issues found.")

        lines.extend([
            "",
            "---",
            "",
            f"**Generated:** {datetime.now().isoformat()}",
        ])

        return "\n".join(lines)
