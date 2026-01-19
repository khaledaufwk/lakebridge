"""
Code analysis utilities for Lakebridge skills.

Provides automated analysis patterns for code review.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
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
