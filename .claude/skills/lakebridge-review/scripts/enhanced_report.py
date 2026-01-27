"""
Enhanced Report Format for SP Migration Review.

REQ-R7: Extends the review report format to include SP-specific sections,
comprehensive analysis results, and actionable recommendations.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


@dataclass
class PatternConversionStatus:
    """Status of a pattern conversion."""
    pattern_name: str
    source_count: int
    target_count: int
    converted: bool
    evidence: Optional[str] = None


@dataclass
class DependencyStatus:
    """Status of a dependency."""
    target_object: str
    dependency_type: str
    is_resolved: bool
    databricks_equivalent: Optional[str] = None


@dataclass
class SPAnalysisResult:
    """Complete SP analysis result for reporting."""
    procedure_name: str
    source_path: Optional[str] = None
    target_path: Optional[str] = None
    source_lines: int = 0
    target_lines: int = 0
    conversion_score: float = 0.0
    complexity_score: int = 0
    patterns: List[PatternConversionStatus] = field(default_factory=list)
    dependencies: List[DependencyStatus] = field(default_factory=list)
    business_logic_issues: List[Dict[str, Any]] = field(default_factory=list)
    type_validation_issues: List[Dict[str, Any]] = field(default_factory=list)


class EnhancedReviewReport:
    """
    Enhanced review report with SP-specific sections.

    Extends standard ReviewReport with:
    - Stored Procedure Analysis section
    - Pattern conversion status table
    - Dependency resolution table
    - Business logic comparison summary
    - Data type validation summary
    - Comprehensive recommendations
    """

    def __init__(
        self,
        summary: str,
        verdict: str,
        issues: List[Any] = None,
        sp_analysis: Optional[SPAnalysisResult] = None,
        git_diff_stats: Optional[str] = None,
        plan_path: Optional[str] = None,
    ):
        self.summary = summary
        self.verdict = verdict
        self.issues = issues or []
        self.sp_analysis = sp_analysis
        self.git_diff_stats = git_diff_stats
        self.plan_path = plan_path
        self.generated_at = datetime.now()

    @property
    def blockers(self) -> List:
        """Get BLOCKER tier issues."""
        return [i for i in self.issues if getattr(i, 'risk_tier', None) and i.risk_tier.value == "BLOCKER"]

    @property
    def high_risk(self) -> List:
        """Get HIGH tier issues."""
        return [i for i in self.issues if getattr(i, 'risk_tier', None) and i.risk_tier.value == "HIGH"]

    def to_markdown(self) -> str:
        """Generate enhanced markdown report with SP-specific sections."""
        lines = [
            "# SP Migration Review Report",
            "",
            f"**Generated:** {self.generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        ]

        if self.plan_path:
            lines.append(f"**Plan Reference:** {self.plan_path}")
        if self.git_diff_stats:
            lines.append(f"**Git Diff Summary:** {self.git_diff_stats}")

        lines.append(f"**Verdict:** {self.verdict}")
        lines.append("")
        lines.append("---")
        lines.append("")

        # Executive Summary
        lines.extend(self._generate_executive_summary())

        # SP Analysis Section (new)
        if self.sp_analysis:
            lines.extend(self._generate_sp_analysis_section())

        # Quick Reference Table
        lines.extend(self._generate_quick_reference())

        # Issues by Tier
        lines.extend(self._generate_issues_by_tier())

        # Recommendations
        lines.extend(self._generate_recommendations())

        # Final Verdict
        lines.extend(self._generate_final_verdict())

        return "\n".join(lines)

    def _generate_executive_summary(self) -> List[str]:
        """Generate executive summary section."""
        lines = [
            "## Executive Summary",
            "",
            self.summary,
            "",
        ]

        # Add metrics if SP analysis available
        if self.sp_analysis:
            lines.extend([
                "### Key Metrics",
                "",
                "| Metric | Value |",
                "|--------|-------|",
                f"| Procedure | `{self.sp_analysis.procedure_name}` |",
                f"| Conversion Score | {self.sp_analysis.conversion_score:.0%} |",
                f"| Complexity Score | {self.sp_analysis.complexity_score}/10 |",
                f"| Source Lines | {self.sp_analysis.source_lines} |",
                f"| Target Lines | {self.sp_analysis.target_lines} |",
                "",
            ])

        lines.append("---")
        lines.append("")

        return lines

    def _generate_sp_analysis_section(self) -> List[str]:
        """Generate SP-specific analysis section."""
        sp = self.sp_analysis
        lines = [
            "## Stored Procedure Analysis",
            "",
            f"**Procedure:** `{sp.procedure_name}`",
        ]

        if sp.source_path:
            lines.append(f"**Source:** `{sp.source_path}`")
        if sp.target_path:
            lines.append(f"**Target:** `{sp.target_path}`")

        lines.append(f"**Conversion Score:** {sp.conversion_score:.0%}")
        lines.append("")

        # Pattern Conversion Table
        if sp.patterns:
            lines.extend([
                "### Pattern Conversion Status",
                "",
                "| Pattern | Source | Target | Status |",
                "|---------|--------|--------|--------|",
            ])

            for pattern in sp.patterns:
                status = "✅ CONVERTED" if pattern.converted else "❌ MISSING"
                evidence = f" ({pattern.evidence})" if pattern.evidence else ""
                lines.append(
                    f"| {pattern.pattern_name} | {pattern.source_count} | {pattern.target_count} | {status}{evidence} |"
                )

            lines.append("")

        # Dependency Table
        if sp.dependencies:
            resolved = [d for d in sp.dependencies if d.is_resolved]
            unresolved = [d for d in sp.dependencies if not d.is_resolved]

            lines.extend([
                "### Dependencies",
                "",
                "| Object | Type | Status | Databricks Equivalent |",
                "|--------|------|--------|----------------------|",
            ])

            for dep in sp.dependencies:
                status = "✅ RESOLVED" if dep.is_resolved else "❌ **MISSING**"
                databricks = dep.databricks_equivalent or "N/A"
                lines.append(
                    f"| `{dep.target_object}` | {dep.dependency_type} | {status} | `{databricks}` |"
                )

            lines.append("")

            if unresolved:
                lines.extend([
                    "#### Unresolved Dependencies (BLOCKERS)",
                    "",
                ])
                for dep in unresolved:
                    lines.append(f"- `{dep.target_object}` ({dep.dependency_type}) - **ACTION REQUIRED**")
                lines.append("")

        # Business Logic Issues
        if sp.business_logic_issues:
            lines.extend([
                "### Business Logic Issues",
                "",
                "| Type | Source | Target | Risk | Message |",
                "|------|--------|--------|------|---------|",
            ])

            for issue in sp.business_logic_issues[:10]:  # Limit to 10
                lines.append(
                    f"| {issue.get('type', 'N/A')} | {issue.get('source', 'N/A')} | "
                    f"{issue.get('target', 'N/A')} | {issue.get('risk', 'N/A')} | {issue.get('message', '')} |"
                )

            lines.append("")

        # Data Type Issues
        if sp.type_validation_issues:
            lines.extend([
                "### Data Type Validation Issues",
                "",
                "| Column | Source Type | Target Type | Severity | Message |",
                "|--------|-------------|-------------|----------|---------|",
            ])

            for issue in sp.type_validation_issues[:10]:  # Limit to 10
                severity_icon = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}.get(
                    issue.get('severity', 'info'), "❓"
                )
                lines.append(
                    f"| {issue.get('column', 'N/A')} | {issue.get('source_type', 'N/A')} | "
                    f"{issue.get('target_type', 'N/A')} | {severity_icon} | {issue.get('message', '')} |"
                )

            lines.append("")

        lines.append("---")
        lines.append("")

        return lines

    def _generate_quick_reference(self) -> List[str]:
        """Generate quick reference table."""
        if not self.issues:
            return []

        lines = [
            "## Quick Reference",
            "",
            "| # | Description | Risk Level | Category |",
            "|---|-------------|------------|----------|",
        ]

        for i, issue in enumerate(self.issues, 1):
            risk = getattr(issue, 'risk_tier', None)
            risk_val = risk.value if risk else "N/A"
            category = getattr(issue, 'category', None)
            cat_val = category.value if category else "N/A"
            title = getattr(issue, 'title', str(issue))

            lines.append(f"| {i} | {title[:50]}{'...' if len(title) > 50 else ''} | {risk_val} | {cat_val} |")

        lines.extend(["", "---", ""])

        return lines

    def _generate_issues_by_tier(self) -> List[str]:
        """Generate issues grouped by tier."""
        lines = []

        tier_groups = [
            ("BLOCKER", self.blockers, "BLOCKERS (Must Fix Before Merge)"),
            ("HIGH", self.high_risk, "HIGH RISK (Should Fix Before Merge)"),
        ]

        for tier_name, tier_issues, header in tier_groups:
            if tier_issues:
                lines.extend([
                    f"## {header}",
                    "",
                ])

                for i, issue in enumerate(tier_issues, 1):
                    title = getattr(issue, 'title', f"Issue {i}")
                    desc = getattr(issue, 'description', str(issue))
                    file_path = getattr(issue, 'file_path', None)
                    line_start = getattr(issue, 'line_start', None)
                    code_snippet = getattr(issue, 'code_snippet', None)
                    solutions = getattr(issue, 'solutions', [])

                    lines.extend([
                        f"### Issue #{i}: {title}",
                        "",
                        f"**Description:** {desc}",
                        "",
                    ])

                    if file_path:
                        lines.append("**Location:**")
                        lines.append(f"- File: `{file_path}`")
                        if line_start:
                            lines.append(f"- Line: {line_start}")
                        lines.append("")

                    if code_snippet:
                        lines.extend([
                            "**Code:**",
                            "```",
                            code_snippet[:500],  # Limit snippet length
                            "```",
                            "",
                        ])

                    if solutions:
                        lines.append("**Solutions:**")
                        for j, sol in enumerate(solutions, 1):
                            lines.append(f"{j}. {sol}")
                        lines.append("")

                    lines.extend(["---", ""])

        return lines

    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations section."""
        lines = [
            "## Recommendations",
            "",
        ]

        if self.sp_analysis:
            sp = self.sp_analysis

            # Unresolved dependencies
            unresolved = [d for d in sp.dependencies if not d.is_resolved]
            if unresolved:
                lines.append("### Dependency Resolution")
                lines.append("")
                lines.append("Resolve these dependencies before proceeding:")
                lines.append("")
                for dep in unresolved:
                    lines.append(f"1. **`{dep.target_object}`** ({dep.dependency_type})")
                    lines.append(f"   - Check if table/view exists in Databricks")
                    lines.append(f"   - Migrate source data if not present")
                    lines.append(f"   - Create UDF if function dependency")
                lines.append("")

            # Unconverted patterns
            unconverted = [p for p in sp.patterns if not p.converted]
            if unconverted:
                lines.append("### Pattern Conversion")
                lines.append("")
                for pattern in unconverted:
                    lines.append(f"1. **{pattern.pattern_name}** ({pattern.source_count} occurrences)")
                    lines.append(f"   - Use appropriate Spark equivalent")
                    lines.append(f"   - Test conversion thoroughly")
                lines.append("")

            # Score-based recommendations
            if sp.conversion_score < 0.8:
                lines.extend([
                    "### Conversion Quality",
                    "",
                    f"Current conversion score ({sp.conversion_score:.0%}) is below target (80%).",
                    "",
                    "- Review all unconverted patterns",
                    "- Ensure business logic is preserved",
                    "- Add validation tests",
                    "",
                ])

        if not any([self.blockers, self.high_risk]):
            lines.extend([
                "### Next Steps",
                "",
                "1. Deploy to staging environment",
                "2. Run validation notebook",
                "3. Compare results with source",
                "4. Schedule production deployment",
                "",
            ])

        return lines

    def _generate_final_verdict(self) -> List[str]:
        """Generate final verdict section."""
        lines = [
            "## Final Verdict",
            "",
            f"**Status:** {self.verdict}",
            "",
        ]

        if self.blockers:
            lines.append(f"**Reasoning:** {len(self.blockers)} blocker(s) must be resolved before merge.")
            lines.append("")
            lines.append("**Required Actions:**")
            for i, issue in enumerate(self.blockers, 1):
                title = getattr(issue, 'title', f"Issue {i}")
                lines.append(f"{i}. Fix: {title}")
        elif self.high_risk:
            lines.append(f"**Reasoning:** {len(self.high_risk)} high-risk issue(s) should be addressed.")
        else:
            lines.append("**Reasoning:** All checks passed. Code is ready for deployment.")

        lines.extend([
            "",
            "---",
            "",
            f"*Report generated by Lakebridge Review Skill - {self.generated_at.strftime('%Y-%m-%d %H:%M:%S')}*",
        ])

        return lines

    def save(self, output_path: str) -> str:
        """Save report to file."""
        from pathlib import Path

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_markdown(), encoding="utf-8")

        return str(path)


class ReportBuilder:
    """
    Builder for creating enhanced review reports.

    Integrates with Phase 1-4 components to build comprehensive reports.
    """

    def __init__(self, procedure_name: str):
        self.procedure_name = procedure_name
        self.summary = ""
        self.issues = []
        self.sp_analysis = SPAnalysisResult(procedure_name=procedure_name)
        self.git_diff_stats = None
        self.plan_path = None

    def set_summary(self, summary: str) -> 'ReportBuilder':
        """Set executive summary."""
        self.summary = summary
        return self

    def add_issue(self, issue: Any) -> 'ReportBuilder':
        """Add a review issue."""
        self.issues.append(issue)
        return self

    def set_source_info(
        self,
        source_path: str,
        source_lines: int
    ) -> 'ReportBuilder':
        """Set source file information."""
        self.sp_analysis.source_path = source_path
        self.sp_analysis.source_lines = source_lines
        return self

    def set_target_info(
        self,
        target_path: str,
        target_lines: int
    ) -> 'ReportBuilder':
        """Set target file information."""
        self.sp_analysis.target_path = target_path
        self.sp_analysis.target_lines = target_lines
        return self

    def set_conversion_score(self, score: float) -> 'ReportBuilder':
        """Set conversion score (0.0 to 1.0)."""
        self.sp_analysis.conversion_score = score
        return self

    def set_complexity_score(self, score: int) -> 'ReportBuilder':
        """Set complexity score (1-10)."""
        self.sp_analysis.complexity_score = score
        return self

    def add_pattern(
        self,
        name: str,
        source_count: int,
        target_count: int,
        converted: bool,
        evidence: Optional[str] = None
    ) -> 'ReportBuilder':
        """Add pattern conversion status."""
        self.sp_analysis.patterns.append(PatternConversionStatus(
            pattern_name=name,
            source_count=source_count,
            target_count=target_count,
            converted=converted,
            evidence=evidence
        ))
        return self

    def add_dependency(
        self,
        target_object: str,
        dependency_type: str,
        is_resolved: bool,
        databricks_equivalent: Optional[str] = None
    ) -> 'ReportBuilder':
        """Add dependency status."""
        self.sp_analysis.dependencies.append(DependencyStatus(
            target_object=target_object,
            dependency_type=dependency_type,
            is_resolved=is_resolved,
            databricks_equivalent=databricks_equivalent
        ))
        return self

    def add_business_logic_issue(self, issue: Dict[str, Any]) -> 'ReportBuilder':
        """Add business logic issue."""
        self.sp_analysis.business_logic_issues.append(issue)
        return self

    def add_type_validation_issue(self, issue: Dict[str, Any]) -> 'ReportBuilder':
        """Add type validation issue."""
        self.sp_analysis.type_validation_issues.append(issue)
        return self

    def set_git_diff_stats(self, stats: str) -> 'ReportBuilder':
        """Set git diff statistics."""
        self.git_diff_stats = stats
        return self

    def set_plan_path(self, path: str) -> 'ReportBuilder':
        """Set plan file path."""
        self.plan_path = path
        return self

    def build(self) -> EnhancedReviewReport:
        """Build the enhanced review report."""
        # Determine verdict
        has_blockers = any(
            getattr(i, 'risk_tier', None) and i.risk_tier.value == "BLOCKER"
            for i in self.issues
        )
        unresolved_deps = [d for d in self.sp_analysis.dependencies if not d.is_resolved]
        unconverted_patterns = [p for p in self.sp_analysis.patterns if not p.converted]

        if has_blockers or unresolved_deps:
            verdict = "FAIL"
        elif unconverted_patterns or self.sp_analysis.conversion_score < 0.8:
            verdict = "NEEDS_WORK"
        else:
            verdict = "PASS"

        return EnhancedReviewReport(
            summary=self.summary,
            verdict=verdict,
            issues=self.issues,
            sp_analysis=self.sp_analysis,
            git_diff_stats=self.git_diff_stats,
            plan_path=self.plan_path,
        )
