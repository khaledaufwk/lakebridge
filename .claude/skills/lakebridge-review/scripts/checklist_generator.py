"""
SP-Specific Review Checklist Generator.

REQ-R5: Generate comprehensive review checklists specific to each
stored procedure's patterns, dependencies, and conversion status.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime


@dataclass
class ChecklistItem:
    """A single checklist item."""
    category: str
    description: str
    is_checked: bool = False
    risk_level: str = "MEDIUM"  # BLOCKER, HIGH, MEDIUM, LOW
    notes: Optional[str] = None


@dataclass
class ChecklistSection:
    """A section of the checklist."""
    title: str
    items: List[ChecklistItem] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class SPChecklistConfig:
    """Configuration for checklist generation."""
    procedure_name: str
    source_path: Optional[str] = None
    target_path: Optional[str] = None
    source_line_count: int = 0
    target_line_count: int = 0
    patterns_found: List[Dict[str, Any]] = field(default_factory=list)
    patterns_converted: List[Dict[str, Any]] = field(default_factory=list)
    dependencies: List[Dict[str, Any]] = field(default_factory=list)
    business_logic_issues: List[Dict[str, Any]] = field(default_factory=list)
    conversion_score: float = 0.0
    complexity_score: int = 0


class SPChecklistGenerator:
    """
    Generate procedure-specific review checklists.

    Creates comprehensive markdown checklists covering:
    1. Source Analysis
    2. Pattern Conversion Status
    3. Business Logic Validation
    4. Dependency Resolution
    5. Data Quality Rules
    6. Performance Considerations
    7. Testing Requirements
    """

    def generate_checklist(
        self,
        config: SPChecklistConfig
    ) -> str:
        """
        Generate comprehensive markdown checklist for SP review.

        Args:
            config: SPChecklistConfig with procedure details

        Returns:
            Complete markdown checklist
        """
        sections = [
            self._generate_header(config),
            self._generate_source_analysis(config),
            self._generate_pattern_conversion_status(config),
            self._generate_business_logic_validation(config),
            self._generate_dependency_resolution(config),
            self._generate_data_quality_rules(config),
            self._generate_performance_considerations(config),
            self._generate_testing_requirements(config),
            self._generate_review_verdict(config),
        ]

        return "\n".join(sections)

    def generate_from_analysis(
        self,
        procedure_name: str,
        source_sql: str,
        target_notebook: str,
        conversion_result: Optional[Dict] = None,
        dependency_analysis: Optional[Dict] = None,
        business_logic_comparison: Optional[Dict] = None,
    ) -> str:
        """
        Generate checklist from analysis results.

        Integrates with Phase 1-3 components to auto-populate checklist.

        Args:
            procedure_name: Name of the stored procedure
            source_sql: Original T-SQL content
            target_notebook: Converted notebook content
            conversion_result: Result from SPConverter
            dependency_analysis: Result from DependencyAnalyzer
            business_logic_comparison: Result from BusinessLogicComparator

        Returns:
            Complete markdown checklist
        """
        # Calculate metrics
        source_lines = len(source_sql.split("\n"))
        target_lines = len(target_notebook.split("\n"))

        # Extract patterns found
        patterns_found = []
        patterns_converted = []
        if conversion_result:
            for p in conversion_result.get("patterns_converted", []):
                patterns_converted.append({"name": p, "converted": True})
            for p in conversion_result.get("patterns_manual", []):
                patterns_found.append({"name": p, "converted": False})

        # Extract dependencies
        dependencies = []
        if dependency_analysis:
            for dep in dependency_analysis.get("dependencies", []):
                dependencies.append(dep)

        # Extract business logic issues
        business_logic_issues = []
        if business_logic_comparison:
            for comp in business_logic_comparison.get("comparisons", []):
                if comp.get("status") != "match":
                    business_logic_issues.append(comp)

        # Calculate scores
        conversion_score = conversion_result.get("conversion_score", 0.0) if conversion_result else 0.0
        complexity_score = self._calculate_complexity(source_sql)

        config = SPChecklistConfig(
            procedure_name=procedure_name,
            source_line_count=source_lines,
            target_line_count=target_lines,
            patterns_found=patterns_found,
            patterns_converted=patterns_converted,
            dependencies=dependencies,
            business_logic_issues=business_logic_issues,
            conversion_score=conversion_score,
            complexity_score=complexity_score,
        )

        return self.generate_checklist(config)

    def _generate_header(self, config: SPChecklistConfig) -> str:
        """Generate checklist header."""
        status = self._get_conversion_status(config.conversion_score)
        complexity = self._get_complexity_label(config.complexity_score)

        return f"""## Stored Procedure Review: {config.procedure_name}

**Source:** `{config.source_path or 'N/A'}`
**Target:** `{config.target_path or 'N/A'}`
**Complexity Score:** {config.complexity_score}/10 ({complexity})
**Conversion Score:** {config.conversion_score:.0%}
**Conversion Status:** {status}

**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

---
"""

    def _generate_source_analysis(self, config: SPChecklistConfig) -> str:
        """Generate source analysis section."""
        patterns_count = len(config.patterns_found) + len(config.patterns_converted)
        tables_count = len([d for d in config.dependencies if d.get("type") in ["reads", "TABLE_READ"]])
        procs_count = len([d for d in config.dependencies if d.get("type") in ["calls", "PROCEDURE_CALL"]])
        funcs_count = len([d for d in config.dependencies if d.get("type") in ["uses_function", "FUNCTION_CALL"]])

        return f"""### 1. Source Analysis

| Metric | Value |
|--------|-------|
| Lines of Code | {config.source_line_count} |
| Patterns Detected | {patterns_count} |
| Tables Referenced | {tables_count} |
| Procedures Called | {procs_count} |
| Functions Used | {funcs_count} |

---
"""

    def _generate_pattern_conversion_status(self, config: SPChecklistConfig) -> str:
        """Generate pattern conversion status section."""
        lines = [
            "### 2. Pattern Conversion Status",
            "",
            "| Pattern | Found | Converted | Status |",
            "|---------|-------|-----------|--------|",
        ]

        # Combine all patterns
        all_patterns = {}
        for p in config.patterns_found:
            name = p.get("name", "Unknown")
            all_patterns[name] = {"found": True, "converted": False}

        for p in config.patterns_converted:
            name = p.get("name", "Unknown")
            if name in all_patterns:
                all_patterns[name]["converted"] = True
            else:
                all_patterns[name] = {"found": True, "converted": True}

        # Standard patterns to check
        standard_patterns = ["CURSOR", "TEMP_TABLE", "MERGE", "SPATIAL", "DYNAMIC_SQL", "WHILE_LOOP"]

        for pattern in standard_patterns:
            if pattern in all_patterns:
                found = "Yes"
                converted = "Yes" if all_patterns[pattern]["converted"] else "No"
                status = "✅" if all_patterns[pattern]["converted"] else "❌ PENDING"
            else:
                found = "No"
                converted = "N/A"
                status = "➖"

            lines.append(f"| {pattern} | {found} | {converted} | {status} |")

        # Add any additional patterns
        for pattern, info in all_patterns.items():
            if pattern not in standard_patterns:
                found = "Yes"
                converted = "Yes" if info["converted"] else "No"
                status = "✅" if info["converted"] else "❌ PENDING"
                lines.append(f"| {pattern} | {found} | {converted} | {status} |")

        lines.append("")
        lines.append("---")
        lines.append("")

        return "\n".join(lines)

    def _generate_business_logic_validation(self, config: SPChecklistConfig) -> str:
        """Generate business logic validation section."""
        lines = [
            "### 3. Business Logic Validation",
            "",
        ]

        # Input tables
        input_tables = [d for d in config.dependencies if d.get("type") in ["reads", "TABLE_READ"]]
        if input_tables:
            lines.append("#### Input Tables")
            for t in input_tables:
                target = t.get("target", t.get("target_object", "Unknown"))
                databricks = t.get("databricks_path", t.get("databricks_equivalent", "Not mapped"))
                resolved = t.get("resolved", t.get("is_resolved", False))
                checkbox = "[x]" if resolved else "[ ]"
                lines.append(f"- {checkbox} `{target}` → `{databricks}`")
            lines.append("")

        # Output tables
        output_tables = [d for d in config.dependencies if d.get("type") in ["writes", "TABLE_WRITE"]]
        if output_tables:
            lines.append("#### Output Tables")
            for t in output_tables:
                target = t.get("target", t.get("target_object", "Unknown"))
                databricks = t.get("databricks_path", t.get("databricks_equivalent", "Not mapped"))
                resolved = t.get("resolved", t.get("is_resolved", False))
                checkbox = "[x]" if resolved else "[ ]"
                lines.append(f"- {checkbox} `{target}` → `{databricks}` - Schema matches")
            lines.append("")

        # Key transformations
        if config.business_logic_issues:
            lines.append("#### Issues Found")
            for issue in config.business_logic_issues[:10]:  # Limit to 10
                issue_type = issue.get("type", "Unknown")
                source = issue.get("source", "N/A")
                risk = issue.get("risk", "MEDIUM")
                message = issue.get("message", "")
                lines.append(f"- [ ] **{risk}**: {issue_type} - {source}")
                if message:
                    lines.append(f"  - {message}")
            lines.append("")
        else:
            lines.append("#### Transformations")
            lines.append("- [x] All transformations verified")
            lines.append("")

        lines.append("---")
        lines.append("")

        return "\n".join(lines)

    def _generate_dependency_resolution(self, config: SPChecklistConfig) -> str:
        """Generate dependency resolution section."""
        lines = [
            "### 4. Dependency Resolution",
            "",
        ]

        resolved = [d for d in config.dependencies if d.get("resolved", d.get("is_resolved", False))]
        unresolved = [d for d in config.dependencies if not d.get("resolved", d.get("is_resolved", False))]

        # Resolved
        if resolved:
            lines.append("#### Resolved")
            for d in resolved:
                target = d.get("target", d.get("target_object", "Unknown"))
                databricks = d.get("databricks_path", d.get("databricks_equivalent", ""))
                lines.append(f"- [x] `{target}` → `{databricks}`")
            lines.append("")

        # Unresolved (BLOCKERS)
        if unresolved:
            lines.append("#### Unresolved (BLOCKERS)")
            for d in unresolved:
                target = d.get("target", d.get("target_object", "Unknown"))
                dep_type = d.get("type", d.get("dependency_type", "Unknown"))
                lines.append(f"- [ ] `{target}` ({dep_type}) - **ACTION REQUIRED**")
            lines.append("")
        else:
            lines.append("#### Status")
            lines.append("- [x] All dependencies resolved")
            lines.append("")

        lines.append("---")
        lines.append("")

        return "\n".join(lines)

    def _generate_data_quality_rules(self, config: SPChecklistConfig) -> str:
        """Generate data quality rules section."""
        # Extract key columns from dependencies
        output_tables = [d for d in config.dependencies if d.get("type") in ["writes", "TABLE_WRITE"]]

        lines = [
            "### 5. Data Quality Rules",
            "",
            "- [ ] Primary key columns are NOT NULL",
            "- [ ] Foreign key references are valid",
            "- [ ] Required fields are present",
            "- [ ] Data types match source schema",
            "- [ ] No orphaned records",
            "- [ ] Date ranges are valid (no future dates unless expected)",
            "",
        ]

        if output_tables:
            lines.append("#### Output Table Checks")
            for t in output_tables:
                target = t.get("target", t.get("target_object", "Unknown"))
                lines.append(f"- [ ] `{target}` - Schema validated")
                lines.append(f"- [ ] `{target}` - Constraints enforced")
            lines.append("")

        lines.append("---")
        lines.append("")

        return "\n".join(lines)

    def _generate_performance_considerations(self, config: SPChecklistConfig) -> str:
        """Generate performance considerations section."""
        lines = [
            "### 6. Performance Considerations",
            "",
        ]

        # Check for patterns that need performance attention
        has_cursor = any(p.get("name") == "CURSOR" for p in config.patterns_found + config.patterns_converted)
        has_temp = any(p.get("name") == "TEMP_TABLE" for p in config.patterns_found + config.patterns_converted)
        has_spatial = any(p.get("name") == "SPATIAL" for p in config.patterns_found + config.patterns_converted)

        if has_cursor:
            lines.append("- [ ] Window functions have appropriate partition columns")
            lines.append("- [ ] Large window operations use cluster-aware partitioning")

        if has_temp:
            lines.append("- [ ] Temp views are cached if reused multiple times")
            lines.append("- [ ] Temp views are unpersisted after use")

        if has_spatial:
            lines.append("- [ ] Spatial operations use H3 indexing for efficiency")
            lines.append("- [ ] Large polygon operations are optimized")

        # General performance checks
        lines.extend([
            "- [ ] Large tables (>1M rows) use appropriate partitioning",
            "- [ ] Joins use broadcast hints for small dimension tables",
            "- [ ] Shuffle operations are minimized",
            "- [ ] Checkpointing is used for long lineage chains",
            "",
            "---",
            "",
        ])

        return "\n".join(lines)

    def _generate_testing_requirements(self, config: SPChecklistConfig) -> str:
        """Generate testing requirements section."""
        return """### 7. Testing Requirements

- [ ] Validation notebook created
- [ ] Row count comparison: Source vs Target
- [ ] Sample record comparison (100+ random rows)
- [ ] Aggregation comparison (SUM, COUNT, AVG)
- [ ] Edge case testing:
  - [ ] NULL handling
  - [ ] Empty result sets
  - [ ] Boundary values
  - [ ] Special characters
- [ ] Performance testing:
  - [ ] Execution time acceptable
  - [ ] Resource utilization reasonable
- [ ] Incremental load testing (if applicable)

---
"""

    def _generate_review_verdict(self, config: SPChecklistConfig) -> str:
        """Generate review verdict section."""
        # Determine status
        unresolved = [d for d in config.dependencies if not d.get("resolved", d.get("is_resolved", False))]
        unconverted = [p for p in config.patterns_found if not p.get("converted", False)]

        blockers = []
        if unresolved:
            blockers.append(f"{len(unresolved)} unresolved dependencies")
        if unconverted:
            blockers.append(f"{len(unconverted)} unconverted patterns")
        if config.conversion_score < 0.8:
            blockers.append(f"Conversion score below 80% ({config.conversion_score:.0%})")

        if blockers:
            status = "❌ FAIL"
            status_class = "FAIL"
        elif config.business_logic_issues:
            status = "⚠️ NEEDS_WORK"
            status_class = "NEEDS_WORK"
        else:
            status = "✅ PASS"
            status_class = "PASS"

        lines = [
            "### Review Verdict",
            "",
            f"**Status:** {status}",
            "",
        ]

        if blockers:
            lines.append("**Blockers:**")
            for b in blockers:
                lines.append(f"- {b}")
            lines.append("")

        # Recommendations
        lines.append("**Recommendations:**")
        if status_class == "FAIL":
            lines.append("- Resolve all blocking dependencies before proceeding")
            lines.append("- Complete pattern conversion for all detected patterns")
            lines.append("- Re-run review after fixes are applied")
        elif status_class == "NEEDS_WORK":
            lines.append("- Review and address business logic issues")
            lines.append("- Verify transformations with domain expert")
            lines.append("- Complete all testing requirements")
        else:
            lines.append("- Deploy to staging environment for integration testing")
            lines.append("- Monitor first production run closely")
            lines.append("- Document any manual steps required")

        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append(f"*Review generated by Lakebridge Review Skill - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

        return "\n".join(lines)

    def _calculate_complexity(self, sql: str) -> int:
        """Calculate complexity score (1-10) based on SQL patterns."""
        import re

        score = 1

        # Line count
        lines = len(sql.split("\n"))
        if lines > 500:
            score += 3
        elif lines > 200:
            score += 2
        elif lines > 100:
            score += 1

        # Pattern complexity
        patterns = [
            (r"DECLARE\s+\w+\s+CURSOR", 2),  # CURSOR
            (r"WHILE\s+", 1),                 # WHILE loop
            (r"MERGE\s+", 1),                 # MERGE
            (r"geography::|STDistance", 2),   # Spatial
            (r"EXEC\s*\(|sp_executesql", 2),  # Dynamic SQL
            (r"WITH\s+\w+\s+AS", 1),          # CTE
            (r"BEGIN\s+TRY", 1),              # Error handling
            (r"##?\w+", 1),                   # Temp tables
        ]

        for pattern, weight in patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                score += weight

        return min(score, 10)

    def _get_conversion_status(self, score: float) -> str:
        """Get status label from conversion score."""
        if score >= 0.95:
            return "COMPLETE"
        elif score >= 0.8:
            return "MOSTLY COMPLETE"
        elif score >= 0.5:
            return "PARTIAL"
        else:
            return "INCOMPLETE"

    def _get_complexity_label(self, score: int) -> str:
        """Get complexity label from score."""
        if score <= 3:
            return "Low"
        elif score <= 6:
            return "Medium"
        else:
            return "High"


class QuickChecklist:
    """
    Generate quick review checklists for common scenarios.
    """

    @staticmethod
    def for_cursor_conversion(procedure_name: str) -> str:
        """Generate checklist for CURSOR conversion."""
        return f"""## Quick Checklist: CURSOR Conversion - {procedure_name}

### Window Function Conversion
- [ ] Identified cursor purpose (running total, gap detection, etc.)
- [ ] Partition columns correctly identified
- [ ] Order columns correctly identified
- [ ] Window frame specified (ROWS BETWEEN, RANGE BETWEEN)

### Data Verification
- [ ] Output matches original cursor results
- [ ] Edge cases handled (NULLs, empty partitions)
- [ ] Performance acceptable for large datasets

### Common Issues
- [ ] No DISTINCT on window partitions (causes incorrect results)
- [ ] Correct use of lag/lead offset
- [ ] unboundedPreceding vs currentRow boundaries correct
"""

    @staticmethod
    def for_merge_conversion(procedure_name: str) -> str:
        """Generate checklist for MERGE conversion."""
        return f"""## Quick Checklist: MERGE Conversion - {procedure_name}

### Delta MERGE Setup
- [ ] Target table exists and is Delta format
- [ ] Merge condition correctly translated
- [ ] All WHEN clauses converted

### WHEN Clause Verification
- [ ] WHEN MATCHED UPDATE → .whenMatchedUpdate()
- [ ] WHEN NOT MATCHED INSERT → .whenNotMatchedInsert()
- [ ] WHEN NOT MATCHED BY SOURCE → .whenNotMatchedBySourceDelete()
- [ ] Additional conditions translated correctly

### Data Verification
- [ ] Row counts match expected after merge
- [ ] Updates applied correctly
- [ ] Inserts have all required columns
- [ ] Deletes (if any) are correct
"""

    @staticmethod
    def for_spatial_conversion(procedure_name: str) -> str:
        """Generate checklist for Spatial conversion."""
        return f"""## Quick Checklist: Spatial Conversion - {procedure_name}

### Spatial Library Setup
- [ ] H3 library installed on cluster
- [ ] Haversine UDF registered
- [ ] Coordinate system correct (WGS84/EPSG:4326)

### Function Mapping
- [ ] geography::Point → (lat, lon) columns
- [ ] STDistance → Haversine UDF
- [ ] STContains → H3 containment or Shapely
- [ ] STIntersects → H3 ring intersection

### Data Verification
- [ ] Distance calculations within acceptable tolerance
- [ ] Point-in-polygon tests return correct results
- [ ] Performance acceptable for dataset size
"""

    @staticmethod
    def for_temp_table_conversion(procedure_name: str) -> str:
        """Generate checklist for Temp Table conversion."""
        return f"""## Quick Checklist: Temp Table Conversion - {procedure_name}

### Temp View Setup
- [ ] All #temp tables identified
- [ ] SELECT INTO converted to DataFrame + createOrReplaceTempView
- [ ] CREATE TABLE #temp converted appropriately

### Caching Strategy
- [ ] Frequently-used temp tables cached
- [ ] Cache unpersisted after use
- [ ] Global temp tables (##) handled specially

### Data Verification
- [ ] Temp view contents match original temp table
- [ ] All references to temp table updated
- [ ] Cleanup code added at end of notebook
"""
