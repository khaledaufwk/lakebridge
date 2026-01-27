"""
Dependency Tracker for SP Migration Review.

REQ-R4: Track and validate dependencies between stored procedures,
tables, views, and functions.
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum
from collections import defaultdict


class DependencyType(Enum):
    """Types of dependencies."""
    TABLE_READ = "reads"
    TABLE_WRITE = "writes"
    PROCEDURE_CALL = "calls"
    FUNCTION_CALL = "uses_function"
    VIEW_REFERENCE = "references_view"


@dataclass
class Dependency:
    """A single dependency reference."""
    source_object: str          # e.g., "stg.spDeltaSyncFactWorkersHistory"
    target_object: str          # e.g., "dbo.Worker"
    dependency_type: DependencyType
    is_resolved: bool = False   # True if target exists in Databricks
    databricks_equivalent: Optional[str] = None  # e.g., "wakecap_prod.silver.silver_worker"
    line_number: Optional[int] = None
    context: Optional[str] = None


@dataclass
class DependencyGraph:
    """Graph of dependencies between objects."""
    nodes: Set[str] = field(default_factory=set)
    edges: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))

    def add_edge(self, source: str, target: str):
        """Add a dependency edge."""
        self.nodes.add(source)
        self.nodes.add(target)
        if target not in self.edges[source]:
            self.edges[source].append(target)

    def get_dependencies(self, node: str) -> List[str]:
        """Get all dependencies for a node."""
        return self.edges.get(node, [])

    def get_dependents(self, node: str) -> List[str]:
        """Get all nodes that depend on this node."""
        return [n for n, deps in self.edges.items() if node in deps]


class DependencyTracker:
    """
    Track dependencies for stored procedure migrations.

    Capabilities:
    - Extract table reads/writes
    - Identify procedure calls
    - Detect function usage
    - Track view references
    - Generate dependency reports
    - Determine conversion order
    """

    # Patterns for dependency extraction
    PATTERNS = {
        # Table reads: FROM and JOIN
        "table_from": re.compile(
            r"FROM\s+(\[?[\w]+\]?\.)?\[?([\w]+)\]?",
            re.IGNORECASE
        ),
        "table_join": re.compile(
            r"JOIN\s+(\[?[\w]+\]?\.)?\[?([\w]+)\]?",
            re.IGNORECASE
        ),
        # Table writes
        "insert_into": re.compile(
            r"INSERT\s+INTO\s+(\[?[\w]+\]?\.)?\[?([\w]+)\]?",
            re.IGNORECASE
        ),
        "update_table": re.compile(
            r"UPDATE\s+(\[?[\w]+\]?\.)?\[?([\w]+)\]?",
            re.IGNORECASE
        ),
        "delete_from": re.compile(
            r"DELETE\s+(?:FROM\s+)?(\[?[\w]+\]?\.)?\[?([\w]+)\]?",
            re.IGNORECASE
        ),
        "merge_into": re.compile(
            r"MERGE\s+(?:INTO\s+)?(\[?[\w]+\]?\.)?\[?([\w]+)\]?",
            re.IGNORECASE
        ),
        "select_into": re.compile(
            r"INTO\s+(\[?[\w]+\]?\.)?\[?([\w]+)\]?\s+FROM",
            re.IGNORECASE
        ),
        # Procedure calls
        "exec_proc": re.compile(
            r"(?:EXEC(?:UTE)?)\s+(\[?[\w]+\]?\.)?\[?(sp[\w]+|pr[\w]+)\]?",
            re.IGNORECASE
        ),
        # Function calls
        "function_call": re.compile(
            r"(\[?[\w]+\]?)\.(fn[\w]+)\s*\(",
            re.IGNORECASE
        ),
        # View references (detected same as tables, but filtered by naming convention)
        "view_reference": re.compile(
            r"FROM\s+(\[?[\w]+\]?\.)?\[?(vw[\w]+|v_[\w]+)\]?",
            re.IGNORECASE
        ),
    }

    # Default table mapping patterns
    DEFAULT_TABLE_MAPPING = {
        # Schema mappings
        "dbo": "silver",
        "stg": "bronze",
        "dim": "gold",
        "fact": "gold",
    }

    def __init__(
        self,
        catalog: str = "wakecap_prod",
        schemas: Optional[List[str]] = None,
        table_mapping: Optional[Dict[str, str]] = None
    ):
        """
        Initialize dependency tracker.

        Args:
            catalog: Target Databricks catalog
            schemas: Schemas to check for resolution
            table_mapping: Custom source -> Databricks table mapping
        """
        self.catalog = catalog
        self.schemas = schemas or ["bronze", "silver", "gold", "migration"]
        self.table_mapping = table_mapping or {}
        self.dependency_graph = DependencyGraph()

    def extract_dependencies(
        self,
        source_sql: str,
        object_name: str
    ) -> List[Dependency]:
        """
        Extract all dependencies from source SQL.

        Args:
            source_sql: T-SQL source code
            object_name: Name of the source object (e.g., "stg.spCalculateFactWorkersShifts")

        Returns:
            List of Dependency objects
        """
        dependencies = []
        lines = source_sql.split("\n")

        # Track line numbers
        line_lookup = {}
        for i, line in enumerate(lines, 1):
            line_lookup[i] = line

        # Remove comments for cleaner parsing
        clean_sql = self._remove_comments(source_sql)

        # Extract table reads
        dependencies.extend(
            self._extract_table_reads(clean_sql, object_name, source_sql)
        )

        # Extract table writes
        dependencies.extend(
            self._extract_table_writes(clean_sql, object_name, source_sql)
        )

        # Extract procedure calls
        dependencies.extend(
            self._extract_procedure_calls(clean_sql, object_name, source_sql)
        )

        # Extract function calls
        dependencies.extend(
            self._extract_function_calls(clean_sql, object_name, source_sql)
        )

        # Extract view references
        dependencies.extend(
            self._extract_view_references(clean_sql, object_name, source_sql)
        )

        # Update dependency graph
        for dep in dependencies:
            self.dependency_graph.add_edge(object_name, dep.target_object)

        return self._dedupe_dependencies(dependencies)

    def resolve_dependencies(
        self,
        dependencies: List[Dependency],
        existing_tables: Optional[Set[str]] = None
    ) -> List[Dependency]:
        """
        Check if dependencies exist in Databricks.

        Args:
            dependencies: List of dependencies to resolve
            existing_tables: Optional set of existing Databricks tables

        Returns:
            Updated dependencies with is_resolved and databricks_equivalent
        """
        existing_tables = existing_tables or set()

        for dep in dependencies:
            # Try to find Databricks equivalent
            databricks_name = self._map_to_databricks(dep.target_object)
            dep.databricks_equivalent = databricks_name

            # Check if it exists
            if databricks_name and databricks_name.lower() in {t.lower() for t in existing_tables}:
                dep.is_resolved = True
            elif databricks_name in self.table_mapping.values():
                dep.is_resolved = True
            else:
                dep.is_resolved = False

        return dependencies

    def generate_dependency_report(
        self,
        procedure_name: str,
        dependencies: List[Dependency]
    ) -> str:
        """
        Generate markdown report of dependencies.

        Args:
            procedure_name: Name of the procedure
            dependencies: List of dependencies

        Returns:
            Markdown formatted report
        """
        lines = [
            f"## Dependency Report: {procedure_name}",
            "",
            f"**Total Dependencies:** {len(dependencies)}",
            "",
        ]

        # Group by type
        by_type = defaultdict(list)
        for dep in dependencies:
            by_type[dep.dependency_type].append(dep)

        # Resolved vs Unresolved
        resolved = [d for d in dependencies if d.is_resolved]
        unresolved = [d for d in dependencies if not d.is_resolved]

        # Summary
        lines.extend([
            "### Summary",
            "",
            f"- Resolved: {len(resolved)}",
            f"- Unresolved: {len(unresolved)} {'(BLOCKERS)' if unresolved else ''}",
            "",
        ])

        # Resolved dependencies
        if resolved:
            lines.extend([
                "### Resolved Dependencies",
                "",
                "| Source Object | Type | Databricks Equivalent |",
                "|--------------|------|----------------------|",
            ])
            for dep in resolved:
                lines.append(
                    f"| {dep.target_object} | {dep.dependency_type.value} | {dep.databricks_equivalent or 'N/A'} |"
                )
            lines.append("")

        # Unresolved dependencies (BLOCKERS)
        if unresolved:
            lines.extend([
                "### Unresolved Dependencies (BLOCKERS)",
                "",
                "| Source Object | Type | Expected Databricks Path | Action Required |",
                "|--------------|------|-------------------------|-----------------|",
            ])
            for dep in unresolved:
                action = self._get_resolution_action(dep)
                lines.append(
                    f"| {dep.target_object} | {dep.dependency_type.value} | {dep.databricks_equivalent or 'Unknown'} | {action} |"
                )
            lines.append("")

        # By type breakdown
        lines.extend([
            "### Dependencies by Type",
            "",
        ])

        for dep_type, deps in by_type.items():
            resolved_count = len([d for d in deps if d.is_resolved])
            lines.append(f"- **{dep_type.value}:** {len(deps)} ({resolved_count} resolved)")

        # Procedure calls (special handling)
        proc_calls = by_type.get(DependencyType.PROCEDURE_CALL, [])
        if proc_calls:
            lines.extend([
                "",
                "### Procedure Call Chain",
                "",
                "These procedures must be converted before this one:",
                "",
            ])
            for dep in proc_calls:
                status = "CONVERTED" if dep.is_resolved else "PENDING"
                lines.append(f"- `{dep.target_object}` - {status}")

        # Function dependencies
        func_calls = by_type.get(DependencyType.FUNCTION_CALL, [])
        if func_calls:
            lines.extend([
                "",
                "### Function Dependencies",
                "",
                "These UDFs must be registered before running:",
                "",
            ])
            for dep in func_calls:
                status = "REGISTERED" if dep.is_resolved else "MISSING - Create UDF"
                lines.append(f"- `{dep.target_object}` - {status}")

        return "\n".join(lines)

    def get_conversion_order(
        self,
        procedures: List[str],
        all_dependencies: Optional[Dict[str, List[Dependency]]] = None
    ) -> List[str]:
        """
        Determine optimal conversion order based on dependencies.

        Uses topological sort to ensure dependencies are converted first.

        Args:
            procedures: List of procedure names to order
            all_dependencies: Optional pre-computed dependencies per procedure

        Returns:
            Procedures sorted so dependencies are converted first
        """
        # Build graph from all dependencies
        if all_dependencies:
            for proc, deps in all_dependencies.items():
                for dep in deps:
                    if dep.dependency_type == DependencyType.PROCEDURE_CALL:
                        self.dependency_graph.add_edge(proc, dep.target_object)

        # Topological sort
        return self._topological_sort(procedures)

    def get_missing_dependencies(
        self,
        dependencies: List[Dependency]
    ) -> Dict[str, List[Dependency]]:
        """
        Get missing dependencies grouped by type.

        Args:
            dependencies: List of all dependencies

        Returns:
            Dict with keys 'tables', 'procedures', 'functions', 'views'
        """
        missing = {
            "tables": [],
            "procedures": [],
            "functions": [],
            "views": [],
        }

        for dep in dependencies:
            if not dep.is_resolved:
                if dep.dependency_type in [DependencyType.TABLE_READ, DependencyType.TABLE_WRITE]:
                    missing["tables"].append(dep)
                elif dep.dependency_type == DependencyType.PROCEDURE_CALL:
                    missing["procedures"].append(dep)
                elif dep.dependency_type == DependencyType.FUNCTION_CALL:
                    missing["functions"].append(dep)
                elif dep.dependency_type == DependencyType.VIEW_REFERENCE:
                    missing["views"].append(dep)

        return missing

    # ==================== Extraction Helpers ====================

    def _remove_comments(self, sql: str) -> str:
        """Remove SQL comments."""
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql

    def _extract_table_reads(
        self,
        sql: str,
        object_name: str,
        original_sql: str
    ) -> List[Dependency]:
        """Extract table read dependencies."""
        dependencies = []

        # FROM clauses
        for match in self.PATTERNS["table_from"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")

            # Skip temp tables and CTEs
            if table.startswith("#") or table.startswith("@"):
                continue

            # Skip if it's a view (handled separately)
            if table.lower().startswith("vw") or table.lower().startswith("v_"):
                continue

            line_num = self._find_line_number(original_sql, match.group(0))

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{table}",
                dependency_type=DependencyType.TABLE_READ,
                line_number=line_num,
                context=match.group(0)
            ))

        # JOIN clauses
        for match in self.PATTERNS["table_join"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")

            if table.startswith("#") or table.startswith("@"):
                continue

            if table.lower().startswith("vw") or table.lower().startswith("v_"):
                continue

            line_num = self._find_line_number(original_sql, match.group(0))

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{table}",
                dependency_type=DependencyType.TABLE_READ,
                line_number=line_num,
                context=match.group(0)
            ))

        return dependencies

    def _extract_table_writes(
        self,
        sql: str,
        object_name: str,
        original_sql: str
    ) -> List[Dependency]:
        """Extract table write dependencies."""
        dependencies = []

        # INSERT INTO
        for match in self.PATTERNS["insert_into"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")

            if table.startswith("#"):
                continue

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{table}",
                dependency_type=DependencyType.TABLE_WRITE,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        # UPDATE
        for match in self.PATTERNS["update_table"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")

            if table.startswith("#"):
                continue

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{table}",
                dependency_type=DependencyType.TABLE_WRITE,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        # DELETE
        for match in self.PATTERNS["delete_from"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")

            if table.startswith("#"):
                continue

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{table}",
                dependency_type=DependencyType.TABLE_WRITE,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        # MERGE INTO
        for match in self.PATTERNS["merge_into"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")

            if table.startswith("#"):
                continue

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{table}",
                dependency_type=DependencyType.TABLE_WRITE,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        # SELECT INTO
        for match in self.PATTERNS["select_into"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            table = match.group(2).strip("[]")

            if table.startswith("#"):
                continue

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{table}",
                dependency_type=DependencyType.TABLE_WRITE,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        return dependencies

    def _extract_procedure_calls(
        self,
        sql: str,
        object_name: str,
        original_sql: str
    ) -> List[Dependency]:
        """Extract procedure call dependencies."""
        dependencies = []

        for match in self.PATTERNS["exec_proc"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            proc = match.group(2).strip("[]")

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{proc}",
                dependency_type=DependencyType.PROCEDURE_CALL,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        return dependencies

    def _extract_function_calls(
        self,
        sql: str,
        object_name: str,
        original_sql: str
    ) -> List[Dependency]:
        """Extract function call dependencies."""
        dependencies = []

        for match in self.PATTERNS["function_call"].finditer(sql):
            schema = match.group(1).strip("[].")
            func = match.group(2).strip("[]")

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{func}",
                dependency_type=DependencyType.FUNCTION_CALL,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        return dependencies

    def _extract_view_references(
        self,
        sql: str,
        object_name: str,
        original_sql: str
    ) -> List[Dependency]:
        """Extract view reference dependencies."""
        dependencies = []

        for match in self.PATTERNS["view_reference"].finditer(sql):
            schema = (match.group(1) or "dbo").strip("[].")
            view = match.group(2).strip("[]")

            dependencies.append(Dependency(
                source_object=object_name,
                target_object=f"{schema}.{view}",
                dependency_type=DependencyType.VIEW_REFERENCE,
                line_number=self._find_line_number(original_sql, match.group(0)),
                context=match.group(0)
            ))

        return dependencies

    # ==================== Resolution Helpers ====================

    def _map_to_databricks(self, source_object: str) -> Optional[str]:
        """Map source object to Databricks equivalent."""
        # Check explicit mapping first
        if source_object.lower() in {k.lower(): k for k in self.table_mapping}:
            for k, v in self.table_mapping.items():
                if k.lower() == source_object.lower():
                    return v

        # Try default mapping
        parts = source_object.split(".")
        if len(parts) == 2:
            schema, table = parts

            # Map schema
            target_schema = self.DEFAULT_TABLE_MAPPING.get(schema.lower(), "silver")

            # Convert table name (e.g., Worker -> silver_worker)
            table_lower = table.lower()

            # Handle common patterns
            if table_lower.startswith("fact"):
                target_schema = "gold"
                target_table = f"gold_{table_lower}"
            elif table_lower.startswith("dim"):
                target_schema = "gold"
                target_table = f"gold_{table_lower}"
            else:
                target_table = f"{target_schema}_{table_lower}"

            return f"{self.catalog}.{target_schema}.{target_table}"

        return None

    def _get_resolution_action(self, dep: Dependency) -> str:
        """Get recommended action for unresolved dependency."""
        if dep.dependency_type == DependencyType.TABLE_READ:
            return "Migrate source table or create reference table"
        elif dep.dependency_type == DependencyType.TABLE_WRITE:
            return "Create target table schema"
        elif dep.dependency_type == DependencyType.PROCEDURE_CALL:
            return "Convert called procedure first"
        elif dep.dependency_type == DependencyType.FUNCTION_CALL:
            return "Create equivalent UDF"
        elif dep.dependency_type == DependencyType.VIEW_REFERENCE:
            return "Create view or inline SQL"
        return "Manual review required"

    def _find_line_number(self, sql: str, pattern: str) -> Optional[int]:
        """Find line number where pattern occurs."""
        lines = sql.split("\n")
        pattern_lower = pattern.lower()
        for i, line in enumerate(lines, 1):
            if pattern_lower in line.lower():
                return i
        return None

    def _dedupe_dependencies(self, dependencies: List[Dependency]) -> List[Dependency]:
        """Remove duplicate dependencies."""
        seen = set()
        result = []
        for dep in dependencies:
            key = (dep.target_object.lower(), dep.dependency_type)
            if key not in seen:
                seen.add(key)
                result.append(dep)
        return result

    def _topological_sort(self, procedures: List[str]) -> List[str]:
        """Topological sort of procedures based on dependencies."""
        # Build in-degree map
        in_degree = defaultdict(int)
        for proc in procedures:
            in_degree[proc] = 0

        for proc in procedures:
            for dep in self.dependency_graph.get_dependencies(proc):
                if dep in procedures:
                    in_degree[proc] += 1

        # Queue of nodes with no dependencies
        queue = [p for p in procedures if in_degree[p] == 0]
        result = []

        while queue:
            # Sort queue for deterministic order
            queue.sort()
            node = queue.pop(0)
            result.append(node)

            # Reduce in-degree of dependent nodes
            for dep in self.dependency_graph.get_dependents(node):
                if dep in in_degree:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        queue.append(dep)

        # Add any remaining (circular dependencies)
        for proc in procedures:
            if proc not in result:
                result.append(proc)

        return result


class DependencyAnalyzer:
    """
    High-level analyzer for procedure dependencies.

    Combines DependencyTracker with additional analysis capabilities.
    """

    def __init__(
        self,
        catalog: str = "wakecap_prod",
        table_mapping: Optional[Dict[str, str]] = None
    ):
        """
        Initialize analyzer.

        Args:
            catalog: Target Databricks catalog
            table_mapping: Source -> Databricks table mapping
        """
        self.tracker = DependencyTracker(
            catalog=catalog,
            table_mapping=table_mapping
        )

    def analyze(
        self,
        source_sql: str,
        procedure_name: str,
        existing_tables: Optional[Set[str]] = None
    ) -> Dict:
        """
        Analyze dependencies for a stored procedure.

        Args:
            source_sql: T-SQL source code
            procedure_name: Name of the procedure
            existing_tables: Set of existing Databricks tables

        Returns:
            Analysis result dictionary
        """
        # Extract dependencies
        dependencies = self.tracker.extract_dependencies(source_sql, procedure_name)

        # Resolve dependencies
        resolved_deps = self.tracker.resolve_dependencies(
            dependencies,
            existing_tables
        )

        # Get missing grouped by type
        missing = self.tracker.get_missing_dependencies(resolved_deps)

        # Calculate readiness
        total = len(resolved_deps)
        resolved_count = len([d for d in resolved_deps if d.is_resolved])
        readiness = (resolved_count / total * 100) if total > 0 else 100

        return {
            "procedure_name": procedure_name,
            "total_dependencies": total,
            "resolved_count": resolved_count,
            "unresolved_count": total - resolved_count,
            "readiness_percentage": readiness,
            "dependencies": [
                {
                    "target": d.target_object,
                    "type": d.dependency_type.value,
                    "resolved": d.is_resolved,
                    "databricks_path": d.databricks_equivalent,
                    "line": d.line_number,
                }
                for d in resolved_deps
            ],
            "missing": {
                "tables": [d.target_object for d in missing["tables"]],
                "procedures": [d.target_object for d in missing["procedures"]],
                "functions": [d.target_object for d in missing["functions"]],
                "views": [d.target_object for d in missing["views"]],
            },
            "can_convert": readiness == 100,
            "blockers": [
                f"{d.target_object} ({d.dependency_type.value})"
                for d in resolved_deps
                if not d.is_resolved
            ],
        }

    def analyze_batch(
        self,
        procedures: Dict[str, str],
        existing_tables: Optional[Set[str]] = None
    ) -> Dict:
        """
        Analyze dependencies for multiple procedures.

        Args:
            procedures: Dict of procedure_name -> source_sql
            existing_tables: Set of existing Databricks tables

        Returns:
            Batch analysis result
        """
        results = {}
        all_dependencies = {}

        for name, sql in procedures.items():
            results[name] = self.analyze(sql, name, existing_tables)
            all_dependencies[name] = self.tracker.extract_dependencies(sql, name)

        # Get conversion order
        conversion_order = self.tracker.get_conversion_order(
            list(procedures.keys()),
            all_dependencies
        )

        return {
            "procedures": results,
            "conversion_order": conversion_order,
            "ready_to_convert": [
                name for name, result in results.items()
                if result["can_convert"]
            ],
            "blocked": [
                name for name, result in results.items()
                if not result["can_convert"]
            ],
        }
