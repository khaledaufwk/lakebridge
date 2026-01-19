"""
Plan generation utilities for Lakebridge skills.

Provides templates and helpers for creating implementation plans.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum
from datetime import datetime
from pathlib import Path
import re


class TaskType(Enum):
    """Types of tasks that can be planned."""
    CHORE = "chore"
    FEATURE = "feature"
    REFACTOR = "refactor"
    FIX = "fix"
    ENHANCEMENT = "enhancement"
    MIGRATION = "migration"


class Complexity(Enum):
    """Task complexity levels."""
    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"


@dataclass
class PlanSection:
    """A section in the implementation plan."""
    title: str
    content: str
    required: bool = True


@dataclass
class PlanTemplate:
    """Template for generating implementation plans."""
    task_name: str
    task_type: TaskType
    complexity: Complexity
    description: str
    objective: str
    problem_statement: Optional[str] = None
    solution_approach: Optional[str] = None
    relevant_files: List[str] = field(default_factory=list)
    new_files: List[str] = field(default_factory=list)
    phases: List[Dict[str, str]] = field(default_factory=list)
    tasks: List[Dict[str, List[str]]] = field(default_factory=list)
    acceptance_criteria: List[str] = field(default_factory=list)
    validation_commands: List[Dict[str, str]] = field(default_factory=list)
    testing_strategy: Optional[str] = None
    notes: Optional[str] = None

    # Migration-specific fields
    source_database: Optional[str] = None
    source_server: Optional[str] = None
    object_counts: Optional[Dict[str, int]] = None
    complexity_indicators: Optional[List[str]] = None
    target_catalog: Optional[str] = None
    target_schema: Optional[str] = None


class PlanGenerator:
    """
    Generates implementation plans from templates.

    Usage:
        generator = PlanGenerator()

        # Analyze task
        task_type, complexity = generator.analyze_prompt(
            "Add OAuth authentication to the app"
        )

        # Create template
        template = PlanTemplate(
            task_name="Add OAuth Authentication",
            task_type=task_type,
            complexity=complexity,
            description="Implement OAuth 2.0 authentication...",
            objective="Users can log in via Google/GitHub OAuth",
            ...
        )

        # Generate markdown
        markdown = generator.generate(template)

        # Save to file
        filepath = generator.save(template, "specs/")
    """

    # Keywords for task type detection
    TASK_TYPE_KEYWORDS = {
        TaskType.MIGRATION: ["migrate", "migration", "databricks", "sql server", "convert"],
        TaskType.FEATURE: ["add", "implement", "create", "new", "build"],
        TaskType.FIX: ["fix", "bug", "error", "issue", "broken", "failing"],
        TaskType.REFACTOR: ["refactor", "restructure", "reorganize", "clean up"],
        TaskType.ENHANCEMENT: ["improve", "enhance", "optimize", "update", "upgrade"],
        TaskType.CHORE: ["chore", "config", "setup", "documentation", "deps"],
    }

    # Keywords for complexity detection
    COMPLEXITY_KEYWORDS = {
        Complexity.COMPLEX: [
            "migration", "architecture", "system", "multiple", "integrate",
            "database", "auth", "security", "pipeline", "workflow"
        ],
        Complexity.MEDIUM: [
            "feature", "component", "module", "service", "api", "endpoint"
        ],
        Complexity.SIMPLE: [
            "fix", "update", "change", "rename", "move", "delete", "add"
        ],
    }

    def analyze_prompt(self, prompt: str) -> tuple[TaskType, Complexity]:
        """
        Analyze a prompt to determine task type and complexity.

        Returns tuple of (TaskType, Complexity).
        """
        prompt_lower = prompt.lower()

        # Detect task type
        task_type = TaskType.FEATURE  # Default
        for ttype, keywords in self.TASK_TYPE_KEYWORDS.items():
            if any(kw in prompt_lower for kw in keywords):
                task_type = ttype
                break

        # Detect complexity
        complexity = Complexity.MEDIUM  # Default

        # Check for complex indicators
        complex_count = sum(1 for kw in self.COMPLEXITY_KEYWORDS[Complexity.COMPLEX]
                           if kw in prompt_lower)
        simple_count = sum(1 for kw in self.COMPLEXITY_KEYWORDS[Complexity.SIMPLE]
                          if kw in prompt_lower)

        if complex_count >= 2 or task_type == TaskType.MIGRATION:
            complexity = Complexity.COMPLEX
        elif simple_count >= 2 and complex_count == 0:
            complexity = Complexity.SIMPLE

        return task_type, complexity

    def generate_filename(self, task_name: str) -> str:
        """Generate a kebab-case filename from task name."""
        # Convert to lowercase
        name = task_name.lower()
        # Remove special characters
        name = re.sub(r"[^a-z0-9\s-]", "", name)
        # Replace spaces with hyphens
        name = re.sub(r"\s+", "-", name)
        # Remove multiple hyphens
        name = re.sub(r"-+", "-", name)
        # Trim hyphens from ends
        name = name.strip("-")

        return f"{name}.md"

    def generate(self, template: PlanTemplate) -> str:
        """Generate markdown plan from template."""
        sections = []

        # Title
        sections.append(f"# Plan: {template.task_name}\n")

        # Task Description
        sections.append("## Task Description\n")
        sections.append(f"{template.description}\n")

        # Objective
        sections.append("## Objective\n")
        sections.append(f"{template.objective}\n")

        # Problem Statement (for features or medium/complex)
        if template.problem_statement and (
            template.task_type == TaskType.FEATURE or
            template.complexity in [Complexity.MEDIUM, Complexity.COMPLEX]
        ):
            sections.append("## Problem Statement\n")
            sections.append(f"{template.problem_statement}\n")

        # Solution Approach
        if template.solution_approach and (
            template.task_type == TaskType.FEATURE or
            template.complexity in [Complexity.MEDIUM, Complexity.COMPLEX]
        ):
            sections.append("## Solution Approach\n")
            sections.append(f"{template.solution_approach}\n")

        # Migration-specific: Source System Analysis
        if template.task_type == TaskType.MIGRATION:
            sections.append("## Source System Analysis\n")
            sections.append(f"- **Database**: {template.source_database or 'TBD'}\n")
            sections.append(f"- **Server**: {template.source_server or 'TBD'}\n")
            if template.object_counts:
                counts = ", ".join(f"{k}: {v}" for k, v in template.object_counts.items())
                sections.append(f"- **Object Count**: {counts}\n")
            if template.complexity_indicators:
                sections.append(f"- **Complexity Indicators**: {', '.join(template.complexity_indicators)}\n")
            sections.append("")

            sections.append("## Target Architecture\n")
            sections.append(f"- **Catalog**: {template.target_catalog or 'TBD'}\n")
            sections.append(f"- **Schema**: {template.target_schema or 'TBD'}\n")
            sections.append("- **Pipeline Type**: DLT\n")
            sections.append("- **Medallion Layers**: Bronze, Silver, Gold\n")

        # Relevant Files
        sections.append("## Relevant Files\n")
        sections.append("Use these files to complete the task:\n")
        for f in template.relevant_files:
            sections.append(f"- {f}\n")

        if template.new_files:
            sections.append("\n### New Files\n")
            for f in template.new_files:
                sections.append(f"- {f}\n")

        # Implementation Phases
        if template.phases and template.complexity in [Complexity.MEDIUM, Complexity.COMPLEX]:
            sections.append("\n## Implementation Phases\n")
            for i, phase in enumerate(template.phases, 1):
                sections.append(f"### Phase {i}: {phase.get('name', f'Phase {i}')}\n")
                sections.append(f"{phase.get('description', '')}\n")

        # Step by Step Tasks
        sections.append("\n## Step by Step Tasks\n")
        sections.append("IMPORTANT: Execute every step in order, top to bottom.\n")

        for i, task in enumerate(template.tasks, 1):
            task_name = task.get("name", f"Task {i}")
            sections.append(f"\n### {i}. {task_name}\n")
            for action in task.get("actions", []):
                sections.append(f"- {action}\n")

        # Testing Strategy
        if template.testing_strategy and (
            template.task_type == TaskType.FEATURE or
            template.complexity in [Complexity.MEDIUM, Complexity.COMPLEX]
        ):
            sections.append("\n## Testing Strategy\n")
            sections.append(f"{template.testing_strategy}\n")

        # Acceptance Criteria
        sections.append("\n## Acceptance Criteria\n")
        for criterion in template.acceptance_criteria:
            sections.append(f"- [ ] {criterion}\n")

        # Validation Commands
        sections.append("\n## Validation Commands\n")
        sections.append("Execute these commands to validate the task is complete:\n")
        for cmd in template.validation_commands:
            command = cmd.get("command", "")
            description = cmd.get("description", "")
            sections.append(f"- `{command}` - {description}\n")

        # Notes
        if template.notes:
            sections.append("\n## Notes\n")
            sections.append(f"{template.notes}\n")

        # Migration-specific notes
        if template.task_type == TaskType.MIGRATION:
            sections.append("\n### Required Databricks Secrets\n")
            sections.append("```bash\n")
            sections.append("databricks secrets create-scope --scope migration_secrets\n")
            sections.append("databricks secrets put --scope migration_secrets --key sqlserver_jdbc_url\n")
            sections.append("databricks secrets put --scope migration_secrets --key sqlserver_user\n")
            sections.append("databricks secrets put --scope migration_secrets --key sqlserver_password\n")
            sections.append("```\n")

            sections.append("\n### Required Cluster Libraries\n")
            sections.append("- SQL Server JDBC Driver: `mssql-jdbc-12.4.2.jre11.jar`\n")

        return "".join(sections)

    def save(self, template: PlanTemplate, output_dir: str = "specs/") -> str:
        """
        Save plan to file.

        Returns the filepath.
        """
        # Ensure directory exists
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate filename and content
        filename = self.generate_filename(template.task_name)
        content = self.generate(template)

        # Write file
        filepath = output_path / filename
        filepath.write_text(content, encoding="utf-8")

        return str(filepath)

    def create_migration_template(
        self,
        source_database: str,
        source_server: str,
        target_catalog: str,
        target_schema: str,
        tables: List[str],
        procedures: List[str] = None,
        views: List[str] = None,
    ) -> PlanTemplate:
        """
        Create a pre-filled template for database migration.

        This is a convenience method for the common migration use case.
        """
        procedures = procedures or []
        views = views or []

        return PlanTemplate(
            task_name=f"Migrate {source_database} to Databricks",
            task_type=TaskType.MIGRATION,
            complexity=Complexity.COMPLEX,
            description=f"Migrate {source_database} database from SQL Server to Databricks "
                       f"using DLT pipelines with medallion architecture.",
            objective=f"All tables from {source_database} are accessible in "
                     f"{target_catalog}.{target_schema} with bronze, silver, and gold layers.",
            source_database=source_database,
            source_server=source_server,
            object_counts={
                "tables": len(tables),
                "procedures": len(procedures),
                "views": len(views),
            },
            target_catalog=target_catalog,
            target_schema=target_schema,
            phases=[
                {"name": "Setup & Configuration", "description": "Configure credentials and verify connectivity"},
                {"name": "Assessment & Extraction", "description": "Extract SQL objects and analyze complexity"},
                {"name": "Transpilation", "description": "Convert T-SQL to Databricks SQL"},
                {"name": "DLT Pipeline Generation", "description": "Generate bronze/silver/gold layers"},
                {"name": "Databricks Deployment", "description": "Deploy to workspace with serverless compute"},
                {"name": "Validation", "description": "Run pipeline and validate data"},
            ],
            tasks=[
                {"name": "Configure Credentials", "actions": [
                    "Create ~/.databricks/labs/lakebridge/.credentials.yml",
                    "Add SQL Server connection details",
                    "Add Databricks workspace and token",
                ]},
                {"name": "Test Connectivity", "actions": [
                    "Run python test_connections.py",
                    "Verify SQL Server access",
                    "Verify Databricks API access",
                ]},
                {"name": "Extract SQL Objects", "actions": [
                    "Query sys.tables for table list",
                    "Query sys.procedures for stored procedures",
                    "Query sys.views for views",
                ]},
                {"name": "Generate DLT Pipeline", "actions": [
                    "Create bronze layer for JDBC ingestion",
                    "Create silver layer with data quality",
                    "Create gold layer for business views",
                ]},
                {"name": "Deploy to Databricks", "actions": [
                    "Create secret scope",
                    "Create target schema",
                    "Upload pipeline notebook",
                    "Create DLT pipeline with serverless=True",
                ]},
                {"name": "Validate Migration", "actions": [
                    "Start pipeline update",
                    "Monitor to completion",
                    "Compare row counts",
                ]},
            ],
            acceptance_criteria=[
                "All source tables have bronze layer equivalents",
                "Silver layer filters soft-deleted records",
                "Pipeline runs without errors",
                "Row counts match source within tolerance",
            ],
            validation_commands=[
                {"command": "python test_connections.py", "description": "Verify connectivity"},
                {"command": "python extract_sql_objects.py", "description": "Extract and count objects"},
                {"command": "python generate_dlt_pipeline.py", "description": "Generate pipeline notebook"},
                {"command": "python deploy_to_databricks.py", "description": "Deploy to workspace"},
                {"command": "python monitor_pipeline.py", "description": "Monitor execution"},
            ],
        )
