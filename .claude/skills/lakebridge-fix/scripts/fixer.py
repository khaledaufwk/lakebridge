"""
Issue fixing utilities for Lakebridge skills.

Provides automated fixes for common migration issues.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Callable, Dict, Any
from enum import Enum
from pathlib import Path
from datetime import datetime
import re
import sys

# Add shared scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared" / "scripts"))


class FixStatus(Enum):
    """Status of a fix attempt."""
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    MANUAL_REQUIRED = "manual_required"


@dataclass
class FixAction:
    """A single fix action."""
    issue_title: str
    action_description: str
    status: FixStatus
    file_changed: Optional[str] = None
    lines_changed: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "issue": self.issue_title,
            "action": self.action_description,
            "status": self.status.value,
            "file": self.file_changed,
            "lines": self.lines_changed,
            "error": self.error,
        }


@dataclass
class FixResult:
    """Result of fixing issues from a review."""
    success: bool
    actions: List[FixAction] = field(default_factory=list)
    blockers_fixed: int = 0
    high_risk_fixed: int = 0
    medium_risk_fixed: int = 0
    low_risk_fixed: int = 0
    skipped: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def total_fixed(self) -> int:
        """Total issues fixed."""
        return self.blockers_fixed + self.high_risk_fixed + self.medium_risk_fixed + self.low_risk_fixed

    def to_markdown(self) -> str:
        """Generate markdown fix report."""
        lines = [
            "# Fix Report",
            "",
            f"**Generated**: {self.generated_at.isoformat()}",
            f"**Status**: {'ALL FIXED' if self.success else 'PARTIAL'}",
            "",
            "---",
            "",
            "## Summary",
            "",
            f"- Blockers fixed: {self.blockers_fixed}",
            f"- High risk fixed: {self.high_risk_fixed}",
            f"- Medium risk fixed: {self.medium_risk_fixed}",
            f"- Low risk fixed: {self.low_risk_fixed}",
            f"- Total: {self.total_fixed}",
            "",
            "---",
            "",
            "## Fixes Applied",
            "",
        ]

        for action in self.actions:
            status_emoji = "" if action.status == FixStatus.SUCCESS else ""
            lines.append(f"### {status_emoji} {action.issue_title}")
            lines.append("")
            lines.append(f"**Action**: {action.action_description}")
            lines.append(f"**Status**: {action.status.value}")
            if action.file_changed:
                lines.append(f"**File**: `{action.file_changed}`")
            if action.error:
                lines.append(f"**Error**: {action.error}")
            lines.append("")

        if self.skipped:
            lines.append("---")
            lines.append("")
            lines.append("## Skipped Issues")
            lines.append("")
            for reason in self.skipped:
                lines.append(f"- {reason}")
            lines.append("")

        return "\n".join(lines)

    def save(self, output_dir: str = "app_fix_reports/") -> str:
        """Save fix report to file."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = self.generated_at.strftime("%Y%m%d_%H%M%S")
        filename = f"fix_{timestamp}.md"
        filepath = output_path / filename

        filepath.write_text(self.to_markdown(), encoding="utf-8")
        return str(filepath)


class IssueFixer:
    """
    Fixes common migration issues.

    Provides automated fixes for issues detected during review.

    Usage:
        fixer = IssueFixer()
        fixer.load_credentials()

        # Fix missing schema
        action = fixer.fix_missing_schema("catalog", "schema")

        # Fix notebook format
        fixed_content = fixer.fix_notebook_format(content)

        # Fix ambiguous columns
        fixed_content = fixer.fix_ambiguous_columns(content)
    """

    def __init__(self):
        """Initialize fixer."""
        self._credentials = None
        self._databricks_client = None
        self._result = FixResult(success=True)

    def load_credentials(self, credentials_path: Optional[Path] = None) -> "IssueFixer":
        """Load credentials from file."""
        from credentials import CredentialsManager

        self._credentials = CredentialsManager(credentials_path).load()
        return self

    @property
    def credentials(self):
        """Get loaded credentials."""
        if self._credentials is None:
            raise RuntimeError("Credentials not loaded. Call load_credentials() first.")
        return self._credentials

    @property
    def databricks(self):
        """Get Databricks client."""
        if self._databricks_client is None:
            from databricks_client import DatabricksClient

            self._databricks_client = DatabricksClient(
                host=self.credentials.databricks.host,
                token=self.credentials.databricks.token
            )
        return self._databricks_client

    def get_result(self) -> FixResult:
        """Get the fix result."""
        return self._result

    def _add_action(self, action: FixAction) -> None:
        """Add an action to the result."""
        self._result.actions.append(action)
        if action.status == FixStatus.FAILED:
            self._result.success = False

    # -------------------------------------------------------------------------
    # Infrastructure Fixes
    # -------------------------------------------------------------------------

    def fix_missing_schema(self, catalog: str, schema: str) -> FixAction:
        """Create missing schema in Unity Catalog."""
        action = FixAction(
            issue_title="Missing target schema",
            action_description=f"Create schema {catalog}.{schema}",
            status=FixStatus.SUCCESS,
        )

        try:
            created = self.databricks.ensure_schema(catalog, schema)
            if created:
                action.action_description = f"Created schema {catalog}.{schema}"
            else:
                action.action_description = f"Schema {catalog}.{schema} already exists"
            self._result.blockers_fixed += 1
        except Exception as e:
            action.status = FixStatus.FAILED
            action.error = str(e)

        self._add_action(action)
        return action

    def fix_missing_secret_scope(self, scope_name: str) -> FixAction:
        """Create missing secret scope."""
        action = FixAction(
            issue_title="Missing secret scope",
            action_description=f"Create secret scope {scope_name}",
            status=FixStatus.SUCCESS,
        )

        try:
            created = self.databricks.ensure_secret_scope(scope_name)
            if created:
                action.action_description = f"Created secret scope {scope_name}"
            else:
                action.action_description = f"Secret scope {scope_name} already exists"
            self._result.blockers_fixed += 1
        except Exception as e:
            action.status = FixStatus.FAILED
            action.error = str(e)

        self._add_action(action)
        return action

    def fix_missing_secrets(self, scope_name: str) -> FixAction:
        """Set SQL Server secrets in scope."""
        action = FixAction(
            issue_title="Missing SQL Server secrets",
            action_description=f"Set secrets in {scope_name}",
            status=FixStatus.SUCCESS,
        )

        try:
            self.databricks.set_migration_secrets(
                scope=scope_name,
                jdbc_url=self.credentials.sqlserver.jdbc_url,
                user=self.credentials.sqlserver.user,
                password=self.credentials.sqlserver.password,
            )
            action.action_description = "Configured sqlserver_jdbc_url, sqlserver_user, sqlserver_password"
            self._result.blockers_fixed += 1
        except Exception as e:
            action.status = FixStatus.FAILED
            action.error = str(e)

        self._add_action(action)
        return action

    # -------------------------------------------------------------------------
    # Notebook Content Fixes
    # -------------------------------------------------------------------------

    def fix_notebook_format(self, content: str) -> str:
        """
        Fix common notebook format issues.

        Returns fixed content.
        """
        fixed = content

        # Add notebook header if missing
        if not fixed.startswith("# Databricks notebook source"):
            fixed = "# Databricks notebook source\n" + fixed
            self._add_action(FixAction(
                issue_title="Missing notebook header",
                action_description="Added '# Databricks notebook source' header",
                status=FixStatus.SUCCESS,
            ))
            self._result.blockers_fixed += 1

        # Add DLT import if missing
        if "import dlt" not in fixed:
            # Find a good place to add it (after header/magic cells)
            lines = fixed.split("\n")
            insert_idx = 1
            for i, line in enumerate(lines):
                if line.startswith("# COMMAND") or (line.startswith("# MAGIC") and i > 0):
                    insert_idx = i + 1
                    break

            lines.insert(insert_idx, "import dlt")
            lines.insert(insert_idx + 1, "from pyspark.sql.functions import col, current_timestamp")
            fixed = "\n".join(lines)

            self._add_action(FixAction(
                issue_title="Missing DLT import",
                action_description="Added 'import dlt' statement",
                status=FixStatus.SUCCESS,
            ))
            self._result.blockers_fixed += 1

        return fixed

    def fix_ambiguous_columns(self, content: str, table_prefix_map: Optional[Dict[str, str]] = None) -> str:
        """
        Fix ambiguous column names in DLT notebook.

        Replaces generic column names like '_ingested_at' with prefixed versions.

        Args:
            content: Notebook content
            table_prefix_map: Optional mapping of table names to prefixes
                             e.g., {"worker": "worker", "project": "proj"}

        Returns fixed content.
        """
        fixed = content

        # Find all table definitions
        table_pattern = r'@dlt\.table\s*\([^)]*name\s*=\s*["\'](\w+)["\']'
        tables = re.findall(table_pattern, content)

        # Auto-generate prefix map if not provided
        if table_prefix_map is None:
            table_prefix_map = {}
            for table in tables:
                # Extract base name (remove bronze_, silver_, gold_ prefix)
                base = re.sub(r'^(bronze_|silver_|gold_)', '', table)
                table_prefix_map[table] = base

        # Replace generic column names with prefixed versions
        for table, prefix in table_prefix_map.items():
            # Find the function definition for this table
            func_pattern = rf'def\s+{table}\s*\(\s*\):\s*\n(.*?)(?=\ndef\s|\Z)'
            match = re.search(func_pattern, content, re.DOTALL)
            if match:
                func_body = match.group(1)
                # Replace generic column names
                new_body = func_body.replace(
                    '("_ingested_at"',
                    f'("{prefix}_ingested_at"'
                )
                new_body = new_body.replace(
                    "('_ingested_at'",
                    f"('{prefix}_ingested_at'"
                )
                if new_body != func_body:
                    fixed = fixed.replace(func_body, new_body)

        if fixed != content:
            self._add_action(FixAction(
                issue_title="Ambiguous column names",
                action_description="Renamed _ingested_at columns with table prefixes",
                status=FixStatus.SUCCESS,
            ))
            self._result.high_risk_fixed += 1

        return fixed

    def fix_serverless_flag(self, content: str) -> str:
        """
        Fix serverless=False to serverless=True in pipeline creation code.

        Returns fixed content.
        """
        if "serverless=False" in content or "serverless = False" in content:
            fixed = content.replace("serverless=False", "serverless=True")
            fixed = fixed.replace("serverless = False", "serverless = True")

            self._add_action(FixAction(
                issue_title="Not using serverless compute",
                action_description="Changed serverless=False to serverless=True",
                status=FixStatus.SUCCESS,
            ))
            self._result.high_risk_fixed += 1

            return fixed

        return content

    # -------------------------------------------------------------------------
    # Pipeline Fixes
    # -------------------------------------------------------------------------

    def fix_pipeline_serverless(self, old_pipeline_id: str, notebook_content: str,
                                 pipeline_name: str, catalog: str, schema: str,
                                 workspace_path: str) -> FixAction:
        """
        Delete pipeline and recreate with serverless=True.

        This fixes WAITING_FOR_RESOURCES / QuotaExhausted errors.
        """
        action = FixAction(
            issue_title="Pipeline not using serverless",
            action_description="Recreate pipeline with serverless=True",
            status=FixStatus.SUCCESS,
        )

        try:
            # Delete old pipeline
            self.databricks.delete_pipeline(old_pipeline_id)

            # Re-upload notebook (may have been fixed)
            self.databricks.upload_notebook(notebook_content, workspace_path, overwrite=True)

            # Create new pipeline with serverless
            new_pipeline_id = self.databricks.create_pipeline(
                name=pipeline_name,
                notebook_path=workspace_path,
                catalog=catalog,
                schema=schema,
                serverless=True,
                development=True,
            )

            action.action_description = f"Recreated pipeline {new_pipeline_id} with serverless=True"
            self._result.blockers_fixed += 1

        except Exception as e:
            action.status = FixStatus.FAILED
            action.error = str(e)

        self._add_action(action)
        return action

    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------

    def fix_all_infrastructure(self, catalog: str, schema: str, scope_name: str) -> None:
        """Fix all infrastructure issues at once."""
        self.fix_missing_schema(catalog, schema)
        self.fix_missing_secret_scope(scope_name)
        self.fix_missing_secrets(scope_name)

    def fix_notebook_content(self, content: str) -> str:
        """Apply all notebook content fixes."""
        fixed = self.fix_notebook_format(content)
        fixed = self.fix_ambiguous_columns(fixed)
        fixed = self.fix_serverless_flag(fixed)
        return fixed
