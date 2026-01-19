"""
Pipeline deployment utilities for Lakebridge skills.

Handles end-to-end deployment of DLT pipelines to Databricks.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import sys

# Add shared scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared" / "scripts"))


@dataclass
class DeploymentResult:
    """Result of a pipeline deployment."""
    success: bool
    pipeline_id: Optional[str] = None
    update_id: Optional[str] = None
    workspace_path: Optional[str] = None
    pipeline_url: Optional[str] = None
    error: Optional[str] = None
    steps_completed: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "pipeline_id": self.pipeline_id,
            "update_id": self.update_id,
            "workspace_path": self.workspace_path,
            "pipeline_url": self.pipeline_url,
            "error": self.error,
            "steps_completed": self.steps_completed,
            "warnings": self.warnings,
        }


class PipelineDeployer:
    """
    Deploys DLT pipelines to Databricks.

    Handles the complete deployment workflow:
    1. Validate credentials
    2. Create secret scope
    3. Set SQL Server secrets
    4. Create target schema
    5. Upload pipeline notebook
    6. Create DLT pipeline (serverless)
    7. Start pipeline update

    Usage:
        deployer = PipelineDeployer()
        deployer.load_credentials()

        result = deployer.deploy(
            notebook_content=pipeline_code,
            pipeline_name="MyMigration",
        )

        if result.success:
            print(f"Pipeline URL: {result.pipeline_url}")
    """

    def __init__(self):
        """Initialize deployer."""
        self._credentials = None
        self._databricks_client = None

    def load_credentials(self, credentials_path: Optional[Path] = None) -> "PipelineDeployer":
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

    def validate_notebook_format(self, content: str) -> List[str]:
        """
        Validate notebook content format.

        Returns list of issues (empty if valid).
        """
        issues = []

        # Check first line
        if not content.startswith("# Databricks notebook source"):
            issues.append("First line must be '# Databricks notebook source'")

        # Check for DLT import
        if "import dlt" not in content:
            issues.append("Missing 'import dlt' statement")

        # Check for table decorators
        if "@dlt.table" not in content:
            issues.append("No @dlt.table decorators found - pipeline will have NO_TABLES_IN_PIPELINE error")

        # Check for cell separators
        if "# COMMAND ----------" not in content:
            issues.append("Missing cell separators '# COMMAND ----------'")

        return issues

    def deploy(
        self,
        notebook_content: str,
        pipeline_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        secret_scope: Optional[str] = None,
        workspace_folder: str = "/Workspace/Shared/migrations",
        skip_secrets: bool = False,
    ) -> DeploymentResult:
        """
        Deploy a complete DLT pipeline.

        Args:
            notebook_content: Python notebook source code
            pipeline_name: Name for the pipeline
            catalog: Unity Catalog name (defaults to credentials)
            schema: Target schema name (defaults to credentials)
            secret_scope: Secret scope name (defaults to 'migration_secrets')
            workspace_folder: Workspace folder for notebook
            skip_secrets: Skip setting secrets (if already configured)

        Returns:
            DeploymentResult with pipeline details or error.
        """
        result = DeploymentResult(success=False)
        steps = result.steps_completed

        # Use defaults from credentials
        catalog = catalog or self.credentials.databricks.catalog
        schema = schema or self.credentials.databricks.schema
        secret_scope = secret_scope or self.credentials.secret_scope

        try:
            # Step 1: Validate notebook format
            format_issues = self.validate_notebook_format(notebook_content)
            if format_issues:
                result.warnings.extend(format_issues)
                # Don't fail, but warn - might still work

            # Step 2: Create/ensure secret scope
            if not skip_secrets:
                created = self.databricks.ensure_secret_scope(secret_scope)
                steps.append(f"Secret scope '{secret_scope}': {'created' if created else 'exists'}")

                # Step 3: Set SQL Server secrets
                self.databricks.set_migration_secrets(
                    scope=secret_scope,
                    jdbc_url=self.credentials.sqlserver.jdbc_url,
                    user=self.credentials.sqlserver.user,
                    password=self.credentials.sqlserver.password,
                )
                steps.append("SQL Server secrets configured")

            # Step 4: Create/ensure schema
            created = self.databricks.ensure_schema(catalog, schema)
            steps.append(f"Schema '{catalog}.{schema}': {'created' if created else 'exists'}")

            # Step 5: Upload notebook
            workspace_path = f"{workspace_folder}/{pipeline_name}"
            self.databricks.upload_notebook(notebook_content, workspace_path)
            result.workspace_path = workspace_path
            steps.append(f"Notebook uploaded to {workspace_path}")

            # Verify upload
            verify = self.databricks.verify_notebook(workspace_path)
            if not verify.get("exists"):
                raise RuntimeError(f"Notebook upload verification failed: {verify.get('error')}")
            if verify.get("object_type") != "NOTEBOOK":
                result.warnings.append(f"Upload type is {verify.get('object_type')}, expected NOTEBOOK")

            # Step 6: Create pipeline (ALWAYS serverless)
            pipeline_id = self.databricks.create_pipeline(
                name=pipeline_name,
                notebook_path=workspace_path,
                catalog=catalog,
                schema=schema,
                serverless=True,  # CRITICAL: Avoids VM quota issues
                development=True,
            )
            result.pipeline_id = pipeline_id
            result.pipeline_url = self.databricks.get_pipeline_url(pipeline_id)
            steps.append(f"Pipeline created: {pipeline_id}")

            # Step 7: Start pipeline
            update_id = self.databricks.start_pipeline(pipeline_id, full_refresh=True)
            result.update_id = update_id
            steps.append(f"Pipeline started: update {update_id}")

            result.success = True

        except Exception as e:
            result.error = str(e)
            steps.append(f"FAILED: {e}")

        return result

    def monitor(
        self,
        pipeline_id: str,
        update_id: str,
        poll_interval: int = 30,
        timeout: int = 3600,
    ) -> Dict[str, Any]:
        """
        Monitor a pipeline update to completion.

        Returns dict with:
        - state: Final state (COMPLETED, FAILED, etc.)
        - success: Whether completed successfully
        - duration_seconds: Time taken
        - error: Error message if failed
        """
        start_time = datetime.now()

        status = self.databricks.wait_for_pipeline(
            pipeline_id=pipeline_id,
            update_id=update_id,
            poll_interval=poll_interval,
            timeout=timeout,
        )

        duration = (datetime.now() - start_time).total_seconds()

        return {
            "state": status.state.value,
            "success": status.is_success,
            "duration_seconds": duration,
            "error": status.error,
        }

    def redeploy(
        self,
        pipeline_id: str,
        notebook_content: str,
        workspace_path: str,
    ) -> DeploymentResult:
        """
        Redeploy a pipeline with updated notebook content.

        Useful for fixing pipeline issues without recreating everything.
        """
        result = DeploymentResult(success=False)
        steps = result.steps_completed

        try:
            # Stop if running
            self.databricks.stop_pipeline(pipeline_id)
            steps.append("Stopped existing pipeline")

            # Re-upload notebook
            self.databricks.upload_notebook(notebook_content, workspace_path, overwrite=True)
            steps.append("Notebook re-uploaded")

            # Start fresh
            update_id = self.databricks.start_pipeline(pipeline_id, full_refresh=True)
            result.pipeline_id = pipeline_id
            result.update_id = update_id
            result.workspace_path = workspace_path
            result.pipeline_url = self.databricks.get_pipeline_url(pipeline_id)
            steps.append(f"Pipeline restarted: update {update_id}")

            result.success = True

        except Exception as e:
            result.error = str(e)
            steps.append(f"FAILED: {e}")

        return result

    def delete_and_recreate(
        self,
        old_pipeline_id: str,
        notebook_content: str,
        pipeline_name: str,
        catalog: str,
        schema: str,
        workspace_path: str,
    ) -> DeploymentResult:
        """
        Delete a failed pipeline and create a new one.

        Useful for recovering from pipeline configuration issues.
        """
        result = DeploymentResult(success=False)
        steps = result.steps_completed

        try:
            # Delete old pipeline
            self.databricks.delete_pipeline(old_pipeline_id)
            steps.append(f"Deleted old pipeline: {old_pipeline_id}")

            # Re-upload notebook
            self.databricks.upload_notebook(notebook_content, workspace_path, overwrite=True)
            steps.append("Notebook uploaded")

            # Create new pipeline (serverless)
            pipeline_id = self.databricks.create_pipeline(
                name=pipeline_name,
                notebook_path=workspace_path,
                catalog=catalog,
                schema=schema,
                serverless=True,
                development=True,
            )
            result.pipeline_id = pipeline_id
            result.pipeline_url = self.databricks.get_pipeline_url(pipeline_id)
            steps.append(f"New pipeline created: {pipeline_id}")

            # Start
            update_id = self.databricks.start_pipeline(pipeline_id, full_refresh=True)
            result.update_id = update_id
            result.workspace_path = workspace_path
            steps.append(f"Pipeline started: update {update_id}")

            result.success = True

        except Exception as e:
            result.error = str(e)
            steps.append(f"FAILED: {e}")

        return result
