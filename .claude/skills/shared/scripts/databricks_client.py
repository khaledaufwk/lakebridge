"""
Databricks client for Lakebridge migrations.

Provides high-level operations for:
- Secret scope management
- Schema creation
- Notebook upload
- DLT pipeline creation and monitoring
"""

import base64
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List
from enum import Enum


class PipelineState(Enum):
    """DLT pipeline states."""
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    WAITING_FOR_RESOURCES = "WAITING_FOR_RESOURCES"


@dataclass
class PipelineStatus:
    """Status of a DLT pipeline update."""
    pipeline_id: str
    update_id: str
    state: PipelineState
    message: Optional[str] = None
    error: Optional[str] = None

    @property
    def is_terminal(self) -> bool:
        """Check if pipeline is in terminal state."""
        return self.state in [
            PipelineState.COMPLETED,
            PipelineState.FAILED,
            PipelineState.CANCELED,
        ]

    @property
    def is_success(self) -> bool:
        """Check if pipeline completed successfully."""
        return self.state == PipelineState.COMPLETED


class DatabricksClient:
    """
    High-level Databricks client for migration operations.

    Wraps the Databricks SDK with migration-specific functionality
    and error handling.

    Usage:
        from credentials import CredentialsManager

        creds = CredentialsManager().load()
        client = DatabricksClient(
            host=creds.databricks.host,
            token=creds.databricks.token
        )

        # Create infrastructure
        client.ensure_schema("catalog", "schema")
        client.ensure_secret_scope("migration_secrets")

        # Deploy pipeline
        pipeline_id = client.create_pipeline(
            name="Migration",
            notebook_path="/Workspace/Shared/pipeline",
            catalog="catalog",
            schema="schema"
        )

        # Monitor
        status = client.wait_for_pipeline(pipeline_id)
    """

    def __init__(self, host: str, token: str):
        """Initialize client with workspace credentials."""
        self.host = host if host.startswith("https://") else f"https://{host}"
        self.token = token
        self._client = None

    @property
    def client(self):
        """Lazy-load Databricks SDK client."""
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(host=self.host, token=self.token)
        return self._client

    # -------------------------------------------------------------------------
    # Secret Scope Management
    # -------------------------------------------------------------------------

    def ensure_secret_scope(self, scope_name: str) -> bool:
        """
        Ensure a secret scope exists, creating if necessary.

        Returns True if scope was created, False if it already existed.
        """
        try:
            self.client.secrets.create_scope(scope=scope_name)
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                return False
            raise

    def set_secret(self, scope: str, key: str, value: str) -> None:
        """Set a secret value in a scope."""
        self.client.secrets.put_secret(
            scope=scope,
            key=key,
            string_value=value
        )

    def set_migration_secrets(
        self,
        scope: str,
        jdbc_url: str,
        user: str,
        password: str
    ) -> None:
        """
        Set all required secrets for SQL Server migration.

        Creates three secrets:
        - sqlserver_jdbc_url
        - sqlserver_user
        - sqlserver_password
        """
        self.set_secret(scope, "sqlserver_jdbc_url", jdbc_url)
        self.set_secret(scope, "sqlserver_user", user)
        self.set_secret(scope, "sqlserver_password", password)

    def list_secrets(self, scope: str) -> List[str]:
        """List secret keys in a scope."""
        try:
            secrets = self.client.secrets.list_secrets(scope=scope)
            return [s.key for s in secrets]
        except Exception:
            return []

    # -------------------------------------------------------------------------
    # Schema Management
    # -------------------------------------------------------------------------

    def ensure_schema(self, catalog: str, schema: str) -> bool:
        """
        Ensure a schema exists in Unity Catalog.

        Returns True if schema was created, False if it already existed.

        CRITICAL: Target schema must exist before running DLT pipelines.
        """
        try:
            self.client.schemas.create(name=schema, catalog_name=catalog)
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                return False
            raise

    def schema_exists(self, catalog: str, schema: str) -> bool:
        """Check if a schema exists."""
        try:
            self.client.schemas.get(full_name=f"{catalog}.{schema}")
            return True
        except Exception:
            return False

    # -------------------------------------------------------------------------
    # Notebook Management
    # -------------------------------------------------------------------------

    def upload_notebook(
        self,
        content: str,
        workspace_path: str,
        overwrite: bool = True
    ) -> str:
        """
        Upload a Python notebook to Databricks workspace.

        Args:
            content: Python source code (must start with '# Databricks notebook source')
            workspace_path: Target path (e.g., '/Workspace/Shared/migrations/pipeline')
            overwrite: Whether to overwrite existing notebook

        Returns:
            The workspace path of the uploaded notebook.

        CRITICAL: Content must be in Databricks notebook format:
        - First line: '# Databricks notebook source'
        - Cell separators: '# COMMAND ----------'
        """
        from databricks.sdk.service.workspace import ImportFormat, Language

        # Ensure parent directory exists
        parent = str(Path(workspace_path).parent)
        try:
            self.client.workspace.mkdirs(parent)
        except Exception:
            pass  # Directory may already exist

        # Encode content
        content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")

        # Upload
        self.client.workspace.import_(
            path=workspace_path,
            content=content_b64,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=overwrite
        )

        return workspace_path

    def verify_notebook(self, workspace_path: str) -> dict:
        """
        Verify a notebook was uploaded correctly.

        Returns dict with:
        - exists: bool
        - object_type: str (should be 'NOTEBOOK')
        - language: str
        """
        try:
            status = self.client.workspace.get_status(workspace_path)
            return {
                "exists": True,
                "object_type": status.object_type.name if status.object_type else None,
                "language": status.language.name if status.language else None,
            }
        except Exception as e:
            return {
                "exists": False,
                "error": str(e),
            }

    # -------------------------------------------------------------------------
    # Pipeline Management
    # -------------------------------------------------------------------------

    def create_pipeline(
        self,
        name: str,
        notebook_path: str,
        catalog: str,
        schema: str,
        serverless: bool = True,
        development: bool = True,
    ) -> str:
        """
        Create a DLT pipeline.

        Args:
            name: Pipeline name
            notebook_path: Path to pipeline notebook in workspace
            catalog: Unity Catalog name
            schema: Target schema name
            serverless: Use serverless compute (CRITICAL: avoids VM quota issues)
            development: Run in development mode

        Returns:
            Pipeline ID

        IMPORTANT: Always use serverless=True to avoid Azure VM quota errors.
        """
        from databricks.sdk.service.pipelines import (
            NotebookLibrary,
            PipelineLibrary,
        )

        result = self.client.pipelines.create(
            name=name,
            catalog=catalog,
            target=schema,
            development=development,
            serverless=serverless,
            libraries=[
                PipelineLibrary(notebook=NotebookLibrary(path=notebook_path))
            ],
        )

        return result.pipeline_id

    def start_pipeline(
        self,
        pipeline_id: str,
        full_refresh: bool = True
    ) -> str:
        """
        Start a pipeline update.

        Returns the update ID.
        """
        update = self.client.pipelines.start_update(
            pipeline_id=pipeline_id,
            full_refresh=full_refresh
        )
        return update.update_id

    def get_pipeline_status(
        self,
        pipeline_id: str,
        update_id: str
    ) -> PipelineStatus:
        """Get the current status of a pipeline update."""
        update = self.client.pipelines.get_update(
            pipeline_id=pipeline_id,
            update_id=update_id
        )

        state_str = update.update.state.value if update.update.state else "UNKNOWN"

        try:
            state = PipelineState(state_str)
        except ValueError:
            state = PipelineState.RUNNING

        return PipelineStatus(
            pipeline_id=pipeline_id,
            update_id=update_id,
            state=state,
            message=getattr(update.update, "message", None),
        )

    def wait_for_pipeline(
        self,
        pipeline_id: str,
        update_id: str,
        poll_interval: int = 30,
        timeout: int = 3600,
    ) -> PipelineStatus:
        """
        Wait for a pipeline update to complete.

        Args:
            pipeline_id: Pipeline ID
            update_id: Update ID
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait

        Returns:
            Final PipelineStatus
        """
        start_time = time.time()

        while True:
            status = self.get_pipeline_status(pipeline_id, update_id)

            if status.is_terminal:
                return status

            if time.time() - start_time > timeout:
                status.error = f"Timeout after {timeout} seconds"
                return status

            time.sleep(poll_interval)

    def delete_pipeline(self, pipeline_id: str) -> None:
        """Delete a pipeline."""
        self.client.pipelines.delete(pipeline_id=pipeline_id)

    def stop_pipeline(self, pipeline_id: str) -> None:
        """Stop a running pipeline."""
        try:
            self.client.pipelines.stop(pipeline_id=pipeline_id)
        except Exception:
            pass  # May already be stopped

    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------

    def get_pipeline_url(self, pipeline_id: str) -> str:
        """Get the URL to view a pipeline in Databricks UI."""
        return f"{self.host}/#joblist/pipelines/{pipeline_id}"

    def deploy_migration_pipeline(
        self,
        notebook_content: str,
        pipeline_name: str,
        catalog: str,
        schema: str,
        secret_scope: str,
        jdbc_url: str,
        sql_user: str,
        sql_password: str,
        workspace_path: Optional[str] = None,
    ) -> dict:
        """
        Deploy a complete migration pipeline.

        This is a convenience method that:
        1. Creates/ensures schema exists
        2. Creates/ensures secret scope exists
        3. Sets SQL Server secrets
        4. Uploads notebook
        5. Creates pipeline (serverless)
        6. Starts pipeline

        Returns dict with pipeline_id, update_id, and workspace_path.
        """
        # Default workspace path
        if workspace_path is None:
            workspace_path = f"/Workspace/Shared/migrations/{pipeline_name}"

        # Setup infrastructure
        self.ensure_schema(catalog, schema)
        self.ensure_secret_scope(secret_scope)
        self.set_migration_secrets(secret_scope, jdbc_url, sql_user, sql_password)

        # Upload notebook
        self.upload_notebook(notebook_content, workspace_path)

        # Create and start pipeline
        pipeline_id = self.create_pipeline(
            name=pipeline_name,
            notebook_path=workspace_path,
            catalog=catalog,
            schema=schema,
            serverless=True,
            development=True,
        )

        update_id = self.start_pipeline(pipeline_id, full_refresh=True)

        return {
            "pipeline_id": pipeline_id,
            "update_id": update_id,
            "workspace_path": workspace_path,
            "pipeline_url": self.get_pipeline_url(pipeline_id),
        }
