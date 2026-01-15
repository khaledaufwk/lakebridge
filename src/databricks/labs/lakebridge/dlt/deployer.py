"""
DLT Deployer - Deploy Delta Live Tables pipelines to Databricks.

This module provides tools for deploying DLT pipelines to Databricks
workspaces using the Databricks SDK.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    PipelineSpec,
    PipelineCluster,
    PipelineLibrary,
    NotebookLibrary,
    CreatePipelineResponse,
    PipelineStateInfo,
    PipelineState as SDKPipelineState,
    StartUpdate,
    UpdateInfo,
)
from databricks.sdk.service.workspace import (
    ImportFormat,
    Language,
)

logger = logging.getLogger(__name__)


class PipelineState(str, Enum):
    """Pipeline execution states."""
    
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STARTING = "STARTING"
    FAILED = "FAILED"
    DELETED = "DELETED"
    UNKNOWN = "UNKNOWN"


@dataclass
class DLTPipelineConfig:
    """Configuration for a DLT pipeline."""
    
    name: str
    notebook_path: str
    target_catalog: str | None = None
    target_schema: str | None = None
    storage: str | None = None
    
    # Cluster configuration
    num_workers: int | None = None
    node_type_id: str | None = None
    driver_node_type_id: str | None = None
    spark_conf: dict[str, str] = field(default_factory=dict)
    
    # Pipeline settings
    development: bool = True
    continuous: bool = False
    photon: bool = True
    serverless: bool = False
    channel: str = "CURRENT"  # CURRENT or PREVIEW
    
    # Additional libraries
    libraries: list[str] = field(default_factory=list)
    
    # Notifications
    email_notifications: list[str] = field(default_factory=list)
    
    # Tags
    tags: dict[str, str] = field(default_factory=dict)
    
    def to_pipeline_spec(self) -> dict[str, Any]:
        """Convert to Databricks pipeline spec dictionary."""
        spec: dict[str, Any] = {
            "name": self.name,
            "development": self.development,
            "continuous": self.continuous,
            "photon_enabled": self.photon,
            "channel": self.channel,
        }
        
        # Target catalog/schema (Unity Catalog)
        if self.target_catalog:
            spec["catalog"] = self.target_catalog
        if self.target_schema:
            spec["target"] = self.target_schema
        
        # Storage location
        if self.storage:
            spec["storage"] = self.storage
        
        # Serverless
        if self.serverless:
            spec["serverless"] = True
        else:
            # Cluster configuration (only for non-serverless)
            cluster_config: dict[str, Any] = {}
            if self.num_workers is not None:
                cluster_config["num_workers"] = self.num_workers
            if self.node_type_id:
                cluster_config["node_type_id"] = self.node_type_id
            if self.driver_node_type_id:
                cluster_config["driver_node_type_id"] = self.driver_node_type_id
            if self.spark_conf:
                cluster_config["spark_conf"] = self.spark_conf
            
            if cluster_config:
                spec["clusters"] = [{"label": "default", **cluster_config}]
        
        # Libraries (notebooks)
        libraries = [{"notebook": {"path": self.notebook_path}}]
        for lib in self.libraries:
            if lib.endswith(".py"):
                libraries.append({"notebook": {"path": lib}})
            elif lib.endswith(".jar"):
                libraries.append({"jar": lib})
            elif lib.endswith(".whl"):
                libraries.append({"whl": lib})
        spec["libraries"] = libraries
        
        return spec


@dataclass
class PipelineRunResult:
    """Result of a pipeline run."""
    
    pipeline_id: str
    update_id: str | None = None
    state: PipelineState = PipelineState.UNKNOWN
    error: str | None = None
    duration_seconds: float | None = None
    
    @property
    def success(self) -> bool:
        """Check if pipeline run was successful."""
        return self.state == PipelineState.IDLE and self.error is None


class DLTDeployer:
    """Deploy and manage DLT pipelines in Databricks.
    
    Usage:
        from databricks.sdk import WorkspaceClient
        from databricks.labs.lakebridge.dlt import DLTDeployer, DLTPipelineConfig
        
        ws = WorkspaceClient()
        deployer = DLTDeployer(ws)
        
        # Create pipeline
        config = DLTPipelineConfig(
            name="migration_pipeline",
            notebook_path="/Workspace/migrations/dlt_pipeline",
            target_catalog="my_catalog",
            target_schema="my_schema",
        )
        pipeline_id = deployer.create_pipeline(config)
        
        # Run pipeline
        result = deployer.run_pipeline(pipeline_id, wait=True)
    """
    
    def __init__(self, workspace_client: WorkspaceClient):
        """Initialize the DLT deployer.
        
        Args:
            workspace_client: Databricks WorkspaceClient
        """
        self.ws = workspace_client
    
    def upload_notebook(
        self,
        local_path: str | Path,
        workspace_path: str,
        overwrite: bool = True,
    ) -> str:
        """Upload a notebook to the Databricks workspace.
        
        Args:
            local_path: Path to local notebook file
            workspace_path: Target path in workspace
            overwrite: Whether to overwrite existing notebook
            
        Returns:
            Workspace path of uploaded notebook
        """
        local_path = Path(local_path)
        
        if not local_path.exists():
            raise FileNotFoundError(f"Notebook not found: {local_path}")
        
        content = local_path.read_text()
        
        # Determine language
        if local_path.suffix == ".py":
            language = Language.PYTHON
        elif local_path.suffix == ".sql":
            language = Language.SQL
        elif local_path.suffix == ".scala":
            language = Language.SCALA
        else:
            language = Language.PYTHON
        
        logger.info(f"Uploading notebook to {workspace_path}")
        
        self.ws.workspace.import_(
            path=workspace_path,
            content=content.encode(),
            format=ImportFormat.SOURCE,
            language=language,
            overwrite=overwrite,
        )
        
        return workspace_path
    
    def create_pipeline(
        self,
        config: DLTPipelineConfig,
    ) -> str:
        """Create a new DLT pipeline.
        
        Args:
            config: Pipeline configuration
            
        Returns:
            Pipeline ID
        """
        spec = config.to_pipeline_spec()
        
        logger.info(f"Creating pipeline: {config.name}")
        
        response = self.ws.pipelines.create(**spec)
        
        pipeline_id = response.pipeline_id
        logger.info(f"Created pipeline with ID: {pipeline_id}")
        
        return pipeline_id
    
    def update_pipeline(
        self,
        pipeline_id: str,
        config: DLTPipelineConfig,
    ) -> None:
        """Update an existing DLT pipeline.
        
        Args:
            pipeline_id: ID of pipeline to update
            config: New pipeline configuration
        """
        spec = config.to_pipeline_spec()
        
        logger.info(f"Updating pipeline: {pipeline_id}")
        
        self.ws.pipelines.update(
            pipeline_id=pipeline_id,
            **spec,
        )
    
    def delete_pipeline(self, pipeline_id: str) -> None:
        """Delete a DLT pipeline.
        
        Args:
            pipeline_id: ID of pipeline to delete
        """
        logger.info(f"Deleting pipeline: {pipeline_id}")
        self.ws.pipelines.delete(pipeline_id=pipeline_id)
    
    def get_pipeline_state(self, pipeline_id: str) -> PipelineState:
        """Get current state of a pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Current pipeline state
        """
        info = self.ws.pipelines.get(pipeline_id=pipeline_id)
        
        if info.state:
            try:
                return PipelineState(info.state.value)
            except ValueError:
                return PipelineState.UNKNOWN
        
        return PipelineState.UNKNOWN
    
    def run_pipeline(
        self,
        pipeline_id: str,
        full_refresh: bool = False,
        wait: bool = True,
        timeout_seconds: int = 3600,
        poll_interval: int = 30,
    ) -> PipelineRunResult:
        """Run a DLT pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            full_refresh: Whether to do a full refresh
            wait: Whether to wait for completion
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks
            
        Returns:
            PipelineRunResult with execution details
        """
        logger.info(f"Starting pipeline: {pipeline_id}")
        
        start_time = time.time()
        
        # Start the pipeline
        update = self.ws.pipelines.start_update(
            pipeline_id=pipeline_id,
            full_refresh=full_refresh,
        )
        
        update_id = update.update_id
        
        if not wait:
            return PipelineRunResult(
                pipeline_id=pipeline_id,
                update_id=update_id,
                state=PipelineState.RUNNING,
            )
        
        # Wait for completion
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                return PipelineRunResult(
                    pipeline_id=pipeline_id,
                    update_id=update_id,
                    state=PipelineState.UNKNOWN,
                    error=f"Timeout after {timeout_seconds} seconds",
                    duration_seconds=elapsed,
                )
            
            state = self.get_pipeline_state(pipeline_id)
            
            if state == PipelineState.IDLE:
                # Check if there were any errors in the update
                update_info = self._get_update_info(pipeline_id, update_id)
                error = self._extract_error(update_info)
                
                return PipelineRunResult(
                    pipeline_id=pipeline_id,
                    update_id=update_id,
                    state=state,
                    error=error,
                    duration_seconds=time.time() - start_time,
                )
            
            elif state == PipelineState.FAILED:
                update_info = self._get_update_info(pipeline_id, update_id)
                error = self._extract_error(update_info)
                
                return PipelineRunResult(
                    pipeline_id=pipeline_id,
                    update_id=update_id,
                    state=state,
                    error=error or "Pipeline failed",
                    duration_seconds=time.time() - start_time,
                )
            
            logger.info(f"Pipeline state: {state.value}, waiting...")
            time.sleep(poll_interval)
    
    def stop_pipeline(self, pipeline_id: str) -> None:
        """Stop a running pipeline.
        
        Args:
            pipeline_id: Pipeline ID
        """
        logger.info(f"Stopping pipeline: {pipeline_id}")
        self.ws.pipelines.stop(pipeline_id=pipeline_id)
    
    def list_pipelines(
        self,
        filter_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """List all DLT pipelines.
        
        Args:
            filter_name: Optional name filter (substring match)
            
        Returns:
            List of pipeline info dictionaries
        """
        pipelines = []
        
        for pipeline in self.ws.pipelines.list_pipelines():
            if filter_name and filter_name not in (pipeline.name or ""):
                continue
            
            pipelines.append({
                "pipeline_id": pipeline.pipeline_id,
                "name": pipeline.name,
                "state": pipeline.state.value if pipeline.state else None,
                "creator_user_name": pipeline.creator_user_name,
            })
        
        return pipelines
    
    def _get_update_info(
        self,
        pipeline_id: str,
        update_id: str | None,
    ) -> dict[str, Any] | None:
        """Get information about a specific update."""
        if not update_id:
            return None
        
        try:
            update = self.ws.pipelines.get_update(
                pipeline_id=pipeline_id,
                update_id=update_id,
            )
            return {
                "update_id": update.update.update_id if update.update else None,
                "state": update.update.state if update.update else None,
                "cause": update.update.cause if update.update else None,
            }
        except Exception as e:
            logger.warning(f"Failed to get update info: {e}")
            return None
    
    def _extract_error(self, update_info: dict[str, Any] | None) -> str | None:
        """Extract error message from update info."""
        if not update_info:
            return None
        
        # Check for error cause
        cause = update_info.get("cause")
        if cause and "error" in str(cause).lower():
            return str(cause)
        
        return None
    
    def deploy_and_run(
        self,
        notebook_path: str | Path,
        workspace_notebook_path: str,
        config: DLTPipelineConfig,
        wait: bool = True,
    ) -> PipelineRunResult:
        """Upload notebook, create pipeline, and run it.
        
        Convenience method for full deployment workflow.
        
        Args:
            notebook_path: Local path to notebook
            workspace_notebook_path: Target path in workspace
            config: Pipeline configuration
            wait: Whether to wait for completion
            
        Returns:
            PipelineRunResult
        """
        # Upload notebook
        self.upload_notebook(notebook_path, workspace_notebook_path)
        
        # Update config with workspace path
        config.notebook_path = workspace_notebook_path
        
        # Check if pipeline exists
        existing = self.list_pipelines(filter_name=config.name)
        
        if existing:
            # Update existing pipeline
            pipeline_id = existing[0]["pipeline_id"]
            self.update_pipeline(pipeline_id, config)
        else:
            # Create new pipeline
            pipeline_id = self.create_pipeline(config)
        
        # Run pipeline
        return self.run_pipeline(pipeline_id, wait=wait)
