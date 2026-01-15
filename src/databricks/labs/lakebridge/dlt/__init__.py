"""
Lakebridge DLT Module - Delta Live Tables deployment for migrations.

This module provides tools for deploying migrated SQL code to Databricks
Delta Live Tables (DLT) pipelines.

Features:
- Convert transpiled SQL to DLT notebook format
- Generate DLT pipeline configurations
- Deploy pipelines to Databricks workspace
- Manage pipeline lifecycle (create, update, delete, run)

Usage:
    from databricks.labs.lakebridge.dlt import DLTDeployer, DLTGenerator
    
    # Generate DLT notebook from transpiled SQL
    generator = DLTGenerator(catalog="my_catalog", schema="my_schema")
    notebook_content = generator.generate_notebook(transpiled_sql_files)
    
    # Deploy to Databricks
    deployer = DLTDeployer(workspace_client)
    pipeline_id = deployer.create_pipeline(
        name="migration_pipeline",
        notebook_path="/Workspace/migrations/dlt_pipeline",
        target_catalog="my_catalog",
        target_schema="my_schema",
    )
"""

from databricks.labs.lakebridge.dlt.generator import (
    DLTGenerator,
    DLTTableDefinition,
    DLTViewDefinition,
    DLTStreamDefinition,
    DLTNotebook,
)
from databricks.labs.lakebridge.dlt.deployer import (
    DLTDeployer,
    DLTPipelineConfig,
    PipelineState,
)
from databricks.labs.lakebridge.dlt.converter import (
    SQLToDLTConverter,
    ConversionResult,
)

__all__ = [
    # Generator
    "DLTGenerator",
    "DLTTableDefinition",
    "DLTViewDefinition",
    "DLTStreamDefinition",
    "DLTNotebook",
    # Deployer
    "DLTDeployer",
    "DLTPipelineConfig",
    "PipelineState",
    # Converter
    "SQLToDLTConverter",
    "ConversionResult",
]
