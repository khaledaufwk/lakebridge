"""
Fix scripts for Lakebridge skills.

Provides automated stored procedure conversion capabilities:
- fixer: Core issue fixing functionality
- sp_converter: Main SP to Databricks notebook converter (Phase 1)
- temp_converter: T-SQL temp table to Spark temp view converter (Phase 2)
- spatial_converter: Spatial function stub generator (Phase 2)
- validation_generator: Validation notebook generator (Phase 2/3)
- analyzer_patterns: Shared pattern definitions
- dependency_resolver: Dependency resolution and creation (Phase 3)
"""

from .fixer import IssueFixer, FixResult, FixAction

# Phase 1 SP Converter
try:
    from .sp_converter import SPConverter, CursorConverter, MergeConverter, ConversionResult
    from .analyzer_patterns import SP_SOURCE_PATTERNS, SP_TARGET_PATTERNS, TSQL_TO_SPARK_MAPPINGS
    PHASE1_AVAILABLE = True
except ImportError:
    PHASE1_AVAILABLE = False

# Phase 2 Converters
try:
    from .temp_converter import TempTableConverter, TempTableInfo
    from .spatial_converter import SpatialConverter, SpatialFunctionInfo
    from .validation_generator import ValidationNotebookGenerator, ValidationConfig
    PHASE2_AVAILABLE = True
except ImportError:
    PHASE2_AVAILABLE = False

# Phase 3 Dependency Resolver (REQ-F6)
try:
    from .dependency_resolver import (
        DependencyResolver,
        DependencyResolverFactory,
        DependencyAction,
        ActionStatus,
        TableSchema,
        TSQL_TO_SPARK_TYPES,
    )
    PHASE3_AVAILABLE = True
except ImportError:
    PHASE3_AVAILABLE = False

__all__ = [
    # Core
    "IssueFixer",
    "FixResult",
    "FixAction",
    # Phase 1
    "SPConverter",
    "CursorConverter",
    "MergeConverter",
    "ConversionResult",
    "SP_SOURCE_PATTERNS",
    "SP_TARGET_PATTERNS",
    "TSQL_TO_SPARK_MAPPINGS",
    # Phase 2
    "TempTableConverter",
    "TempTableInfo",
    "SpatialConverter",
    "SpatialFunctionInfo",
    "ValidationNotebookGenerator",
    "ValidationConfig",
    # Phase 3
    "DependencyResolver",
    "DependencyResolverFactory",
    "DependencyAction",
    "ActionStatus",
    "TableSchema",
    "TSQL_TO_SPARK_TYPES",
    # Flags
    "PHASE1_AVAILABLE",
    "PHASE2_AVAILABLE",
    "PHASE3_AVAILABLE",
]
