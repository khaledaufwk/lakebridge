"""
TimescaleDB Loader Package
==========================
Incremental loader for TimescaleDB to Databricks Delta Lake.
"""

from .loader import (
    TimescaleDBLoaderV2,
    TimescaleDBCredentials,
    WatermarkManagerV2,
    TableConfigV2,
    LoadResult,
    LoadStatus,
    WatermarkType,
)

__all__ = [
    "TimescaleDBLoaderV2",
    "TimescaleDBCredentials",
    "WatermarkManagerV2",
    "TableConfigV2",
    "LoadResult",
    "LoadStatus",
    "WatermarkType",
]

__version__ = "2.0.0"
