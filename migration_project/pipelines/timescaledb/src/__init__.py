# TimescaleDB Loader Module
# =========================
# Parametrized reusable loader for incremental data loading from TimescaleDB

from .timescaledb_loader import (
    TimescaleDBLoader,
    TimescaleDBCredentials,
    TableConfig,
    WatermarkManager,
    WatermarkType,
    LoadStatus,
    LoadResult,
)

from .table_discovery import (
    TimescaleDBDiscovery,
    DiscoveredTable,
    discover_and_generate_registry,
)

__all__ = [
    # Core loader
    "TimescaleDBLoader",
    "TimescaleDBCredentials",
    "TableConfig",
    "WatermarkManager",
    "WatermarkType",
    "LoadStatus",
    "LoadResult",
    # Discovery
    "TimescaleDBDiscovery",
    "DiscoveredTable",
    "discover_and_generate_registry",
]
