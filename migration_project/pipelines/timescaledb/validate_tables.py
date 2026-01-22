"""
TimescaleDB Table Validation Script
====================================
Validates that the tables defined in timescaledb_tables.yml exist in TimescaleDB.

Usage:
    # Local Python (requires psycopg2):
    python validate_tables.py

    # Or in Databricks notebook:
    %run ./validate_tables
"""

import yaml
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Configuration
CONFIG_PATH = Path(__file__).parent / "config" / "timescaledb_tables.yml"
CREDENTIALS_PATH = Path.home() / ".databricks/labs/lakebridge/.credentials.yml"


def load_registry_tables(config_path: Path) -> Set[str]:
    """Load tables from the registry YAML file."""
    with open(config_path) as f:
        registry = yaml.safe_load(f)

    tables = set()
    default_schema = registry.get('defaults', {}).get('source_schema', 'public')

    for table in registry.get("tables", []):
        schema = table.get("source_schema", default_schema)
        table_name = table["source_table"]
        tables.add(f"{schema}.{table_name}")

    return tables


def get_timescaledb_tables_psycopg2(credentials: Dict) -> Set[str]:
    """Get tables from TimescaleDB using psycopg2."""
    import psycopg2

    conn = psycopg2.connect(
        host=credentials["host"],
        port=credentials.get("port", 5432),
        database=credentials["database"],
        user=credentials["user"],
        password=credentials["password"],
        sslmode=credentials.get("sslmode", "require")
    )

    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema', '_timescaledb_catalog',
                                   '_timescaledb_internal', '_timescaledb_config', '_timescaledb_cache')
          AND table_name NOT LIKE 'pg_%'
          AND table_name NOT LIKE '_hyper_%'
        ORDER BY table_schema, table_name
    """)

    tables = set()
    for row in cursor.fetchall():
        schema, table = row
        tables.add(f"{schema}.{table}")

    cursor.close()
    conn.close()

    return tables


def compare_tables(registry_tables: Set[str], actual_tables: Set[str]) -> Tuple[Set[str], Set[str], Set[str]]:
    """Compare registry tables with actual tables."""
    matched = registry_tables & actual_tables
    missing_from_db = registry_tables - actual_tables
    not_in_registry = actual_tables - registry_tables

    return matched, missing_from_db, not_in_registry


def print_report(matched: Set[str], missing_from_db: Set[str], not_in_registry: Set[str]):
    """Print validation report."""
    print("=" * 70)
    print("TIMESCALEDB TABLE VALIDATION REPORT")
    print("=" * 70)
    print()
    print(f"Registry tables:  {len(matched) + len(missing_from_db)}")
    print(f"Actual tables:    {len(matched) + len(not_in_registry)}")
    print(f"Matched:          {len(matched)}")
    print(f"Missing from DB:  {len(missing_from_db)}")
    print(f"Not in registry:  {len(not_in_registry)}")
    print()

    if not missing_from_db:
        print("-" * 70)
        print("VALIDATION PASSED: All registry tables exist in database")
        print("-" * 70)
    else:
        print("-" * 70)
        print("MISSING FROM DATABASE:")
        print("-" * 70)
        for t in sorted(missing_from_db):
            print(f"  [X] {t}")
        print()

    if not_in_registry:
        print("-" * 70)
        print("NOT IN REGISTRY (excluded or new tables):")
        print("-" * 70)
        for t in sorted(not_in_registry)[:30]:
            print(f"  [?] {t}")
        if len(not_in_registry) > 30:
            print(f"  ... and {len(not_in_registry) - 30} more")


def validate_local():
    """Run validation using local psycopg2 connection."""
    print("Loading credentials...")

    if CREDENTIALS_PATH.exists():
        with open(CREDENTIALS_PATH) as f:
            all_creds = yaml.safe_load(f)
            credentials = all_creds.get("timescaledb", {})
    else:
        import os
        credentials = {
            "host": os.environ.get("TIMESCALEDB_HOST"),
            "port": os.environ.get("TIMESCALEDB_PORT", 5432),
            "database": os.environ.get("TIMESCALEDB_DATABASE"),
            "user": os.environ.get("TIMESCALEDB_USER"),
            "password": os.environ.get("TIMESCALEDB_PASSWORD"),
            "sslmode": os.environ.get("TIMESCALEDB_SSLMODE", "require")
        }

    if not credentials.get("host") or credentials["host"].startswith("YOUR_"):
        print("ERROR: TimescaleDB credentials not configured.")
        return None

    print("Loading registry tables...")
    registry_tables = load_registry_tables(CONFIG_PATH)

    print("Connecting to TimescaleDB...")
    actual_tables = get_timescaledb_tables_psycopg2(credentials)

    print("Comparing tables...")
    matched, missing, extra = compare_tables(registry_tables, actual_tables)

    print_report(matched, missing, extra)

    return {
        "matched": len(matched),
        "missing_from_db": len(missing),
        "not_in_registry": len(extra)
    }


def print_registry_summary():
    """Print summary of tables in the registry."""
    with open(CONFIG_PATH) as f:
        registry = yaml.safe_load(f)

    tables = registry.get("tables", [])
    default_schema = registry.get('defaults', {}).get('source_schema', 'public')

    print("=" * 70)
    print("REGISTRY SUMMARY (from timescaledb_tables.yml)")
    print("=" * 70)
    print()
    print(f"Source: {registry.get('source_system', 'timescaledb')}")
    print(f"Database: {registry.get('source_database', 'N/A')}")
    print(f"Target: {registry.get('target_catalog', 'N/A')}.{registry.get('target_schema', 'N/A')}")
    print(f"Load Mode: {registry.get('defaults', {}).get('load_mode', 'incremental')}")
    print(f"Merge Strategy: {registry.get('defaults', {}).get('merge_strategy', 'upsert')}")
    print()
    print(f"Total tables: {len(tables)}")
    print()

    # Count geometry tables
    geom_tables = [t for t in tables if t.get('has_geometry')]
    print(f"Tables with geometry: {len(geom_tables)}")
    print()

    print("Tables:")
    for t in tables:
        schema = t.get('source_schema', default_schema)
        name = t['source_table']
        pk = t.get('primary_key_columns', ['Id'])
        wm = t.get('watermark_column', 'UpdatedAt')
        geom = " (geometry)" if t.get('has_geometry') else ""
        print(f"  - {schema}.{name} [PK: {pk}, WM: {wm}]{geom}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--summary":
        print_registry_summary()
    else:
        try:
            result = validate_local()
        except ImportError:
            print("psycopg2 not installed. Install with: pip install psycopg2-binary")
            print("\nShowing registry summary instead:")
            print_registry_summary()
        except Exception as e:
            print(f"Error: {e}")
            print("\nShowing registry summary instead:")
            print_registry_summary()
