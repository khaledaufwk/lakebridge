#!/usr/bin/env python3
"""
Explore new TimescaleDB databases (observations, weather_station)
Discovers tables, primary keys, and watermark columns
"""

import psycopg2
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

# Load credentials
CREDS_PATH = Path(__file__).parent / "credentials_template.yml"

def load_credentials() -> Dict[str, Any]:
    """Load all credentials from YAML file."""
    with open(CREDS_PATH) as f:
        content = f.read()

    # Parse the YAML - handle duplicate keys by reading sections
    # Split by timescaledb sections
    sections = content.split("timescaledb:")

    result = {}

    # Parse databricks section
    result['databricks'] = yaml.safe_load(sections[0])['databricks']

    # Parse each timescaledb section
    for i, section in enumerate(sections[1:], 1):
        # Find the database name in this section
        section_yaml = yaml.safe_load("timescaledb:" + section.split("#")[0])
        if section_yaml and 'timescaledb' in section_yaml:
            db_name = section_yaml['timescaledb'].get('database', f'db_{i}')
            result[f'timescaledb_{db_name}'] = section_yaml['timescaledb']

    return result


def get_connection(config: Dict[str, Any]):
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password'],
        sslmode=config.get('sslmode', 'require')
    )


def discover_tables(conn) -> List[Dict[str, Any]]:
    """Discover all tables in public schema."""
    query = """
    SELECT
        table_schema,
        table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name NOT LIKE '_timescaledb%'
      AND table_name NOT LIKE 'pg_%'
    ORDER BY table_name
    """

    with conn.cursor() as cur:
        cur.execute(query)
        return [{'schema': row[0], 'table': row[1]} for row in cur.fetchall()]


def get_columns(conn, schema: str, table: str) -> List[Dict[str, Any]]:
    """Get column information for a table."""
    query = """
    SELECT
        column_name,
        data_type,
        is_nullable,
        column_default,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        return [{
            'name': row[0],
            'data_type': row[1],
            'nullable': row[2] == 'YES',
            'default': row[3],
            'position': row[4]
        } for row in cur.fetchall()]


def get_primary_keys(conn, schema: str, table: str) -> List[str]:
    """Get primary key columns for a table."""
    query = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = %s
      AND tc.table_name = %s
    ORDER BY kcu.ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        return [row[0] for row in cur.fetchall()]


def check_hypertable(conn, schema: str, table: str) -> bool:
    """Check if table is a TimescaleDB hypertable."""
    query = """
    SELECT EXISTS (
        SELECT 1
        FROM _timescaledb_catalog.hypertable ht
        JOIN pg_class c ON ht.table_name = c.relname
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = %s AND c.relname = %s
    )
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query, (schema, table))
            return cur.fetchone()[0]
    except:
        return False


def get_row_count(conn, schema: str, table: str) -> int:
    """Get approximate row count."""
    query = """
    SELECT reltuples::bigint
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = %s AND c.relname = %s
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query, (schema, table))
            result = cur.fetchone()
            return max(0, int(result[0])) if result else 0
    except:
        return 0


def detect_watermark_column(columns: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """Detect best watermark column based on naming conventions."""
    # Priority order for watermark patterns
    patterns = [
        ("WatermarkUTC", "timestamp"),
        ("watermark_utc", "timestamp"),
        ("watermark", "timestamp"),
        ("modified_at", "timestamp"),
        ("updated_at", "timestamp"),
        ("ModifiedAt", "timestamp"),
        ("UpdatedAt", "timestamp"),
        ("created_at", "timestamp"),
        ("CreatedAt", "timestamp"),
        ("last_modified", "timestamp"),
        ("LastModified", "timestamp"),
        ("timestamp", "timestamp"),
        ("Timestamp", "timestamp"),
        ("event_time", "timestamp"),
        ("EventTime", "timestamp"),
        ("GeneratedAt", "timestamp"),
        ("generated_at", "timestamp"),
        ("recorded_at", "timestamp"),
        ("RecordedAt", "timestamp"),
    ]

    timestamp_types = [
        "timestamp without time zone",
        "timestamp with time zone",
        "timestamptz",
        "timestamp",
    ]

    col_names = {c['name']: c['data_type'] for c in columns}

    # Check patterns in order
    for pattern, wm_type in patterns:
        if pattern in col_names:
            return pattern, wm_type

    # Fallback: any timestamp column with time/date keywords
    for col_name, data_type in col_names.items():
        if data_type.lower() in [t.lower() for t in timestamp_types]:
            if any(kw in col_name.lower() for kw in ['time', 'date', 'at', 'utc']):
                return col_name, "timestamp"

    return None, None


def detect_geometry_columns(columns: List[Dict[str, Any]]) -> List[str]:
    """Detect geometry columns."""
    geom_cols = []
    for col in columns:
        if col['data_type'] == 'USER-DEFINED' or 'geometry' in col['data_type'].lower():
            geom_cols.append(col['name'])
    return geom_cols


def explore_database(config: Dict[str, Any], db_name: str) -> Dict[str, Any]:
    """Explore a database and return discovery results."""
    print(f"\n{'='*60}")
    print(f"Exploring database: {config['database']}")
    print(f"Host: {config['host']}:{config['port']}")
    print(f"{'='*60}")

    conn = get_connection(config)

    try:
        tables = discover_tables(conn)
        print(f"\nFound {len(tables)} tables")

        results = {
            'database': config['database'],
            'host': config['host'],
            'port': config['port'],
            'discovered_at': datetime.now().isoformat(),
            'tables': []
        }

        for t in tables:
            schema, table = t['schema'], t['table']
            print(f"\n  {table}:")

            columns = get_columns(conn, schema, table)
            pks = get_primary_keys(conn, schema, table)
            is_hyper = check_hypertable(conn, schema, table)
            row_count = get_row_count(conn, schema, table)
            wm_col, wm_type = detect_watermark_column(columns)
            geom_cols = detect_geometry_columns(columns)

            print(f"    Columns: {len(columns)}")
            print(f"    Primary Keys: {pks or '(none)'}")
            print(f"    Watermark: {wm_col or '(none)'} ({wm_type or 'n/a'})")
            print(f"    Hypertable: {is_hyper}")
            print(f"    Row Count: ~{row_count:,}")
            if geom_cols:
                print(f"    Geometry: {geom_cols}")

            # Column details
            print(f"    Column list:")
            for col in columns:
                pk_marker = " [PK]" if col['name'] in pks else ""
                wm_marker = " [WM]" if col['name'] == wm_col else ""
                print(f"      - {col['name']}: {col['data_type']}{pk_marker}{wm_marker}")

            table_info = {
                'source_schema': schema,
                'source_table': table,
                'primary_key_columns': pks if pks else ['Id'],  # Default to Id if no PK
                'watermark_column': wm_col or 'CreatedAt',  # Default
                'watermark_type': wm_type or 'timestamp',
                'is_hypertable': is_hyper,
                'row_count_estimate': row_count,
                'columns': columns,
                'has_geometry': len(geom_cols) > 0,
                'geometry_columns': geom_cols if geom_cols else None,
            }

            results['tables'].append(table_info)

        return results

    finally:
        conn.close()


def generate_registry_yaml(
    discovery_results: Dict[str, Any],
    table_prefix: str,
    output_path: str
) -> str:
    """Generate registry YAML for discovered tables."""

    registry = {
        'registry_version': '2.0',
        'source_system': 'timescaledb',
        'source_database': discovery_results['database'],
        'table_prefix': table_prefix,
        'target_catalog': 'wakecap_prod',
        'target_schema': 'raw',
        'generated_at': discovery_results['discovered_at'],
        'defaults': {
            'source_schema': 'public',
            'watermark_type': 'timestamp',
            'is_full_load': False,
            'fetch_size': 50000,
            'batch_size': 100000,
            'enabled': True,
        },
        'tables': [],
        'excluded_tables': []
    }

    for t in discovery_results['tables']:
        # Categorize table
        name_lower = t['source_table'].lower()
        if 'observation' in name_lower or 'fact' in name_lower:
            category = 'facts'
        elif 'history' in name_lower or 'log' in name_lower:
            category = 'history'
        elif 'assignment' in name_lower or 'mapping' in name_lower:
            category = 'assignments'
        else:
            category = 'dimensions'

        table_config = {
            'source_table': t['source_table'],
            'primary_key_columns': t['primary_key_columns'],
            'watermark_column': t['watermark_column'],
            'category': category,
        }

        # Add optional fields
        if t['watermark_type'] and t['watermark_type'] != 'timestamp':
            table_config['watermark_type'] = t['watermark_type']

        if t['is_hypertable']:
            table_config['is_hypertable'] = True
            table_config['fetch_size'] = 100000
            table_config['batch_size'] = 200000

        if t['has_geometry']:
            table_config['has_geometry'] = True
            table_config['geometry_columns'] = t['geometry_columns']

        if t['row_count_estimate'] > 10000000:  # 10M+
            table_config['fetch_size'] = 100000
            table_config['batch_size'] = 500000
            table_config['comment'] = f"Large table: ~{t['row_count_estimate']:,} rows"

        registry['tables'].append(table_config)

    # Sort by category then name
    registry['tables'].sort(key=lambda x: (x['category'], x['source_table']))

    # Write YAML
    yaml_content = yaml.dump(registry, default_flow_style=False, sort_keys=False, allow_unicode=True)

    with open(output_path, 'w') as f:
        f.write(yaml_content)

    print(f"\nRegistry written to: {output_path}")
    return yaml_content


def main():
    print("=" * 60)
    print("NEW DATABASE EXPLORATION")
    print("=" * 60)

    # Parse credentials manually since YAML has duplicate keys
    with open(CREDS_PATH) as f:
        content = f.read()

    # Extract the three timescaledb configs
    lines = content.split('\n')

    dbs = []
    current_db = {}
    in_timescale = False

    for line in lines:
        if line.strip().startswith('# TimescaleDB Connection'):
            if current_db:
                dbs.append(current_db)
            current_db = {}
            in_timescale = False
        elif line.strip() == 'timescaledb:':
            in_timescale = True
        elif in_timescale and ':' in line and not line.strip().startswith('#'):
            key, value = line.strip().split(':', 1)
            key = key.strip()
            value = value.strip()
            if key in ['host', 'port', 'database', 'user', 'password', 'sslmode']:
                current_db[key] = int(value) if key == 'port' else value

    if current_db:
        dbs.append(current_db)

    print(f"\nFound {len(dbs)} TimescaleDB configurations")
    for db in dbs:
        print(f"  - {db.get('database', 'unknown')}")

    # Explore each new database (skip wakecap_app which is already done)
    config_dir = Path(__file__).parent / "pipelines" / "timescaledb" / "config"

    for db_config in dbs:
        db_name = db_config.get('database', '')

        if db_name == 'wakecap_app':
            print(f"\nSkipping {db_name} (already configured)")
            continue

        # Determine prefix based on database name
        if 'observation' in db_name.lower():
            prefix = 'observation_'
        elif 'weather' in db_name.lower():
            prefix = 'weather_'
        else:
            prefix = f"{db_name}_"

        try:
            results = explore_database(db_config, db_name)

            # Generate registry YAML
            output_file = config_dir / f"timescaledb_tables_{db_name.replace('wakecap_', '')}.yml"
            generate_registry_yaml(results, prefix, str(output_file))

        except Exception as e:
            print(f"\nError exploring {db_name}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print("EXPLORATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
