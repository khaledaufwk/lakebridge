"""
Generate TimescaleDB Table Registry
===================================
Connects to TimescaleDB and generates a registry YAML based on actual tables.
Uses a generic, parameterized pattern for all tables with PK and incremental load.
"""

import yaml
from pathlib import Path
import psycopg2
from datetime import datetime

def main():
    # Load credentials
    creds_path = Path.home() / '.databricks/labs/lakebridge/.credentials.yml'
    with open(creds_path) as f:
        creds = yaml.safe_load(f)['timescaledb']

    print(f"Connecting to {creds['host']}:{creds['port']}/{creds['database']}...")

    conn = psycopg2.connect(
        host=creds['host'],
        port=creds.get('port', 5432),
        database=creds['database'],
        user=creds['user'],
        password=creds['password'],
        sslmode=creds.get('sslmode', 'require')
    )
    cursor = conn.cursor()

    # Step 1: Get all public schema tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema = 'public'
        ORDER BY table_name
    """)
    all_tables = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(all_tables)} tables in public schema")

    # Filter out backups, templates, and system tables
    exclude_patterns = ['_BK_', '_backup_', '_TempCache_', 'UploadTemplate', '_bk_']
    exclude_exact = ['spatial_ref_sys', 'SeedTracker', 'MigrationLog', 'MigrationRecordFailLog',
                     'equipment_csv', 'people_to_demobilize', 'people_to_demoblize_2',
                     'ryias_cameras_mappings', 'trade_mapping_ryias_update', 'jafurah_phase',
                     '__EFMigrationsHistory']

    filtered_tables = []
    for t in all_tables:
        skip = False
        for pattern in exclude_patterns:
            if pattern in t:
                skip = True
                break
        if t in exclude_exact:
            skip = True
        if not skip:
            filtered_tables.append(t)

    print(f"After filtering: {len(filtered_tables)} tables")

    # Step 2: Get primary keys for each table
    def get_pk(table_name):
        cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """, (table_name,))
        cols = [row[0] for row in cursor.fetchall()]
        return cols if cols else ['Id']

    # Step 3: Get watermark column for each table (for incremental load)
    def get_watermark(table_name):
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name IN ('WatermarkUTC', 'UpdatedAt', 'ModifiedAt', 'CreatedAt')
            ORDER BY
                CASE column_name
                    WHEN 'WatermarkUTC' THEN 1
                    WHEN 'UpdatedAt' THEN 2
                    WHEN 'ModifiedAt' THEN 3
                    WHEN 'CreatedAt' THEN 4
                END
            LIMIT 1
        """, (table_name,))
        row = cursor.fetchone()
        return row[0] if row else 'CreatedAt'

    # Step 4: Check for geometry columns
    def has_geometry(table_name):
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND (udt_name = 'geometry' OR data_type = 'USER-DEFINED')
        """, (table_name,))
        return cursor.fetchone()[0] > 0

    # Build registry - generic pattern, no categorization
    registry = {
        'registry_version': '2.0',
        'source_system': 'timescaledb',
        'source_database': creds['database'],
        'target_catalog': 'wakecap_prod',
        'target_schema': 'raw_timescaledb',
        'generated_date': datetime.now().strftime('%Y-%m-%d'),
        'description': 'TimescaleDB bronze layer - incremental load with watermark tracking',

        # Default configuration for all tables
        'defaults': {
            'source_schema': 'public',
            'watermark_type': 'timestamp',
            'load_mode': 'incremental',
            'merge_strategy': 'upsert',
            'fetch_size': 10000,
            'batch_size': 100000,
            'enabled': True
        },

        # Connection configuration (parameterized)
        'connection': {
            'secret_scope': 'wakecap-timescale',
            'driver': 'org.postgresql.Driver',
            'ssl_mode': 'require'
        },

        # Metadata columns added to all tables
        'metadata_columns': [
            {'name': '_loaded_at', 'expression': 'current_timestamp()'},
            {'name': '_source_system', 'expression': "'timescaledb'"},
            {'name': '_source_table', 'expression': 'source_table_name'}
        ],

        'tables': []
    }

    print("Processing tables...")
    for i, table in enumerate(filtered_tables):
        if (i + 1) % 20 == 0:
            print(f"  Processed {i + 1}/{len(filtered_tables)} tables...")

        pk_cols = get_pk(table)
        wm_col = get_watermark(table)
        has_geom = has_geometry(table)

        entry = {
            'source_table': table,
            'primary_key_columns': pk_cols,
            'watermark_column': wm_col,
        }

        # Geometry tables need special column mapping
        if has_geom:
            entry['has_geometry'] = True
            entry['geometry_handling'] = 'ST_AsText'

        registry['tables'].append(entry)

    cursor.close()
    conn.close()

    # Sort tables alphabetically
    registry['tables'].sort(key=lambda x: x['source_table'])

    # Write to file
    output_path = Path(__file__).parent / 'config' / 'timescaledb_tables.yml'

    with open(output_path, 'w') as f:
        f.write('# TimescaleDB Table Registry\n')
        f.write('# ================================================\n')
        f.write(f'# Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write(f'# Source: {creds["database"]} database\n')
        f.write(f'# Total tables: {len(registry["tables"])}\n')
        f.write('#\n')
        f.write('# Pattern: Generic incremental load with PK-based upsert\n')
        f.write('# All tables use watermark column for incremental extraction\n')
        f.write('# ================================================\n\n')
        yaml.dump(registry, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"\n{'='*60}")
    print(f"Registry generated: {output_path}")
    print(f"{'='*60}")
    print(f"Total tables: {len(registry['tables'])}")

    # Count geometry tables
    geom_tables = [t for t in registry['tables'] if t.get('has_geometry')]
    print(f"Tables with geometry: {len(geom_tables)}")

    print(f"\nSample entries:")
    for t in registry['tables'][:5]:
        print(f"  - {t['source_table']} (PK: {t['primary_key_columns']}, WM: {t['watermark_column']})")

    return registry


if __name__ == "__main__":
    main()
