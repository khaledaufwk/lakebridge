"""
Generate complete table configuration for WakeCapDW ingestion.
Parses SQL files to extract primary keys and watermark columns.
"""

import os
import re
import json
from pathlib import Path

def parse_sql_file(filepath):
    """Parse a SQL CREATE TABLE file to extract metadata."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Extract schema and table name from filename
    filename = os.path.basename(filepath).replace('.sql', '')
    parts = filename.split('.', 1)
    schema = parts[0] if len(parts) > 1 else 'dbo'
    table = parts[1] if len(parts) > 1 else parts[0]

    # Find primary key from comment
    pk_match = re.search(r'--\s*Primary Key:\s*\(\[?([^\]\)]+)\]?\)', content)
    primary_key = None
    if pk_match:
        # Clean up the PK columns
        pk_raw = pk_match.group(1)
        pk_cols = [col.strip().strip('[]') for col in pk_raw.split(',')]
        primary_key = ', '.join(pk_cols)
    else:
        # Try to find IDENTITY column as PK
        identity_match = re.search(r'\[(\w+)\]\s+\w+\s+IDENTITY', content)
        if identity_match:
            primary_key = identity_match.group(1)

    # Check for WatermarkUTC column
    has_watermark = 'WatermarkUTC' in content or 'watermarkutc' in content.lower()

    # Check for geometry/geography columns
    has_geometry = 'geography' in content.lower() or 'geometry' in content.lower()

    # Determine if it's a staging table (wc2021_, wc2023_, etc.)
    is_staging = schema == 'stg' and ('wc2021' in table.lower() or 'wc2023' in table.lower())

    # Skip system/test tables
    skip_table = any(x in table.lower() for x in ['__refactorlog', '_copytable_test', 'crew2', 'tmp_'])

    return {
        'source_schema': schema,
        'source_table': table,
        'primary_key_columns': primary_key,
        'watermark_column': 'WatermarkUTC' if has_watermark else None,
        'is_full_load': not has_watermark,
        'has_geometry': has_geometry,
        'is_staging': is_staging,
        'skip': skip_table
    }


def categorize_table(table_info):
    """Categorize a table based on its name and schema."""
    schema = table_info['source_schema']
    table = table_info['source_table']

    if table_info['skip']:
        return 'skip'

    if schema == 'security':
        return 'security'

    if schema == 'stg':
        if 'wc2021' in table.lower() or 'wc2023' in table.lower():
            return 'staging_sources'
        return 'staging'

    if schema == 'mrg':
        return 'merge'

    if table.startswith('Fact'):
        return 'facts'

    if 'Assignment' in table:
        return 'assignments'

    return 'dimensions'


def main():
    # Find all SQL table files
    source_dir = Path(__file__).parent.parent.parent / 'source_sql' / 'tables'

    tables_by_category = {
        'dimensions': [],
        'facts': [],
        'assignments': [],
        'staging': [],
        'staging_sources': [],
        'merge': [],
        'security': [],
        'skip': []
    }

    for sql_file in sorted(source_dir.glob('*.sql')):
        table_info = parse_sql_file(sql_file)
        category = categorize_table(table_info)

        if category != 'skip':
            # Clean up for JSON output
            entry = {
                'source_schema': table_info['source_schema'],
                'source_table': table_info['source_table'],
                'primary_key_columns': table_info['primary_key_columns'] or f"{table_info['source_table']}ID",
                'watermark_column': table_info['watermark_column'],
                'is_full_load': table_info['is_full_load']
            }

            if table_info['has_geometry']:
                entry['has_geometry'] = True
                entry['note'] = 'Contains geography/geometry columns - may need special handling'

            tables_by_category[category].append(entry)
        else:
            tables_by_category['skip'].append({
                'source_schema': table_info['source_schema'],
                'source_table': table_info['source_table'],
                'reason': 'System/test table'
            })

    # Build final config
    config = {
        'metadata': {
            'description': 'Complete WakeCapDW Table Configuration for Databricks Incremental Load',
            'source_system': 'wakecapdw',
            'source_database': 'WakeCapDW_20251215',
            'source_server': 'wakecap24.database.windows.net',
            'target_catalog': 'wakecap_prod',
            'target_schema': 'raw',
            'total_tables': sum(len(v) for k, v in tables_by_category.items() if k != 'skip'),
            'skipped_tables': len(tables_by_category['skip'])
        },
        'connection': {
            'secret_scope': 'akv-wakecap24',
            'source_driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'source_user': 'wakecap_reader',
            'source_pwd_secret_name': 'sqlserver-wakecap-password',
            'source_url': 'jdbc:sqlserver://wakecap24.database.windows.net:1433'
        },
        'tables': {k: v for k, v in tables_by_category.items() if k != 'skip'},
        'skipped_tables': tables_by_category['skip']
    }

    # Print summary
    print("=" * 60)
    print("WakeCapDW Table Configuration Summary")
    print("=" * 60)
    for category, tables in tables_by_category.items():
        if category != 'skip':
            print(f"{category}: {len(tables)} tables")
    print(f"skipped: {len(tables_by_category['skip'])} tables")
    print(f"TOTAL: {config['metadata']['total_tables']} tables to ingest")
    print("=" * 60)

    # Write to JSON
    output_path = Path(__file__).parent.parent / 'config' / 'wakecapdw_tables_complete.json'
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"\nConfiguration written to: {output_path}")

    return config


if __name__ == '__main__':
    main()
