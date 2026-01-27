"""
TimescaleDB Metadata Gathering Script
Connects to wakecap_app database and collects comprehensive metadata
"""

import psycopg2
import json
from datetime import datetime

# Connection parameters
conn_params = {
    'host': 'tsdb-1c1f39cf-wakecap-production.a.timescaledb.io',
    'port': 21231,
    'database': 'wakecap_app',
    'user': 'tsdbdatateam',
    'password': 'hbcgr8u6u3prsgpe',
    'sslmode': 'require'
}

# Dictionary to store all results
metadata = {
    'gathered_at': datetime.now().isoformat(),
    'connection': {
        'host': conn_params['host'],
        'database': conn_params['database']
    },
    'tables_with_sizes': [],
    'primary_keys': [],
    'indexes': [],
    'hypertables': [],
    'timestamp_columns': [],
    'watermark_index_check': []
}

def execute_query(conn, query, description):
    """Execute a query and return results with column names - handles errors gracefully"""
    print(f"\n{'='*60}")
    print(f"Executing: {description}")
    print('='*60)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        print(f"Retrieved {len(results)} rows")
        cursor.close()
        return results
    except Exception as e:
        print(f"Error: {e}")
        # Rollback to clear the error state
        conn.rollback()
        return []

# Connect and gather metadata
print("Connecting to TimescaleDB...")
try:
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True  # Prevent transaction issues
    print("Connected successfully!")

    # 1. All tables with row counts and sizes (safer version)
    query1 = """
    SELECT
        n.nspname as schemaname,
        c.relname as tablename,
        pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
        pg_total_relation_size(c.oid) as size_bytes,
        c.reltuples::bigint as estimated_rows
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
        AND c.relkind = 'r'
    ORDER BY pg_total_relation_size(c.oid) DESC;
    """
    metadata['tables_with_sizes'] = execute_query(conn, query1, "Tables with row counts and sizes")

    # 2. Primary keys for all tables
    query2 = """
    SELECT
        tc.table_name,
        string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as pk_columns
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
    GROUP BY tc.table_name
    ORDER BY tc.table_name;
    """
    metadata['primary_keys'] = execute_query(conn, query2, "Primary keys for all tables")

    # 3. All indexes with details
    query3 = """
    SELECT
        i.schemaname,
        i.tablename,
        i.indexname,
        i.indexdef,
        pg_size_pretty(pg_relation_size(c.oid)) as index_size
    FROM pg_indexes i
    JOIN pg_class c ON c.relname = i.indexname
    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
    WHERE i.schemaname = 'public'
    ORDER BY i.tablename, i.indexname;
    """
    metadata['indexes'] = execute_query(conn, query3, "All indexes with details")

    # 4. Hypertables info
    query4 = """
    SELECT
        hypertable_schema,
        hypertable_name,
        num_chunks,
        compression_enabled,
        total_chunks
    FROM timescaledb_information.hypertables;
    """
    metadata['hypertables'] = execute_query(conn, query4, "Hypertables info")

    # 5. Columns with timestamps (potential watermark columns)
    query5 = """
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
        AND (column_name ILIKE '%created%' OR column_name ILIKE '%updated%' OR column_name ILIKE '%generated%' OR column_name ILIKE '%timestamp%' OR column_name ILIKE '%date%' OR column_name ILIKE '%at%')
        AND data_type IN ('timestamp without time zone', 'timestamp with time zone', 'date')
    ORDER BY table_name, column_name;
    """
    metadata['timestamp_columns'] = execute_query(conn, query5, "Columns with timestamps (potential watermark columns)")

    # 6. Check for missing indexes on watermark columns
    query6 = """
    SELECT
        t.table_name,
        c.column_name as watermark_column,
        CASE WHEN idx.indexname IS NOT NULL THEN 'YES' ELSE 'NO' END as has_index,
        idx.indexname
    FROM information_schema.tables t
    JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
    LEFT JOIN pg_indexes idx ON idx.tablename = t.table_name
        AND idx.schemaname = 'public'
        AND idx.indexdef LIKE '%' || c.column_name || '%'
    WHERE t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
        AND (c.column_name IN ('UpdatedAt', 'CreatedAt', 'GeneratedAt', 'ModifiedAt'))
    ORDER BY t.table_name, c.column_name;
    """
    metadata['watermark_index_check'] = execute_query(conn, query6, "Check for missing indexes on watermark columns")

    # 7. Get all columns for each table (bonus - useful for migration)
    query7 = """
    SELECT
        table_name,
        column_name,
        ordinal_position,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable,
        column_default
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
    """
    metadata['all_columns'] = execute_query(conn, query7, "All columns for each table")

    # 8. Foreign keys
    query8 = """
    SELECT
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        tc.constraint_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
    ORDER BY tc.table_name;
    """
    metadata['foreign_keys'] = execute_query(conn, query8, "Foreign keys")

    conn.close()
    print("\n" + "="*60)
    print("Connection closed successfully!")

except Exception as e:
    print(f"Connection error: {e}")
    raise

# Print formatted results
print("\n" + "="*80)
print("COMPREHENSIVE TIMESCALEDB METADATA REPORT")
print("="*80)

print(f"\nGathered at: {metadata['gathered_at']}")
print(f"Database: {metadata['connection']['database']} @ {metadata['connection']['host']}")

print("\n" + "-"*80)
print("1. TABLES WITH ROW COUNTS AND SIZES")
print("-"*80)
print(f"{'Table Name':<50} {'Size':<15} {'Est. Rows':>15}")
print("-"*80)
for t in metadata['tables_with_sizes']:
    rows = t.get('estimated_rows', 0) or 0
    print(f"{t['tablename']:<50} {t['total_size']:<15} {rows:>15,}")

print(f"\nTotal tables: {len(metadata['tables_with_sizes'])}")
total_size = sum(t.get('size_bytes', 0) or 0 for t in metadata['tables_with_sizes'])
print(f"Total size: {total_size / (1024**3):.2f} GB")

print("\n" + "-"*80)
print("2. PRIMARY KEYS")
print("-"*80)
print(f"{'Table Name':<50} {'PK Columns':<30}")
print("-"*80)
for pk in metadata['primary_keys']:
    print(f"{pk['table_name']:<50} {pk['pk_columns']:<30}")

print(f"\nTables with primary keys: {len(metadata['primary_keys'])}")

print("\n" + "-"*80)
print("3. INDEXES (Summary by table)")
print("-"*80)
# Group indexes by table
index_by_table = {}
for idx in metadata['indexes']:
    table = idx['tablename']
    if table not in index_by_table:
        index_by_table[table] = []
    index_by_table[table].append(idx)

for table, indexes in sorted(index_by_table.items()):
    print(f"\n{table} ({len(indexes)} indexes):")
    for idx in indexes:
        print(f"  - {idx['indexname']} ({idx['index_size']})")

print(f"\nTotal indexes: {len(metadata['indexes'])}")

print("\n" + "-"*80)
print("4. HYPERTABLES (TimescaleDB)")
print("-"*80)
if metadata['hypertables']:
    print(f"{'Hypertable Name':<50} {'Chunks':<10} {'Total':<10} {'Compression':<15}")
    print("-"*85)
    for ht in metadata['hypertables']:
        comp = 'Enabled' if ht.get('compression_enabled') else 'Disabled'
        num_chunks = ht.get('num_chunks', 0) or 0
        total_chunks = ht.get('total_chunks', 0) or 0
        print(f"{ht['hypertable_name']:<50} {num_chunks:<10} {total_chunks:<10} {comp:<15}")
else:
    print("No hypertables found")

print("\n" + "-"*80)
print("5. TIMESTAMP COLUMNS (Potential Watermarks)")
print("-"*80)
# Group by table
ts_by_table = {}
for col in metadata['timestamp_columns']:
    table = col['table_name']
    if table not in ts_by_table:
        ts_by_table[table] = []
    ts_by_table[table].append(col)

for table, cols in sorted(ts_by_table.items()):
    col_info = [f"{c['column_name']} ({c['data_type'][:10]})" for c in cols]
    print(f"{table}:")
    for ci in col_info:
        print(f"  - {ci}")

print(f"\nTables with timestamp columns: {len(ts_by_table)}")

print("\n" + "-"*80)
print("6. WATERMARK INDEX CHECK")
print("-"*80)
print(f"{'Table Name':<50} {'Column':<20} {'Has Index':<10}")
print("-"*80)
missing_indexes = []
for check in metadata['watermark_index_check']:
    print(f"{check['table_name']:<50} {check['watermark_column']:<20} {check['has_index']:<10}")
    if check['has_index'] == 'NO':
        missing_indexes.append(f"{check['table_name']}.{check['watermark_column']}")

if missing_indexes:
    print(f"\nWARNING: {len(missing_indexes)} watermark columns missing indexes:")
    for mi in missing_indexes:
        print(f"  - {mi}")

print("\n" + "-"*80)
print("7. FOREIGN KEYS")
print("-"*80)
if metadata.get('foreign_keys'):
    fk_by_table = {}
    for fk in metadata['foreign_keys']:
        table = fk['table_name']
        if table not in fk_by_table:
            fk_by_table[table] = []
        fk_by_table[table].append(fk)

    for table, fks in sorted(fk_by_table.items()):
        print(f"\n{table}:")
        for fk in fks:
            print(f"  - {fk['column_name']} -> {fk['foreign_table_name']}.{fk['foreign_column_name']}")

    print(f"\nTotal foreign key constraints: {len(metadata['foreign_keys'])}")
else:
    print("No foreign keys found")

# Save to JSON file
output_file = 'C:/Users/khaledadmin/lakebridge/migration_project/timescaledb_metadata.json'
with open(output_file, 'w') as f:
    json.dump(metadata, f, indent=2, default=str)
print(f"\n\nMetadata saved to: {output_file}")

print("\n" + "="*80)
print("METADATA GATHERING COMPLETE")
print("="*80)
