#!/usr/bin/env python3
"""
Analyze DeviceLocation and DeviceLocationSummary tables in TimescaleDB.
"""

import sys
from pathlib import Path

import yaml
import psycopg2
from psycopg2.extras import RealDictCursor


def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    creds = load_credentials()
    ts = creds['timescaledb']

    conn = psycopg2.connect(
        host=ts['host'],
        port=ts['port'],
        database=ts['database'],
        user=ts['user'],
        password=ts['password'],
        sslmode=ts.get('sslmode', 'require')
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    tables = ['DeviceLocation', 'DeviceLocationSummary']

    for table in tables:
        print("=" * 80)
        print(f"TABLE: {table}")
        print("=" * 80)

        # 1. Table columns and types
        print("\n--- COLUMNS ---")
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{table}'
            ORDER BY ordinal_position;
        """)
        for col in cursor.fetchall():
            print(f"  {col['column_name']:<30} {col['data_type']:<20} NULL={col['is_nullable']}")

        # 2. Primary key
        print("\n--- PRIMARY KEY ---")
        cursor.execute(f"""
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'public."{table}"'::regclass AND i.indisprimary;
        """)
        pk_cols = [r['column_name'] for r in cursor.fetchall()]
        print(f"  {pk_cols if pk_cols else 'No primary key'}")

        # 3. Indexes
        print("\n--- INDEXES ---")
        cursor.execute(f"""
            SELECT
                i.relname as index_name,
                pg_get_indexdef(i.oid) as index_def,
                pg_size_pretty(pg_relation_size(i.oid)) as index_size
            FROM pg_index x
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_class t ON t.oid = x.indrelid
            WHERE t.relname = '{table}' AND t.relnamespace = 'public'::regnamespace
            ORDER BY pg_relation_size(i.oid) DESC;
        """)
        indexes = cursor.fetchall()
        for idx in indexes:
            print(f"  {idx['index_name']} ({idx['index_size']})")
            print(f"    {idx['index_def']}")

        # 4. Hypertable info
        print("\n--- HYPERTABLE INFO ---")
        cursor.execute(f"""
            SELECT
                hypertable_name,
                num_dimensions,
                num_chunks,
                compression_enabled
            FROM timescaledb_information.hypertables
            WHERE hypertable_name = '{table}';
        """)
        ht = cursor.fetchone()
        if ht:
            print(f"  Hypertable: Yes")
            print(f"  Dimensions: {ht['num_dimensions']}")
            print(f"  Chunks: {ht['num_chunks']}")
            print(f"  Compression: {ht['compression_enabled']}")
        else:
            print(f"  Hypertable: No (regular table)")

        # 5. Hypertable dimensions (partitioning)
        print("\n--- PARTITIONING DIMENSIONS ---")
        cursor.execute(f"""
            SELECT
                dimension_number,
                column_name,
                column_type,
                time_interval,
                integer_interval,
                num_partitions
            FROM timescaledb_information.dimensions
            WHERE hypertable_name = '{table}'
            ORDER BY dimension_number;
        """)
        dims = cursor.fetchall()
        time_col = None
        for dim in dims:
            print(f"  Dimension {dim['dimension_number']}: {dim['column_name']} ({dim['column_type']})")
            if dim['time_interval']:
                print(f"    Time interval: {dim['time_interval']}")
            if dim['integer_interval']:
                print(f"    Integer interval: {dim['integer_interval']}")
            if dim['num_partitions']:
                print(f"    Partitions: {dim['num_partitions']}")
            if 'timestamp' in str(dim['column_type']).lower():
                time_col = dim['column_name']

        # 6. Chunk info
        print("\n--- CHUNK INFO (sample) ---")
        try:
            cursor.execute(f"""
                SELECT
                    chunk_name,
                    range_start,
                    range_end,
                    is_compressed
                FROM timescaledb_information.chunks
                WHERE hypertable_name = '{table}'
                ORDER BY range_start DESC
                LIMIT 5;
            """)
            chunks = cursor.fetchall()
            for chunk in chunks:
                compressed = "COMPRESSED" if chunk['is_compressed'] else "uncompressed"
                print(f"  {chunk['chunk_name']}: {chunk['range_start']} to {chunk['range_end']} ({compressed})")
        except Exception as e:
            print(f"  Error getting chunks: {e}")

        # 7. Row count and date range
        print("\n--- ROW COUNT & DATE RANGE ---")
        try:
            if time_col:
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total_rows,
                        MIN("{time_col}") as min_date,
                        MAX("{time_col}") as max_date
                    FROM "public"."{table}";
                """)
            else:
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total_rows,
                        MIN("CreatedAt") as min_date,
                        MAX("CreatedAt") as max_date
                    FROM "public"."{table}";
                """)
            stats = cursor.fetchone()
            print(f"  Total rows: {stats['total_rows']:,}")
            print(f"  Date range: {stats['min_date']} to {stats['max_date']}")
        except Exception as e:
            print(f"  Error: {e}")

        # 8. Data distribution by time (last 6 months)
        print("\n--- DATA DISTRIBUTION (last 6 months) ---")
        try:
            if time_col:
                cursor.execute(f"""
                    SELECT
                        date_trunc('month', "{time_col}") as month,
                        COUNT(*) as row_count
                    FROM "public"."{table}"
                    WHERE "{time_col}" >= NOW() - INTERVAL '6 months'
                    GROUP BY 1
                    ORDER BY 1 DESC;
                """)
            else:
                cursor.execute(f"""
                    SELECT
                        date_trunc('month', "CreatedAt") as month,
                        COUNT(*) as row_count
                    FROM "public"."{table}"
                    WHERE "CreatedAt" >= NOW() - INTERVAL '6 months'
                    GROUP BY 1
                    ORDER BY 1 DESC;
                """)
            dist = cursor.fetchall()
            for d in dist:
                print(f"  {d['month']}: {d['row_count']:,} rows")
        except Exception as e:
            print(f"  Error: {e}")

        # 9. Check for UpdatedAt column and its usage
        print("\n--- WATERMARK COLUMN ANALYSIS ---")
        try:
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = '{table}'
                  AND column_name IN ('UpdatedAt', 'CreatedAt', 'GeneratedAt', 'GeneratedAtDate');
            """)
            wm_cols = [r['column_name'] for r in cursor.fetchall()]
            print(f"  Available timestamp columns: {wm_cols}")

            for col in wm_cols:
                cursor.execute(f"""
                    SELECT
                        MIN("{col}") as min_val,
                        MAX("{col}") as max_val,
                        COUNT(*) FILTER (WHERE "{col}" IS NULL) as null_count
                    FROM "public"."{table}";
                """)
                stats = cursor.fetchone()
                print(f"  {col}: {stats['min_val']} to {stats['max_val']} (nulls: {stats['null_count']})")
        except Exception as e:
            print(f"  Error: {e}")

        print()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
