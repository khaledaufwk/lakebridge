#!/usr/bin/env python3
"""
Query TimescaleDB to get all tables sorted by storage size.
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


def format_size(size_bytes):
    """Format bytes to human readable size."""
    if size_bytes is None:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:,.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:,.1f} PB"


def main():
    print("=" * 80)
    print("TimescaleDB Table Sizes")
    print("=" * 80)

    # Load credentials
    creds = load_credentials()
    ts = creds['timescaledb']

    print(f"\nConnecting to {ts['host']}:{ts['port']}/{ts['database']}...")

    # Connect to TimescaleDB
    conn = psycopg2.connect(
        host=ts['host'],
        port=ts['port'],
        database=ts['database'],
        user=ts['user'],
        password=ts['password'],
        sslmode=ts.get('sslmode', 'require')
    )

    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Query to get table sizes (works for both regular and hypertables)
    query = """
    WITH table_sizes AS (
        SELECT
            schemaname,
            tablename,
            pg_total_relation_size(schemaname || '.' || quote_ident(tablename)) as total_size,
            pg_relation_size(schemaname || '.' || quote_ident(tablename)) as table_size,
            pg_indexes_size(schemaname || '.' || quote_ident(tablename)) as index_size
        FROM pg_tables
        WHERE schemaname = 'public'
    ),
    row_counts AS (
        SELECT
            relname as tablename,
            n_live_tup as row_estimate
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
    )
    SELECT
        ts.tablename,
        ts.total_size,
        ts.table_size,
        ts.index_size,
        COALESCE(rc.row_estimate, 0) as row_estimate
    FROM table_sizes ts
    LEFT JOIN row_counts rc ON ts.tablename = rc.tablename
    ORDER BY ts.total_size DESC;
    """

    print("Querying table sizes...\n")
    cursor.execute(query)
    results = cursor.fetchall()

    # Print results
    print(f"{'#':<4} {'Table Name':<45} {'Total Size':>12} {'Table':>10} {'Index':>10} {'Rows (est)':>15}")
    print("-" * 100)

    total_size = 0
    total_rows = 0

    for i, row in enumerate(results, 1):
        table_name = row['tablename']
        size = row['total_size'] or 0
        table_size = row['table_size'] or 0
        index_size = row['index_size'] or 0
        rows = row['row_estimate'] or 0

        total_size += size
        total_rows += rows

        print(f"{i:<4} {table_name:<45} {format_size(size):>12} {format_size(table_size):>10} {format_size(index_size):>10} {rows:>15,}")

    print("-" * 100)
    print(f"{'TOTAL':<50} {format_size(total_size):>12} {'':<10} {'':<10} {total_rows:>15,}")

    # Also check for hypertables specifically
    print("\n" + "=" * 80)
    print("Hypertables (TimescaleDB compressed tables)")
    print("=" * 80)

    hypertable_query = """
    SELECT
        hypertable_name,
        hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass) as total_size,
        pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) as size_pretty
    FROM timescaledb_information.hypertables
    WHERE hypertable_schema = 'public'
    ORDER BY hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass) DESC;
    """

    try:
        cursor.execute(hypertable_query)
        hypertables = cursor.fetchall()

        if hypertables:
            print(f"\n{'#':<4} {'Hypertable Name':<45} {'Total Size':>15}")
            print("-" * 70)
            for i, row in enumerate(hypertables, 1):
                print(f"{i:<4} {row['hypertable_name']:<45} {row['size_pretty']:>15}")
        else:
            print("\nNo hypertables found.")
    except Exception as e:
        print(f"\nCould not query hypertables: {e}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 80)
    print(f"Total tables: {len(results)}")
    print(f"Total size: {format_size(total_size)}")
    print("=" * 80)


if __name__ == "__main__":
    main()
