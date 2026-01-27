#!/usr/bin/env python3
"""List tables in SQL Server."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
import pymssql


def load_credentials():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    creds = load_credentials()
    mssql = creds['mssql']

    conn = pymssql.connect(
        server=mssql['server'],
        user=mssql['user'],
        password=mssql['password'],
        database=mssql['database'],
        port=mssql.get('port', 1433)
    )
    cursor = conn.cursor()

    # List all tables
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME LIKE '%Worker%'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)

    print("Tables containing 'Worker':")
    for row in cursor.fetchall():
        print(f"  {row[0]}.{row[1]}")

    # Also check for FactWorkersHistory variations
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME LIKE '%Fact%'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)

    print("\nTables containing 'Fact':")
    for row in cursor.fetchall():
        print(f"  {row[0]}.{row[1]}")

    conn.close()


if __name__ == "__main__":
    main()
