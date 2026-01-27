#!/usr/bin/env python3
"""Check SQL Server date range."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
import pymssql


def main():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        creds = yaml.safe_load(f)

    mssql = creds['mssql']
    conn = pymssql.connect(
        server=mssql['server'],
        user=mssql['user'],
        password=mssql['password'],
        database=mssql['database'],
        port=mssql.get('port', 1433)
    )
    cursor = conn.cursor()

    print("SQL Server FactWorkersHistory Info:")
    print("=" * 50)

    # Date range
    cursor.execute("""
        SELECT MIN(ShiftLocalDate), MAX(ShiftLocalDate)
        FROM dbo.FactWorkersHistory WITH (NOLOCK)
    """)
    row = cursor.fetchone()
    print(f"\nDate Range: {row[0]} to {row[1]}")

    # Total count
    cursor.execute("""
        SELECT COUNT(*)
        FROM dbo.FactWorkersHistory WITH (NOLOCK)
    """)
    print(f"Total Rows: {cursor.fetchone()[0]:,}")

    # Latest dates
    cursor.execute("""
        SELECT TOP 10 CAST(ShiftLocalDate AS DATE) as dt, COUNT(*) as cnt
        FROM dbo.FactWorkersHistory WITH (NOLOCK)
        GROUP BY CAST(ShiftLocalDate AS DATE)
        ORDER BY dt DESC
    """)
    print("\nLatest 10 dates:")
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]:,}")

    conn.close()


if __name__ == "__main__":
    main()
