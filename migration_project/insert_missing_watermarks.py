#!/usr/bin/env python3
"""Insert missing watermarks for DeviceLocation tables."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient

def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Use SQL warehouse
    warehouses = list(w.warehouses.list())
    serverless = None
    for wh in warehouses:
        if "serverless" in wh.name.lower():
            serverless = wh
            break

    if not serverless:
        print("No serverless warehouse found")
        return 1

    print("Inserting missing watermarks for DeviceLocation tables...")
    print("=" * 80)

    # Get max timestamps from both tables
    queries = {
        "DeviceLocation": """
            SELECT MAX(GeneratedAt) as max_ts, COUNT(*) as cnt
            FROM wakecap_prod.raw.timescale_devicelocation
        """,
        "DeviceLocationSummary": """
            SELECT MAX(GeneratedAt) as max_ts, COUNT(*) as cnt
            FROM wakecap_prod.raw.timescale_devicelocationsummary
        """
    }

    watermarks = {}
    for table, query in queries.items():
        result = w.statement_execution.execute_statement(
            warehouse_id=serverless.id,
            statement=query,
            wait_timeout="50s"
        )
        if result.result and result.result.data_array:
            row = result.result.data_array[0]
            watermarks[table] = {"max_ts": row[0], "count": row[1]}
            print(f"{table}: max_ts={row[0]}, count={row[1]}")

    print("\nInserting watermarks...")

    # Insert watermarks
    for table, data in watermarks.items():
        insert_sql = f"""
        INSERT INTO wakecap_prod.migration._timescaledb_watermarks (
            source_system, source_schema, source_table,
            watermark_column, watermark_type, watermark_expression,
            last_watermark_value, last_watermark_timestamp, last_watermark_bigint,
            last_load_start_time, last_load_end_time,
            last_load_status, last_load_row_count, last_error_message,
            pipeline_id, pipeline_run_id,
            created_at, updated_at, loader_name
        ) VALUES (
            'timescaledb', 'public', '{table}',
            'GeneratedAt', 'timestamp', NULL,
            '{data["max_ts"]}', TIMESTAMP'{data["max_ts"]}', NULL,
            current_timestamp(), current_timestamp(),
            'success', {data["count"]}, NULL,
            '/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation', NULL,
            current_timestamp(), current_timestamp(), 'timescaledb_loader'
        )
        """

        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=serverless.id,
                statement=insert_sql,
                wait_timeout="30s"
            )
            print(f"  {table}: Inserted successfully")
        except Exception as e:
            print(f"  {table}: Error - {e}")

    # Verify
    print("\nVerifying watermarks...")
    verify_sql = """
    SELECT source_table, watermark_column, last_watermark_timestamp, last_load_row_count
    FROM wakecap_prod.migration._timescaledb_watermarks
    WHERE source_table IN ('DeviceLocation', 'DeviceLocationSummary')
    """
    result = w.statement_execution.execute_statement(
        warehouse_id=serverless.id,
        statement=verify_sql,
        wait_timeout="30s"
    )
    if result.result and result.result.data_array:
        for row in result.result.data_array:
            print(f"  {row[0]}: {row[1]} = {row[2]} ({row[3]} rows)")
    else:
        print("  No watermarks found after insert!")

if __name__ == "__main__":
    main()
