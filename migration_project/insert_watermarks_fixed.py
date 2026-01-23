#!/usr/bin/env python3
"""Insert missing watermarks for DeviceLocation tables with correct schema."""
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

    print("\nInserting watermarks using MERGE...")

    # Use MERGE to insert watermarks (matching loader's approach)
    for table, data in watermarks.items():
        merge_sql = f"""
        MERGE INTO wakecap_prod.migration._timescaledb_watermarks AS target
        USING (
            SELECT
                'timescaledb' AS source_system,
                'public' AS source_schema,
                '{table}' AS source_table,
                'GeneratedAt' AS watermark_column,
                'timestamp' AS watermark_type,
                '{data["max_ts"]}' AS last_watermark_value,
                TIMESTAMP'{data["max_ts"]}' AS last_watermark_timestamp,
                CAST(NULL AS BIGINT) AS last_watermark_bigint,
                current_timestamp() AS last_load_start_time,
                current_timestamp() AS last_load_end_time,
                'success' AS last_load_status,
                CAST({data["count"]} AS BIGINT) AS last_load_row_count,
                CAST(NULL AS STRING) AS last_error_message,
                '/migration_project/pipelines/timescaledb/notebooks/bronze_loader_devicelocation' AS pipeline_id,
                CAST(NULL AS STRING) AS pipeline_run_id,
                current_timestamp() AS created_at,
                current_timestamp() AS updated_at,
                'timescaledb_loader' AS created_by
        ) AS source
        ON target.source_system = source.source_system
           AND target.source_schema = source.source_schema
           AND target.source_table = source.source_table
        WHEN MATCHED THEN UPDATE SET
            target.watermark_column = source.watermark_column,
            target.watermark_type = source.watermark_type,
            target.last_watermark_value = source.last_watermark_value,
            target.last_watermark_timestamp = source.last_watermark_timestamp,
            target.last_watermark_bigint = source.last_watermark_bigint,
            target.last_load_start_time = source.last_load_start_time,
            target.last_load_end_time = source.last_load_end_time,
            target.last_load_status = source.last_load_status,
            target.last_load_row_count = source.last_load_row_count,
            target.last_error_message = source.last_error_message,
            target.pipeline_id = source.pipeline_id,
            target.pipeline_run_id = source.pipeline_run_id,
            target.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            source_system, source_schema, source_table,
            watermark_column, watermark_type,
            last_watermark_value, last_watermark_timestamp, last_watermark_bigint,
            last_load_start_time, last_load_end_time,
            last_load_status, last_load_row_count, last_error_message,
            pipeline_id, pipeline_run_id,
            created_at, updated_at, created_by
        ) VALUES (
            source.source_system, source.source_schema, source.source_table,
            source.watermark_column, source.watermark_type,
            source.last_watermark_value, source.last_watermark_timestamp, source.last_watermark_bigint,
            source.last_load_start_time, source.last_load_end_time,
            source.last_load_status, source.last_load_row_count, source.last_error_message,
            source.pipeline_id, source.pipeline_run_id,
            source.created_at, source.updated_at, source.created_by
        )
        """

        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=serverless.id,
                statement=merge_sql,
                wait_timeout="30s"
            )
            print(f"  {table}: MERGE completed successfully")
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
