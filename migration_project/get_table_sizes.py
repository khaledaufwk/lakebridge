#!/usr/bin/env python3
"""Get table sizes from TimescaleDB source for comparison."""
import yaml
from pathlib import Path
from databricks.sdk import WorkspaceClient

def main():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)
    
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    
    # Find running warehouse
    warehouses = list(w.warehouses.list())
    warehouse_id = None
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            warehouse_id = wh.id
            break
    
    if not warehouse_id:
        print("No running warehouse")
        return
        
    # Get all table info sorted by size
    print("=" * 140)
    print("ALL TABLES - SORTED BY SIZE (DESCENDING)")
    print("=" * 140)
    print(f"\n{'#':<4} {'Table':<40} {'Status':<10} {'Rows':<15} {'Load Type':<15} {'Watermark Col':<15} {'Last Load'}")
    print("-" * 140)
    
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="""
            SELECT 
                source_table,
                last_load_status,
                COALESCE(last_load_row_count, 0) as rows,
                CASE 
                    WHEN last_watermark_timestamp IS NOT NULL THEN 'INCREMENTAL'
                    ELSE 'FULL'
                END as load_type,
                COALESCE(watermark_expression, watermark_column) as watermark_col,
                last_load_end_time
            FROM wakecap_prod.migration._timescaledb_watermarks
            WHERE source_system = 'timescaledb'
            ORDER BY COALESCE(last_load_row_count, 0) DESC
        """,
        wait_timeout="30s"
    )
    
    if result.result and result.result.data_array:
        total_rows = 0
        incremental_count = 0
        full_count = 0
        failed_tables = []
        
        for i, row in enumerate(result.result.data_array, 1):
            table = row[0] or ''
            status = row[1] or ''
            rows = int(row[2]) if row[2] else 0
            load_type = row[3] or ''
            wm_col = row[4] or ''
            last_load = str(row[5])[:19] if row[5] else ''
            
            total_rows += rows
            if load_type == 'INCREMENTAL':
                incremental_count += 1
            else:
                full_count += 1
            
            if status == 'failed':
                failed_tables.append(table)
                status_display = "FAILED"
            elif status == 'success':
                status_display = "OK"
            else:
                status_display = status.upper() if status else "?"
            
            print(f"{i:<4} {table:<40} {status_display:<10} {rows:<15,} {load_type:<15} {wm_col:<15} {last_load}")
        
        print("-" * 140)
        print(f"\nTOTAL: {len(result.result.data_array)} tables, {total_rows:,} rows")
        print(f"Incremental: {incremental_count} tables | Full Load: {full_count} tables")
        
        if failed_tables:
            print(f"\nFAILED TABLES ({len(failed_tables)}):")
            for t in failed_tables:
                print(f"  - {t}")

if __name__ == "__main__":
    main()
