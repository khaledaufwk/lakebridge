#!/usr/bin/env python3
"""List SQL warehouses and query watermarks."""
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
    
    # List warehouses
    print("SQL Warehouses:")
    warehouses = list(w.warehouses.list())
    warehouse_id = None
    for wh in warehouses:
        print(f"  {wh.name}: {wh.id} (State: {wh.state.value if wh.state else 'N/A'})")
        if wh.state and wh.state.value == "RUNNING":
            warehouse_id = wh.id
        elif not warehouse_id:
            warehouse_id = wh.id
    
    if not warehouse_id:
        print("No warehouse found")
        return
    
    print(f"\nUsing warehouse: {warehouse_id}")
    
    # Query watermarks
    print("\n" + "=" * 100)
    print("TABLE LOAD STATUS - SORTED BY SIZE (ROWS)")
    print("=" * 100)
    
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement="""
                SELECT 
                    source_table,
                    last_load_status,
                    COALESCE(last_load_row_count, 0) as rows_loaded,
                    CASE WHEN watermark_expression IS NOT NULL THEN 'GREATEST' ELSE watermark_column END as watermark_type,
                    last_watermark_timestamp,
                    ROUND(TIMESTAMPDIFF(SECOND, last_load_start_time, COALESCE(last_load_end_time, current_timestamp())) / 60.0, 1) as duration_min,
                    last_load_start_time,
                    last_error_message
                FROM wakecap_prod.migration._timescaledb_watermarks
                WHERE source_system = 'timescaledb'
                ORDER BY COALESCE(last_load_row_count, 0) DESC
            """,
            wait_timeout="60s"
        )
        
        if result.result and result.result.data_array:
            print(f"\n{'Table':<35} {'Status':<12} {'Rows':<12} {'Watermark':<12} {'Duration':<10} {'Start Time'}")
            print("-" * 120)
            
            total_rows = 0
            running_count = 0
            for row in result.result.data_array:
                table = row[0] or ''
                status = row[1] or ''
                rows = int(row[2]) if row[2] else 0
                wm_type = row[3] or ''
                wm_ts = str(row[4])[:19] if row[4] else ''
                duration = row[5] or 0
                start_time = str(row[6])[:19] if row[6] else ''
                error = row[7][:50] if row[7] else ''
                
                total_rows += rows
                if status == 'running':
                    running_count += 1
                    print(f"{table:<35} {status:<12} {rows:<12,} {wm_type:<12} {duration:<10} {start_time} *** RUNNING")
                elif status == 'failed':
                    print(f"{table:<35} {status:<12} {rows:<12,} {wm_type:<12} {duration:<10} {start_time} ERROR: {error}")
                else:
                    print(f"{table:<35} {status:<12} {rows:<12,} {wm_type:<12} {duration:<10} {start_time}")
            
            print("-" * 120)
            print(f"Total rows: {total_rows:,}")
            print(f"Tables currently running: {running_count}")
            
    except Exception as e:
        print(f"SQL Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
