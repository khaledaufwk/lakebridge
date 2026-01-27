#!/usr/bin/env python3
"""Query watermarks and table stats."""
import yaml
from pathlib import Path
import time
from databricks.sdk import WorkspaceClient

def main():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)
    
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    
    # Find serverless warehouse
    warehouses = list(w.warehouses.list())
    warehouse_id = None
    for wh in warehouses:
        if "serverless" in wh.name.lower():
            warehouse_id = wh.id
            print(f"Using warehouse: {wh.name} ({wh.id}) - State: {wh.state.value if wh.state else 'N/A'}")
            
            # Start warehouse if stopped
            if wh.state and wh.state.value == "STOPPED":
                print("Starting warehouse...")
                w.warehouses.start(warehouse_id)
                for _ in range(30):  # Wait up to 5 minutes
                    time.sleep(10)
                    wh_status = w.warehouses.get(warehouse_id)
                    print(f"  Warehouse state: {wh_status.state.value if wh_status.state else 'N/A'}")
                    if wh_status.state and wh_status.state.value == "RUNNING":
                        break
            break
    
    if not warehouse_id:
        print("No serverless warehouse found")
        return
    
    # Query watermarks
    print("\n" + "=" * 120)
    print("TABLE LOAD STATUS - SORTED BY SIZE (ROWS)")
    print("=" * 120)
    
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
        wait_timeout="50s"
    )
    
    if result.result and result.result.data_array:
        print(f"\n{'Table':<35} {'Status':<12} {'Rows':<12} {'Watermark':<12} {'Duration':<10} {'Start Time':<20} Notes")
        print("-" * 140)
        
        total_rows = 0
        running_count = 0
        incremental_tables = []
        full_load_tables = []
        
        for row in result.result.data_array:
            table = row[0] or ''
            status = row[1] or ''
            rows = int(row[2]) if row[2] else 0
            wm_type = row[3] or ''
            wm_ts = str(row[4])[:19] if row[4] else 'None'
            duration = float(row[5]) if row[5] else 0
            start_time = str(row[6])[:19] if row[6] else ''
            error = row[7][:40] if row[7] else ''
            
            total_rows += rows
            
            # Track incremental vs full
            if wm_ts and wm_ts != 'None':
                incremental_tables.append((table, rows, wm_type))
            else:
                full_load_tables.append((table, rows, wm_type))
            
            notes = ""
            if status == 'running':
                running_count += 1
                notes = "*** RUNNING ***"
            elif status == 'failed':
                notes = f"ERROR: {error}"
            elif duration > 10:
                notes = "SLOW"
            
            print(f"{table:<35} {status:<12} {rows:<12,} {wm_type:<12} {duration:<10.1f} {start_time:<20} {notes}")
        
        print("-" * 140)
        print(f"\nSUMMARY:")
        print(f"  Total rows loaded: {total_rows:,}")
        print(f"  Total tables: {len(result.result.data_array)}")
        print(f"  Tables currently running: {running_count}")
        print(f"  Tables with incremental watermark: {len(incremental_tables)}")
        print(f"  Tables doing full load (no watermark): {len(full_load_tables)}")
        
        if full_load_tables:
            print(f"\n  Full load tables (may cause slowness):")
            for t, r, wm in sorted(full_load_tables, key=lambda x: -x[1])[:10]:
                print(f"    - {t}: {r:,} rows ({wm})")

if __name__ == "__main__":
    main()
