"""Detailed analysis of Bronze job run - check watermarks and per-table timing."""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import yaml
from datetime import datetime, timedelta
import time
import sys

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

# Load credentials
with open(r'C:\Users\khaledadmin\.databricks\labs\lakebridge\.credentials.yml') as f:
    creds = yaml.safe_load(f)

w = WorkspaceClient(
    host=creds['databricks']['host'],
    token=creds['databricks']['token']
)

# Find a SQL warehouse
warehouses = list(w.warehouses.list())
warehouse_id = None
for wh in warehouses:
    print(f"Warehouse: {wh.name} ({wh.id}) - State: {wh.state}")
    if wh.state.value == 'RUNNING':
        warehouse_id = wh.id
        break

if not warehouse_id:
    # Start the serverless warehouse (faster startup)
    for wh in warehouses:
        if 'Serverless' in wh.name:
            print(f"\nStarting warehouse {wh.name}...")
            w.warehouses.start(wh.id)
            warehouse_id = wh.id
            break

    if not warehouse_id:
        wh = warehouses[0]
        print(f"\nStarting warehouse {wh.name}...")
        w.warehouses.start(wh.id)
        warehouse_id = wh.id

    # Wait for warehouse to start
    print("Waiting for warehouse to start...")
    for i in range(60):
        wh_status = w.warehouses.get(warehouse_id)
        if wh_status.state.value == 'RUNNING':
            print("Warehouse is running!")
            break
        time.sleep(5)
        print(f"  Still starting... ({i*5}s)")

print(f"\nUsing warehouse: {warehouse_id}")

def run_query(query, description):
    """Run a SQL query and return results."""
    print(f"\n{'='*100}")
    print(description)
    print("="*100)

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="50s"
        )

        if response.status.state == StatementState.SUCCEEDED:
            return response.result.data_array if response.result else []
        elif response.status.state == StatementState.PENDING:
            # Poll for results
            stmt_id = response.statement_id
            for _ in range(30):
                time.sleep(2)
                status = w.statement_execution.get_statement(stmt_id)
                if status.status.state == StatementState.SUCCEEDED:
                    return status.result.data_array if status.result else []
                elif status.status.state in (StatementState.FAILED, StatementState.CANCELED):
                    print(f"Query failed: {status.status}")
                    if status.status.error:
                        print(f"Error: {status.status.error}")
                    return []
            return []
        else:
            print(f"Query state: {response.status.state}")
            if response.status.error:
                print(f"Error: {response.status.error}")
            return []
    except Exception as e:
        print(f"Query error: {e}")
        return []

# Query 1: ALL tables with timing info
query1 = """
SELECT
    source_table,
    watermark_column,
    last_load_status,
    COALESCE(last_load_row_count, 0) as row_count,
    COALESCE(ROUND(TIMESTAMPDIFF(SECOND, last_load_start_time, last_load_end_time) / 60.0, 2), 0) as duration_minutes,
    last_load_start_time,
    last_load_end_time
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb'
ORDER BY row_count DESC
"""

results1 = run_query(query1, "ALL tables (ordered by row count):")

if results1:
    total_duration = 0
    total_rows = 0
    print(f"\n{'Table':<40} {'Status':<10} {'Rows':>15} {'Duration':>10}")
    print("-" * 85)
    for row in results1:
        table = row[0] or ""
        status = row[2] or ""
        rows = int(row[3]) if row[3] else 0
        duration = float(row[4]) if row[4] else 0
        total_duration += duration
        total_rows += rows
        print(f"{table:<40} {status:<10} {rows:>15,} {duration:>8.2f} m")
    print("-" * 85)
    print(f"{'TOTAL':<40} {'':<10} {total_rows:>15,} {total_duration:>8.2f} m")
    print(f"\nTotal tables: {len(results1)}")

# Check table configuration for append-only optimization
print("\n" + "="*100)
print("Table Configuration Analysis (is_append_only flag):")
print("="*100)

# Load the table config
config_path = r'C:\Users\khaledadmin\lakebridge\migration_project\pipelines\timescaledb\config\timescaledb_tables_v2.yml'
with open(config_path) as f:
    config = yaml.safe_load(f)

append_only_tables = []
merge_tables = []
hypertables = []
table_configs = {}

for table in config.get('tables', []):
    table_name = table.get('source_table', '')
    is_append_only = table.get('is_append_only', False)
    is_hypertable = table.get('is_hypertable', False)
    table_configs[table_name] = table

    if is_append_only:
        append_only_tables.append(table_name)
    else:
        merge_tables.append(table_name)

    if is_hypertable:
        hypertables.append(table_name)

print(f"\n[OK] Append-only tables ({len(append_only_tables)}): Uses fast APPEND mode")
for t in append_only_tables:
    print(f"   - {t}")

print(f"\n[!] MERGE tables ({len(merge_tables)}): Uses slower MERGE operation")

print(f"\n[i] Hypertables ({len(hypertables)}): TimescaleDB partitioned tables")
for t in hypertables:
    is_append = t in append_only_tables
    status = "[OK] append-only" if is_append else "[!!] using MERGE - SHOULD BE OPTIMIZED"
    print(f"   - {t} ({status})")

# Identify optimization opportunities
print("\n" + "="*100)
print("OPTIMIZATION OPPORTUNITIES:")
print("="*100)

# Cross-reference large tables with config
if results1:
    print("\n1. LARGE TABLES NOT USING APPEND-ONLY MODE:")
    print("-" * 80)
    large_merge_tables = []
    for row in results1:
        table = row[0] or ""
        rows = int(row[3]) if row[3] else 0
        duration = float(row[4]) if row[4] else 0

        if rows > 100000 and table not in append_only_tables:
            wm = row[1] or ""
            large_merge_tables.append((table, rows, duration, wm))
            print(f"   {table:<40} {rows:>12,} rows  {duration:>6.2f} min  (watermark: {wm})")

    if not large_merge_tables:
        print("   None found - all large tables are optimized!")

    # Check for tables using CreatedAt that could be append-only
    print("\n2. TABLES WITH CreatedAt WATERMARK (potential append-only candidates):")
    print("-" * 80)
    created_at_tables = []
    for row in results1:
        table = row[0] or ""
        rows = int(row[3]) if row[3] else 0
        wm = row[1] or ""

        if wm == 'CreatedAt' and table not in append_only_tables and rows > 10000:
            created_at_tables.append((table, rows, wm))
            print(f"   {table:<40} {rows:>12,} rows")

    if not created_at_tables:
        print("   None found!")

    # Check for hypertables not using append-only
    print("\n3. HYPERTABLES NOT USING APPEND-ONLY (HIGH IMPACT):")
    print("-" * 80)
    for t in hypertables:
        if t not in append_only_tables:
            # Find row count
            for row in results1:
                if row[0] == t:
                    rows = int(row[3]) if row[3] else 0
                    duration = float(row[4]) if row[4] else 0
                    print(f"   [!!] {t:<40} {rows:>12,} rows  {duration:>6.2f} min")
                    break

# Calculate potential savings
print("\n" + "="*100)
print("ESTIMATED IMPACT:")
print("="*100)

total_merge_time = 0
for row in results1:
    table = row[0] or ""
    duration = float(row[4]) if row[4] else 0
    rows = int(row[3]) if row[3] else 0

    if table not in append_only_tables and rows > 50000:
        total_merge_time += duration

print(f"\nTotal time spent on large MERGE tables: {total_merge_time:.2f} minutes")
print(f"Estimated savings with append-only mode: ~{total_merge_time * 0.5:.2f} minutes (50% reduction)")
print("\nNote: Append-only mode skips the expensive MERGE comparison step.")
print("This is safe for tables that only INSERT new rows (no updates).")
