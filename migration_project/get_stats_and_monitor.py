#!/usr/bin/env python3
"""Get table stats and monitor the running job."""

import sys
from pathlib import Path
import yaml
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from databricks.sdk import WorkspaceClient

def load_credentials():
    template_path = Path(__file__).parent / "credentials_template.yml"
    with open(template_path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_run_info():
    run_file = Path(__file__).parent / "current_run.txt"
    job_id = None
    run_id = None

    if run_file.exists():
        with open(run_file) as f:
            for line in f:
                if line.startswith("JOB_ID="):
                    job_id = int(line.strip().split("=")[1])
                elif line.startswith("RUN_ID="):
                    run_id = int(line.strip().split("=")[1])

    return job_id, run_id


def get_table_stats(w, warehouse_id):
    """Get row counts for all timescale_ tables in raw schema."""
    stats = {}

    # Get list of tables
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="""
        SELECT table_name
        FROM wakecap_prod.information_schema.tables
        WHERE table_schema = 'raw' AND table_name LIKE 'timescale_%'
        ORDER BY table_name
        """,
        wait_timeout="50s"
    )

    if result.result and result.result.data_array:
        tables = [row[0] for row in result.result.data_array]
        print(f"Found {len(tables)} tables to check")

        for i, table in enumerate(tables):
            try:
                count_result = w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=f"SELECT COUNT(*) FROM wakecap_prod.raw.{table}",
                    wait_timeout="50s"
                )
                if count_result.result and count_result.result.data_array:
                    stats[table] = int(count_result.result.data_array[0][0])
                    print(f"  [{i+1}/{len(tables)}] {table}: {stats[table]:,} rows")
            except Exception as e:
                stats[table] = f"Error: {e}"
                print(f"  [{i+1}/{len(tables)}] {table}: Error")

    return stats


def main():
    creds = load_credentials()
    job_id, run_id = load_run_info()

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    print("=" * 70)
    print("TIMESCALEDB BRONZE JOB - TABLE STATS AND STATUS")
    print("=" * 70)
    print(f"Check time: {datetime.now()}")
    print(f"Job ID: {job_id}")
    print(f"Run ID: {run_id}")

    # Get warehouse
    warehouses = list(w.warehouses.list())
    warehouse_id = warehouses[0].id if warehouses else None

    if not warehouse_id:
        print("No SQL warehouse found!")
        return 1

    # Get job status
    print("\n" + "-" * 70)
    print("JOB STATUS:")
    print("-" * 70)

    if run_id:
        try:
            run = w.jobs.get_run(run_id)

            state = run.state.life_cycle_state if run.state else "Unknown"
            result = run.state.result_state if run.state and run.state.result_state else "In Progress"

            print(f"State: {state}")
            print(f"Result: {result}")

            if run.start_time:
                start = datetime.fromtimestamp(run.start_time / 1000)
                elapsed = (datetime.now() - start).total_seconds() / 60
                print(f"Started: {start}")
                print(f"Elapsed: {elapsed:.1f} minutes")

            if run.end_time:
                end = datetime.fromtimestamp(run.end_time / 1000)
                print(f"Ended: {end}")

            # Task details
            if run.tasks:
                for task in run.tasks:
                    task_state = task.state.life_cycle_state if task.state else "Unknown"
                    task_result = task.state.result_state if task.state and task.state.result_state else "N/A"
                    print(f"\nTask '{task.task_key}': {task_state} / {task_result}")

        except Exception as e:
            print(f"Error getting run status: {e}")

    # Get current stats
    print("\n" + "-" * 70)
    print("CURRENT TABLE ROW COUNTS:")
    print("-" * 70)

    stats = get_table_stats(w, warehouse_id)

    total = sum(v for v in stats.values() if isinstance(v, int))
    print(f"\nTotal rows: {total:,}")

    # Save stats to file
    stats_file = Path(__file__).parent / "table_stats.txt"
    with open(stats_file, 'w') as f:
        f.write(f"# Stats at {datetime.now()}\n")
        for table, count in sorted(stats.items()):
            f.write(f"{table}={count}\n")

    print(f"\nStats saved to: {stats_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
