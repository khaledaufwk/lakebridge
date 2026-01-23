#!/usr/bin/env python3
"""Check watermarks table schema."""
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

    query = "DESCRIBE TABLE wakecap_prod.migration._timescaledb_watermarks"

    print("Watermarks table schema:")
    print("=" * 80)

    result = w.statement_execution.execute_statement(
        warehouse_id=serverless.id,
        statement=query,
        wait_timeout="30s"
    )

    if result.result and result.result.data_array:
        for row in result.result.data_array:
            print(f"  {row[0]}: {row[1]}")

if __name__ == "__main__":
    main()
