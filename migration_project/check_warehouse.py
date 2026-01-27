#!/usr/bin/env python3
"""Check and start SQL warehouse if needed."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient


def main():
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        creds = yaml.safe_load(f)

    db = creds['databricks']
    w = WorkspaceClient(host=db['host'], token=db['token'])

    print("SQL Warehouses:")
    for wh in w.warehouses.list():
        print(f"  {wh.name}: {wh.state} (ID: {wh.id})")

        if wh.state.value != "RUNNING":
            print(f"    Starting warehouse {wh.name}...")
            try:
                w.warehouses.start(wh.id)
                print(f"    Start command sent")
            except Exception as e:
                print(f"    Error: {e}")


if __name__ == "__main__":
    main()
