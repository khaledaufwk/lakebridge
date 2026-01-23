#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
import yaml
from databricks.sdk import WorkspaceClient

creds_path = Path(__file__).parent / "credentials_template.yml"
with open(creds_path, encoding='utf-8') as f:
    creds = yaml.safe_load(f)

w = WorkspaceClient(host=creds['databricks']['host'], token=creds['databricks']['token'])

for wh in w.warehouses.list():
    print(f"{wh.name} ({wh.id}): {wh.state.value}")
    if wh.health:
        print(f"  Health: {wh.health.status}")
