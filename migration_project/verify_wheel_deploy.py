#!/usr/bin/env python3
"""Verify wheel deployment to Unity Catalog volume."""

import yaml
from pathlib import Path
from databricks.sdk import WorkspaceClient

def main():
    # Load credentials
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # List wheel files in volume
    volume_path = "/Volumes/wakecap_prod/migration/libs"
    print(f"Listing files in {volume_path}:\n")

    try:
        files = list(w.files.list_directory_contents(volume_path))
        wheel_files = [f for f in files if f.name.endswith('.whl')]

        for f in sorted(wheel_files, key=lambda x: x.name):
            size_kb = (f.file_size or 0) / 1024
            print(f"  {f.name:<50} {size_kb:>8.1f} KB")

        print(f"\nTotal wheel files: {len(wheel_files)}")

        # Check for v2.2.0
        v22 = [f for f in wheel_files if '2.2.0' in f.name]
        if v22:
            print("\n[OK] timescaledb_loader-2.2.0 wheel is deployed!")
        else:
            print("\n[WARNING] v2.2.0 wheel not found!")

    except Exception as e:
        print(f"Error listing files: {e}")

if __name__ == "__main__":
    main()
