#!/usr/bin/env python3
"""Deploy updated notebook, config, and wheel to Databricks."""

import yaml
import base64
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

def main():
    # Load credentials
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    base_path = Path(__file__).parent / "pipelines" / "timescaledb"

    # Deploy notebook
    notebook_path = base_path / "notebooks" / "bronze_loader_optimized.py"
    if notebook_path.exists():
        content = notebook_path.read_text(encoding='utf-8')
        workspace_path = "/Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_optimized"

        try:
            w.workspace.mkdirs("/Workspace/migration_project/pipelines/timescaledb/notebooks")
        except:
            pass

        w.workspace.import_(
            path=workspace_path,
            content=base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )
        print(f"Deployed notebook to {workspace_path}")

    # Deploy config
    config_path = base_path / "config" / "timescaledb_tables_v2.yml"
    if config_path.exists():
        content = config_path.read_text(encoding='utf-8')
        workspace_path = "/Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables_v2.yml"

        try:
            w.workspace.mkdirs("/Workspace/migration_project/pipelines/timescaledb/config")
        except:
            pass

        w.workspace.import_(
            path=workspace_path,
            content=base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            format=ImportFormat.AUTO,
            overwrite=True
        )
        print(f"Deployed config to {workspace_path}")

    # Deploy loader source
    loader_path = base_path / "src" / "timescaledb_loader_v2.py"
    if loader_path.exists():
        content = loader_path.read_text(encoding='utf-8')
        workspace_path = "/Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2.py"

        try:
            w.workspace.mkdirs("/Workspace/migration_project/pipelines/timescaledb/src")
        except:
            pass

        w.workspace.import_(
            path=workspace_path,
            content=base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True
        )
        print(f"Deployed loader to {workspace_path}")

    # Deploy wheel to Unity Catalog volume
    wheel_path = base_path / "package" / "dist" / "timescaledb_loader-2.2.0-py3-none-any.whl"
    if wheel_path.exists():
        volume_path = "/Volumes/wakecap_prod/migration/libs/timescaledb_loader-2.2.0-py3-none-any.whl"

        with open(wheel_path, 'rb') as f:
            wheel_content = f.read()

        # Upload using Files API for Unity Catalog volumes
        w.files.upload(volume_path, wheel_content, overwrite=True)
        print(f"Deployed wheel to {volume_path}")
    else:
        print(f"Wheel not found at {wheel_path}")

    print("\nAll files deployed successfully!")

if __name__ == "__main__":
    main()
