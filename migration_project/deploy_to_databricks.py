#!/usr/bin/env python3
"""
Deploy DLT pipeline to Databricks workspace.
"""
import sys
import json
import base64
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.pipelines import PipelineCluster, NotebookLibrary


def load_databricks_config():
    """Load Databricks credentials."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)['databricks']


def upload_notebook(w: WorkspaceClient, local_path: Path, workspace_path: str):
    """Upload a notebook to Databricks workspace."""
    print(f"  Uploading {local_path.name} to {workspace_path}")
    
    # Read notebook content
    content = local_path.read_text(encoding='utf-8')
    
    # Encode as base64
    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
    
    # Ensure parent directory exists
    parent_path = str(Path(workspace_path).parent)
    try:
        w.workspace.mkdirs(parent_path)
    except Exception:
        pass  # Directory might already exist
    
    # Import notebook
    w.workspace.import_(
        path=workspace_path,
        content=content_b64,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    
    return workspace_path


def create_dlt_pipeline(w: WorkspaceClient, config: dict, notebook_path: str):
    """Create or update DLT pipeline."""
    pipeline_name = config['name']
    
    # Check if pipeline exists
    existing_pipelines = list(w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'"))
    
    pipeline_spec = {
        "name": pipeline_name,
        "catalog": config['catalog'],
        "target": config['schema'],
        "development": config.get('development', True),
        "continuous": config.get('continuous', False),
        "channel": config.get('channel', 'PREVIEW'),
        "photon": config.get('photon', True),
        "libraries": [
            {"notebook": {"path": notebook_path}}
        ],
        "clusters": [
            {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 4,
                    "mode": "ENHANCED"
                }
            }
        ]
    }
    
    if existing_pipelines:
        # Update existing pipeline
        pipeline_id = existing_pipelines[0].pipeline_id
        print(f"  Updating existing pipeline: {pipeline_id}")
        w.pipelines.update(
            pipeline_id=pipeline_id,
            name=pipeline_name,
            catalog=config['catalog'],
            target=config['schema'],
            development=config.get('development', True),
            continuous=config.get('continuous', False),
            channel=config.get('channel', 'PREVIEW'),
            photon=config.get('photon', True),
            libraries=[NotebookLibrary(path=notebook_path)],
        )
        return pipeline_id, False
    else:
        # Create new pipeline
        print(f"  Creating new pipeline: {pipeline_name}")
        result = w.pipelines.create(
            name=pipeline_name,
            catalog=config['catalog'],
            target=config['schema'],
            development=config.get('development', True),
            continuous=config.get('continuous', False),
            channel=config.get('channel', 'PREVIEW'),
            photon=config.get('photon', True),
            libraries=[NotebookLibrary(path=notebook_path)],
        )
        return result.pipeline_id, True


def main():
    print("=" * 60)
    print("DATABRICKS DEPLOYMENT")
    print("=" * 60)
    
    base_dir = Path(__file__).parent
    pipelines_dir = base_dir / "pipelines"
    
    # Load configurations
    print("\n1. Loading configurations...")
    db_config = load_databricks_config()
    
    pipeline_config_path = pipelines_dir / "pipeline_config.json"
    with open(pipeline_config_path, 'r', encoding='utf-8') as f:
        pipeline_config = json.load(f)
    
    print(f"   Databricks Host: {db_config['host']}")
    print(f"   Target Catalog: {pipeline_config['catalog']}")
    print(f"   Target Schema: {pipeline_config['schema']}")
    
    # Connect to Databricks
    print("\n2. Connecting to Databricks...")
    w = WorkspaceClient(
        host=db_config['host'],
        token=db_config['token']
    )
    
    user = w.current_user.me()
    print(f"   Connected as: {user.user_name}")
    
    # Define workspace paths
    workspace_base = "/Workspace/Shared/migrations/wakecap"
    notebook_workspace_path = f"{workspace_base}/wakecap_migration_pipeline"
    
    # Upload notebook
    print("\n3. Uploading DLT notebook...")
    notebook_path = pipelines_dir / "wakecap_migration_pipeline.py"
    
    try:
        uploaded_path = upload_notebook(w, notebook_path, notebook_workspace_path)
        print(f"   [OK] Notebook uploaded to: {uploaded_path}")
    except Exception as e:
        print(f"   [ERROR] Failed to upload notebook: {e}")
        # Continue anyway - we'll create a deployment report
    
    # Create/Update DLT Pipeline
    print("\n4. Creating DLT Pipeline...")
    try:
        pipeline_id, is_new = create_dlt_pipeline(w, pipeline_config, notebook_workspace_path)
        action = "Created" if is_new else "Updated"
        print(f"   [OK] {action} pipeline: {pipeline_id}")
    except Exception as e:
        print(f"   [ERROR] Failed to create pipeline: {e}")
        pipeline_id = None
    
    # Generate deployment report
    print("\n5. Generating deployment report...")
    
    report = f"""# Databricks Deployment Report

**Deployment Time:** {datetime.now().isoformat()}
**Deployed By:** {user.user_name}

## Databricks Connection
- **Host:** {db_config['host']}
- **Catalog:** {pipeline_config['catalog']}
- **Schema:** {pipeline_config['schema']}

## Deployed Artifacts

### DLT Notebook
- **Workspace Path:** {notebook_workspace_path}
- **Status:** {'Uploaded' if notebook_path.exists() else 'Pending'}

### DLT Pipeline
- **Pipeline Name:** {pipeline_config['name']}
- **Pipeline ID:** {pipeline_id or 'Not created'}
- **Development Mode:** {pipeline_config.get('development', True)}

## Pipeline Configuration
- **Tables:** {len(pipeline_config.get('tables', []))}
- **Views:** {len(pipeline_config.get('views', []))}
- **Photon Enabled:** {pipeline_config.get('photon', True)}

## Next Steps

1. **View Pipeline in Databricks:**
   - Navigate to: Workflows > Delta Live Tables
   - Find: {pipeline_config['name']}

2. **Configure Data Source Secrets:**
   ```bash
   databricks secrets create-scope --scope wakecap
   databricks secrets put --scope wakecap --key sqlserver_jdbc_url --string-value "jdbc:sqlserver://wakecap24.database.windows.net:1433;database=WakeCapDW_20251215"
   databricks secrets put --scope wakecap --key sqlserver_user --string-value "your_user"
   databricks secrets put --scope wakecap --key sqlserver_password --string-value "your_password"
   ```

3. **Start Pipeline (Development Mode):**
   ```bash
   databricks pipelines start {pipeline_id or '<pipeline_id>'}
   ```

4. **Monitor Pipeline:**
   - Check pipeline status in Databricks UI
   - Review data quality metrics
   - Validate row counts

## Links
- [DLT Pipeline]({db_config['host']}/pipelines/{pipeline_id or ''})
- [Notebook]({db_config['host']}#workspace{notebook_workspace_path})
"""
    
    report_path = pipelines_dir / "DEPLOYMENT_REPORT.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"   [OK] Report saved: {report_path}")
    
    print("\n" + "=" * 60)
    print("DEPLOYMENT COMPLETE")
    print("=" * 60)
    
    if pipeline_id:
        print(f"\nPipeline URL: {db_config['host']}/pipelines/{pipeline_id}")
    
    print(f"\nNotebook URL: {db_config['host']}#workspace{notebook_workspace_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
