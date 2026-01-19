"""Upload split pipeline notebooks (Bronze, Silver, Gold) to Databricks."""
import yaml
import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

# Load credentials
creds_path = r"C:\Users\khaledadmin\.databricks\labs\lakebridge\.credentials.yml"
with open(creds_path, 'r') as f:
    creds = yaml.safe_load(f)

databricks_creds = creds.get('databricks', {})
host = databricks_creds.get('host')
token = databricks_creds.get('token')

w = WorkspaceClient(host=host, token=token)

# Notebooks to upload
notebooks = [
    {
        "local_path": r"C:\Users\khaledadmin\lakebridge\migration_project\dlt_bronze_tables.py",
        "workspace_path": "/Workspace/Shared/migrations/wakecap/dlt_bronze_tables",
        "name": "Bronze"
    },
    {
        "local_path": r"C:\Users\khaledadmin\lakebridge\migration_project\dlt_silver_tables.py",
        "workspace_path": "/Workspace/Shared/migrations/wakecap/dlt_silver_tables",
        "name": "Silver"
    },
    {
        "local_path": r"C:\Users\khaledadmin\lakebridge\migration_project\dlt_gold_views.py",
        "workspace_path": "/Workspace/Shared/migrations/wakecap/dlt_gold_views",
        "name": "Gold"
    }
]

print("="*60)
print("UPLOADING SPLIT NOTEBOOKS")
print("="*60)

workspace_paths = []
for nb in notebooks:
    print(f"\nUploading {nb['name']} notebook...")

    with open(nb['local_path'], 'r', encoding='utf-8') as f:
        content = f.read()

    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')

    try:
        w.workspace.import_(
            path=nb['workspace_path'],
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=content_b64,
            overwrite=True
        )
        print(f"  SUCCESS: {nb['workspace_path']}")
        workspace_paths.append(nb['workspace_path'])
    except Exception as e:
        print(f"  ERROR: {e}")
        raise

print("\n" + "="*60)
print("UPDATING PIPELINE CONFIGURATION")
print("="*60)

pipeline_id = "bc59257f-73aa-42ed-9fc6-01a6c14dbb0a"
pipeline = w.pipelines.get(pipeline_id=pipeline_id)

print(f"\nPipeline: {pipeline.name}")
print(f"Old notebooks: {[lib.notebook.path for lib in pipeline.spec.libraries if lib.notebook]}")

# Create new library config with all 3 notebooks
new_libraries = [
    PipelineLibrary(notebook=NotebookLibrary(path=path))
    for path in workspace_paths
]

print(f"New notebooks: {workspace_paths}")

try:
    w.pipelines.update(
        pipeline_id=pipeline_id,
        name=pipeline.name,
        libraries=new_libraries,
        target=pipeline.spec.target,
        catalog=pipeline.spec.catalog,
        configuration=pipeline.spec.configuration,
        clusters=pipeline.spec.clusters,
        development=True,
        continuous=False
    )
    print("\nSUCCESS: Pipeline updated with 3 split notebooks")
except Exception as e:
    print(f"\nERROR updating pipeline: {e}")
    raise

print("\n" + "="*60)
print("UPLOAD COMPLETE")
print("="*60)
print(f"""
Notebooks uploaded:
  - Bronze: {notebooks[0]['workspace_path']}
  - Silver: {notebooks[1]['workspace_path']}
  - Gold:   {notebooks[2]['workspace_path']}

Pipeline ID: {pipeline_id}

Changes from original:
  - Split into 3 notebooks (Bronze, Silver, Gold)
  - FactWorkersHistory DISABLED (too slow)
  - Incremental loading with _dlt_load_time timestamp
  - Streaming reads from Bronze to Silver
""")
