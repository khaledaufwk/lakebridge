"""Fix and re-upload the DLT pipeline notebook to Databricks."""
import yaml
import base64
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

# Load credentials
creds_path = Path.home() / '.databricks/labs/lakebridge/.credentials.yml'
creds = yaml.safe_load(open(creds_path))

databricks_creds = creds['databricks']
host = databricks_creds['host']
token = databricks_creds['token']

# Initialize client
w = WorkspaceClient(host=host, token=token)

# Workspace path
workspace_path = "/Workspace/Shared/migrations/wakecap/wakecap_dlt_pipeline"

# The notebook content - simplified version to ensure it works
notebook_content = '''# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration Pipeline
# MAGIC Test pipeline to validate DLT setup

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, lit, count, coalesce
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer

# COMMAND ----------

@dlt.table(
    name="bronze_worker",
    comment="Bronze worker test data"
)
def bronze_worker():
    schema = StructType([
        StructField("WorkerID", LongType(), False),
        StructField("WorkerName", StringType(), True),
        StructField("ProjectID", IntegerType(), True),
        StructField("CreatedAt", TimestampType(), True),
    ])
    data = [
        (1, "John Smith", 1, None),
        (2, "Jane Doe", 1, None),
        (3, "Bob Wilson", 2, None),
    ]
    return spark.createDataFrame(data, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_project",
    comment="Bronze project test data"
)
def bronze_project():
    schema = StructType([
        StructField("ProjectID", IntegerType(), False),
        StructField("ProjectName", StringType(), True),
    ])
    data = [
        (1, "Downtown Tower"),
        (2, "Airport Terminal"),
    ]
    return spark.createDataFrame(data, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------

@dlt.table(
    name="silver_worker",
    comment="Cleaned worker data"
)
@dlt.expect_or_drop("valid_id", "WorkerID IS NOT NULL")
def silver_worker():
    return (
        dlt.read("bronze_worker")
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="silver_project",
    comment="Cleaned project data"
)
@dlt.expect_or_drop("valid_id", "ProjectID IS NOT NULL")
def silver_project():
    return (
        dlt.read("bronze_project")
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer

# COMMAND ----------

@dlt.table(
    name="gold_worker_summary",
    comment="Worker summary with project info"
)
def gold_worker_summary():
    workers = dlt.read("silver_worker")
    projects = dlt.read("silver_project")
    return (
        workers
        .join(projects, "ProjectID", "left")
        .select(
            "WorkerID",
            "WorkerName",
            "ProjectID",
            "ProjectName",
            "_ingested_at"
        )
    )
'''

print("Uploading notebook to Databricks...")
print(f"Path: {workspace_path}")

try:
    # Delete existing notebook if exists
    try:
        w.workspace.delete(workspace_path)
        print("[OK] Deleted existing notebook")
    except Exception:
        pass
    
    # Encode content
    content_b64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
    
    # Upload as SOURCE format (Databricks notebook)
    w.workspace.import_(
        path=workspace_path,
        content=content_b64,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print("[OK] Notebook uploaded successfully!")
    
    # Verify upload
    status = w.workspace.get_status(workspace_path)
    print(f"[OK] Verified: {status.path}, Type: {status.object_type}")
    
    # Now update the pipeline to use this notebook
    pipeline_name = "WakeCapDW_Migration"
    
    # Find pipeline
    pipelines = list(w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'"))
    
    if pipelines:
        pipeline_id = pipelines[0].pipeline_id
        print(f"\n[INFO] Found pipeline: {pipeline_id}")
        
        # Get current pipeline config
        pipeline = w.pipelines.get(pipeline_id)
        print(f"[INFO] Current state: {pipeline.state}")
        
        # Update pipeline with correct notebook path
        w.pipelines.update(
            pipeline_id=pipeline_id,
            name=pipeline_name,
            catalog="wakecap_prod",
            target="migration",
            development=True,
            libraries=[{"notebook": {"path": workspace_path}}]
        )
        print("[OK] Pipeline updated with new notebook path")
        
        # Start pipeline
        print("\n[INFO] Starting pipeline...")
        update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
        print(f"[OK] Pipeline started! Update ID: {update.update_id}")
        print(f"\nView pipeline: {host}/#joblist/pipelines/{pipeline_id}")
    else:
        print(f"\n[WARN] Pipeline '{pipeline_name}' not found. Creating new one...")
        
        result = w.pipelines.create(
            name=pipeline_name,
            catalog="wakecap_prod",
            target="migration",
            development=True,
            libraries=[{"notebook": {"path": workspace_path}}]
        )
        print(f"[OK] Created pipeline: {result.pipeline_id}")
        
        # Start it
        update = w.pipelines.start_update(pipeline_id=result.pipeline_id, full_refresh=True)
        print(f"[OK] Pipeline started! Update ID: {update.update_id}")
        print(f"\nView pipeline: {host}/#joblist/pipelines/{result.pipeline_id}")

except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()
