"""Fix the gold table ambiguous column reference and re-upload."""
import yaml
import base64
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

creds = yaml.safe_load(open(Path.home() / '.databricks/labs/lakebridge/.credentials.yml'))['databricks']
w = WorkspaceClient(host=creds['host'], token=creds['token'])

workspace_path = '/Workspace/Shared/migrations/wakecap/wakecap_dlt_pipeline'

# Fixed notebook content - renamed columns to avoid ambiguity
notebook_content = '''# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration Pipeline

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
        .withColumn("worker_ingested_at", current_timestamp())
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
        .withColumn("project_ingested_at", current_timestamp())
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
            col("WorkerID"),
            col("WorkerName"),
            col("ProjectID"),
            col("ProjectName"),
            col("worker_ingested_at").alias("ingested_at")
        )
    )
'''

print('Uploading fixed notebook...')
content_b64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
w.workspace.import_(
    path=workspace_path,
    content=content_b64,
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)
print('[OK] Notebook updated')

# Restart pipeline
pipeline_id = '02f3192b-6db3-44b6-867c-18cf492db6c5'
print('Restarting pipeline...')
update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
print(f'[OK] Pipeline restarted! Update ID: {update.update_id}')
