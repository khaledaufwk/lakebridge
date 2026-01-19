# Conversion Templates

## DLT Table Templates

### Bronze Layer - JDBC Ingestion

```python
import dlt
from pyspark.sql.functions import current_timestamp

SECRET_SCOPE = "migration_secrets"

def read_sql_server_table(table_name: str, schema_name: str = "dbo"):
    """Read a table from SQL Server via JDBC"""
    return (
        spark.read.format("jdbc")
        .option("url", dbutils.secrets.get(SECRET_SCOPE, "sqlserver_jdbc_url"))
        .option("dbtable", f"[{schema_name}].[{table_name}]")
        .option("user", dbutils.secrets.get(SECRET_SCOPE, "sqlserver_user"))
        .option("password", dbutils.secrets.get(SECRET_SCOPE, "sqlserver_password"))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

@dlt.table(
    name="bronze_{table_name_lower}",
    comment="Raw data from {schema_name}.{table_name}",
    table_properties={"quality": "bronze"}
)
def bronze_{table_name_lower}():
    return read_sql_server_table("{table_name}", "{schema_name}")
```

### Silver Layer - Data Quality

```python
@dlt.table(
    name="silver_{table_name_lower}",
    comment="Cleaned {table_name} with data quality checks"
)
@dlt.expect_or_drop("valid_pk", "{pk_column} IS NOT NULL")
@dlt.expect("no_future_dates", "{date_column} <= CURRENT_DATE()", on_violation="WARN")
def silver_{table_name_lower}():
    return (
        dlt.read("bronze_{table_name_lower}")
        .filter(col("DeletedAt").isNull())  # Soft delete filter
        .withColumn("{table_name_lower}_ingested_at", current_timestamp())
        .dropDuplicates(["{pk_column}"])
    )
```

### Gold Layer - Business View

```python
@dlt.table(
    name="gold_{view_name_lower}",
    comment="Business view: {view_description}"
)
def gold_{view_name_lower}():
    return spark.sql("""
        SELECT
            -- Explicitly select columns to avoid ambiguity
            t1.{pk_column},
            t1.{column1},
            t2.{column2},
            t1.{table1}_ingested_at AS ingested_at
        FROM LIVE.silver_{table1} t1
        JOIN LIVE.silver_{table2} t2
            ON t1.{join_key} = t2.{join_key}
        WHERE {filter_condition}
    """)
```

## Stored Procedure Templates

### Simple MERGE -> DLT APPLY CHANGES

```python
import dlt
from pyspark.sql.functions import col, expr

# For procedures like spDeltaSync* that do MERGE operations
dlt.create_streaming_table(
    name="{target_table}",
    comment="Sync from {source_table} - converted from {procedure_name}"
)

dlt.apply_changes(
    target="{target_table}",
    source="bronze_{source_table}",
    keys=["{pk_column}"],
    sequence_by=col("{timestamp_column}"),
    apply_as_deletes=expr("{delete_condition}"),
    except_column_list=["{columns_to_exclude}"]
)
```

### Calculation Procedure -> DLT Batch

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dlt.table(
    name="{output_table}",
    comment="Converted from {procedure_name}"
)
def {function_name}():
    """
    Original procedure: {procedure_name}
    Patterns found: {patterns}
    """
    # Load source data
    source = dlt.read("silver_{source_table}")

    # Define window for calculations
    window_spec = Window.partitionBy("{partition_col}").orderBy("{order_col}")

    # Apply transformations
    result = (
        source
        .withColumn("row_num", row_number().over(window_spec))
        .withColumn("prev_value", lag("{value_col}").over(window_spec))
        .withColumn("running_total", sum("{value_col}").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ))
    )

    return result
```

### Complex CURSOR Procedure -> Notebook

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Converted from: {procedure_name}
# MAGIC
# MAGIC **Original**: {line_count} lines T-SQL
# MAGIC **Patterns**: CURSOR, {other_patterns}
# MAGIC **Converted by**: Claude Code

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

# Configuration
CATALOG = "{catalog}"
SCHEMA = "{schema}"

# COMMAND ----------

def {procedure_name_snake}():
    """
    Converted cursor logic to window functions.

    Original T-SQL pattern:
    {original_cursor_pattern}
    """
    # Read source data
    df = spark.table(f"{CATALOG}.{SCHEMA}.{source_table}")

    # Convert cursor iteration to window function
    window_spec = Window.partitionBy("{group_by}").orderBy("{order_by}")

    result = (
        df
        .withColumn("prev_value", lag("{column}").over(window_spec))
        .withColumn("next_value", lead("{column}").over(window_spec))
        .withColumn("running_calc", sum("{column}").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ))
    )

    # Write results
    result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{target_table}")

    return result.count()

# COMMAND ----------

# Execute
rows_processed = {procedure_name_snake}()
print(f"Processed {rows_processed} rows")
```

## UDF Templates

### Simple Function -> SQL UDF

```sql
-- Converted from: {function_name}
-- Original return type: {original_return_type}

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{function_name_lower}(
    param1 STRING,
    param2 INT
)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Converted from {original_schema}.{function_name}'
RETURN (
    -- Converted logic
    CONCAT(param1, CAST(param2 AS STRING))
);
```

### String/Date Function -> Python UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, TimestampType
from datetime import datetime, timedelta

@udf(returnType=StringType())
def fn_{function_name_lower}(param1: str, param2: int) -> str:
    """
    Converted from: {original_schema}.{function_name}

    Original T-SQL:
    {original_function_body}
    """
    if param1 is None:
        return None

    # Converted logic
    result = param1.upper() + str(param2)
    return result

# Register for SQL use
spark.udf.register("fn_{function_name_lower}", fn_{function_name_lower})
```

### Geography Function -> H3 UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType
import h3

@udf(returnType=DoubleType())
def fn_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Converted from: dbo.fnCalculateDistance
    Uses Haversine formula (originally STDistance)
    """
    from math import radians, sin, cos, sqrt, atan2

    if any(x is None for x in [lat1, lon1, lat2, lon2]):
        return None

    R = 6371  # Earth's radius in km

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))

    return R * c

@udf(returnType=StringType())
def fn_point_to_h3(lat: float, lon: float, resolution: int = 9) -> str:
    """
    Convert lat/lon to H3 index (replaces geography::Point)
    """
    if lat is None or lon is None:
        return None
    return h3.geo_to_h3(lat, lon, resolution)

# Register UDFs
spark.udf.register("fn_distance_km", fn_distance_km)
spark.udf.register("fn_point_to_h3", fn_point_to_h3)
```

## Deployment Template

```python
#!/usr/bin/env python
"""Deploy migration pipeline to Databricks"""

import yaml
import base64
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary

def deploy_pipeline():
    # Load credentials
    creds_path = Path.home() / '.databricks/labs/lakebridge/.credentials.yml'
    creds = yaml.safe_load(creds_path.read_text())['databricks']

    w = WorkspaceClient(host=creds['host'], token=creds['token'])

    catalog = creds['catalog']
    schema = creds['schema']

    # 1. Create schema if not exists
    try:
        w.schemas.create(name=schema, catalog_name=catalog)
        print(f"Created schema: {catalog}.{schema}")
    except Exception as e:
        if "already exists" not in str(e):
            raise
        print(f"Schema exists: {catalog}.{schema}")

    # 2. Create/verify secret scope
    scope_name = "{scope_name}"
    try:
        w.secrets.create_scope(scope=scope_name)
        print(f"Created scope: {scope_name}")
    except Exception as e:
        if "already exists" not in str(e):
            raise
        print(f"Scope exists: {scope_name}")

    # 3. Upload notebook
    notebook_content = Path("{notebook_path}").read_text()
    workspace_path = "/Workspace/Shared/migrations/{pipeline_name}"

    w.workspace.mkdirs(str(Path(workspace_path).parent))
    w.workspace.import_(
        path=workspace_path,
        content=base64.b64encode(notebook_content.encode()).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print(f"Uploaded notebook: {workspace_path}")

    # 4. Create pipeline with SERVERLESS
    result = w.pipelines.create(
        name="{pipeline_name}",
        catalog=catalog,
        target=schema,
        development=True,
        serverless=True,  # CRITICAL: Avoids VM quota issues
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=workspace_path))]
    )
    print(f"Created pipeline: {result.pipeline_id}")

    # 5. Start pipeline
    update = w.pipelines.start_update(
        pipeline_id=result.pipeline_id,
        full_refresh=True
    )
    print(f"Started update: {update.update_id}")

    return result.pipeline_id, update.update_id

if __name__ == "__main__":
    pipeline_id, update_id = deploy_pipeline()
    print(f"\nPipeline URL: https://{creds['host']}/pipelines/{pipeline_id}")
```
