# Lakebridge DLT Module

Delta Live Tables (DLT) deployment tools for SQL migrations.

## Overview

The DLT module provides tools for deploying migrated SQL code to Databricks Delta Live Tables pipelines:

- **Generator**: Convert transpiled SQL to DLT notebook format
- **Deployer**: Create and manage DLT pipelines in Databricks
- **Converter**: Analyze SQL files and generate appropriate DLT definitions
- **Templates**: Pre-built pipelines for common migration patterns

## Module Structure

```
dlt/
├── __init__.py           # Module exports
├── generator.py          # DLT notebook generation
├── deployer.py           # Pipeline deployment to Databricks
├── converter.py          # SQL to DLT conversion with dependency analysis
└── templates/
    ├── __init__.py       # Template loader
    ├── bronze_silver_gold.py   # Medallion architecture template
    ├── cdc_pipeline.py         # CDC/SCD Type 2 template
    └── mssql_migration.py      # MSSQL migration template
```

## CLI Commands

### Generate DLT Notebook

Convert transpiled SQL files to a DLT notebook:

```bash
databricks labs lakebridge dlt-generate \
  --input-dir ./transpiled_sql/ \
  --output-file ./dlt_pipeline.py \
  --catalog my_catalog \
  --schema my_schema \
  --notebook-name "My Migration Pipeline" \
  --description "Migrated from MSSQL"
```

### Deploy Pipeline

Upload notebook and create DLT pipeline:

```bash
databricks labs lakebridge dlt-deploy \
  --notebook-path ./dlt_pipeline.py \
  --workspace-path /Workspace/migrations/my_pipeline \
  --pipeline-name "My Migration Pipeline" \
  --target-catalog my_catalog \
  --target-schema my_schema \
  --serverless  # Use serverless compute
```

### Run Pipeline

Execute a DLT pipeline:

```bash
databricks labs lakebridge dlt-run \
  --pipeline-id <pipeline-id> \
  --wait \
  --full-refresh
```

### List Pipelines

```bash
databricks labs lakebridge dlt-list \
  --filter-name "Migration"
```

### Check Status

```bash
databricks labs lakebridge dlt-status \
  --pipeline-id <pipeline-id>
```

## Programmatic Usage

### Generate DLT from SQL Files

```python
from databricks.labs.lakebridge.dlt import SQLToDLTConverter

converter = SQLToDLTConverter(
    catalog="my_catalog",
    schema="my_schema",
    source_system="mssql",
)

result = converter.convert_directory(
    input_dir="transpiled_sql/",
    notebook_name="migration_pipeline",
    description="Migrated from WakeCapDW",
)

if result.success:
    result.notebook.save(Path("dlt_pipeline.py"))
    print(f"Generated {result.table_count} tables")
```

### Deploy to Databricks

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.lakebridge.dlt import DLTDeployer, DLTPipelineConfig

ws = WorkspaceClient()
deployer = DLTDeployer(ws)

# Upload notebook
deployer.upload_notebook(
    "dlt_pipeline.py",
    "/Workspace/migrations/wakecap_dlt"
)

# Create pipeline
config = DLTPipelineConfig(
    name="WakeCapDW_Migration",
    notebook_path="/Workspace/migrations/wakecap_dlt",
    target_catalog="migration_catalog",
    target_schema="wakecap",
    serverless=True,
    photon=True,
)

pipeline_id = deployer.create_pipeline(config)

# Run pipeline
result = deployer.run_pipeline(pipeline_id, wait=True)
if result.success:
    print(f"Pipeline completed in {result.duration_seconds}s")
```

### Using the Generator Directly

```python
from databricks.labs.lakebridge.dlt import DLTGenerator, DLTTableDefinition

generator = DLTGenerator(
    catalog="my_catalog",
    schema="my_schema",
)

# From SQL files
notebook = generator.from_sql_files(
    sql_files=["customers.sql", "orders.sql", "products.sql"],
    notebook_name="sales_pipeline",
)

# Or create definitions manually
table = generator.from_sql_string(
    sql="SELECT * FROM raw_customers WHERE active = true",
    table_name="active_customers",
    comment="Filtered active customers only",
)

notebook.tables.append(table)
notebook.save(Path("sales_pipeline.py"))
```

## DLT Table Types

The converter automatically detects table types based on SQL patterns:

| Type | Detection Pattern | DLT Decorator |
|------|-------------------|---------------|
| **Materialized Table** | CREATE TABLE, INSERT INTO | `@dlt.table` |
| **View** | CREATE VIEW | `@dlt.view` |
| **Streaming Table** | MERGE INTO, CDC patterns | `@dlt.table` with `spark.readStream` |

## Data Quality Expectations

Add data quality rules to tables:

```python
from databricks.labs.lakebridge.dlt import DLTExpectation, DLTDataQuality

table.expectations = [
    DLTExpectation(
        name="valid_customer_id",
        constraint="customer_id IS NOT NULL",
        action=DLTDataQuality.DROP,  # Drop invalid rows
    ),
    DLTExpectation(
        name="valid_email",
        constraint="email LIKE '%@%'",
        action=DLTDataQuality.WARN,  # Log but keep rows
    ),
]
```

## Templates

### Bronze-Silver-Gold (Medallion Architecture)

```python
from databricks.labs.lakebridge.dlt.templates import get_template

template = get_template("bronze_silver_gold")
# Customize and save
```

Features:
- Bronze: Raw data ingestion with Auto Loader
- Silver: Cleaned data with data quality expectations
- Gold: Business-level aggregates

### CDC Pipeline

Features:
- Change Data Capture event ingestion
- SCD Type 1 (overwrite) handling
- SCD Type 2 (history tracking) with `__START_AT` / `__END_AT`
- Current records view

### MSSQL Migration

Features:
- JDBC connection to SQL Server
- Multi-table ingestion
- Customer/Order/Product example schema
- Daily sales aggregation

## Pipeline Configuration Options

```python
config = DLTPipelineConfig(
    name="My Pipeline",
    notebook_path="/Workspace/path/to/notebook",
    
    # Unity Catalog target
    target_catalog="catalog_name",
    target_schema="schema_name",
    
    # Compute options
    serverless=True,          # Use serverless compute
    photon=True,              # Enable Photon acceleration
    
    # Or manual cluster config
    num_workers=4,
    node_type_id="i3.xlarge",
    
    # Pipeline settings
    development=True,         # Development mode (faster iterations)
    continuous=False,         # Triggered vs continuous
    channel="CURRENT",        # CURRENT or PREVIEW
    
    # Spark configuration
    spark_conf={
        "spark.sql.shuffle.partitions": "10"
    },
)
```

## End-to-End Migration Workflow

1. **Transpile SQL** (existing lakebridge functionality)
   ```bash
   databricks labs lakebridge transpile \
     --source-dialect tsql \
     --input-source ./source_sql/ \
     --output-folder ./transpiled/
   ```

2. **Generate DLT Notebook**
   ```bash
   databricks labs lakebridge dlt-generate \
     --input-dir ./transpiled/ \
     --output-file ./dlt_pipeline.py \
     --catalog prod_catalog \
     --schema sales
   ```

3. **Deploy to Databricks**
   ```bash
   databricks labs lakebridge dlt-deploy \
     --notebook-path ./dlt_pipeline.py \
     --workspace-path /Workspace/Shared/migrations/sales \
     --pipeline-name "Sales Migration" \
     --target-catalog prod_catalog \
     --target-schema sales
   ```

4. **Run and Monitor**
   ```bash
   databricks labs lakebridge dlt-run \
     --pipeline-id <id> \
     --wait
   ```

## Output Files

| File Type | Description |
|-----------|-------------|
| `*.py` | DLT notebook (Python with Databricks magic commands) |
| Pipeline ID | Returned from create/deploy operations |
| Run reports | Available in Databricks UI |

## Best Practices

1. **Use Unity Catalog**: Always specify `target_catalog` and `target_schema`
2. **Start in Development Mode**: Set `development=True` for faster iterations
3. **Add Data Quality**: Use expectations to validate data at each layer
4. **Follow Medallion Architecture**: Bronze → Silver → Gold pattern
5. **Use Serverless**: `serverless=True` for simpler infrastructure
6. **Test Incrementally**: Run individual tables before full pipeline
