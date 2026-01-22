# WakeCapDW Migration Project

This directory contains the production scripts and pipelines for migrating WakeCapDW from SQL Server to Databricks Delta Lake.

## Production Scripts

| Category | Script | Purpose |
|----------|--------|---------|
| **Setup** | `setup_migration.py` | Initial setup with secrets and JDBC configuration |
| **Setup** | `deploy_timescaledb_bronze.py` | Deploy TimescaleDB bronze layer |
| **Setup** | `run_setup.py` | Quick setup runner |
| **Pipeline** | `start_pipeline.py` | Start DLT pipelines |
| **Monitor** | `monitor_pipeline.py` | Monitor pipeline execution |
| **Monitor** | `monitor_optimized_job.py` | Monitor job execution |
| **Monitor** | `check_watermarks.py` | Verify watermark values |
| **Jobs** | `run_bronze_all.py` | Run all bronze table loads |
| **Jobs** | `run_bronze_raw.py` | Run raw bronze table loads |
| **Jobs** | `deploy_optimized_pipeline.py` | Deploy optimized pipeline |
| **Jobs** | `run_optimized_job.py` | Run optimized job |
| **Analysis** | `analyze_sql.py` | Analyze SQL complexity |
| **Analysis** | `extract_sql_objects.py` | Extract SQL objects from source |
| **Analysis** | `transpile_sql.py` | Transpile SQL to Spark SQL |
| **Generation** | `generate_dlt_pipeline.py` | Generate DLT notebooks |
| **Deployment** | `deploy_to_databricks.py` | Deploy to Databricks workspace |
| **Config** | `configure_secrets.py` | Configure Databricks secrets |

## Directory Structure

```
migration_project/
├── pipelines/
│   ├── dlt/                    # DLT layer definitions
│   │   ├── bronze_all_tables.py
│   │   ├── streaming_dimensions.py
│   │   ├── streaming_facts.py
│   │   ├── batch_calculations.py
│   │   ├── silver_dimensions.py
│   │   ├── silver_facts.py
│   │   └── gold_views.py
│   ├── notebooks/              # Calculation notebooks
│   │   ├── hierarchy_processor.py
│   │   ├── h3_calculations.py
│   │   ├── scheduling_calculator.py
│   │   ├── time_series_processor.py
│   │   ├── golden_record_builder.py
│   │   └── kpi_calculator.py
│   ├── udfs/                   # Python UDFs
│   ├── security/               # Row-level security filters
│   ├── timescaledb/           # TimescaleDB bronze loading
│   │   ├── dlt_timescaledb_bronze.py
│   │   ├── notebooks/
│   │   │   ├── bronze_loader_optimized.py
│   │   │   ├── bronze_loader_dimensions.py
│   │   │   ├── bronze_loader_facts.py
│   │   │   └── bronze_loader_assignments.py
│   │   ├── src/               # Loader modules
│   │   └── config/            # Table registries
│   └── wakecap_migration_pipeline.py  # Main pipeline
├── databricks/
│   ├── config/                # Configuration files
│   │   └── wakecapdw_tables_complete.json
│   ├── jobs/                  # Job definitions
│   ├── notebooks/             # Databricks notebooks
│   ├── scripts/               # Deployment scripts
│   └── dlt_pipeline_config.json
├── adf/                       # Azure Data Factory (legacy)
├── DEPLOYMENT_GUIDE.md        # Deployment instructions
├── MIGRATION_PLAN.md          # Migration plan
├── MIGRATION_STATUS.md        # Current status
└── README.md                  # This file
```

## Quick Start

### 1. Initial Setup

```bash
python setup_migration.py --host <databricks-host> --token <token>
```

### 2. Deploy Pipeline

```bash
python deploy_to_databricks.py
```

### 3. Start Pipeline

```bash
python start_pipeline.py --pipeline-id <id>
```

### 4. Monitor Progress

```bash
python monitor_pipeline.py --pipeline-id <id>
```

## Documentation

- **DEPLOYMENT_GUIDE.md** - Step-by-step deployment instructions
- **MIGRATION_PLAN.md** - Detailed migration plan and architecture
- **MIGRATION_STATUS.md** - Current migration status and progress

## Related Documentation

See the main repository documentation:
- `/specs/` - Engineering specifications
- `/.claude/skills/` - Lakebridge skill documentation
