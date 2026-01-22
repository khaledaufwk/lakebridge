![Databricks Labs Lakebridge](/docs/lakebridge/static/img/lakebridge-lockup-white-background.svg)
===============
by [Databricks Labs](https://www.databricks.com/learn/labs)

[![build](https://github.com/databrickslabs/lakebridge/actions/workflows/push.yml/badge.svg)](https://github.com/databrickslabs/lakebridge/actions/workflows/push.yml)
![PyPI - Downloads](https://img.shields.io/pypi/dm/databricks-labs-lakebridge?cacheSeconds=3600)

-----

**Lakebridge** is a comprehensive toolkit for migrating SQL workloads to Databricks. It provides tools for assessment, transpilation, reconciliation, and deployment to Delta Live Tables (DLT).

## 📚 Documentation

Full documentation: https://databrickslabs.github.io/lakebridge/

## 🚀 Quick Start

### Installation

```bash
pip install databricks-labs-lakebridge

# For agent features (experimental)
pip install databricks-labs-lakebridge[agent]
```

### Basic Usage

```bash
# Transpile SQL from MSSQL to Databricks
databricks labs lakebridge transpile \
  --source-dialect tsql \
  --input-source ./sql_files/ \
  --output-folder ./transpiled/

# Generate DLT pipeline from transpiled SQL
databricks labs lakebridge dlt-generate \
  --input-dir ./transpiled/ \
  --output-file ./dlt_pipeline.py \
  --catalog my_catalog \
  --schema my_schema

# Deploy to Databricks
databricks labs lakebridge dlt-deploy \
  --notebook-path ./dlt_pipeline.py \
  --workspace-path /Workspace/migrations/my_pipeline \
  --pipeline-name "My Migration Pipeline"
```

## 📁 Project Structure

```
lakebridge/
├── src/databricks/labs/lakebridge/
│   ├── agent/                    # AI-powered migration workflows (experimental)
│   │   ├── agent_sdk.py          # Claude Agent SDK wrapper
│   │   ├── logging.py            # Structured workflow logging
│   │   ├── runner.py             # High-level workflow runner
│   │   └── workflows/            # Step implementations
│   │       ├── plan_step.py      # /plan command implementation
│   │       ├── build_step.py     # /build command implementation
│   │       ├── review_step.py    # /review command implementation
│   │       ├── fix_step.py       # /fix command implementation
│   │       ├── question_step.py  # /question command implementation
│   │       └── full_workflow.py  # End-to-end orchestration
│   │
│   ├── analyzer/                 # SQL code analysis
│   ├── assessments/              # Pre-migration profiling & assessment
│   ├── connections/              # Database connectors (MSSQL, Snowflake)
│   ├── contexts/                 # Application context management
│   ├── coverage/                 # Transpilation coverage reporting
│   ├── deployment/               # Databricks deployment utilities
│   │
│   ├── dlt/                      # Delta Live Tables deployment
│   │   ├── generator.py          # DLT notebook generation
│   │   ├── deployer.py           # Pipeline deployment to Databricks
│   │   ├── converter.py          # SQL to DLT conversion
│   │   └── templates/            # Pre-built DLT templates
│   │       ├── bronze_silver_gold.py  # Medallion architecture
│   │       ├── cdc_pipeline.py        # CDC/SCD Type 2
│   │       └── mssql_migration.py     # MSSQL migration template
│   │
│   ├── helpers/                  # Utility functions
│   ├── intermediate/             # Intermediate representations
│   ├── reconcile/                # Data reconciliation tools
│   ├── resources/                # Configuration templates & schemas
│   ├── transpiler/               # SQL transpilation engines
│   │   ├── sqlglot/              # SQLGlot-based transpiler
│   │   ├── lsp/                  # Language Server Protocol support
│   │   └── switch_runner.py      # LLM-powered transpilation
│   │
│   └── cli.py                    # Command-line interface
│
├── .claude/                      # AI Agent slash commands
│   └── commands/
│       ├── plan.md               # /plan - Create implementation plans
│       ├── build.md              # /build - Implement plans
│       ├── review.md             # /review - Code review & validation
│       ├── fix.md                # /fix - Fix review issues
│       └── question.md           # /question - Answer codebase questions
│
├── tests/                        # Test suite
│   ├── unit/                     # Unit tests
│   ├── integration/              # Integration tests
│   └── resources/                # Test fixtures
│
└── docs/                         # Documentation site (Docusaurus)
```

## 🔧 CLI Commands

### Transpilation
| Command | Description |
|---------|-------------|
| `transpile` | Transpile SQL from source dialect to Databricks SQL |
| `describe-transpilers` | List available transpiler engines |

### Assessment
| Command | Description |
|---------|-------------|
| `configure-assessment` | Configure assessment pipeline |
| `profile` | Profile source database workloads |

### Reconciliation
| Command | Description |
|---------|-------------|
| `reconcile` | Reconcile data between source and Databricks |
| `configure-secrets` | Configure secrets for reconciliation |

### DLT Deployment
| Command | Description |
|---------|-------------|
| `dlt-generate` | Generate DLT notebook from transpiled SQL |
| `dlt-deploy` | Deploy DLT notebook and create pipeline |
| `dlt-run` | Execute a DLT pipeline |
| `dlt-list` | List DLT pipelines |
| `dlt-status` | Get pipeline status |

### Agent Commands (Experimental)
| Command | Description |
|---------|-------------|
| `agent-plan` | Create AI-generated implementation plan |
| `agent-build` | Implement a plan using AI agent |
| `agent-review` | Review completed work with risk tiers |
| `agent-fix` | Fix issues from review report |
| `agent-question` | Answer questions about codebase |
| `agent-migrate` | Full workflow: plan → build → review → fix |

## 🤖 AI-Powered Migration Workflow

Lakebridge includes experimental AI agent capabilities for automating migrations. Use the slash commands in Claude/Cursor IDE or the CLI commands.

### End-to-End Migration Example

```bash
# Step 1: Plan the migration
databricks labs lakebridge agent-plan \
  --prompt "Migrate WakeCapDW stored procedures to Databricks DLT" \
  --working-dir ./migration_project

# Step 2: Build (implement the plan)
databricks labs lakebridge agent-build \
  --plan-path specs/wakecap-migration.md \
  --working-dir ./migration_project

# Step 3: Review the implementation
databricks labs lakebridge agent-review \
  --prompt "WakeCapDW migration" \
  --plan-path specs/wakecap-migration.md \
  --working-dir ./migration_project

# Step 4: Fix any issues (if review failed)
databricks labs lakebridge agent-fix \
  --prompt "WakeCapDW migration" \
  --plan-path specs/wakecap-migration.md \
  --review-path app_review/review_*.md \
  --working-dir ./migration_project

# Or run the complete workflow in one command:
databricks labs lakebridge agent-migrate \
  --prompt "Migrate WakeCapDW to Databricks DLT" \
  --working-dir ./migration_project
```

### Using Slash Commands (in Cursor/Claude IDE)

```
/plan Migrate WakeCapDW stored procedures to Databricks DLT

/build specs/wakecap-migration.md

/review "WakeCapDW migration" specs/wakecap-migration.md

/fix "WakeCapDW migration" specs/wakecap-migration.md app_review/review.md
```

## 🗄️ Database Connections

Configure database credentials at `~/.databricks/labs/lakebridge/.credentials.yml`:

```yaml
secret_vault_type: local
secret_vault_name: null

mssql:
  database: MyDatabase
  driver: ODBC Driver 18 for SQL Server
  server: myserver.database.windows.net
  port: 1433
  user: myuser
  password: mypassword
  auth_type: sql_authentication
  encrypt: true
  trustServerCertificate: false
  loginTimeout: 30

snowflake:
  account: my_account
  user: myuser
  password: mypassword
  warehouse: my_warehouse

# TimescaleDB (PostgreSQL-based time-series database)
# Used for incremental data loading into Raw Zone
timescaledb:
  host: myserver.timescale.cloud
  port: 5432
  database: mydatabase
  user: myuser
  password: mypassword
  sslmode: require
  # Incremental loading configuration
  incremental:
    enabled: true
    watermark_column: created_at    # Column to track new records
    watermark_type: timestamp       # timestamp or integer
    initial_watermark: null         # Starting point (null = beginning)
```

## 📊 Delta Live Tables Templates

Pre-built templates for common migration patterns:

| Template | Description |
|----------|-------------|
| `bronze_silver_gold.py` | Medallion architecture (raw → cleaned → aggregated) |
| `cdc_pipeline.py` | CDC with SCD Type 1 and Type 2 support |
| `mssql_migration.py` | Complete MSSQL to Databricks migration |

## 🛠️ Development

```bash
# Clone the repository
git clone https://github.com/databrickslabs/lakebridge.git
cd lakebridge

# Install with hatch
pip install hatch
hatch shell

# Run tests
hatch run test

# Format code
hatch run fmt

# Verify code quality
hatch run verify
```

## Contribution

Please see the contribution guidance [here](docs/lakebridge/docs/dev/contributing.md) on how to contribute to the project.

## Project Support

Please note that this project is provided for your exploration only and is not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS, and we do not make any guarantees.

Any issues discovered through the use of this project should be filed as [GitHub Issues](https://github.com/databrickslabs/lakebridge/issues/) on this repository.

## License

See [LICENSE](LICENSE) file.
