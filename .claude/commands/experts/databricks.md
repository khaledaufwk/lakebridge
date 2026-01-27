# Databricks Expert

Reference this expert when writing Databricks notebooks or scripts that interact with Databricks APIs.

## Critical: PERSIST/CACHE Not Supported on Serverless Compute

**Problem:** Serverless compute in Databricks does not support `PERSIST TABLE`, `CACHE TABLE`, or DataFrame `cache()` / `persist()` operations. Using these results in the error:

```
[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000
```

This affects both SQL commands and PySpark DataFrame operations:

```python
# DON'T DO THIS on serverless compute - All of these fail:
df.cache()
df.persist()
df.persist(StorageLevel.MEMORY_AND_DISK)
spark.sql("CACHE TABLE my_table")
spark.sql("PERSIST TABLE my_table")
```

**Solution:** Remove all cache/persist operations when running on serverless compute. The serverless infrastructure manages memory automatically:

```python
# DO THIS - Remove cache() calls entirely
# df.cache()  # Removed - not supported on serverless
df_count = df.count()

# If you need to reuse a DataFrame multiple times, just reference it directly
# Spark will optimize the execution plan automatically

# At the end, remove unpersist() calls too
# df.unpersist()  # Removed - no longer needed
```

**Alternative for classic compute:** If you must use caching and can switch to classic job clusters or all-purpose clusters, caching works as expected:

```python
# Only on classic compute (not serverless):
df.cache()
# ... multiple operations on df ...
df.unpersist()
```

**Best Practice:** Design notebooks to work without caching by default. Serverless compute provides auto-scaling and optimized query execution that often performs better than manual caching anyway.

---

## Critical: Target Table Schema Mismatch (INT vs STRING for UUIDs)

**Problem:** When migrating from TimescaleDB (which uses UUID strings) to Gold layer tables that were originally designed with INT columns for IDs, the MERGE fails:

```
[CAST_INVALID_INPUT] The value '0f52747e-9de9-421d-8aa8-fa9d01d66196' of the type "STRING" cannot be cast to "INT" because it is malformed.
```

**Solution:** Use STRING type for all ID columns that may contain UUIDs, and add logic to detect and recreate tables with wrong schema:

```python
# Target table schema - use STRING for ID columns
target_schema = """
    FactShiftCombinedID BIGINT GENERATED ALWAYS AS IDENTITY,
    ProjectID STRING NOT NULL,
    WorkerID STRING NOT NULL,
    CrewID STRING,
    ...
"""

# Check if table exists with wrong schema and recreate if needed
table_needs_recreate = False
try:
    schema_df = spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
    project_id_type = schema_df.filter("col_name = 'ProjectID'").select("data_type").collect()
    if project_id_type and project_id_type[0][0].upper() == "INT":
        print(f"[WARN] Table exists with INT schema, need to recreate with STRING")
        table_needs_recreate = True
except:
    table_needs_recreate = True

if table_needs_recreate:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    spark.sql(f"CREATE TABLE {TARGET_TABLE} ({target_schema}) USING DELTA")
```

---

## Critical: Gold Layer Column Name Differences (WakeCap)

**Problem:** The `gold_fact_workers_history` table uses geographic column names (`Latitude`, `Longitude`) instead of generic coordinate names (`X`, `Y`). Using the wrong column names results in:

```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `X` cannot be resolved. Did you mean one of the following? [`Latitude`, `Longitude`, ...]
```

**Solution:** Use the correct column names from the gold layer schema:

```python
# DON'T DO THIS - X/Y don't exist in gold_fact_workers_history
history_df = spark.table(SOURCE_WORKERS_HISTORY).select(
    F.col("X").alias("LocationX"),
    F.col("Y").alias("LocationY")
)

# DO THIS - Use Latitude/Longitude
history_df = spark.table(SOURCE_WORKERS_HISTORY).select(
    F.col("Latitude").alias("LocationX"),
    F.col("Longitude").alias("LocationY")
)
```

**Best Practice:** Always check the actual schema before selecting columns:
```python
# Check available columns
print(spark.table("wakecap_prod.gold.gold_fact_workers_history").columns)
```

---

## Critical: Column DEFAULT Values Not Supported Without Feature Flag

**Problem:** Delta Lake requires the `allowColumnDefaults` feature to be explicitly enabled before using `DEFAULT` values in CREATE TABLE statements. Without it, you'll get:

```
[WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED] Failed to execute CREATE TABLE command because it assigned a column DEFAULT value, but the corresponding table feature was not enabled.
```

**Solution:** Remove DEFAULT values from CREATE TABLE statements:

```python
# DON'T DO THIS - DEFAULT values require feature flag
target_schema = """
    WatermarkUTC TIMESTAMP DEFAULT current_timestamp(),
    CreatedAt TIMESTAMP DEFAULT current_timestamp(),
    UpdatedAt TIMESTAMP DEFAULT current_timestamp()
"""

# DO THIS - Remove DEFAULT values, set them in the INSERT/MERGE statement instead
target_schema = """
    WatermarkUTC TIMESTAMP,
    CreatedAt TIMESTAMP,
    UpdatedAt TIMESTAMP
"""

# Then in your MERGE statement, use current_timestamp() directly:
"""
WHEN NOT MATCHED THEN INSERT (..., WatermarkUTC, CreatedAt, UpdatedAt)
VALUES (..., current_timestamp(), current_timestamp(), current_timestamp())
"""
```

**Alternative:** Enable the feature on the table first (not recommended for new tables):
```sql
ALTER TABLE tableName SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
```

---

## Critical: DBUtils in USER_ISOLATION Mode

**Problem:** `DBUtils(spark)` does not work properly in USER_ISOLATION (Shared) cluster mode. The following pattern will fail:

```python
# DON'T DO THIS - Fails in USER_ISOLATION mode
from pyspark.dbutils import DBUtils
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)  # Will fail or return limited functionality
```

**Solution:** In Databricks notebooks, `dbutils` is already available as a global variable. Always:

1. **Pass dbutils explicitly** from the notebook to any module/class that needs it
2. **Try multiple fallback sources** if dbutils is not passed

### Recommended Pattern for Loading Secrets

```python
@classmethod
def from_databricks_secrets(cls, scope: str, dbutils=None) -> "YourCredentialsClass":
    """Load credentials from Databricks secrets.

    Args:
        scope: The secret scope name
        dbutils: Optional dbutils instance. If not provided, will try to get from globals
                or create one. Pass the notebook's dbutils for USER_ISOLATION clusters.
    """
    try:
        # Try to get dbutils from various sources
        if dbutils is None:
            # First try to get from IPython user namespace (notebook globals)
            try:
                from IPython import get_ipython
                ipython = get_ipython()
                if ipython is not None:
                    dbutils = ipython.user_ns.get('dbutils')
            except:
                pass

        if dbutils is None:
            # Try to get from builtins
            import builtins
            dbutils = getattr(builtins, 'dbutils', None)

        if dbutils is None:
            # Last resort: try to create DBUtils (works in SINGLE_USER mode only)
            from pyspark.dbutils import DBUtils
            spark = SparkSession.getActiveSession()
            dbutils = DBUtils(spark)

        # Now use dbutils.secrets.get()
        return cls(
            host=dbutils.secrets.get(scope, "your-host-key"),
            password=dbutils.secrets.get(scope, "your-password-key"),
            # ... other fields
        )
    except Exception as e:
        raise ValueError(f"Failed to load credentials from Databricks secrets: {e}")
```

### In Notebooks - Always Pass dbutils Explicitly

```python
# In your notebook:
# dbutils is already available as a global

# When calling modules that need secrets:
credentials = YourCredentials.from_databricks_secrets("your-scope", dbutils=dbutils)

# When initializing loaders or clients:
loader = YourLoader(credentials, spark=spark, dbutils=dbutils)
```

## Critical: currentRunId() Not Accessible in Python

**Problem:** `currentRunId()` is not whitelisted and cannot be accessed from Python in Databricks notebooks. The following patterns will fail:

```python
# DON'T DO THIS - currentRunId() is not accessible
run_id = dbutils.notebook.getContext().currentRunId().get()

# DON'T DO THIS - Also fails
context = dbutils.notebook.getContext()
run_id = context.currentRunId()
```

There is no supported way to access the pipeline run ID using `dbutils.notebook.getContext()` in Python.

**Solution:** Remove any code that attempts to access `currentRunId()`. If you need to track runs:

1. **Use widget parameters** - Pass a unique identifier as a notebook parameter
2. **Use timestamps** - Generate a unique run identifier based on execution time
3. **Use job/task context** - If running as a job, use job-level identifiers instead

```python
# Alternative: Generate your own run identifier
from datetime import datetime
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Alternative: Use a widget parameter for tracking
dbutils.widgets.text("run_id", "", "Run ID")
run_id = dbutils.widgets.get("run_id") or datetime.now().strftime("%Y%m%d_%H%M%S")
```

## Critical: sys.path.append Doesn't Work with /Workspace Paths

**Problem:** `sys.path.append` does not work with Databricks `/Workspace` paths. You cannot import custom modules from workspace files using this approach:

```python
# DON'T DO THIS - Fails with /Workspace paths
import sys
sys.path.append("/Workspace/migration_project/pipelines/timescaledb/src")
from timescaledb_loader_v2 import TimescaleDBLoaderV2  # ImportError!

# DON'T DO THIS - Also fails
sys.path.insert(0, "/Workspace/my_project/lib")
import my_module  # ImportError!
```

The `/Workspace` filesystem is not a standard filesystem and Python's import system cannot resolve modules from it via `sys.path`.

**Solution:** Package your modules as a wheel and install via `%pip`:

1. **Create a wheel package** from your module:
```bash
# Directory structure
my_package/
├── setup.py
├── my_module/
│   ├── __init__.py
│   └── loader.py
```

2. **Upload the wheel** to a location accessible by the cluster (DBFS, Unity Catalog Volume, or workspace)

3. **Install in notebook** via `%pip`:
```python
# Install from DBFS
%pip install /dbfs/path/to/my_package-0.1.0-py3-none-any.whl

# Install from Unity Catalog Volume
%pip install /Volumes/catalog/schema/volume/my_package-0.1.0-py3-none-any.whl

# Then import normally
from my_module.loader import MyLoader
```

**Alternative - Use %run for simple cases:**
```python
# %run can execute another notebook and share its namespace
%run /Workspace/migration_project/utils/common_functions
```

Note: `%run` executes the entire notebook, not just imports. It's suitable for shared utilities but not for proper module packaging.

## Critical: [CANNOT_DETERMINE_TYPE] Error with createDataFrame

**Problem:** `spark.createDataFrame()` fails with `[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring` when:
1. The data list is empty
2. A column has all `None` values (Spark can't infer the type)

This commonly occurs when creating summary DataFrames where an "error" column is `None` for all successful operations:

```python
# DON'T DO THIS - Fails when all errors are None or results is empty
summary_data = [
    Row(
        table=r.table_name,
        status=r.status,
        rows_loaded=r.rows_loaded,
        error=r.error_message if r.error_message else None  # All None = type inference fails!
    )
    for r in results
]
summary_df = spark.createDataFrame(summary_data)  # PySparkValueError!
```

**Solution:** Always provide an explicit schema when creating DataFrames that may have nullable columns or empty data:

```python
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Define explicit schema - this ensures type is known even when all values are NULL
summary_schema = StructType([
    StructField("table", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("error", StringType(), True)  # Explicit StringType for nullable column
])

# Check for empty results to avoid other issues
if results:
    summary_data = [
        Row(
            table=r.table_name,
            status=r.status,
            rows_loaded=r.rows_loaded,
            duration_seconds=r.duration or 0.0,
            error=r.error_message[:100] if r.error_message else None
        )
        for r in results
    ]
    summary_df = spark.createDataFrame(summary_data, schema=summary_schema)
    display(summary_df)
else:
    print("No results to display")
```

**Key points:**
- Always use explicit `schema=` parameter when columns may be all-NULL
- Check if the data list is empty before creating the DataFrame
- Use appropriate Spark types: `StringType()`, `LongType()`, `DoubleType()`, `IntegerType()`, `BooleanType()`, `TimestampType()`

---

## Unnecessary: %pip install pyyaml

**Problem:** Notebooks often include `%pip install pyyaml --quiet` or similar pip install commands for PyYAML. This is unnecessary and wasteful because PyYAML is already pre-installed on all Databricks clusters.

```python
# DON'T DO THIS - PyYAML is already installed on the cluster
%pip install pyyaml --quiet
%pip install pyyaml
```

**Solution:** Remove all `%pip install pyyaml` commands from notebooks. PyYAML is part of the default Databricks runtime and is always available.

```python
# Just import it directly - no installation needed
import yaml

# Use it normally
config = yaml.safe_load(config_string)
```

**Pre-installed packages:** Many common packages are already available in Databricks Runtime. Before adding `%pip install`, check if the package is already included in the runtime. See [Databricks Runtime release notes](https://docs.databricks.com/release-notes/runtime/releases.html) for the full list.

## Critical: Azure VM Quota Exceeded When Creating Job Clusters

**Problem:** Job clusters fail to start with `AZURE_QUOTA_EXCEEDED_EXCEPTION` when the Azure subscription doesn't have enough vCPU quota for the specified VM family. Error message:

```
AZURE_QUOTA_EXCEEDED_EXCEPTION (CLIENT_ERROR): The VM size you are specifying is not available.
QuotaExceeded: Operation could not be completed as it results in exceeding approved
standardDSv2Family Cores quota. Current Limit: 10, Current Usage: 8, Additional Required: 16
```

**Solution:** Use an existing all-purpose cluster instead of creating new job clusters:

1. **Identify available cluster** with sufficient quota (e.g., DSv3 family instead of DSv2):
   ```python
   clusters = w.clusters.list()
   for c in clusters:
       print(f'{c.cluster_id}: {c.cluster_name} - {c.state}')
   ```

2. **Update job to use existing cluster** instead of `job_clusters`:
   ```python
   from databricks.sdk.service.jobs import JobSettings, Task, NotebookTask

   # Use existing_cluster_id instead of job_cluster_key
   w.jobs.update(
       job_id=job_id,
       new_settings=JobSettings(
           name=job.settings.name,
           tasks=[
               Task(
                   task_key="my_task",
                   existing_cluster_id="0118-134705-lklfkwvh",  # Use existing cluster
                   notebook_task=NotebookTask(
                       notebook_path="/path/to/notebook"
                   )
               )
           ],
           job_clusters=None,  # Remove job clusters
           schedule=job.settings.schedule
       )
   )
   ```

**Azure Quota by VM Family:**
| Family | Common Types | Notes |
|--------|-------------|-------|
| DSv2 | Standard_DS3_v2, Standard_DS4_v2 | Often limited quota |
| DSv3 | Standard_D4s_v3, Standard_D8s_v3 | Usually more quota available |
| Dv5 | Standard_D4s_v5, Standard_D8s_v5 | Newer generation |

**Best Practice:** Configure a dedicated all-purpose cluster for migration workloads (e.g., "Migrate Compute - Khaled Auf") using a VM family with sufficient quota (DSv3), then reference it in jobs via `existing_cluster_id`.

---

## Cluster Access Modes

| Mode | DBUtils(spark) Works? | Best Practice |
|------|----------------------|---------------|
| SINGLE_USER | Yes | Can use DBUtils(spark) |
| USER_ISOLATION (Shared) | No | Must use notebook's global `dbutils` |
| NO_ISOLATION | Limited | Use notebook's global `dbutils` |

## Cluster Library Management

### Checking Cluster Libraries Before Notebook Execution

**Best Practice:** Before deploying notebooks that depend on custom libraries (like wheels from Unity Catalog Volumes), verify the target cluster has those libraries installed.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host=host, token=token)

# Find cluster by name
clusters = w.clusters.list()
cluster = next((c for c in clusters if c.cluster_name == "Migrate Compute - Khaled Auf"), None)

if cluster:
    cluster_id = cluster.cluster_id

    # Get installed libraries
    lib_statuses = w.libraries.cluster_status(cluster_id=cluster_id)

    installed = []
    for lib_status in lib_statuses.library_statuses or []:
        lib = lib_status.library
        status = lib_status.status.value

        if lib.whl:
            installed.append({"type": "whl", "path": lib.whl, "status": status})
        elif lib.pypi:
            installed.append({"type": "pypi", "package": lib.pypi.package, "status": status})

    print(f"Installed libraries: {installed}")
```

### Installing Libraries on a Cluster

```python
from databricks.sdk.service.compute import Library

# Install a wheel from Unity Catalog Volume
w.libraries.install(
    cluster_id=cluster_id,
    libraries=[
        Library(whl="/Volumes/wakecap_prod/migration/libs/timescaledb_loader-2.0.0-py3-none-any.whl")
    ]
)

# Install a PyPI package
from databricks.sdk.service.compute import PythonPyPiLibrary
w.libraries.install(
    cluster_id=cluster_id,
    libraries=[
        Library(pypi=PythonPyPiLibrary(package="requests"))
    ]
)
```

### Pre-validation Pattern for Build Skills

Configure required libraries in `credentials_template.yml`:

```yaml
compute:
  cluster_name: "Migrate Compute - Khaled Auf"
  required_libraries:
    - name: "timescaledb_loader"
      type: "whl"
      path: "/Volumes/wakecap_prod/migration/libs/timescaledb_loader-2.0.0-py3-none-any.whl"
    - name: "PyYAML"
      type: "pypi"
      package: "pyyaml"
      preinstalled: true  # Skip validation - pre-installed on Databricks
```

Then validate before building:

```python
from scripts.credentials import CredentialsManager
from scripts.databricks_client import DatabricksClient

creds = CredentialsManager().load()
client = DatabricksClient(host=creds.databricks.host, token=creds.databricks.token)

if creds.compute:
    cluster = client.get_cluster_by_name(creds.compute.cluster_name)
    result = client.ensure_cluster_libraries(
        cluster_id=cluster["cluster_id"],
        required_libraries=creds.compute.get_required_libraries(),
        auto_install=True
    )
    if not result["success"]:
        raise RuntimeError(f"Missing libraries: {result['missing']}")
```

## Secret Scopes

### Creating Secrets via CLI

```bash
# Create secret scope
databricks secrets create-scope your-scope-name

# Store secrets
databricks secrets put-secret your-scope-name host --string-value "your-host"
databricks secrets put-secret your-scope-name password --string-value "your-password"
```

### Using Azure Key Vault-backed Scopes

```bash
# Link to Azure Key Vault
databricks secrets create-scope your-scope-name \
    --scope-backend-type AZURE_KEYVAULT \
    --resource-id /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault}
```

## Widget Parameters

Widgets are the standard way to parameterize notebooks:

```python
# Create widgets
dbutils.widgets.text("load_mode", "incremental", "Load Mode")
dbutils.widgets.dropdown("category", "ALL", ["ALL", "dimensions", "facts"], "Category")

# Get widget values
load_mode = dbutils.widgets.get("load_mode")
category = dbutils.widgets.get("category")
```

## File System Operations

```python
# List files
dbutils.fs.ls("/mnt/data/")

# Read file info
dbutils.fs.head("/mnt/data/file.txt", 1000)

# Copy files
dbutils.fs.cp("/source/path", "/dest/path", recurse=True)
```

## Notebook Utilities

```python
# Run another notebook
dbutils.notebook.run("/path/to/notebook", timeout_seconds=3600, arguments={"param": "value"})

# Exit notebook with value
dbutils.notebook.exit("Success")
```

---

## Critical: Type Mismatch When Joining UUID Strings with INT Columns

**Problem:** When joining columns where one side contains UUID strings (from TimescaleDB or other sources) and the other side is an INT (from SQL Server or dimension tables), Spark attempts to cast the UUID string to BIGINT, which fails with:

```
[CAST_INVALID_INPUT] The value '26bbd5af-45a6-429b-8604-c6e54e24a80c' of the type "STRING"
cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax,
or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead.
SQLSTATE: 22018
```

This commonly occurs in dimension lookups where:
- The fact table has UUID-based foreign keys (e.g., `ApprovedById` as UUID string)
- The dimension table has INT-based primary keys (e.g., `WorkerId` as INT)

```python
# DON'T DO THIS - UUID string cannot be cast to INT
approved_by_lookup_df = worker_df.select(
    F.col("WorkerId").alias("dim_ApprovedByWorkerID"),
    F.col("WorkerId").alias("dim_ApprovedByExtID")  # INT column
)

# Join fails when ApprovedById is a UUID string
att_df.join(approved_by_lookup_df,
    F.col("ApprovedById") == F.col("dim_ApprovedByExtID"),  # UUID vs INT = error!
    "left"
)
```

**Solution:** Cast the INT column to STRING before the join:

```python
# Cast INT to STRING to match UUID format
approved_by_lookup_df = worker_df.select(
    F.col("WorkerId").alias("dim_ApprovedByWorkerID"),
    F.col("WorkerId").cast("string").alias("dim_ApprovedByExtID")  # Cast to STRING
)

# Now the join works correctly
att_df.join(approved_by_lookup_df,
    F.col("ApprovedById") == F.col("dim_ApprovedByExtID"),  # STRING vs STRING = OK
    "left"
)
```

**Alternative - Use try_cast for fault tolerance:**

```python
# If you want to handle malformed UUIDs gracefully
att_df.withColumn("ApprovedById_int", F.expr("try_cast(ApprovedById as BIGINT)"))
```

---

## Critical: Silver-to-Gold Column Naming Mismatches (WakeCap Migration)

**Problem:** When migrating from SQL Server (WakeCapDW) to Databricks using a medallion architecture, the Silver layer column names differ from the original SQL Server column names. Gold layer notebooks that assume SQL Server column names will fail with:

```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter
with name `Worker` cannot be resolved. Did you mean one of the following?
[`WorkerName`, `WorkerId`, `WorkerCode`, ...]
```

**Root Cause:** The TimescaleDB bronze layer → silver layer transformation renames columns to follow a consistent naming convention. Gold layer notebooks must reference the *silver* column names, not the original SQL Server names.

### Silver Layer Column Name Reference (WakeCap)

| Table | Original SQL Server | Silver Layer Name |
|-------|--------------------|--------------------|
| **silver_worker** | Worker | WorkerName |
| | ExtWorkerId | *(not available)* |
| | ValidFrom/ValidTo | *(use CreatedAt/UpdatedAt)* |
| **silver_floor** | Floor | FloorName |
| | FloorNumber | *(not available - derive from FloorName)* |
| **silver_zone** | Zone | ZoneName |
| **silver_crew** | Crew | CrewName |
| **silver_device** | Device | DeviceName |
| **silver_crew_type** | CrewType | CrewTypeName |
| **silver_workshift** | Workshift | WorkshiftName |
| **silver_workshift_schedule** | StartTime | StartHour *(decimal hours)* |
| | EndTime | EndHour *(decimal hours)* |
| | DayOfWeek | WorkshiftDayId |
| **silver_crew_composition** | ValidFrom | CreatedAt |
| | ValidTo | DeletedAt |
| **silver_resource_device** | ValidFrom | AssignedAt |
| | ValidTo | UnassignedAt |
| | ProjectId | *(not available)* |
| **silver_workshift_resource_assignment** | ValidFrom | EffectiveDate |
| | ValidTo | *(not available - use latest EffectiveDate)* |
| **silver_zone_category** | ZoneCategoryId | Id |
| | ZoneCategoryName | ZoneCategoryName |

### Common Fix Patterns

**1. Name Columns (Worker, Floor, Zone, Crew, Device):**
```python
# DON'T DO THIS - SQL Server column names
F.col("Worker").alias("WorkerName")
F.col("Floor").alias("FloorName")

# DO THIS - Silver layer already has these names
F.col("WorkerName")
F.col("FloorName")
```

**2. Validity Date Columns (ValidFrom/ValidTo):**
```python
# DON'T DO THIS - SQL Server pattern
.filter(F.col("ValidTo").isNull())
.orderBy(F.col("ValidFrom").desc())

# DO THIS - Check the specific silver table
# For crew_composition: CreatedAt/DeletedAt
.filter(F.col("DeletedAt").isNull())
.orderBy(F.col("CreatedAt").desc())

# For resource_device: AssignedAt/UnassignedAt
.filter(F.col("UnassignedAt").isNull())
.orderBy(F.col("AssignedAt").desc())

# For workshift_resource_assignment: EffectiveDate only
.orderBy(F.col("EffectiveDate").desc())
```

**3. Time Columns (workshift_schedule):**

**IMPORTANT:** In the Silver layer, `StartHour` and `EndHour` are stored as **TIMESTAMP** (not decimal hours). This causes `DATATYPE_MISMATCH` errors if you try to use arithmetic operations:

```
[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "FLOOR(StartHour)" due to data type mismatch: The first parameter requires the ("DOUBLE" or "DECIMAL" or "BIGINT") type, however "StartHour" has the type "TIMESTAMP"
```

```python
# DON'T DO THIS - Assumes StartHour is DOUBLE
F.floor(F.col("StartHour"))
F.col("StartHour") % 1 * 60  # Arithmetic on TIMESTAMP fails

# DO THIS - Use date_format for display
F.date_format(F.col("StartHour"), "HH:mm")  # Outputs "08:30"

# DO THIS - Convert TIMESTAMP to decimal hours for calculations
F.hour(F.col("StartHour")) + F.minute(F.col("StartHour")) / 60.0  # e.g., 8.5

# Full pattern for display and calculations:
ws_df = ws_df.withColumn(
    "StartTimeDisplay",
    F.when(
        F.col("StartHour").isNotNull(),
        F.date_format(F.col("StartHour"), "HH:mm")
    ).otherwise(F.lit(None))
).withColumn(
    "StartHourDecimal",
    F.hour(F.col("StartHour")) + F.minute(F.col("StartHour")) / 60.0
)
```

**4. Missing Columns - Compute or Skip:**

**IMPORTANT:** When extracting numeric parts from strings, handle empty results. `regexp_replace` returns empty string `''` when no matches, which cannot be cast to INT.

```python
# DON'T DO THIS - Empty string cast to INT fails
floor_df = floor_df.withColumn(
    "FloorNumericPart",
    F.regexp_replace(F.col("FloorName"), "[^0-9]", "").cast("int")  # FAILS for "Ground"
)

# Error: [CAST_INVALID_INPUT] The value '' of the type "STRING" cannot be cast to "INT"

# DO THIS - Handle empty strings with WHEN condition
floor_df = floor_df.withColumn(
    "_numeric_str",
    F.regexp_replace(F.col("FloorName"), "[^0-9]", "")
).withColumn(
    "FloorNumericPart",
    F.when(
        (F.col("_numeric_str").isNotNull()) & (F.col("_numeric_str") != ""),
        F.col("_numeric_str").cast("int")
    ).otherwise(F.lit(None).cast("int"))
).drop("_numeric_str")

# ExtWorkerId doesn't exist - skip or use WorkerId/WorkerCode
```

### Verification Pattern

Before writing Gold notebooks, verify Silver column names:
```python
# Check actual columns in silver table
silver_cols = spark.table("wakecap_prod.silver.silver_worker").columns
print("Available columns:", silver_cols)

# Or read from silver_tables.yml config
import yaml
with open("migration_project/pipelines/silver/config/silver_tables.yml") as f:
    config = yaml.safe_load(f)
    for table in config["tables"]:
        if table["name"] == "silver_worker":
            for col in table["columns"]:
                print(f"{col['source']} -> {col['target']}")
```

### Impact on Views

Views that depend on other Gold tables (`gold_vw_*` depending on `gold_fact_*`) must ensure the source Gold table:
1. Exists and has been built successfully
2. Contains the expected columns

If a view fails, check if its source fact table was built correctly first.

---

## Critical: UUID Case Sensitivity in String Comparisons

**Problem:** UUID strings from different sources may have different cases (lowercase vs uppercase). Spark string comparison is **case-sensitive**, causing joins to fail silently (0 rows matched) even when the UUIDs are logically the same.

Common scenario:
- TimescaleDB stores UUIDs in lowercase: `57eee601-8c8f-4b2a-9a1e-abc123456789`
- SQL Server stores UUIDs in uppercase: `57EEE601-8C8F-4B2A-9A1E-ABC123456789`

```python
# DON'T DO THIS - Case-sensitive comparison fails to match
project_lookup_df = project_df.select(
    F.col("ProjectID").alias("dim_ProjectID"),
    F.col("ExtProjectID").alias("dim_ExtProjectID")  # Uppercase UUIDs
)

# This join returns 0 rows even though the UUIDs are the same (different case)
att_with_project = att_df.join(
    project_lookup_df,
    F.col("att.ProjectId") == F.col("dim_ExtProjectID"),  # lowercase vs UPPERCASE
    "inner"
)
```

**Solution:** Normalize both sides to the same case using `F.upper()` or `F.lower()`:

```python
# Normalize to uppercase for case-insensitive comparison
att_with_project = att_df.alias("att").join(
    project_lookup_df.alias("p"),
    F.upper(F.col("att.ProjectId")) == F.upper(F.col("p.dim_ExtProjectID")),
    "inner"
)
```

**Alternative - Pre-normalize in the lookup DataFrame:**

```python
# Normalize once in the lookup, use in all joins
project_lookup_df = project_df.select(
    F.col("ProjectID").alias("dim_ProjectID"),
    F.upper(F.col("ExtProjectID")).alias("dim_ExtProjectID_upper")
)

# Then normalize only the fact side in joins
att_df.join(
    project_lookup_df,
    F.upper(F.col("ProjectId")) == F.col("dim_ExtProjectID_upper"),
    "inner"
)
```

**Debugging tip:** When joins return 0 rows unexpectedly, check sample values from both sides:

```python
# Debug: Check case of UUIDs
fact_df.select(F.col("ProjectId")).distinct().show(5, truncate=False)
dim_df.select(F.col("ExtProjectID")).distinct().show(5, truncate=False)
```

---

## Critical: UUID vs BIGINT Type Mismatch in Joins

**Problem:** When joining tables from different sources (e.g., TimescaleDB vs SQL Server), ID columns may have different types:
- TimescaleDB uses **UUID strings** for primary/foreign keys
- SQL Server uses **BIGINT** (auto-increment integers) for primary/foreign keys
- Spark attempts implicit casting during joins, leading to `CAST_INVALID_INPUT` errors

Error message:
```
[CAST_INVALID_INPUT] The value 'ebc12fc8-7749-49ff-8bee-8ab4cec3d10b' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018
```

**Root Cause:** Different tables in the Silver layer may have ID columns stored as different types:
- `silver_crew.CrewTypeId` → UUID string (from timescale_crew.CrewTypeId)
- `silver_crew_type.CrewTypeId` → Could be BIGINT (from timescale_crewtype.Id)

When Spark joins these columns, it tries to cast the UUID string to BIGINT, which fails.

### Solution: Cast Both Sides to STRING

Always cast both sides of ID column joins to STRING for safe comparison:

```python
# DON'T DO THIS - May fail with type mismatch
crew_df = crew_df.join(
    crew_type_df,
    F.col("CrewTypeId") == F.col("ct_CrewTypeId"),
    "left"
)

# DO THIS - Cast to string for safe comparison
crew_type_df = spark.table(SOURCE_CREW_TYPE).select(
    F.col("CrewTypeId").cast("string").alias("ct_CrewTypeId"),  # Cast to string
    F.col("CrewTypeName")
)

crew_df = crew_df.join(
    crew_type_df,
    F.col("CrewTypeId").cast("string") == F.col("ct_CrewTypeId"),  # Cast both sides
    "left"
).drop("ct_CrewTypeId")
```

### Apply to All ID Column Joins

This pattern should be applied consistently to all ID column joins:

```python
# ProjectId joins
F.col("ProjectId").cast("string") == F.col("p_ProjectId").cast("string")

# WorkerId joins
F.col("WorkerId").cast("string") == F.col("w_WorkerId").cast("string")

# FloorId joins
F.col("FloorId").cast("string") == F.col("f_FloorId").cast("string")

# ZoneId joins
F.col("ZoneId").cast("string") == F.col("z_ZoneId").cast("string")

# DeviceId joins
F.col("DeviceId").cast("string") == F.col("d_DeviceId").cast("string")

# CrewId joins
F.col("CrewId").cast("string") == F.col("c_CrewId").cast("string")

# Any foreign key ID joins
F.col("ForeignKeyId").cast("string") == F.col("fk_Id").cast("string")
```

### Full Example Pattern

```python
# Add worker details
# Note: IDs may be UUID (string) or BIGINT - cast to string for safe comparison
if opt_status.get("Worker"):
    worker_df = spark.table(SOURCE_WORKER).select(
        F.col("WorkerId").cast("string").alias("w_WorkerId"),
        F.col("WorkerName"),
        F.col("WorkerCode")
    )

    hist_df = hist_df.join(
        worker_df,
        F.col("WorkerId").cast("string") == F.col("w_WorkerId"),
        "left"
    ).drop("w_WorkerId")
    print("[OK] Worker details joined")
```

### When to Use This Pattern

- **Always** when joining ID columns between Silver layer tables
- **Always** when joining Gold tables with Silver dimension tables
- **Especially** when source data comes from different databases (TimescaleDB, SQL Server, PostgreSQL)
- **Even if** you think both columns should be the same type (data sources may have inconsistencies)
