# Databricks Expert

Reference this expert when writing Databricks notebooks or scripts that interact with Databricks APIs.

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
