# Databricks notebook source
print("Step 1: About to run loader module...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

print("Step 2: Classes after %run...")
print(f"  TimescaleDBCredentials available: {'TimescaleDBCredentials' in dir()}")
print(f"  TimescaleDBLoaderV2 available: {'TimescaleDBLoaderV2' in dir()}")

# COMMAND ----------

print("Step 3: Check method signature...")
import inspect
sig = inspect.signature(TimescaleDBCredentials.from_databricks_secrets)
print(f"  from_databricks_secrets signature: {sig}")

# COMMAND ----------

print("Step 4: Try calling with dbutils...")
try:
    print(f"  dbutils available: {dbutils}")
    creds = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
    print(f"  Credentials created: host={creds.host}")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("All tests passed!")
