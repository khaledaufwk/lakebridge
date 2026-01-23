# Databricks notebook source
print("Step 1: Loading loader module...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

print("Step 2: Loading credentials (passing dbutils explicitly)...")
try:
    credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
    print(f"  Host: {credentials.host}")
    print(f"  Port: {credentials.port}")
    print(f"  Database: {credentials.database}")
    print(f"  Username: {credentials.username}")
    print("Credentials loaded!")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Step 3: Testing JDBC URL...")
jdbc_url = f"jdbc:postgresql://{credentials.host}:{credentials.port}/{credentials.database}"
print(f"JDBC URL: {jdbc_url}")

# COMMAND ----------

print("All tests passed!")
