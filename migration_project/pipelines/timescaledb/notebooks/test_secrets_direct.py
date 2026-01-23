# Databricks notebook source
print("Testing direct access to dbutils.secrets...")

# COMMAND ----------

print("Step 1: Check if dbutils is available...")
print(f"dbutils type: {type(dbutils)}")

# COMMAND ----------

print("Step 2: List secret scopes...")
try:
    scopes = dbutils.secrets.listScopes()
    print(f"Available scopes:")
    for scope in scopes:
        print(f"  - {scope.name}")
except Exception as e:
    print(f"ERROR listing scopes: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

print("Step 3: Get secrets from wakecap-timescale scope...")
try:
    host = dbutils.secrets.get("wakecap-timescale", "timescaledb-host")
    print(f"  Host: {host}")
    port = dbutils.secrets.get("wakecap-timescale", "timescaledb-port")
    print(f"  Port: {port}")
    database = dbutils.secrets.get("wakecap-timescale", "timescaledb-database")
    print(f"  Database: {database}")
    user = dbutils.secrets.get("wakecap-timescale", "timescaledb-user")
    print(f"  User: {user}")
    print("All secrets loaded successfully!")
except Exception as e:
    print(f"ERROR getting secrets: {e}")
    import traceback
    traceback.print_exc()
    raise
