# Databricks notebook source
print("Testing JDBC connection...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"Host: {credentials.host}")
print(f"Port: {credentials.port}")
print(f"Database: {credentials.database}")

# COMMAND ----------

print("Building JDBC URL...")
jdbc_url = credentials.jdbc_url
print(f"JDBC URL: {jdbc_url}")

# COMMAND ----------

print("Attempting JDBC connection...")
try:
    test_df = spark.read.jdbc(
        jdbc_url,
        "(SELECT 1 as test_value) as test_query",
        properties={
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver"
        }
    )
    print("Query executed, collecting results...")
    test_df.show()
    print("JDBC connection successful!")
except Exception as e:
    print(f"JDBC ERROR: {e}")
    import traceback
    traceback.print_exc()
    # Don't raise - let's see the error and continue

# COMMAND ----------

print("Done!")
