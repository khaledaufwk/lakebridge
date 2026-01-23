# Databricks notebook source
# MAGIC %md
# MAGIC # Test Small Load

# COMMAND ----------

print("Setting up...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"Credentials loaded: {credentials.host}")

# COMMAND ----------

loader = TimescaleDBLoaderV2(
    spark=spark,
    credentials=credentials,
    target_catalog="wakecap_prod",
    target_schema="raw",
    pipeline_id="test_small_load",
    pipeline_run_id=None,
    table_prefix="timescale_",
    max_retries=3,
    retry_delay=10
)
print("Loader initialized!")

# COMMAND ----------

# Try a simple query to DeviceLocation table first
print("Testing query to DeviceLocation...")
try:
    test_df = spark.read.jdbc(
        credentials.jdbc_url,
        "(SELECT \"Id\", \"DeviceId\", \"GeneratedAt\" FROM \"DeviceLocation\" LIMIT 10) as test",
        properties={
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver"
        }
    )
    print(f"Got {test_df.count()} rows")
    test_df.show()
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Test complete!")
