# Databricks notebook source
# MAGIC %md
# MAGIC # Test Connection and Loader

# COMMAND ----------

print("Step 1: Testing %run of loader module...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

print("Step 2: Loader module loaded successfully!")
print(f"Classes available: TimescaleDBCredentials, TimescaleDBLoaderV2, TableConfigV2, WatermarkType, LoadStatus")

# COMMAND ----------

print("Step 3: Loading credentials from secrets...")
try:
    credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale")
    print(f"  Host: {credentials.host}")
    print(f"  Port: {credentials.port}")
    print(f"  Database: {credentials.database}")
    print(f"  Username: {credentials.username}")
    print(f"  Password: {'*' * len(credentials.password)}")
    print("Credentials loaded successfully!")
except Exception as e:
    print(f"ERROR loading credentials: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Step 4: Testing JDBC connection...")
try:
    jdbc_url = f"jdbc:postgresql://{credentials.host}:{credentials.port}/{credentials.database}"
    connection_properties = {
        "user": credentials.username,
        "password": credentials.password,
        "driver": "org.postgresql.Driver"
    }

    # Simple query to test connection
    test_df = spark.read.jdbc(
        jdbc_url,
        "(SELECT 1 as test_value) as test_query",
        properties=connection_properties
    )
    test_df.show()
    print("JDBC connection successful!")
except Exception as e:
    print(f"ERROR connecting to database: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("Step 5: Initializing loader...")
try:
    loader = TimescaleDBLoaderV2(
        spark=spark,
        credentials=credentials,
        target_catalog="wakecap_prod",
        target_schema="raw",
        pipeline_id="test_connection",
        pipeline_run_id=None,
        table_prefix="timescale_",
        max_retries=3,
        retry_delay=10
    )
    print("Loader initialized successfully!")
except Exception as e:
    print(f"ERROR initializing loader: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

print("\n" + "="*60)
print("ALL TESTS PASSED!")
print("="*60)
