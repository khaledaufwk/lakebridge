# Databricks notebook source
print("Testing DeviceLocation queries...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)

# COMMAND ----------

print("Query 1: Get DeviceLocation columns...")
try:
    df = spark.read.jdbc(
        credentials.jdbc_url,
        """(SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'DeviceLocation'
            ORDER BY ordinal_position) as cols""",
        properties={
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver"
        }
    )
    df.show(30, truncate=False)
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

print("Query 2: Sample DeviceLocation with all columns...")
try:
    df = spark.read.jdbc(
        credentials.jdbc_url,
        '''(SELECT * FROM "DeviceLocation" LIMIT 5) as sample''',
        properties={
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver"
        }
    )
    df.printSchema()
    df.show(truncate=False)
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

print("Done!")
