# Databricks notebook source
print("Testing database queries...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

credentials = TimescaleDBCredentials.from_databricks_secrets("wakecap-timescale", dbutils=dbutils)
print(f"Host: {credentials.host}")
print(f"Database: {credentials.database}")

# COMMAND ----------

print("Query 1: Simple test query...")
try:
    df1 = spark.read.jdbc(
        credentials.jdbc_url,
        "(SELECT 1 as val) as t",
        properties={
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver"
        }
    )
    df1.show()
    print("Success!")
except Exception as e:
    print(f"ERROR: {e}")

# COMMAND ----------

print("Query 2: List tables...")
try:
    df2 = spark.read.jdbc(
        credentials.jdbc_url,
        "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 10) as t",
        properties={
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver"
        }
    )
    df2.show(truncate=False)
    print("Success!")
except Exception as e:
    print(f"ERROR: {e}")

# COMMAND ----------

print("Query 3: Count DeviceLocation...")
try:
    df3 = spark.read.jdbc(
        credentials.jdbc_url,
        '''(SELECT COUNT(*) as cnt FROM "DeviceLocation") as t''',
        properties={
            "user": credentials.username,
            "password": credentials.password,
            "driver": "org.postgresql.Driver"
        }
    )
    df3.show()
    print("Success!")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

print("All queries done!")
