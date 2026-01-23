# Databricks notebook source
print("Step 1: About to run %run command...")

# COMMAND ----------

# MAGIC %run /Workspace/migration_project/pipelines/timescaledb/src/timescaledb_loader_v2

# COMMAND ----------

print("Step 2: %run completed!")
print(f"TimescaleDBCredentials: {TimescaleDBCredentials}")
print(f"TimescaleDBLoaderV2: {TimescaleDBLoaderV2}")
