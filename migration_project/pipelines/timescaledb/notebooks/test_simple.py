# Databricks notebook source
print("Hello from notebook!")

# COMMAND ----------

print("Testing spark...")
print(f"Spark version: {spark.version}")

# COMMAND ----------

print("Testing dbutils...")
print(f"Notebook path: {dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()}")

# COMMAND ----------

print("All basic tests passed!")
