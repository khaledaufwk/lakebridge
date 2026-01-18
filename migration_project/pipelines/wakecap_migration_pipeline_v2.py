# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration Pipeline (Test Version)
# MAGIC 
# MAGIC This is a test version that creates placeholder tables to verify the pipeline works.
# MAGIC After testing, uncomment the JDBC sections to enable real data ingestion.
# MAGIC 
# MAGIC **Source:** Azure SQL Server - WakeCapDW_20251215
# MAGIC **Target:** Databricks Unity Catalog

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
CATALOG = "wakecap_prod"
SCHEMA = "migration"
SECRET_SCOPE = "wakecap_migration"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Test Tables
# MAGIC Creating test tables with sample schema

# COMMAND ----------

@dlt.table(
    name="bronze_worker_test",
    comment="Test bronze table for Worker - validates pipeline structure"
)
def bronze_worker_test():
    """Bronze test table: Worker schema."""
    schema = StructType([
        StructField("WorkerID", LongType(), False),
        StructField("Worker", StringType(), True),
        StructField("WorkerName", StringType(), True),
        StructField("ProjectID", IntegerType(), True),
        StructField("CompanyID", IntegerType(), True),
        StructField("TradeID", IntegerType(), True),
        StructField("CreatedAt", TimestampType(), True),
        StructField("UpdatedAt", TimestampType(), True),
        StructField("DeletedAt", TimestampType(), True),
    ])
    
    # Sample data for testing
    data = [
        (1, "W001", "John Smith", 1, 1, 1, None, None, None),
        (2, "W002", "Jane Doe", 1, 1, 2, None, None, None),
        (3, "W003", "Bob Wilson", 2, 2, 1, None, None, None),
    ]
    
    return spark.createDataFrame(data, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_project_test",
    comment="Test bronze table for Project"
)
def bronze_project_test():
    """Bronze test table: Project schema."""
    schema = StructType([
        StructField("ProjectID", IntegerType(), False),
        StructField("Project", StringType(), True),
        StructField("ProjectName", StringType(), True),
        StructField("OrganizationID", IntegerType(), True),
        StructField("CreatedAt", TimestampType(), True),
        StructField("DeletedAt", TimestampType(), True),
    ])
    
    data = [
        (1, "PRJ001", "Downtown Tower", 1, None, None),
        (2, "PRJ002", "Airport Terminal", 1, None, None),
    ]
    
    return spark.createDataFrame(data, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_crew_test",
    comment="Test bronze table for Crew"
)
def bronze_crew_test():
    """Bronze test table: Crew schema."""
    schema = StructType([
        StructField("CrewID", IntegerType(), False),
        StructField("Crew", StringType(), True),
        StructField("CrewName", StringType(), True),
        StructField("ProjectID", IntegerType(), True),
        StructField("DeletedAt", TimestampType(), True),
    ])
    
    data = [
        (1, "CRW001", "Foundation Team", 1, None),
        (2, "CRW002", "Electrical Team", 1, None),
        (3, "CRW003", "Plumbing Team", 2, None),
    ]
    
    return spark.createDataFrame(data, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned Data

# COMMAND ----------

@dlt.table(
    name="silver_worker_test",
    comment="Cleaned worker test data"
)
@dlt.expect_or_drop("valid_worker_id", "WorkerID IS NOT NULL")
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
def silver_worker_test():
    """Silver table: Cleaned worker data."""
    return (
        dlt.read("bronze_worker_test")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("test_data"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_project_test",
    comment="Cleaned project test data"
)
@dlt.expect_or_drop("valid_project_id", "ProjectID IS NOT NULL")
def silver_project_test():
    """Silver table: Cleaned project data."""
    return (
        dlt.read("bronze_project_test")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("test_data"))
    )

# COMMAND ----------

@dlt.table(
    name="silver_crew_test",
    comment="Cleaned crew test data"
)
@dlt.expect_or_drop("valid_crew_id", "CrewID IS NOT NULL")
def silver_crew_test():
    """Silver table: Cleaned crew data."""
    return (
        dlt.read("bronze_crew_test")
        .filter(col("DeletedAt").isNull())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("test_data"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Views

# COMMAND ----------

@dlt.table(
    name="gold_worker_summary_test",
    comment="Worker summary with project and crew info"
)
def gold_worker_summary_test():
    """Gold table: Worker summary joining dimensions."""
    workers = dlt.read("silver_worker_test")
    projects = dlt.read("silver_project_test")
    crews = dlt.read("silver_crew_test")
    
    return (
        workers
        .join(projects, "ProjectID", "left")
        .join(crews.select("CrewID", "Crew", "CrewName", "ProjectID"), 
              ["ProjectID"], "left")
        .select(
            workers["WorkerID"],
            workers["Worker"],
            workers["WorkerName"],
            projects["Project"],
            projects["ProjectName"],
            col("Crew"),
            col("CrewName"),
            workers["_ingested_at"]
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_project_metrics_test",
    comment="Project metrics aggregation"
)
def gold_project_metrics_test():
    """Gold table: Project with worker counts."""
    workers = dlt.read("silver_worker_test")
    projects = dlt.read("silver_project_test")
    
    worker_counts = (
        workers
        .groupBy("ProjectID")
        .agg(count("WorkerID").alias("worker_count"))
    )
    
    return (
        projects
        .join(worker_counts, "ProjectID", "left")
        .select(
            projects["ProjectID"],
            projects["Project"],
            projects["ProjectName"],
            coalesce(col("worker_count"), lit(0)).alias("worker_count"),
            projects["_ingested_at"]
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Test Completed!
# MAGIC 
# MAGIC This test pipeline validates that the DLT framework is working correctly.
# MAGIC 
# MAGIC ### Next Steps for Production:
# MAGIC 
# MAGIC 1. **Add SQL Server JDBC Driver to Cluster:**
# MAGIC    - Download: `mssql-jdbc-12.4.2.jre11.jar`
# MAGIC    - Upload to DBFS or Unity Catalog Volume
# MAGIC    - Add to cluster libraries
# MAGIC 
# MAGIC 2. **Configure Network Access:**
# MAGIC    - Ensure Databricks can reach Azure SQL Server
# MAGIC    - Add Databricks IPs to SQL Server firewall
# MAGIC 
# MAGIC 3. **Update Pipeline with JDBC Tables:**
# MAGIC    - Use `wakecap_migration_pipeline.py` (full version)
# MAGIC    - Uncomment JDBC connection code
# MAGIC 
# MAGIC 4. **Run Full Migration:**
# MAGIC    - Start with a few key tables
# MAGIC    - Validate data quality
# MAGIC    - Scale to all tables
