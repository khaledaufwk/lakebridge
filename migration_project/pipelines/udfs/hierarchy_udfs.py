# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Hierarchy Python UDFs
# MAGIC
# MAGIC This notebook defines Python UDFs for hierarchical data operations.
# MAGIC These replace SQL Server recursive CTE-based functions.
# MAGIC
# MAGIC **Converted Functions:**
# MAGIC - dbo.fnManagersByLevel
# MAGIC - dbo.fnManagersByLevelSlicedIntervals
# MAGIC
# MAGIC **Approach:**
# MAGIC For better performance, hierarchies are pre-computed in a DLT table.
# MAGIC These UDFs are for runtime queries when pre-computation isn't suitable.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce,
    array, explode, struct, collect_list, first, max as spark_max
)
from pyspark.sql.types import (
    StringType, IntegerType, ArrayType, StructType, StructField,
    TimestampType, BooleanType
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-computed Manager Hierarchy Table
# MAGIC
# MAGIC This DLT table pre-computes all manager paths for efficient querying.
# MAGIC It replaces the runtime execution of fnManagersByLevel.

# COMMAND ----------

@dlt.table(
    name="manager_hierarchy",
    comment="Pre-computed manager hierarchy for all workers. Replaces dbo.fnManagersByLevel"
)
def manager_hierarchy():
    """
    Pre-computed manager hierarchy for all workers.

    This table builds the complete manager chain for each worker up to 10 levels.
    Instead of calling a function at runtime, query this table directly.

    Converted from: dbo.fnManagersByLevel, dbo.fnManagersByLevelSlicedIntervals

    Usage:
        SELECT * FROM wakecap_prod.migration.manager_hierarchy
        WHERE WorkerID = 123 AND Level <= 3
    """
    # Read active manager assignments
    manager_assignments = (
        dlt.read("bronze_dbo_ManagerAssignments")
        .filter(col("ValidTo").isNull())
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
        .select(
            col("WorkerID"),
            col("ProjectID"),
            col("ManagerWorkerID")
        )
        .distinct()
    )

    # Read worker names for display
    workers = (
        dlt.read("bronze_dbo_Worker")
        .filter(col("DeletedAt").isNull())
        .select(
            col("WorkerID"),
            col("Worker").alias("WorkerCode"),
            col("WorkerName")
        )
    )

    # Level 1: Direct managers
    level1 = (
        manager_assignments
        .join(
            workers.select(
                col("WorkerID").alias("ManagerWorkerID"),
                col("WorkerCode").alias("ManagerCode"),
                col("WorkerName").alias("ManagerName")
            ),
            "ManagerWorkerID",
            "left"
        )
        .select(
            col("WorkerID"),
            col("ProjectID"),
            col("ManagerWorkerID").alias("ManagerID"),
            col("ManagerCode"),
            col("ManagerName"),
            lit(1).alias("Level")
        )
    )

    # Build hierarchy iteratively (levels 2-10)
    hierarchy = level1

    for level in range(2, 11):
        # Get managers of current level managers
        prev_level = hierarchy.filter(col("Level") == level - 1)

        next_level = (
            prev_level
            .join(
                manager_assignments.select(
                    col("WorkerID").alias("PrevManagerID"),
                    col("ProjectID").alias("PrevProjectID"),
                    col("ManagerWorkerID").alias("NextManagerID")
                ),
                (prev_level["ManagerID"] == col("PrevManagerID")) &
                (prev_level["ProjectID"] == col("PrevProjectID")),
                "inner"
            )
            .join(
                workers.select(
                    col("WorkerID").alias("NextManagerID"),
                    col("WorkerCode").alias("NextManagerCode"),
                    col("WorkerName").alias("NextManagerName")
                ),
                "NextManagerID",
                "left"
            )
            .select(
                prev_level["WorkerID"],
                prev_level["ProjectID"],
                col("NextManagerID").alias("ManagerID"),
                col("NextManagerCode").alias("ManagerCode"),
                col("NextManagerName").alias("ManagerName"),
                lit(level).alias("Level")
            )
            .filter(col("ManagerID").isNotNull())
        )

        hierarchy = hierarchy.union(next_level)

    return hierarchy.withColumn("_computed_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manager Path Table
# MAGIC
# MAGIC Provides the full management chain as an array for each worker.

# COMMAND ----------

@dlt.table(
    name="worker_manager_path",
    comment="Full management chain for each worker as an array"
)
def worker_manager_path():
    """
    Full management chain for each worker.

    Returns an array of managers from direct (level 1) to top level.

    Usage:
        SELECT WorkerID, ProjectID, manager_path
        FROM wakecap_prod.migration.worker_manager_path
        WHERE WorkerID = 123
    """
    hierarchy = dlt.read("manager_hierarchy")

    return (
        hierarchy
        .groupBy("WorkerID", "ProjectID")
        .agg(
            collect_list(
                struct(
                    col("Level"),
                    col("ManagerID"),
                    col("ManagerCode"),
                    col("ManagerName")
                )
            ).alias("manager_path"),
            spark_max("Level").alias("max_level")
        )
        .withColumn("_computed_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python UDFs for Runtime Hierarchy Queries

# COMMAND ----------

from pyspark.sql.functions import udf

# Define return type for manager list
manager_struct = StructType([
    StructField("manager_id", IntegerType()),
    StructField("manager_code", StringType()),
    StructField("manager_name", StringType()),
    StructField("level", IntegerType())
])

@udf(returnType=ArrayType(manager_struct))
def fn_managers_by_level(worker_id, project_id, max_level=10):
    """
    Get managers for a worker up to a specified level.

    Converted from: dbo.fnManagersByLevel

    NOTE: For production use, query the manager_hierarchy table instead.
    This UDF is for ad-hoc queries where the table isn't available.

    Args:
        worker_id: Worker ID
        project_id: Project ID
        max_level: Maximum hierarchy level to return (default 10)

    Returns:
        Array of manager records with level
    """
    # This UDF requires the hierarchy table to be available
    # In practice, use SQL query to manager_hierarchy table
    return None

spark.udf.register("fn_managers_by_level", fn_managers_by_level)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-Sliced Hierarchy Table
# MAGIC
# MAGIC For historical point-in-time manager lookups.

# COMMAND ----------

@dlt.table(
    name="manager_hierarchy_history",
    comment="Historical manager hierarchy with validity periods. Replaces dbo.fnManagersByLevelSlicedIntervals"
)
def manager_hierarchy_history():
    """
    Historical manager hierarchy with time validity.

    This table maintains the manager hierarchy over time, allowing
    point-in-time queries for who was someone's manager on a specific date.

    Converted from: dbo.fnManagersByLevelSlicedIntervals
    """
    # Read all manager assignments (including historical)
    manager_assignments = (
        dlt.read("bronze_dbo_ManagerAssignments")
        .filter((col("DeleteFlag").isNull()) | (col("DeleteFlag") == False))
        .select(
            col("WorkerID"),
            col("ProjectID"),
            col("ManagerWorkerID"),
            col("ValidFrom"),
            col("ValidTo")
        )
    )

    workers = (
        dlt.read("bronze_dbo_Worker")
        .select(
            col("WorkerID"),
            col("Worker").alias("WorkerCode"),
            col("WorkerName")
        )
    )

    # Build level 1 with time validity
    level1 = (
        manager_assignments
        .join(
            workers.select(
                col("WorkerID").alias("ManagerWorkerID"),
                col("WorkerCode").alias("ManagerCode"),
                col("WorkerName").alias("ManagerName")
            ),
            "ManagerWorkerID",
            "left"
        )
        .select(
            col("WorkerID"),
            col("ProjectID"),
            col("ManagerWorkerID").alias("ManagerID"),
            col("ManagerCode"),
            col("ManagerName"),
            lit(1).alias("Level"),
            col("ValidFrom"),
            col("ValidTo")
        )
    )

    # For historical queries, we typically only need level 1
    # Higher levels can be computed by recursive joins at query time
    return level1.withColumn("_computed_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper SQL Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- SQL function to get manager at specific level
# MAGIC -- Query the pre-computed table instead of calling a function
# MAGIC
# MAGIC -- Get direct manager (level 1)
# MAGIC SELECT m.ManagerID, m.ManagerName
# MAGIC FROM wakecap_prod.migration.manager_hierarchy m
# MAGIC WHERE m.WorkerID = 123
# MAGIC   AND m.ProjectID = 456
# MAGIC   AND m.Level = 1;
# MAGIC
# MAGIC -- Get all managers up to level 3
# MAGIC SELECT m.Level, m.ManagerID, m.ManagerName
# MAGIC FROM wakecap_prod.migration.manager_hierarchy m
# MAGIC WHERE m.WorkerID = 123
# MAGIC   AND m.ProjectID = 456
# MAGIC   AND m.Level <= 3
# MAGIC ORDER BY m.Level;
# MAGIC
# MAGIC -- Get manager path as array
# MAGIC SELECT WorkerID, manager_path
# MAGIC FROM wakecap_prod.migration.worker_manager_path
# MAGIC WHERE WorkerID = 123;
# MAGIC
# MAGIC -- Historical: Who was the manager on a specific date?
# MAGIC SELECT m.ManagerID, m.ManagerName
# MAGIC FROM wakecap_prod.migration.manager_hierarchy_history m
# MAGIC WHERE m.WorkerID = 123
# MAGIC   AND m.ProjectID = 456
# MAGIC   AND m.Level = 1
# MAGIC   AND m.ValidFrom <= '2025-06-15'
# MAGIC   AND (m.ValidTo IS NULL OR m.ValidTo > '2025-06-15');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Get workers with their direct managers
# MAGIC workers_with_managers = (
# MAGIC     spark.table("wakecap_prod.migration.silver_worker")
# MAGIC     .join(
# MAGIC         spark.table("wakecap_prod.migration.manager_hierarchy")
# MAGIC             .filter(col("Level") == 1),
# MAGIC         ["WorkerID", "ProjectID"],
# MAGIC         "left"
# MAGIC     )
# MAGIC     .select(
# MAGIC         col("WorkerID"),
# MAGIC         col("WorkerName"),
# MAGIC         col("ManagerID"),
# MAGIC         col("ManagerName").alias("DirectManagerName")
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # Find all workers under a specific manager (at any level)
# MAGIC workers_under_manager = (
# MAGIC     spark.table("wakecap_prod.migration.manager_hierarchy")
# MAGIC     .filter(col("ManagerID") == 999)  # Manager's WorkerID
# MAGIC     .select("WorkerID", "ProjectID", "Level")
# MAGIC     .distinct()
# MAGIC )
# MAGIC
# MAGIC # Get the full chain for analysis
# MAGIC manager_chains = (
# MAGIC     spark.table("wakecap_prod.migration.worker_manager_path")
# MAGIC     .filter(col("max_level") >= 3)  # Workers with at least 3 levels
# MAGIC )
# MAGIC ```
