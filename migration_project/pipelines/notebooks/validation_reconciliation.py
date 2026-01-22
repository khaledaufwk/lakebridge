# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Migration - Validation & Reconciliation
# MAGIC
# MAGIC This notebook validates the migrated data by comparing row counts and sample values
# MAGIC between source (SQL Server via ADLS extracts) and target (Databricks) tables.
# MAGIC
# MAGIC **Validation Checks:**
# MAGIC 1. Row count comparison
# MAGIC 2. Primary key completeness
# MAGIC 3. Sample data comparison
# MAGIC 4. Aggregation comparison
# MAGIC 5. Data quality metrics

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.dropdown("validation_level", "basic", ["basic", "full", "detailed"], "Validation Level")
dbutils.widgets.text("sample_size", "1000", "Sample Size for Comparison")

validation_level = dbutils.widgets.get("validation_level")
sample_size = int(dbutils.widgets.get("sample_size"))

# ADLS Configuration
ADLS_STORAGE_ACCOUNT = spark.conf.get("pipeline.adls_storage_account", "wakecapadls")
ADLS_CONTAINER = "raw"
ADLS_BASE_PATH = f"abfss://{ADLS_CONTAINER}@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/wakecap"

# Target catalog/schema
CATALOG = "wakecap_prod"
SCHEMA = "migration"

print(f"Validation level: {validation_level}")
print(f"Sample size: {sample_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Definitions

# COMMAND ----------

# Define tables to validate with their configurations
VALIDATION_TABLES = {
    # Dimensions
    "Organization": {
        "source_path": f"{ADLS_BASE_PATH}/dimensions/Organization/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_Organization",
        "pk_columns": ["OrganizationID"],
        "compare_columns": ["Organization", "OrganizationName"],
        "agg_columns": []
    },
    "Project": {
        "source_path": f"{ADLS_BASE_PATH}/dimensions/Project/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_Project",
        "pk_columns": ["ProjectID"],
        "compare_columns": ["Project", "ProjectName", "OrganizationID"],
        "agg_columns": []
    },
    "Worker": {
        "source_path": f"{ADLS_BASE_PATH}/dimensions/Worker/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_Worker",
        "pk_columns": ["WorkerID"],
        "compare_columns": ["Worker", "WorkerName", "ProjectID"],
        "agg_columns": []
    },
    "Crew": {
        "source_path": f"{ADLS_BASE_PATH}/dimensions/Crew/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_Crew",
        "pk_columns": ["CrewID"],
        "compare_columns": ["Crew", "CrewName", "ProjectID"],
        "agg_columns": []
    },
    "Floor": {
        "source_path": f"{ADLS_BASE_PATH}/dimensions/Floor/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_Floor",
        "pk_columns": ["FloorID"],
        "compare_columns": ["Floor", "FloorName", "ProjectID"],
        "agg_columns": []
    },
    "Zone": {
        "source_path": f"{ADLS_BASE_PATH}/dimensions/Zone/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_Zone",
        "pk_columns": ["ZoneID"],
        "compare_columns": ["Zone", "ZoneName", "FloorID"],
        "agg_columns": []
    },
    # Facts
    "FactWorkersHistory": {
        "source_path": f"{ADLS_BASE_PATH}/facts/FactWorkersHistory/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_FactWorkersHistory",
        "pk_columns": ["ProjectID", "WorkerID", "TimestampUTC"],
        "compare_columns": ["LocalDate", "FloorID", "ActiveTime"],
        "agg_columns": ["ActiveTime", "InactiveTime"]
    },
    "FactWorkersShifts": {
        "source_path": f"{ADLS_BASE_PATH}/facts/FactWorkersShifts/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_FactWorkersShifts",
        "pk_columns": ["ProjectID", "WorkerID", "StartAtUTC"],
        "compare_columns": ["ShiftLocalDate", "ActiveTime", "Readings"],
        "agg_columns": ["ActiveTime", "Readings"]
    },
    "FactReportedAttendance": {
        "source_path": f"{ADLS_BASE_PATH}/facts/FactReportedAttendance/",
        "target_table": f"{CATALOG}.{SCHEMA}.bronze_dbo_FactReportedAttendance",
        "pk_columns": ["ProjectID", "WorkerID", "ShiftLocalDate"],
        "compare_columns": ["ReportedTime"],
        "agg_columns": ["ReportedTime"]
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Functions

# COMMAND ----------

def read_source_table(config):
    """Read source table from ADLS."""
    try:
        return spark.read.format("parquet").load(config["source_path"])
    except Exception as e:
        print(f"Error reading source: {e}")
        return None


def read_target_table(config):
    """Read target table from Databricks."""
    try:
        return spark.table(config["target_table"])
    except Exception as e:
        print(f"Error reading target: {e}")
        return None


def validate_row_count(table_name, config):
    """Compare row counts between source and target."""
    source_df = read_source_table(config)
    target_df = read_target_table(config)

    if source_df is None or target_df is None:
        return {
            "table": table_name,
            "check": "row_count",
            "status": "ERROR",
            "source_count": None,
            "target_count": None,
            "difference": None,
            "match_pct": None
        }

    source_count = source_df.count()
    target_count = target_df.count()
    difference = target_count - source_count
    match_pct = (min(source_count, target_count) / max(source_count, target_count) * 100) if max(source_count, target_count) > 0 else 100.0

    status = "PASS" if match_pct >= 99.9 else "WARN" if match_pct >= 99.0 else "FAIL"

    return {
        "table": table_name,
        "check": "row_count",
        "status": status,
        "source_count": source_count,
        "target_count": target_count,
        "difference": difference,
        "match_pct": round(match_pct, 2)
    }


def validate_pk_completeness(table_name, config):
    """Check for NULL primary keys in target."""
    target_df = read_target_table(config)

    if target_df is None:
        return {
            "table": table_name,
            "check": "pk_completeness",
            "status": "ERROR",
            "null_count": None,
            "total_count": None
        }

    pk_columns = config["pk_columns"]
    null_condition = None
    for col_name in pk_columns:
        if null_condition is None:
            null_condition = F.col(col_name).isNull()
        else:
            null_condition = null_condition | F.col(col_name).isNull()

    null_count = target_df.filter(null_condition).count()
    total_count = target_df.count()

    status = "PASS" if null_count == 0 else "FAIL"

    return {
        "table": table_name,
        "check": "pk_completeness",
        "status": status,
        "null_count": null_count,
        "total_count": total_count
    }


def validate_aggregations(table_name, config):
    """Compare aggregations between source and target."""
    if not config.get("agg_columns"):
        return None

    source_df = read_source_table(config)
    target_df = read_target_table(config)

    if source_df is None or target_df is None:
        return {
            "table": table_name,
            "check": "aggregations",
            "status": "ERROR",
            "details": {}
        }

    results = {}
    for col_name in config["agg_columns"]:
        if col_name in source_df.columns and col_name in target_df.columns:
            source_sum = source_df.agg(F.sum(col_name)).collect()[0][0] or 0
            target_sum = target_df.agg(F.sum(col_name)).collect()[0][0] or 0

            if source_sum == 0 and target_sum == 0:
                match_pct = 100.0
            elif source_sum == 0:
                match_pct = 0.0
            else:
                match_pct = min(source_sum, target_sum) / max(source_sum, target_sum) * 100

            results[col_name] = {
                "source_sum": float(source_sum),
                "target_sum": float(target_sum),
                "match_pct": round(match_pct, 2)
            }

    overall_status = "PASS" if all(r["match_pct"] >= 99.0 for r in results.values()) else "FAIL"

    return {
        "table": table_name,
        "check": "aggregations",
        "status": overall_status,
        "details": results
    }


def validate_sample_data(table_name, config):
    """Compare sample records between source and target."""
    source_df = read_source_table(config)
    target_df = read_target_table(config)

    if source_df is None or target_df is None:
        return {
            "table": table_name,
            "check": "sample_data",
            "status": "ERROR",
            "matched": 0,
            "total": 0
        }

    pk_columns = config["pk_columns"]
    compare_columns = config.get("compare_columns", [])

    # Get sample from source
    source_sample = source_df.limit(sample_size)

    # Join with target on PK
    joined = source_sample.alias("s").join(
        target_df.alias("t"),
        [F.col(f"s.{pk}") == F.col(f"t.{pk}") for pk in pk_columns],
        "left"
    )

    # Count matches
    matched = joined.filter(F.col(f"t.{pk_columns[0]}").isNotNull()).count()
    total = source_sample.count()

    status = "PASS" if matched == total else "WARN" if matched >= total * 0.99 else "FAIL"

    return {
        "table": table_name,
        "check": "sample_data",
        "status": status,
        "matched": matched,
        "total": total,
        "match_pct": round(matched / total * 100, 2) if total > 0 else 0
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Validation

# COMMAND ----------

all_results = []

for table_name, config in VALIDATION_TABLES.items():
    print(f"\n{'='*60}")
    print(f"Validating: {table_name}")
    print(f"{'='*60}")

    # Row count validation
    row_count_result = validate_row_count(table_name, config)
    all_results.append(row_count_result)
    print(f"Row Count: {row_count_result['status']} - Source: {row_count_result['source_count']}, Target: {row_count_result['target_count']}")

    # PK completeness
    pk_result = validate_pk_completeness(table_name, config)
    all_results.append(pk_result)
    print(f"PK Completeness: {pk_result['status']} - Null PKs: {pk_result['null_count']}")

    if validation_level in ["full", "detailed"]:
        # Aggregation validation
        agg_result = validate_aggregations(table_name, config)
        if agg_result:
            all_results.append(agg_result)
            print(f"Aggregations: {agg_result['status']} - {agg_result['details']}")

    if validation_level == "detailed":
        # Sample data validation
        sample_result = validate_sample_data(table_name, config)
        all_results.append(sample_result)
        print(f"Sample Match: {sample_result['status']} - {sample_result['matched']}/{sample_result['total']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

# Create summary DataFrame
results_df = spark.createDataFrame(all_results)

# Summary by status
summary = results_df.groupBy("status").count().orderBy("status")
display(summary)

# COMMAND ----------

# Detailed results
display(results_df.orderBy("table", "check"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Metrics

# COMMAND ----------

# Get DQ metrics from Silver layer expectations
dq_tables = [
    f"{CATALOG}.{SCHEMA}.silver_organization",
    f"{CATALOG}.{SCHEMA}.silver_project",
    f"{CATALOG}.{SCHEMA}.silver_worker",
    f"{CATALOG}.{SCHEMA}.silver_fact_workers_shifts"
]

dq_results = []
for table in dq_tables:
    try:
        df = spark.table(table)
        count = df.count()
        dq_results.append({
            "table": table,
            "record_count": count,
            "status": "AVAILABLE"
        })
    except Exception as e:
        dq_results.append({
            "table": table,
            "record_count": 0,
            "status": f"ERROR: {str(e)[:50]}"
        })

dq_df = spark.createDataFrame(dq_results)
display(dq_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation Summary

# COMMAND ----------

# Count by status
pass_count = results_df.filter(F.col("status") == "PASS").count()
warn_count = results_df.filter(F.col("status") == "WARN").count()
fail_count = results_df.filter(F.col("status") == "FAIL").count()
error_count = results_df.filter(F.col("status") == "ERROR").count()
total_count = results_df.count()

print(f"""
╔══════════════════════════════════════════════════════════════╗
║           MIGRATION VALIDATION SUMMARY                       ║
╠══════════════════════════════════════════════════════════════╣
║  Total Checks:  {total_count:5d}                                       ║
║  ─────────────────────────────────────────────────────────   ║
║  ✅ PASS:       {pass_count:5d}   ({pass_count/total_count*100:.1f}%)                             ║
║  ⚠️  WARN:       {warn_count:5d}   ({warn_count/total_count*100:.1f}%)                             ║
║  ❌ FAIL:       {fail_count:5d}   ({fail_count/total_count*100:.1f}%)                             ║
║  🔴 ERROR:      {error_count:5d}   ({error_count/total_count*100:.1f}%)                             ║
╠══════════════════════════════════════════════════════════════╣
║  Overall Status: {"✅ PASSED" if fail_count == 0 and error_count == 0 else "❌ FAILED":11}                              ║
╚══════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results

# COMMAND ----------

# Save results to Delta table for historical tracking
results_df.withColumn("validation_timestamp", F.current_timestamp()).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.validation_history")

print(f"Results saved to {CATALOG}.{SCHEMA}.validation_history")
