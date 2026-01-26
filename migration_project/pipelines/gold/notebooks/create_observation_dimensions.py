# Databricks notebook source
# MAGIC %md
# MAGIC # Create Observation Dimension Tables
# MAGIC
# MAGIC Creates observation-related dimension tables in Silver layer from SQL Server.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# JDBC connection to SQL Server
jdbc_url = "jdbc:sqlserver://wakecap24.database.windows.net:1433;database=WakeCapDW_20251215;encrypt=true;trustServerCertificate=false"

# Try to get credentials from secrets, fall back to hardcoded values
try:
    jdbc_user = dbutils.secrets.get(scope="lakebridge", key="mssql-user")
    jdbc_password = dbutils.secrets.get(scope="lakebridge", key="mssql-password")
except Exception:
    # Fallback to hardcoded values (for initial setup)
    jdbc_user = "snowconvert"
    jdbc_password = "Tr0pic@lThund3r#M0nk3yD4nce9!xR2vZ8pL"

connection_properties = {
    "user": jdbc_user,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

TARGET_CATALOG = "wakecap_prod"
TARGET_SCHEMA = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Create Dimension Tables

# COMMAND ----------

# Define dimension tables to load
dimensions = [
    {
        "name": "Project",
        "query": "(SELECT ProjectID, ExtProjectID, ProjectName, ExtSourceID FROM dbo.Project) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_project_dw"
    },
    {
        "name": "ObservationDiscriminator",
        "query": "(SELECT ObservationDiscriminatorID, ObservationDiscriminator, ExtSourceID FROM dbo.ObservationDiscriminator) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_discriminator"
    },
    {
        "name": "ObservationSource",
        "query": "(SELECT ObservationSourceID, ObservationSource, ExtSourceID FROM dbo.ObservationSource) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_source"
    },
    {
        "name": "ObservationType",
        "query": "(SELECT ObservationTypeID, ObservationType, ExtSourceID FROM dbo.ObservationType) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_type"
    },
    {
        "name": "ObservationSeverity",
        "query": "(SELECT ObservationSeverityID, ObservationSeverity, ExtSourceID FROM dbo.ObservationSeverity) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_severity"
    },
    {
        "name": "ObservationStatus",
        "query": "(SELECT ObservationStatusID, ObservationStatus, ExtSourceID FROM dbo.ObservationStatus) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_status"
    },
    {
        "name": "ObservationClinicViolationStatus",
        "query": "(SELECT ObservationClinicViolationStatusID, ObservationClinicViolationStatus, ExtSourceID FROM dbo.ObservationClinicViolationStatus) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_clinic_violation_status"
    },
    {
        "name": "Company/Organization",
        "query": "(SELECT CompanyID, ExtCompanyID, CompanyName, CompanyType, ProjectID, ExtSourceID FROM dbo.Company) AS t",
        "target": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_organization"
    },
]

# COMMAND ----------

# Load each dimension from SQL Server and save to Delta
results = []

for dim in dimensions:
    print(f"\nLoading {dim['name']}...")
    try:
        # Read from SQL Server
        df = spark.read.jdbc(
            url=jdbc_url,
            table=dim["query"],
            properties=connection_properties
        )

        row_count = df.count()
        print(f"  Loaded {row_count} rows from SQL Server")

        # Write to Delta table (overwrite)
        df.write.format("delta").mode("overwrite").saveAsTable(dim["target"])
        print(f"  [OK] Created {dim['target']}")

        results.append({"table": dim["target"], "status": "OK", "rows": row_count})

    except Exception as e:
        print(f"  [ERROR] {str(e)[:100]}")
        results.append({"table": dim["target"], "status": "ERROR", "error": str(e)[:100]})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print("=" * 60)
print("VERIFICATION RESULTS")
print("=" * 60)

for dim in dimensions:
    try:
        count = spark.table(dim["target"]).count()
        print(f"  [OK] {dim['target'].split('.')[-1]}: {count} rows")
    except Exception as e:
        print(f"  [MISSING] {dim['target'].split('.')[-1]}: {str(e)[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\nDimension tables created:")
for r in results:
    if r["status"] == "OK":
        print(f"  {r['table']}: {r['rows']} rows")
    else:
        print(f"  {r['table']}: FAILED - {r.get('error', 'Unknown')}")
