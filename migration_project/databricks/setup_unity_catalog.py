# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup for WakeCapDW Migration
# MAGIC
# MAGIC This notebook configures Unity Catalog for the WakeCapDW migration:
# MAGIC 1. Creates storage credential for ADLS access
# MAGIC 2. Creates external location for raw data
# MAGIC 3. Creates catalog and schemas
# MAGIC 4. Sets up permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for configuration
dbutils.widgets.text("storage_account", "wakecapadls", "ADLS Storage Account")
dbutils.widgets.text("container", "raw", "Container Name")
dbutils.widgets.text("catalog_name", "wakecap_prod", "Catalog Name")
dbutils.widgets.text("storage_credential_name", "wakecap_adls_credential", "Storage Credential Name")
dbutils.widgets.text("external_location_name", "wakecap_raw_data", "External Location Name")

storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
catalog_name = dbutils.widgets.get("catalog_name")
storage_credential_name = dbutils.widgets.get("storage_credential_name")
external_location_name = dbutils.widgets.get("external_location_name")

adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/wakecap"

print(f"Storage Account: {storage_account}")
print(f"Container: {container}")
print(f"Catalog: {catalog_name}")
print(f"ADLS Path: {adls_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Storage Credential
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Service Principal with Storage Blob Data Contributor role on the storage account
# MAGIC - Application (client) ID, Directory (tenant) ID, and Client Secret

# COMMAND ----------

# Get service principal credentials from secrets (must be configured in Databricks secret scope)
# You can also pass these directly if running manually

try:
    sp_application_id = dbutils.secrets.get(scope="wakecap-secrets", key="sp-application-id")
    sp_directory_id = dbutils.secrets.get(scope="wakecap-secrets", key="sp-directory-id")
    sp_client_secret = dbutils.secrets.get(scope="wakecap-secrets", key="sp-client-secret")
    use_service_principal = True
    print("Using Service Principal credentials from secret scope")
except Exception as e:
    print(f"Secret scope not configured: {e}")
    print("Will use managed identity if available, or manual configuration required")
    use_service_principal = False

# COMMAND ----------

# Create storage credential (requires metastore admin privileges)
if use_service_principal:
    spark.sql(f"""
        CREATE STORAGE CREDENTIAL IF NOT EXISTS {storage_credential_name}
        WITH (
            AZURE_SERVICE_PRINCIPAL (
                DIRECTORY_ID = '{sp_directory_id}',
                APPLICATION_ID = '{sp_application_id}',
                CLIENT_SECRET = '{sp_client_secret}'
            )
        )
        COMMENT 'Storage credential for WakeCapDW ADLS access'
    """)
    print(f"Storage credential '{storage_credential_name}' created/verified")
else:
    # Use managed identity (workspace must have managed identity configured)
    spark.sql(f"""
        CREATE STORAGE CREDENTIAL IF NOT EXISTS {storage_credential_name}
        WITH (AZURE_MANAGED_IDENTITY ())
        COMMENT 'Storage credential for WakeCapDW ADLS access using managed identity'
    """)
    print(f"Storage credential '{storage_credential_name}' created with managed identity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create External Location

# COMMAND ----------

# Create external location pointing to ADLS path
spark.sql(f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS {external_location_name}
    URL '{adls_path}'
    WITH (STORAGE CREDENTIAL {storage_credential_name})
    COMMENT 'External location for WakeCapDW raw data in ADLS'
""")

print(f"External location '{external_location_name}' created")
print(f"URL: {adls_path}")

# COMMAND ----------

# Verify external location access
try:
    files = dbutils.fs.ls(adls_path)
    print(f"Successfully accessed external location. Found {len(files)} items.")
    for f in files[:10]:
        print(f"  - {f.name}")
except Exception as e:
    print(f"Warning: Could not list files at {adls_path}")
    print(f"Error: {e}")
    print("This may be expected if no data has been extracted yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Catalog and Schemas

# COMMAND ----------

# Create catalog
spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalog_name}
    COMMENT 'WakeCapDW Production Catalog'
""")
print(f"Catalog '{catalog_name}' created/verified")

# Use catalog
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# Create schemas for medallion architecture
schemas = [
    ("migration", "Bronze/Silver/Gold tables from DLT pipeline"),
    ("bronze", "Raw data from ADLS (if separate from DLT)"),
    ("silver", "Cleansed and validated data"),
    ("gold", "Business-ready aggregations and views"),
    ("staging", "Staging area for complex calculations"),
    ("security", "Row filters and security objects"),
    ("validation", "Validation and reconciliation results")
]

for schema_name, comment in schemas:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
        COMMENT '{comment}'
    """)
    print(f"Schema '{catalog_name}.{schema_name}' created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Grant Permissions

# COMMAND ----------

# Grant permissions on external location (adjust group names as needed)
data_engineers_group = "data_engineers"
data_scientists_group = "data_scientists"
analysts_group = "analysts"

# Grant on external location
try:
    spark.sql(f"GRANT READ FILES ON EXTERNAL LOCATION {external_location_name} TO `{data_engineers_group}`")
    spark.sql(f"GRANT READ FILES ON EXTERNAL LOCATION {external_location_name} TO `{data_scientists_group}`")
    print(f"Granted READ FILES on external location to groups")
except Exception as e:
    print(f"Note: Could not grant external location permissions: {e}")
    print("You may need to grant these manually or the groups may not exist yet.")

# COMMAND ----------

# Grant catalog permissions
try:
    # Data Engineers - full access
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{data_engineers_group}`")
    spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {catalog_name} TO `{data_engineers_group}`")

    # Data Scientists - read access
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{data_scientists_group}`")

    # Analysts - read access
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{analysts_group}`")

    print("Catalog permissions granted")
except Exception as e:
    print(f"Note: Could not grant catalog permissions: {e}")
    print("Groups may not exist. Grant permissions manually after creating groups.")

# COMMAND ----------

# Grant schema permissions
for schema_name, _ in schemas:
    try:
        # Data Engineers - full access
        spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {catalog_name}.{schema_name} TO `{data_engineers_group}`")

        # Data Scientists - read on silver/gold
        if schema_name in ["silver", "gold", "validation"]:
            spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{data_scientists_group}`")
            spark.sql(f"GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `{data_scientists_group}`")

        # Analysts - read on gold only
        if schema_name == "gold":
            spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{analysts_group}`")
            spark.sql(f"GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `{analysts_group}`")

    except Exception as e:
        print(f"Note: Could not grant permissions on {schema_name}: {e}")

print("Schema permissions configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Setup

# COMMAND ----------

# List all schemas in catalog
print(f"\nSchemas in {catalog_name}:")
schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
display(schemas_df)

# COMMAND ----------

# Check external location
print(f"\nExternal Location Details:")
ext_loc_df = spark.sql(f"DESCRIBE EXTERNAL LOCATION {external_location_name}")
display(ext_loc_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
╔══════════════════════════════════════════════════════════════╗
║           UNITY CATALOG SETUP COMPLETE                       ║
╠══════════════════════════════════════════════════════════════╣
║  Catalog:           {catalog_name:<40} ║
║  Storage Credential: {storage_credential_name:<39} ║
║  External Location:  {external_location_name:<39} ║
║  ADLS Path:          {adls_path[:39]:<39} ║
╠══════════════════════════════════════════════════════════════╣
║  Schemas Created:                                            ║
║    - {catalog_name}.migration   (DLT pipeline tables)           ║
║    - {catalog_name}.bronze      (Raw data layer)               ║
║    - {catalog_name}.silver      (Cleansed data layer)          ║
║    - {catalog_name}.gold        (Business layer)               ║
║    - {catalog_name}.staging     (Calculation staging)          ║
║    - {catalog_name}.security    (Row filters)                  ║
║    - {catalog_name}.validation  (Reconciliation)               ║
╠══════════════════════════════════════════════════════════════╣
║  Next Steps:                                                 ║
║  1. Run ADF pipeline to extract data to ADLS                 ║
║  2. Deploy DLT pipeline with bronze/silver/gold notebooks    ║
║  3. Install h3 and shapely libraries on cluster              ║
║  4. Run validation notebook                                  ║
╚══════════════════════════════════════════════════════════════╝
""")
