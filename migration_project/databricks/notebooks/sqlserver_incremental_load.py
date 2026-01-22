# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server Incremental Load
# MAGIC
# MAGIC This notebook reads data from Azure SQL Server (WakeCapDW) and merges it into Delta tables.
# MAGIC Based on the watermark-based incremental load pattern.
# MAGIC
# MAGIC ## Parameters
# MAGIC - `source_table`: Source table name or query
# MAGIC - `target_table`: Target Delta table name
# MAGIC - `primary_key_columns`: Comma-separated list of PK columns
# MAGIC - `source_watermark_column`: Column to use for incremental filtering

# COMMAND ----------

from datetime import datetime
from delta.tables import DeltaTable
import json
import logging

# COMMAND ----------

# DBTITLE 1,Widget Parameters - Connection
dbutils.widgets.text("secret_scope", "akv-wakecap24", "Azure Key Vault scope registered in Databricks")
secret_scope = dbutils.widgets.get("secret_scope")

dbutils.widgets.text("source_driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
source_driver = dbutils.widgets.get("source_driver")

dbutils.widgets.text("source_user", "wakecap_reader")
source_user = dbutils.widgets.get("source_user")

dbutils.widgets.text("source_pwd_secret_name", "sqlserver-wakecap-password", "Secret name in Azure Key Vault")
source_pwd_secret_name = dbutils.widgets.get("source_pwd_secret_name")
source_password = dbutils.secrets.get(scope=secret_scope, key=source_pwd_secret_name)

dbutils.widgets.text("source_url", "jdbc:sqlserver://wakecap24.database.windows.net:1433", "Source JDBC URL without the database name")
source_url = dbutils.widgets.get("source_url")

dbutils.widgets.text("source_database", "WakeCapDW_20251215")
source_database = dbutils.widgets.get("source_database")

# Build full JDBC URL with SQL Server specific options
source_url_full = f"{source_url};database={source_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"

# COMMAND ----------

# DBTITLE 1,Widget Parameters - Source
dbutils.widgets.text("source_system_ref", "wakecapdw", "Short name of the source system (no spaces)")
source_system_ref = dbutils.widgets.get("source_system_ref")

dbutils.widgets.text("source_schema", "dbo", "Source schema name")
source_schema = dbutils.widgets.get("source_schema")

dbutils.widgets.text("source_dataset_ref", "", "Short name of the dataset/table (no spaces)")
source_dataset_ref = dbutils.widgets.get("source_dataset_ref")

dbutils.widgets.text("source_table", "", "Source table name or a SELECT query")
source_table_param = dbutils.widgets.get("source_table")

# If source_table is empty, derive from schema.dataset
if source_table_param == "":
    source_table_param = f"SELECT * FROM [{source_schema}].[{source_dataset_ref}]"

dbutils.widgets.text("watermark_table", "", "Table to run watermark check on (defaults to source table)")
watermark_table = dbutils.widgets.get("watermark_table")
if watermark_table == "":
    watermark_table = f"[{source_schema}].[{source_dataset_ref}]"

dbutils.widgets.text("source_watermark_column", "WatermarkUTC", "Column name to use as watermark")
source_watermark_column = dbutils.widgets.get("source_watermark_column")

# COMMAND ----------

# DBTITLE 1,Widget Parameters - Target
dbutils.widgets.text("target_catalog", "wakecap_prod")
target_catalog = dbutils.widgets.get("target_catalog")

dbutils.widgets.text("target_schema", "raw")
target_schema = dbutils.widgets.get("target_schema")

dbutils.widgets.text("target_table", "", "If not specified, will be derived from source_dataset_ref")
target_table = dbutils.widgets.get("target_table")
if target_table == "":
    target_table = f"{source_system_ref}_{source_schema}_{source_dataset_ref}".lower()

targetTableFullName = f"{target_catalog}.{target_schema}.{target_table}"

# COMMAND ----------

# DBTITLE 1,Widget Parameters - Merge Configuration
dbutils.widgets.text("primary_key_columns", "", "Comma separated list of PK columns for MERGE")
primary_key_columns = [pk.strip() for pk in dbutils.widgets.get("primary_key_columns").split(',') if pk.strip()]

dbutils.widgets.text("custom_column_mapping", '{}', "JSON to override default 1:1 column mapping")
custom_column_mapping = json.loads(dbutils.widgets.get("custom_column_mapping"))

dbutils.widgets.text("ignore_changes_columns", "", "Columns that won't trigger UPDATE")
ignore_changes_columns = [ic.strip() for ic in dbutils.widgets.get("ignore_changes_columns").split(',') if ic.strip()]

dbutils.widgets.text("compare_override_columns", '{}', "JSON to override default comparison")
compare_override_columns = json.loads(dbutils.widgets.get("compare_override_columns"))

dbutils.widgets.text("allow_update", "1", "Set to 1 to allow UPDATE in MERGE")
allow_update = dbutils.widgets.get("allow_update") == "1"

dbutils.widgets.text("is_full_load", "0", "Set to 1 to force full load (ignore watermark)")
is_full_load = dbutils.widgets.get("is_full_load") == "1"

# COMMAND ----------

# DBTITLE 1,Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 60)
print("SQL Server Incremental Load")
print("=" * 60)
print(f"Source: {source_database}.{source_schema}.{source_dataset_ref}")
print(f"Target: {targetTableFullName}")
print(f"Primary Keys: {primary_key_columns}")
print(f"Watermark Column: {source_watermark_column}")
print(f"Full Load: {is_full_load}")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Check Target Table Exists
# Check if target table exists, if not we'll do initial load
target_exists = spark.catalog.tableExists(targetTableFullName)
print(f"Target table exists: {target_exists}")

if not target_exists:
    print("Target table does not exist. Will perform initial full load and create table.")
    is_full_load = True

# COMMAND ----------

# DBTITLE 1,Get Old Watermark from Target Table Properties
sourceSyncPropertyName = f"sync.{source_system_ref}_{source_schema}_{source_dataset_ref}".lower()

oldWatermark = datetime(1900, 1, 1, 0, 0, 0, 0)

if target_exists and not is_full_load:
    try:
        propertyDF = (
            spark.sql(f"SHOW TBLPROPERTIES {targetTableFullName}")
            .filter(f"key = '{sourceSyncPropertyName}'")
            .select("value")
            .collect()
        )

        if len(propertyDF) > 0:
            oldWatermark = datetime.strptime(propertyDF[0][0], '%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        logger.warning(f"Could not read watermark property: {e}")

print(f"Old Watermark: {oldWatermark}")

# COMMAND ----------

# DBTITLE 1,Get New Watermark from Source
# Get the max watermark from source
newWatermarkQuery = f"SELECT MAX({source_watermark_column}) as v FROM {watermark_table}"

newWatermarkTable = (spark.read.format("jdbc")
    .option("driver", source_driver)
    .option("url", source_url_full)
    .option("query", newWatermarkQuery)
    .option("user", source_user)
    .option("password", source_password)
    .load()
)

newWatermark = newWatermarkTable.collect()[0][0]
print(f"New Watermark: {newWatermark}")

if newWatermark is None:
    print("No data in source table. Exiting.")
    dbutils.notebook.exit(json.dumps({"status": "no_data", "rows_processed": 0}))

# COMMAND ----------

# DBTITLE 1,Check if There's New Data
if not is_full_load and oldWatermark >= newWatermark:
    print(f"No new data to process. Old watermark ({oldWatermark}) >= New watermark ({newWatermark})")
    dbutils.notebook.exit(json.dumps({"status": "no_new_data", "rows_processed": 0}))

# COMMAND ----------

# DBTITLE 1,Build Source Query with Watermark Filter
# Format watermarks for SQL Server
oldWatermarkStr = oldWatermark.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
newWatermarkStr = newWatermark.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# Build the source query
if "SELECT" in source_table_param.upper():
    # It's already a query
    base_query = source_table_param
else:
    # It's a table name
    base_query = f"SELECT * FROM {source_table_param}"

# Add watermark filter if not full load
if is_full_load:
    source_query = f"({base_query}) AS src"
else:
    source_query = f"""(
        SELECT * FROM ({base_query}) AS inner_src
        WHERE {source_watermark_column} > '{oldWatermarkStr}'
        AND {source_watermark_column} <= '{newWatermarkStr}'
    ) AS src"""

print("Source Query:")
print(source_query)

# COMMAND ----------

# DBTITLE 1,Read Source Data via JDBC
sourceTable = (spark.read.format("jdbc")
    .option("driver", source_driver)
    .option("url", source_url_full)
    .option("dbtable", source_query)
    .option("user", source_user)
    .option("password", source_password)
    .option("fetchsize", "10000")
    .load()
)

source_count = sourceTable.count()
print(f"Source rows to process: {source_count}")

if source_count == 0:
    print("No rows to process. Exiting.")
    dbutils.notebook.exit(json.dumps({"status": "no_rows", "rows_processed": 0}))

# Register as temp view for SQL operations
sourceTable.createOrReplaceTempView("source_table")

# COMMAND ----------

# DBTITLE 1,Initial Load - Create Target Table
if not target_exists:
    print(f"Creating target table: {targetTableFullName}")

    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

    # Write as Delta table
    (sourceTable
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(targetTableFullName)
    )

    # Set watermark property
    newWatermarkStr = newWatermark.strftime('%Y-%m-%d %H:%M:%S.%f')
    spark.sql(f"ALTER TABLE {targetTableFullName} SET TBLPROPERTIES ('{sourceSyncPropertyName}' = '{newWatermarkStr}')")

    final_count = spark.table(targetTableFullName).count()
    print(f"Initial load complete. Rows: {final_count}")

    dbutils.notebook.exit(json.dumps({
        "status": "initial_load_complete",
        "rows_processed": final_count,
        "watermark": newWatermarkStr
    }))

# COMMAND ----------

# DBTITLE 1,Build Column Mapping
# Compare source and target columns
targetDF = DeltaTable.forName(spark, targetTableFullName).toDF()
fullMapping = {}

for target_col in targetDF.columns:
    if target_col in custom_column_mapping:
        fullMapping[target_col] = custom_column_mapping[target_col]
    elif target_col in sourceTable.columns:
        fullMapping[target_col] = target_col

# Build SELECT clause
parts = []
for key, value in fullMapping.items():
    if str(key) == str(value):
        parts.append(str(key))
    else:
        parts.append(f"{value} as {key}")

selectBlock = ", \n        ".join(parts)

print("Column Mapping:")
for k, v in fullMapping.items():
    if k != v:
        print(f"  {v} -> {k}")

# COMMAND ----------

# DBTITLE 1,Build MERGE Statement
sourceTableFilteredQuery = f"""
    SELECT
        {selectBlock}
    FROM source_table
    WHERE {source_watermark_column} > '{oldWatermarkStr}'::timestamp
    AND {source_watermark_column} <= '{newWatermarkStr}'::timestamp
"""
sourceTableBlock = sourceTableFilteredQuery

# Build match conditions from primary keys
matchOnConditions = [f"s.{pk} = t.{pk}" for pk in primary_key_columns]

# Optimization: Cache and add PK range filters for incremental loads
if oldWatermark > datetime(1900, 1, 1, 0, 0, 0, 0):
    # Cache the filtered source data
    print("Caching source data for optimization...")
    spark.sql(f"CACHE TABLE source_cached AS {sourceTableFilteredQuery}")
    sourceTableBlock = "    SELECT * FROM source_cached"

    # Add PK range filters to optimize target scan
    targetSchema = targetDF.schema
    statPKs = []
    for pk in primary_key_columns:
        if pk in [f.name for f in targetSchema.fields]:
            dtype = targetSchema[pk].dataType.typeName()
            if dtype in {'timestamp', 'date', 'integer', 'bigint', 'long', 'int'}:
                statPKs.append(pk)

    if len(statPKs) > 0:
        statQuery = f"""
SELECT
    {', '.join([f"MIN({pk}) as min_{pk}, MAX({pk}) as max_{pk}" for pk in statPKs])}
FROM source_cached"""
        print(statQuery)

        stat_df = spark.sql(statQuery).collect()

        for pk in statPKs:
            min_val = stat_df[0][f"min_{pk}"]
            max_val = stat_df[0][f"max_{pk}"]

            if min_val is None or max_val is None:
                logger.warning(f"PK {pk} has NULL values. Skipping range filter.")
                continue

            dtype = targetSchema[pk].dataType.typeName()
            typeCast = ""
            delimiter = ""

            if dtype == 'timestamp':
                typeCast = "::TIMESTAMP"
                delimiter = "'"
            elif dtype == 'date':
                typeCast = "::DATE"
                delimiter = "'"

            strmin = f"{delimiter}{min_val}{delimiter}{typeCast}"
            strmax = f"{delimiter}{max_val}{delimiter}{typeCast}"

            if strmin == strmax:
                matchOnConditions.append(f"(t.{pk} = {strmin})")
            else:
                matchOnConditions.append(f"(t.{pk} BETWEEN {strmin} AND {strmax})")

matchOnBlock = '\n  AND '.join(matchOnConditions)

# Build UPDATE clause
whenMatchedUpdateBlock = ""
if allow_update:
    whenMatchedUpdateConditions = []
    for key in fullMapping.keys():
        if key in ignore_changes_columns or key in primary_key_columns:
            continue

        if key in compare_override_columns:
            whenMatchedUpdateConditions.append(compare_override_columns[key])
        else:
            whenMatchedUpdateConditions.append(f"s.{key} IS DISTINCT FROM t.{key}")

    whenMatchedUpdateConditionBlock = ""
    if len(whenMatchedUpdateConditions) > 0:
        whenMatchedUpdateConditionBlock = f"""
AND (
    {' OR '.join(whenMatchedUpdateConditions)}
)
"""
    whenMatchedUpdateBlock = f"WHEN MATCHED {whenMatchedUpdateConditionBlock} THEN UPDATE SET *"

# COMMAND ----------

# DBTITLE 1,Execute MERGE
fullSQL = f"""
MERGE INTO {targetTableFullName} t
USING
(
{sourceTableBlock}
) s ON {matchOnBlock}
{whenMatchedUpdateBlock}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({', '.join(fullMapping.keys())})
    VALUES ({', '.join(fullMapping.keys())})
"""

print("MERGE SQL:")
print(fullSQL)

# Execute the MERGE
spark.sql(fullSQL)

# COMMAND ----------

# DBTITLE 1,Update Watermark Property
newWatermarkStr = newWatermark.strftime('%Y-%m-%d %H:%M:%S.%f')
spark.sql(f"ALTER TABLE {targetTableFullName} SET TBLPROPERTIES ('{sourceSyncPropertyName}' = '{newWatermarkStr}')")

print(f"Updated watermark to: {newWatermarkStr}")

# COMMAND ----------

# DBTITLE 1,Cleanup and Return Results
# Uncache if we cached
try:
    spark.sql("UNCACHE TABLE IF EXISTS source_cached")
except:
    pass

# Get final row count
final_count = spark.table(targetTableFullName).count()

result = {
    "status": "success",
    "source_table": f"{source_schema}.{source_dataset_ref}",
    "target_table": targetTableFullName,
    "rows_processed": source_count,
    "total_rows": final_count,
    "old_watermark": oldWatermarkStr,
    "new_watermark": newWatermarkStr
}

print("=" * 60)
print("COMPLETE")
print(f"Rows processed: {source_count}")
print(f"Total rows in target: {final_count}")
print("=" * 60)

dbutils.notebook.exit(json.dumps(result))
