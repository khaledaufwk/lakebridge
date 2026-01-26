# Databricks notebook source
# MAGIC %md
# MAGIC # Debug Fact Observations

# COMMAND ----------

# Test 1: Check source table schema
print("=== Source Table Schema ===")
try:
    df = spark.table("wakecap_prod.raw.observation_observation")
    df.printSchema()
    print(f"\nRow count: {df.count():,}")
except Exception as e:
    print(f"ERROR: {e}")

# COMMAND ----------

# Test 2: Check if columns we need exist
print("=== Required Columns ===")
df = spark.table("wakecap_prod.raw.observation_observation")
cols = df.columns
print(f"Total columns: {len(cols)}")

# Columns we need
needed = [
    "Id", "ProjectId", "Discriminator", "Source", "Type", "Severity", "Status",
    "ClinicViolationStatus", "Timestamp", "Threshold", "LocationWKT",
    "Latitude", "Longitude", "CreatedAt", "UpdatedAt", "DeletedAt"
]

for c in needed:
    if c in cols:
        print(f"  [OK] {c}")
    else:
        # Check case-insensitive
        matches = [x for x in cols if x.lower() == c.lower()]
        if matches:
            print(f"  [CASE] {c} -> {matches[0]}")
        else:
            print(f"  [MISSING] {c}")

# COMMAND ----------

# Test 3: Sample data from source
print("=== Sample Row ===")
df = spark.table("wakecap_prod.raw.observation_observation")
row = df.limit(1).collect()
if row:
    for k, v in row[0].asDict().items():
        print(f"  {k}: {v} ({type(v).__name__})")

# COMMAND ----------

# Test 4: Test ProjectId casting
print("=== ProjectId Test ===")
from pyspark.sql import functions as F

df = spark.table("wakecap_prod.raw.observation_observation")
df_with_cast = df.withColumn("ProjectId_int", F.col("ProjectId").cast("int"))

# Check for NULL casts
null_count = df_with_cast.filter(F.col("ProjectId_int").isNull() & F.col("ProjectId").isNotNull()).count()
total = df_with_cast.count()
print(f"Total rows: {total:,}")
print(f"Rows where ProjectId failed to cast: {null_count:,}")

# Sample ProjectId values
print("\nSample ProjectId values:")
df.select("ProjectId").distinct().limit(10).show()

# COMMAND ----------

# Test 5: Check dimension tables
print("=== Dimension Tables ===")
dims = [
    "wakecap_prod.silver.silver_project",
    "wakecap_prod.silver.silver_observation_discriminator",
    "wakecap_prod.silver.silver_observation_source",
    "wakecap_prod.silver.silver_observation_type"
]

for dim in dims:
    try:
        count = spark.table(dim).count()
        print(f"  [OK] {dim}: {count} rows")
    except Exception as e:
        print(f"  [ERROR] {dim}: {e}")

# COMMAND ----------

# Test 6: Try a simple join
print("=== Test Join ===")
from pyspark.sql import functions as F

try:
    source = spark.table("wakecap_prod.raw.observation_observation").limit(100)
    source = source.withColumn("ProjectId_int", F.col("ProjectId").cast("int"))

    project = spark.table("wakecap_prod.silver.silver_project")
    print(f"Project dimension columns: {project.columns}")
    print(f"Project sample ExtProjectID values:")
    project.select("ExtProjectID").limit(5).show()

    joined = source.alias("s").join(
        project.alias("p"),
        F.col("s.ProjectId_int") == F.col("p.ExtProjectID"),
        "inner"
    )

    count = joined.count()
    print(f"Joined count: {count}")
except Exception as e:
    import traceback
    print(f"ERROR: {e}")
    traceback.print_exc()

# COMMAND ----------

print("=== Debug Complete ===")
