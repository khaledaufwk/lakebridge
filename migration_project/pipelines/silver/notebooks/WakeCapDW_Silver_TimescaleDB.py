# Databricks notebook source
# MAGIC %md
# MAGIC # WakeCapDW Silver Layer - TimescaleDB Sources
# MAGIC
# MAGIC **Purpose:** Consolidated Silver layer transformations for all TimescaleDB source tables.
# MAGIC This notebook processes bronze layer data from TimescaleDB and creates silver dimension tables.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. **Dim Organization** - From wc2023_Organization
# MAGIC 2. **Dim Project** - From wc2023_Project
# MAGIC 3. **Dim Worker** - From wc2023_People
# MAGIC 4. **Manager Assignments Expanded** - Joins silver_crew_manager + silver_crew_composition
# MAGIC
# MAGIC **Source Tables (Bronze):**
# MAGIC - `wakecap_prod.bronze.wc2023_organization`
# MAGIC - `wakecap_prod.bronze.wc2023_project`
# MAGIC - `wakecap_prod.bronze.wc2023_people`
# MAGIC - `wakecap_prod.silver.silver_crew_manager`
# MAGIC - `wakecap_prod.silver.silver_crew_composition`
# MAGIC
# MAGIC **Target Tables (Silver):**
# MAGIC - `wakecap_prod.silver.silver_organization`
# MAGIC - `wakecap_prod.silver.silver_project`
# MAGIC - `wakecap_prod.silver.silver_worker`
# MAGIC - `wakecap_prod.silver.silver_manager_assignments_expanded`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global Configuration

# COMMAND ----------

# Catalog and Schema Configuration
TARGET_CATALOG = "wakecap_prod"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
MIGRATION_SCHEMA = "migration"

WATERMARK_TABLE = f"{TARGET_CATALOG}.{MIGRATION_SCHEMA}._gold_watermarks"

# ExtSourceID for TimescaleDB sources
EXT_SOURCE_ID_TS = 14  # For organization, project
EXT_SOURCE_ID_PEOPLE = 15  # For people/workers

print(f"Catalog: {TARGET_CATALOG}")
print(f"Bronze Schema: {BRONZE_SCHEMA}")
print(f"Silver Schema: {SILVER_SCHEMA}")

# COMMAND ----------

# Widget parameters
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"], "Load Mode")
dbutils.widgets.dropdown("section", "all", ["all", "organization", "project", "worker", "manager_assignments"], "Section to Run")

load_mode = dbutils.widgets.get("load_mode")
section_to_run = dbutils.widgets.get("section")

print(f"Load Mode: {load_mode}")
print(f"Section: {section_to_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_watermark(table_name):
    """Get last processed watermark for incremental loading."""
    try:
        result = spark.sql(f"""
            SELECT last_watermark_value
            FROM {WATERMARK_TABLE}
            WHERE table_name = '{table_name}'
        """).collect()
        if result and result[0][0]:
            return result[0][0]
    except Exception as e:
        print(f"  Watermark lookup failed: {str(e)[:50]}")
    return datetime(1900, 1, 1)


def update_watermark(table_name, watermark_value, row_count):
    """Update watermark after successful processing."""
    try:
        spark.sql(f"""
            MERGE INTO {WATERMARK_TABLE} AS target
            USING (SELECT '{table_name}' as table_name,
                          CAST('{watermark_value}' AS TIMESTAMP) as last_watermark_value,
                          {row_count} as row_count,
                          current_timestamp() as last_processed_at,
                          current_timestamp() as updated_at) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET
                target.last_watermark_value = source.last_watermark_value,
                target.row_count = source.row_count,
                target.last_processed_at = source.last_processed_at,
                target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        print(f"  Warning: Could not update watermark: {str(e)[:50]}")


def fn_ext_source_id_alias(col_name):
    """PySpark equivalent of stg.fnExtSourceIDAlias(ExtSourceID)."""
    return F.when(F.col(col_name).isin(15, 18), F.lit(15)) \
            .when(F.col(col_name).isin(1, 2, 10, 14, 21), F.lit(1)) \
            .otherwise(F.col(col_name))


def check_table_exists(table_name):
    """Check if a table exists."""
    try:
        spark.sql(f"SELECT 1 FROM {table_name} LIMIT 0")
        return True
    except:
        return False

# COMMAND ----------

# Ensure schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{MIGRATION_SCHEMA}")
print("[OK] Schemas verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Dim Organization
# MAGIC
# MAGIC **Source:** `wc2023_organization`
# MAGIC **Target:** `silver_organization`
# MAGIC **Original SP:** `stg.spDeltaSyncDimOrganization`

# COMMAND ----------

def process_dim_organization():
    """Process Organization dimension from TimescaleDB."""
    print("=" * 60)
    print("PROCESSING: Dim Organization")
    print("=" * 60)

    SOURCE_TABLE = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_organization"
    TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_organization"

    # Check source
    if not check_table_exists(SOURCE_TABLE):
        print(f"[SKIP] Source not found: {SOURCE_TABLE}")
        return "SOURCE_NOT_FOUND"

    # Load source
    source_df = spark.table(SOURCE_TABLE)
    source_count = source_df.count()
    print(f"Source records: {source_count}")

    # Add ExtSourceID and calculated columns
    source_prepared = source_df \
        .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID_TS)) \
        .withColumn("ImageURL", F.substring(F.trim(F.col("LogoUrl")), 1, 1024))

    # ROW_NUMBER deduplication on Id
    dedup_window = Window.partitionBy("Id").orderBy(F.lit(1))
    source_final = source_prepared \
        .withColumn("RN", F.row_number().over(dedup_window)) \
        .filter(F.col("RN") == 1) \
        .drop("RN")

    final_count = source_final.count()
    print(f"After dedup: {final_count}")

    # Create target table if not exists
    target_schema = """
        OrganizationID BIGINT GENERATED ALWAYS AS IDENTITY,
        ExtOrganizationID STRING,
        ExtSourceID INT,
        Organization STRING,
        OrganizationName STRING,
        Status STRING,
        ImageURL STRING,
        TimeZoneName STRING,
        CreatedAt TIMESTAMP,
        UpdatedAt TIMESTAMP,
        WatermarkUTC TIMESTAMP DEFAULT current_timestamp(),
        _silver_processed_at TIMESTAMP DEFAULT current_timestamp()
    """

    if not check_table_exists(TARGET_TABLE):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({target_schema})
            USING DELTA CLUSTER BY (ExtSourceID, ExtOrganizationID)
        """)
        print(f"[OK] Created target table")

    target_before = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

    # Execute MERGE
    source_final.createOrReplaceTempView("org_source")

    merge_sql = f"""
    MERGE INTO {TARGET_TABLE} AS t
    USING org_source AS s
    ON t.ExtOrganizationID = s.Id
       AND CASE WHEN t.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE t.ExtSourceID END
         = CASE WHEN s.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE s.ExtSourceID END

    WHEN MATCHED AND (
        t.Organization IS DISTINCT FROM s.Name OR
        t.Status IS DISTINCT FROM s.Status OR
        t.ImageURL IS DISTINCT FROM s.ImageURL
    )
    THEN UPDATE SET
        t.WatermarkUTC = current_timestamp(),
        t._silver_processed_at = current_timestamp(),
        t.Organization = s.Name,
        t.OrganizationName = s.Name,
        t.Status = s.Status,
        t.ImageURL = s.ImageURL,
        t.ExtSourceID = s.ExtSourceID,
        t.CreatedAt = s.CreatedAt,
        t.UpdatedAt = s.UpdatedAt

    WHEN NOT MATCHED THEN INSERT (
        ExtOrganizationID, ExtSourceID, Organization, OrganizationName, Status, ImageURL, CreatedAt, UpdatedAt
    )
    VALUES (s.Id, s.ExtSourceID, s.Name, s.Name, s.Status, s.ImageURL, s.CreatedAt, s.UpdatedAt)
    """

    spark.sql(merge_sql)
    spark.catalog.dropTempView("org_source")

    target_after = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    update_watermark("silver_organization", datetime.now(), target_after)

    print(f"Target: {target_before} -> {target_after} (+{target_after - target_before})")
    return f"SUCCESS: {final_count} processed"

# COMMAND ----------

if section_to_run in ["all", "organization"]:
    org_result = process_dim_organization()
    print(f"\nOrganization Result: {org_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Dim Project
# MAGIC
# MAGIC **Source:** `wc2023_project`
# MAGIC **Target:** `silver_project`
# MAGIC **Original SP:** `stg.spDeltaSyncDimProject`

# COMMAND ----------

def process_dim_project():
    """Process Project dimension from TimescaleDB."""
    print("=" * 60)
    print("PROCESSING: Dim Project")
    print("=" * 60)

    SOURCE_TABLE = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_project"
    TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_project"
    DIM_TIMEZONE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_timezone_mapping"
    DIM_ORGANIZATION = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_organization"

    # Check source
    if not check_table_exists(SOURCE_TABLE):
        print(f"[SKIP] Source not found: {SOURCE_TABLE}")
        return "SOURCE_NOT_FOUND"

    # Load source
    source_df = spark.table(SOURCE_TABLE)
    source_count = source_df.count()
    print(f"Source records: {source_count}")

    # Add calculated columns
    source_calc = source_df \
        .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID_TS)) \
        .withColumn("Name2", F.trim(F.col("Name")))

    # ROW_NUMBER deduplication
    dedup_window = Window.partitionBy("Id").orderBy(F.lit(1))
    source_dedup = source_calc \
        .withColumn("RN", F.row_number().over(dedup_window)) \
        .filter(F.col("RN") == 1) \
        .drop("RN")

    # Join with TimeZone mapping (optional)
    if check_table_exists(DIM_TIMEZONE):
        tz_lookup_df = spark.table(DIM_TIMEZONE) \
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy("IANATimeZone").orderBy(F.lit(1))
            )) \
            .filter(F.col("_rn") == 1) \
            .select(
                F.col("WindowsTimeZone").alias("dim_WindowsTimeZone"),
                F.col("IANATimeZone").alias("dim_IANATimeZone")
            )
        source_with_tz = source_dedup.alias("s").join(
            tz_lookup_df.alias("tz"),
            F.col("s.TimeZone") == F.col("tz.dim_IANATimeZone"),
            "left"
        ).select("s.*", F.col("tz.dim_WindowsTimeZone").alias("WindowsTimeZone"))
    else:
        source_with_tz = source_dedup.withColumn("WindowsTimeZone", F.lit(None).cast("string"))

    # Join with Organization dimension (optional)
    if check_table_exists(DIM_ORGANIZATION):
        org_lookup_df = spark.table(DIM_ORGANIZATION) \
            .withColumn("_ext_alias", fn_ext_source_id_alias("ExtSourceID")) \
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy("ExtOrganizationID", "_ext_alias").orderBy(F.lit(1))
            )) \
            .filter(F.col("_rn") == 1) \
            .select(
                F.col("OrganizationID").alias("dim_OrganizationID"),
                F.col("ExtOrganizationID").alias("dim_ExtOrganizationID")
            )
        source_final = source_with_tz.alias("s").join(
            org_lookup_df.alias("org"),
            F.col("s.TenantId") == F.col("org.dim_ExtOrganizationID"),
            "left"
        ).select("s.*", F.col("org.dim_OrganizationID").alias("OrganizationID"))
    else:
        source_final = source_with_tz.withColumn("OrganizationID", F.lit(None).cast("int"))

    final_count = source_final.count()
    print(f"After joins: {final_count}")

    # Create target table if not exists
    target_schema = """
        ProjectID BIGINT GENERATED ALWAYS AS IDENTITY,
        ExtProjectID STRING,
        ExtSourceID INT,
        Project STRING,
        Status STRING,
        ProjectType STRING,
        ExtProjectTypeID STRING,
        ContractType STRING,
        ExtContractTypeID STRING,
        TimeZoneName STRING,
        IANATimeZoneName STRING,
        DateFormat STRING,
        StartDate TIMESTAMP,
        IncludeMissedReadings BOOLEAN,
        AttendanceThreshold INT,
        UndeliveredThreshold INT,
        DefaultTracingDistance DOUBLE,
        ZoneBufferDistance DOUBLE,
        ExtCustomerExpectedHoursID STRING,
        ExtWakecapWorkingHoursID STRING,
        OrganizationID INT,
        ExtOrganizationID STRING,
        CreatedAt TIMESTAMP,
        UpdatedAt TIMESTAMP,
        WatermarkUTC TIMESTAMP DEFAULT current_timestamp(),
        _silver_processed_at TIMESTAMP DEFAULT current_timestamp()
    """

    if not check_table_exists(TARGET_TABLE):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({target_schema})
            USING DELTA CLUSTER BY (ExtSourceID, ExtProjectID)
        """)
        print(f"[OK] Created target table")

    target_before = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

    # Execute MERGE
    source_final.createOrReplaceTempView("project_source")

    merge_sql = f"""
    MERGE INTO {TARGET_TABLE} AS t
    USING project_source AS s
    ON t.ExtProjectID = s.Id
       AND CASE WHEN t.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE t.ExtSourceID END
         = CASE WHEN s.ExtSourceID IN (1, 2, 10, 14, 21) THEN 1 ELSE s.ExtSourceID END

    WHEN MATCHED AND (
        t.Project IS DISTINCT FROM s.Name2 OR
        t.Status IS DISTINCT FROM s.Status OR
        t.OrganizationID IS DISTINCT FROM s.OrganizationID
    )
    THEN UPDATE SET
        t.WatermarkUTC = current_timestamp(),
        t._silver_processed_at = current_timestamp(),
        t.Project = s.Name2,
        t.Status = s.Status,
        t.OrganizationID = s.OrganizationID,
        t.ExtOrganizationID = s.TenantId,
        t.IANATimeZoneName = s.TimeZone,
        t.TimeZoneName = s.WindowsTimeZone,
        t.UpdatedAt = s.UpdatedAt

    WHEN NOT MATCHED THEN INSERT (
        ExtProjectID, ExtSourceID, Project, Status, OrganizationID, ExtOrganizationID,
        IANATimeZoneName, TimeZoneName, CreatedAt, UpdatedAt
    )
    VALUES (
        s.Id, s.ExtSourceID, s.Name2, s.Status, s.OrganizationID, s.TenantId,
        s.TimeZone, s.WindowsTimeZone, s.CreatedAt, s.UpdatedAt
    )
    """

    spark.sql(merge_sql)
    spark.catalog.dropTempView("project_source")

    target_after = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    update_watermark("silver_project", datetime.now(), target_after)

    print(f"Target: {target_before} -> {target_after} (+{target_after - target_before})")
    return f"SUCCESS: {final_count} processed"

# COMMAND ----------

if section_to_run in ["all", "project"]:
    proj_result = process_dim_project()
    print(f"\nProject Result: {proj_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Dim Worker
# MAGIC
# MAGIC **Source:** `wc2023_people`
# MAGIC **Target:** `silver_worker`
# MAGIC **Original SP:** `stg.spDeltaSyncDimWorker`

# COMMAND ----------

def process_dim_worker():
    """Process Worker dimension from TimescaleDB."""
    print("=" * 60)
    print("PROCESSING: Dim Worker")
    print("=" * 60)

    SOURCE_TABLE = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.wc2023_people"
    TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_worker"

    # Check source
    if not check_table_exists(SOURCE_TABLE):
        print(f"[SKIP] Source not found: {SOURCE_TABLE}")
        return "SOURCE_NOT_FOUND"

    # Load source
    source_df = spark.table(SOURCE_TABLE)
    source_count = source_df.count()
    print(f"Source records: {source_count}")

    # Helper for LEFT(TRIM(...), length)
    def left_trim(col_name, max_length):
        return F.substring(F.trim(F.col(col_name)), 1, max_length)

    # Apply calculated columns
    source_with_calc = source_df \
        .withColumn("ExtSourceID", F.lit(EXT_SOURCE_ID_PEOPLE)) \
        .withColumn("Worker", left_trim("PeopleCode", 50)) \
        .withColumn("WorkerName", left_trim("Name", 100)) \
        .withColumn("MobileNumber", left_trim("Mobile", 50)) \
        .withColumn("Email2", left_trim("Email", 50)) \
        .withColumn("ImageURL", left_trim("Picture", 1024)) \
        .withColumn("Color", left_trim("HelmetColor", 10)) \
        .withColumn("Address2", left_trim("Address", 100)) \
        .withColumn("Nationality2", left_trim("Nationality", 100))

    # ROW_NUMBER deduplication
    dedup_window = Window.partitionBy("Id").orderBy(F.lit(1))
    source_final = source_with_calc \
        .withColumn("RN", F.row_number().over(dedup_window)) \
        .filter(F.col("RN") == 1) \
        .drop("RN")

    final_count = source_final.count()
    print(f"After dedup: {final_count}")

    # Create target table if not exists
    target_schema = """
        WorkerID BIGINT GENERATED ALWAYS AS IDENTITY,
        ExtWorkerID STRING,
        ExtSourceID INT,
        ExtProjectID STRING,
        ProjectID INT,
        ExtCompanyID STRING,
        CompanyID INT,
        ExtTradeID STRING,
        TradeID INT,
        ExtTitleID STRING,
        ExtDepartmentID STRING,
        Worker STRING,
        WorkerName STRING,
        MobileNumber STRING,
        Email STRING,
        ImageURL STRING,
        Color STRING,
        Address STRING,
        Nationality STRING,
        CreatedAt TIMESTAMP,
        UpdatedAt TIMESTAMP,
        DeletedAt TIMESTAMP,
        WatermarkUTC TIMESTAMP DEFAULT current_timestamp(),
        _silver_processed_at TIMESTAMP DEFAULT current_timestamp()
    """

    if not check_table_exists(TARGET_TABLE):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({target_schema})
            USING DELTA CLUSTER BY (ExtSourceID, ExtWorkerID)
        """)
        print(f"[OK] Created target table")

    target_before = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

    # Execute MERGE
    source_final.createOrReplaceTempView("worker_source")

    merge_sql = f"""
    MERGE INTO {TARGET_TABLE} AS t
    USING worker_source AS s
    ON t.ExtWorkerID = s.Id
       AND CASE WHEN t.ExtSourceID IN (15, 18) THEN 15 ELSE t.ExtSourceID END
         = CASE WHEN s.ExtSourceID IN (15, 18) THEN 15 ELSE s.ExtSourceID END

    WHEN MATCHED AND (
        t.Worker IS DISTINCT FROM s.Worker OR
        t.WorkerName IS DISTINCT FROM s.WorkerName OR
        t.Email IS DISTINCT FROM s.Email2 OR
        t.DeletedAt IS DISTINCT FROM s.DeletedAt
    )
    THEN UPDATE SET
        t.WatermarkUTC = current_timestamp(),
        t._silver_processed_at = current_timestamp(),
        t.Worker = s.Worker,
        t.WorkerName = s.WorkerName,
        t.MobileNumber = s.MobileNumber,
        t.Email = s.Email2,
        t.ImageURL = s.ImageURL,
        t.Color = s.Color,
        t.Address = s.Address2,
        t.Nationality = s.Nationality2,
        t.ExtProjectID = s.ProjectId,
        t.ExtCompanyID = s.CompanyId,
        t.ExtTradeID = s.TradeId,
        t.ExtTitleID = s.TitleId,
        t.ExtDepartmentID = s.DepartmentId,
        t.CreatedAt = s.CreatedAt,
        t.UpdatedAt = s.UpdatedAt,
        t.DeletedAt = s.DeletedAt

    WHEN NOT MATCHED THEN INSERT (
        ExtWorkerID, ExtSourceID, Worker, WorkerName, MobileNumber, Email, ImageURL, Color,
        Address, Nationality, ExtProjectID, ExtCompanyID, ExtTradeID, ExtTitleID, ExtDepartmentID,
        CreatedAt, UpdatedAt, DeletedAt
    )
    VALUES (
        s.Id, s.ExtSourceID, s.Worker, s.WorkerName, s.MobileNumber, s.Email2, s.ImageURL, s.Color,
        s.Address2, s.Nationality2, s.ProjectId, s.CompanyId, s.TradeId, s.TitleId, s.DepartmentId,
        s.CreatedAt, s.UpdatedAt, s.DeletedAt
    )
    """

    spark.sql(merge_sql)
    spark.catalog.dropTempView("worker_source")

    target_after = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    update_watermark("silver_worker", datetime.now(), target_after)

    print(f"Target: {target_before} -> {target_after} (+{target_after - target_before})")
    return f"SUCCESS: {final_count} processed"

# COMMAND ----------

if section_to_run in ["all", "worker"]:
    worker_result = process_dim_worker()
    print(f"\nWorker Result: {worker_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Manager Assignments Expanded
# MAGIC
# MAGIC **Sources:** `silver_crew_manager` + `silver_crew_composition`
# MAGIC **Target:** `silver_manager_assignments_expanded`
# MAGIC **Equivalent to:** `dbo.vwManagerAssignments_Expanded`
# MAGIC
# MAGIC This creates an expanded view of manager assignments by joining crew composition
# MAGIC (workers in crews) with crew managers (who manages each crew).

# COMMAND ----------

def process_manager_assignments_expanded():
    """Process Manager Assignments Expanded from silver tables."""
    print("=" * 60)
    print("PROCESSING: Manager Assignments Expanded")
    print("=" * 60)

    SOURCE_CREW_MANAGER = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_manager"
    SOURCE_CREW_COMPOSITION = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_crew_composition"
    TARGET_TABLE = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.silver_manager_assignments_expanded"

    # Check sources
    if not check_table_exists(SOURCE_CREW_MANAGER):
        print(f"[SKIP] Source not found: {SOURCE_CREW_MANAGER}")
        return "SOURCE_NOT_FOUND: crew_manager"

    if not check_table_exists(SOURCE_CREW_COMPOSITION):
        print(f"[SKIP] Source not found: {SOURCE_CREW_COMPOSITION}")
        return "SOURCE_NOT_FOUND: crew_composition"

    # Load sources
    crew_manager_df = spark.table(SOURCE_CREW_MANAGER)
    crew_composition_df = spark.table(SOURCE_CREW_COMPOSITION)

    crew_manager_count = crew_manager_df.count()
    crew_composition_count = crew_composition_df.count()
    print(f"Crew Manager records: {crew_manager_count:,}")
    print(f"Crew Composition records: {crew_composition_count:,}")

    # Get watermarks
    new_mgr_watermark = crew_manager_df.agg(F.max("UpdatedAt")).collect()[0][0]
    new_comp_watermark = crew_composition_df.agg(F.max("UpdatedAt")).collect()[0][0]

    if new_mgr_watermark is None:
        new_mgr_watermark = datetime.now()
    if new_comp_watermark is None:
        new_comp_watermark = datetime.now()

    # Prepare workers (exclude deleted)
    workers_df = crew_composition_df.filter(
        F.col("DeletedAt").isNull()
    ).select(
        F.col("ProjectId"),
        F.col("CrewId"),
        F.col("WorkerId")
    ).distinct()

    workers_count = workers_df.count()
    print(f"Active workers in crews: {workers_count:,}")

    # Prepare managers
    managers_df = crew_manager_df.select(
        F.col("ProjectId"),
        F.col("CrewId"),
        F.col("ManagerId").alias("ManagerWorkerId"),
        F.col("EffectiveDate").alias("ValidFrom_ShiftLocalDate"),
        F.col("UpdatedAt")
    ).withColumn(
        "ValidTo_ShiftLocalDate", F.lit(None).cast("date")
    )

    # Create expanded assignments
    expanded_assignments = workers_df.alias("w").join(
        managers_df.alias("m"),
        (F.col("w.ProjectId") == F.col("m.ProjectId")) &
        (F.col("w.CrewId") == F.col("m.CrewId")),
        "inner"
    ).select(
        F.col("w.ProjectId"),
        F.col("w.CrewId"),
        F.col("w.WorkerId"),
        F.col("m.ManagerWorkerId"),
        F.col("m.ValidFrom_ShiftLocalDate"),
        F.col("m.ValidTo_ShiftLocalDate"),
        F.col("m.UpdatedAt"),
        F.current_timestamp().alias("_silver_processed_at")
    )

    expanded_count = expanded_assignments.count()
    print(f"Expanded assignments: {expanded_count:,}")

    # Write to target (overwrite for simplicity since this is a view equivalent)
    if check_table_exists(TARGET_TABLE):
        target_before = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    else:
        target_before = 0

    (expanded_assignments
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(TARGET_TABLE)
    )

    target_after = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]

    # Update watermark
    combined_watermark = max(new_mgr_watermark, new_comp_watermark)
    update_watermark("silver_manager_assignments_expanded", combined_watermark, target_after)

    print(f"Target: {target_before} -> {target_after}")
    return f"SUCCESS: {expanded_count} records"

# COMMAND ----------

if section_to_run in ["all", "manager_assignments"]:
    mgr_result = process_manager_assignments_expanded()
    print(f"\nManager Assignments Result: {mgr_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("WAKECAP SILVER LAYER PROCESSING COMPLETE")
print("=" * 60)
print(f"""
Configuration:
  Load Mode: {load_mode}
  Section:   {section_to_run}
  Catalog:   {TARGET_CATALOG}

Tables Processed:
  - silver_organization
  - silver_project
  - silver_worker
  - silver_manager_assignments_expanded

All silver layer transformations from TimescaleDB sources completed.
""")

dbutils.notebook.exit("SUCCESS")
