# ADF to Databricks Gap Analysis

**Generated:** 2026-01-28
**Last Updated:** 2026-01-28 (Gap 4 corrected: MV uses window function, not DeletedAt)
**Source:** `Wakecap-ADF-Prod24` ARM Template Export
**Target:** Databricks Unity Catalog (`wakecap_prod`)

---

## Executive Summary

Analysis of the live ADF pipelines versus the current Databricks implementation reveals **7 gaps** that need to be addressed for full parity.

| Gap | Severity | Status | Recommendation |
|-----|----------|--------|----------------|
| 1. Date Range Filtering | Medium | **FIXED** | Added to gold_fact_reported_attendance.py |
| 2. ResourceTimesheet LinkedUserId Lookup | Medium | **FIXED** | Added LinkedUserId to silver_worker, fixed Gold lookup |
| 3. DeviceLocation Spatial Joins | High | **IN PROGRESS** | H3 notebooks deployed, pending Bronze geometry data |
| 4. SyncFacts MV_ResourceDevice_NoViolation | High | **CORRECTED** | No DeletedAt/ProjectId needed - MV uses window function for violation detection |
| 5. Inactive ADF Activities | Info | N/A | Document only |
| 6. Weather Station Sensor (SyncFacts) | High | **FIXED** | Added weather_station_sensor to Bronze/Silver/Gold |
| 7. Observation Dimensions (SyncDimensionsObservations) | Medium | **FIXED** | Added 6 dimension tables from observation Bronze |

---

## ADF Pipeline Structure

### Main Pipelines

```
Nightly (Trigger: Daily 00:00 UTC)
    └── SyncDimensions
            ├── SyncDimOrganization
            ├── SyncDimProject
            ├── SyncDimFloor (Space)
            ├── SyncDimZone
            ├── SyncDimTrade
            ├── SyncDimWorker (People)
            ├── SyncDimWorkshift
            ├── SyncDimWorkshiftDetails
            ├── SyncDimCrew
            ├── SyncDimDevice
            ├── SyncDimCompany
            ├── SyncDimTitle
            ├── SyncDimDepartment
            ├── SyncDimWorkerStatus
            ├── SyncDimActivity
            ├── SyncDimLocationGroupAssignments
            └── Quasi Facts and Recalc
                    └── SyncQuasiFactsAndRecalc
                            ├── SyncDeviceAssignments
                            ├── SyncCrewComposition
                            ├── SyncWorkersWorkshiftAssignments
                            ├── SyncCrewManagerAssignments
                            ├── SyncManagerAssignments_copy1 (OBS)
                            ├── SyncResourceHours
                            ├── SyncResourceTimesheet
                            ├── SyncLocationAssignments
                            ├── CalcDeviceAssignments
                            ├── CalculateCrewAssignments
                            ├── CalculateTradeAssignments
                            ├── CalculateWorkshiftAssignmentsCombined
                            ├── CalcManagerAssignments
                            ├── CalcManagerAssignmentSnapshots
                            ├── CalculateFactReportedAttendance
                            └── RecalcAssignments
                                    ├── spWorkersHistory_UpdateAssignments_1_Crews
                                    ├── spWorkersHistory_UpdateAssignments_2_WorkShiftDates
                                    ├── spCalculateWorkerLocationAssignments
                                    ├── spWorkersHistory_UpdateAssignments_3_LocationClass
                                    └── spCalculateFactWorkersShifts

Hourly_Facts (Trigger: Every 1 hour)
    └── SyncFacts
            ├── REFRESH MV_ResourceDevice_NoViolation
            ├── DeltaCopyAssetLocation (DeviceLocation with spatial joins)
            ├── DeltaCopyWeatherStationSensor
            ├── DeltaCopyObservations
            ├── spCalculateFactWorkersShifts_Partial
            └── spCalculateFactWorkersShiftsCombined
```

---

## Gap 1: Date Range Filtering

### ADF Implementation

Multiple ADF sync activities filter dates to exclude invalid/outlier values:

```sql
-- ADF uses this pattern throughout:
WHERE "EffectiveDate" BETWEEN '2000-01-01' AND '2100-01-01'
WHERE "Date" BETWEEN '2000-01-01' AND '2100-01-01'
WHERE "Day" BETWEEN '2000-01-01' AND '2100-01-01'

-- For nullable date columns:
CASE WHEN "JoinDate" BETWEEN '2000-01-01' AND '2100-01-01' THEN "JoinDate" ELSE NULL END AS "JoinDate"
```

### Affected Tables (Active in ADF)

| ADF Activity | Column(s) | Filter Type |
|--------------|-----------|-------------|
| SyncCrewComposition | EffectiveDate | WHERE filter |
| SyncWorkersWorkshiftAssignments | EffectiveDate | WHERE filter |
| SyncCrewManagerAssignments | EffectiveDate | WHERE filter |
| SyncManagerAssignments_copy1 | EffectiveDate | WHERE filter |
| SyncResourceHours | Date | WHERE filter |
| SyncResourceTimesheet | Day | WHERE filter |
| SyncLocationAssignments | From, To | WHERE filter |
| SyncDimWorker | JoinDate, ReleaseDate, ActivatedAt, DeActivatedAt | CASE expression |

### Databricks Status

**Current:** No explicit date range filters in Bronze or Silver layer.

**Risk:** Invalid dates (e.g., `0001-01-01`, `9999-12-31`) may propagate to Gold tables.

### Recommendation

Add date validation to Silver layer via data quality expectations:

```yaml
# In silver_tables.yml for affected tables
expectations:
  critical:
    - "EffectiveDate BETWEEN '2000-01-01' AND '2100-01-01' OR EffectiveDate IS NULL"
```

Or add explicit filters in the Silver loader:

```python
# In silver_loader.py
if 'EffectiveDate' in df.columns:
    df = df.filter(
        (F.col("EffectiveDate").between("2000-01-01", "2100-01-01")) |
        F.col("EffectiveDate").isNull()
    )
```

---

## Gap 2: ResourceTimesheet LinkedUserId Lookup

### ADF Implementation

The `SyncResourceTimesheet` activity includes a critical join to resolve `ApprovedBy` (a user GUID) to `ApprovedById` (a People/Worker ID):

```sql
SELECT
    rt.*,
    p."Id" AS "ApprovedById"
FROM public."ResourceTimesheet" rt
LEFT JOIN
(
    SELECT
        "LinkedUserId",
        "Id" AS "ApprovedById",
        ROW_NUMBER() OVER (PARTITION BY "LinkedUserId" ORDER BY NULL) rn
    FROM "People" p
    WHERE p."LinkedUserId" IS NOT NULL
) p ON rt."ApprovedBy" = p."LinkedUserId" AND p.rn = 1
WHERE "Day" BETWEEN '2000-01-01' AND '2100-01-01'
```

### Databricks Status

**Current Implementation:**
- `silver_fact_resource_timesheet` has both `ApprovedBy` (user GUID) and `ApprovedById` columns
- The `ApprovedById` is loaded directly from the Bronze source
- Gold layer `gold_fact_reported_attendance.py` attempts to join on `ApprovedById`

**Gap:** The Bronze layer loads `ResourceTimesheet` directly from TimescaleDB, which may NOT include the pre-computed `ApprovedById` from the People join. Need to verify if TimescaleDB has this column or if it needs to be computed in Silver/Gold.

### Verification Query

```sql
-- Check if ApprovedById exists and is populated in Bronze
SELECT
    COUNT(*) as total,
    COUNT(ApprovedById) as with_approved_by_id,
    COUNT(ApprovedBy) as with_approved_by
FROM wakecap_prod.raw.timescale_resourcetimesheet
```

### Recommendation

If `ApprovedById` is not populated from source, add the join in Silver layer:

```python
# In silver_fact_resource_timesheet processing
# Join with silver_worker to resolve ApprovedBy → ApprovedById
worker_lookup = spark.table("wakecap_prod.silver.silver_worker").select(
    F.col("LinkedUserId"),
    F.col("WorkerId").alias("ResolvedApprovedById")
).filter(F.col("LinkedUserId").isNotNull())

# Add row_number for deduplication (same as ADF)
window = Window.partitionBy("LinkedUserId").orderBy(F.lit(1))
worker_lookup = worker_lookup.withColumn("rn", F.row_number().over(window))
worker_lookup = worker_lookup.filter(F.col("rn") == 1).drop("rn")

# Join and coalesce
timesheet_df = timesheet_df.join(
    worker_lookup,
    timesheet_df.ApprovedBy == worker_lookup.LinkedUserId,
    "left"
).withColumn(
    "ApprovedById",
    F.coalesce(F.col("ApprovedById"), F.col("ResolvedApprovedById"))
)
```

---

## Gap 3: DeviceLocation Spatial Joins (SyncFacts)

### ADF Implementation

The `DeltaCopyAssetLocation` activity in `SyncFacts` pipeline performs complex spatial operations:

```sql
SELECT
    dl."DeviceId" AS node_id,
    dl."ProjectId" AS project_id,
    dl."SpaceId" AS space_id,
    ST_Y(dl."Point") as latitude,
    ST_X(dl."Point") as longitude,
    z."Id" as "ZoneId",
    da."ResourceId"
FROM public."DeviceLocation" dl
LEFT JOIN public."Zone" z
    ON dl."SpaceId" = z."SpaceId"
    AND ST_CONTAINS(z."Coordinates", dl."Point")  -- Spatial containment!
    AND (z."DeletedAt" IS NULL OR z."DeletedAt" > dl."GeneratedAt")
INNER JOIN public."MV_ResourceDevice_NoViolation" da
    ON da."DeviceId" = dl."DeviceId"
    AND dl."GeneratedAt" >= da."AssignedAt"
    AND (dl."GeneratedAt" < da."UnAssignedAt" OR da."UnAssignedAt" IS NULL)
WHERE dl."CreatedAt" > '@TSFROM'
  AND dl."CreatedAt" <= '@TSTO'
```

### Key Operations

1. **Spatial Join:** `ST_CONTAINS(z.Coordinates, dl.Point)` - Find which zone contains each device location point
2. **Device Assignment Join:** Link device location to worker via `MV_ResourceDevice_NoViolation`
3. **Temporal Validity:** Check assignment validity at the time of the location reading

### Databricks Status

**Current:**
- Bronze layer has `timescale_devicelocation` with `PointWKT` geometry column
- No spatial containment logic in Silver or Gold layer
- No equivalent to `MV_ResourceDevice_NoViolation` materialized view

**Impact:** Zone assignments are NOT computed dynamically in Databricks - workers may not be assigned to correct zones.

### Recommendation

**Option A: Create equivalent materialized view in Silver**

```python
# Create silver_mv_resource_device_no_violation view
# This replicates the PostgreSQL materialized view logic

resource_device_df = spark.table("wakecap_prod.silver.silver_resource_device")

mv_df = resource_device_df.filter(
    F.col("DeletedAt").isNull()  # Active assignments only
).select(
    "DeviceId",
    "WorkerId",  # ResourceId in ADF terms
    "ProjectId",
    "AssignedAt",
    "UnassignedAt"
)

mv_df.createOrReplaceTempView("mv_resource_device_no_violation")
```

**Option B: Implement spatial join using H3 library**

```python
import h3

# UDF to get H3 index for a point
@F.udf(returnType=StringType())
def point_to_h3(lat, lon, resolution=9):
    if lat is None or lon is None:
        return None
    return h3.geo_to_h3(lat, lon, resolution)

# Pre-compute H3 indexes for zones and device locations
# Then join on H3 index for efficient spatial matching
```

---

## Gap 4: MV_ResourceDevice_NoViolation Equivalent

### ADF Implementation

Before syncing DeviceLocation, ADF refreshes a PostgreSQL materialized view:

```sql
REFRESH MATERIALIZED VIEW public."MV_ResourceDevice_NoViolation";
```

### Actual MV Definition (Verified 2026-01-28)

The materialized view uses a **window function** to detect overlapping device assignments, NOT DeletedAt filtering:

```sql
SELECT t."ResourceId", t."DeviceId", t."AssignedAt", t."UnAssignedAt", t."Violation"
FROM (
    SELECT "ResourceDevice"."ResourceId", "ResourceDevice"."DeviceId",
           "ResourceDevice"."AssignedAt", "ResourceDevice"."UnAssignedAt",
           CASE WHEN (max(COALESCE("ResourceDevice"."UnAssignedAt", '2100-01-01'::timestamp))
                OVER (PARTITION BY "ResourceDevice"."DeviceId"
                      ORDER BY "ResourceDevice"."AssignedAt"
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) > "ResourceDevice"."AssignedAt")
                THEN 1 ELSE NULL::integer END AS "Violation"
    FROM "ResourceDevice"
) t
WHERE (t."Violation" IS NULL);
```

**Key Points:**
- The MV contains: `ResourceId`, `DeviceId`, `AssignedAt`, `UnAssignedAt`, `Violation`
- **NO** `DeletedAt` column - the MV does NOT filter by deletion status
- **NO** `ProjectId` column - project is not part of this view
- The "violation" is detected when a device has overlapping assignments (previous UnAssignedAt > current AssignedAt)
- Only non-violating (clean) assignments are included

### Databricks Status

**Existing implementation is sufficient.** The Silver layer `silver_resource_device` table already has:
- `DeviceId`
- `WorkerId` (equivalent to ResourceId)
- `AssignedAt`
- `UnassignedAt`

### Recommendation

Create a Databricks equivalent view/CTE using the same window function logic:

```python
def get_resource_device_no_violation(spark):
    """
    Equivalent to MV_ResourceDevice_NoViolation
    Filters out overlapping device assignments using window function
    """
    return spark.sql("""
        SELECT ResourceId, DeviceId, AssignedAt, UnAssignedAt
        FROM (
            SELECT
                WorkerId AS ResourceId,
                DeviceId,
                AssignedAt,
                UnassignedAt AS UnAssignedAt,
                CASE WHEN MAX(COALESCE(UnassignedAt, TIMESTAMP '2100-01-01'))
                     OVER (PARTITION BY DeviceId
                           ORDER BY AssignedAt
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) > AssignedAt
                     THEN 1 ELSE NULL END AS Violation
            FROM wakecap_prod.silver.silver_resource_device
        )
        WHERE Violation IS NULL
    """)
```

**Note:** The original Gap 4 "fix" that added DeletedAt/ProjectId columns was incorrect. These columns do not exist in the TimescaleDB ResourceDevice table or the MV_ResourceDevice_NoViolation view. The columns have been removed from silver_tables.yml.

---

## Gap 5: Inactive ADF Activities (Documentation Only)

The following ADF activities are marked **Inactive** and should NOT be replicated in Databricks:

| Activity | Reason | Databricks Status |
|----------|--------|-------------------|
| SyncLocationGroupAssignments | Legacy MySQL source (ExtSourceID=1) | Skip - legacy |
| SyncWorkersTasks | Legacy MySQL source (ExtSourceID=1) | Skip - legacy |
| SyncLocationAssignments_archived | ResourceZone deprecated | Skip - deprecated |
| SyncResourceAttendance | Inactive | Skip |
| SyncResourceApprovedHour | Inactive | Skip |
| SyncResourceApprovedHoursSegment | Inactive | Skip |
| CalculateFactProgress | Inactive (depends on above) | **Gold has it** - verify if needed |

**Note:** `gold_fact_progress.py` exists in Databricks. Verify if this should be active or match ADF inactive status.

---

## Action Items

### Immediate (High Priority)

1. **Verify ApprovedById in Bronze**
   - Query `timescale_resourcetimesheet` to check if `ApprovedById` is populated
   - If not, add LinkedUserId lookup to Silver layer

2. **Add Date Range Validation**
   - Add data quality expectations to Silver tables with date columns
   - Add explicit filters for EffectiveDate, Date, Day columns

### Short-term (Medium Priority)

3. **Create MV_ResourceDevice_NoViolation equivalent**
   - Add to Silver layer as a view or pre-computed table
   - Use in Gold fact tables that need device-worker resolution

4. **Implement Spatial Zone Assignment**
   - Option A: H3-based spatial indexing (recommended for scale)
   - Option B: Geometry-based ST_CONTAINS equivalent using Sedona library

### Documentation

5. **Update ADF_BRONZE_MAPPING.md**
   - Document all inactive activities
   - Mark FactProgress as "needs verification"

---

## Appendix: ADF Source Queries

### SyncDimWorker (People)

```sql
SELECT
    "Id",
    "ProjectId",
    "PeopleCode",
    "Name",
    "Address",
    "Mobile",
    "Email",
    "TitleId",
    "DepartmentId",
    "TradeId",
    "CompanyId",
    "ResourceCodeLicense",
    "CreatedAt",
    "UpdatedAt",
    CASE WHEN "JoinDate" BETWEEN '2000-01-01' AND '2100-01-01' THEN "JoinDate" ELSE NULL END AS "JoinDate",
    CASE WHEN "ReleaseDate" BETWEEN '2000-01-01' AND '2100-01-01' THEN "ReleaseDate" ELSE NULL END AS "ReleaseDate",
    "Picture",
    "HelmetColor",
    "DeletedAt",
    "LinkedUserId",
    CASE WHEN "ActivatedAt" BETWEEN '2000-01-01' AND '2100-01-01' THEN "ActivatedAt" ELSE NULL END AS "ActivatedAt",
    CASE WHEN "DeActivatedAt" BETWEEN '2000-01-01' AND '2100-01-01' THEN "DeActivatedAt" ELSE NULL END AS "DeActivatedAt",
    "Nationality"
FROM public."People"
```

### SyncDimZone

```sql
SELECT
    z."Id",
    LEFT(TRIM(z."Name"), 255) AS "Name",
    "Height",
    LEFT(TRIM("Color"), 10) AS "Color",
    ST_AsText(ST_ClipByBox2D("Coordinates", ST_MakeBox2D(ST_Point(-180, -90), ST_Point(180, 90)))) AS "Coordinates",
    "SpaceId",
    z."CreatedAt", z."UpdatedAt", z."DeletedAt",
    "ZoneCategoryId",
    LEFT(TRIM(zc."Name"), 50) AS "ZoneCategory",
    GREATEST(
        COALESCE(z."CreatedAt", '2000-01-01'),
        COALESCE(z."UpdatedAt", '2000-01-01'),
        COALESCE(zc."CreatedAt", '2000-01-01'),
        COALESCE(zc."UpdatedAt", '2000-01-01')
    ) AS "WatermarkDate"
FROM public."Zone" z
LEFT JOIN public."ZoneCategory" zc ON z."ZoneCategoryId" = zc."Id"
```

### SyncDimCrew

```sql
SELECT
    c."Id",
    c."ProjectId",
    c."Code",
    c."Name",
    c."CrewTypeId",
    ct."TypeCode" AS "CrewTypeCode",
    ct."Type" AS "CrewTypeName",
    c."DisciplineId",
    d."Name" AS "DisciplineName",
    c."CreatedAt",
    c."UpdatedAt",
    c."DeletedAt",
    GREATEST(
        COALESCE(c."CreatedAt", '2000-01-01'), COALESCE(c."UpdatedAt", '2000-01-01'), COALESCE(c."DeletedAt", '2000-01-01'),
        COALESCE(ct."CreatedAt", '2000-01-01'), COALESCE(ct."UpdatedAt", '2000-01-01'), COALESCE(ct."DeletedAt", '2000-01-01'),
        COALESCE(d."CreatedAt", '2000-01-01'), COALESCE(d."UpdatedAt", '2000-01-01')
    ) AS "WatermarkDate"
FROM public."Crew" c
LEFT JOIN public."CrewType" ct ON c."CrewTypeId" = ct."Id"
LEFT JOIN public."Discipline" d ON c."DisciplineId" = d."Id"
```

---

## Fixes Applied (2026-01-28)

### Gap 1: Date Range Filtering - FIXED

**File:** `pipelines/gold/notebooks/gold_fact_reported_attendance.py`

Added date range filtering to match ADF logic:
```python
DATE_MIN = "2000-01-01"
DATE_MAX = "2100-01-01"

# Filter ResourceHours by valid date range
rh_df = resource_hours_df \
    .filter(F.col("Date").between(DATE_MIN, DATE_MAX))

# Filter ResourceTimesheet by valid date range
rt_df = resource_timesheet_df \
    .filter(F.col("Day").between(DATE_MIN, DATE_MAX))
```

### Gap 2: LinkedUserId Lookup - FIXED

**Files Modified:**
1. `pipelines/silver/config/silver_tables.yml` - Added `LinkedUserId` column to `silver_worker`
2. `pipelines/gold/notebooks/gold_fact_reported_attendance.py` - Fixed ApprovedBy lookup

**Silver Config Change:**
```yaml
# Added to silver_worker columns:
- source: LinkedUserId
  target: LinkedUserId
  comment: "User GUID for ApprovedBy resolution"
```

**Gold Notebook Change:**
```python
# Join ApprovedById (user GUID) to LinkedUserId to resolve to WorkerId
approved_by_window = Window.partitionBy("LinkedUserId").orderBy(F.lit(1))
approved_by_lookup_df = worker_deduped \
    .filter(F.col("LinkedUserId").isNotNull()) \
    .withColumn("_rn", F.row_number().over(approved_by_window)) \
    .filter(F.col("_rn") == 1) \
    .select(
        F.col("WorkerId").alias("dim_ApprovedByWorkerID"),
        F.col("LinkedUserId").cast("string").alias("dim_ApprovedByExtID")
    )
```

### Gap 4: MV_ResourceDevice_NoViolation - CORRECTED

**Original (Incorrect) Analysis:** Assumed the MV filtered by `DeletedAt IS NULL` and needed `ProjectId`.

**Actual Behavior (Verified 2026-01-28):** The PostgreSQL MV uses a **window function** to detect overlapping device assignments:
- It computes a "Violation" flag when previous assignment's `UnAssignedAt` overlaps with current `AssignedAt`
- It filters to only non-violating (clean) assignments
- **NO** `DeletedAt` column exists in ResourceDevice table or the MV
- **NO** `ProjectId` column exists in the MV

**Correction Applied:**
- Removed invalid `DeletedAt` and `ProjectId` columns from `silver_resource_device` config
- Existing Silver table columns (DeviceId, WorkerId, AssignedAt, UnassignedAt) are sufficient
- Databricks equivalent view should use window function logic (see Gap 4 section above)

### Gap 3: DeviceLocation Spatial Joins - NOT IMPLEMENTED

This gap requires:
1. Loading raw `DeviceLocation` table (very large - real-time tracking data)
2. Implementing spatial containment using Sedona or H3 library
3. Joining to Zone coordinates for zone membership

Recommended approach for future implementation:
- Use H3 hexagonal indexing for efficient spatial joins
- Pre-compute H3 indexes for zones and device locations
- Join on H3 index for scalable spatial matching

### Gap 6: Weather Station Sensor - FIXED

**ADF Activity:** `DeltaCopyWeatherStationSensor` in SyncFacts pipeline

The ADF syncs weather station sensor data from `weather-station` TimescaleDB database:
```sql
SELECT id, network_id, project_id, generated_at, gateway_received_at,
       wind_speed, rain_fall, temperature, air_pressure, pm25, wind_direction,
       pm10, humidity, tsp, h2s, custom_sensor_4, custom_sensor_5,
       created_at, serial_no, cumulative_rain_fall, co2, so2, co
FROM public.weather_station_sensor
```

**Files Modified:**

1. `pipelines/timescaledb/config/timescaledb_tables_weather.yml`
   - Added `weather_station_sensor` table to Bronze config

2. `pipelines/silver/config/silver_tables.yml`
   - Added `silver_fact_weather_sensor` table definition

3. `pipelines/gold/notebooks/gold_fact_weather_observations.py`
   - Updated to read from Silver layer instead of SQL Server
   - Matches ADF column mapping and transformation logic

### Gap 7: Observation Dimensions - FIXED

**ADF Pipeline:** `SyncDimensionsObservations`

The ADF creates dimension tables from DISTINCT values in the Observation table:
- `ObservationSource` - DISTINCT Source values
- `ObservationStatus` - DISTINCT Status values
- `ObservationType` - DISTINCT Type values
- `ObservationSeverity` - DISTINCT Severity values
- `ObservationDiscriminator` - DISTINCT Discriminator values
- `ObservationClinicViolationStatus` - DISTINCT ClinicViolationStatus values

**Files Modified:**

1. `pipelines/silver/config/silver_tables.yml`
   - Added 6 observation dimension table definitions

2. `pipelines/gold/notebooks/create_observation_dimensions.py`
   - Updated to read from Bronze `observation_observation` table
   - Creates dimension tables matching ADF pattern:
     ```sql
     SELECT DISTINCT LEFT(TRIM("Column"), 50) AS "ObservationXxx", 19 AS "ExtSourceID"
     FROM public."Observation"
     WHERE "Column" IS NOT NULL
     ```

---

*Generated by ADF ARM Template analysis - 2026-01-28*
*Updated with fixes applied - 2026-01-28*
*Updated with weather and observation fixes - 2026-01-28*
