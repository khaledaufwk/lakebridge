# Implementation Plan: stg.spDeltaSyncFactObservations Migration

## SP Analysis Summary

### Source Stored Procedure
- **File**: `migration_project/source_sql/stored_procedures/stg.spDeltaSyncFactObservations.sql`
- **Line Count**: 585 lines
- **Complexity**: HIGH
- **Purpose**: Delta sync from TimescaleDB wc2023_observation_Observations to dbo.FactObservations with dimension lookups and stalled record handling

### Detected Patterns and Complexity

| Pattern | Description | Conversion Approach |
|---------|-------------|---------------------|
| **TEMP_TABLE** | Uses `#batch` temp table for staging resolved records | Use Spark DataFrame as staging |
| **MERGE** | Main MERGE INTO dbo.FactObservations with complex UPDATE/INSERT | Use `DeltaTable.merge()` with condition builder |
| **CTE** | Uses CTE for performance filtering on existing fact table | Use DataFrame filtering with broadcast joins |
| **WINDOW_FUNCTION** | ROW_NUMBER() OVER (PARTITION BY...) for deduplication | Use PySpark Window functions |
| **CUSTOM_FUNCTION** | `stg.fnExtSourceIDAlias()` for source grouping | Inline Python/SQL CASE expression |
| **STALLED_RECORDS** | Two-phase stalled record handling (recover + store new) | Implement as separate DataFrames with merge logic |
| **MULTI_DIMENSION_LOOKUP** | 12 dimension table joins with INNER/LEFT logic | Sequential DataFrame joins with broadcast |

### Input Tables (14 total)

| SQL Server Table | Databricks Table | Join Type | Purpose |
|------------------|------------------|-----------|---------|
| `stg.wc2023_observation_Observations` | `wakecap_prod.raw.timescale_observations` | Source | Raw observation data from TimescaleDB |
| `stg.wc2023_observation_Observations_stalled` | `wakecap_prod.silver.silver_observations_stalled` | Source | Previously stalled records |
| `dbo.Project` | `wakecap_prod.silver.silver_project` | INNER | ProjectID resolution |
| `dbo.ObservationDiscriminator` | `wakecap_prod.silver.silver_observation_discriminator` | INNER | Discriminator lookup |
| `dbo.ObservationSource` | `wakecap_prod.silver.silver_observation_source` | INNER | Source lookup |
| `dbo.ObservationType` | `wakecap_prod.silver.silver_observation_type` | INNER | Type lookup |
| `dbo.ObservationSeverity` | `wakecap_prod.silver.silver_observation_severity` | LEFT | Severity lookup (nullable) |
| `dbo.ObservationStatus` | `wakecap_prod.silver.silver_observation_status` | LEFT | Status lookup (nullable) |
| `dbo.ObservationClinicViolationStatus` | `wakecap_prod.silver.silver_observation_clinic_violation_status` | LEFT | Clinic violation status (nullable) |
| `dbo.Company` | `wakecap_prod.silver.silver_organization` | LEFT | Company/Organization lookup |
| `dbo.Floor` | `wakecap_prod.silver.silver_floor` | LEFT | Floor/Space lookup |
| `dbo.Zone` | `wakecap_prod.silver.silver_zone` | LEFT | Zone lookup |
| `dbo.Worker` | `wakecap_prod.silver.silver_worker` | LEFT | Worker/Resource lookup |
| `dbo.Device` | `wakecap_prod.silver.silver_device` | LEFT | Device lookup |

### Output Tables (2)

| SQL Server Table | Databricks Table | Operation |
|------------------|------------------|-----------|
| `dbo.FactObservations` | `wakecap_prod.gold.fact_observations` | MERGE (upsert) |
| `stg.wc2023_observation_Observations_stalled` | `wakecap_prod.silver.silver_observations_stalled` | MERGE (stalled records) |

### Function Dependencies

| Function | Purpose | Conversion |
|----------|---------|------------|
| `stg.fnExtSourceIDAlias(@ExtSourceID)` | Groups source IDs: (14-19)->15, (1,2,11,12)->2, else original | Inline Python/SQL CASE expression |

---

## Target Table Schema: dbo.FactObservations

```sql
CREATE TABLE dbo.FactObservations (
    ObservationID int IDENTITY(1,1) NOT NULL,  -- Auto-generated, not in source
    TimestampUTC datetime NOT NULL,            -- From GeneratedAt
    ProjectID int NOT NULL,                    -- Dimension lookup
    ObservationDiscriminatorID int NOT NULL,   -- Dimension lookup
    ObservationSourceID int NOT NULL,          -- Dimension lookup
    ObservationTypeID int NOT NULL,            -- Dimension lookup
    ObservationSeverityID int NULL,            -- Dimension lookup (nullable)
    ObservationStatusID int NULL,              -- Dimension lookup (nullable)
    ObservationClinicViolationStatusID int NULL, -- Dimension lookup (nullable)
    PackageID int NULL,                        -- Not used in source
    CompanyID int NULL,                        -- Dimension lookup
    FloorID int NULL,                          -- Dimension lookup
    ZoneID int NULL,                           -- Dimension lookup
    WorkerID int NULL,                         -- Dimension lookup
    DeviceID int NULL,                         -- Dimension lookup
    IsInaccurate bit NULL,                     -- From Inaccurate
    IsFalseAlarm bit NULL,                     -- From FalseAlarm
    Description nvarchar(1000) NULL,
    CameraName nvarchar(100) NULL,
    SerialNumber nvarchar(100) NULL,
    PermitNumber nvarchar(100) NULL,
    StopWorkNotice bit NULL,
    RemainingLeaveDays int NULL,
    Threshold float NULL,
    Latitude float NULL,
    Longitude float NULL,
    PlateNumber nvarchar(100) NULL,
    Speed float NULL,
    SpeedLimit float NULL,
    ZoneEntryTimeUTC datetime NULL,
    ZoneExitTimeUTC datetime NULL,
    DurationExceeded float NULL,
    OpenedAtUTC datetime NULL,
    ViewedAtUTC datetime NULL,                 -- Computed: CASE WHEN ViewedAt IS NULL AND Viewed = 1 THEN '1900-01-01' ELSE ViewedAt END
    ClosedAtUTC datetime NULL,
    MediaURL nvarchar(2048) NULL,
    CreatedAt datetime NULL,
    UpdatedAt datetime NULL,
    DeletedAt datetime NULL,
    ExtObservationID int NOT NULL,             -- From source Id (business key)
    ExtPackageID int NULL,
    ExtParentID int NULL,
    WatermarkUTC datetime NULL,                -- Auto-set on update
    ExtSourceID int NOT NULL                   -- Constant: 19
);
-- Primary Key: (ExtObservationID, ExtSourceID)
```

---

## Conversion Approach

### Recommended: Python Notebook with DataFrame Operations

Due to the complexity (TEMP_TABLE + complex MERGE + 12 dimension lookups + stalled record handling), a Python notebook is recommended over SQL-only approach.

**Key Design Decisions:**
1. **Staging DataFrame** instead of temp table
2. **DeltaTable.merge()** for the main MERGE operation
3. **Broadcast joins** for dimension lookups (small dimension tables)
4. **Window functions** for ROW_NUMBER deduplication
5. **Inline function** for fnExtSourceIDAlias (simple CASE expression)
6. **Three-phase processing**: Recover stalled -> Process new -> Store new stalled

---

## Step-by-Step Conversion Tasks

### Task 1: Create Gold Schema and Stalled Records Table

Create necessary schemas and the stalled records tracking table.

### Task 2: Create fnExtSourceIDAlias Helper Function

Implement as inline SQL CASE expression:
```python
def add_ext_source_alias(df, source_col="ExtSourceID", target_col="ExtSourceIDAlias"):
    return df.withColumn(
        target_col,
        F.when(F.col(source_col).isin(14, 15, 16, 17, 18, 19), F.lit(15))
         .when(F.col(source_col).isin(1, 2, 11, 12), F.lit(2))
         .otherwise(F.col(source_col))
    )
```

### Task 3: Load Dimension Tables with Deduplication

Load all 12 dimension tables with ROW_NUMBER deduplication pattern:
- Project (INNER)
- ObservationDiscriminator (INNER)
- ObservationSource (INNER)
- ObservationType (INNER)
- ObservationSeverity (LEFT)
- ObservationStatus (LEFT)
- ObservationClinicViolationStatus (LEFT)
- Company (LEFT)
- Floor (LEFT)
- Zone (LEFT)
- Worker (LEFT)
- Device (LEFT)

### Task 4: Phase 1 - Recover Previously Stalled Records

Re-process stalled records where all required dimensions can now be resolved.

### Task 5: Phase 2 - Build Staging DataFrame

Create staging DataFrame with all dimension lookups (equivalent to `#batch` temp table).

### Task 6: Phase 3 - MERGE into FactObservations

Implement DeltaTable.merge() with:
- Match condition on ExtObservationID and ExtSourceIDAlias
- Update condition checking all 28+ columns with float tolerance
- Insert for new records

### Task 7: Phase 4 - Handle Stalled Records

- Delete recovered stalled records that were successfully processed
- Insert new stalled records that couldn't be resolved

### Task 8: Add Watermark Tracking

Update watermark table after successful processing.

---

## Expected Output File

**Path:** `migration_project/pipelines/gold/notebooks/delta_sync_fact_observations.py`

---

## Validation Commands

### Row Count Comparison
```sql
-- Source observation count
SELECT COUNT(*) as source_count
FROM wakecap_prod.raw.timescale_observations;

-- Target fact count
SELECT COUNT(*) as fact_count
FROM wakecap_prod.gold.fact_observations
WHERE ExtSourceID = 19;

-- Stalled count
SELECT COUNT(*) as stalled_count
FROM wakecap_prod.silver.silver_observations_stalled;

-- Sum should equal source
SELECT
    (SELECT COUNT(*) FROM wakecap_prod.gold.fact_observations WHERE ExtSourceID = 19) +
    (SELECT COUNT(*) FROM wakecap_prod.silver.silver_observations_stalled) as total,
    (SELECT COUNT(*) FROM wakecap_prod.raw.timescale_observations) as source;
```

### Data Quality Checks
```sql
-- Check required dimension lookups succeeded (no nulls in INNER join columns)
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN ProjectID IS NULL THEN 1 ELSE 0 END) as null_project,
    SUM(CASE WHEN ObservationDiscriminatorID IS NULL THEN 1 ELSE 0 END) as null_discriminator,
    SUM(CASE WHEN ObservationSourceID IS NULL THEN 1 ELSE 0 END) as null_source,
    SUM(CASE WHEN ObservationTypeID IS NULL THEN 1 ELSE 0 END) as null_type
FROM wakecap_prod.gold.fact_observations
WHERE ExtSourceID = 19;
```

---

## Acceptance Criteria

| Criteria | Target | Validation Query |
|----------|--------|------------------|
| Row count (fact + stalled) | = Source count | Sum comparison |
| No NULL in required dimensions | 0 nulls | Data quality check |
| Duplicate check | 0 duplicates | `COUNT(DISTINCT ExtObservationID) = COUNT(*)` |
| Watermark updated | After each run | Check watermark table |

---

## Dependencies

### Required Tables (Must Exist)
- `wakecap_prod.raw.timescale_observations` (Bronze layer - source)
- `wakecap_prod.silver.silver_project`
- `wakecap_prod.silver.silver_observation_discriminator`
- `wakecap_prod.silver.silver_observation_source`
- `wakecap_prod.silver.silver_observation_type`
- `wakecap_prod.silver.silver_observation_severity`
- `wakecap_prod.silver.silver_observation_status`
- `wakecap_prod.silver.silver_observation_clinic_violation_status`
- `wakecap_prod.silver.silver_organization`
- `wakecap_prod.silver.silver_floor`
- `wakecap_prod.silver.silver_zone`
- `wakecap_prod.silver.silver_worker`
- `wakecap_prod.silver.silver_device`

---

## Notes

### Key Conversion Challenges

1. **fnExtSourceIDAlias Function**: Groups source IDs for cross-system matching. Critical for MERGE condition.

2. **Stalled Record Pattern**: SQL Server uses INSERT back to source table; Databricks uses separate stalled table with MERGE.

3. **Change Detection**: Original has explicit column-by-column change detection with float tolerance (0.00001). Must preserve.

4. **ViewedAt2 Computed Column**: Special handling for ViewedAt when null but Viewed=1 (use epoch date '1900-01-01').
