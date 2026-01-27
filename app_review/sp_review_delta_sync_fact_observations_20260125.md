# Stored Procedure Review: stg.spDeltaSyncFactObservations

## Review Summary

| Metric | Value |
|--------|-------|
| **Source SP** | `stg.spDeltaSyncFactObservations` (585 lines T-SQL) |
| **Converted Notebook** | `delta_sync_fact_observations.py` (1296 lines PySpark) |
| **Patterns Converted** | 7/7 |
| **Dimension Tables** | 12/12 |
| **Column Mappings** | 40+ columns |
| **Review Date** | 2026-01-25 |
| **Verdict** | **PASS** |

---

## Pattern Conversion Analysis

| Pattern | Source (T-SQL) | Converted (PySpark) | Status |
|---------|----------------|---------------------|--------|
| **TEMP_TABLE** | `#batch` temp table | DataFrame with `cache()` | CORRECT |
| **MERGE INTO** | SQL MERGE | SQL MERGE via `spark.sql()` | CORRECT |
| **ROW_NUMBER()** | `ROW_NUMBER() OVER (PARTITION BY... ORDER BY (SELECT NULL))` | `Window.partitionBy().orderBy(F.lit(1))` | CORRECT |
| **fnExtSourceIDAlias()** | Custom function grouping 14-19->15, 1-12->2 | Inline CASE expression (lines 159-171) | CORRECT |
| **Stalled Records Phase 1** | INSERT recovered stalled back to source | Separate DataFrame joins with recovery logic | CORRECT |
| **Stalled Records Phase 2** | MERGE into stalled table for new failures | MERGE into stalled table | CORRECT |
| **CTE Performance Filter** | CTE with DISTINCT dimension values | Not directly converted (performance optimization) | ACCEPTABLE |

---

## Dimension Table Joins Analysis

All 12 dimension lookups are present with correct join types:

| # | Dimension | Join Type | Source Match Column | Status |
|---|-----------|-----------|---------------------|--------|
| 1 | Project | INNER | ExtProjectID | CORRECT |
| 2 | ObservationDiscriminator | INNER | ObservationDiscriminator | CORRECT |
| 3 | ObservationSource | INNER | ObservationSource | CORRECT |
| 4 | ObservationType | INNER | ObservationType | CORRECT |
| 5 | ObservationSeverity | LEFT | ObservationSeverity | CORRECT |
| 6 | ObservationStatus | LEFT | ObservationStatus | CORRECT |
| 7 | ObservationClinicViolationStatus | LEFT | ObservationClinicViolationStatus | CORRECT |
| 8 | Company/Organization | LEFT | ExtCompanyID | CORRECT |
| 9 | Floor | LEFT | ExtSpaceID | CORRECT |
| 10 | Zone | LEFT | ExtZoneID | CORRECT |
| 11 | Worker | LEFT | ExtWorkerID (ResourceId) | CORRECT |
| 12 | Device | LEFT | ExtDeviceID | CORRECT |

---

## Column Mapping Verification

| Source Column | Target Column | Special Handling | Status |
|---------------|---------------|------------------|--------|
| GeneratedAt | TimestampUTC | Direct mapping | CORRECT |
| ProjectID2 | ProjectID | Alias from dimension | CORRECT |
| Id | ExtObservationID | Business key | CORRECT |
| Inaccurate | IsInaccurate | Rename | CORRECT |
| FalseAlarm | IsFalseAlarm | Rename | CORRECT |
| ViewedAt | ViewedAtUTC | **ViewedAt2 computed logic** | CORRECT |
| OpenedAt | OpenedAtUTC | Direct | CORRECT |
| ClosedAt | ClosedAtUTC | Direct | CORRECT |
| ZoneEntryTime | ZoneEntryTimeUTC | Direct | CORRECT |
| ZoneExitTime | ZoneExitTimeUTC | Direct | CORRECT |
| SerialNo | SerialNumber | Rename | CORRECT |
| MediaUrl | MediaURL | Rename | CORRECT |
| PackageId | ExtPackageID | Rename | CORRECT |
| ParentId | ExtParentID | Rename | CORRECT |
| Threshold | Threshold | Float tolerance | CORRECT |
| Speed | Speed | Float tolerance | CORRECT |
| SpeedLimit | SpeedLimit | Float tolerance | CORRECT |
| DurationExceeded | DurationExceeded | Float tolerance | CORRECT |

**ViewedAt2 Computed Column:**
- T-SQL: `CASE WHEN ViewedAt IS NULL AND Viewed = 1 THEN '1900-01-01' ELSE ViewedAt END`
- PySpark: `F.when(F.col("ViewedAt").isNull() & (F.col("Viewed") == True), F.to_timestamp(F.lit("1900-01-01"))).otherwise(F.col("ViewedAt"))`
- Status: CORRECT

---

## Float Comparison Tolerance Analysis

| Float Column | Tolerance | Status |
|--------------|-----------|--------|
| Threshold | 0.00001 | CORRECT |
| Speed | 0.00001 | CORRECT |
| SpeedLimit | 0.00001 | CORRECT |
| DurationExceeded | 0.00001 | CORRECT |

All float columns use proper NULL handling with tolerance-based comparison.

---

## Business Logic Comparison

### fnExtSourceIDAlias Function

- Groups ExtSourceID 14-19 to alias 15
- Groups ExtSourceID 1,2,11,12 to alias 2
- Otherwise returns original value
- Status: CORRECTLY CONVERTED

### Resolution Filter for LEFT Joins

Pattern ensuring records are only processed when dimension lookup succeeds OR source value was NULL.
- Status: CORRECTLY IMPLEMENTED

### MERGE Condition

Match on ExtObservationID AND ExtSourceIDAlias with proper aliasing logic.
- Status: CORRECT

---

## Issues Found

### HIGH RISK Issues (1)

#### 1. Missing CTE Performance Optimization
- **Location**: T-SQL lines 361-382
- **Description**: The original SP uses a CTE to filter the target table by joining with distinct values from batch to improve MERGE performance
- **Impact**: The converted notebook does not pre-filter the target table, which may result in slower MERGE performance on large tables
- **Recommendation**: Consider adding partition pruning hints or restructuring the MERGE to leverage Delta's data skipping

### MEDIUM RISK Issues (1)

#### 2. Hardcoded Catalog/Schema Names
- **Location**: Lines 37-61
- **Description**: Catalog and schema names are hardcoded as `wakecap_prod`
- **Impact**: Reduces portability across environments (dev/test/prod)
- **Recommendation**: Consider using widgets or configuration files for environment-specific settings

### LOW RISK Issues (2)

#### 3. count() Operations on Dimension Tables
- **Location**: Lines 284, 293, 302, 311, etc.
- **Description**: Multiple `count()` operations on dimension tables for logging purposes
- **Impact**: Minor performance overhead during execution
- **Recommendation**: Consider removing or making optional via debug flag

#### 4. Verbose Logging
- **Location**: Throughout notebook
- **Description**: Extensive print statements may clutter logs in production
- **Impact**: Minor - no functional impact
- **Recommendation**: Consider using proper logging levels

---

## Stalled Record Handling

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 - Recovery | Recovers previously stalled records where dimensions are now available | CORRECTLY IMPLEMENTED |
| Phase 2 - Store New | Identifies records that failed dimension lookup, MERGE into stalled table | CORRECTLY IMPLEMENTED |

---

## Enhanced Features (Not in Original)

| Feature | Status |
|---------|--------|
| Pre-flight checks | ENHANCED |
| Watermark tracking | ENHANCED |
| Incremental loading widget | ENHANCED |
| Batch size limit widget | ENHANCED |
| Project filter widget | ENHANCED |
| Validation section | ENHANCED |

---

## Summary of Issues by Risk Tier

| Risk Level | Count | Description |
|------------|-------|-------------|
| **BLOCKER** | 0 | None |
| **HIGH RISK** | 1 | Missing CTE performance optimization |
| **MEDIUM RISK** | 1 | Hardcoded catalog/schema names |
| **LOW RISK** | 2 | count() overhead, verbose logging |

---

## Verdict: **PASS**

The conversion is functionally complete and correct. All critical patterns have been properly converted:

- All 12 dimension table joins with correct INNER/LEFT types
- fnExtSourceIDAlias function correctly inlined
- ViewedAt2 computed column correctly implemented
- Float tolerance comparisons preserved (0.00001)
- Stalled record handling (recovery and storage) implemented
- MERGE with full change detection logic preserved
- ROW_NUMBER deduplication pattern converted to Window functions

The HIGH RISK issue regarding missing CTE performance optimization is a performance concern, not a correctness issue. The notebook may run slower on large datasets but will produce correct results.

---

## Recommendations

1. **Performance**: Add partition pruning or data skipping hints to improve MERGE performance
2. **Portability**: Externalize catalog/schema configuration for environment portability
3. **Production**: Consider adding a debug mode to reduce logging overhead

---

**Review completed by:** ADW Review Subagent
**Date:** 2026-01-25
