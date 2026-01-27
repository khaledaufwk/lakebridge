# TimescaleDB Bronze Layer Performance Analysis Report

**Generated:** 2026-01-26
**Database:** wakecap_app (TimescaleDB Read Replica)
**Target:** Databricks Unity Catalog (wakecap_prod.raw)

---

## Executive Summary

This analysis compares the current Bronze layer implementation plan against the actual TimescaleDB database structure, identifies performance bottlenecks, and provides actionable recommendations to improve incremental load performance.

### Key Findings

| Category | Status | Impact |
|----------|--------|--------|
| **Missing Watermark Indexes** | CRITICAL | 6 large tables (80M+ rows) lack indexes on watermark columns, causing full table scans |
| **PK Configuration Mismatch** | HIGH | 2 tables have incorrect PK definitions in YAML |
| **Hypertable Compression** | N/A | Disabled on read replica (expected) |
| **Index Optimization** | MEDIUM | Potential for composite indexes on GREATEST expressions |

### Current Job Performance

| Run | Duration | Status | Notes |
|-----|----------|--------|-------|
| 2026-01-26 17:38 | 51.6 min | SUCCESS | 65/76 tables, 77.7M rows |
| 2026-01-25 07:53 | 65.3 min | SUCCESS | Full incremental |
| 2026-01-23 14:56 | 169.8 min | SUCCESS | Likely included full loads |

---

## Database Overview

| Metric | Value |
|--------|-------|
| **Total Tables** | 142 |
| **Database Size** | 143 GB |
| **Hypertables** | 2 (DeviceLocation, DeviceLocationSummary) |
| **Materialized Views** | 18 |
| **Total Indexes** | 409 |
| **Tables in Bronze Config** | 76 |

### Top 10 Tables by Size (Excluding MVs/Backups)

| Table | Size | Est. Rows | Watermark Index |
|-------|------|-----------|-----------------|
| AuditTrail | 25 GB | 600K | **EXCLUDED** (audit log) |
| SGSRosterWorkshiftLog | 17 GB | 71M | **MISSING** |
| ViewFactWorkshiftsCache | 2.7 GB | 9.5M | **MISSING** |
| MobileSyncRejectedActions | 1.3 GB | 6K | **EXCLUDED** |
| WorkshiftResourceAssignment | 938 MB | 3.4M | YES |
| ResourceHours | 865 MB | 3.3M | **MISSING** |
| EquipmentTelemetry | 659 MB | 3.0M | **MISSING** |
| ResourceTimesheet | 483 MB | 2.7M | **MISSING** |
| ResourceAttendance | 440 MB | 1.4M | **MISSING** |
| ManualLocationAssignment | 220 MB | 1.5M | **MISSING** |

---

## Critical Issue #1: Missing Watermark Indexes

**Impact:** Full table scans on incremental loads, causing 10-100x slower query performance.

### Large Tables Missing Watermark Indexes

| Table | Rows | Size | Watermark Expression | Index Status |
|-------|------|------|---------------------|--------------|
| **SGSRosterWorkshiftLog** | 70.9M | 17 GB | GREATEST(CreatedAt, UpdatedAt) | **NO INDEX** |
| **ViewFactWorkshiftsCache** | 9.5M | 2.7 GB | WatermarkUTC | **NO INDEX** |
| **ResourceHours** | 3.3M | 865 MB | GREATEST(CreatedAt, UpdatedAt) | **NO INDEX** |
| **EquipmentTelemetry** | 3.0M | 659 MB | CreatedAt | **NO INDEX** |
| **ResourceTimesheet** | 2.7M | 483 MB | GREATEST(CreatedAt, UpdatedAt) | **NO INDEX** |
| **ManualLocationAssignment** | 1.5M | 220 MB | GREATEST(CreatedAt, UpdatedAt) | **NO INDEX** |
| **ResourceAttendance** | 1.4M | 440 MB | GREATEST(CreatedAt, UpdatedAt) | **NO INDEX** |

### Tables WITH Watermark Indexes (Good)

| Table | Rows | Indexed Columns |
|-------|------|-----------------|
| WorkshiftResourceAssignment | 3.4M | CreatedAt, UpdatedAt (separate indexes) |
| DeviceLocation | 93.8M | GeneratedAt, CreatedAt |
| DeviceLocationSummary | 922K | GeneratedAt |
| AuditTrail | 600K | CreatedAt |
| SGSIntegrationLog | 237K | CreatedAt |

### Recommended Index Creation (On Read Replica)

```sql
-- PRIORITY 1: SGSRosterWorkshiftLog (71M rows, 17GB)
-- This is the LARGEST active table - index will have MASSIVE impact
CREATE INDEX CONCURRENTLY IX_SGSRosterWorkshiftLog_UpdatedAt
ON "SGSRosterWorkshiftLog" ("UpdatedAt");

CREATE INDEX CONCURRENTLY IX_SGSRosterWorkshiftLog_CreatedAt
ON "SGSRosterWorkshiftLog" ("CreatedAt");

-- PRIORITY 2: ViewFactWorkshiftsCache (9.5M rows)
CREATE INDEX CONCURRENTLY IX_ViewFactWorkshiftsCache_WatermarkUTC
ON "ViewFactWorkshiftsCache" ("WatermarkUTC");

-- PRIORITY 3: ResourceHours (3.3M rows)
CREATE INDEX CONCURRENTLY IX_ResourceHours_UpdatedAt
ON "ResourceHours" ("UpdatedAt");

CREATE INDEX CONCURRENTLY IX_ResourceHours_CreatedAt
ON "ResourceHours" ("CreatedAt");

-- PRIORITY 4: EquipmentTelemetry (3.0M rows)
CREATE INDEX CONCURRENTLY IX_EquipmentTelemetry_CreatedAt
ON "EquipmentTelemetry" ("CreatedAt");

-- PRIORITY 5: ResourceTimesheet (2.7M rows)
CREATE INDEX CONCURRENTLY IX_ResourceTimesheet_UpdatedAt
ON "ResourceTimesheet" ("UpdatedAt");

-- PRIORITY 6: ManualLocationAssignment (1.5M rows)
CREATE INDEX CONCURRENTLY IX_ManualLocationAssignment_UpdatedAt
ON "ManualLocationAssignment" ("UpdatedAt");

-- PRIORITY 7: ResourceAttendance (1.4M rows)
CREATE INDEX CONCURRENTLY IX_ResourceAttendance_UpdatedAt
ON "ResourceAttendance" ("UpdatedAt");
```

**Note:** Use `CREATE INDEX CONCURRENTLY` to avoid blocking reads on the read replica.

### Estimated Performance Improvement

| Table | Current Load Time | Est. With Index | Improvement |
|-------|-------------------|-----------------|-------------|
| SGSRosterWorkshiftLog | ~20-30 min | ~2-5 min | **5-10x** |
| ViewFactWorkshiftsCache | ~5-10 min | ~1-2 min | **3-5x** |
| ResourceHours | ~3-5 min | ~30s-1 min | **3-5x** |
| EquipmentTelemetry | ~3-5 min | ~30s-1 min | **3-5x** |
| **Total Job** | ~52-65 min | ~15-25 min | **2-3x** |

---

## Critical Issue #2: Primary Key Configuration Mismatches

### EquipmentTelemetry - INCORRECT PK

**Current YAML Configuration:**
```yaml
- source_table: EquipmentTelemetry
  primary_key_columns: [Id]  # WRONG!
```

**Actual Database PK:**
```
(EquipmentId, Id)  # Composite key!
```

**Impact:** MERGE operations may fail or produce incorrect results due to non-unique key matching.

**Fix:**
```yaml
- source_table: EquipmentTelemetry
  primary_key_columns: [EquipmentId, Id]  # Correct composite PK
  watermark_column: CreatedAt
  category: facts
  fetch_size: 100000
  batch_size: 500000
  is_hypertable: true
  is_append_only: true
```

### ViewFactWorkshiftsCache - PK Column Order

**Current YAML Configuration:**
```yaml
primary_key_columns: [ExtWorkerID, ShiftLocalDate, ProjectId]
```

**Actual Database Unique Index:**
```
(ProjectId, ExtWorkerID, ShiftLocalDate)
```

**Impact:** Low - column order doesn't affect uniqueness, but may affect query plan selection.

**Recommendation:** Match database order for consistency:
```yaml
primary_key_columns: [ProjectId, ExtWorkerID, ShiftLocalDate]
```

---

## Issue #3: Hypertable Configuration

### Current Status

| Hypertable | Rows | Size | Compression | Watermark Index |
|------------|------|------|-------------|-----------------|
| DeviceLocation | 93.8M | N/A | **DISABLED** | YES (GeneratedAt) |
| DeviceLocationSummary | 922K | N/A | **DISABLED** | YES (GeneratedAt) |

### Analysis

The hypertables have compression **disabled** on the read replica, which is expected since:
1. Compression requires write access to recompress chunks
2. Read replicas typically don't support compression operations

**Current Configuration is CORRECT:**
- DeviceLocation uses `is_append_only: true` (no MERGE needed)
- GeneratedAt watermark with existing index
- Large batch sizes (500K) for efficient loading

---

## Issue #4: GREATEST Expression Optimization

### Current Approach

Many tables use `GREATEST(COALESCE("CreatedAt", '1900-01-01'), COALESCE("UpdatedAt", '1900-01-01'))` for watermarking.

### Problem

PostgreSQL cannot efficiently use separate indexes on CreatedAt and UpdatedAt when evaluating a GREATEST expression. This requires a **full table scan** even if individual column indexes exist.

### Solution Options

**Option A: Expression Index (Recommended)**
```sql
-- Create an expression index for the GREATEST function
CREATE INDEX CONCURRENTLY IX_ResourceHours_WatermarkExpr
ON "ResourceHours" (GREATEST(COALESCE("CreatedAt", '1900-01-01'::timestamp),
                             COALESCE("UpdatedAt", '1900-01-01'::timestamp)));
```

**Option B: Generated Column + Index**
```sql
-- Add a generated column (requires PostgreSQL 12+)
ALTER TABLE "ResourceHours"
ADD COLUMN _watermark timestamp GENERATED ALWAYS AS
  (GREATEST(COALESCE("CreatedAt", '1900-01-01'), COALESCE("UpdatedAt", '1900-01-01'))) STORED;

CREATE INDEX CONCURRENTLY IX_ResourceHours_Watermark ON "ResourceHours" ("_watermark");
```

**Option C: Simplify Watermark (If Business Logic Allows)**

If `UpdatedAt` is always populated when a row changes, use a simpler query:
```yaml
# Instead of:
watermark_expression: "GREATEST(COALESCE(\"CreatedAt\", '1900-01-01'), COALESCE(\"UpdatedAt\", '1900-01-01'))"

# Use:
watermark_column: UpdatedAt
# With index: CREATE INDEX ON table (UpdatedAt)
```

---

## Configuration vs Database Comparison Summary

### Tables Configured Correctly

| Table | PK Match | Watermark | Index | Status |
|-------|----------|-----------|-------|--------|
| Activity | YES | UpdatedAt | NO | Need Index |
| Company | YES | UpdatedAt | NO | Need Index |
| DeviceLocation | YES | GeneratedAt | YES | GOOD |
| DeviceLocationSummary | YES | GeneratedAt | YES | GOOD |
| WorkshiftResourceAssignment | YES | UpdatedAt | YES | GOOD |
| People | YES | UpdatedAt | NO | Need Index |

### Tables Needing Configuration Fixes

| Table | Issue | Fix Required |
|-------|-------|--------------|
| EquipmentTelemetry | PK should be [EquipmentId, Id] | Update YAML |
| ViewFactWorkshiftsCache | PK order should be [ProjectId, ExtWorkerID, ShiftLocalDate] | Minor - optional |

---

## Recommendations Summary

### Immediate Actions (High Impact)

1. **Create Watermark Indexes on Read Replica**
   - Priority: SGSRosterWorkshiftLog, ViewFactWorkshiftsCache, ResourceHours
   - Expected improvement: 2-3x faster job execution
   - SQL provided above

2. **Fix EquipmentTelemetry PK Configuration**
   ```yaml
   primary_key_columns: [EquipmentId, Id]
   ```

### Medium-Term Actions

3. **Consider Expression Indexes for GREATEST Watermarks**
   - Create expression indexes for frequently-loaded tables
   - Test performance improvement before full rollout

4. **Monitor Job Performance**
   - Track per-table load times in watermark table
   - Identify remaining bottlenecks after index creation

### Long-Term Considerations

5. **Evaluate Append-Only for More Tables**
   - Tables with only InsertAt patterns can skip MERGE
   - Current append-only tables: DeviceLocation, DeviceLocationSummary, EquipmentTelemetry, Inspection, ViewFactWorkshiftsCache

6. **Consider Parallel Loading**
   - Current: 3 parallel tasks (wakecap_app, observation, weather)
   - Potential: Further parallelize large tables within wakecap_app task

---

## Index Creation Script

Execute on TimescaleDB read replica (or primary with replication to replica):

```sql
-- ============================================
-- BRONZE LAYER PERFORMANCE OPTIMIZATION INDEXES
-- Execute with CONCURRENTLY to avoid blocking
-- ============================================

-- Priority 1: SGSRosterWorkshiftLog (71M rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_SGSRosterWorkshiftLog_UpdatedAt
ON public."SGSRosterWorkshiftLog" ("UpdatedAt");

-- Priority 2: ViewFactWorkshiftsCache (9.5M rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_ViewFactWorkshiftsCache_WatermarkUTC
ON public."ViewFactWorkshiftsCache" ("WatermarkUTC");

-- Priority 3: ResourceHours (3.3M rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_ResourceHours_UpdatedAt
ON public."ResourceHours" ("UpdatedAt");

-- Priority 4: EquipmentTelemetry (3.0M rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_EquipmentTelemetry_CreatedAt
ON public."EquipmentTelemetry" ("CreatedAt");

-- Priority 5: ResourceTimesheet (2.7M rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_ResourceTimesheet_UpdatedAt
ON public."ResourceTimesheet" ("UpdatedAt");

-- Priority 6: ManualLocationAssignment (1.5M rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_ManualLocationAssignment_UpdatedAt
ON public."ManualLocationAssignment" ("UpdatedAt");

-- Priority 7: ResourceAttendance (1.4M rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_ResourceAttendance_UpdatedAt
ON public."ResourceAttendance" ("UpdatedAt");

-- Priority 8: TrainingSessionTrainee (337K rows)
CREATE INDEX CONCURRENTLY IF NOT EXISTS IX_TrainingSessionTrainee_UpdatedAt
ON public."TrainingSessionTrainee" ("UpdatedAt");

-- Verify indexes were created
SELECT
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_indexes
JOIN pg_class ON pg_class.relname = indexname
WHERE indexname LIKE 'IX_%UpdatedAt'
   OR indexname LIKE 'IX_%CreatedAt'
   OR indexname LIKE 'IX_%WatermarkUTC'
ORDER BY tablename;
```

---

## Appendix: Full Missing Index List

252 watermark columns across all tables are missing indexes. The full list is available in the analysis output. Priority should be given to:

1. Tables > 1M rows
2. Tables loaded incrementally (not is_full_load: true)
3. Tables in the active Bronze configuration

---

## Appendix B: Excluded Tables (Implemented 2026-01-26)

The following tables have been excluded from the Bronze layer to improve job performance:

| Table | Rows | Size | Reason | Est. Time Saved |
|-------|------|------|--------|-----------------|
| SGSRosterWorkshiftLog | 71M | 17 GB | No Gold layer dependencies, external SGS data | ~25 min |
| ViewFactWorkshiftsCache | 9.5M | 2.7 GB | Pre-computed cache, redundant with Gold | ~5 min |

**Total estimated savings: ~30 min per job run**

### Files Updated

1. `pipelines/timescaledb/config/timescaledb_tables_v2.yml`
   - Added `enabled: false` to SGSRosterWorkshiftLog
   - Added `enabled: false` to ViewFactWorkshiftsCache
   - Updated excluded_tables documentation

2. `pipelines/silver/config/silver_tables.yml`
   - Added `enabled: false` to silver_fact_sgs_roster
   - Added `enabled: false` to silver_fact_workshifts_cache
   - Commented out from processing group

### Loading Excluded Tables On-Demand

If these tables are needed in the future, they can be loaded by:

1. **Option A: Re-enable in config**
   ```yaml
   # In timescaledb_tables_v2.yml, change:
   enabled: true
   ```

2. **Option B: Create separate on-demand job**
   - Create a new job that only loads these tables
   - Run manually when roster/cache data is needed

---

*Report generated by analyzing TimescaleDB structure against implementation plan.*
