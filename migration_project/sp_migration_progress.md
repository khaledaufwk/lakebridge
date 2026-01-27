# Stored Procedure Migration Progress

**Started:** 2026-01-26
**Ralph Loop Session:** Active (iteration 1)

## Current Status

### Gold Layer Tables (Databricks)
| Table | Rows | Date Range | Status |
|-------|------|------------|--------|
| gold_fact_workers_history | 85,185,979 | 2025-12-18 to 2026-01-23 | PARTIAL - Missing dimension joins |
| fact_observations | TBD | TBD | EXISTS |

### Priority Stored Procedures

| SP Name | Target Table | Source Lines | Status | Validation |
|---------|--------------|--------------|--------|------------|
| stg.spDeltaSyncFactWorkersHistory | gold_fact_workers_history | 1561 | NEEDS ENHANCEMENT | Pending |
| stg.spDeltaSyncFactObservations | gold_fact_observations | 1165 | NOT STARTED | - |
| stg.spDeltaSyncFactProgress | gold_fact_progress | TBD | NOT STARTED | - |
| stg.spCalculateFactWorkersShifts | gold_fact_workers_shifts | 1639 | NOT STARTED | - |
| stg.spCalculateFactProgress | gold_fact_progress | TBD | NOT STARTED | - |

## Validation Reference

**Reference File:** `migration_project/validate_gold_factworkerhistory.py`
**Commit:** 28971f9

---

## Iteration 1: stg.spDeltaSyncFactWorkersHistory

### Current State Analysis

**Existing Notebook:** `migration_project/pipelines/gold/notebooks/gold_fact_workers_history.py`

**What the notebook DOES:**
- Reads from silver_fact_workers_history (DeviceLocation data)
- Parses WKT coordinates to Latitude/Longitude
- Basic time conversion (ActiveTime/InactiveTime to days)
- Merges into gold table with UPSERT

**What the notebook is MISSING (compared to SQL SP):**

| Feature | SQL SP | Current Notebook | Gap |
|---------|--------|------------------|-----|
| WorkerId resolution | Joins Worker table via ExtWorkerId | NULL placeholder | CRITICAL |
| ProjectId timezone | Converts via Project.TimeZoneName | Uses UTC only | HIGH |
| ZoneId resolution | Joins Zone table via ExtZoneId | Uses FloorId | HIGH |
| CrewId assignment | Joins vwCrewAssignments | NULL placeholder | HIGH |
| WorkshiftId lookup | Complex workshift assignment logic | NULL placeholder | HIGH |
| ShiftLocalDate calc | Uses FactWorkersShifts + fnNearestNeighbor | Uses GeneratedAt date | HIGH |
| TimeCategoryId | fnCalcTimeCategory UDF | Fixed value = 2 | MEDIUM |
| LocationAssignmentClassId | Spatial intersection check | NULL placeholder | MEDIUM |
| ProductiveClassId | Calculation logic | NULL placeholder | LOW |

### Dimension Tables Needed
1. `silver_worker` - for WorkerId resolution
2. `silver_project` - for timezone conversion
3. `silver_zone` - for ZoneId resolution
4. `silver_crew_assignments` - for CrewId assignment
5. `silver_workshift_assignments` - for WorkshiftId lookup
6. `silver_workshift_details` - for TimeCategoryId calculation

### Action Plan

**Phase 1: Enhance Notebook with Dimension Joins**
1. Add Worker ID resolution via ExtWorkerId mapping
2. Add Project timezone conversion
3. Add Zone ID resolution via ExtZoneId mapping
4. Add Crew assignment lookup

**Phase 2: Implement Workshift Logic**
1. Create ShiftLocalDate calculation
2. Implement WorkshiftId lookup
3. Implement TimeCategoryId UDF

**Phase 3: Implement Location Classification**
1. Port LocationAssignmentClassId logic (spatial)
2. Port ProductiveClassId logic

### Enhancement Progress

**Created:** `migration_project/pipelines/gold/notebooks/gold_fact_workers_history_enhanced.py`

**Implemented:**
- [x] WorkerId resolution via silver_resource_device temporal join
- [x] CrewId resolution via silver_crew_composition
- [x] Proper temporal join for device-to-worker mapping
- [x] Data quality metrics and validation

**Verified Join Success:**
- Device -> Worker join: 90.6% match rate (77.2M of 85.2M rows)
- 23,885 distinct workers identified

**Still TODO:**
- [ ] ShiftLocalDate calculation (requires FactWorkersShifts + workshift logic)
- [ ] WorkshiftId lookup (requires vwWorkshiftAssignments equivalent)
- [ ] TimeCategoryId calculation (requires fnCalcTimeCategory UDF)
- [ ] LocationAssignmentClassId (spatial intersection - complex)
- [ ] ProductiveClassId calculation
- [ ] ZoneId resolution (currently uses FloorId as fallback)

### Deployment Status

**Deployed:** 2026-01-26
**Path:** `/Workspace/migration_project/pipelines/gold/notebooks/gold_fact_workers_history_enhanced`

### Next Steps
1. ~~Deploy enhanced notebook to Databricks~~ DONE
2. Run enhanced notebook with full load mode
3. Run validation comparing Databricks to SQL Server
4. If validation passes (>= 99%), proceed to next SP (stg.spDeltaSyncFactObservations)
