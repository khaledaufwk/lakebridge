# Stored Procedure Conversion Plan using Ralph Loop

**Created:** 2026-01-26
**Last Updated:** 2026-01-26
**Project:** WakeCapDW Migration to Databricks
**Methodology:** Autonomous AI-Assisted Conversion using Ralph Loop + ADW Workflow

---

## Executive Summary

This plan outlines the systematic approach for converting **all 70 SQL Server stored procedures** to Databricks notebooks/DLT using the **Ralph Loop** plugin with the **ADW_SP_plan_build_review_fix** skill. The approach leverages:

1. **Ralph Loop** - Iterative self-referential execution with progress tracking
2. **ADW Workflow** - Plan → Build → Review → Fix pipeline per SP
3. **Automatic Validation** - ≥99% row count match requirement before marking PASS

### Quick Stats

| Category | Count | Status |
|----------|-------|--------|
| **Total SPs** | 70 | - |
| Already Converted | 5 | ✅ COMPLETE |
| **To Convert** | 51 | 🔄 PENDING |
| Skip (Admin/Dev) | 14 | ⏭️ NOT NEEDED |

### Category Breakdown (To Convert: 51)

| Category | Count | Method |
|----------|-------|--------|
| Gold Layer Facts | 7 | ADW Workflow (complex) |
| DeltaSync Dimensions | 18 | ADW Workflow → DLT |
| DeltaSync Assignments | 9 | ADW Workflow → DLT |
| DeltaSync Facts (Additional) | 2 | ADW Workflow |
| Calculate Assignments | 8 | ADW Workflow |
| WorkersHistory Support | 5 | ADW Workflow |
| Other Calculate | 2 | Direct conversion |

---

## 0. AUTONOMOUS EXECUTION (Ralph Loop + Claude Tasks)

### 0.1 How Ralph Loop Works with Claude Tasks

```
┌─────────────────────────────────────────────────────────────────────────┐
│              TASK-BASED AUTONOMOUS WORKFLOW (Ralph Loop)                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  BOOTSTRAP (First iteration only):                                       │
│  1. TaskList - Check if tasks exist                                     │
│  2. If no tasks: TaskCreate for all 51 SPs with metadata                │
│                                                                          │
│  EACH ITERATION:                                                         │
│  1. TaskList → Find first pending task (not blocked)                    │
│  2. TaskUpdate → Mark as in_progress (shows spinner in UI)              │
│  3. Run ADW_SP_plan_build_review_fix skill for that SP                  │
│  4. TaskUpdate → Mark as completed (or update description if failed)    │
│  5. Edit tracking table below for git persistence                       │
│  6. git commit -m "SP migration: {sp_name} - {status}"                  │
│  7. Continue to next iteration                                          │
│                                                                          │
│  COMPLETION:                                                             │
│  - When TaskList shows 0 pending tasks                                  │
│  - Output: <promise>ALL_SP_MIGRATIONS_COMPLETE</promise>                │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 0.2 Starting the Autonomous Loop

To start the autonomous SP conversion process:

```bash
/ralph-loop
```

The `.claude/ralph-loop.local.md` file contains the full prompt. The loop will:
1. **Bootstrap**: Create 51 Claude Tasks (if not already created)
2. **Process**: Use TaskList/TaskUpdate to track progress visually
3. **Convert**: Run ADW workflow for each SP
4. **Persist**: Update markdown table for git history
5. **Continue**: Until all tasks completed

### Benefits of Task-Based Approach

| Feature | Description |
|---------|-------------|
| **Visual Progress** | Spinner shows "Converting spDeltaSyncFactWorkersHistory" |
| **Dependencies** | Tasks can be blocked until prerequisites complete |
| **Resume** | TaskList shows exactly where we left off |
| **Native UI** | Works with Claude Code's built-in task display |

### 0.3 SP Master Tracking Table

**IMPORTANT:** Ralph Loop reads and updates this table. Status values:
- `PENDING` - Not yet started
- `IN_PROGRESS` - Currently being converted
- `PASS` - Conversion complete, validation ≥99% match
- `FAIL` - Conversion failed, needs manual intervention
- `BLOCKED` - Blocked by dependency or issue
- `SKIP` - Intentionally skipped (Admin/Dev SPs)

<!-- RALPH_TRACKING_START -->
| # | SP Name | Lines | Category | Status | Session | Notes |
|---|---------|-------|----------|--------|---------|-------|
| 1 | stg.spDeltaSyncFactWorkersHistory | 782 | gold_facts | PASS | 2026-01-26 | Review: PASS (3 HIGH, 4 MED items) |
| 2 | stg.spDeltaSyncFactObservations | 584 | gold_facts | PASS | 2026-01-26 | Already converted (1336 lines) |
| 3 | stg.spCalculateWorkerLocationAssignments | 289 | gold_facts | PASS | 2026-01-26 | 0 blockers, 1 HIGH (doc only) |
| 4 | stg.spCalculateManagerAssignmentSnapshots | 265 | gold_facts | PASS | 2026-01-26 | Recursive CTE + PIVOT + MERGE |
| 5 | stg.spDeltaSyncFactWeatherObservations | 242 | gold_facts | PASS | 2026-01-26 | 0 blockers, 16 sensor columns, float tolerance |
| 6 | stg.spCalculateFactReportedAttendance | 215 | gold_facts | PASS | 2026-01-26 | FULL OUTER JOIN + UNION + NOT MATCHED BY SOURCE |
| 7 | stg.spDeltaSyncFactProgress | 117 | gold_facts | PENDING | - | |
| 8 | stg.spDeltaSyncDimWorker | 227 | deltasync_dims | PENDING | - | |
| 9 | stg.spDeltaSyncDimTask | 219 | deltasync_dims | PENDING | - | |
| 10 | stg.spDeltaSyncDimProject | 198 | deltasync_dims | PENDING | - | |
| 11 | stg.spDeltaSyncDimWorkshiftDetails | 167 | deltasync_dims | PENDING | - | |
| 12 | stg.spDeltaSyncDimZone | 162 | deltasync_dims | PENDING | - | SPATIAL |
| 13 | stg.spDeltaSyncDimActivity | 160 | deltasync_dims | PENDING | - | |
| 14 | stg.spDeltaSyncDimCrew | 151 | deltasync_dims | PENDING | - | |
| 15 | stg.spDeltaSyncDimDevice | 149 | deltasync_dims | PENDING | - | |
| 16 | stg.spDeltaSyncDimFloor | 137 | deltasync_dims | PENDING | - | SPATIAL |
| 17 | stg.spDeltaSyncDimTrade | 132 | deltasync_dims | PENDING | - | |
| 18 | stg.spDeltaSyncDimCompany | 126 | deltasync_dims | PENDING | - | |
| 19 | stg.spDeltaSyncDimWorkerStatus | 125 | deltasync_dims | PENDING | - | |
| 20 | stg.spDeltaSyncDimTitle | 125 | deltasync_dims | PENDING | - | |
| 21 | stg.spDeltaSyncDimDepartment | 125 | deltasync_dims | PENDING | - | |
| 22 | stg.spDeltaSyncDimLocationGroup | 121 | deltasync_dims | PENDING | - | |
| 23 | stg.spDeltaSyncDimWorkshift | 115 | deltasync_dims | PENDING | - | |
| 24 | stg.spDeltaSyncDimOrganization | 90 | deltasync_dims | PENDING | - | |
| 25 | stg.spDeltaSyncDimObservationSource | 28 | deltasync_dims | PENDING | - | |
| 26 | stg.spDeltaSyncLocationGroupAssignments | 201 | deltasync_assign | PENDING | - | |
| 27 | stg.spDeltaSyncPermissions | 138 | deltasync_assign | PENDING | - | |
| 28 | stg.spDeltaSyncWorkerLocationAssignments | 122 | deltasync_assign | PENDING | - | |
| 29 | stg.spDeltaSyncManagerAssignments | 114 | deltasync_assign | PENDING | - | |
| 30 | stg.spDeltaSyncDeviceAssignments | 108 | deltasync_assign | PENDING | - | |
| 31 | stg.spDeltaSyncCrewAssignments | 108 | deltasync_assign | PENDING | - | |
| 32 | stg.spDeltaSyncCrewManagerAssignments | 106 | deltasync_assign | PENDING | - | |
| 33 | stg.spDeltaSyncWorkshiftAssignments | 102 | deltasync_assign | PENDING | - | |
| 34 | stg.spDeltaSyncWorkerStatusAssignments | 72 | deltasync_assign | PENDING | - | |
| 35 | stg.spDeltaSyncFactReportedAttendance_ResourceTimesheet | 141 | deltasync_facts | PENDING | - | |
| 36 | stg.spDeltaSyncFactReportedAttendance_ResourceHours | 110 | deltasync_facts | PENDING | - | |
| 37 | stg.spCalculateLocationGroupAssignments | 153 | calc_assign | PENDING | - | |
| 38 | stg.spCalculateManagerAssignments | 151 | calc_assign | PENDING | - | |
| 39 | stg.spCalculateDeviceAssignments | 143 | calc_assign | PENDING | - | |
| 40 | stg.spCalculateTradeAssignments | 130 | calc_assign | PENDING | - | |
| 41 | stg.spCalculateCrewAssignments | 130 | calc_assign | PENDING | - | |
| 42 | stg.spCalculateWorkshiftAssignments | 129 | calc_assign | PENDING | - | |
| 43 | stg.spCalculateFactProgress | 112 | calc_assign | PENDING | - | |
| 44 | stg.spCalculateContactTracingRule_Manager | 108 | calc_assign | PENDING | - | |
| 45 | stg.spWorkersHistory_UpdateAssignments_2_WorkShiftDates | 276 | workers_history | PENDING | - | |
| 46 | stg.spCalculateFactWorkersShifts_FixOverlaps | 254 | workers_history | PENDING | - | |
| 47 | stg.spWorkersHistory_UpdateAssignments_1_Crews | 149 | workers_history | PENDING | - | |
| 48 | stg.spWorkersHistory_FixOverlaps | 79 | workers_history | PENDING | - | |
| 49 | stg.spWorkersHistory_UpdateAssignments | 12 | workers_history | PENDING | - | Wrapper |
| 50 | stg.spCalculateProjectActiveFlag | 36 | other_calc | PENDING | - | Simple |
| 51 | stg.spCalculateFactWorkersShifts_Partial | 28 | other_calc | PENDING | - | Simple |
| - | stg.spCalculateFactWorkersShifts | 821 | ALREADY_DONE | ✅ PASS | - | calc_fact_workers_shifts.py |
| - | stg.spCalculateFactWorkersShiftsCombined | 645 | ALREADY_DONE | ✅ PASS | - | calc_fact_workers_shifts_combined.py |
| - | stg.spCalculateFactWorkersContacts_ByRule | 477 | ALREADY_DONE | ✅ PASS | - | calc_worker_contacts.py |
| - | mrg.spMergeOldData | 453 | ALREADY_DONE | ✅ PASS | - | merge_old_data.py |
| - | stg.spWorkersHistory_UpdateAssignments_3_LocationClass | 553 | ALREADY_DONE | ✅ PASS | - | update_workers_history_location_class.py |
| - | stg.spResetDatabase | 534 | admin | ⏭️ SKIP | - | Admin only |
| - | dbo.spCalculateStatsForWeb | 155 | admin | ⏭️ SKIP | - | Web stats |
| - | dbo.spMaintainPartitions | 89 | admin | ⏭️ SKIP | - | Delta handles |
| - | dbo.spMaintainPartitions_Site | 85 | admin | ⏭️ SKIP | - | Delta handles |
| - | dbo.spMaintainIndexes | 70 | admin | ⏭️ SKIP | - | Delta Z-ORDER |
| - | stg.spResetSyncState | 45 | admin | ⏭️ SKIP | - | Admin only |
| - | dbo.spUpdateStatsForWeb | 41 | admin | ⏭️ SKIP | - | Web stats |
| - | stg.spUpdateSyncState | 37 | admin | ⏭️ SKIP | - | Watermarks replace |
| - | stg.spCleanupLog | 20 | admin | ⏭️ SKIP | - | Delta VACUUM |
| - | dbo.spUpdateDBStats | 7 | admin | ⏭️ SKIP | - | Not needed |
| - | stg.Dev_InitRecalcShiftLocalDateAndOthers | 173 | dev_test | ⏭️ SKIP | - | Dev only |
| - | stg.spDeltaSyncDimCrew_CopySP_Test2 | 113 | dev_test | ⏭️ SKIP | - | Test copy |
| - | stg.spDeltaSyncWorkerLocationAssignments_archived | 106 | dev_test | ⏭️ SKIP | - | Archived |
| - | stg.spDeltaSyncDimCrew_CopySP_Test | 105 | dev_test | ⏭️ SKIP | - | Test copy |
<!-- RALPH_TRACKING_END -->

### 0.4 Progress Summary

**Calculated from tracking table above:**
- Total SPs: 70
- Completed (PASS): 5
- Pending: 51
- Skipped: 14
- Failed/Blocked: 0

**Progress: 5/56 (8.9%) of convertible SPs**

---

## 1. Complete SP Inventory (70 Total)

### 1.1 Already Converted (5 Complex SPs) - COMPLETE

| Notebook | Source SP | Lines | Status |
|----------|-----------|-------|--------|
| `calc_fact_workers_shifts.py` | stg.spCalculateFactWorkersShifts | 821 | COMPLETE |
| `calc_fact_workers_shifts_combined.py` | stg.spCalculateFactWorkersShiftsCombined | 645 | COMPLETE |
| `calc_worker_contacts.py` | stg.spCalculateFactWorkersContacts_ByRule | 477 | COMPLETE |
| `merge_old_data.py` | mrg.spMergeOldData | 453 | COMPLETE |
| `update_workers_history_location_class.py` | stg.spWorkersHistory_UpdateAssignments_3_LocationClass | 553 | COMPLETE |

### 1.2 Gold Layer Fact SPs - HIGH PRIORITY (7 SPs)

These are the core fact table population procedures. Convert using Ralph Loop with notebooks.

| Priority | Stored Procedure | Lines | Complexity | Patterns | Target |
|----------|-----------------|-------|------------|----------|--------|
| **1** | stg.spDeltaSyncFactWorkersHistory | 782 | 9/10 | TEMP_TABLE, SPATIAL, WINDOW, CTE | gold_fact_workers_history |
| **2** | stg.spDeltaSyncFactObservations | 584 | 7/10 | TEMP_TABLE, MERGE, WINDOW, CTE | gold_fact_observations |
| **3** | stg.spCalculateWorkerLocationAssignments | 289 | 6/10 | TEMP_TABLE, MERGE, WINDOW, CTE | gold_worker_location_assignments |
| **4** | stg.spCalculateManagerAssignmentSnapshots | 265 | 6/10 | TEMP_TABLE, MERGE, PIVOT, CTE | gold_manager_snapshots |
| **5** | stg.spDeltaSyncFactWeatherObservations | 242 | 6/10 | TEMP_TABLE, MERGE, WINDOW, CTE | gold_fact_weather_observations |
| **6** | stg.spCalculateFactReportedAttendance | 215 | 5/10 | TEMP_TABLE, MERGE | gold_fact_reported_attendance |
| **7** | stg.spDeltaSyncFactProgress | 117 | 4/10 | MERGE, WINDOW | gold_fact_progress |

### 1.3 DeltaSync Dimension SPs - MEDIUM PRIORITY (18 SPs)

These handle Silver layer dimension table syncing. Convert to **DLT streaming tables** with APPLY CHANGES.

| Stored Procedure | Lines | Complexity | Patterns | Target DLT Table |
|-----------------|-------|------------|----------|------------------|
| stg.spDeltaSyncDimWorker | 227 | 5/10 | MERGE, WINDOW, CTE | silver_dim_worker |
| stg.spDeltaSyncDimTask | 219 | 5/10 | MERGE, CTE | silver_dim_task |
| stg.spDeltaSyncDimProject | 198 | 5/10 | MERGE, CTE | silver_dim_project |
| stg.spDeltaSyncDimWorkshiftDetails | 167 | 5/10 | TEMP_TABLE, MERGE, WINDOW, CTE | silver_dim_workshift_details |
| stg.spDeltaSyncDimZone | 162 | 6/10 | MERGE, **SPATIAL**, WINDOW, CTE | silver_dim_zone |
| stg.spDeltaSyncDimActivity | 160 | 4/10 | MERGE, CTE | silver_dim_activity |
| stg.spDeltaSyncDimCrew | 151 | 4/10 | MERGE, CTE | silver_dim_crew |
| stg.spDeltaSyncDimDevice | 149 | 4/10 | MERGE, CTE | silver_dim_device |
| stg.spDeltaSyncDimFloor | 137 | 5/10 | MERGE, **SPATIAL**, WINDOW, CTE | silver_dim_floor |
| stg.spDeltaSyncDimTrade | 132 | 4/10 | MERGE, CTE | silver_dim_trade |
| stg.spDeltaSyncDimCompany | 126 | 4/10 | MERGE, CTE | silver_dim_company |
| stg.spDeltaSyncDimWorkerStatus | 125 | 4/10 | MERGE, CTE | silver_dim_worker_status |
| stg.spDeltaSyncDimTitle | 125 | 4/10 | MERGE, CTE | silver_dim_title |
| stg.spDeltaSyncDimDepartment | 125 | 4/10 | MERGE, CTE | silver_dim_department |
| stg.spDeltaSyncDimLocationGroup | 121 | 4/10 | MERGE, CTE | silver_dim_location_group |
| stg.spDeltaSyncDimWorkshift | 115 | 4/10 | MERGE, CTE | silver_dim_workshift |
| stg.spDeltaSyncDimOrganization | 90 | 3/10 | MERGE, CTE | silver_dim_organization |
| stg.spDeltaSyncDimObservationSource | 28 | 2/10 | MERGE | silver_dim_observation_source |

### 1.4 DeltaSync Assignment SPs - MEDIUM PRIORITY (9 SPs)

Bridge/association table syncing. Convert to **DLT streaming tables**.

| Stored Procedure | Lines | Complexity | Target DLT Table |
|-----------------|-------|------------|------------------|
| stg.spDeltaSyncLocationGroupAssignments | 201 | 4/10 | silver_location_group_assignments |
| stg.spDeltaSyncPermissions | 138 | 4/10 | silver_permissions |
| stg.spDeltaSyncWorkerLocationAssignments | 122 | 4/10 | silver_worker_location_assignments |
| stg.spDeltaSyncManagerAssignments | 114 | 4/10 | silver_manager_assignments |
| stg.spDeltaSyncDeviceAssignments | 108 | 3/10 | silver_device_assignments |
| stg.spDeltaSyncCrewAssignments | 108 | 3/10 | silver_crew_assignments |
| stg.spDeltaSyncCrewManagerAssignments | 106 | 3/10 | silver_crew_manager_assignments |
| stg.spDeltaSyncWorkshiftAssignments | 102 | 3/10 | silver_workshift_assignments |
| stg.spDeltaSyncWorkerStatusAssignments | 72 | 3/10 | silver_worker_status_assignments |

### 1.5 DeltaSync Fact SPs (Additional) - MEDIUM PRIORITY (2 SPs)

| Stored Procedure | Lines | Complexity | Target |
|-----------------|-------|------------|--------|
| stg.spDeltaSyncFactReportedAttendance_ResourceTimesheet | 141 | 4/10 | gold_fact_reported_attendance_timesheet |
| stg.spDeltaSyncFactReportedAttendance_ResourceHours | 110 | 4/10 | gold_fact_reported_attendance_hours |

### 1.6 Calculate Assignment SPs - MEDIUM PRIORITY (8 SPs)

Assignment calculation logic. Convert to notebooks or DLT batch tables.

| Stored Procedure | Lines | Complexity | Target |
|-----------------|-------|------------|--------|
| stg.spCalculateLocationGroupAssignments | 153 | 4/10 | gold_location_group_assignments |
| stg.spCalculateManagerAssignments | 151 | 4/10 | gold_manager_assignments |
| stg.spCalculateDeviceAssignments | 143 | 4/10 | gold_device_assignments |
| stg.spCalculateTradeAssignments | 130 | 4/10 | gold_trade_assignments |
| stg.spCalculateCrewAssignments | 130 | 4/10 | gold_crew_assignments |
| stg.spCalculateWorkshiftAssignments | 129 | 4/10 | gold_workshift_assignments |
| stg.spCalculateFactProgress | 112 | 4/10 | gold_fact_progress_calc |
| stg.spCalculateContactTracingRule_Manager | 108 | 4/10 | gold_contact_tracing_manager |

### 1.7 WorkersHistory Support SPs - MEDIUM PRIORITY (5 SPs)

Supporting logic for FactWorkersHistory. Convert to notebooks.

| Stored Procedure | Lines | Complexity | Target |
|-----------------|-------|------------|--------|
| stg.spWorkersHistory_UpdateAssignments_2_WorkShiftDates | 276 | 5/10 | workers_history_workshift_dates.py |
| stg.spCalculateFactWorkersShifts_FixOverlaps | 254 | 5/10 | fix_workers_shifts_overlaps.py |
| stg.spWorkersHistory_UpdateAssignments_1_Crews | 149 | 4/10 | workers_history_crews.py |
| stg.spWorkersHistory_FixOverlaps | 79 | 3/10 | workers_history_fix_overlaps.py |
| stg.spWorkersHistory_UpdateAssignments | 12 | 1/10 | Wrapper - integrate into main |

### 1.8 Other Calculate SPs - LOW PRIORITY (2 SPs)

| Stored Procedure | Lines | Complexity | Target |
|-----------------|-------|------------|--------|
| stg.spCalculateProjectActiveFlag | 36 | 2/10 | Inline in project dimension |
| stg.spCalculateFactWorkersShifts_Partial | 28 | 2/10 | Partial refresh utility |

### 1.9 Admin/Maintenance SPs - NOT NEEDED IN DATABRICKS (10 SPs)

These handle SQL Server-specific maintenance tasks. **Not needed** in Databricks - Delta Lake handles optimization automatically.

| Stored Procedure | Lines | Reason to Skip |
|-----------------|-------|----------------|
| stg.spResetDatabase | 534 | Admin reset - not for production |
| dbo.spCalculateStatsForWeb | 155 | Web stats - separate concern |
| dbo.spMaintainPartitions | 89 | Delta auto-optimizes |
| dbo.spMaintainPartitions_Site | 85 | Delta auto-optimizes |
| dbo.spMaintainIndexes | 70 | Delta Z-ORDER replaces |
| stg.spResetSyncState | 45 | Sync state reset - admin |
| dbo.spUpdateStatsForWeb | 41 | Web stats - separate concern |
| stg.spUpdateSyncState | 37 | Watermark tracking replaces |
| stg.spCleanupLog | 20 | Delta VACUUM replaces |
| dbo.spUpdateDBStats | 7 | Not needed in Databricks |

### 1.10 Dev/Test SPs - SKIP (4 SPs)

| Stored Procedure | Lines | Reason to Skip |
|-----------------|-------|----------------|
| stg.Dev_InitRecalcShiftLocalDateAndOthers | 173 | Dev utility only |
| stg.spDeltaSyncDimCrew_CopySP_Test2 | 113 | Test copy |
| stg.spDeltaSyncWorkerLocationAssignments_archived | 106 | Archived version |
| stg.spDeltaSyncDimCrew_CopySP_Test | 105 | Test copy |

---

## 2. Conversion Strategy by Category

### 2.1 Conversion Target Summary

| SP Category | Count | Conversion Target | Method |
|-------------|-------|-------------------|--------|
| Gold Fact SPs | 7 | Python Notebooks | Ralph Loop (complex) |
| DeltaSync Dimensions | 18 | DLT Streaming Tables | Batch/Template |
| DeltaSync Assignments | 9 | DLT Streaming Tables | Batch/Template |
| DeltaSync Facts | 2 | Python Notebooks | Ralph Loop |
| Calculate Assignments | 8 | DLT Batch Tables or Notebooks | Mixed |
| WorkersHistory Support | 5 | Python Notebooks | Ralph Loop |
| Other Calculate | 2 | Inline/Simple Notebooks | Direct conversion |
| Admin/Maintenance | 10 | Skip | Not needed |
| Dev/Test | 4 | Skip | Not needed |

### 2.2 Recommended Conversion Order

```
Phase 1 (Week 1): Gold Layer Facts
├── Priority 1: stg.spDeltaSyncFactWorkersHistory (782 lines) - Ralph Loop
├── Priority 2: stg.spDeltaSyncFactObservations (584 lines) - Ralph Loop
└── Priority 3: stg.spCalculateWorkerLocationAssignments (289 lines) - Ralph Loop

Phase 2 (Week 2): Gold Layer Facts (continued) + DeltaSync Dimensions
├── Priorities 4-7: Remaining Gold Fact SPs - Ralph Loop
└── DeltaSync Dimensions batch (18 SPs) - Template-based DLT

Phase 3 (Week 3): Assignments + Support
├── DeltaSync Assignments (9 SPs) - Template-based DLT
├── Calculate Assignments (8 SPs) - Mixed approach
└── WorkersHistory Support (5 SPs) - Ralph Loop

Phase 4 (Week 4): Cleanup + Validation
├── Remaining low-priority SPs
├── Integration testing
└── Documentation updates
```

---

## 3. Ralph Loop Methodology

### 3.1 Core Philosophy

Ralph Loop implements **iterative, self-referential AI development**:

1. **Same prompt** fed across iterations
2. **Previous work persists** in files and git history
3. **Claude reads its own output** from prior iterations
4. **Automatic verification** (tests, validation queries) gates completion

### 3.2 When to Use Ralph Loop

| Use Case | Ralph Loop? | Reason |
|----------|-------------|--------|
| Complex SP with CURSOR/DYNAMIC_SQL | Yes | Requires multiple refinement passes |
| SP with spatial/geometry logic | Yes | H3/Shapely integration needs iteration |
| SP with 300+ lines | Yes | Too complex for single-shot |
| Simple MERGE-only SP (<150 lines) | No | Single conversion sufficient |
| Dimension sync (DeltaSync*) | Maybe | Use if validation fails first attempt |

### 3.3 Key Best Practices

#### Escape Hatches (Critical)
```bash
# ALWAYS set max-iterations as safety net
/ralph-loop "..." --max-iterations 30 --completion-promise "CONVERSION_COMPLETE"
```

#### Clear Completion Criteria
```
Output <promise>CONVERSION_COMPLETE</promise> when:
- All business logic implemented
- Validation query passes
- No syntax errors in notebook
- Delta MERGE operation working
```

#### Incremental Goals
```
Phase 1: Parse source SP, document business logic
Phase 2: Create notebook skeleton with widgets
Phase 3: Implement core transformation logic
Phase 4: Add Delta MERGE/APPEND operations
Phase 5: Add validation and reconciliation
Phase 6: Test with sample data
```

---

## 4. Conversion Templates

### 4.1 Standard Notebook Structure

Based on existing conversions (e.g., `calc_fact_workers_shifts.py`):

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # {Notebook Title}
# MAGIC
# MAGIC **Converted from:** `{schema}.{procedure_name}` ({lines} lines)
# MAGIC
# MAGIC **Purpose:** {description}
# MAGIC
# MAGIC **Original Patterns:** {CURSOR, TEMP_TABLE, MERGE, SPATIAL, etc.}
# MAGIC
# MAGIC **Conversion Approach:**
# MAGIC - CURSOR -> Spark parallel processing
# MAGIC - TEMP TABLE -> DataFrame intermediate results
# MAGIC - Complex CTEs -> Step-by-step DataFrame transformations
# MAGIC - MERGE -> Delta MERGE operation
# MAGIC
# MAGIC **Key Business Logic:**
# MAGIC 1. {step 1}
# MAGIC 2. {step 2}
# MAGIC ...

# COMMAND ----------
# Configuration (widgets)

# COMMAND ----------
# Step 1: Load Source Data

# COMMAND ----------
# Step 2-N: Transformation Logic

# COMMAND ----------
# Final Step: Delta MERGE/Write

# COMMAND ----------
# Validation
```

### 4.2 Pattern Conversion Reference

| SQL Server Pattern | Databricks Equivalent |
|--------------------|----------------------|
| `CURSOR OVER partitions` | `Window.partitionBy()` + parallel processing |
| `#TempTable` | DataFrame variable or temp view |
| `MERGE INTO ... USING` | `DeltaTable.merge()` |
| `CTE (WITH clause)` | Chained DataFrame transformations |
| `PIVOT/UNPIVOT` | `pivot()` / `unpivot()` functions |
| `GEOGRAPHY::STDistance` | H3 `h3_distance()` or Haversine UDF |
| `DYNAMIC_SQL` | Parameterized queries or f-strings |
| `TRY_CONVERT` | `try_cast()` |

### 4.3 DLT Streaming Table Template (for DeltaSync SPs)

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="silver_dim_{table_name}",
    comment="Converted from stg.spDeltaSyncDim{TableName}",
    table_properties={
        "quality": "silver",
        "source_sp": "stg.spDeltaSyncDim{TableName}"
    }
)
@dlt.expect_or_drop("valid_pk", "Id IS NOT NULL")
def silver_dim_{table_name}():
    return (
        dlt.read_stream("bronze_{source_table}")
        .filter(F.col("DeletedAt").isNull())
        # Add transformation logic here
        .select(
            # Column mappings
        )
    )
```

---

## 5. Prompt Templates (Reference)

> **Note:** In **autonomous mode** (`/ralph-loop`), the ADW_SP_plan_build_review_fix skill handles prompt generation automatically. These templates are provided for **manual mode** or custom scenarios.

### 5.1 Template Prompt Structure

```
Convert SQL Server stored procedure to Databricks notebook.

## Source
- Procedure: {schema}.{procedure_name}
- Location: migration_project/source_sql/stored_procedures/{filename}
- Lines: {count}
- Patterns: {patterns}

## Target
- Output: migration_project/pipelines/gold/notebooks/{notebook_name}.py
- Target Table: wakecap_prod.gold.{table_name}
- Source Tables: {silver_tables}

## Requirements
1. Read source SP and understand all business logic
2. Document each transformation step in markdown cells
3. Use existing patterns from migration_project/pipelines/notebooks/
4. Handle NULL values appropriately
5. Implement Delta MERGE for incremental updates
6. Add validation query comparing source vs target counts

## Completion Criteria
Output <promise>CONVERSION_COMPLETE</promise> when:
- Notebook created with all transformation logic
- No Python syntax errors
- Delta write operation implemented
- Validation cell added
- Business logic matches source SP

## Constraints
- Max iterations: {max_iter}
- Use wakecap_prod catalog
- Follow existing notebook patterns
- Include widget parameters for date filtering
```

### 5.2 Priority 1: stg.spDeltaSyncFactWorkersHistory (782 lines)

```bash
/ralph-loop "
Convert SQL Server stored procedure to Databricks notebook.

## Source
- Procedure: stg.spDeltaSyncFactWorkersHistory
- Location: migration_project/source_sql/stored_procedures/stg.spDeltaSyncFactWorkersHistory.sql
- Lines: 782
- Patterns: TEMP_TABLE, SPATIAL (zone lookups), WINDOW_FUNCTION, CTE

## Target
- Output: migration_project/pipelines/gold/notebooks/gold_fact_workers_history_full.py
- Target Table: wakecap_prod.gold.gold_fact_workers_history
- Source Tables:
  - wakecap_prod.silver.silver_fact_workers_history (82M rows)
  - wakecap_prod.silver.silver_worker
  - wakecap_prod.silver.silver_crew
  - wakecap_prod.silver.silver_workshift
  - wakecap_prod.silver.silver_zone

## Key Business Logic to Implement
1. Load incremental data from silver_fact_workers_history using watermark
2. Lookup WorkerId from DeviceAssignments (point-in-time)
3. Lookup CrewId from CrewAssignments (point-in-time)
4. Lookup WorkshiftId from WorkshiftAssignments
5. Calculate TimeCategoryId based on shift times
6. Determine LocationAssignmentClassId from zone assignments
7. Calculate ProductiveClassId from zone classifications
8. Apply timezone conversion for LocalDate/ShiftLocalDate

## Reference
- Review existing pattern: migration_project/pipelines/notebooks/calc_fact_workers_shifts.py
- Use similar widget structure and DataFrame patterns

## Completion Criteria
Output <promise>CONVERSION_COMPLETE</promise> when:
- All 8 business logic steps implemented
- Notebook has no syntax errors
- Delta MERGE implemented with proper composite key
- Watermark tracking added
- Validation query comparing silver input vs gold output counts

After 25 iterations without completion:
- Document blocking issues
- List what was implemented vs remaining
- Suggest alternative approaches
" --max-iterations 30 --completion-promise "CONVERSION_COMPLETE"
```

### 5.3 Priority 2: stg.spDeltaSyncFactObservations (584 lines)

```bash
/ralph-loop "
Convert SQL Server stored procedure to Databricks notebook.

## Source
- Procedure: stg.spDeltaSyncFactObservations
- Location: migration_project/source_sql/stored_procedures/stg.spDeltaSyncFactObservations.sql
- Lines: 584
- Patterns: TEMP_TABLE, MERGE, WINDOW_FUNCTION, CTE

## Target
- Output: migration_project/pipelines/gold/notebooks/gold_fact_observations.py
- Target Table: wakecap_prod.gold.gold_fact_observations
- Source Tables:
  - wakecap_prod.raw.timescale_observation (if exists)
  - wakecap_prod.silver.silver_observation_*

## Key Business Logic
1. Incremental observation loading with watermark
2. Status tracking and transitions
3. Severity/type classifications
4. Source system mappings
5. Dimension lookups (Worker, Project, Zone)

## Reference
- Pattern: migration_project/pipelines/notebooks/calc_fact_workers_shifts.py

## Completion Criteria
Output <promise>CONVERSION_COMPLETE</promise> when:
- All transformation logic implemented
- Delta MERGE with proper key
- Watermark tracking
- Validation query added

After 20 iterations without completion:
- Document blockers and partial progress
" --max-iterations 25 --completion-promise "CONVERSION_COMPLETE"
```

### 5.4 Priority 3-7: Template (Adjust per SP)

```bash
# Template for remaining Gold SPs
/ralph-loop "
Convert {procedure_name} ({lines} lines) to Databricks notebook.

Source: migration_project/source_sql/stored_procedures/{filename}
Target: migration_project/pipelines/gold/notebooks/{notebook_name}.py
Patterns: {patterns}

Follow patterns from existing notebooks in migration_project/pipelines/notebooks/.
Implement Delta MERGE, add validation, use watermark tracking.

Output <promise>CONVERSION_COMPLETE</promise> when done.
" --max-iterations {15-25 based on complexity} --completion-promise "CONVERSION_COMPLETE"
```

### 5.5 Batch DeltaSync Dimension Conversion

For the 18 DeltaSync Dimension SPs, use a batch approach:

```bash
/ralph-loop "
Create DLT streaming table definitions for the following DeltaSync Dimension SPs:

## Target Output
- File: migration_project/pipelines/dlt/silver_dimensions_batch.py
- Pattern: DLT streaming tables with APPLY CHANGES

## SPs to Convert (18 total)
1. stg.spDeltaSyncDimWorker (227 lines)
2. stg.spDeltaSyncDimTask (219 lines)
3. stg.spDeltaSyncDimProject (198 lines)
4. stg.spDeltaSyncDimWorkshiftDetails (167 lines)
5. stg.spDeltaSyncDimZone (162 lines) - Has SPATIAL
6. stg.spDeltaSyncDimActivity (160 lines)
7. stg.spDeltaSyncDimCrew (151 lines)
8. stg.spDeltaSyncDimDevice (149 lines)
9. stg.spDeltaSyncDimFloor (137 lines) - Has SPATIAL
10. stg.spDeltaSyncDimTrade (132 lines)
11. stg.spDeltaSyncDimCompany (126 lines)
12. stg.spDeltaSyncDimWorkerStatus (125 lines)
13. stg.spDeltaSyncDimTitle (125 lines)
14. stg.spDeltaSyncDimDepartment (125 lines)
15. stg.spDeltaSyncDimLocationGroup (121 lines)
16. stg.spDeltaSyncDimWorkshift (115 lines)
17. stg.spDeltaSyncDimOrganization (90 lines)
18. stg.spDeltaSyncDimObservationSource (28 lines)

## Requirements
- Read each source SP to understand column mappings
- Create DLT table with proper schema
- Handle DeletedAt filtering
- Add DQ expectations (valid_pk)
- For SPATIAL SPs (Zone, Floor), note geometry handling

Output <promise>CONVERSION_COMPLETE</promise> when all 18 DLT tables defined.
" --max-iterations 40 --completion-promise "CONVERSION_COMPLETE"
```

---

## 6. Verification & Testing Strategy

### 6.1 Automatic Verification (In Ralph Loop)

Each iteration should include:

```python
# Validation cell at end of notebook
validation_query = """
SELECT
    'source' as layer, COUNT(*) as cnt
FROM {source_table}
WHERE {date_filter}
UNION ALL
SELECT
    'target' as layer, COUNT(*) as cnt
FROM {target_table}
WHERE {date_filter}
"""

result = spark.sql(validation_query)
display(result)

# Assert counts match (within tolerance for DQ drops)
source_cnt = result.filter("layer = 'source'").first()['cnt']
target_cnt = result.filter("layer = 'target'").first()['cnt']
tolerance = 0.01  # 1% tolerance for DQ drops

assert abs(source_cnt - target_cnt) / source_cnt < tolerance, \
    f"Count mismatch: source={source_cnt}, target={target_cnt}"

print("Validation PASSED")
```

### 6.2 Manual Verification Checklist

After Ralph Loop completes:

- [ ] Review generated notebook for business logic accuracy
- [ ] Compare key aggregations with SQL Server source
- [ ] Verify NULL handling matches original behavior
- [ ] Test with date range filter
- [ ] Run on subset before full table
- [ ] Check Delta table properties (partitioning, Z-ORDER)

---

## 7. Workflow for SP Conversions

### 7.1 Standard Workflow

```
+---------------------------------------------------------------------+
|                     SP CONVERSION WORKFLOW                           |
+---------------------------------------------------------------------+
|                                                                      |
|  1. ASSESS                                                           |
|     +-- Read source SP                                               |
|     +-- Identify patterns (CURSOR, TEMP_TABLE, SPATIAL, etc.)        |
|     +-- Count lines, estimate complexity (1-10)                      |
|     +-- Determine target (DLT table vs notebook)                     |
|                                                                      |
|  2. PREPARE                                                          |
|     +-- Identify source Silver tables                                |
|     +-- Identify target Gold table schema                            |
|     +-- Document key business logic                                  |
|     +-- Create Ralph Loop prompt                                     |
|                                                                      |
|  3. CONVERT (Ralph Loop)                                             |
|     +-- /ralph-loop "{prompt}" --max-iterations N                    |
|     +-- Claude iteratively builds notebook                           |
|     +-- Each iteration: read/write/validate                          |
|     +-- Exits on <promise>CONVERSION_COMPLETE</promise>              |
|                                                                      |
|  4. REVIEW                                                           |
|     +-- Human review of generated notebook                           |
|     +-- Compare with source SP logic                                 |
|     +-- Run /lakebridge-review for automated analysis                |
|     +-- Fix any issues with /lakebridge-fix                          |
|                                                                      |
|  5. TEST                                                             |
|     +-- Run notebook on Databricks cluster                           |
|     +-- Validate row counts                                          |
|     +-- Compare key aggregations                                     |
|     +-- Performance test on full data                                |
|                                                                      |
|  6. DEPLOY                                                           |
|     +-- Deploy notebook to Databricks workspace                      |
|     +-- Create/update job schedule                                   |
|     +-- Update MIGRATION_STATUS.md                                   |
|                                                                      |
+---------------------------------------------------------------------+
```

### 7.2 Command Reference

**Autonomous Mode (Recommended):**
```bash
# Start full autonomous conversion of all 51 SPs
/ralph-loop

# This reads .claude/ralph-loop.local.md which drives the workflow
```

**Single SP Mode (using ADW skill):**
```bash
# Convert one SP using the ADW workflow
Use the ADW_SP_plan_build_review_fix skill to migrate {sp_name}
```

**Management Commands:**
```bash
# Cancel if stuck
/cancel-ralph

# Check progress
grep -E "PENDING|PASS|FAIL" specs/ralph-loop-sp-conversion-plan.md | wc -l

# View recent changes
git diff --stat
```

### 7.3 Complexity-Based Iteration Limits

| Complexity Score | Lines | Max Iterations | Notes |
|-----------------|-------|----------------|-------|
| 1-3 | <150 | 10 | Simple, may not need Ralph Loop |
| 4-5 | 150-300 | 15 | Standard conversion |
| 6-7 | 300-500 | 20 | Complex, needs multiple passes |
| 8-9 | 500-800 | 30 | Very complex, expect refinement |
| 10 | 800+ | 40 | Break into multiple notebooks |

---

## 8. Files & Resources

### 8.1 Reference Files

| Resource | Path | Purpose |
|----------|------|---------|
| Existing Conversion | `migration_project/pipelines/notebooks/calc_fact_workers_shifts.py` | Pattern reference |
| Source SPs | `migration_project/source_sql/stored_procedures/` | Source code (70 files) |
| Silver Tables | `migration_project/pipelines/silver/config/silver_tables.yml` | Source schema |
| Gold Config | `migration_project/pipelines/gold/config/gold_tables.yml` | Target schema |
| Databricks Expert | `.claude/commands/experts/databricks.md` | Platform patterns |

### 8.2 Output Locations

| Artifact | Path |
|----------|------|
| Gold Notebooks | `migration_project/pipelines/gold/notebooks/` |
| DLT Definitions | `migration_project/pipelines/dlt/` |
| Support Notebooks | `migration_project/pipelines/notebooks/` |
| Validation Scripts | `migration_project/pipelines/validation/` |

---

## 9. Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Conversion Accuracy | 99%+ | Row count match source vs target |
| Business Logic Fidelity | 100% | Manual review of key calculations |
| Iteration Efficiency | <20 avg | Iterations per SP conversion |
| Time per SP | <2 hours | Wall clock for complex SPs |
| Total SPs Converted | 56/70 | Excluding Admin/Dev (14 skipped) |

---

## 10. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Ralph Loop infinite iteration | Always use `--max-iterations` |
| Lost context between iterations | Commit work frequently, use git history |
| Complex spatial logic | Use H3 library, reference spatial_udfs.py |
| Performance issues on large tables | Implement partitioning, test on subset first |
| Missing Silver source tables | Verify Silver layer complete before Gold conversion |

---

## 11. Execution Timeline

### Week 1: Gold Layer Facts (Priority 1-3)
- [ ] stg.spDeltaSyncFactWorkersHistory (782 lines) - Ralph Loop
- [ ] stg.spDeltaSyncFactObservations (584 lines) - Ralph Loop
- [ ] stg.spCalculateWorkerLocationAssignments (289 lines) - Ralph Loop

### Week 2: Gold Layer Facts (Priority 4-7) + DeltaSync Dimensions
- [ ] stg.spCalculateManagerAssignmentSnapshots (265 lines)
- [ ] stg.spDeltaSyncFactWeatherObservations (242 lines)
- [ ] stg.spCalculateFactReportedAttendance (215 lines)
- [ ] stg.spDeltaSyncFactProgress (117 lines)
- [ ] Batch: 18 DeltaSync Dimension SPs -> DLT

### Week 3: Assignments + Support
- [ ] 9 DeltaSync Assignment SPs -> DLT
- [ ] 2 Additional DeltaSync Fact SPs
- [ ] 8 Calculate Assignment SPs
- [ ] 5 WorkersHistory Support SPs

### Week 4: Cleanup + Validation
- [ ] 2 Other Calculate SPs (inline)
- [ ] Integration testing all notebooks
- [ ] Update MIGRATION_STATUS.md
- [ ] Documentation and handoff

---

## Appendix A: Quick Start - Autonomous Mode

### Option 1: Start Full Autonomous Loop (Recommended)

```bash
# Navigate to project
cd /c/Users/khaledadmin/lakebridge

# Start the autonomous Ralph Loop
# This will process ALL 51 pending SPs using the ADW workflow
/ralph-loop
```

The loop will:
1. Read `specs/ralph-loop-sp-conversion-plan.md` to find the next PENDING SP
2. Run `ADW_SP_plan_build_review_fix` skill for that SP
3. Update the tracking table with the result
4. Commit changes and continue to the next SP
5. Stop when all SPs are processed or completion criteria met

### Option 2: Convert Single SP Manually

```bash
# Use the ADW skill directly for a single SP
Use the ADW_SP_plan_build_review_fix skill to migrate stg.spDeltaSyncFactWorkersHistory
```

### Option 3: Convert Multiple SPs in Batch

```bash
# Start Ralph Loop with specific SPs
/ralph-loop "
Convert the following stored procedures in order:
1. stg.spDeltaSyncFactWorkersHistory
2. stg.spDeltaSyncFactObservations
3. stg.spDeltaSyncFactProgress

Use the ADW_SP_plan_build_review_fix skill for each.
Update specs/ralph-loop-sp-conversion-plan.md tracking table after each.

Output <promise>BATCH_COMPLETE</promise> when all 3 are done.
" --max-iterations 30
```

### Monitoring Progress

```bash
# Check tracking table status
grep -E "PENDING|PASS|FAIL|BLOCKED" specs/ralph-loop-sp-conversion-plan.md | head -20

# View recent commits
git log --oneline -10

# Check ADW session files
ls -la adw_sessions/
```

### Canceling the Loop

```bash
# Cancel if stuck or need to pause
/cancel-ralph
```

---

## Appendix B: SP Inventory Verification

Total count verification:
- Already Converted: 5
- Gold Facts: 7
- DeltaSync Dimensions: 18
- DeltaSync Assignments: 9
- DeltaSync Facts (Additional): 2
- Calculate Assignments: 8
- WorkersHistory Support: 5
- Other Calculate: 2
- Admin/Maintenance: 10
- Dev/Test: 4

**Total: 5 + 7 + 18 + 9 + 2 + 8 + 5 + 2 + 10 + 4 = 70** ✓

**To Convert: 7 + 18 + 9 + 2 + 8 + 5 + 2 = 51** ✓

**Skip: 10 + 4 = 14** ✓

---

## Appendix C: ADW Workflow Reference

The ADW_SP_plan_build_review_fix skill orchestrates 4 phases:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│      PLAN       │ --> │      BUILD      │ --> │     REVIEW      │ --> │      FIX        │
│                 │     │                 │     │                 │     │                 │
│ - Read source   │     │ - Convert T-SQL │     │ - Analyze diff  │     │ - Fix blockers  │
│ - Analyze       │     │ - Generate      │     │ - Validate      │     │ - Fix high risk │
│   patterns      │     │   notebook      │     │   logic         │     │ - Re-validate   │
│ - Create plan   │     │ - Deploy        │     │ - Issue         │     │                 │
│                 │     │                 │     │   PASS/FAIL     │     │ (conditional)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │                       │
        v                       v                       v                       v
  specs/sp-*.md          pipelines/*/        app_review/          app_fix_reports/
```

**Session Tracking:** `adw_sessions/sp_{sp_name}_{workflow_id}.json`

---

*Plan Version: 3.0 | Last Updated: 2026-01-26*
*Updated for autonomous Ralph Loop + ADW workflow integration*
*Added SP Master Tracking Table with 70 SPs categorized*
