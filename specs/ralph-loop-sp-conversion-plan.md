# Stored Procedure Conversion Plan using Ralph Loop

**Created:** 2026-01-26
**Project:** WakeCapDW Migration to Databricks
**Methodology:** Iterative AI-Assisted Conversion using Ralph Loop

---

## Executive Summary

This plan outlines the systematic approach for converting remaining SQL Server stored procedures to Databricks notebooks/DLT using the **Ralph Loop** plugin. The approach leverages iterative refinement with automatic verification (test execution, linting) to ensure high-quality conversions.

---

## 1. Current State Analysis

### 1.1 Already Converted (5 Complex SPs)

| Notebook | Source SP | Lines | Status |
|----------|-----------|-------|--------|
| `calc_fact_workers_shifts.py` | stg.spCalculateFactWorkersShifts | 821 | Converted |
| `calc_fact_workers_shifts_combined.py` | stg.spCalculateFactWorkersShiftsCombined | 645 | Converted |
| `calc_worker_contacts.py` | stg.spCalculateFactWorkersContacts_ByRule | 477 | Converted |
| `merge_old_data.py` | mrg.spMergeOldData | 453 | Converted |
| `update_workers_history_location_class.py` | stg.spWorkersHistory_UpdateAssignments_3_LocationClass | 553 | Converted |

### 1.2 Remaining Gold Layer Fact SPs (Priority Order by Complexity)

| Priority | Stored Procedure | Lines | Complexity | Patterns | Target |
|----------|-----------------|-------|------------|----------|--------|
| **1** | stg.spDeltaSyncFactWorkersHistory | 782 | 9/10 | TEMP_TABLE, SPATIAL, WINDOW, CTE | gold_fact_workers_history |
| **2** | stg.spDeltaSyncFactObservations | 584 | 7/10 | TEMP_TABLE, MERGE, WINDOW | gold_fact_observations |
| **3** | stg.spCalculateWorkerLocationAssignments | 289 | 6/10 | TEMP_TABLE, MERGE, WINDOW | gold_worker_location_assignments |
| **4** | stg.spDeltaSyncFactWeatherObservations | 242 | 6/10 | TEMP_TABLE, MERGE, WINDOW | gold_fact_weather_observations |
| **5** | stg.spCalculateFactReportedAttendance | 215 | 5/10 | TEMP_TABLE, MERGE | gold_fact_reported_attendance |
| **6** | stg.spDeltaSyncFactProgress | 180 | 5/10 | MERGE, WINDOW | gold_fact_progress |
| **7** | stg.spCalculateManagerAssignmentSnapshots | 265 | 5/10 | TEMP_TABLE, MERGE, PIVOT | gold_manager_snapshots |

### 1.3 Supporting Assignment SPs (Medium Priority)

| Stored Procedure | Lines | Complexity | Target |
|-----------------|-------|------------|--------|
| stg.spDeltaSyncLocationGroupAssignments | 201 | 4/10 | silver/gold assignments |
| stg.spWorkersHistory_UpdateAssignments_2_WorkShiftDates | 276 | 5/10 | Supporting logic |
| stg.spCalculateFactWorkersShifts_FixOverlaps | 254 | 5/10 | Data quality fix |

---

## 2. Ralph Loop Methodology

### 2.1 Core Philosophy

Ralph Loop implements **iterative, self-referential AI development**:

1. **Same prompt** fed across iterations
2. **Previous work persists** in files and git history
3. **Claude reads its own output** from prior iterations
4. **Automatic verification** (tests, validation queries) gates completion

### 2.2 When to Use Ralph Loop

| Use Case | Ralph Loop? | Reason |
|----------|-------------|--------|
| Complex SP with CURSOR/DYNAMIC_SQL | Yes | Requires multiple refinement passes |
| SP with spatial/geometry logic | Yes | H3/Shapely integration needs iteration |
| SP with 300+ lines | Yes | Too complex for single-shot |
| Simple MERGE-only SP (<150 lines) | No | Single conversion sufficient |
| Dimension sync (DeltaSync*) | Maybe | Use if validation fails first attempt |

### 2.3 Key Best Practices

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

## 3. Conversion Template

### 3.1 Standard Notebook Structure

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
# MAGIC - CURSOR → Spark parallel processing
# MAGIC - TEMP TABLE → DataFrame intermediate results
# MAGIC - Complex CTEs → Step-by-step DataFrame transformations
# MAGIC - MERGE → Delta MERGE operation
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

### 3.2 Pattern Conversion Reference

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

---

## 4. Ralph Loop Prompts for Each SP

### 4.1 Template Prompt Structure

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

### 4.2 Specific Prompts by Priority

#### Priority 1: stg.spDeltaSyncFactWorkersHistory (782 lines)

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

#### Priority 2: stg.spDeltaSyncFactObservations (584 lines)

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

#### Priority 3: stg.spCalculateWorkerLocationAssignments (289 lines)

```bash
/ralph-loop "
Convert SQL Server stored procedure to Databricks notebook.

## Source
- Procedure: stg.spCalculateWorkerLocationAssignments
- Location: migration_project/source_sql/stored_procedures/stg.spCalculateWorkerLocationAssignments.sql
- Lines: 289
- Patterns: TEMP_TABLE, MERGE, WINDOW_FUNCTION

## Target
- Output: migration_project/pipelines/gold/notebooks/gold_worker_location_assignments.py
- Target Table: wakecap_prod.gold.gold_worker_location_assignments

## Key Logic
1. Calculate worker-to-location assignments over time
2. Handle assignment gaps and overlaps
3. Track effective dates (ValidFrom, ValidTo)

## Completion Criteria
Output <promise>CONVERSION_COMPLETE</promise> when implemented and validated.
" --max-iterations 20 --completion-promise "CONVERSION_COMPLETE"
```

#### Priority 4-7: Similar Pattern (Adjust per SP)

```bash
# Template for remaining SPs
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

---

## 5. Verification & Testing Strategy

### 5.1 Automatic Verification (In Ralph Loop)

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

### 5.2 Manual Verification Checklist

After Ralph Loop completes:

- [ ] Review generated notebook for business logic accuracy
- [ ] Compare key aggregations with SQL Server source
- [ ] Verify NULL handling matches original behavior
- [ ] Test with date range filter
- [ ] Run on subset before full table
- [ ] Check Delta table properties (partitioning, Z-ORDER)

---

## 6. Workflow for Future SP Conversions

### 6.1 Standard Workflow

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SP CONVERSION WORKFLOW                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. ASSESS                                                           │
│     ├── Read source SP                                               │
│     ├── Identify patterns (CURSOR, TEMP_TABLE, SPATIAL, etc.)       │
│     ├── Count lines, estimate complexity (1-10)                     │
│     └── Determine target (DLT table vs notebook)                    │
│                                                                      │
│  2. PREPARE                                                          │
│     ├── Identify source Silver tables                               │
│     ├── Identify target Gold table schema                           │
│     ├── Document key business logic                                 │
│     └── Create Ralph Loop prompt                                    │
│                                                                      │
│  3. CONVERT (Ralph Loop)                                             │
│     ├── /ralph-loop "{prompt}" --max-iterations N                   │
│     ├── Claude iteratively builds notebook                          │
│     ├── Each iteration: read/write/validate                         │
│     └── Exits on <promise>CONVERSION_COMPLETE</promise>             │
│                                                                      │
│  4. REVIEW                                                           │
│     ├── Human review of generated notebook                          │
│     ├── Compare with source SP logic                                │
│     ├── Run /lakebridge-review for automated analysis               │
│     └── Fix any issues with /lakebridge-fix                         │
│                                                                      │
│  5. TEST                                                             │
│     ├── Run notebook on Databricks cluster                          │
│     ├── Validate row counts                                         │
│     ├── Compare key aggregations                                    │
│     └── Performance test on full data                               │
│                                                                      │
│  6. DEPLOY                                                           │
│     ├── Deploy notebook to Databricks workspace                     │
│     ├── Create/update job schedule                                  │
│     └── Update MIGRATION_STATUS.md                                  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 6.2 Ralph Loop Command Reference

```bash
# Start conversion loop
/ralph-loop "<prompt>" --max-iterations 30 --completion-promise "CONVERSION_COMPLETE"

# Cancel if stuck
/cancel-ralph

# Check progress (in another terminal)
git diff --stat
```

### 6.3 Complexity-Based Iteration Limits

| Complexity Score | Lines | Max Iterations | Notes |
|-----------------|-------|----------------|-------|
| 1-3 | <150 | 10 | Simple, may not need Ralph Loop |
| 4-5 | 150-300 | 15 | Standard conversion |
| 6-7 | 300-500 | 20 | Complex, needs multiple passes |
| 8-9 | 500-800 | 30 | Very complex, expect refinement |
| 10 | 800+ | 40 | Break into multiple notebooks |

---

## 7. Files & Resources

### 7.1 Reference Files

| Resource | Path | Purpose |
|----------|------|---------|
| Existing Conversion | `migration_project/pipelines/notebooks/calc_fact_workers_shifts.py` | Pattern reference |
| Source SPs | `migration_project/source_sql/stored_procedures/` | Source code |
| Silver Tables | `migration_project/pipelines/silver/config/silver_tables.yml` | Source schema |
| Gold Config | `migration_project/pipelines/gold/config/gold_tables.yml` | Target schema |
| Databricks Expert | `.claude/commands/experts/databricks.md` | Platform patterns |

### 7.2 Output Locations

| Artifact | Path |
|----------|------|
| Converted Notebooks | `migration_project/pipelines/gold/notebooks/` |
| DLT Definitions | `migration_project/pipelines/dlt/` |
| Validation Scripts | `migration_project/pipelines/validation/` |

---

## 8. Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Conversion Accuracy | 99%+ | Row count match source vs target |
| Business Logic Fidelity | 100% | Manual review of key calculations |
| Iteration Efficiency | <20 avg | Iterations per SP conversion |
| Time per SP | <2 hours | Wall clock for complex SPs |

---

## 9. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Ralph Loop infinite iteration | Always use `--max-iterations` |
| Lost context between iterations | Commit work frequently, use git history |
| Complex spatial logic | Use H3 library, reference spatial_udfs.py |
| Performance issues on large tables | Implement partitioning, test on subset first |
| Missing Silver source tables | Verify Silver layer complete before Gold conversion |

---

## 10. Next Steps

1. **Immediate:** Start with Priority 1 (stg.spDeltaSyncFactWorkersHistory)
2. **Week 1:** Complete Priorities 1-3
3. **Week 2:** Complete Priorities 4-7
4. **Week 3:** Supporting assignment SPs
5. **Ongoing:** Document patterns, improve prompts based on learnings

---

## Appendix A: Quick Start

```bash
# 1. Navigate to project
cd /c/Users/khaledadmin/lakebridge

# 2. Start Ralph Loop for Priority 1
/ralph-loop "
Convert stg.spDeltaSyncFactWorkersHistory (782 lines) to Databricks notebook.

Source: migration_project/source_sql/stored_procedures/stg.spDeltaSyncFactWorkersHistory.sql
Target: migration_project/pipelines/gold/notebooks/gold_fact_workers_history_full.py
Patterns: TEMP_TABLE, SPATIAL, WINDOW_FUNCTION, CTE

Key tasks:
1. Read and understand source SP
2. Create notebook with proper structure
3. Implement all transformation logic
4. Add Delta MERGE operation
5. Add validation query

Reference: migration_project/pipelines/notebooks/calc_fact_workers_shifts.py

Output <promise>CONVERSION_COMPLETE</promise> when done.
" --max-iterations 30 --completion-promise "CONVERSION_COMPLETE"
```

---

*Plan Version: 1.0 | Last Updated: 2026-01-26*
