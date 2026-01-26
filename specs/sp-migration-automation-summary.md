# Stored Procedure Migration Automation - Summary

**Document Version:** 1.0
**Created:** 2026-01-25
**Related Documents:**
- `specs/lakebridge-review-enhancements.md`
- `specs/lakebridge-fix-enhancements.md`

---

## Objective

Fully automate the migration of remaining SQL Server stored procedures to Databricks notebooks, similar to the existing conversion of `stg.spCalculateFactWorkersShifts` → `calc_fact_workers_shifts.py`.

---

## Current State

### Already Converted (5 procedures)

| Procedure | Lines | Target Notebook | Status |
|-----------|-------|-----------------|--------|
| `stg.spCalculateFactWorkersShifts` | 1639 | `calc_fact_workers_shifts.py` | DEPLOYED |
| `stg.spCalculateFactWorkersShiftsCombined` | 1287 | `calc_fact_workers_shifts_combined.py` | DEPLOYED |
| `stg.spCalculateFactWorkersContacts_ByRule` | 951 | `calc_worker_contacts.py` | DEPLOYED |
| `mrg.spMergeOldData` | 903 | `merge_old_data.py` | DEPLOYED |
| `stg.spWorkersHistory_UpdateAssignments_3_LocationClass` | 553 | `update_workers_history_location_class.py` | DEPLOYED |

### Remaining High-Priority (10+ procedures)

| Procedure | Lines | Patterns | Priority |
|-----------|-------|----------|----------|
| `stg.spDeltaSyncFactWorkersHistory` | 1561 | TEMP, SPATIAL, BATCH | HIGH |
| `stg.spDeltaSyncFactObservations` | 1165 | TEMP, MERGE, WINDOW | HIGH |
| `stg.spDeltaSyncFactWeatherObservations` | 481 | TEMP, MERGE, WINDOW | HIGH |
| `stg.spCalculateManagerAssignmentSnapshots` | 527 | TEMP, MERGE, PIVOT | MEDIUM |
| `stg.spCalculateWorkerLocationAssignments` | 575 | TEMP, MERGE, WINDOW | MEDIUM |
| `stg.spDeltaSyncDimFloor` | 271 | MERGE, SPATIAL | MEDIUM |
| `stg.spDeltaSyncDimZone` | 321 | MERGE, SPATIAL | MEDIUM |
| `stg.spDeltaSyncDimWorkshiftDetails` | 331 | TEMP, MERGE, WINDOW | MEDIUM |
| `stg.spCalculateCrewAssignments` | ~200 | MERGE | LOW |
| `stg.spCalculateDeviceAssignments` | ~200 | MERGE | LOW |

---

## Enhancement Summary

### Review Skill Enhancements (7 Requirements)

| ID | Requirement | Purpose | Priority |
|----|-------------|---------|----------|
| REQ-R1 | SP Pattern Detection | Detect CURSOR, MERGE, SPATIAL, etc. in source | HIGH |
| REQ-R2 | Conversion Completeness Validator | Verify all patterns converted | HIGH |
| REQ-R3 | Business Logic Comparator | Compare source→target logic | HIGH |
| REQ-R4 | Dependency Tracker | Track table/proc/UDF dependencies | MEDIUM |
| REQ-R5 | SP-Specific Checklist Generator | Generate review checklists | MEDIUM |
| REQ-R6 | Data Type Validation | Validate T-SQL→Spark mappings | MEDIUM |
| REQ-R7 | Enhanced Report Format | SP-specific report sections | LOW |

### Fix Skill Enhancements (8 Requirements)

| ID | Requirement | Purpose | Priority |
|----|-------------|---------|----------|
| REQ-F1 | SP Converter Class | Main conversion orchestrator | HIGH |
| REQ-F2 | CURSOR to Window Converter | Convert CURSOR patterns | HIGH |
| REQ-F3 | MERGE Statement Generator | Generate Delta MERGE | HIGH |
| REQ-F4 | Temp Table Converter | Convert #temp to temp views | MEDIUM |
| REQ-F5 | Spatial Function Stub Generator | Generate H3/UDF stubs | MEDIUM |
| REQ-F6 | Dependency Resolver | Create missing dependencies | MEDIUM |
| REQ-F7 | Validation Notebook Generator | Generate test notebooks | MEDIUM |
| REQ-F8 | Complete Notebook Generator | Assemble full notebooks | HIGH |

---

## Implementation Phases

### Phase 1: Core Pattern Conversion (Week 1)

**Goal:** Enable basic SP conversion with pattern detection

| Task | Files | Effort |
|------|-------|--------|
| Implement REQ-R1 (Pattern Detection) | `analyzer.py` | 4 hours |
| Implement REQ-F1 (SP Converter) | `sp_converter.py` | 6 hours |
| Implement REQ-F2 (CURSOR Converter) | `cursor_converter.py` | 8 hours |
| Implement REQ-F3 (MERGE Generator) | `merge_converter.py` | 6 hours |
| Unit tests | `tests/` | 4 hours |

**Deliverable:** Convert 1 procedure end-to-end as proof of concept

### Phase 2: Complete Conversion Pipeline (Week 2)

**Goal:** Full conversion capability for all pattern types

| Task | Files | Effort |
|------|-------|--------|
| Implement REQ-F4 (Temp Tables) | `temp_converter.py` | 4 hours |
| Implement REQ-F5 (Spatial Stubs) | `spatial_converter.py` | 4 hours |
| Implement REQ-F8 (Notebook Generator) | `sp_converter.py` | 6 hours |
| Implement REQ-R2 (Completeness Validator) | `analyzer.py` | 4 hours |
| Integration tests | `tests/` | 4 hours |

**Deliverable:** Convert 3 more procedures automatically

### Phase 3: Validation & Dependencies (Week 3)

**Goal:** Automated validation and dependency management

| Task | Files | Effort |
|------|-------|--------|
| Implement REQ-R3 (Business Logic) | `analyzer_business_logic.py` | 6 hours |
| Implement REQ-R4 (Dependency Tracker) | `analyzer_dependencies.py` | 6 hours |
| Implement REQ-F6 (Dependency Resolver) | `dependency_resolver.py` | 4 hours |
| Implement REQ-F7 (Validation Generator) | `validation_generator.py` | 6 hours |
| End-to-end tests | `tests/` | 4 hours |

**Deliverable:** Full automation pipeline with validation

### Phase 4: Polish & Documentation (Week 4)

**Goal:** Production-ready with documentation

| Task | Files | Effort |
|------|-------|--------|
| Implement REQ-R5 (Checklists) | `analyzer.py` | 4 hours |
| Implement REQ-R6 (Data Types) | `analyzer.py` | 3 hours |
| Implement REQ-R7 (Report Format) | `analyzer.py` | 3 hours |
| Update SKILL.md files | `SKILL.md` | 4 hours |
| Documentation | `docs/` | 4 hours |

**Deliverable:** Complete, documented automation framework

---

## File Structure After Enhancement

```
.claude/skills/
├── lakebridge-review/
│   ├── SKILL.md                      # Updated with SP workflow
│   ├── resources/
│   │   └── review-checklists.md      # Updated with SP checklists
│   └── scripts/
│       ├── __init__.py
│       ├── analyzer.py               # Enhanced with SP patterns
│       ├── analyzer_business_logic.py  # NEW
│       └── analyzer_dependencies.py    # NEW
│
├── lakebridge-fix/
│   ├── SKILL.md                      # Updated with SP workflow
│   └── scripts/
│       ├── __init__.py
│       ├── fixer.py                  # Enhanced with SP integration
│       ├── sp_converter.py           # NEW - Main orchestrator
│       ├── cursor_converter.py       # NEW
│       ├── merge_converter.py        # NEW
│       ├── temp_converter.py         # NEW
│       ├── spatial_converter.py      # NEW
│       ├── dependency_resolver.py    # NEW
│       └── validation_generator.py   # NEW
│
└── shared/
    └── scripts/
        ├── credentials.py
        ├── databricks_client.py
        ├── sqlserver_client.py
        └── transpiler.py             # Existing - may need updates
```

---

## Automated Workflow

After enhancements, the migration workflow becomes:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    AUTOMATED SP MIGRATION                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. USER: "Migrate stg.spDeltaSyncFactWorkersHistory"               │
│                                                                      │
│  2. REVIEW SKILL (automatic):                                        │
│     • Detect patterns: TEMP_TABLE, SPATIAL, BATCH                   │
│     • Calculate complexity: 8/10                                    │
│     • Extract dependencies: 15 tables, 2 functions                  │
│     • Generate checklist                                            │
│     • Output: review_report.md                                      │
│                                                                      │
│  3. FIX SKILL (automatic):                                          │
│     • Resolve dependencies: Check 15 tables exist                   │
│     • Convert TEMP_TABLE → createOrReplaceTempView                  │
│     • Convert SPATIAL → H3 stubs                                    │
│     • Convert BATCH → parameterized DataFrame                       │
│     • Generate complete notebook                                    │
│     • Generate validation notebook                                  │
│     • Output: notebooks/delta_sync_fact_workers_history.py          │
│               notebooks/validate_fact_workers_history.py            │
│                                                                      │
│  4. REVIEW SKILL (automatic):                                        │
│     • Validate conversion completeness                              │
│     • Check all patterns addressed                                  │
│     • Verify dependencies resolved                                  │
│     • Output: conversion_report.md                                  │
│                                                                      │
│  5. BUILD SKILL (manual trigger):                                    │
│     • Deploy notebook to Databricks                                 │
│     • Create job if needed                                          │
│     • Run validation notebook                                       │
│     • Output: deployment_report.md                                  │
│                                                                      │
│  6. RESULT: Converted procedure ready for production                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Conversion Automation Rate | >80% | % of code auto-generated vs manual |
| Pattern Detection Accuracy | >95% | % of patterns correctly identified |
| Conversion Completeness | 100% | All detected patterns have conversions |
| Validation Coverage | 100% | All conversions have validation notebooks |
| Time to Convert | <2 hours | Average time per procedure (vs 8+ hours manual) |
| Deployment Success | >90% | First-run success rate |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Complex CURSOR patterns may not convert | Flag for manual review, provide partial conversion |
| Spatial accuracy requirements | H3 resolution configurable, Shapely fallback |
| Missing dependencies | Dependency resolver creates placeholders |
| Business logic drift | Validation notebook catches discrepancies |
| Large table performance | Partition by date, add widgets for scope |

---

## Next Steps

1. **Review and approve** this specification
2. **Create feature branch** for enhancement work
3. **Implement Phase 1** (Core Pattern Conversion)
4. **Proof of concept** with `stg.spDeltaSyncFactWorkersHistory`
5. **Iterate** based on results

---

## Appendix: Procedure Conversion Priority Matrix

| Procedure | Lines | Complexity | Dependencies | Business Impact | Priority Score |
|-----------|-------|------------|--------------|-----------------|----------------|
| `spDeltaSyncFactWorkersHistory` | 1561 | HIGH | 15 tables | Core fact table | **10** |
| `spDeltaSyncFactObservations` | 1165 | HIGH | 10 tables | Safety data | **9** |
| `spCalculateManagerAssignmentSnapshots` | 527 | MEDIUM | 8 tables | Reporting | **7** |
| `spCalculateWorkerLocationAssignments` | 575 | MEDIUM | 7 tables | Zone analytics | **7** |
| `spDeltaSyncDimFloor` | 271 | MEDIUM | 4 tables | Dimension | **6** |
| `spDeltaSyncDimZone` | 321 | MEDIUM | 4 tables | Dimension | **6** |
| `spDeltaSyncDimWorkshiftDetails` | 331 | MEDIUM | 5 tables | Dimension | **5** |
| `spCalculateCrewAssignments` | ~200 | LOW | 3 tables | Assignment | **4** |
| `spCalculateDeviceAssignments` | ~200 | LOW | 3 tables | Assignment | **4** |

*Priority Score: 1-10 based on complexity, dependencies, and business impact*

---

*Document maintained by: Migration Team*
*Last updated: 2026-01-25*
