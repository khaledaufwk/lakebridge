---
active: true
iteration: 1
max_iterations: 100
completion_promise: "ALL_SP_MIGRATIONS_COMPLETE"
started_at: "2026-01-26T12:00:00Z"
---

# Autonomous SP Migration using Ralph Loop + Claude Tasks

Migrate **all 51 pending stored procedures** to Databricks using Claude Tasks for tracking and the ADW workflow for conversion.

## WORKFLOW OVERVIEW

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    TASK-BASED AUTONOMOUS WORKFLOW                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ITERATION 1 (Bootstrap):                                               │
│  1. TaskList - Check if tasks already exist                             │
│  2. If no tasks, TaskCreate for each of 51 SPs (with dependencies)      │
│                                                                          │
│  EACH SUBSEQUENT ITERATION:                                              │
│  1. TaskList - Find pending tasks not blocked                           │
│  2. TaskGet - Get details of first available task                       │
│  3. TaskUpdate - Mark task as in_progress                               │
│  4. Run ADW_SP_plan_build_review_fix skill for the SP                   │
│  5. TaskUpdate - Mark task as completed (or leave in_progress if fail)  │
│  6. Update markdown tracking table for git persistence                  │
│  7. Commit changes                                                       │
│  8. Continue to next iteration                                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## PHASE 1: BOOTSTRAP TASKS (First Iteration Only)

On the FIRST iteration, check if tasks exist. If not, create them:

### Gold Facts (Priority 1) - 7 Tasks
```
TaskCreate: "Convert stg.spDeltaSyncFactWorkersHistory"
  description: "782 lines | TEMP_TABLE, SPATIAL, WINDOW, CTE | Target: gold_fact_workers_history"
  activeForm: "Converting spDeltaSyncFactWorkersHistory"
  metadata: {category: "gold_facts", lines: 782, priority: 1}

TaskCreate: "Convert stg.spDeltaSyncFactObservations"
  description: "584 lines | TEMP_TABLE, MERGE, WINDOW, CTE | Target: gold_fact_observations"
  activeForm: "Converting spDeltaSyncFactObservations"
  metadata: {category: "gold_facts", lines: 584, priority: 2}

TaskCreate: "Convert stg.spCalculateWorkerLocationAssignments"
  description: "289 lines | TEMP_TABLE, MERGE, WINDOW, CTE | Target: gold_worker_location_assignments"
  activeForm: "Converting spCalculateWorkerLocationAssignments"
  metadata: {category: "gold_facts", lines: 289, priority: 3}

TaskCreate: "Convert stg.spCalculateManagerAssignmentSnapshots"
  description: "265 lines | TEMP_TABLE, MERGE, PIVOT, CTE | Target: gold_manager_snapshots"
  activeForm: "Converting spCalculateManagerAssignmentSnapshots"
  metadata: {category: "gold_facts", lines: 265, priority: 4}

TaskCreate: "Convert stg.spDeltaSyncFactWeatherObservations"
  description: "242 lines | TEMP_TABLE, MERGE, WINDOW, CTE | Target: gold_fact_weather_observations"
  activeForm: "Converting spDeltaSyncFactWeatherObservations"
  metadata: {category: "gold_facts", lines: 242, priority: 5}

TaskCreate: "Convert stg.spCalculateFactReportedAttendance"
  description: "215 lines | TEMP_TABLE, MERGE | Target: gold_fact_reported_attendance"
  activeForm: "Converting spCalculateFactReportedAttendance"
  metadata: {category: "gold_facts", lines: 215, priority: 6}

TaskCreate: "Convert stg.spDeltaSyncFactProgress"
  description: "117 lines | MERGE, WINDOW | Target: gold_fact_progress"
  activeForm: "Converting spDeltaSyncFactProgress"
  metadata: {category: "gold_facts", lines: 117, priority: 7}
```

### DeltaSync Dimensions (Priority 2) - 18 Tasks
Create tasks for: spDeltaSyncDimWorker, spDeltaSyncDimTask, spDeltaSyncDimProject,
spDeltaSyncDimWorkshiftDetails, spDeltaSyncDimZone (SPATIAL), spDeltaSyncDimActivity,
spDeltaSyncDimCrew, spDeltaSyncDimDevice, spDeltaSyncDimFloor (SPATIAL), spDeltaSyncDimTrade,
spDeltaSyncDimCompany, spDeltaSyncDimWorkerStatus, spDeltaSyncDimTitle, spDeltaSyncDimDepartment,
spDeltaSyncDimLocationGroup, spDeltaSyncDimWorkshift, spDeltaSyncDimOrganization,
spDeltaSyncDimObservationSource

### DeltaSync Assignments (Priority 3) - 9 Tasks
Create tasks for: spDeltaSyncLocationGroupAssignments, spDeltaSyncPermissions,
spDeltaSyncWorkerLocationAssignments, spDeltaSyncManagerAssignments, spDeltaSyncDeviceAssignments,
spDeltaSyncCrewAssignments, spDeltaSyncCrewManagerAssignments, spDeltaSyncWorkshiftAssignments,
spDeltaSyncWorkerStatusAssignments

### DeltaSync Facts Additional (Priority 4) - 2 Tasks
Create tasks for: spDeltaSyncFactReportedAttendance_ResourceTimesheet,
spDeltaSyncFactReportedAttendance_ResourceHours

### Calculate Assignments (Priority 5) - 8 Tasks
Create tasks for: spCalculateLocationGroupAssignments, spCalculateManagerAssignments,
spCalculateDeviceAssignments, spCalculateTradeAssignments, spCalculateCrewAssignments,
spCalculateWorkshiftAssignments, spCalculateFactProgress, spCalculateContactTracingRule_Manager

### WorkersHistory Support (Priority 6) - 5 Tasks
Create tasks for: spWorkersHistory_UpdateAssignments_2_WorkShiftDates,
spCalculateFactWorkersShifts_FixOverlaps, spWorkersHistory_UpdateAssignments_1_Crews,
spWorkersHistory_FixOverlaps, spWorkersHistory_UpdateAssignments

### Other Calculate (Priority 7) - 2 Tasks
Create tasks for: spCalculateProjectActiveFlag, spCalculateFactWorkersShifts_Partial

## PHASE 2: PROCESS TASKS (Each Iteration)

### Step 1: Find Next Task
```
TaskList → Find first task with status='pending' (no blockedBy or all blockedBy completed)
```

### Step 2: Start Task
```
TaskUpdate(taskId, status='in_progress')
```

### Step 3: Convert SP
```
Use the ADW_SP_plan_build_review_fix skill to migrate {sp_name from task subject}
```

The ADW skill runs: Plan → Build → Review → Fix (if needed)

### Step 4: Complete Task
On SUCCESS:
```
TaskUpdate(taskId, status='completed')
```

On FAILURE (after 3 attempts):
```
TaskUpdate(taskId, description='BLOCKED: {reason}')
# Leave as in_progress or pending for manual intervention
```

### Step 5: Update Markdown Table
Edit `specs/ralph-loop-sp-conversion-plan.md` to update the tracking table:
- Change Status from `PENDING` to `PASS`, `FAIL`, or `BLOCKED`
- Add notes about any issues

### Step 6: Commit
```bash
git add .
git commit -m "SP migration: {sp_name} - {PASS|FAIL|BLOCKED}"
```

### Step 7: Continue
Loop back to Step 1 for next iteration.

## COMPLETION CRITERIA

Output `<promise>ALL_SP_MIGRATIONS_COMPLETE</promise>` when:
- TaskList shows 0 pending tasks
- At least 45/51 tasks completed (88% success rate)
- Any incomplete tasks documented with reasons

## TASK DEPENDENCY RULES

### Independent Tasks (can run in any order within priority group):
- All gold_facts tasks
- All deltasync_dims tasks
- All deltasync_assign tasks
- All calc_assign tasks

### Dependent Tasks:
- workers_history tasks may depend on gold_facts completion
- other_calc tasks are simple and can run anytime

## REFERENCE FILES

| Resource | Path |
|----------|------|
| Master Plan | `specs/ralph-loop-sp-conversion-plan.md` |
| Source SPs | `migration_project/source_sql/stored_procedures/` |
| Converted Notebooks | `migration_project/pipelines/gold/notebooks/` |
| DLT Definitions | `migration_project/pipelines/dlt/` |
| Validation Pattern | `migration_project/validate_gold_factworkerhistory.py` |

## ESCAPE HATCHES

### If task stuck (>3 ADW attempts):
1. Update task description with failure reason
2. Move to next pending task
3. Continue workflow

### If infrastructure issues:
1. Document the error in task description
2. Pause and ask user (don't burn iterations)

### If all tasks blocked:
1. TaskList to show status summary
2. Report blockers to user
3. Output completion promise (partial success)

## PROGRESS TRACKING

At start of each iteration, run:
```
TaskList → Report: "Progress: {completed}/{total} tasks ({percent}%)"
```

The visual task list in Claude Code UI will show:
- ✓ Completed tasks (green)
- ⟳ In-progress tasks (spinner with activeForm text)
- ○ Pending tasks (gray)

## BENEFITS OF TASK-BASED APPROACH

1. **Visual Progress**: See spinner with "Converting spDeltaSyncFactWorkersHistory"
2. **Dependency Tracking**: Tasks blocked until dependencies complete
3. **Resume Capability**: TaskList shows exactly where we left off
4. **Parallel Awareness**: Easy to see what can run next
5. **Native Integration**: Works with Claude Code's built-in task UI
