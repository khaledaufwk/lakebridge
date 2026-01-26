---
name: ADW_SP_plan_build_review_fix
description: Agentic Developer Workflow for end-to-end stored procedure migration from Azure SQL Server to Databricks using Plan, Build, Review, and Fix subagents
---

# ADW: Stored Procedure Migration Workflow

## Purpose

This Agentic Developer Workflow (ADW) orchestrates the complete migration of a stored procedure from Azure SQL Server to Databricks. It coordinates four specialized subagents in sequence:

1. **Plan** - Analyzes the SP and creates a detailed conversion plan
2. **Build** - Implements the conversion following the plan
3. **Review** - Validates the conversion and identifies issues
4. **Fix** - Addresses any issues found in review (conditional)

## Usage

Invoke this ADW to migrate a stored procedure:
- "Use the ADW_SP_plan_build_review_fix skill to migrate stg.spCalculateFactWorkersShifts"
- "Use the ADW_SP_plan_build_review_fix skill to convert mrg.spMergeOldData"

## Variables

SP_NAME: $ARGUMENTS
WORKFLOW_OUTPUT_DIR: `adw_sessions/`
PLAN_OUTPUT_DIR: `specs/`
REVIEW_OUTPUT_DIR: `app_review/`
FIX_OUTPUT_DIR: `app_fix_reports/`

## Prerequisites

Before running this ADW, ensure:

1. **Credentials configured** at `~/.databricks/labs/lakebridge/.credentials.yml`:
   - SQL Server connection (server, database, user, password)
   - Databricks connection (host, token, catalog, schema)

2. **Source SQL files** available at `migration_project/source_sql/` (will be extracted if not present)

3. **Target infrastructure** ready:
   - Databricks workspace accessible
   - Unity Catalog configured
   - Secret scope created

## Instructions

- **CRITICAL**: You are the workflow ORCHESTRATOR. Your job is to coordinate subagents, NOT to do the work directly.
- If no `SP_NAME` is provided, STOP immediately and ask the user to provide the stored procedure name.
- Use the Task tool to spawn each subagent in sequence.
- Track session state using the WorkflowOrchestrator class.
- Handle failures gracefully - report which phase failed and why.

## Workflow Execution

### Phase 0: Initialization

Before spawning subagents, perform these checks:

```python
from scripts.sp_orchestrator import SPMigrationOrchestrator

# Initialize orchestrator
orchestrator = SPMigrationOrchestrator(sp_name=SP_NAME)

# Validate prerequisites
validation = orchestrator.validate_prerequisites()
if not validation["success"]:
    # Report missing prerequisites and STOP
    print(validation["message"])
    return

# Extract SP definition from SQL Server if not already cached
sp_definition = orchestrator.get_sp_definition()
if not sp_definition:
    # Report extraction failure and STOP
    return
```

### Phase 1: Plan

Spawn the Plan subagent using the Task tool:

```
Task(
    subagent_type="Plan",
    prompt=orchestrator.generate_plan_prompt(),
    model="opus"
)
```

The Plan subagent will:
1. Read the SP source code from `migration_project/source_sql/{schema}.{sp_name}.sql`
2. Analyze complexity patterns (CURSOR, TEMP_TABLE, MERGE, SPATIAL, DYNAMIC_SQL)
3. Identify input/output tables and dependencies
4. Design the conversion approach (DLT table, Python notebook, or hybrid)
5. Create a detailed plan at `specs/sp-{sp_name_kebab}.md`

**Wait for completion.** Capture the plan file path from the result.

```python
orchestrator.complete_phase("plan", session_id=plan_session_id, artifact_path=plan_path)
```

If plan phase fails:
- Report the failure
- STOP workflow

### Phase 2: Build

After plan completes, spawn the Build subagent:

```
Task(
    subagent_type="general-purpose",
    prompt=orchestrator.generate_build_prompt(plan_path),
    model="opus"
)
```

The Build subagent will:
1. Read the plan from `specs/sp-{sp_name_kebab}.md`
2. Convert T-SQL patterns to Spark/Databricks equivalents
3. Generate the output file (DLT notebook or Python notebook)
4. Deploy to Databricks workspace
5. Run initial validation
6. Update `MIGRATION_STATUS.md`

**Wait for completion.** Capture the build session ID.

```python
orchestrator.complete_phase("build", session_id=build_session_id)
```

If build phase fails:
- Report the failure with details
- STOP workflow

### Phase 3: Review

After build completes, spawn the Review subagent:

```
Task(
    subagent_type="Explore",
    prompt=orchestrator.generate_review_prompt(plan_path),
    model="opus"
)
```

The Review subagent will:
1. Analyze git diffs to see what changed
2. Compare source T-SQL with generated Spark code
3. Validate business logic preservation
4. Check for missing dependencies
5. Verify data type mappings
6. Categorize issues by risk tier (BLOCKER, HIGH, MEDIUM, LOW)
7. Write report to `app_review/sp_review_{sp_name}_{timestamp}.md`
8. Provide PASS/FAIL verdict

**Wait for completion.** Parse the review result to extract:
- Review file path
- Verdict (PASS or FAIL)
- Issue counts by tier

```python
orchestrator.complete_phase("review", session_id=review_session_id, artifact_path=review_path)
orchestrator.set_review_verdict(verdict)  # "PASS" or "FAIL"
```

### Phase 4: Fix (Conditional)

If review verdict is FAIL, spawn the Fix subagent:

```python
if orchestrator.should_run_fix():
    Task(
        subagent_type="general-purpose",
        prompt=orchestrator.generate_fix_prompt(plan_path, review_path),
        model="opus"
    )
```

The Fix subagent will:
1. Read the review report from `app_review/sp_review_{sp_name}_{timestamp}.md`
2. Fix all BLOCKER issues (required)
3. Fix all HIGH RISK issues (required)
4. Fix MEDIUM RISK issues if time permits
5. Re-run validation commands
6. Write fix report to `app_fix_reports/sp_fix_{sp_name}_{timestamp}.md`

**Wait for completion.**

```python
orchestrator.complete_phase("fix", session_id=fix_session_id, artifact_path=fix_report_path)
```

If review verdict is PASS:
- Skip fix phase
- Mark as SKIPPED

### Phase 5: Finalization

After all phases complete:

```python
# Get final result
result = orchestrator.get_result()

# Save session for resume capability
orchestrator.save_session()

# Generate summary report
summary = orchestrator.format_summary()
```

## Session Tracking

All workflow state is persisted to `adw_sessions/sp_{sp_name}_{workflow_id}.json`:

```json
{
  "workflow_id": "abc12345",
  "sp_name": "stg.spCalculateFactWorkersShifts",
  "sp_definition_path": "migration_project/source_sql/stg.spCalculateFactWorkersShifts.sql",
  "phases": {
    "plan": {
      "status": "COMPLETED",
      "session_id": "plan-xyz",
      "artifact_path": "specs/sp-calculate-fact-workers-shifts.md",
      "started_at": "2026-01-25T10:00:00Z",
      "completed_at": "2026-01-25T10:15:00Z"
    },
    "build": {
      "status": "COMPLETED",
      "session_id": "build-abc",
      "started_at": "2026-01-25T10:15:00Z",
      "completed_at": "2026-01-25T10:45:00Z"
    },
    "review": {
      "status": "COMPLETED",
      "session_id": "review-def",
      "artifact_path": "app_review/sp_review_calculate_fact_workers_shifts_20260125.md",
      "verdict": "FAIL"
    },
    "fix": {
      "status": "COMPLETED",
      "session_id": "fix-ghi",
      "artifact_path": "app_fix_reports/sp_fix_calculate_fact_workers_shifts_20260125.md"
    }
  },
  "overall_status": "SUCCESS",
  "artifacts": {
    "source_sql": "migration_project/source_sql/stg.spCalculateFactWorkersShifts.sql",
    "plan": "specs/sp-calculate-fact-workers-shifts.md",
    "converted_notebook": "migration_project/pipelines/notebooks/calc_fact_workers_shifts.py",
    "review_report": "app_review/sp_review_calculate_fact_workers_shifts_20260125.md",
    "fix_report": "app_fix_reports/sp_fix_calculate_fact_workers_shifts_20260125.md"
  }
}
```

## Resume Capability

To resume a failed workflow:

```python
# Load existing session
orchestrator = SPMigrationOrchestrator.load_session("adw_sessions/sp_stg_spCalculateFactWorkersShifts_abc12345.json")

# Resume from last successful phase
if not orchestrator.phases["build"].completed:
    # Re-run build phase
    ...
```

## Workflow Diagram

```
                    ┌─────────────────────────────────────────┐
                    │         ADW_SP_plan_build_review_fix    │
                    │              (Orchestrator)             │
                    └─────────────┬───────────────────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Phase 0: Initialization   │
                    │  - Validate prerequisites   │
                    │  - Extract SP definition    │
                    │  - Create session           │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Phase 1: PLAN Subagent    │
                    │  - Analyze SP complexity    │
                    │  - Design conversion        │
                    │  - Create specs/*.md        │
                    └─────────────┬───────────────┘
                                  │
                         plan_path returned
                                  │
                    ┌─────────────▼───────────────┐
                    │   Phase 2: BUILD Subagent   │
                    │  - Implement conversion     │
                    │  - Deploy to Databricks     │
                    │  - Run validation           │
                    └─────────────┬───────────────┘
                                  │
                         code changes made
                                  │
                    ┌─────────────▼───────────────┐
                    │  Phase 3: REVIEW Subagent   │
                    │  - Analyze git diff         │
                    │  - Validate logic           │
                    │  - Create review report     │
                    │  - Issue PASS/FAIL verdict  │
                    └─────────────┬───────────────┘
                                  │
                        ┌─────────┴─────────┐
                        │                   │
                      PASS                FAIL
                        │                   │
                        ▼                   ▼
              ┌─────────────────┐  ┌─────────────────┐
              │  Skip Fix Phase │  │ Phase 4: FIX    │
              │  Report SUCCESS │  │ Subagent        │
              └─────────────────┘  │ - Fix blockers  │
                                   │ - Fix high risk │
                                   │ - Re-validate   │
                                   └────────┬────────┘
                                            │
                                            ▼
                              ┌─────────────────────────┐
                              │  Phase 5: Finalization  │
                              │  - Save session         │
                              │  - Generate summary     │
                              │  - Report completion    │
                              └─────────────────────────┘
```

## Report Format

After workflow completion, provide this summary:

```
Stored Procedure Migration Complete

SP Name: {SP_NAME}
Workflow ID: {workflow_id}

Phase Results:
- Plan: COMPLETED -> specs/sp-{name}.md
- Build: COMPLETED -> pipelines/notebooks/{notebook}.py
- Review: {PASS|FAIL} -> app_review/sp_review_{name}_{ts}.md
- Fix: {COMPLETED|SKIPPED} -> app_fix_reports/sp_fix_{name}_{ts}.md

Conversion Summary:
- Source Lines: {line_count}
- Patterns Detected: {patterns}
- Patterns Converted: {converted_count}
- Manual Review Items: {manual_count}

Artifacts Created:
- Plan: {plan_path}
- Notebook: {notebook_path}
- Review: {review_path}
- Fix Report: {fix_path if applicable}

Session File: adw_sessions/sp_{sp_name}_{workflow_id}.json
(Use this to resume if needed)

Overall Status: {SUCCESS | PARTIAL | FAILED}
```

## Error Handling

### Plan Phase Failure
- Check if SP exists in source database
- Verify SQL Server connectivity
- Check if SP definition was extracted correctly

### Build Phase Failure
- Review plan for completeness
- Check Databricks connectivity
- Verify secret scope and credentials

### Review Phase Failure
- Ensure build completed successfully
- Check for git diff access

### Fix Phase Failure
- Some issues may require manual intervention
- Report remaining blockers
- Provide guidance for manual fixes

## Examples

### Example 1: Simple Procedure (PASS on first review)
```
User: "Use the ADW_SP_plan_build_review_fix skill to migrate stg.spStageWorkers"

1. Plan: Analyzes 150-line SP with MERGE pattern
2. Build: Converts to DLT streaming table
3. Review: PASS (0 blockers, 1 medium risk)
4. Fix: SKIPPED

Status: SUCCESS in ~30 minutes
```

### Example 2: Complex Procedure (Requires fixes)
```
User: "Use the ADW_SP_plan_build_review_fix skill to migrate stg.spCalculateFactWorkersShifts"

1. Plan: Analyzes 1639-line SP with CURSOR, TEMP_TABLE patterns
2. Build: Converts cursors to window functions, creates Python notebook
3. Review: FAIL (2 blockers - missing dependency, spatial UDF not registered)
4. Fix: Resolves dependencies, registers UDF

Status: SUCCESS after fix phase
```

### Example 3: Blocked (Manual intervention needed)
```
User: "Use the ADW_SP_plan_build_review_fix skill to migrate mrg.spMergeOldData"

1. Plan: Analyzes 903-line SP with SPATIAL, CURSOR patterns
2. Build: Partial conversion, H3 stubs generated
3. Review: FAIL (1 blocker - complex spatial logic needs manual review)
4. Fix: Cannot auto-fix spatial logic

Status: PARTIAL - Manual spatial conversion required
Next Steps: Review app_fix_reports/ for guidance
```

## Scripts

This skill includes Python scripts for workflow orchestration:

### sp_orchestrator.py

```python
from scripts.sp_orchestrator import SPMigrationOrchestrator

# Initialize
orchestrator = SPMigrationOrchestrator("stg.spCalculateFactWorkersShifts")

# Validate prerequisites
result = orchestrator.validate_prerequisites()

# Get SP definition
sp_sql = orchestrator.get_sp_definition()

# Generate prompts for each phase
plan_prompt = orchestrator.generate_plan_prompt()
build_prompt = orchestrator.generate_build_prompt(plan_path)
review_prompt = orchestrator.generate_review_prompt(plan_path)
fix_prompt = orchestrator.generate_fix_prompt(plan_path, review_path)

# Track phase completion
orchestrator.complete_phase("plan", session_id="abc", artifact_path="specs/sp-calc.md")
orchestrator.set_review_verdict("FAIL")

# Check conditional execution
if orchestrator.should_run_fix():
    # Spawn fix subagent

# Get result and save
result = orchestrator.get_result()
orchestrator.save_session()
print(orchestrator.format_summary())
```

## Integration with Existing Skills

This ADW integrates with:

- **lakebridge-plan**: Uses the Plan subagent for SP analysis and plan creation
- **lakebridge-build**: Uses Build subagent for implementation and deployment
- **lakebridge-review**: Uses Review subagent for validation and risk assessment
- **lakebridge-fix**: Uses Fix subagent for issue resolution
- **shared**: Uses credentials, databricks_client, sqlserver_client for infrastructure

## Configuration

### Credentials (from shared skill)
```yaml
# ~/.databricks/labs/lakebridge/.credentials.yml
mssql:
  server: wakecap24.database.windows.net
  database: WakeCapDW_20251215
  user: snowconvert
  password: <password>

databricks:
  host: https://adb-xxx.azuredatabricks.net
  token: <token>
  catalog: wakecap_prod
  schema: raw
```

### Table Mappings (for dependency resolution)
```yaml
# Embedded in orchestrator
table_mappings:
  dbo.Worker: wakecap_prod.silver.silver_worker
  dbo.Project: wakecap_prod.silver.silver_project
  dbo.Company: wakecap_prod.silver.silver_organization
  dbo.Zone: wakecap_prod.silver.silver_zone
  dbo.Floor: wakecap_prod.silver.silver_floor
  # ... additional mappings
```
