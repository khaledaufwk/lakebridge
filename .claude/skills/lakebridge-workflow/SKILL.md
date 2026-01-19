---
name: lakebridge-workflow
description: Orchestrate full migration workflow using multiple subagents - plan, build, review, and fix in sequence with session tracking
---

# Lakebridge Workflow Orchestrator

## Purpose

This skill orchestrates a complete development workflow using multiple specialized subagents. It coordinates the Plan, Build, Review, and Fix steps in sequence, tracking session IDs for resume capability and implementing conditional logic based on review verdicts.

## Usage

Invoke this skill when you need to run a complete development or migration workflow:
- "Use the lakebridge-workflow skill to implement user authentication"
- "Use the lakebridge-workflow skill to migrate the WakeCap database to Databricks"

## Variables

USER_PROMPT: $ARGUMENTS

## Instructions

You are the workflow orchestrator. Your job is to coordinate multiple specialized subagents to complete a task from start to finish.

### Workflow Steps

1. **Plan Phase** - Use the Task tool to spawn a Plan subagent
2. **Build Phase** - Use the Task tool to spawn a general-purpose subagent for implementation
3. **Review Phase** - Use the Task tool to spawn an Explore subagent for code review
4. **Fix Phase** - Conditionally spawn a general-purpose subagent if review verdict is FAIL

### Step 1: Planning

Spawn the Plan subagent to create an implementation plan:

```
Task(
    subagent_type="Plan",
    prompt="Create a detailed implementation plan for: {USER_PROMPT}.
            Save the plan to specs/ directory with a descriptive kebab-case filename.
            Include acceptance criteria and validation commands.",
    model="opus"
)
```

Capture the plan file path from the result.

### Step 2: Building

After the plan is created, spawn the Build subagent:

```
Task(
    subagent_type="general-purpose",
    prompt="Implement the plan at {plan_path}.
            Follow all steps in order, top to bottom.
            Run all validation commands before completing.
            Do not stop until validation passes.",
    model="opus"
)
```

### Step 3: Review (Optional but Recommended)

After building, spawn the Review subagent to analyze the work:

```
Task(
    subagent_type="Explore",
    prompt="Review the changes made for: {USER_PROMPT}
            Reference plan: {plan_path}

            Analyze git diffs, categorize issues by risk tier:
            - BLOCKER: Security, breaking changes, data loss
            - HIGH RISK: Performance, missing error handling
            - MEDIUM RISK: Code quality, missing tests
            - LOW RISK: Style, minor improvements

            Write report to app_review/ directory.
            End with PASS or FAIL verdict based on blockers.",
    model="opus"
)
```

Parse the review result to determine the verdict.

### Step 4: Fix (Conditional)

If the review verdict is FAIL, spawn the Fix subagent:

```
Task(
    subagent_type="general-purpose",
    prompt="Fix issues identified in {review_path}
            Reference plan: {plan_path}
            Original request: {USER_PROMPT}

            Priority order:
            1. Fix all BLOCKERS first
            2. Fix HIGH RISK issues
            3. Fix MEDIUM RISK issues if time permits
            4. Document any skipped LOW RISK items

            Run validation commands after fixes.",
    model="opus"
)
```

### Session Tracking

Track session IDs for each phase to enable resume capability:

```json
{
  "workflow_id": "<generated-uuid>",
  "user_prompt": "<original prompt>",
  "sessions": {
    "plan_session_id": "<from Task result>",
    "build_session_id": "<from Task result>",
    "review_session_id": "<from Task result>",
    "fix_session_id": "<from Task result if executed>"
  },
  "artifacts": {
    "plan_path": "specs/<plan-name>.md",
    "review_path": "app_review/review_<timestamp>.md",
    "fix_path": "app_fix_reports/fix_<timestamp>.md"
  },
  "status": {
    "plan": "COMPLETED|FAILED",
    "build": "COMPLETED|FAILED",
    "review": "PASS|FAIL|SKIPPED",
    "fix": "COMPLETED|FAILED|SKIPPED"
  }
}
```

### Conditional Logic

```
IF plan fails:
    STOP - Report planning failure

IF build fails:
    STOP - Report build failure

IF review verdict == PASS:
    SKIP fix phase
    REPORT success

IF review verdict == FAIL:
    RUN fix phase
    IF fix succeeds:
        REPORT success with fixes applied
    ELSE:
        REPORT partial success with remaining issues
```

### Parallel vs Sequential

- Plan MUST complete before Build starts (dependency: plan_path)
- Build MUST complete before Review starts (dependency: code changes)
- Review MUST complete before Fix starts (dependency: review_path, verdict)
- Within each phase, subagents may run their own parallel operations

## Workflow Diagram

```
User Request
     |
     v
+------------------+
|   Plan Subagent  |-----> specs/{name}.md
+--------+---------+
         |
         v
+------------------+
|  Build Subagent  |-----> Modified codebase
+--------+---------+
         |
         v
+------------------+
| Review Subagent  |-----> app_review/review_{ts}.md
+--------+---------+
         |
    verdict?
         |
    +----+----+
    |         |
  PASS      FAIL
    |         |
    v         v
  Done   +------------------+
         |   Fix Subagent   |-----> Fixed codebase
         +------------------+
```

## Report

After workflow completion, provide this summary:

```
Workflow Complete

User Request: {USER_PROMPT}

Phase Results:
- Plan: {status} - {plan_path}
- Build: {status}
- Review: {verdict} - {review_path}
- Fix: {status if executed}

Artifacts:
- Plan: specs/{name}.md
- Review: app_review/review_{timestamp}.md
- Fix Report: app_fix_reports/fix_{timestamp}.md (if applicable)

Session IDs (for resume):
- Plan: {plan_session_id}
- Build: {build_session_id}
- Review: {review_session_id}
- Fix: {fix_session_id}
```

## Examples

### Example 1: Feature Implementation
```
User: "Use the lakebridge-workflow skill to add dark mode toggle"

1. Plan creates: specs/add-dark-mode-toggle.md
2. Build implements the toggle component
3. Review finds: 1 MEDIUM (missing test), 0 BLOCKERS
4. Verdict: PASS
5. Fix: SKIPPED

Result: Feature complete with minor improvement suggestions
```

### Example 2: Migration with Issues
```
User: "Use the lakebridge-workflow skill to migrate WakeCap to Databricks"

1. Plan creates: specs/wakecap-databricks-migration.md
2. Build runs migration scripts
3. Review finds: 2 BLOCKERS (missing schema, no serverless)
4. Verdict: FAIL
5. Fix creates schema, enables serverless
6. Re-validates: PASS

Result: Migration complete after automated fixes
```

## Scripts

This skill includes Python scripts for workflow orchestration:

### orchestrator.py

```python
from scripts.orchestrator import WorkflowOrchestrator, WorkflowPhase

# Initialize orchestrator
orchestrator = WorkflowOrchestrator("Add user authentication")

# Track phases
orchestrator.start_phase(WorkflowPhase.PLAN)
orchestrator.complete_phase(
    WorkflowPhase.PLAN,
    session_id="abc123",
    artifact_path="specs/add-auth.md"
)

# Check conditions
if orchestrator.should_run_build():
    orchestrator.start_phase(WorkflowPhase.BUILD)
    # ...

# Generate prompts for subagents
plan_prompt = orchestrator.generate_plan_prompt()
build_prompt = orchestrator.generate_build_prompt("specs/plan.md")
review_prompt = orchestrator.generate_review_prompt("specs/plan.md")

# Get final result
result = orchestrator.get_result()
print(orchestrator.format_summary())
```
