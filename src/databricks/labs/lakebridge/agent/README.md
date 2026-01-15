# Lakebridge Agent Module

AI-powered migration workflows using Claude Agent SDK.

## Overview

The agent module provides automated migration workflows that use AI to:
- **Plan** migrations by analyzing codebases and creating detailed implementation plans
- **Build** implementations by following plans step-by-step
- **Review** completed work with risk-tiered validation reports
- **Fix** issues identified in code reviews
- **Question** codebases to understand existing patterns

## Module Structure

```
agent/
├── __init__.py           # Module exports
├── agent_sdk.py          # Claude Agent SDK wrapper with Pydantic models
├── logging.py            # Structured logging for workflows
├── runner.py             # High-level AgentRunner interface
└── workflows/
    ├── __init__.py
    ├── plan_step.py      # /plan implementation
    ├── build_step.py     # /build implementation
    ├── review_step.py    # /review implementation
    ├── fix_step.py       # /fix implementation
    ├── question_step.py  # /question implementation
    └── full_workflow.py  # Complete plan→build→review→fix orchestration
```

## Installation

```bash
# Install with agent dependencies
pip install databricks-labs-lakebridge[agent]

# Or install claude-agent-sdk separately
pip install claude-agent-sdk>=0.1.18
```

## Usage

### CLI Commands

```bash
# Create an implementation plan
databricks labs lakebridge agent-plan \
  --prompt "Migrate stored procedures to Databricks DLT" \
  --working-dir /path/to/project

# Implement a plan
databricks labs lakebridge agent-build \
  --plan-path specs/migration-plan.md \
  --working-dir /path/to/project

# Review completed work
databricks labs lakebridge agent-review \
  --prompt "Migration implementation" \
  --plan-path specs/migration-plan.md \
  --working-dir /path/to/project

# Fix issues from review
databricks labs lakebridge agent-fix \
  --prompt "Migration implementation" \
  --plan-path specs/migration-plan.md \
  --review-path app_review/review.md \
  --working-dir /path/to/project

# Answer questions about the codebase
databricks labs lakebridge agent-question \
  --question "How does the transpiler handle stored procedures?" \
  --working-dir /path/to/project

# Run complete workflow
databricks labs lakebridge agent-migrate \
  --prompt "Migrate WakeCapDW to Databricks DLT" \
  --working-dir /path/to/project
```

### Programmatic Usage

```python
from databricks.labs.lakebridge.agent import AgentRunner

# Initialize runner
runner = AgentRunner(
    working_dir="/path/to/project",
    model="claude-opus-4-5-20251101",
    verbose=True,
)

# Run individual steps (async)
plan_result = await runner.plan("Migrate stored procedures to DLT")
build_result = await runner.build(plan_result.plan_path)
review_result = await runner.review(prompt, plan_result.plan_path)

# Or use sync wrappers
plan_result = runner.plan_sync("Migrate stored procedures to DLT")

# Run complete workflow
result = await runner.migrate(
    prompt="Migrate WakeCapDW to Databricks DLT",
    skip_review=False,
    skip_fix=False,
)

if result.success:
    print(f"Migration completed! Verdict: {result.verdict}")
    print(f"Plan: {result.plan_path}")
```

### Slash Commands (Cursor/Claude IDE)

The `.claude/commands/` directory contains slash command definitions:

| Command | File | Description |
|---------|------|-------------|
| `/plan` | `plan.md` | Creates implementation plans in `specs/` |
| `/build` | `build.md` | Implements plans top-to-bottom |
| `/review` | `review.md` | Produces risk-tiered validation reports |
| `/fix` | `fix.md` | Fixes issues from review reports |
| `/question` | `question.md` | Answers codebase questions (read-only) |

#### Example Usage

```
/plan Migrate the WakeCapDW stored procedures to Databricks DLT pipelines

/build specs/wakecap-migration-plan.md

/review "WakeCapDW migration" specs/wakecap-migration-plan.md

/fix "WakeCapDW migration" specs/wakecap-migration-plan.md app_review/review_2025.md
```

## Workflow Architecture

### Plan Step
1. Analyzes user prompt to understand requirements
2. Explores codebase to understand existing patterns
3. Creates detailed implementation plan with:
   - Task description and objectives
   - Relevant files list
   - Step-by-step tasks
   - Acceptance criteria
   - Validation commands
4. Saves plan to `specs/<descriptive-name>.md`

### Build Step
1. Reads plan file
2. Implements each step top-to-bottom
3. Runs validation commands
4. Reports completed work with git diff stats

### Review Step
1. Analyzes git diffs to understand changes
2. Inspects modified files for issues
3. Categorizes issues by risk tier:
   - **🚨 BLOCKER**: Must fix before merge
   - **⚠️ HIGH RISK**: Should fix before merge
   - **⚡ MEDIUM RISK**: Fix soon
   - **💡 LOW RISK**: Nice to have
4. Produces report in `app_review/review_<timestamp>.md`
5. Returns PASS/FAIL verdict

### Fix Step
1. Reads review report
2. Implements recommended solutions by priority
3. Validates fixes
4. Produces fix report in `app_fix_reports/`

## Configuration

### Model Selection

| Model | Use Case |
|-------|----------|
| `opus` | Complex planning and implementation (default) |
| `sonnet` | Faster responses for questions |
| `haiku` | Quick, simple tasks |

### Environment Variables

```bash
export ANTHROPIC_API_KEY="your-api-key"
```

## Logging

Workflow logs are stored in `.lakebridge_agent_logs/` with structured JSON format:

```json
{
  "id": "uuid",
  "timestamp": "2025-01-15T10:30:00",
  "workflow_id": "abc123",
  "step": "plan",
  "category": "step",
  "event_type": "step_end",
  "message": "Plan step completed successfully",
  "duration_ms": 15000,
  "status": "success"
}
```

## Output Directories

| Directory | Contents |
|-----------|----------|
| `specs/` | Implementation plans |
| `app_review/` | Code review reports |
| `app_fix_reports/` | Fix reports |
| `.lakebridge_agent_logs/` | Workflow logs |

## Best Practices

1. **Be specific in prompts**: Include target technology, source system, and scope
2. **Review plans before building**: Ensure the plan matches your expectations
3. **Run validation commands**: Always verify the implementation works
4. **Address blockers first**: Fix critical issues before moving forward
5. **Use verbose mode for debugging**: `--verbose` flag shows detailed progress
