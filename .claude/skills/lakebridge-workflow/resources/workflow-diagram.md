# Workflow Architecture

## Sequential Multi-Subagent Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                    USER REQUEST                                  │
│              "Implement feature X" or                           │
│              "Migrate database Y to Databricks"                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PLAN SUBAGENT                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Analyzes requirements                                   │   │
│  │ • Explores codebase                                       │   │
│  │ • Creates implementation plan                             │   │
│  │ • Outputs: specs/<name>.md                               │   │
│  │ • Returns: plan_session_id                               │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────────┘
                          │ plan_path
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  BUILD SUBAGENT                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Reads plan file                                         │   │
│  │ • Implements step-by-step                                 │   │
│  │ • Runs validation commands                                │   │
│  │ • Outputs: Modified codebase                              │   │
│  │ • Returns: build_session_id                               │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  REVIEW SUBAGENT                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Analyzes git diff                                       │   │
│  │ • Categorizes issues by risk tier                         │   │
│  │ • Generates validation report                             │   │
│  │ • Outputs: app_review/review_<timestamp>.md              │   │
│  │ • Returns: review_session_id, verdict (PASS/FAIL)        │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                     verdict?
                          │
              ┌───────────┴───────────┐
              │                       │
           PASS                     FAIL
              │                       │
              ▼                       ▼
┌─────────────────────┐   ┌─────────────────────────────────────┐
│      COMPLETE       │   │           FIX SUBAGENT              │
│                     │   │  ┌─────────────────────────────┐   │
│  Workflow succeeded │   │  │ • Reads review report        │   │
│  All artifacts      │   │  │ • Fixes blockers first       │   │
│  generated          │   │  │ • Then high/medium/low       │   │
│                     │   │  │ • Re-validates               │   │
└─────────────────────┘   │  │ • Outputs: Fixed codebase    │   │
                          │  │ • Returns: fix_session_id    │   │
                          │  └─────────────────────────────┘   │
                          └──────────────┬──────────────────────┘
                                         │
                                         ▼
                          ┌─────────────────────┐
                          │      COMPLETE       │
                          │                     │
                          │  Workflow succeeded │
                          │  with fixes applied │
                          └─────────────────────┘
```

## Session Tracking

Each subagent returns a session ID that can be used to:
- Resume interrupted workflows
- Fork from specific points
- Audit agent decisions

```json
{
  "workflow_id": "uuid-for-this-run",
  "sessions": {
    "plan_session_id": "abc123",
    "build_session_id": "def456",
    "review_session_id": "ghi789",
    "fix_session_id": "jkl012"
  },
  "artifacts": {
    "plan_path": "specs/feature-name.md",
    "review_path": "app_review/review_2024-01-15.md",
    "fix_path": "app_fix_reports/fix_2024-01-15.md"
  }
}
```

## Risk Tier Flow

```
REVIEW OUTPUT
     │
     ├── BLOCKERS ──────► Must fix before merge
     │                    (Security, data loss, crashes)
     │
     ├── HIGH RISK ─────► Should fix before merge
     │                    (Performance, error handling)
     │
     ├── MEDIUM RISK ───► Fix soon
     │                    (Code quality, tests)
     │
     └── LOW RISK ──────► Nice to have
                          (Style, refactoring)

VERDICT LOGIC:
  IF (BLOCKERS > 0) → FAIL → Run Fix Subagent
  ELSE              → PASS → Workflow Complete
```

## Parallel vs Sequential Execution

| Step | Can Run in Parallel? | Dependency |
|------|---------------------|------------|
| Plan | No | Needs user request |
| Build | No | Needs plan_path from Plan |
| Review | No | Needs code changes from Build |
| Fix | No | Needs review_path and verdict from Review |

**Within each step**, the subagent may run parallel operations:
- Plan: Parallel file reads for exploration
- Build: Parallel tool calls for independent changes
- Review: Parallel grep searches for anti-patterns
- Fix: Sequential fixes (each depends on previous state)
