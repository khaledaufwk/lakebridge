# Plan: Convert Claude Commands to Anthropic Skills Framework

## Task Description

Convert the existing `.claude/commands/` markdown files (plan.md, build.md, review.md, fix.md, question.md) into the official Anthropic Skills framework format, enabling improved modularity, discoverability, and integration with Claude Code's plugin system.

## Objective

Migrate the current 5-command workflow system to the Anthropic Skills architecture, creating:
1. A skill package compatible with `/plugin install`
2. Multi-subagent orchestration skill for full workflows
3. Individual skills that can run standalone or be chained

## Problem Statement

The current `.claude/commands/` structure works but has limitations:
- **No standardization**: Custom format differs from Anthropic's official skills spec
- **Limited discoverability**: Users must know commands exist
- **No plugin ecosystem integration**: Can't be shared or installed from marketplace
- **Tight coupling**: Commands reference local paths, making portability difficult

## Solution Approach

Convert to the [Anthropic Skills Framework](https://github.com/anthropics/skills):
1. Create `SKILL.md` files with YAML frontmatter for each command
2. Structure as an installable plugin package
3. Add orchestration skill for multi-subagent workflows
4. Maintain backward compatibility with existing command invocations

## Source System Analysis

Current command inventory:
- **plan.md** (11KB): Creates implementation plans, migration-specific knowledge
- **build.md** (16KB): Implements plans, T-SQL conversion patterns
- **review.md** (14KB): Risk-tiered code review with migration checklists
- **fix.md** (14KB): Issue remediation from review reports
- **question.md** (3KB): Read-only Q&A about codebase

## Target Architecture

### Directory Structure

```
.claude/
├── settings.json                    # Existing - update for skills
└── skills/                          # NEW - Skills package root
    ├── .claude-plugin/              # Plugin metadata
    │   └── plugin.json
    ├── lakebridge-workflow/         # Orchestration skill
    │   ├── SKILL.md
    │   └── resources/
    │       └── workflow-diagram.md
    ├── lakebridge-plan/             # Plan skill
    │   ├── SKILL.md
    │   └── resources/
    │       └── migration-patterns.md
    ├── lakebridge-build/            # Build skill
    │   ├── SKILL.md
    │   └── resources/
    │       └── conversion-templates.md
    ├── lakebridge-review/           # Review skill
    │   ├── SKILL.md
    │   └── resources/
    │       └── review-checklists.md
    ├── lakebridge-fix/              # Fix skill
    │   └── SKILL.md
    └── lakebridge-question/         # Question skill
        └── SKILL.md
```

### Plugin Metadata

```json
// .claude-plugin/plugin.json
{
  "name": "lakebridge-skills",
  "version": "1.0.0",
  "description": "SQL Server to Databricks migration skills with plan-build-review-fix workflow",
  "skills": [
    "lakebridge-workflow",
    "lakebridge-plan",
    "lakebridge-build",
    "lakebridge-review",
    "lakebridge-fix",
    "lakebridge-question"
  ]
}
```

## Relevant Files

### Existing Files to Migrate
- `.claude/commands/plan.md` - Source for lakebridge-plan skill
- `.claude/commands/build.md` - Source for lakebridge-build skill
- `.claude/commands/review.md` - Source for lakebridge-review skill
- `.claude/commands/fix.md` - Source for lakebridge-fix skill
- `.claude/commands/question.md` - Source for lakebridge-question skill
- `.claude/settings.json` - Update to reference skills directory

### New Files to Create
- `.claude/skills/.claude-plugin/plugin.json` - Plugin manifest
- `.claude/skills/lakebridge-workflow/SKILL.md` - Orchestration skill
- `.claude/skills/lakebridge-plan/SKILL.md` - Planning skill
- `.claude/skills/lakebridge-build/SKILL.md` - Build skill
- `.claude/skills/lakebridge-review/SKILL.md` - Review skill
- `.claude/skills/lakebridge-fix/SKILL.md` - Fix skill
- `.claude/skills/lakebridge-question/SKILL.md` - Question skill

## Implementation Phases

### Phase 1: Foundation
- Create skills directory structure
- Create plugin.json manifest
- Set up resource directories

### Phase 2: Core Implementation
- Convert each command to SKILL.md format
- Extract reusable resources (patterns, templates, checklists)
- Create orchestration workflow skill

### Phase 3: Integration & Polish
- Update settings.json for skills integration
- Test individual skills
- Test full workflow orchestration
- Document usage

## Step by Step Tasks

### 1. Create Skills Directory Structure

- Create `.claude/skills/` directory
- Create subdirectories for each skill
- Create `.claude-plugin/` directory for manifest

```bash
mkdir -p .claude/skills/.claude-plugin
mkdir -p .claude/skills/lakebridge-workflow/resources
mkdir -p .claude/skills/lakebridge-plan/resources
mkdir -p .claude/skills/lakebridge-build/resources
mkdir -p .claude/skills/lakebridge-review/resources
mkdir -p .claude/skills/lakebridge-fix
mkdir -p .claude/skills/lakebridge-question
```

### 2. Create Plugin Manifest

- Write `.claude/skills/.claude-plugin/plugin.json` with package metadata
- Define skill list and descriptions

### 3. Create Orchestration Skill (lakebridge-workflow)

Create `SKILL.md` that coordinates the full workflow:

```yaml
---
name: lakebridge-workflow
description: Orchestrate full migration workflow - plan, build, review, and fix in sequence using subagents
---
```

Key features:
- Accept user prompt for migration task
- Spawn Plan subagent first
- Chain Build subagent with plan output
- Optionally run Review subagent
- Conditionally run Fix subagent on FAIL verdict
- Track session IDs for resume capability

### 4. Convert plan.md to SKILL.md Format

- Extract YAML frontmatter into skills format
- Move migration-specific knowledge to `resources/migration-patterns.md`
- Preserve variable substitution (`$1` → `$ARGUMENTS`)
- Update paths to be relative

### 5. Convert build.md to SKILL.md Format

- Extract T-SQL conversion patterns to `resources/conversion-templates.md`
- Preserve the step-by-step implementation workflow
- Update output directory references

### 6. Convert review.md to SKILL.md Format

- Extract review checklists to `resources/review-checklists.md`
- Preserve risk-tier categorization logic
- Maintain migration-specific checklist

### 7. Convert fix.md to SKILL.md Format

- Preserve issue-to-fix mapping
- Maintain priority ordering (Blockers → High → Medium → Low)
- Keep proven fix patterns inline

### 8. Convert question.md to SKILL.md Format

- Simplest conversion - read-only skill
- Preserve FAQ knowledge base

### 9. Update settings.json

- Add skills directory to configuration
- Ensure backward compatibility with existing commands

### 10. Validate Skills Structure

- Verify all SKILL.md files have valid YAML frontmatter
- Test skill invocation syntax
- Verify resources load correctly

## Skill Format Template

Each skill follows this structure:

```markdown
---
name: lakebridge-{name}
description: {Clear description of what this skill does and when to use it}
---

# {Skill Title}

## Purpose
{What this skill accomplishes}

## Usage
{How to invoke this skill}
"Use the lakebridge-{name} skill to {action}"

## Instructions
{Core instructions for Claude to follow}

## Resources
{Reference to resources/ files if applicable}

## Examples
{Usage examples}
```

## Multi-Subagent Architecture

### Workflow Orchestration Pattern

The `lakebridge-workflow` skill implements a multi-subagent pattern:

```
User Request
     │
     ▼
┌─────────────────┐
│  Plan Subagent  │ ──► specs/{name}.md
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Build Subagent  │ ──► Modified codebase
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Review Subagent │ ──► app_review/review_{timestamp}.md
└────────┬────────┘
         │
    verdict?
         │
    ┌────┴────┐
    │         │
  PASS      FAIL
    │         │
    ▼         ▼
  Done   ┌─────────────────┐
         │  Fix Subagent   │ ──► Fixed codebase
         └─────────────────┘
```

### Subagent Configuration

Each subagent in the workflow uses the Task tool:

```python
# Plan subagent
Task(
    subagent_type="Plan",
    prompt="Create implementation plan for: {user_request}",
    model="opus"
)

# Build subagent
Task(
    subagent_type="general-purpose",
    prompt="Implement the plan at: {plan_path}",
    model="opus"
)

# Review subagent
Task(
    subagent_type="Explore",  # Read-only analysis
    prompt="Review changes against plan: {plan_path}",
    model="opus"
)

# Fix subagent
Task(
    subagent_type="general-purpose",
    prompt="Fix issues from review: {review_path}",
    model="opus"
)
```

### Session Management

The orchestration skill tracks session IDs for each subagent:

```json
{
  "workflow_id": "uuid",
  "plan_session_id": "...",
  "build_session_id": "...",
  "review_session_id": "...",
  "fix_session_id": "..."
}
```

This enables:
- Resuming failed workflows
- Forking from specific points
- Audit trail of agent decisions

## Testing Strategy

### Unit Tests (Per Skill)
1. **Syntax validation**: YAML frontmatter parses correctly
2. **Variable substitution**: `$ARGUMENTS` replaced properly
3. **Resource loading**: Referenced files exist and load

### Integration Tests (Workflow)
1. **Plan generation**: Creates valid plan file in specs/
2. **Build execution**: Implements plan without errors
3. **Review output**: Generates proper risk-tiered report
4. **Fix application**: Addresses identified issues
5. **End-to-end**: Full workflow completes successfully

### Migration-Specific Tests
1. **Connection test**: SQL Server and Databricks connectivity
2. **Transpilation test**: Sample T-SQL converts correctly
3. **DLT notebook format**: Passes Databricks validation
4. **Pipeline deployment**: Creates and runs successfully

## Acceptance Criteria

- [ ] All 6 skills have valid SKILL.md files with YAML frontmatter
- [ ] Plugin manifest (plugin.json) is valid JSON
- [ ] Individual skills can be invoked: "Use the lakebridge-plan skill to..."
- [ ] Workflow skill orchestrates all 4 steps correctly
- [ ] Subagent sessions are tracked and can be resumed
- [ ] Backward compatibility: existing `/plan`, `/build` commands still work
- [ ] Resources are properly externalized and loaded
- [ ] Documentation explains usage for new users

## Validation Commands

Execute these commands to validate the implementation:

```bash
# Verify directory structure
ls -la .claude/skills/
ls -la .claude/skills/.claude-plugin/

# Validate JSON
python -c "import json; json.load(open('.claude/skills/.claude-plugin/plugin.json'))"

# Check SKILL.md files exist
for skill in lakebridge-workflow lakebridge-plan lakebridge-build lakebridge-review lakebridge-fix lakebridge-question; do
  test -f ".claude/skills/$skill/SKILL.md" && echo "$skill: OK" || echo "$skill: MISSING"
done

# Validate YAML frontmatter in each SKILL.md
python -c "
import yaml
import pathlib
for skill_dir in pathlib.Path('.claude/skills').iterdir():
    if skill_dir.is_dir() and not skill_dir.name.startswith('.'):
        skill_file = skill_dir / 'SKILL.md'
        if skill_file.exists():
            content = skill_file.read_text()
            if content.startswith('---'):
                yaml_end = content.find('---', 3)
                yaml.safe_load(content[3:yaml_end])
                print(f'{skill_dir.name}: Valid YAML')
"
```

## Notes

### Backward Compatibility

Keep existing `.claude/commands/` directory during transition:
- Users with muscle memory for `/plan`, `/build` etc. can still use them
- Deprecation notice in old files pointing to new skills
- Remove old commands after validation period

### Plugin Installation

After implementation, the skills package can be:
1. **Used locally**: Skills auto-discovered from `.claude/skills/`
2. **Installed from repo**: `/plugin install lakebridge-skills@local`
3. **Published to marketplace**: Share with other Lakebridge users

### Future Enhancements

- **Parallel subagents**: Run independent analysis tasks concurrently
- **Conditional workflows**: Skip steps based on project type
- **Custom hooks**: Pre/post skill execution hooks
- **Progress tracking**: Real-time workflow status updates
