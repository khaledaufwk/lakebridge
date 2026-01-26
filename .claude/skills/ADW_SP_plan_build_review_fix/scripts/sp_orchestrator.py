"""
Stored Procedure Migration Orchestrator.

Manages the Plan -> Build -> Review -> Fix workflow for migrating
T-SQL stored procedures from Azure SQL Server to Databricks.
"""

import json
import uuid
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Any
from enum import Enum
from pathlib import Path


class WorkflowPhase(Enum):
    """Workflow phases."""
    PLAN = "plan"
    BUILD = "build"
    REVIEW = "review"
    FIX = "fix"


class PhaseStatus(Enum):
    """Status of a workflow phase."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ReviewVerdict(Enum):
    """Review verdict."""
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass
class PhaseResult:
    """Result of a workflow phase."""
    phase: WorkflowPhase
    status: PhaseStatus
    session_id: Optional[str] = None
    artifact_path: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate phase duration."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "phase": self.phase.value,
            "status": self.status.value,
            "session_id": self.session_id,
            "artifact_path": self.artifact_path,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class SPAnalysis:
    """Analysis of a stored procedure."""
    schema_name: str
    sp_name: str
    full_name: str
    line_count: int
    patterns: List[str]
    input_tables: List[str]
    output_tables: List[str]
    function_calls: List[str]
    requires_manual_review: bool


class SPMigrationOrchestrator:
    """
    Orchestrates the complete stored procedure migration workflow.

    This class manages workflow state, generates prompts for subagents,
    and tracks session progress for resume capability.

    Usage:
        orchestrator = SPMigrationOrchestrator("stg.spCalculateFactWorkersShifts")

        # Validate prerequisites
        result = orchestrator.validate_prerequisites()

        # Generate prompts for subagents
        plan_prompt = orchestrator.generate_plan_prompt()

        # Track completion
        orchestrator.complete_phase("plan", session_id="abc", artifact_path="specs/sp-calc.md")

        # Check conditions
        if orchestrator.should_run_fix():
            fix_prompt = orchestrator.generate_fix_prompt(plan_path, review_path)

        # Get result
        result = orchestrator.get_result()
        orchestrator.save_session()
    """

    # Standard table mappings for WakeCap migration
    DEFAULT_TABLE_MAPPINGS = {
        "dbo.Worker": "wakecap_prod.silver.silver_worker",
        "dbo.Project": "wakecap_prod.silver.silver_project",
        "dbo.Company": "wakecap_prod.silver.silver_organization",
        "dbo.Organization": "wakecap_prod.silver.silver_organization",
        "dbo.Zone": "wakecap_prod.silver.silver_zone",
        "dbo.Floor": "wakecap_prod.silver.silver_floor",
        "dbo.Space": "wakecap_prod.silver.silver_floor",
        "dbo.Crew": "wakecap_prod.silver.silver_crew",
        "dbo.Trade": "wakecap_prod.silver.silver_trade",
        "dbo.Device": "wakecap_prod.silver.silver_device",
        "dbo.Workshift": "wakecap_prod.silver.silver_workshift",
        "dbo.FactWorkersHistory": "wakecap_prod.silver.silver_fact_workers_history",
        "dbo.FactWorkersShifts": "wakecap_prod.silver.silver_fact_workers_shifts",
        "dbo.FactObservations": "wakecap_prod.silver.silver_fact_observations",
        "stg.wc2023_People": "wakecap_prod.raw.timescale_people",
        "stg.wc2023_Company": "wakecap_prod.raw.timescale_company",
        "stg.wc2023_Zone": "wakecap_prod.raw.timescale_zone",
    }

    # Complexity patterns that affect conversion approach
    COMPLEXITY_PATTERNS = {
        "CURSOR": ["DECLARE CURSOR", "OPEN CURSOR", "FETCH NEXT", "DEALLOCATE"],
        "TEMP_TABLE": ["#", "CREATE TABLE #", "INTO #", "##"],
        "DYNAMIC_SQL": ["EXEC(", "EXECUTE(", "SP_EXECUTESQL"],
        "SPATIAL": ["GEOGRAPHY", "GEOMETRY", "STDISTANCE", "STINTERSECTS", "STCONTAINS"],
        "MERGE": ["MERGE INTO", "MERGE "],
        "PIVOT": ["PIVOT", "UNPIVOT"],
        "CTE": ["WITH ", ";WITH "],
        "WINDOW_FUNCTION": ["OVER(", "OVER (", "ROW_NUMBER()", "LAG(", "LEAD("],
        "TRANSACTION": ["BEGIN TRAN", "COMMIT TRAN", "ROLLBACK"],
    }

    def __init__(
        self,
        sp_name: str,
        workflow_id: Optional[str] = None,
        base_path: Optional[Path] = None,
        table_mappings: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize SP migration orchestrator.

        Args:
            sp_name: Fully qualified SP name (e.g., "stg.spCalculateFactWorkersShifts")
            workflow_id: Optional ID for resuming a workflow
            base_path: Base path for the migration project
            table_mappings: Custom table name mappings
        """
        self.sp_name = sp_name
        self.workflow_id = workflow_id or str(uuid.uuid4())[:8]
        self.base_path = base_path or Path("migration_project")
        self.table_mappings = table_mappings or self.DEFAULT_TABLE_MAPPINGS

        # Parse SP name
        parts = sp_name.split(".")
        if len(parts) == 2:
            self.schema_name = parts[0]
            self.object_name = parts[1]
        else:
            self.schema_name = "dbo"
            self.object_name = sp_name

        # Workflow state
        self._started_at = datetime.now()
        self._completed_at: Optional[datetime] = None
        self._phases: Dict[WorkflowPhase, PhaseResult] = {}
        self._review_verdict: Optional[ReviewVerdict] = None
        self._sp_definition: Optional[str] = None
        self._sp_analysis: Optional[SPAnalysis] = None

    @property
    def sp_file_name(self) -> str:
        """Get the SP source file name."""
        return f"{self.schema_name}.{self.object_name}.sql"

    @property
    def sp_source_path(self) -> Path:
        """Get the full path to SP source file."""
        return self.base_path / "source_sql" / "stored_procedures" / self.sp_file_name

    @property
    def sp_name_kebab(self) -> str:
        """Get kebab-case version of SP name for file naming."""
        # Convert camelCase/PascalCase to kebab-case
        name = self.object_name
        # Remove 'sp' prefix if present
        if name.lower().startswith("sp"):
            name = name[2:]
        # Insert hyphens before capitals
        name = re.sub(r'([a-z])([A-Z])', r'\1-\2', name)
        return name.lower()

    @property
    def plan_file_path(self) -> str:
        """Get the expected plan file path."""
        return f"specs/sp-{self.sp_name_kebab}.md"

    @property
    def notebook_file_path(self) -> str:
        """Get the expected notebook output path."""
        return f"migration_project/pipelines/gold/notebooks/{self.sp_name_kebab.replace('-', '_')}.py"

    @property
    def session_file_path(self) -> Path:
        """Get the session file path."""
        sessions_dir = Path("adw_sessions")
        safe_name = self.sp_name.replace(".", "_").replace(" ", "_")
        return sessions_dir / f"sp_{safe_name}_{self.workflow_id}.json"

    def validate_prerequisites(self) -> Dict[str, Any]:
        """
        Validate that all prerequisites are met.

        Returns:
            Dictionary with 'success' boolean and 'message' string
        """
        issues = []

        # Check credentials file
        creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
        if not creds_path.exists():
            issues.append(f"Credentials file not found: {creds_path}")

        # Check if source_sql directory exists
        source_sql_dir = self.base_path / "source_sql"
        if not source_sql_dir.exists():
            issues.append(f"Source SQL directory not found: {source_sql_dir}")
            issues.append("Run extraction first or ensure SP files are available")

        # Check if specific SP file exists (warning only, can be extracted)
        if source_sql_dir.exists() and not self.sp_source_path.exists():
            issues.append(f"SP source file not found: {self.sp_source_path}")
            issues.append("Will attempt to extract from SQL Server")

        # Check output directories exist (create if not)
        for dir_path in ["specs", "app_review", "app_fix_reports", "adw_sessions"]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)

        if issues:
            return {
                "success": False,
                "message": "Prerequisites check failed:\n- " + "\n- ".join(issues),
                "issues": issues,
            }

        return {
            "success": True,
            "message": "All prerequisites validated",
            "issues": [],
        }

    def get_sp_definition(self) -> Optional[str]:
        """
        Get the stored procedure definition.

        First checks local cache, then attempts to read from file,
        finally falls back to SQL Server extraction.

        Returns:
            SP definition string or None if not found
        """
        if self._sp_definition:
            return self._sp_definition

        # Try to read from file
        if self.sp_source_path.exists():
            self._sp_definition = self.sp_source_path.read_text(encoding="utf-8")
            return self._sp_definition

        # Would need to extract from SQL Server
        # This is typically done by the Plan subagent
        return None

    def analyze_sp(self, sql_content: str) -> SPAnalysis:
        """
        Analyze stored procedure for complexity patterns.

        Args:
            sql_content: The T-SQL source code

        Returns:
            SPAnalysis with detected patterns and dependencies
        """
        sql_upper = sql_content.upper()
        detected_patterns = []

        # Detect complexity patterns
        for pattern_name, keywords in self.COMPLEXITY_PATTERNS.items():
            for keyword in keywords:
                if keyword.upper() in sql_upper:
                    detected_patterns.append(pattern_name)
                    break

        # Extract table references (simplified)
        table_pattern = r'\b(?:FROM|JOIN|INTO|UPDATE|MERGE\s+INTO?)\s+(\[?\w+\]?\.\[?\w+\]?|\[?\w+\]?)'
        tables = re.findall(table_pattern, sql_content, re.IGNORECASE)
        tables = list(set(t.strip("[]") for t in tables))

        # Separate input vs output tables
        input_tables = []
        output_tables = []
        for table in tables:
            if re.search(rf'\b(?:INTO|UPDATE|MERGE\s+INTO?)\s+\[?{re.escape(table)}', sql_content, re.IGNORECASE):
                output_tables.append(table)
            else:
                input_tables.append(table)

        # Extract function calls
        func_pattern = r'\b(fn_?\w+|dbo\.\w+)\s*\('
        functions = list(set(re.findall(func_pattern, sql_content, re.IGNORECASE)))

        # Determine if manual review is needed
        manual_patterns = {"CURSOR", "DYNAMIC_SQL", "SPATIAL"}
        requires_manual = bool(set(detected_patterns) & manual_patterns)

        self._sp_analysis = SPAnalysis(
            schema_name=self.schema_name,
            sp_name=self.object_name,
            full_name=self.sp_name,
            line_count=sql_content.count("\n") + 1,
            patterns=detected_patterns,
            input_tables=input_tables,
            output_tables=output_tables,
            function_calls=functions,
            requires_manual_review=requires_manual,
        )

        return self._sp_analysis

    def start_phase(self, phase: WorkflowPhase) -> None:
        """Mark a phase as started."""
        self._phases[phase] = PhaseResult(
            phase=phase,
            status=PhaseStatus.RUNNING,
            started_at=datetime.now(),
        )

    def complete_phase(
        self,
        phase: str,
        session_id: Optional[str] = None,
        artifact_path: Optional[str] = None,
    ) -> None:
        """
        Mark a phase as completed.

        Args:
            phase: Phase name ("plan", "build", "review", "fix")
            session_id: Subagent session ID for resume
            artifact_path: Path to generated artifact
        """
        phase_enum = WorkflowPhase(phase)

        if phase_enum not in self._phases:
            self._phases[phase_enum] = PhaseResult(phase=phase_enum, status=PhaseStatus.PENDING)

        self._phases[phase_enum].status = PhaseStatus.COMPLETED
        self._phases[phase_enum].session_id = session_id
        self._phases[phase_enum].artifact_path = artifact_path
        self._phases[phase_enum].completed_at = datetime.now()

    def fail_phase(self, phase: str, error: str) -> None:
        """Mark a phase as failed."""
        phase_enum = WorkflowPhase(phase)

        if phase_enum not in self._phases:
            self._phases[phase_enum] = PhaseResult(phase=phase_enum, status=PhaseStatus.PENDING)

        self._phases[phase_enum].status = PhaseStatus.FAILED
        self._phases[phase_enum].error = error
        self._phases[phase_enum].completed_at = datetime.now()

    def skip_phase(self, phase: str) -> None:
        """Mark a phase as skipped."""
        phase_enum = WorkflowPhase(phase)
        self._phases[phase_enum] = PhaseResult(
            phase=phase_enum,
            status=PhaseStatus.SKIPPED,
        )

    def set_review_verdict(self, verdict: str) -> None:
        """Set the review verdict (PASS or FAIL)."""
        self._review_verdict = ReviewVerdict(verdict.upper())

    def should_run_build(self) -> bool:
        """Check if build phase should run."""
        plan = self._phases.get(WorkflowPhase.PLAN)
        return plan is not None and plan.status == PhaseStatus.COMPLETED

    def should_run_review(self) -> bool:
        """Check if review phase should run."""
        build = self._phases.get(WorkflowPhase.BUILD)
        return build is not None and build.status == PhaseStatus.COMPLETED

    def should_run_fix(self) -> bool:
        """Check if fix phase should run."""
        review = self._phases.get(WorkflowPhase.REVIEW)
        if not review or review.status != PhaseStatus.COMPLETED:
            return False
        return self._review_verdict == ReviewVerdict.FAIL

    def generate_plan_prompt(self) -> str:
        """Generate prompt for Plan subagent."""
        sp_source_info = ""
        if self.sp_source_path.exists():
            sp_source_info = f"\nSource file: {self.sp_source_path}"
        else:
            sp_source_info = f"\nExtract SP from SQL Server and save to: {self.sp_source_path}"

        return f"""Create a detailed implementation plan for migrating the stored procedure: {self.sp_name}

## Task

Migrate T-SQL stored procedure `{self.sp_name}` from Azure SQL Server to Databricks.
{sp_source_info}

## Instructions

1. **Read the SP Source**
   - Read from `{self.sp_source_path}` if it exists
   - If not, extract from SQL Server using credentials at `~/.databricks/labs/lakebridge/.credentials.yml`
   - Save extracted SP to `{self.sp_source_path}`

2. **Analyze Complexity**
   - Count lines of code
   - Identify patterns: CURSOR, TEMP_TABLE, MERGE, SPATIAL, DYNAMIC_SQL, CTE, WINDOW_FUNCTION
   - List input tables (tables being read)
   - List output tables (tables being written)
   - List function dependencies

3. **Design Conversion Approach**
   Based on patterns detected, choose the target format:
   - Simple MERGE/INSERT -> DLT streaming table
   - CTE-based transformations -> DLT batch table
   - CURSOR/DYNAMIC_SQL -> Python notebook with DataFrame operations
   - SPATIAL operations -> Python notebook with H3/Shapely UDFs

4. **Map Dependencies**
   Use these table mappings for WakeCap:
   - dbo.Worker -> wakecap_prod.silver.silver_worker
   - dbo.Project -> wakecap_prod.silver.silver_project
   - dbo.Company -> wakecap_prod.silver.silver_organization
   - dbo.Zone -> wakecap_prod.silver.silver_zone
   - dbo.Floor -> wakecap_prod.silver.silver_floor
   - stg.wc2023_* -> wakecap_prod.raw.timescale_*

5. **Create the Plan**
   Save to: `{self.plan_file_path}`

   Include:
   - SP analysis summary
   - Detected patterns and their conversion approach
   - Step-by-step conversion tasks
   - Expected output file path: `{self.notebook_file_path}`
   - Validation commands
   - Acceptance criteria

Use the lakebridge-plan skill format for the plan document."""

    def generate_build_prompt(self, plan_path: str) -> str:
        """Generate prompt for Build subagent."""
        return f"""Implement the stored procedure migration plan.

## Plan File
Read and implement: `{plan_path}`

## Original Request
Migrate stored procedure: {self.sp_name}

## Instructions

1. Read the plan carefully - follow every step in order

2. Convert the T-SQL to Databricks:
   - Source file: `{self.sp_source_path}`
   - Target file: `{self.notebook_file_path}`

3. For each pattern detected:
   - CURSOR -> Window functions (lag/lead/row_number)
   - TEMP_TABLE -> createOrReplaceTempView() or CTEs
   - MERGE -> DeltaTable.merge() or DLT APPLY CHANGES
   - SPATIAL -> H3 library + Haversine UDFs

4. Use correct table references:
   - Map source tables to Databricks equivalents
   - Use Unity Catalog fully-qualified names (catalog.schema.table)

5. Add proper notebook structure:
   - Databricks notebook header
   - # COMMAND ---------- separators
   - Import statements
   - Documentation comments

6. Deploy to Databricks workspace if connectivity available

7. Run validation commands from the plan

8. Update MIGRATION_STATUS.md with conversion result

Use the lakebridge-build skill patterns for implementation."""

    def generate_review_prompt(self, plan_path: str) -> str:
        """Generate prompt for Review subagent."""
        return f"""Review the stored procedure migration.

## Context
- SP Name: {self.sp_name}
- Plan: `{plan_path}`
- Expected notebook: `{self.notebook_file_path}`

## Instructions

1. **Analyze Git Changes**
   - Run `git diff` to see all changes
   - Identify files added/modified

2. **Validate Conversion Completeness**
   - Read source: `{self.sp_source_path}`
   - Read target: `{self.notebook_file_path}`
   - Verify all T-SQL patterns were converted
   - Check all tables are referenced correctly

3. **Compare Business Logic**
   - Input tables match
   - Output tables match
   - Aggregations preserved
   - Join conditions equivalent
   - Filter conditions equivalent

4. **Check Dependencies**
   - All source tables exist in Databricks (or are mapped)
   - All function dependencies resolved (or stubbed)
   - No missing references

5. **Verify Data Types**
   - VARCHAR/NVARCHAR -> STRING
   - DATETIME -> TIMESTAMP
   - DECIMAL precision preserved
   - GEOGRAPHY -> STRING (WKT)

6. **Categorize Issues**
   - BLOCKER: Missing tables, broken logic, data loss risk
   - HIGH RISK: Performance issues, incomplete error handling
   - MEDIUM RISK: Missing tests, code duplication
   - LOW RISK: Style issues, minor improvements

7. **Write Review Report**
   Save to: `app_review/sp_review_{self.sp_name_kebab}_{datetime.now().strftime('%Y%m%d')}.md`

8. **Issue Verdict**
   - PASS if no blockers
   - FAIL if any blockers exist

Use the lakebridge-review skill format for the report."""

    def generate_fix_prompt(self, plan_path: str, review_path: str) -> str:
        """Generate prompt for Fix subagent."""
        return f"""Fix issues identified in the stored procedure migration review.

## Context
- SP Name: {self.sp_name}
- Plan: `{plan_path}`
- Review Report: `{review_path}`
- Notebook: `{self.notebook_file_path}`

## Instructions

1. **Read Review Report**
   - Parse all issues by risk tier
   - Note recommended solutions for each

2. **Fix Priority Order**
   1. BLOCKERS (required - must fix all)
   2. HIGH RISK (required - should fix all)
   3. MEDIUM RISK (if time permits)
   4. LOW RISK (document for later)

3. **For Each Issue**
   - Read affected file
   - Apply recommended solution
   - Verify fix works
   - Document what was changed

4. **Common SP Migration Fixes**
   - Missing table: Create stub or update mapping
   - Missing UDF: Generate Python UDF registration
   - Incomplete pattern conversion: Complete the conversion
   - Data type mismatch: Add explicit casts

5. **Re-run Validation**
   - Execute validation commands from plan
   - Verify all tests pass

6. **Write Fix Report**
   Save to: `app_fix_reports/sp_fix_{self.sp_name_kebab}_{datetime.now().strftime('%Y%m%d')}.md`

Use the lakebridge-fix skill format for the report."""

    def get_result(self) -> Dict[str, Any]:
        """Get the workflow result as a dictionary."""
        # Determine overall status
        all_completed = all(
            p.status in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED)
            for p in self._phases.values()
        )

        plan = self._phases.get(WorkflowPhase.PLAN)
        build = self._phases.get(WorkflowPhase.BUILD)
        review = self._phases.get(WorkflowPhase.REVIEW)
        fix = self._phases.get(WorkflowPhase.FIX)

        if not plan or plan.status == PhaseStatus.FAILED:
            overall_status = "FAILED"
        elif not build or build.status == PhaseStatus.FAILED:
            overall_status = "FAILED"
        elif self._review_verdict == ReviewVerdict.FAIL and (not fix or fix.status == PhaseStatus.FAILED):
            overall_status = "PARTIAL"
        elif all_completed:
            overall_status = "SUCCESS"
        else:
            overall_status = "IN_PROGRESS"

        return {
            "workflow_id": self.workflow_id,
            "sp_name": self.sp_name,
            "sp_definition_path": str(self.sp_source_path),
            "overall_status": overall_status,
            "review_verdict": self._review_verdict.value if self._review_verdict else None,
            "phases": {
                phase.value: result.to_dict()
                for phase, result in self._phases.items()
            },
            "artifacts": {
                "source_sql": str(self.sp_source_path),
                "plan": plan.artifact_path if plan else None,
                "converted_notebook": self.notebook_file_path,
                "review_report": review.artifact_path if review else None,
                "fix_report": fix.artifact_path if fix else None,
            },
            "started_at": self._started_at.isoformat(),
            "completed_at": self._completed_at.isoformat() if self._completed_at else None,
        }

    def save_session(self) -> Path:
        """Save workflow session to file for resume capability."""
        self.session_file_path.parent.mkdir(parents=True, exist_ok=True)

        result = self.get_result()
        with open(self.session_file_path, "w") as f:
            json.dump(result, f, indent=2)

        return self.session_file_path

    @classmethod
    def load_session(cls, session_path: str) -> "SPMigrationOrchestrator":
        """
        Load a workflow session from file.

        Args:
            session_path: Path to session JSON file

        Returns:
            SPMigrationOrchestrator with restored state
        """
        with open(session_path) as f:
            data = json.load(f)

        orchestrator = cls(
            sp_name=data["sp_name"],
            workflow_id=data["workflow_id"],
        )

        # Restore phase states
        for phase_name, phase_data in data.get("phases", {}).items():
            phase_enum = WorkflowPhase(phase_name)
            orchestrator._phases[phase_enum] = PhaseResult(
                phase=phase_enum,
                status=PhaseStatus(phase_data["status"]),
                session_id=phase_data.get("session_id"),
                artifact_path=phase_data.get("artifact_path"),
                error=phase_data.get("error"),
                started_at=datetime.fromisoformat(phase_data["started_at"]) if phase_data.get("started_at") else None,
                completed_at=datetime.fromisoformat(phase_data["completed_at"]) if phase_data.get("completed_at") else None,
            )

        # Restore review verdict
        if data.get("review_verdict"):
            orchestrator._review_verdict = ReviewVerdict(data["review_verdict"])

        return orchestrator

    def format_summary(self) -> str:
        """Format a summary of the workflow result."""
        result = self.get_result()

        lines = [
            "=" * 60,
            "Stored Procedure Migration Complete",
            "=" * 60,
            "",
            f"SP Name: {self.sp_name}",
            f"Workflow ID: {self.workflow_id}",
            "",
            "Phase Results:",
        ]

        for phase in WorkflowPhase:
            phase_result = self._phases.get(phase)
            if phase_result:
                status = phase_result.status.value.upper()
                artifact = f" -> {phase_result.artifact_path}" if phase_result.artifact_path else ""
                lines.append(f"  - {phase.value.capitalize()}: {status}{artifact}")
            else:
                lines.append(f"  - {phase.value.capitalize()}: NOT RUN")

        if self._review_verdict:
            lines.append(f"\nReview Verdict: {self._review_verdict.value}")

        # Add analysis summary if available
        if self._sp_analysis:
            lines.extend([
                "",
                "Conversion Summary:",
                f"  - Source Lines: {self._sp_analysis.line_count}",
                f"  - Patterns Detected: {', '.join(self._sp_analysis.patterns) or 'None'}",
                f"  - Input Tables: {len(self._sp_analysis.input_tables)}",
                f"  - Output Tables: {len(self._sp_analysis.output_tables)}",
                f"  - Manual Review: {'Required' if self._sp_analysis.requires_manual_review else 'Not Required'}",
            ])

        lines.extend([
            "",
            "Artifacts Created:",
            f"  - Plan: {result['artifacts'].get('plan') or 'N/A'}",
            f"  - Notebook: {result['artifacts'].get('converted_notebook') or 'N/A'}",
            f"  - Review: {result['artifacts'].get('review_report') or 'N/A'}",
            f"  - Fix Report: {result['artifacts'].get('fix_report') or 'N/A'}",
            "",
            f"Session File: {self.session_file_path}",
            "(Use this to resume if needed)",
            "",
            f"Overall Status: {result['overall_status']}",
            "=" * 60,
        ])

        return "\n".join(lines)
