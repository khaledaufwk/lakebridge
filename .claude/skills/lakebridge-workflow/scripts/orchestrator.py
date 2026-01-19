"""
Workflow orchestrator for multi-subagent coordination.

Manages the Plan -> Build -> Review -> Fix workflow with
session tracking and conditional execution.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List
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


@dataclass
class WorkflowResult:
    """Complete workflow result with all phase results."""
    workflow_id: str
    user_prompt: str
    phases: Dict[WorkflowPhase, PhaseResult] = field(default_factory=dict)
    review_verdict: Optional[ReviewVerdict] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def is_success(self) -> bool:
        """Check if workflow completed successfully."""
        # Plan and Build must succeed
        plan = self.phases.get(WorkflowPhase.PLAN)
        build = self.phases.get(WorkflowPhase.BUILD)

        if not plan or plan.status != PhaseStatus.COMPLETED:
            return False
        if not build or build.status != PhaseStatus.COMPLETED:
            return False

        # If review was done, check verdict
        review = self.phases.get(WorkflowPhase.REVIEW)
        if review and review.status == PhaseStatus.COMPLETED:
            if self.review_verdict == ReviewVerdict.FAIL:
                # Fix must have succeeded
                fix = self.phases.get(WorkflowPhase.FIX)
                return fix and fix.status == PhaseStatus.COMPLETED

        return True

    @property
    def plan_path(self) -> Optional[str]:
        """Get the plan artifact path."""
        plan = self.phases.get(WorkflowPhase.PLAN)
        return plan.artifact_path if plan else None

    @property
    def review_path(self) -> Optional[str]:
        """Get the review artifact path."""
        review = self.phases.get(WorkflowPhase.REVIEW)
        return review.artifact_path if review else None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "workflow_id": self.workflow_id,
            "user_prompt": self.user_prompt,
            "is_success": self.is_success,
            "review_verdict": self.review_verdict.value if self.review_verdict else None,
            "phases": {
                phase.value: {
                    "status": result.status.value,
                    "session_id": result.session_id,
                    "artifact_path": result.artifact_path,
                    "error": result.error,
                    "duration_seconds": result.duration_seconds,
                }
                for phase, result in self.phases.items()
            },
            "artifacts": {
                "plan_path": self.plan_path,
                "review_path": self.review_path,
            },
        }


class WorkflowOrchestrator:
    """
    Orchestrates the Plan -> Build -> Review -> Fix workflow.

    This class manages workflow state and provides methods for
    executing each phase. It's designed to be used by Claude
    during skill execution.

    Usage:
        orchestrator = WorkflowOrchestrator("Add user authentication")

        # Execute phases (typically done by Claude via Task tool)
        orchestrator.start_phase(WorkflowPhase.PLAN)
        orchestrator.complete_phase(
            WorkflowPhase.PLAN,
            session_id="abc123",
            artifact_path="specs/add-auth.md"
        )

        # Check workflow state
        if orchestrator.should_run_fix():
            orchestrator.start_phase(WorkflowPhase.FIX)
            ...

        # Get final result
        result = orchestrator.get_result()
    """

    def __init__(self, user_prompt: str, workflow_id: Optional[str] = None):
        """
        Initialize workflow orchestrator.

        Args:
            user_prompt: The original user request
            workflow_id: Optional ID for resuming a workflow
        """
        self.workflow_id = workflow_id or str(uuid.uuid4())[:8]
        self.user_prompt = user_prompt
        self._started_at = datetime.now()
        self._completed_at: Optional[datetime] = None
        self._phases: Dict[WorkflowPhase, PhaseResult] = {}
        self._review_verdict: Optional[ReviewVerdict] = None

    def start_phase(self, phase: WorkflowPhase) -> None:
        """Mark a phase as started."""
        self._phases[phase] = PhaseResult(
            phase=phase,
            status=PhaseStatus.RUNNING,
            started_at=datetime.now(),
        )

    def complete_phase(
        self,
        phase: WorkflowPhase,
        session_id: Optional[str] = None,
        artifact_path: Optional[str] = None,
    ) -> None:
        """Mark a phase as completed."""
        if phase not in self._phases:
            self._phases[phase] = PhaseResult(phase=phase, status=PhaseStatus.PENDING)

        self._phases[phase].status = PhaseStatus.COMPLETED
        self._phases[phase].session_id = session_id
        self._phases[phase].artifact_path = artifact_path
        self._phases[phase].completed_at = datetime.now()

    def fail_phase(self, phase: WorkflowPhase, error: str) -> None:
        """Mark a phase as failed."""
        if phase not in self._phases:
            self._phases[phase] = PhaseResult(phase=phase, status=PhaseStatus.PENDING)

        self._phases[phase].status = PhaseStatus.FAILED
        self._phases[phase].error = error
        self._phases[phase].completed_at = datetime.now()

    def skip_phase(self, phase: WorkflowPhase) -> None:
        """Mark a phase as skipped."""
        self._phases[phase] = PhaseResult(
            phase=phase,
            status=PhaseStatus.SKIPPED,
        )

    def set_review_verdict(self, verdict: ReviewVerdict) -> None:
        """Set the review verdict."""
        self._review_verdict = verdict

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

    def get_result(self) -> WorkflowResult:
        """Get the workflow result."""
        return WorkflowResult(
            workflow_id=self.workflow_id,
            user_prompt=self.user_prompt,
            phases=self._phases.copy(),
            review_verdict=self._review_verdict,
            started_at=self._started_at,
            completed_at=self._completed_at or datetime.now(),
        )

    def generate_plan_prompt(self) -> str:
        """Generate prompt for Plan subagent."""
        return f"""Create a detailed implementation plan for: {self.user_prompt}

Save the plan to specs/ directory with a descriptive kebab-case filename.
Include:
- Task description and objective
- Relevant files analysis
- Step-by-step implementation tasks
- Acceptance criteria
- Validation commands

Think deeply about the best approach before documenting."""

    def generate_build_prompt(self, plan_path: str) -> str:
        """Generate prompt for Build subagent."""
        return f"""Implement the plan at: {plan_path}

Instructions:
- Read the plan carefully
- Implement all steps in order, top to bottom
- Do not skip any steps
- Run all validation commands before completing
- Fix any issues that arise during validation

Original request: {self.user_prompt}"""

    def generate_review_prompt(self, plan_path: str) -> str:
        """Generate prompt for Review subagent."""
        return f"""Review the changes made for: {self.user_prompt}
Reference plan: {plan_path}

Instructions:
- Analyze git diffs to understand what changed
- Categorize issues by risk tier:
  - BLOCKER: Security, breaking changes, data loss
  - HIGH RISK: Performance, missing error handling
  - MEDIUM RISK: Code quality, missing tests
  - LOW RISK: Style, minor improvements

- Write report to app_review/ directory with timestamp
- End with clear PASS or FAIL verdict
- FAIL if any BLOCKERS exist"""

    def generate_fix_prompt(self, plan_path: str, review_path: str) -> str:
        """Generate prompt for Fix subagent."""
        return f"""Fix issues identified in the review report.

Review report: {review_path}
Original plan: {plan_path}
Original request: {self.user_prompt}

Priority order:
1. Fix all BLOCKERS first (required)
2. Fix HIGH RISK issues (required)
3. Fix MEDIUM RISK if time permits
4. Document any skipped LOW RISK items

After fixing:
- Run validation commands from the plan
- Write fix report to app_fix_reports/ directory"""

    def format_summary(self) -> str:
        """Format a summary of the workflow result."""
        result = self.get_result()

        lines = [
            f"Workflow Complete: {self.workflow_id}",
            f"Request: {self.user_prompt[:50]}...",
            "",
            "Phase Results:",
        ]

        for phase in WorkflowPhase:
            phase_result = result.phases.get(phase)
            if phase_result:
                status = phase_result.status.value.upper()
                artifact = f" -> {phase_result.artifact_path}" if phase_result.artifact_path else ""
                lines.append(f"  - {phase.value.capitalize()}: {status}{artifact}")
            else:
                lines.append(f"  - {phase.value.capitalize()}: NOT RUN")

        if result.review_verdict:
            lines.append(f"\nReview Verdict: {result.review_verdict.value}")

        lines.append(f"\nOverall: {'SUCCESS' if result.is_success else 'ISSUES REMAIN'}")

        return "\n".join(lines)
