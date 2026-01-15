"""
Full Workflow - Orchestrates the complete plan → build → review → fix workflow.

This module runs the complete AI Developer Workflow (ADW) pattern:
1. Plan: Create implementation plan from user prompt
2. Build: Implement the plan
3. Review: Validate the implementation
4. Fix: Fix any issues found in review (if needed)

Usage:
    from databricks.labs.lakebridge.agent.workflows.full_workflow import run_full_workflow
    
    result = await run_full_workflow(
        prompt="Migrate stored procedures to Databricks",
        working_dir="/path/to/project",
    )
    
    if result.success:
        print(f"Workflow completed! Verdict: {result.verdict}")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from databricks.labs.lakebridge.agent.agent_sdk import ModelName
from databricks.labs.lakebridge.agent.logging import (
    AgentLogger,
    LogLevel,
    create_workflow_logger,
)
from databricks.labs.lakebridge.agent.workflows.plan_step import run_plan_step
from databricks.labs.lakebridge.agent.workflows.build_step import run_build_step
from databricks.labs.lakebridge.agent.workflows.review_step import run_review_step
from databricks.labs.lakebridge.agent.workflows.fix_step import run_fix_step


@dataclass
class WorkflowResult:
    """Result of a full workflow execution."""
    
    success: bool = False
    error: str | None = None
    
    # Step results
    plan_success: bool = False
    plan_path: str | None = None
    plan_session_id: str | None = None
    
    build_success: bool = False
    build_session_id: str | None = None
    
    review_success: bool = False
    review_session_id: str | None = None
    review_path: str | None = None
    verdict: str | None = None
    
    fix_success: bool = False
    fix_session_id: str | None = None
    fix_executed: bool = False
    
    # Timing
    duration_seconds: float = 0.0
    
    # Logger for accessing logs
    logger: AgentLogger | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "error": self.error,
            "plan_success": self.plan_success,
            "plan_path": self.plan_path,
            "plan_session_id": self.plan_session_id,
            "build_success": self.build_success,
            "build_session_id": self.build_session_id,
            "review_success": self.review_success,
            "review_session_id": self.review_session_id,
            "review_path": self.review_path,
            "verdict": self.verdict,
            "fix_success": self.fix_success,
            "fix_session_id": self.fix_session_id,
            "fix_executed": self.fix_executed,
            "duration_seconds": self.duration_seconds,
        }


async def run_full_workflow(
    prompt: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    review_model: str = ModelName.OPUS.value,
    fix_model: str = ModelName.OPUS.value,
    verbose: bool = False,
    skip_review: bool = False,
    skip_fix: bool = False,
) -> WorkflowResult:
    """Run the complete plan-build-review-fix workflow.
    
    Args:
        prompt: User prompt describing the work to be done
        working_dir: Working directory for the agent
        model: Model for plan and build steps (defaults to Opus)
        review_model: Model for review step (defaults to Opus)
        fix_model: Model for fix step (defaults to Opus)
        verbose: Whether to output verbose logs
        skip_review: Skip the review step
        skip_fix: Skip the fix step even if review fails
        
    Returns:
        WorkflowResult with success status and step details
    """
    result = WorkflowResult()
    workflow_start_time = time.time()
    
    # Create workflow logger
    logger = create_workflow_logger("plan_build_review_fix", working_dir, verbose)
    result.logger = logger
    
    # Log workflow start
    logger.log_workflow_start(
        workflow_type="plan_build_review_fix",
        prompt=prompt,
        working_dir=working_dir,
        model=model,
    )
    
    logger.log_system_event(
        level=LogLevel.INFO,
        message="Starting plan-build-review-fix workflow",
        metadata={
            "prompt": prompt,
            "working_dir": working_dir,
            "model": model,
            "review_model": review_model,
            "fix_model": fix_model,
            "skip_review": skip_review,
            "skip_fix": skip_fix,
        },
    )
    
    try:
        # =====================================================================
        # Step 1: Plan
        # =====================================================================
        logger.log_system_event(
            level=LogLevel.INFO,
            message="Step 1/4: Plan",
            metadata={"prompt_preview": prompt[:100]},
        )
        
        plan_success, plan_session_id, plan_path = await run_plan_step(
            prompt=prompt,
            working_dir=working_dir,
            model=model,
            logger=logger,
            verbose=verbose,
        )
        
        result.plan_success = plan_success
        result.plan_session_id = plan_session_id
        result.plan_path = plan_path
        
        if not plan_success or not plan_path:
            result.error = "Plan step failed"
            logger.log_system_event(
                level=LogLevel.ERROR,
                message="Plan step failed - workflow aborted",
            )
            return result
        
        # =====================================================================
        # Step 2: Build
        # =====================================================================
        logger.log_system_event(
            level=LogLevel.INFO,
            message="Step 2/4: Build",
            metadata={"plan_path": plan_path},
        )
        
        build_success, build_session_id = await run_build_step(
            plan_path=plan_path,
            working_dir=working_dir,
            model=model,
            logger=logger,
            verbose=verbose,
        )
        
        result.build_success = build_success
        result.build_session_id = build_session_id
        
        if not build_success:
            result.error = "Build step failed"
            logger.log_system_event(
                level=LogLevel.ERROR,
                message="Build step failed - workflow aborted",
            )
            return result
        
        # =====================================================================
        # Step 3: Review (optional)
        # =====================================================================
        if skip_review:
            logger.log_system_event(
                level=LogLevel.INFO,
                message="Step 3/4: Review - SKIPPED",
            )
            result.review_success = True
            result.verdict = "SKIPPED"
        else:
            logger.log_system_event(
                level=LogLevel.INFO,
                message="Step 3/4: Review",
                metadata={"plan_path": plan_path},
            )
            
            review_success, review_session_id, verdict, review_path = await run_review_step(
                prompt=prompt,
                plan_path=plan_path,
                working_dir=working_dir,
                model=review_model,
                logger=logger,
                verbose=verbose,
            )
            
            result.review_success = review_success
            result.review_session_id = review_session_id
            result.verdict = verdict
            result.review_path = review_path
            
            if not review_success:
                result.error = "Review step failed"
                logger.log_system_event(
                    level=LogLevel.ERROR,
                    message="Review step failed - workflow aborted",
                )
                return result
        
        # =====================================================================
        # Step 4: Fix (conditional on review verdict)
        # =====================================================================
        if skip_fix or skip_review:
            logger.log_system_event(
                level=LogLevel.INFO,
                message="Step 4/4: Fix - SKIPPED",
            )
            result.fix_executed = False
        elif result.verdict == "FAIL":
            logger.log_system_event(
                level=LogLevel.INFO,
                message="Step 4/4: Fix - Review found issues, proceeding to fix",
                metadata={"review_path": result.review_path},
            )
            
            if not result.review_path:
                result.error = "Review path not found for fix step"
                return result
            
            fix_success, fix_session_id = await run_fix_step(
                prompt=prompt,
                plan_path=plan_path,
                review_path=result.review_path,
                working_dir=working_dir,
                model=fix_model,
                logger=logger,
                verbose=verbose,
            )
            
            result.fix_success = fix_success
            result.fix_session_id = fix_session_id
            result.fix_executed = True
            
            if not fix_success:
                result.error = "Fix step failed"
                logger.log_system_event(
                    level=LogLevel.ERROR,
                    message="Fix step failed",
                )
                # Don't return - workflow can still be considered partially successful
        else:
            logger.log_system_event(
                level=LogLevel.INFO,
                message="Step 4/4: Fix - SKIPPED (review passed)",
                metadata={"verdict": result.verdict},
            )
            result.fix_executed = False
        
        # =====================================================================
        # Workflow Complete
        # =====================================================================
        result.success = True
        result.duration_seconds = time.time() - workflow_start_time
        
        logger.log_workflow_end(
            success=True,
            duration_seconds=int(result.duration_seconds),
            verdict=result.verdict,
        )
        
        logger.log_system_event(
            level=LogLevel.INFO,
            message=f"Workflow completed successfully in {int(result.duration_seconds)}s",
            metadata={
                "verdict": result.verdict,
                "fix_executed": result.fix_executed,
                "plan_path": plan_path,
            },
        )
        
        return result
        
    except Exception as e:
        result.error = str(e)
        result.duration_seconds = time.time() - workflow_start_time
        
        logger.log_workflow_end(
            success=False,
            duration_seconds=int(result.duration_seconds),
            error=str(e),
        )
        
        logger.log_system_event(
            level=LogLevel.ERROR,
            message=f"Workflow exception: {e}",
        )
        
        return result


async def run_plan_only(
    prompt: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    verbose: bool = False,
) -> WorkflowResult:
    """Run only the plan step.
    
    Args:
        prompt: User prompt describing the work to plan
        working_dir: Working directory for the agent
        model: Model to use (defaults to Opus)
        verbose: Whether to output verbose logs
        
    Returns:
        WorkflowResult with plan details
    """
    result = WorkflowResult()
    workflow_start_time = time.time()
    
    logger = create_workflow_logger("plan", working_dir, verbose)
    result.logger = logger
    
    plan_success, plan_session_id, plan_path = await run_plan_step(
        prompt=prompt,
        working_dir=working_dir,
        model=model,
        logger=logger,
        verbose=verbose,
    )
    
    result.plan_success = plan_success
    result.plan_session_id = plan_session_id
    result.plan_path = plan_path
    result.success = plan_success
    result.duration_seconds = time.time() - workflow_start_time
    
    if not plan_success:
        result.error = "Plan step failed"
    
    return result


async def run_plan_build(
    prompt: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    verbose: bool = False,
) -> WorkflowResult:
    """Run plan and build steps only.
    
    Args:
        prompt: User prompt describing the work
        working_dir: Working directory for the agent
        model: Model to use (defaults to Opus)
        verbose: Whether to output verbose logs
        
    Returns:
        WorkflowResult with plan and build details
    """
    return await run_full_workflow(
        prompt=prompt,
        working_dir=working_dir,
        model=model,
        verbose=verbose,
        skip_review=True,
        skip_fix=True,
    )


async def run_plan_build_review(
    prompt: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    review_model: str = ModelName.OPUS.value,
    verbose: bool = False,
) -> WorkflowResult:
    """Run plan, build, and review steps (no fix).
    
    Args:
        prompt: User prompt describing the work
        working_dir: Working directory for the agent
        model: Model for plan and build (defaults to Opus)
        review_model: Model for review (defaults to Opus)
        verbose: Whether to output verbose logs
        
    Returns:
        WorkflowResult with plan, build, and review details
    """
    return await run_full_workflow(
        prompt=prompt,
        working_dir=working_dir,
        model=model,
        review_model=review_model,
        verbose=verbose,
        skip_fix=True,
    )
