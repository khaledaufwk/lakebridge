"""
Agent Runner - High-level interface for running agent workflows.

This module provides a simple interface for running agent-based
migration workflows from the CLI or programmatically.

Usage:
    from databricks.labs.lakebridge.agent import AgentRunner
    
    runner = AgentRunner(working_dir="/path/to/project")
    
    # Run individual steps
    plan_result = await runner.plan("Migrate stored procedures")
    build_result = await runner.build(plan_path=plan_result.plan_path)
    review_result = await runner.review(prompt, plan_path)
    fix_result = await runner.fix(prompt, plan_path, review_path)
    answer = await runner.question("How does the transpiler work?")
    
    # Run full workflow
    result = await runner.migrate("Migrate stored procedures")
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databricks.labs.lakebridge.agent.agent_sdk import ModelName
from databricks.labs.lakebridge.agent.workflows.plan_step import run_plan_step
from databricks.labs.lakebridge.agent.workflows.build_step import run_build_step
from databricks.labs.lakebridge.agent.workflows.review_step import run_review_step
from databricks.labs.lakebridge.agent.workflows.fix_step import run_fix_step
from databricks.labs.lakebridge.agent.workflows.question_step import run_question_step
from databricks.labs.lakebridge.agent.workflows.full_workflow import (
    run_full_workflow,
    run_plan_only,
    run_plan_build,
    run_plan_build_review,
    WorkflowResult,
)


@dataclass
class PlanResult:
    """Result from the plan step."""
    success: bool
    session_id: str | None
    plan_path: str | None
    error: str | None = None


@dataclass
class BuildResult:
    """Result from the build step."""
    success: bool
    session_id: str | None
    error: str | None = None


@dataclass
class ReviewResult:
    """Result from the review step."""
    success: bool
    session_id: str | None
    verdict: str | None  # "PASS" or "FAIL"
    review_path: str | None
    error: str | None = None


@dataclass
class FixResult:
    """Result from the fix step."""
    success: bool
    session_id: str | None
    error: str | None = None


@dataclass
class QuestionResult:
    """Result from the question step."""
    success: bool
    answer: str | None
    error: str | None = None


class AgentRunner:
    """High-level interface for running agent workflows.
    
    Provides a simple API for running individual agent steps
    or complete workflows.
    """
    
    def __init__(
        self,
        working_dir: str | Path = ".",
        model: str = ModelName.OPUS.value,
        review_model: str | None = None,
        fix_model: str | None = None,
        question_model: str | None = None,
        verbose: bool = False,
    ):
        """Initialize the agent runner.
        
        Args:
            working_dir: Working directory for agent operations
            model: Default model for plan/build steps
            review_model: Model for review step (defaults to same as model)
            fix_model: Model for fix step (defaults to same as model)
            question_model: Model for question step (defaults to Sonnet)
            verbose: Whether to output verbose logs
        """
        self.working_dir = str(Path(working_dir).resolve())
        self.model = model
        self.review_model = review_model or model
        self.fix_model = fix_model or model
        self.question_model = question_model or ModelName.SONNET.value
        self.verbose = verbose
    
    # =========================================================================
    # Individual Steps
    # =========================================================================
    
    async def plan(
        self,
        prompt: str,
        model: str | None = None,
    ) -> PlanResult:
        """Create an implementation plan from a prompt.
        
        Args:
            prompt: Description of what to plan
            model: Model to use (defaults to runner's model)
            
        Returns:
            PlanResult with success status and plan path
        """
        try:
            success, session_id, plan_path = await run_plan_step(
                prompt=prompt,
                working_dir=self.working_dir,
                model=model or self.model,
                verbose=self.verbose,
            )
            return PlanResult(
                success=success,
                session_id=session_id,
                plan_path=plan_path,
            )
        except Exception as e:
            return PlanResult(
                success=False,
                session_id=None,
                plan_path=None,
                error=str(e),
            )
    
    async def build(
        self,
        plan_path: str,
        model: str | None = None,
    ) -> BuildResult:
        """Implement a plan.
        
        Args:
            plan_path: Path to the plan file
            model: Model to use (defaults to runner's model)
            
        Returns:
            BuildResult with success status
        """
        try:
            success, session_id = await run_build_step(
                plan_path=plan_path,
                working_dir=self.working_dir,
                model=model or self.model,
                verbose=self.verbose,
            )
            return BuildResult(
                success=success,
                session_id=session_id,
            )
        except Exception as e:
            return BuildResult(
                success=False,
                session_id=None,
                error=str(e),
            )
    
    async def review(
        self,
        prompt: str,
        plan_path: str,
        model: str | None = None,
    ) -> ReviewResult:
        """Review completed work.
        
        Args:
            prompt: Original prompt describing the work
            plan_path: Path to the plan file
            model: Model to use (defaults to runner's review_model)
            
        Returns:
            ReviewResult with verdict and review path
        """
        try:
            success, session_id, verdict, review_path = await run_review_step(
                prompt=prompt,
                plan_path=plan_path,
                working_dir=self.working_dir,
                model=model or self.review_model,
                verbose=self.verbose,
            )
            return ReviewResult(
                success=success,
                session_id=session_id,
                verdict=verdict,
                review_path=review_path,
            )
        except Exception as e:
            return ReviewResult(
                success=False,
                session_id=None,
                verdict=None,
                review_path=None,
                error=str(e),
            )
    
    async def fix(
        self,
        prompt: str,
        plan_path: str,
        review_path: str,
        model: str | None = None,
    ) -> FixResult:
        """Fix issues from a review.
        
        Args:
            prompt: Original prompt describing the work
            plan_path: Path to the plan file
            review_path: Path to the review report
            model: Model to use (defaults to runner's fix_model)
            
        Returns:
            FixResult with success status
        """
        try:
            success, session_id = await run_fix_step(
                prompt=prompt,
                plan_path=plan_path,
                review_path=review_path,
                working_dir=self.working_dir,
                model=model or self.fix_model,
                verbose=self.verbose,
            )
            return FixResult(
                success=success,
                session_id=session_id,
            )
        except Exception as e:
            return FixResult(
                success=False,
                session_id=None,
                error=str(e),
            )
    
    async def question(
        self,
        question: str,
        model: str | None = None,
    ) -> QuestionResult:
        """Answer a question about the codebase.
        
        Args:
            question: Question to answer
            model: Model to use (defaults to runner's question_model)
            
        Returns:
            QuestionResult with the answer
        """
        try:
            success, answer = await run_question_step(
                question=question,
                working_dir=self.working_dir,
                model=model or self.question_model,
                verbose=self.verbose,
            )
            return QuestionResult(
                success=success,
                answer=answer,
            )
        except Exception as e:
            return QuestionResult(
                success=False,
                answer=None,
                error=str(e),
            )
    
    # =========================================================================
    # Complete Workflows
    # =========================================================================
    
    async def migrate(
        self,
        prompt: str,
        model: str | None = None,
        review_model: str | None = None,
        fix_model: str | None = None,
        skip_review: bool = False,
        skip_fix: bool = False,
    ) -> WorkflowResult:
        """Run the full migration workflow: plan → build → review → fix.
        
        Args:
            prompt: Description of the migration to perform
            model: Model for plan/build (defaults to runner's model)
            review_model: Model for review (defaults to runner's review_model)
            fix_model: Model for fix (defaults to runner's fix_model)
            skip_review: Skip the review step
            skip_fix: Skip the fix step even if review fails
            
        Returns:
            WorkflowResult with all step details
        """
        return await run_full_workflow(
            prompt=prompt,
            working_dir=self.working_dir,
            model=model or self.model,
            review_model=review_model or self.review_model,
            fix_model=fix_model or self.fix_model,
            verbose=self.verbose,
            skip_review=skip_review,
            skip_fix=skip_fix,
        )
    
    async def plan_and_build(
        self,
        prompt: str,
        model: str | None = None,
    ) -> WorkflowResult:
        """Run plan and build steps only.
        
        Args:
            prompt: Description of the work
            model: Model to use (defaults to runner's model)
            
        Returns:
            WorkflowResult with plan and build details
        """
        return await run_plan_build(
            prompt=prompt,
            working_dir=self.working_dir,
            model=model or self.model,
            verbose=self.verbose,
        )
    
    async def plan_build_review(
        self,
        prompt: str,
        model: str | None = None,
        review_model: str | None = None,
    ) -> WorkflowResult:
        """Run plan, build, and review steps (no fix).
        
        Args:
            prompt: Description of the work
            model: Model for plan/build (defaults to runner's model)
            review_model: Model for review (defaults to runner's review_model)
            
        Returns:
            WorkflowResult with plan, build, and review details
        """
        return await run_plan_build_review(
            prompt=prompt,
            working_dir=self.working_dir,
            model=model or self.model,
            review_model=review_model or self.review_model,
            verbose=self.verbose,
        )
    
    # =========================================================================
    # Synchronous Wrappers
    # =========================================================================
    
    def plan_sync(self, prompt: str, model: str | None = None) -> PlanResult:
        """Synchronous wrapper for plan()."""
        return asyncio.run(self.plan(prompt, model))
    
    def build_sync(self, plan_path: str, model: str | None = None) -> BuildResult:
        """Synchronous wrapper for build()."""
        return asyncio.run(self.build(plan_path, model))
    
    def review_sync(
        self, prompt: str, plan_path: str, model: str | None = None
    ) -> ReviewResult:
        """Synchronous wrapper for review()."""
        return asyncio.run(self.review(prompt, plan_path, model))
    
    def fix_sync(
        self, prompt: str, plan_path: str, review_path: str, model: str | None = None
    ) -> FixResult:
        """Synchronous wrapper for fix()."""
        return asyncio.run(self.fix(prompt, plan_path, review_path, model))
    
    def question_sync(self, question: str, model: str | None = None) -> QuestionResult:
        """Synchronous wrapper for question()."""
        return asyncio.run(self.question(question, model))
    
    def migrate_sync(
        self,
        prompt: str,
        model: str | None = None,
        review_model: str | None = None,
        fix_model: str | None = None,
        skip_review: bool = False,
        skip_fix: bool = False,
    ) -> WorkflowResult:
        """Synchronous wrapper for migrate()."""
        return asyncio.run(self.migrate(
            prompt, model, review_model, fix_model, skip_review, skip_fix
        ))
