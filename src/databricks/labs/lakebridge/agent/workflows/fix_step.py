"""
Fix Step - Fixes issues identified in code review reports.

This step reads a code review report and implements recommended solutions,
prioritizing by risk tier (Blockers → High → Medium → Low), then validates
fixes and generates a fix report.

Usage:
    from databricks.labs.lakebridge.agent.workflows.fix_step import run_fix_step
    
    success, session_id = await run_fix_step(
        prompt="Migrate stored procedures to Databricks",
        plan_path="/path/to/specs/migration-plan.md",
        review_path="/path/to/app_review/review_20240115.md",
        working_dir="/path/to/project",
    )
"""

from __future__ import annotations

import time
from pathlib import Path

from databricks.labs.lakebridge.agent.agent_sdk import (
    ModelName,
    QueryInput,
    QueryOptions,
    MessageHandlers,
    HooksConfig,
    HookMatcher,
    HookInput,
    HookResponse,
    HookContext,
    PreToolUseInput,
    PostToolUseInput,
    StopInput,
    query_to_completion,
)
from databricks.labs.lakebridge.agent.logging import AgentLogger, StepStatus


STEP_NAME = "fix"

# Default tools for the fix step
FIX_TOOLS = [
    "Read", "Glob", "Grep", "Bash", "Write", "Edit",
    "Task", "TodoWrite", "Skill",
]


def create_logging_hooks(logger: AgentLogger) -> HooksConfig:
    """Create hooks that log tool events.
    
    Args:
        logger: AgentLogger instance for logging
        
    Returns:
        HooksConfig with pre/post tool use hooks
    """
    async def pre_tool_use_hook(
        hook_input: HookInput,
        tool_use_id: str | None,
        context: HookContext,
    ) -> HookResponse:
        """Log tool call before execution."""
        if not isinstance(hook_input, PreToolUseInput):
            return HookResponse.allow()
        
        logger.log_tool_start(
            tool_name=hook_input.tool_name,
            tool_input=hook_input.tool_input,
            tool_use_id=tool_use_id,
        )
        return HookResponse.allow()
    
    async def post_tool_use_hook(
        hook_input: HookInput,
        tool_use_id: str | None,
        context: HookContext,
    ) -> HookResponse:
        """Log tool usage after execution."""
        if not isinstance(hook_input, PostToolUseInput):
            return HookResponse.allow()
        
        logger.log_tool_end(
            tool_name=hook_input.tool_name,
            tool_input=hook_input.tool_input,
            tool_response=hook_input.tool_response,
            tool_use_id=tool_use_id,
        )
        return HookResponse.allow()
    
    async def stop_hook(
        hook_input: HookInput,
        tool_use_id: str | None,
        context: HookContext,
    ) -> HookResponse:
        """Log when agent stops."""
        from databricks.labs.lakebridge.agent.logging import LogLevel
        
        if not isinstance(hook_input, StopInput):
            return HookResponse.allow()
        
        logger.log_system_event(
            level=LogLevel.INFO,
            message="Agent stopped",
            metadata={"stop_hook_active": hook_input.stop_hook_active},
        )
        return HookResponse.allow()
    
    return HooksConfig(
        pre_tool_use=[HookMatcher(matcher=None, hooks=[pre_tool_use_hook], timeout=30)],
        post_tool_use=[HookMatcher(matcher=None, hooks=[post_tool_use_hook], timeout=30)],
        stop=[HookMatcher(matcher=None, hooks=[stop_hook], timeout=30)],
    )


def create_message_handlers(logger: AgentLogger) -> MessageHandlers:
    """Create handlers that log agent responses.
    
    Args:
        logger: AgentLogger instance for logging
        
    Returns:
        MessageHandlers for response logging
    """
    async def on_assistant_block(block) -> None:
        """Log individual assistant message blocks."""
        block_type = type(block).__name__
        
        if block_type == "TextBlock":
            logger.log_text_response(getattr(block, "text", ""))
        elif block_type == "ThinkingBlock":
            logger.log_thinking_response(getattr(block, "thinking", ""))
    
    async def on_result(msg) -> None:
        """Log final result."""
        logger.log_result(
            result=msg.result,
            success=msg.subtype.value == "success" if msg.subtype else False,
            usage=msg.usage.model_dump() if msg.usage else None,
            session_id=msg.session_id,
        )
    
    return MessageHandlers(
        on_assistant_block=on_assistant_block,
        on_result=on_result,
    )


async def run_fix_step(
    prompt: str,
    plan_path: str,
    review_path: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    logger: AgentLogger | None = None,
    verbose: bool = False,
) -> tuple[bool, str | None]:
    """Run the /fix step.
    
    Reads the review report and implements recommended solutions,
    prioritizing by risk tier.
    
    Args:
        prompt: Original user prompt describing the work
        plan_path: Path to the plan file
        review_path: Path to the review report with issues
        working_dir: Working directory for the agent
        model: Model to use (defaults to Opus)
        logger: Optional logger instance
        verbose: Whether to output verbose logs
        
    Returns:
        Tuple of (success, session_id)
    """
    # Validate paths exist
    if not Path(plan_path).exists():
        raise ValueError(f"Plan file not found: {plan_path}")
    if not Path(review_path).exists():
        raise ValueError(f"Review file not found: {review_path}")
    
    # Create logger if not provided
    if logger is None:
        from databricks.labs.lakebridge.agent.logging import create_workflow_logger
        logger = create_workflow_logger("fix", working_dir, verbose)
    
    logger.step = STEP_NAME
    step_start_time = time.time()
    
    # Ensure fix report output directory exists
    fix_dir = Path(working_dir) / "app_fix_reports"
    fix_dir.mkdir(parents=True, exist_ok=True)
    
    # Log step start
    logger.log_step_start(
        step=STEP_NAME,
        payload={
            "prompt": prompt,
            "plan_path": plan_path,
            "review_path": review_path,
            "model": model,
            "working_dir": working_dir,
        },
        summary=f"Starting fix step with review: {review_path}",
    )
    
    try:
        # Build the fix command
        # Escape quotes in prompt to prevent command injection
        escaped_prompt = prompt.replace('"', '\\"')
        fix_command = f'/fix "{escaped_prompt}" {plan_path} {review_path}'
        
        # Build the agent query
        query_input = QueryInput(
            prompt=fix_command,
            options=QueryOptions(
                model=model,
                cwd=working_dir,
                allowed_tools=FIX_TOOLS,
                hooks=create_logging_hooks(logger),
                bypass_permissions=True,
            ),
            handlers=create_message_handlers(logger),
        )
        
        # Execute the agent
        result = await query_to_completion(query_input)
        
        duration_ms = int((time.time() - step_start_time) * 1000)
        
        if result.success:
            logger.log_step_end(
                status=StepStatus.SUCCESS,
                duration_ms=duration_ms,
                payload={"session_id": result.session_id},
                summary="Fix step completed successfully",
            )
            return True, result.session_id
        else:
            logger.log_step_end(
                status=StepStatus.FAILED,
                duration_ms=duration_ms,
                error=result.error,
                summary=f"Fix step failed: {result.error}",
            )
            return False, None
    
    except Exception as e:
        duration_ms = int((time.time() - step_start_time) * 1000)
        logger.log_step_end(
            status=StepStatus.FAILED,
            duration_ms=duration_ms,
            error=str(e),
            summary=f"Fix step exception: {e}",
        )
        return False, None
