"""
Build Step - Implements plans from the plan step.

This step reads a plan file and implements it top-to-bottom,
executing all tasks, running validation commands, and reporting
the completed work.

Usage:
    from databricks.labs.lakebridge.agent.workflows.build_step import run_build_step
    
    success, session_id = await run_build_step(
        plan_path="/path/to/specs/migration-plan.md",
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


STEP_NAME = "build"

# Default tools for the build step
BUILD_TOOLS = [
    "Read", "Glob", "Grep", "Bash", "Write", "Edit",
    "Task", "TodoWrite", "WebFetch", "WebSearch", "Skill",
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


async def run_build_step(
    plan_path: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    logger: AgentLogger | None = None,
    verbose: bool = False,
) -> tuple[bool, str | None]:
    """Run the /build step.
    
    Implements the plan file top-to-bottom, executing all tasks
    and running validation commands.
    
    Args:
        plan_path: Path to the plan file to implement
        working_dir: Working directory for the agent
        model: Model to use (defaults to Opus)
        logger: Optional logger instance
        verbose: Whether to output verbose logs
        
    Returns:
        Tuple of (success, session_id)
    """
    # Validate plan path exists
    if not Path(plan_path).exists():
        raise ValueError(f"Plan file not found: {plan_path}")
    
    # Create logger if not provided
    if logger is None:
        from databricks.labs.lakebridge.agent.logging import create_workflow_logger
        logger = create_workflow_logger("build", working_dir, verbose)
    
    logger.step = STEP_NAME
    step_start_time = time.time()
    
    # Log step start
    logger.log_step_start(
        step=STEP_NAME,
        payload={"plan_path": plan_path, "model": model, "working_dir": working_dir},
        summary=f"Starting build step with plan: {plan_path}",
    )
    
    try:
        # Build the agent query
        query_input = QueryInput(
            prompt=f"/build {plan_path}",
            options=QueryOptions(
                model=model,
                cwd=working_dir,
                allowed_tools=BUILD_TOOLS,
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
                summary="Build step completed successfully",
            )
            return True, result.session_id
        else:
            logger.log_step_end(
                status=StepStatus.FAILED,
                duration_ms=duration_ms,
                error=result.error,
                summary=f"Build step failed: {result.error}",
            )
            return False, None
    
    except Exception as e:
        duration_ms = int((time.time() - step_start_time) * 1000)
        logger.log_step_end(
            status=StepStatus.FAILED,
            duration_ms=duration_ms,
            error=str(e),
            summary=f"Build step exception: {e}",
        )
        return False, None
