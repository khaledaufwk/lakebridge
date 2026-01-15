"""
Question Step - Answers questions about the codebase without making changes.

This step analyzes the project structure and documentation to answer
user questions in a read-only manner. No files are modified.

Usage:
    from databricks.labs.lakebridge.agent.workflows.question_step import run_question_step
    
    success, answer = await run_question_step(
        question="How does the transpiler handle stored procedures?",
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


STEP_NAME = "question"

# Default tools for the question step (read-only)
QUESTION_TOOLS = [
    "Bash(git ls-files:*)",  # Only git ls-files allowed
    "Read",
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


async def run_question_step(
    question: str,
    working_dir: str,
    model: str = ModelName.SONNET.value,
    logger: AgentLogger | None = None,
    verbose: bool = False,
) -> tuple[bool, str | None]:
    """Run the /question step.
    
    Answers questions about the codebase by analyzing project structure
    and documentation. This is a read-only operation.
    
    Args:
        question: User question about the codebase
        working_dir: Working directory for the agent
        model: Model to use (defaults to Sonnet for faster responses)
        logger: Optional logger instance
        verbose: Whether to output verbose logs
        
    Returns:
        Tuple of (success, answer)
    """
    # Create logger if not provided
    if logger is None:
        from databricks.labs.lakebridge.agent.logging import create_workflow_logger
        logger = create_workflow_logger("question", working_dir, verbose)
    
    logger.step = STEP_NAME
    step_start_time = time.time()
    
    # Log step start
    logger.log_step_start(
        step=STEP_NAME,
        payload={"question": question, "model": model, "working_dir": working_dir},
        summary=f"Answering question: {question[:100]}...",
    )
    
    try:
        # Build the agent query
        query_input = QueryInput(
            prompt=f"/question {question}",
            options=QueryOptions(
                model=model,
                cwd=working_dir,
                allowed_tools=QUESTION_TOOLS,
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
                summary="Question answered successfully",
            )
            return True, result.result
        else:
            logger.log_step_end(
                status=StepStatus.FAILED,
                duration_ms=duration_ms,
                error=result.error,
                summary=f"Question step failed: {result.error}",
            )
            return False, None
    
    except Exception as e:
        duration_ms = int((time.time() - step_start_time) * 1000)
        logger.log_step_end(
            status=StepStatus.FAILED,
            duration_ms=duration_ms,
            error=str(e),
            summary=f"Question step exception: {e}",
        )
        return False, None
