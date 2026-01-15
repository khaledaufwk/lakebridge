"""
Review Step - Reviews completed work and produces validation reports.

This step analyzes git diffs, identifies issues across four risk tiers
(Blocker, High, Medium, Low), and produces comprehensive validation reports
in the app_review/ directory.

Usage:
    from databricks.labs.lakebridge.agent.workflows.review_step import run_review_step
    
    success, session_id, verdict, review_path = await run_review_step(
        prompt="Migrate stored procedures to Databricks",
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


STEP_NAME = "review"

# Default tools for the review step (limited - analysis only)
REVIEW_TOOLS = [
    "Read", "Glob", "Grep", "Bash", "Write", "Skill",
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


def extract_verdict(result_text: str | None) -> str | None:
    """Extract PASS/FAIL verdict from review result.
    
    Args:
        result_text: The result text from the review
        
    Returns:
        "PASS", "FAIL", or None if not determinable
    """
    if not result_text:
        return None
    
    upper_text = result_text.upper()
    
    # Look for explicit verdict
    if "PASS" in upper_text and "FAIL" not in upper_text:
        return "PASS"
    elif "FAIL" in upper_text:
        return "FAIL"
    
    return None


def find_review_file(working_dir: str) -> str | None:
    """Find the most recently created review file.
    
    Args:
        working_dir: Working directory to search
        
    Returns:
        Path to the review file, or None if not found
    """
    review_dir = Path(working_dir) / "app_review"
    if not review_dir.exists():
        return None
    
    review_files = sorted(
        review_dir.glob("review_*.md"),
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )
    
    if review_files:
        return str(review_files[0])
    
    return None


async def run_review_step(
    prompt: str,
    plan_path: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    logger: AgentLogger | None = None,
    verbose: bool = False,
) -> tuple[bool, str | None, str | None, str | None]:
    """Run the /review step.
    
    Analyzes completed work, identifies issues across risk tiers,
    and produces a comprehensive validation report.
    
    Args:
        prompt: Original user prompt describing the work
        plan_path: Path to the plan file that was implemented
        working_dir: Working directory for the agent
        model: Model to use (defaults to Opus for thorough analysis)
        logger: Optional logger instance
        verbose: Whether to output verbose logs
        
    Returns:
        Tuple of (success, session_id, verdict, review_path)
        - verdict is "PASS" or "FAIL" extracted from the review
        - review_path is the path to the generated review file
    """
    # Validate plan path exists
    if not Path(plan_path).exists():
        raise ValueError(f"Plan file not found: {plan_path}")
    
    # Create logger if not provided
    if logger is None:
        from databricks.labs.lakebridge.agent.logging import create_workflow_logger
        logger = create_workflow_logger("review", working_dir, verbose)
    
    logger.step = STEP_NAME
    step_start_time = time.time()
    
    # Ensure review output directory exists
    review_dir = Path(working_dir) / "app_review"
    review_dir.mkdir(parents=True, exist_ok=True)
    
    # Log step start
    logger.log_step_start(
        step=STEP_NAME,
        payload={
            "prompt": prompt,
            "plan_path": plan_path,
            "model": model,
            "working_dir": working_dir,
        },
        summary=f"Starting review step for: {prompt[:80]}...",
    )
    
    try:
        # Build the review command
        # Escape quotes in prompt to prevent command injection
        escaped_prompt = prompt.replace('"', '\\"')
        review_command = f'/review "{escaped_prompt}" {plan_path}'
        
        # Build the agent query
        query_input = QueryInput(
            prompt=review_command,
            options=QueryOptions(
                model=model,
                cwd=working_dir,
                allowed_tools=REVIEW_TOOLS,
                hooks=create_logging_hooks(logger),
                bypass_permissions=True,
            ),
            handlers=create_message_handlers(logger),
        )
        
        # Execute the agent
        result = await query_to_completion(query_input)
        
        duration_ms = int((time.time() - step_start_time) * 1000)
        
        # Extract verdict from result
        verdict = extract_verdict(result.result)
        
        # Find the review file
        review_path = find_review_file(working_dir)
        
        if result.success:
            verdict_emoji = "✅" if verdict == "PASS" else "⚠️" if verdict == "FAIL" else "❓"
            logger.log_step_end(
                status=StepStatus.SUCCESS,
                duration_ms=duration_ms,
                payload={
                    "session_id": result.session_id,
                    "verdict": verdict,
                    "review_path": review_path,
                },
                summary=f"Review completed: {verdict_emoji} {verdict or 'Unknown'}",
            )
            return True, result.session_id, verdict, review_path
        else:
            logger.log_step_end(
                status=StepStatus.FAILED,
                duration_ms=duration_ms,
                error=result.error,
                summary=f"Review step failed: {result.error}",
            )
            return False, None, None, None
    
    except Exception as e:
        duration_ms = int((time.time() - step_start_time) * 1000)
        logger.log_step_end(
            status=StepStatus.FAILED,
            duration_ms=duration_ms,
            error=str(e),
            summary=f"Review step exception: {e}",
        )
        return False, None, None, None
