"""
Plan Step - Creates implementation plans based on user requirements.

This step analyzes user prompts and creates detailed implementation plans
saved to the specs/ directory. Plans include task descriptions, relevant files,
step-by-step tasks, acceptance criteria, and validation commands.

Usage:
    from databricks.labs.lakebridge.agent.workflows.plan_step import run_plan_step
    
    success, session_id, plan_path = await run_plan_step(
        prompt="Migrate stored procedures to Databricks",
        working_dir="/path/to/project",
    )
"""

from __future__ import annotations

import re
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
    quick_prompt,
    AdhocPrompt,
)
from databricks.labs.lakebridge.agent.logging import AgentLogger, StepStatus


STEP_NAME = "plan"

# Default tools for the plan step
PLAN_TOOLS = [
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


async def extract_plan_path(
    working_dir: str,
    session_id: str | None,
    model: str = ModelName.OPUS.value,
) -> str | None:
    """Use quick_prompt to extract the plan file path.
    
    Args:
        working_dir: Working directory
        session_id: Session ID from plan step
        model: Model to use for extraction
        
    Returns:
        Path to the plan file, or None if not found
    """
    extraction_prompt = """You just ran /plan and created a plan file.

IMPORTANT: Respond with ONLY the absolute file path to the plan file you created.
- No explanation
- No markdown
- No quotes
- Just the raw file path on a single line

Example correct response:
/Users/user/project/specs/feature-plan.md

What is the absolute path to the plan file you created?"""

    try:
        result = await quick_prompt(AdhocPrompt(
            prompt=extraction_prompt,
            model=model,
            cwd=working_dir,
        ))

        if result:
            # Clean up the result
            path = result.strip().strip("`").strip('"').strip("'").strip()
            path = path.split("\n")[0].strip()

            if path.startswith("/") or path.startswith("./"):
                return path
            else:
                # Try to extract a path from the response
                match = re.search(r'(/[^\s]+\.md)', result)
                if match:
                    return match.group(1)

        # Fallback: look for recently created .md files in specs/
        specs_dir = Path(working_dir) / "specs"
        if specs_dir.exists():
            md_files = sorted(specs_dir.glob("*.md"), key=lambda f: f.stat().st_mtime, reverse=True)
            if md_files:
                return str(md_files[0])

        return None

    except Exception:
        # Fallback: look for recently created .md files in specs/
        specs_dir = Path(working_dir) / "specs"
        if specs_dir.exists():
            md_files = sorted(specs_dir.glob("*.md"), key=lambda f: f.stat().st_mtime, reverse=True)
            if md_files:
                return str(md_files[0])
        return None


async def run_plan_step(
    prompt: str,
    working_dir: str,
    model: str = ModelName.OPUS.value,
    logger: AgentLogger | None = None,
    verbose: bool = False,
) -> tuple[bool, str | None, str | None]:
    """Run the /plan step.
    
    Creates a detailed implementation plan based on the user's prompt.
    The plan is saved to specs/<descriptive-name>.md
    
    Args:
        prompt: User prompt describing what to plan
        working_dir: Working directory for the agent
        model: Model to use (defaults to Opus)
        logger: Optional logger instance
        verbose: Whether to output verbose logs
        
    Returns:
        Tuple of (success, session_id, plan_path)
    """
    # Create logger if not provided
    if logger is None:
        from databricks.labs.lakebridge.agent.logging import create_workflow_logger
        logger = create_workflow_logger("plan", working_dir, verbose)
    
    logger.step = STEP_NAME
    step_start_time = time.time()
    
    # Log step start
    logger.log_step_start(
        step=STEP_NAME,
        payload={"prompt": prompt, "model": model, "working_dir": working_dir},
        summary=f"Starting plan step for: {prompt[:100]}...",
    )
    
    # Ensure specs directory exists
    specs_dir = Path(working_dir) / "specs"
    specs_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Build the agent query
        query_input = QueryInput(
            prompt=f"/plan {prompt}",
            options=QueryOptions(
                model=model,
                cwd=working_dir,
                allowed_tools=PLAN_TOOLS,
                hooks=create_logging_hooks(logger),
                bypass_permissions=True,
            ),
            handlers=create_message_handlers(logger),
        )
        
        # Execute the agent
        result = await query_to_completion(query_input)
        
        duration_ms = int((time.time() - step_start_time) * 1000)
        
        if result.success:
            # Extract plan file path
            plan_path = await extract_plan_path(working_dir, result.session_id, model)
            
            logger.log_step_end(
                status=StepStatus.SUCCESS,
                duration_ms=duration_ms,
                payload={"session_id": result.session_id, "plan_path": plan_path},
                summary=f"Plan step completed successfully. Plan: {plan_path}",
            )
            return True, result.session_id, plan_path
        else:
            logger.log_step_end(
                status=StepStatus.FAILED,
                duration_ms=duration_ms,
                error=result.error,
                summary=f"Plan step failed: {result.error}",
            )
            return False, None, None
    
    except Exception as e:
        duration_ms = int((time.time() - step_start_time) * 1000)
        logger.log_step_end(
            status=StepStatus.FAILED,
            duration_ms=duration_ms,
            error=str(e),
            summary=f"Plan step exception: {e}",
        )
        return False, None, None
