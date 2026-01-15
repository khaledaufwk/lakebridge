"""
Lakebridge Agent Logging - Logging utilities for agent workflows.

This module provides structured logging for agent workflow steps,
tool usage, and system events. Logs are output to both console
and can be stored for later analysis.

Usage:
    from databricks.labs.lakebridge.agent.logging import AgentLogger
    
    logger = AgentLogger(workflow_id="abc123", step="plan")
    logger.log_step_start(prompt="Migrate stored procedures")
    logger.log_tool_use("Read", {"file_path": "/path/to/file"})
    logger.log_step_end(status="success", duration_ms=5000)
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class LogLevel(str, Enum):
    """Log level enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class EventCategory(str, Enum):
    """Event category for structured logging."""
    SYSTEM = "system"
    STEP = "step"
    HOOK = "hook"
    RESPONSE = "response"
    TOOL = "tool"


class StepStatus(str, Enum):
    """Status for workflow steps."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class LogEntry:
    """Structured log entry for agent workflows."""
    
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)
    workflow_id: str = ""
    step: str | None = None
    agent_id: str | None = None
    
    # Event classification
    category: EventCategory = EventCategory.SYSTEM
    event_type: str = ""
    level: LogLevel = LogLevel.INFO
    
    # Content
    message: str = ""
    content: str = ""
    summary: str = ""
    
    # Metadata
    payload: dict[str, Any] = field(default_factory=dict)
    duration_ms: int | None = None
    status: StepStatus | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "workflow_id": self.workflow_id,
            "step": self.step,
            "agent_id": self.agent_id,
            "category": self.category.value,
            "event_type": self.event_type,
            "level": self.level.value,
            "message": self.message,
            "content": self.content,
            "summary": self.summary,
            "payload": self.payload,
            "duration_ms": self.duration_ms,
            "status": self.status.value if self.status else None,
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class AgentLogger:
    """Structured logger for agent workflows.
    
    Provides methods for logging different types of events during
    agent workflow execution, including step starts/ends, tool usage,
    and system events.
    """
    
    def __init__(
        self,
        workflow_id: str | None = None,
        step: str | None = None,
        agent_id: str | None = None,
        log_file: Path | None = None,
        verbose: bool = False,
    ):
        """Initialize the agent logger.
        
        Args:
            workflow_id: Unique identifier for the workflow
            step: Current step name (plan, build, review, fix, question)
            agent_id: Agent identifier
            log_file: Optional file path to write logs
            verbose: Whether to output verbose logs to console
        """
        self.workflow_id = workflow_id or str(uuid.uuid4())
        self.step = step
        self.agent_id = agent_id
        self.log_file = log_file
        self.verbose = verbose
        self.entries: list[LogEntry] = []
        self._step_start_time: float | None = None
        
    def _create_entry(
        self,
        category: EventCategory,
        event_type: str,
        message: str,
        level: LogLevel = LogLevel.INFO,
        content: str = "",
        summary: str = "",
        payload: dict[str, Any] | None = None,
        duration_ms: int | None = None,
        status: StepStatus | None = None,
    ) -> LogEntry:
        """Create a log entry with common fields populated."""
        entry = LogEntry(
            workflow_id=self.workflow_id,
            step=self.step,
            agent_id=self.agent_id,
            category=category,
            event_type=event_type,
            level=level,
            message=message,
            content=content,
            summary=summary or message[:100],
            payload=payload or {},
            duration_ms=duration_ms,
            status=status,
        )
        return entry
    
    def _log_entry(self, entry: LogEntry) -> str:
        """Log an entry and return its ID."""
        self.entries.append(entry)
        
        # Log to Python logger
        log_level = getattr(logging, entry.level.value)
        logger.log(log_level, f"[{entry.category.value}] {entry.message}")
        
        # Verbose console output
        if self.verbose:
            print(f"[{entry.timestamp.strftime('%H:%M:%S')}] [{entry.category.value}] {entry.message}")
        
        # Write to file if configured
        if self.log_file:
            with open(self.log_file, "a") as f:
                f.write(entry.to_json() + "\n")
        
        return entry.id
    
    # ==========================================================================
    # Step Logging
    # ==========================================================================
    
    def log_step_start(
        self,
        step: str | None = None,
        payload: dict[str, Any] | None = None,
        summary: str = "",
    ) -> str:
        """Log the start of a workflow step.
        
        Args:
            step: Step name (overrides instance step)
            payload: Additional data for the step
            summary: Human-readable summary
            
        Returns:
            Log entry ID
        """
        if step:
            self.step = step
        
        self._step_start_time = time.time()
        
        entry = self._create_entry(
            category=EventCategory.STEP,
            event_type="step_start",
            message=f"Starting {self.step} step",
            summary=summary or f"Starting {self.step} step",
            payload=payload,
            status=StepStatus.IN_PROGRESS,
        )
        return self._log_entry(entry)
    
    def log_step_end(
        self,
        status: StepStatus = StepStatus.SUCCESS,
        duration_ms: int | None = None,
        payload: dict[str, Any] | None = None,
        summary: str = "",
        error: str | None = None,
    ) -> str:
        """Log the end of a workflow step.
        
        Args:
            status: Final status of the step
            duration_ms: Duration in milliseconds (auto-calculated if not provided)
            payload: Additional data for the step
            summary: Human-readable summary
            error: Error message if step failed
            
        Returns:
            Log entry ID
        """
        if duration_ms is None and self._step_start_time:
            duration_ms = int((time.time() - self._step_start_time) * 1000)
        
        level = LogLevel.INFO if status == StepStatus.SUCCESS else LogLevel.ERROR
        message = f"Completed {self.step} step: {status.value}"
        if error:
            message = f"Failed {self.step} step: {error}"
        
        final_payload = payload or {}
        if error:
            final_payload["error"] = error
        
        entry = self._create_entry(
            category=EventCategory.STEP,
            event_type="step_end",
            message=message,
            level=level,
            summary=summary or message,
            payload=final_payload,
            duration_ms=duration_ms,
            status=status,
        )
        
        self._step_start_time = None
        return self._log_entry(entry)
    
    # ==========================================================================
    # Tool Logging
    # ==========================================================================
    
    def log_tool_start(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        tool_use_id: str | None = None,
    ) -> str:
        """Log the start of a tool execution.
        
        Args:
            tool_name: Name of the tool being used
            tool_input: Input parameters for the tool
            tool_use_id: Unique identifier for this tool use
            
        Returns:
            Log entry ID
        """
        summary = self._get_tool_summary(tool_name, tool_input)
        
        entry = self._create_entry(
            category=EventCategory.HOOK,
            event_type="PreToolUse",
            message=f"Using tool: {tool_name}",
            summary=summary,
            payload={
                "tool_name": tool_name,
                "tool_input": tool_input,
                "tool_use_id": tool_use_id,
            },
        )
        return self._log_entry(entry)
    
    def log_tool_end(
        self,
        tool_name: str,
        tool_input: dict[str, Any],
        tool_response: Any = None,
        tool_use_id: str | None = None,
        success: bool = True,
    ) -> str:
        """Log the completion of a tool execution.
        
        Args:
            tool_name: Name of the tool used
            tool_input: Input parameters for the tool
            tool_response: Response from the tool
            tool_use_id: Unique identifier for this tool use
            success: Whether the tool executed successfully
            
        Returns:
            Log entry ID
        """
        summary = self._get_tool_summary(tool_name, tool_input)
        if success:
            summary += " ✓"
        else:
            summary += " ✗"
        
        level = LogLevel.INFO if success else LogLevel.WARNING
        
        entry = self._create_entry(
            category=EventCategory.HOOK,
            event_type="PostToolUse",
            message=f"Completed tool: {tool_name}",
            level=level,
            summary=summary,
            payload={
                "tool_name": tool_name,
                "tool_input": tool_input,
                "tool_use_id": tool_use_id,
                "tool_response": str(tool_response)[:500] if tool_response else None,
                "success": success,
            },
        )
        return self._log_entry(entry)
    
    def _get_tool_summary(self, tool_name: str, tool_input: dict[str, Any]) -> str:
        """Build a human-readable summary for a tool call."""
        if tool_name == "Read" and "file_path" in tool_input:
            file_name = Path(tool_input["file_path"]).name
            return f"Read: {file_name}"
        elif tool_name == "Write" and "file_path" in tool_input:
            file_name = Path(tool_input["file_path"]).name
            return f"Write: {file_name}"
        elif tool_name == "Edit" and "file_path" in tool_input:
            file_name = Path(tool_input["file_path"]).name
            return f"Edit: {file_name}"
        elif tool_name == "Bash" and "command" in tool_input:
            cmd = tool_input["command"][:40]
            return f"Bash: {cmd}..."
        elif tool_name == "Glob" and "pattern" in tool_input:
            return f"Glob: {tool_input['pattern']}"
        elif tool_name == "Grep" and "pattern" in tool_input:
            return f"Grep: {tool_input['pattern']}"
        elif tool_name == "Skill" and "skill" in tool_input:
            return f"Skill: /{tool_input['skill']}"
        elif tool_name == "Task":
            desc = tool_input.get("description", "")[:30]
            return f"Task: {desc}..."
        return f"Tool: {tool_name}"
    
    # ==========================================================================
    # Response Logging
    # ==========================================================================
    
    def log_text_response(self, text: str) -> str:
        """Log a text response from the agent.
        
        Args:
            text: The text content of the response
            
        Returns:
            Log entry ID
        """
        preview = text[:150] + "..." if len(text) > 150 else text
        
        entry = self._create_entry(
            category=EventCategory.RESPONSE,
            event_type="TextBlock",
            message="Agent response",
            content=text,
            summary=f"Response: {preview}",
            payload={"text": text},
        )
        return self._log_entry(entry)
    
    def log_thinking_response(self, thinking: str) -> str:
        """Log a thinking/reasoning block from the agent.
        
        Args:
            thinking: The thinking content
            
        Returns:
            Log entry ID
        """
        preview = thinking[:100] + "..." if len(thinking) > 100 else thinking
        
        entry = self._create_entry(
            category=EventCategory.RESPONSE,
            event_type="ThinkingBlock",
            message="Agent thinking",
            content=thinking,
            summary=f"Thinking: {preview}",
            payload={"thinking": thinking},
        )
        return self._log_entry(entry)
    
    def log_result(
        self,
        result: str | None,
        success: bool,
        usage: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> str:
        """Log the final result of a query.
        
        Args:
            result: The result text
            success: Whether the query succeeded
            usage: Token usage information
            session_id: Session identifier
            
        Returns:
            Log entry ID
        """
        status = "success" if success else "error"
        level = LogLevel.INFO if success else LogLevel.ERROR
        
        entry = self._create_entry(
            category=EventCategory.RESPONSE,
            event_type="result",
            message=f"Query completed: {status}",
            level=level,
            content=result or "",
            summary=f"Step completed: {status}",
            payload={
                "subtype": status,
                "usage": usage,
                "session_id": session_id,
            },
        )
        return self._log_entry(entry)
    
    # ==========================================================================
    # System Logging
    # ==========================================================================
    
    def log_system_event(
        self,
        level: LogLevel,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Log a system-level event.
        
        Args:
            level: Log level
            message: Event message
            metadata: Additional metadata
            
        Returns:
            Log entry ID
        """
        entry = self._create_entry(
            category=EventCategory.SYSTEM,
            event_type="system",
            message=message,
            level=level,
            summary=message[:100],
            payload=metadata or {},
        )
        return self._log_entry(entry)
    
    def log_workflow_start(
        self,
        workflow_type: str,
        prompt: str,
        working_dir: str,
        model: str,
    ) -> str:
        """Log the start of a workflow.
        
        Args:
            workflow_type: Type of workflow (e.g., "plan_build_review_fix")
            prompt: User prompt
            working_dir: Working directory
            model: Model being used
            
        Returns:
            Log entry ID
        """
        return self.log_system_event(
            level=LogLevel.INFO,
            message=f"Workflow started: {workflow_type}",
            metadata={
                "workflow_type": workflow_type,
                "prompt": prompt,
                "working_dir": working_dir,
                "model": model,
            },
        )
    
    def log_workflow_end(
        self,
        success: bool,
        duration_seconds: int,
        verdict: str | None = None,
        error: str | None = None,
    ) -> str:
        """Log the end of a workflow.
        
        Args:
            success: Whether the workflow succeeded
            duration_seconds: Total duration in seconds
            verdict: Review verdict (PASS/FAIL) if applicable
            error: Error message if failed
            
        Returns:
            Log entry ID
        """
        level = LogLevel.INFO if success else LogLevel.ERROR
        status = "completed" if success else "failed"
        message = f"Workflow {status} in {duration_seconds}s"
        if verdict:
            message += f" - Verdict: {verdict}"
        if error:
            message += f" - Error: {error}"
        
        return self.log_system_event(
            level=level,
            message=message,
            metadata={
                "success": success,
                "duration_seconds": duration_seconds,
                "verdict": verdict,
                "error": error,
            },
        )
    
    # ==========================================================================
    # Export
    # ==========================================================================
    
    def get_entries(self) -> list[LogEntry]:
        """Get all log entries."""
        return self.entries.copy()
    
    def export_to_file(self, file_path: Path) -> None:
        """Export all entries to a JSON file.
        
        Args:
            file_path: Path to write the JSON file
        """
        data = [entry.to_dict() for entry in self.entries]
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
    
    def get_summary(self) -> dict[str, Any]:
        """Get a summary of all logged events.
        
        Returns:
            Summary dictionary with counts by category and status
        """
        summary: dict[str, Any] = {
            "total_entries": len(self.entries),
            "by_category": {},
            "by_level": {},
            "steps": [],
        }
        
        for entry in self.entries:
            # Count by category
            cat = entry.category.value
            summary["by_category"][cat] = summary["by_category"].get(cat, 0) + 1
            
            # Count by level
            lvl = entry.level.value
            summary["by_level"][lvl] = summary["by_level"].get(lvl, 0) + 1
            
            # Track step events
            if entry.category == EventCategory.STEP and entry.event_type == "step_end":
                summary["steps"].append({
                    "step": entry.step,
                    "status": entry.status.value if entry.status else None,
                    "duration_ms": entry.duration_ms,
                })
        
        return summary


# =============================================================================
# Helper Functions
# =============================================================================


def create_workflow_logger(
    workflow_type: str,
    working_dir: str | None = None,
    verbose: bool = False,
) -> AgentLogger:
    """Create a logger for a new workflow.
    
    Args:
        workflow_type: Type of workflow (e.g., "plan", "build", "review")
        working_dir: Working directory to create log file in
        verbose: Whether to enable verbose output
        
    Returns:
        Configured AgentLogger instance
    """
    workflow_id = str(uuid.uuid4())
    log_file = None
    
    if working_dir:
        log_dir = Path(working_dir) / ".lakebridge_agent_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{workflow_type}_{timestamp}_{workflow_id[:8]}.json"
    
    return AgentLogger(
        workflow_id=workflow_id,
        log_file=log_file,
        verbose=verbose,
    )
