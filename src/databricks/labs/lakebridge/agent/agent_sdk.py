"""
Lakebridge Agent SDK - Abstract typed layer for Claude Agent SDK control.

This module provides well-typed Pydantic models for configuring and controlling
the Claude Agent SDK. It wraps the SDK with a clean interface for use in
lakebridge agent workflows.

Usage:
    from databricks.labs.lakebridge.agent.agent_sdk import (
        query_to_completion,
        quick_prompt,
        QueryInput,
        QueryOptions,
        ModelName,
    )
    
    result = await query_to_completion(
        QueryInput(
            prompt="/plan Migrate stored procedures",
            options=QueryOptions(
                model=ModelName.OPUS,
                cwd="/path/to/project",
                allowed_tools=["Read", "Write", "Bash"],
            ),
        )
    )
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Literal,
    TypeAlias,
)

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)

# =============================================================================
# ENUMS - Model & Configuration
# =============================================================================


class ModelName(str, Enum):
    """Available Claude models for the Agent SDK."""

    # Claude 4.5 models (latest)
    OPUS_4_5 = "claude-opus-4-5-20251101"
    SONNET_4_5 = "claude-sonnet-4-5-20250929"
    HAIKU_4_5 = "claude-haiku-4-5-20251001"

    # Convenience aliases (point to latest)
    OPUS = "claude-opus-4-5-20251101"
    SONNET = "claude-sonnet-4-5-20250929"
    HAIKU = "claude-haiku-4-5-20251001"


class SettingSource(str, Enum):
    """Setting sources for loading skills/commands from filesystem."""

    USER = "user"
    PROJECT = "project"


# =============================================================================
# ENUMS - Hook Events
# =============================================================================


class HookEventName(str, Enum):
    """Hook event types available in the Agent SDK."""

    # Python SDK supported
    PRE_TOOL_USE = "PreToolUse"
    POST_TOOL_USE = "PostToolUse"
    USER_PROMPT_SUBMIT = "UserPromptSubmit"
    STOP = "Stop"
    SUBAGENT_STOP = "SubagentStop"
    PRE_COMPACT = "PreCompact"


class PermissionDecision(str, Enum):
    """Permission decisions for PreToolUse hooks."""

    ALLOW = "allow"
    DENY = "deny"
    ASK = "ask"


# =============================================================================
# ENUMS - Messages
# =============================================================================


class MessageType(str, Enum):
    """Types of messages from the Agent SDK."""

    SYSTEM = "system"
    ASSISTANT = "assistant"
    USER = "user"
    RESULT = "result"


class SystemMessageSubtype(str, Enum):
    """Subtypes for system messages."""

    INIT = "init"
    COMPACT_BOUNDARY = "compact_boundary"


class ResultSubtype(str, Enum):
    """Subtypes for result messages."""

    SUCCESS = "success"
    ERROR = "error"
    INTERRUPTED = "interrupted"


# =============================================================================
# BUILT-IN TOOLS
# =============================================================================


class BuiltInTool(str, Enum):
    """Built-in tools available in Claude Code."""

    BASH = "Bash"
    EDIT = "Edit"
    GLOB = "Glob"
    GREP = "Grep"
    NOTEBOOK_EDIT = "NotebookEdit"
    NOTEBOOK_READ = "NotebookRead"
    READ = "Read"
    SLASH_COMMAND = "SlashCommand"
    TASK = "Task"
    TODO_WRITE = "TodoWrite"
    WEB_FETCH = "WebFetch"
    WEB_SEARCH = "WebSearch"
    WRITE = "Write"
    SKILL = "Skill"


# =============================================================================
# TOOL PERMISSIONS
# =============================================================================


class ToolPermissions(BaseModel):
    """Tool permission configuration with allow/ask/deny arrays."""

    allow: list[str] = Field(
        default_factory=list,
        description="Tools/patterns to explicitly allow without prompting",
    )
    ask: list[str] = Field(
        default_factory=list,
        description="Tools/patterns that require user confirmation",
    )
    deny: list[str] = Field(
        default_factory=list,
        description="Tools/patterns to completely block",
    )


# =============================================================================
# TOKEN USAGE & COST TRACKING
# =============================================================================


class CacheCreationDetails(BaseModel):
    """Detailed cache creation token breakdown."""

    ephemeral_5m_input_tokens: int = Field(default=0)
    ephemeral_1h_input_tokens: int = Field(default=0)


class TokenUsage(BaseModel):
    """Token usage data from a message or result."""

    input_tokens: int = Field(default=0, description="Base input tokens processed")
    output_tokens: int = Field(default=0, description="Tokens generated in response")
    cache_creation_input_tokens: int = Field(
        default=0, description="Tokens used to create cache entries"
    )
    cache_read_input_tokens: int = Field(
        default=0, description="Tokens read from cache"
    )
    cache_creation: CacheCreationDetails | None = Field(default=None)
    service_tier: str | None = Field(default=None, description="e.g., 'standard'")
    total_cost_usd: float | None = Field(
        default=None, description="Total cost (only in final result)"
    )

    def calculate_cost(
        self,
        input_cost_per_token: float = 0.00003,
        output_cost_per_token: float = 0.00015,
        cache_read_cost_per_token: float = 0.0000075,
    ) -> float:
        """Calculate estimated cost based on token counts."""
        return (
            self.input_tokens * input_cost_per_token
            + self.output_tokens * output_cost_per_token
            + self.cache_read_input_tokens * cache_read_cost_per_token
        )


class UsageAccumulator(BaseModel):
    """Tracks cumulative usage, deduplicating by message ID."""

    processed_ids: set[str] = Field(default_factory=set)
    step_usages: list[TokenUsage] = Field(default_factory=list)
    total_input_tokens: int = Field(default=0)
    total_output_tokens: int = Field(default=0)
    total_cache_read_tokens: int = Field(default=0)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def process(self, message_id: str, usage: TokenUsage) -> bool:
        """Process usage if message ID is new. Returns True if processed."""
        if message_id in self.processed_ids:
            return False

        self.processed_ids.add(message_id)
        self.step_usages.append(usage)
        self.total_input_tokens += usage.input_tokens
        self.total_output_tokens += usage.output_tokens
        self.total_cache_read_tokens += usage.cache_read_input_tokens
        return True


# =============================================================================
# HOOK INPUT TYPES
# =============================================================================


class HookInputBase(BaseModel):
    """Common fields present in all hook inputs."""

    hook_event_name: HookEventName
    session_id: str = ""
    transcript_path: str = ""
    cwd: str = ""


class PreToolUseInput(HookInputBase):
    """Input for PreToolUse hooks - runs before tool execution."""

    hook_event_name: Literal[HookEventName.PRE_TOOL_USE] = HookEventName.PRE_TOOL_USE
    tool_name: str = ""
    tool_input: dict[str, Any] = Field(default_factory=dict)


class PostToolUseInput(HookInputBase):
    """Input for PostToolUse hooks - runs after tool execution."""

    hook_event_name: Literal[HookEventName.POST_TOOL_USE] = HookEventName.POST_TOOL_USE
    tool_name: str = ""
    tool_input: dict[str, Any] = Field(default_factory=dict)
    tool_response: Any = None


class UserPromptSubmitInput(HookInputBase):
    """Input for UserPromptSubmit hooks."""

    hook_event_name: Literal[HookEventName.USER_PROMPT_SUBMIT] = (
        HookEventName.USER_PROMPT_SUBMIT
    )
    prompt: str = ""


class StopInput(HookInputBase):
    """Input for Stop hooks."""

    hook_event_name: Literal[HookEventName.STOP] = HookEventName.STOP
    stop_hook_active: bool = False


class SubagentStopInput(HookInputBase):
    """Input for SubagentStop hooks."""

    hook_event_name: Literal[HookEventName.SUBAGENT_STOP] = HookEventName.SUBAGENT_STOP
    agent_id: str | None = None
    agent_transcript_path: str | None = None
    stop_hook_active: bool = False


class PreCompactInput(HookInputBase):
    """Input for PreCompact hooks."""

    hook_event_name: Literal[HookEventName.PRE_COMPACT] = HookEventName.PRE_COMPACT
    trigger: Literal["manual", "auto"] = "auto"
    custom_instructions: str | None = None


# Union of all hook inputs
HookInput: TypeAlias = (
    PreToolUseInput
    | PostToolUseInput
    | UserPromptSubmitInput
    | StopInput
    | SubagentStopInput
    | PreCompactInput
)


# =============================================================================
# HOOK OUTPUT TYPES
# =============================================================================


class HookContext(BaseModel):
    """Context passed to hook callbacks."""

    cancellation_requested: bool = False


class PreToolUseOutput(BaseModel):
    """Output specific to PreToolUse hooks."""

    hook_event_name: Literal[HookEventName.PRE_TOOL_USE] = HookEventName.PRE_TOOL_USE
    permission_decision: PermissionDecision | None = None
    permission_decision_reason: str | None = None
    updated_input: dict[str, Any] | None = None


class PostToolUseOutput(BaseModel):
    """Output specific to PostToolUse hooks."""

    hook_event_name: Literal[HookEventName.POST_TOOL_USE] = HookEventName.POST_TOOL_USE
    additional_context: str | None = None


class UserPromptSubmitOutput(BaseModel):
    """Output specific to UserPromptSubmit hooks."""

    hook_event_name: Literal[HookEventName.USER_PROMPT_SUBMIT] = (
        HookEventName.USER_PROMPT_SUBMIT
    )
    additional_context: str | None = None


class GenericHookOutput(BaseModel):
    """Generic output for hooks without specific fields."""

    hook_event_name: HookEventName


HookSpecificOutput: TypeAlias = (
    PreToolUseOutput | PostToolUseOutput | UserPromptSubmitOutput | GenericHookOutput
)


class HookResponse(BaseModel):
    """Full response from a hook callback."""

    # Execution control
    continue_execution: bool = Field(default=True, alias="continue")
    stop_reason: str | None = Field(default=None, alias="stopReason")
    suppress_output: bool = Field(default=False, alias="suppressOutput")
    system_message: str | None = Field(default=None, alias="systemMessage")

    # Hook-specific output
    hook_specific_output: HookSpecificOutput | None = Field(
        default=None, alias="hookSpecificOutput"
    )

    model_config = ConfigDict(populate_by_name=True)

    @classmethod
    def allow(cls) -> "HookResponse":
        """Allow the operation to proceed."""
        return cls()

    @classmethod
    def deny(cls, reason: str, system_message: str | None = None) -> "HookResponse":
        """Deny a PreToolUse operation."""
        return cls(
            system_message=system_message,
            hook_specific_output=PreToolUseOutput(
                permission_decision=PermissionDecision.DENY,
                permission_decision_reason=reason,
            ),
        )

    @classmethod
    def allow_modified(
        cls, updated_input: dict[str, Any], reason: str | None = None
    ) -> "HookResponse":
        """Allow with modified input."""
        return cls(
            hook_specific_output=PreToolUseOutput(
                permission_decision=PermissionDecision.ALLOW,
                permission_decision_reason=reason,
                updated_input=updated_input,
            )
        )

    @classmethod
    def stop(cls, reason: str) -> "HookResponse":
        """Stop agent execution."""
        return cls(continue_execution=False, stop_reason=reason)


# Callback signature
HookCallback: TypeAlias = Callable[
    [HookInput, str | None, HookContext],
    Awaitable[HookResponse],
]


# =============================================================================
# HOOK CONFIGURATION
# =============================================================================


class HookMatcher(BaseModel):
    """Matcher configuration for filtering which tools trigger hooks."""

    matcher: str | None = Field(
        None, description="Regex pattern for tool names. None matches all."
    )
    hooks: list[HookCallback] = Field(..., description="Callbacks to execute")
    timeout: int = Field(default=60, description="Timeout in seconds")

    model_config = ConfigDict(arbitrary_types_allowed=True)


class HooksConfig(BaseModel):
    """Configuration for all hook event types."""

    pre_tool_use: list[HookMatcher] = Field(default_factory=list, alias="PreToolUse")
    post_tool_use: list[HookMatcher] = Field(default_factory=list, alias="PostToolUse")
    user_prompt_submit: list[HookMatcher] = Field(
        default_factory=list, alias="UserPromptSubmit"
    )
    stop: list[HookMatcher] = Field(default_factory=list, alias="Stop")
    subagent_stop: list[HookMatcher] = Field(
        default_factory=list, alias="SubagentStop"
    )
    pre_compact: list[HookMatcher] = Field(default_factory=list, alias="PreCompact")

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    @classmethod
    def from_callbacks(
        cls,
        callbacks: dict[HookEventName | str, list[HookCallback]],
        default_timeout: int = 60,
    ) -> "HooksConfig":
        """Create HooksConfig from a simple mapping of hook names to callback lists."""
        config = cls()

        for event_name, hook_fns in callbacks.items():
            key = event_name.value if isinstance(event_name, HookEventName) else event_name
            matcher = HookMatcher(matcher=None, hooks=hook_fns, timeout=default_timeout)

            if key == "PreToolUse":
                config.pre_tool_use.append(matcher)
            elif key == "PostToolUse":
                config.post_tool_use.append(matcher)
            elif key == "UserPromptSubmit":
                config.user_prompt_submit.append(matcher)
            elif key == "Stop":
                config.stop.append(matcher)
            elif key == "SubagentStop":
                config.subagent_stop.append(matcher)
            elif key == "PreCompact":
                config.pre_compact.append(matcher)

        return config


# Simpler type for passing hooks as {event_name: [callbacks]}
HookCallbackMap: TypeAlias = dict[HookEventName | str, list[HookCallback]]


# =============================================================================
# MESSAGE TYPES
# =============================================================================


class SystemInitMessage(BaseModel):
    """System init message with session info."""

    type: Literal[MessageType.SYSTEM] = MessageType.SYSTEM
    subtype: Literal[SystemMessageSubtype.INIT] = SystemMessageSubtype.INIT
    session_id: str = ""
    slash_commands: list[str] = Field(default_factory=list)
    tools: list[str] = Field(default_factory=list)


class CompactBoundaryMessage(BaseModel):
    """Message indicating conversation compaction."""

    type: Literal[MessageType.SYSTEM] = MessageType.SYSTEM
    subtype: Literal[SystemMessageSubtype.COMPACT_BOUNDARY] = (
        SystemMessageSubtype.COMPACT_BOUNDARY
    )
    compact_metadata: dict[str, Any] = Field(default_factory=dict)


class AssistantMessage(BaseModel):
    """Message from Claude with content blocks."""

    type: Literal[MessageType.ASSISTANT] = MessageType.ASSISTANT
    id: str = ""
    content_blocks: list[Any] = Field(default_factory=list)
    message: str | None = None
    tool_use: dict[str, Any] | None = None
    usage: TokenUsage | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class UserMessage(BaseModel):
    """Message from user or tool result with content blocks."""

    type: Literal[MessageType.USER] = MessageType.USER
    content_blocks: list[Any] = Field(default_factory=list)
    message: str | None = None
    tool_result: dict[str, Any] | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ResultMessage(BaseModel):
    """Final result message."""

    type: Literal[MessageType.RESULT] = MessageType.RESULT
    subtype: ResultSubtype = ResultSubtype.SUCCESS
    result: str | None = None
    error: str | None = None
    usage: TokenUsage | None = None
    session_id: str | None = None


AgentMessage: TypeAlias = (
    SystemInitMessage
    | CompactBoundaryMessage
    | AssistantMessage
    | UserMessage
    | ResultMessage
)


# =============================================================================
# MCP SERVER CONFIGURATION
# =============================================================================


class MCPServerConfig(BaseModel):
    """Configuration for an MCP server."""

    name: str
    command: str | None = None
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    is_sdk_server: bool = Field(
        default=False, description="True for in-process SDK MCP servers"
    )


# =============================================================================
# QUERY OPTIONS
# =============================================================================


class QueryOptions(BaseModel):
    """Complete configuration options for an Agent SDK query."""

    # Model
    model: ModelName | str = Field(default=ModelName.SONNET)

    # Session management
    resume: str | None = Field(None, description="Session ID to resume")
    fork_session: bool = Field(
        default=False, alias="forkSession", description="Fork when resuming"
    )

    # Working directory
    cwd: str | None = Field(None, description="Working directory for the agent")

    # Tools
    allowed_tools: list[str] = Field(
        default_factory=list,
        alias="allowedTools",
        description="Tools the agent can use. Include 'Skill' for skills.",
    )
    permissions: ToolPermissions | None = None
    bypass_permissions: bool = Field(
        default=True,
        alias="bypassPermissions",
        description="Skip permission prompts (default: True)",
    )

    # Hooks
    hooks: HooksConfig | HookCallbackMap | None = None

    # MCP servers
    mcp_servers: dict[str, MCPServerConfig] = Field(
        default_factory=dict, alias="mcpServers"
    )

    # Settings sources (for skills/commands)
    setting_sources: list[SettingSource] = Field(
        default_factory=lambda: [SettingSource.PROJECT],
        alias="settingSources",
        description="Sources for loading skills/commands (default: ['project'])",
    )

    # Limits
    max_turns: int | None = Field(default=None, alias="maxTurns")
    max_tokens: int | None = Field(default=None, alias="maxTokens")

    # System prompt
    system_prompt: str | Any | None = Field(
        default=None,
        alias="systemPrompt",
        description="System prompt config",
    )

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        use_enum_values=True,
    )

    @field_validator("model", mode="before")
    @classmethod
    def validate_model(cls, v: Any) -> str:
        if isinstance(v, ModelName):
            return v.value
        return str(v)


# =============================================================================
# MESSAGE HANDLERS
# =============================================================================


SystemMessageHandler: TypeAlias = Callable[[SystemInitMessage | CompactBoundaryMessage], Awaitable[None]]
AssistantMessageHandler: TypeAlias = Callable[[AssistantMessage], Awaitable[None]]
AssistantBlockHandler: TypeAlias = Callable[[Any], Awaitable[None]]
UserMessageHandler: TypeAlias = Callable[[UserMessage], Awaitable[None]]
ResultMessageHandler: TypeAlias = Callable[[ResultMessage], Awaitable[None]]
AnyMessageHandler: TypeAlias = Callable[[AgentMessage], Awaitable[None]]


class MessageHandlers(BaseModel):
    """Callbacks for handling different message types during query execution."""

    on_system: SystemMessageHandler | None = Field(
        None, description="Called for SystemInitMessage and CompactBoundaryMessage"
    )
    on_assistant: AssistantMessageHandler | None = Field(
        None, description="Called for each AssistantMessage"
    )
    on_assistant_block: AssistantBlockHandler | None = Field(
        None, description="Called for each content block in AssistantMessage"
    )
    on_user: UserMessageHandler | None = Field(
        None, description="Called for each UserMessage (tool results)"
    )
    on_result: ResultMessageHandler | None = Field(
        None, description="Called when query completes with ResultMessage"
    )
    on_any: AnyMessageHandler | None = Field(
        None, description="Called for every message (after specific handler)"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


# =============================================================================
# QUERY INPUT / OUTPUT
# =============================================================================


class QueryInput(BaseModel):
    """Input for an Agent SDK query."""

    prompt: str
    options: QueryOptions = Field(default_factory=QueryOptions)
    handlers: MessageHandlers | None = Field(
        None, description="Optional callbacks for message handling"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class QueryOutput(BaseModel):
    """Output from an Agent SDK query execution."""

    success: bool
    result: str | None = None
    error: str | None = None
    session_id: str | None = None
    usage: TokenUsage | None = None
    usage_accumulator: UsageAccumulator | None = None
    messages: list[AgentMessage] = Field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


# =============================================================================
# SYSTEM PROMPT CONFIGURATION
# =============================================================================


class SystemPromptMode(str, Enum):
    """How to handle the system prompt."""

    DEFAULT = "default"
    APPEND = "append"
    OVERWRITE = "overwrite"


class SystemPromptConfig(BaseModel):
    """Configuration for system prompt handling."""

    mode: SystemPromptMode = Field(
        default=SystemPromptMode.DEFAULT,
        description="How to handle the system prompt",
    )
    system_prompt: str | None = Field(
        None,
        description="Custom system prompt text (required for APPEND and OVERWRITE modes)",
    )

    def to_sdk_config(self) -> dict[str, Any] | str | None:
        """Convert to SDK-compatible system_prompt config."""
        if self.mode == SystemPromptMode.DEFAULT:
            return {
                "type": "preset",
                "preset": "claude_code",
            }
        elif self.mode == SystemPromptMode.APPEND:
            if not self.system_prompt:
                raise ValueError("system_prompt required for APPEND mode")
            return {
                "type": "preset",
                "preset": "claude_code",
                "append": self.system_prompt,
            }
        elif self.mode == SystemPromptMode.OVERWRITE:
            if not self.system_prompt:
                raise ValueError("system_prompt required for OVERWRITE mode")
            return self.system_prompt
        else:
            raise ValueError(f"Unknown mode: {self.mode}")


# =============================================================================
# ADHOC PROMPTS
# =============================================================================


class AdhocPrompt(BaseModel):
    """Input for a quick, one-off prompt."""

    prompt: str = Field(..., description="The prompt to send to Claude")
    model: ModelName | str = Field(
        default=ModelName.SONNET,
        description="Model to use (defaults to Sonnet)",
    )
    cwd: str | None = Field(
        None,
        description="Working directory for the agent",
    )
    system_prompt: str | SystemPromptConfig | None = Field(
        None,
        description="System prompt config",
    )

    model_config = ConfigDict(use_enum_values=True)


# =============================================================================
# SDK EXECUTION
# =============================================================================


def _convert_hooks_to_sdk_format(config: HooksConfig) -> dict[str, Any]:
    """Convert our HooksConfig to claude-agent-sdk's expected format."""
    try:
        from claude_agent_sdk.types import HookMatcher as SDKHookMatcher
    except ImportError:
        logger.warning("claude-agent-sdk not installed, hooks will not work")
        return {}

    result: dict[str, list[Any]] = {}

    def wrap_callback(cb: HookCallback, hook_type: str) -> Callable[[dict[str, Any], str | None, Any], Awaitable[dict[str, Any]]]:
        """Wrap a typed callback to accept/return raw dicts."""
        async def wrapper(input_dict: dict[str, Any], tool_use_id: str | None, sdk_context: Any) -> dict[str, Any]:
            enriched_input = {
                "hook_event_name": hook_type,
                "session_id": input_dict.get("session_id", ""),
                "transcript_path": input_dict.get("transcript_path", ""),
                "cwd": input_dict.get("cwd", ""),
                **input_dict,
            }

            typed_input: HookInput

            if hook_type == "PreToolUse":
                typed_input = PreToolUseInput(**enriched_input)
            elif hook_type == "PostToolUse":
                typed_input = PostToolUseInput(**enriched_input)
            elif hook_type == "UserPromptSubmit":
                typed_input = UserPromptSubmitInput(**enriched_input)
            elif hook_type == "Stop":
                typed_input = StopInput(**enriched_input)
            elif hook_type == "SubagentStop":
                typed_input = SubagentStopInput(**enriched_input)
            elif hook_type == "PreCompact":
                typed_input = PreCompactInput(**enriched_input)
            else:
                typed_input = HookInputBase(
                    hook_event_name=HookEventName(hook_type) if hook_type in [e.value for e in HookEventName] else HookEventName.STOP,
                    session_id=enriched_input.get("session_id", ""),
                    transcript_path=enriched_input.get("transcript_path", ""),
                    cwd=enriched_input.get("cwd", ""),
                )

            our_context = HookContext(
                cancellation_requested=getattr(sdk_context, "signal", None) is not None
            )

            response = await cb(typed_input, tool_use_id, our_context)

            output: dict[str, Any] = {}
            if not response.continue_execution:
                output["continue"] = False
            if response.stop_reason:
                output["stopReason"] = response.stop_reason
            if response.suppress_output:
                output["suppressOutput"] = True
            if response.system_message:
                output["systemMessage"] = response.system_message
            if response.hook_specific_output:
                hso = response.hook_specific_output
                output["hookSpecificOutput"] = {
                    "hookEventName": hso.hook_event_name.value if hasattr(hso.hook_event_name, "value") else hso.hook_event_name,
                }
                if isinstance(hso, PreToolUseOutput):
                    if hso.permission_decision:
                        output["hookSpecificOutput"]["permissionDecision"] = hso.permission_decision.value
                    if hso.permission_decision_reason:
                        output["hookSpecificOutput"]["permissionDecisionReason"] = hso.permission_decision_reason
                    if hso.updated_input:
                        output["hookSpecificOutput"]["updatedInput"] = hso.updated_input
                elif isinstance(hso, (PostToolUseOutput, UserPromptSubmitOutput)):
                    if hso.additional_context:
                        output["hookSpecificOutput"]["additionalContext"] = hso.additional_context

            return output
        return wrapper

    def convert_matchers(matchers: list[HookMatcher], key: str) -> None:
        if matchers:
            result[key] = [
                SDKHookMatcher(
                    matcher=m.matcher,
                    hooks=[wrap_callback(cb, key) for cb in m.hooks]
                )
                for m in matchers
            ]

    convert_matchers(config.pre_tool_use, "PreToolUse")
    convert_matchers(config.post_tool_use, "PostToolUse")
    convert_matchers(config.user_prompt_submit, "UserPromptSubmit")
    convert_matchers(config.stop, "Stop")
    convert_matchers(config.subagent_stop, "SubagentStop")
    convert_matchers(config.pre_compact, "PreCompact")

    return result


async def query_to_completion(query_input: QueryInput) -> QueryOutput:
    """Execute an Agent SDK query and return the complete result.

    This is the primary method for running Claude Agent SDK queries.

    Args:
        query_input: QueryInput containing prompt and options.

    Returns:
        QueryOutput with success status, result, session_id, usage, and messages.
    """
    try:
        from claude_agent_sdk import (
            ClaudeSDKClient,
            ClaudeAgentOptions,
            AssistantMessage as SDKAssistantMessage,
            UserMessage as SDKUserMessage,
            SystemMessage as SDKSystemMessage,
            ResultMessage as SDKResultMessage,
            TextBlock as SDKTextBlock,
            ThinkingBlock as SDKThinkingBlock,
            ToolUseBlock as SDKToolUseBlock,
            ToolResultBlock as SDKToolResultBlock,
        )
    except ImportError as e:
        logger.error(f"claude-agent-sdk not installed: {e}")
        return QueryOutput(
            success=False,
            error="claude-agent-sdk not installed. Install with: pip install claude-agent-sdk",
        )

    started_at = datetime.now()
    opts = query_input.options
    handlers = query_input.handlers

    # Normalize hooks to HooksConfig
    resolved_hooks: HooksConfig | None = None
    if opts.hooks is not None:
        if isinstance(opts.hooks, dict):
            resolved_hooks = HooksConfig.from_callbacks(opts.hooks)
        else:
            resolved_hooks = opts.hooks

    # Build SDK options dict
    options_dict: dict[str, Any] = {
        "model": opts.model if isinstance(opts.model, str) else opts.model.value,
    }

    if opts.cwd:
        options_dict["cwd"] = opts.cwd
    if opts.allowed_tools:
        options_dict["allowed_tools"] = opts.allowed_tools
    if opts.max_turns:
        options_dict["max_turns"] = opts.max_turns
    if opts.resume:
        options_dict["resume"] = opts.resume

    # Handle system_prompt
    if opts.system_prompt is None:
        options_dict["system_prompt"] = {
            "type": "preset",
            "preset": "claude_code",
        }
    elif isinstance(opts.system_prompt, str):
        options_dict["system_prompt"] = opts.system_prompt
    elif hasattr(opts.system_prompt, "to_sdk_config"):
        options_dict["system_prompt"] = opts.system_prompt.to_sdk_config()
    else:
        options_dict["system_prompt"] = opts.system_prompt

    if opts.setting_sources:
        options_dict["setting_sources"] = [s.value if hasattr(s, 'value') else s for s in opts.setting_sources]

    if resolved_hooks is not None:
        options_dict["hooks"] = _convert_hooks_to_sdk_format(resolved_hooks)

    sdk_options = ClaudeAgentOptions(**options_dict)

    # Execute query
    messages: list[AgentMessage] = []
    usage_accumulator = UsageAccumulator()
    session_id: str | None = None
    final_result: str | None = None
    final_error: str | None = None
    final_usage: TokenUsage | None = None
    success = False

    async def call_handlers(parsed: AgentMessage) -> None:
        if handlers is None:
            return

        if isinstance(parsed, (SystemInitMessage, CompactBoundaryMessage)):
            if handlers.on_system:
                await handlers.on_system(parsed)
        elif isinstance(parsed, AssistantMessage):
            if handlers.on_assistant:
                await handlers.on_assistant(parsed)
        elif isinstance(parsed, UserMessage):
            if handlers.on_user:
                await handlers.on_user(parsed)
        elif isinstance(parsed, ResultMessage):
            if handlers.on_result:
                await handlers.on_result(parsed)

        if handlers.on_any:
            await handlers.on_any(parsed)

    try:
        async with ClaudeSDKClient(options=sdk_options) as client:
            await client.query(query_input.prompt)

            async for message in client.receive_response():
                if isinstance(message, SDKSystemMessage):
                    subtype = getattr(message, "subtype", None)
                    data = getattr(message, "data", {})

                    if subtype == "init":
                        parsed = SystemInitMessage(
                            session_id=data.get("session_id", ""),
                            slash_commands=data.get("slash_commands", []),
                            tools=data.get("tools", []),
                        )
                        session_id = parsed.session_id
                    elif subtype == "compact_boundary":
                        parsed = CompactBoundaryMessage(
                            compact_metadata=data.get("compact_metadata", data),
                        )
                    else:
                        continue

                    messages.append(parsed)
                    await call_handlers(parsed)

                elif isinstance(message, SDKAssistantMessage):
                    text_parts: list[str] = []
                    tool_use_data: dict[str, Any] | None = None
                    content_blocks: list[Any] = []

                    for block in message.content:
                        content_blocks.append(block)

                        if handlers and handlers.on_assistant_block:
                            await handlers.on_assistant_block(block)

                        if isinstance(block, SDKTextBlock):
                            text_parts.append(block.text)
                        elif isinstance(block, SDKToolUseBlock):
                            if tool_use_data is None:
                                tool_use_data = {
                                    "id": block.id,
                                    "name": block.name,
                                    "input": block.input,
                                }

                    parsed = AssistantMessage(
                        id=str(id(message)),
                        content_blocks=content_blocks,
                        message="\n".join(text_parts) if text_parts else None,
                        tool_use=tool_use_data,
                        usage=None,
                    )
                    messages.append(parsed)
                    await call_handlers(parsed)

                elif isinstance(message, SDKUserMessage):
                    text_parts: list[str] = []
                    tool_result_data: dict[str, Any] | None = None
                    content_blocks: list[Any] = []

                    content = message.content
                    if isinstance(content, str):
                        text_parts.append(content)
                    elif isinstance(content, list):
                        for block in content:
                            content_blocks.append(block)
                            if isinstance(block, SDKTextBlock):
                                text_parts.append(block.text)
                            elif isinstance(block, SDKToolResultBlock):
                                if tool_result_data is None:
                                    tool_result_data = {
                                        "tool_use_id": block.tool_use_id,
                                        "content": block.content,
                                        "is_error": block.is_error or False,
                                    }

                    parsed = UserMessage(
                        content_blocks=content_blocks,
                        message="\n".join(text_parts) if text_parts else None,
                        tool_result=tool_result_data,
                    )
                    messages.append(parsed)
                    await call_handlers(parsed)

                elif isinstance(message, SDKResultMessage):
                    usage_data = getattr(message, "usage", None)
                    parsed_usage: TokenUsage | None = None

                    if usage_data:
                        if isinstance(usage_data, dict):
                            parsed_usage = TokenUsage(
                                input_tokens=usage_data.get("input_tokens", 0),
                                output_tokens=usage_data.get("output_tokens", 0),
                                cache_read_input_tokens=usage_data.get("cache_read_input_tokens", 0),
                                cache_creation_input_tokens=usage_data.get("cache_creation_input_tokens", 0),
                                total_cost_usd=getattr(message, "total_cost_usd", None),
                            )
                        else:
                            parsed_usage = TokenUsage(
                                input_tokens=getattr(usage_data, "input_tokens", 0),
                                output_tokens=getattr(usage_data, "output_tokens", 0),
                                cache_read_input_tokens=getattr(usage_data, "cache_read_input_tokens", 0),
                                cache_creation_input_tokens=getattr(usage_data, "cache_creation_input_tokens", 0),
                                total_cost_usd=getattr(message, "total_cost_usd", None),
                            )

                    is_error = getattr(message, "is_error", False)

                    parsed = ResultMessage(
                        subtype=ResultSubtype.ERROR if is_error else ResultSubtype.SUCCESS,
                        result=getattr(message, "result", None),
                        error=getattr(message, "error", None) if is_error else None,
                        usage=parsed_usage,
                        session_id=getattr(message, "session_id", None),
                    )

                    messages.append(parsed)
                    await call_handlers(parsed)

                    if is_error:
                        success = False
                        final_error = parsed.error
                    else:
                        success = True
                        final_result = parsed.result

                    final_usage = parsed_usage
                    if parsed.session_id:
                        session_id = parsed.session_id

    except Exception as e:
        success = False
        final_error = str(e)
        logger.exception(f"Agent query failed: {e}")

    completed_at = datetime.now()

    return QueryOutput(
        success=success,
        result=final_result,
        error=final_error,
        session_id=session_id,
        usage=final_usage,
        usage_accumulator=usage_accumulator,
        messages=messages,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=(completed_at - started_at).total_seconds(),
    )


async def quick_prompt(input_data: AdhocPrompt) -> str:
    """Fire a quick, one-off prompt and get back just the result string.

    Args:
        input_data: AdhocPrompt with prompt, model, cwd, and optional system_prompt

    Returns:
        The result string from Claude's response
    """
    try:
        from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage as SDKResultMessage
    except ImportError as e:
        logger.error(f"claude-agent-sdk not installed: {e}")
        return ""

    # Build system prompt config
    system_prompt_config: dict[str, Any] | str | None

    if input_data.system_prompt is None:
        system_prompt_config = {
            "type": "preset",
            "preset": "claude_code",
        }
    elif isinstance(input_data.system_prompt, str):
        system_prompt_config = input_data.system_prompt
    elif isinstance(input_data.system_prompt, SystemPromptConfig):
        system_prompt_config = input_data.system_prompt.to_sdk_config()
    else:
        raise ValueError(f"Unknown system_prompt type: {type(input_data.system_prompt)}")

    options = ClaudeAgentOptions(
        model=input_data.model if isinstance(input_data.model, str) else input_data.model.value,
        cwd=input_data.cwd,
        system_prompt=system_prompt_config,
        setting_sources=["project"],
    )

    result: str | None = None

    async for message in query(prompt=input_data.prompt, options=options):
        if isinstance(message, SDKResultMessage):
            result = getattr(message, "result", None)

    return result or ""


# =============================================================================
# ABSTRACT INTERFACE
# =============================================================================


class AgentSDKInterface(ABC):
    """Abstract interface for custom Agent SDK implementations."""

    @abstractmethod
    async def query(self, input_data: QueryInput) -> AsyncGenerator[AgentMessage, None]:
        """Execute a query and yield messages as they arrive."""
        ...

    @abstractmethod
    async def run(self, input_data: QueryInput) -> QueryOutput:
        """Execute a query and wait for complete result."""
        ...
