"""
Lakebridge Agent Module - AI-powered migration workflows.

This module provides agent-based commands for automated code migration:
- /plan: Creates implementation plans
- /build: Implements plans
- /review: Reviews completed work
- /fix: Fixes issues from reviews
- /question: Answers codebase questions

Usage:
    from databricks.labs.lakebridge.agent import AgentRunner
    
    runner = AgentRunner(working_dir="/path/to/project")
    await runner.plan("Migrate stored procedures to Databricks")
"""

from databricks.labs.lakebridge.agent.agent_sdk import (
    ModelName,
    QueryInput,
    QueryOptions,
    QueryOutput,
    MessageHandlers,
    HooksConfig,
    HookMatcher,
    HookResponse,
    TokenUsage,
    query_to_completion,
    quick_prompt,
    AdhocPrompt,
)

from databricks.labs.lakebridge.agent.runner import AgentRunner

__all__ = [
    # SDK Types
    "ModelName",
    "QueryInput",
    "QueryOptions", 
    "QueryOutput",
    "MessageHandlers",
    "HooksConfig",
    "HookMatcher",
    "HookResponse",
    "TokenUsage",
    # Functions
    "query_to_completion",
    "quick_prompt",
    "AdhocPrompt",
    # Runner
    "AgentRunner",
]
