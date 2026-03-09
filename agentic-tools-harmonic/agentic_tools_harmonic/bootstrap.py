from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional, Type

from agentic_tools_core.models import ToolDefinition
from agentic_tools_core.registry import ToolRegistry
from pydantic import BaseModel

PendingTool = tuple[ToolDefinition, Callable[..., dict[str, Any]], Optional[Type[BaseModel]], Optional[Type[BaseModel]]]

_PENDING: list[PendingTool] = []


def tool(
    definition: ToolDefinition,
    *,
    input_model: Optional[Type[BaseModel]] = None,
    output_model: Optional[Type[BaseModel]] = None,
) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
    def decorator(fn: Callable[..., dict[str, Any]]) -> Callable[..., dict[str, Any]]:
        _PENDING.append((definition, fn, input_model, output_model))
        return fn

    return decorator


def register_into(registry: ToolRegistry) -> None:
    for definition, handler, input_model, output_model in _PENDING:
        if registry.has(definition.tool_id):
            continue
        registry.register(definition, handler, input_model=input_model, output_model=output_model)
