from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from inspect import signature
from typing import Any, Optional, Type

from agentic_tools_core.models import ToolDefinition, ToolResult
from pydantic import BaseModel


@dataclass
class RegisteredTool:
    definition: ToolDefinition
    handler: Callable[..., dict[str, Any]]
    input_model: Optional[Type[BaseModel]] = None
    output_model: Optional[Type[BaseModel]] = None


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, RegisteredTool] = {}

    def register(
        self,
        definition: ToolDefinition,
        handler: Callable[..., dict[str, Any]],
        input_model: Optional[Type[BaseModel]] = None,
        output_model: Optional[Type[BaseModel]] = None,
    ) -> None:
        if definition.tool_id in self._tools:
            raise ValueError(f"Duplicate tool_id: {definition.tool_id}")
        if "preview" not in signature(handler).parameters:
            raise ValueError(f"Tool handler '{definition.tool_id}' must accept a preview argument")
        self._tools[definition.tool_id] = RegisteredTool(
            definition=definition,
            handler=handler,
            input_model=input_model,
            output_model=output_model,
        )

    def get_definition(self, tool_id: str) -> ToolDefinition:
        return self._tools[tool_id].definition

    def validate_input(self, tool_id: str, tool_input: dict[str, Any]) -> dict[str, Any]:
        if tool_id not in self._tools:
            raise KeyError(f"Unknown tool: {tool_id}")
        entry = self._tools[tool_id]
        if entry.input_model is None:
            return dict(tool_input)
        validated = entry.input_model.model_validate(tool_input)
        return validated.model_dump()

    def list_definitions(self) -> list[ToolDefinition]:
        return [entry.definition for entry in self._tools.values()]

    def get_registered(self, tool_id: str) -> RegisteredTool:
        return self._tools[tool_id]

    def list_registered(self) -> list[RegisteredTool]:
        return list(self._tools.values())

    def has(self, tool_id: str) -> bool:
        return tool_id in self._tools

    def execute(self, tool_id: str, tool_input: dict[str, Any], preview: bool = False) -> ToolResult:
        if tool_id not in self._tools:
            raise KeyError(f"Unknown tool: {tool_id}")
        sanitized_input = self.validate_input(tool_id=tool_id, tool_input=tool_input)
        raw = self._tools[tool_id].handler(sanitized_input, preview=preview)
        return ToolResult.model_validate(raw)


REGISTRY = ToolRegistry()


def tool(
    definition: ToolDefinition,
    *,
    input_model: Optional[Type[BaseModel]] = None,
    output_model: Optional[Type[BaseModel]] = None,
) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
    def decorator(fn: Callable[..., dict[str, Any]]) -> Callable[..., dict[str, Any]]:
        REGISTRY.register(definition, fn, input_model=input_model, output_model=output_model)
        return fn

    return decorator
