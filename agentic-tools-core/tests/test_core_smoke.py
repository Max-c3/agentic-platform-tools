from __future__ import annotations

import json
from pathlib import Path

from agentic_tools_core.catalog import write_catalog
from agentic_tools_core.models import ToolDefinition
from agentic_tools_core.registry import ToolRegistry


def test_registry_round_trip() -> None:
    registry = ToolRegistry()

    def handler(payload: dict[str, object], preview: bool = False) -> dict[str, object]:
        return {"output": {"payload": payload, "preview": preview}, "summary": "ok"}

    registry.register(
        ToolDefinition(
            tool_id="test.echo",
            display_name="Echo",
            source_path="tests/echo.py",
            function_name="handler",
            owner="tests",
            version="1.0.0",
            description="Echo test tool.",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            side_effects="None",
            approval_class="none",
            integration="test",
            is_write=False,
        ),
        handler,
    )

    result = registry.execute("test.echo", {"value": 1}, preview=True)
    assert result.output == {"payload": {"value": 1}, "preview": True}


def test_write_catalog(tmp_path: Path) -> None:
    registry = ToolRegistry()

    def handler(payload: dict[str, object], preview: bool = False) -> dict[str, object]:
        return {"output": {"payload": payload, "preview": preview}, "summary": "ok"}

    registry.register(
        ToolDefinition(
            tool_id="test.echo",
            display_name="Echo",
            source_path="tests/echo.py",
            function_name="handler",
            owner="tests",
            version="1.0.0",
            description="Echo test tool.",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            side_effects="None",
            approval_class="none",
            integration="test",
            is_write=False,
        ),
        handler,
    )

    target = tmp_path / "tool_catalog.json"
    write_catalog(registry, target)
    payload = json.loads(target.read_text())
    assert payload["tools"][0]["tool_id"] == "test.echo"
