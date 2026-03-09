from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agentic_tools_core.registry import ToolRegistry


def build_catalog(registry: ToolRegistry) -> list[dict[str, Any]]:
    return [definition.model_dump() for definition in sorted(registry.list_definitions(), key=lambda item: item.tool_id)]


def write_catalog(registry: ToolRegistry, path: Path) -> Path:
    payload = {"tools": build_catalog(registry)}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path


def read_catalog(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())
