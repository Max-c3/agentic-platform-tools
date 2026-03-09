from __future__ import annotations

from pathlib import Path

from agentic_tools_core.catalog import write_catalog
from agentic_tools_core.registry import ToolRegistry
from agentic_tools_harmonic import register_tools


def generate_catalog(path: Path | None = None) -> Path:
    target = path or Path(__file__).resolve().parents[1] / "tool_catalog.json"
    registry = ToolRegistry()
    register_tools(registry)
    return write_catalog(registry, target)
