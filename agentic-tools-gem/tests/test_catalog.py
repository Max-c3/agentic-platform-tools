from __future__ import annotations

import json
from pathlib import Path

from agentic_tools_core.registry import ToolRegistry
from agentic_tools_gem import register_tools
from agentic_tools_gem.catalog import generate_catalog

EXPECTED_TOOL_IDS = {
    "gem.add_candidate_note",
    "gem.add_profiles_to_project",
    "gem.create_project",
    "gem.set_custom_value",
}


def test_registers_expected_tool_ids() -> None:
    registry = ToolRegistry()
    register_tools(registry)
    assert {item.tool_id for item in registry.list_definitions()} == EXPECTED_TOOL_IDS


def test_generated_catalog_matches_registry(tmp_path: Path) -> None:
    registry = ToolRegistry()
    register_tools(registry)
    generated = generate_catalog(tmp_path / "tool_catalog.json")
    payload = json.loads(generated.read_text())
    assert {tool["tool_id"] for tool in payload["tools"]} == EXPECTED_TOOL_IDS


def test_checked_in_catalog_has_required_fields() -> None:
    payload = json.loads(Path("tool_catalog.json").read_text())
    required = {
        "tool_id",
        "display_name",
        "source_path",
        "function_name",
        "input_schema",
        "output_schema",
        "side_effects",
        "approval_class",
        "common_failures",
        "examples",
        "anti_patterns",
        "owner",
        "version",
        "integration",
        "is_write",
    }
    assert payload["tools"]
    for tool in payload["tools"]:
        assert required.issubset(tool.keys())
