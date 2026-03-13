from __future__ import annotations

import json
from pathlib import Path

from agentic_tools_core.registry import ToolRegistry
from agentic_tools_gem import register_tools
from agentic_tools_gem.catalog import generate_catalog

EXPECTED_TOOL_IDS = {
    "gem.add_custom_field_options",
    "gem.add_candidate_note",
    "gem.add_profiles_to_project",
    "gem.create_candidate",
    "gem.create_custom_field",
    "gem.create_project_field",
    "gem.create_project_field_option",
    "gem.create_project",
    "gem.find_candidates",
    "gem.find_projects",
    "gem.get_project",
    "gem.get_sequence",
    "gem.get_candidate",
    "gem.list_candidate_notes",
    "gem.list_candidates",
    "gem.list_custom_field_options",
    "gem.list_custom_fields",
    "gem.list_project_candidates",
    "gem.list_project_field_options",
    "gem.list_project_fields",
    "gem.list_project_membership_log",
    "gem.list_projects",
    "gem.list_sequences",
    "gem.list_uploaded_resumes",
    "gem.list_users",
    "gem.remove_candidates_from_project",
    "gem.set_project_field_value",
    "gem.set_custom_value",
    "gem.update_candidate",
    "gem.update_custom_field_option",
    "gem.update_project",
    "gem.update_project_field_option",
    "gem.upload_resume",
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


def test_checked_in_catalog_matches_generated_catalog(tmp_path: Path) -> None:
    generated = generate_catalog(tmp_path / "tool_catalog.json")
    expected = json.loads(Path("tool_catalog.json").read_text())
    payload = json.loads(generated.read_text())
    assert payload == expected


def test_write_tools_do_not_advertise_checkpoint_approvals() -> None:
    registry = ToolRegistry()
    register_tools(registry)
    write_definitions = [item for item in registry.list_definitions() if item.integration == "gem" and item.is_write]
    assert write_definitions
    assert all(item.approval_class == "none" for item in write_definitions)


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
