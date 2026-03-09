from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_core.common.identity.deduplicate_profiles import deduplicate_profiles
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/add_profiles_to_project.py"


class Input(BaseModel):
    project_id: str = Field(min_length=2)
    profiles: list[dict[str, Any]] = Field(default_factory=list)
    user_id: Optional[str] = None


class Output(BaseModel):
    project_id: str
    added_candidate_ids: list[str] = Field(default_factory=list)
    mapping: list[dict[str, str]] = Field(default_factory=list)
    user_id: str = ""
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.add_profiles_to_project",
        display_name="Add Profiles To Gem Project",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.1.0",
        description="Import/upsert profiles and add them to a Gem project.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="Writes candidates into Gem project membership.",
        approval_class="checkpoint_low",
        common_failures=["invalid_project_id", "profile_validation_error"],
        examples=["project_id='proj_123'"],
        anti_patterns=["profiles empty"],
        integration="gem",
        is_write=True,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = Input.model_validate(payload)
    deduped_profiles, report = deduplicate_profiles(data.profiles)

    if preview:
        preview_ids = [str(profile.get("candidate_id") or f"preview_{idx+1}") for idx, profile in enumerate(deduped_profiles)]
        mapping = [
            {
                "source_candidate_id": _source_reference(profile),
                "gem_candidate_id": preview_ids[idx],
            }
            for idx, profile in enumerate(deduped_profiles)
        ]
        preview_output = Output(
            project_id=data.project_id,
            added_candidate_ids=preview_ids,
            mapping=mapping,
            user_id=data.user_id or "",
            provider_response={"preview": True, "dedupe_report": report},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will add {len(preview_ids)} profiles to project {data.project_id}.",
            "compensation": {"type": "logical_revert", "reason": "remove imported profiles from project"},
        }

    created = get_gem_client().add_profiles_to_project(
        project_id=data.project_id,
        profiles=deduped_profiles,
        user_id=data.user_id,
    )
    out = Output.model_validate(created)
    return {
        "output": out.model_dump(),
        "summary": f"Added {len(out.added_candidate_ids)} profiles to project {out.project_id}.",
    }


def _source_reference(profile: dict[str, Any]) -> str:
    for key in ("candidate_id", "email", "linkedin", "linked_in_handle", "name"):
        value = str(profile.get(key) or "").strip()
        if value:
            return value
    return ""
