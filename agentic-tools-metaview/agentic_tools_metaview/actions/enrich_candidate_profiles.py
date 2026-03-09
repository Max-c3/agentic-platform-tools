from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_core.common.identity.deduplicate_profiles import deduplicate_profiles
from agentic_tools_metaview.bootstrap import tool
from agentic_tools_metaview.runtime_clients import get_metaview_client

SOURCE = "app/tools/metaview/actions/enrich_candidate_profiles.py"


class Input(BaseModel):
    profiles: list[dict[str, Any]] = Field(default_factory=list)


class Output(BaseModel):
    candidates: list[dict[str, Any]]
    dedupe_report: dict[str, Any]


@tool(
    ToolDefinition(
        tool_id="metaview.enrich_candidate_profiles",
        display_name="Enrich Candidate Profiles",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.0.0",
        description="Enrich candidate profiles in Metaview.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["rate_limit", "upstream_unavailable"],
        examples=["profiles=[...]"],
        anti_patterns=["empty profiles"],
        integration="metaview",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    enriched = get_metaview_client().enrich_candidate_profiles(profiles=data.profiles)
    deduped, report = deduplicate_profiles(enriched)
    normalized = Output(candidates=deduped, dedupe_report=report)
    return {
        "output": normalized.model_dump(),
        "summary": f"Enriched {len(normalized.candidates)} profiles with Metaview.",
    }
