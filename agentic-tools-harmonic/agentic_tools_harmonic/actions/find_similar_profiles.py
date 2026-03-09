from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_core.common.identity.deduplicate_profiles import deduplicate_profiles
from agentic_tools_harmonic.bootstrap import tool
from agentic_tools_harmonic.runtime_clients import get_harmonic_client

SOURCE = "app/tools/harmonic/actions/find_similar_profiles.py"


class Input(BaseModel):
    seed_profiles: list[dict[str, Any]] = Field(default_factory=list)
    per_seed: int = Field(default=10, ge=1, le=50)


class Output(BaseModel):
    candidates: list[dict[str, Any]]
    dedupe_report: dict[str, Any]


@tool(
    ToolDefinition(
        tool_id="harmonic.find_similar_profiles",
        display_name="Find Similar Profiles",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.0.0",
        description="Find similar candidate profiles in Harmonic.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["rate_limit", "upstream_unavailable"],
        examples=["per_seed=10"],
        anti_patterns=["missing seed profiles"],
        integration="harmonic",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    candidates = get_harmonic_client().find_similar_profiles(seed_profiles=data.seed_profiles, per_seed=data.per_seed)
    deduped, report = deduplicate_profiles(candidates)
    normalized = Output(candidates=deduped, dedupe_report=report)
    return {
        "output": normalized.model_dump(),
        "summary": f"Found {len(normalized.candidates)} similar profiles from Harmonic.",
    }
