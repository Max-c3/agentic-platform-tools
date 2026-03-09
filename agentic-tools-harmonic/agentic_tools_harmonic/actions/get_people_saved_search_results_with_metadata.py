from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_core.common.identity.deduplicate_profiles import deduplicate_profiles
from agentic_tools_harmonic.bootstrap import tool
from agentic_tools_harmonic.runtime_clients import get_harmonic_client

SOURCE = "app/tools/harmonic/actions/get_people_saved_search_results_with_metadata.py"


class Input(BaseModel):
    saved_search_id_or_urn: str = Field(min_length=1)
    size: int = Field(default=100, ge=1, le=1000)
    cursor: Optional[str] = None


class Output(BaseModel):
    saved_search_id_or_urn: str
    count: int = 0
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    dedupe_report: dict[str, Any] = Field(default_factory=dict)
    page_info: dict[str, Any] = Field(default_factory=dict)
    raw_metadata: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="harmonic.get_people_saved_search_results_with_metadata",
        display_name="Get People Saved Search Results With Metadata",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.0.0",
        description="Retrieve people saved search results and metadata from Harmonic.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["saved_search_not_found", "upstream_unavailable"],
        examples=["saved_search_id_or_urn='urn:harmonic:saved_search:123'"],
        anti_patterns=["size > 1000"],
        integration="harmonic",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    result = get_harmonic_client().get_people_saved_search_results_with_metadata(
        data.saved_search_id_or_urn,
        size=data.size,
        cursor=data.cursor,
    )
    deduped, report = deduplicate_profiles(result.get("candidates", []))
    normalized = Output.model_validate(
        {
            **result,
            "candidates": deduped,
            "dedupe_report": report,
        }
    )
    return {
        "output": normalized.model_dump(),
        "summary": f"Loaded {len(normalized.candidates)} people from Harmonic saved search {normalized.saved_search_id_or_urn}.",
    }
