from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/get_candidate.py"


class Input(BaseModel):
    candidate_id: str = Field(min_length=1)


class Output(BaseModel):
    candidate_id: str
    candidate: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.get_candidate",
        display_name="Get Gem Candidate",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.1.0",
        description="Fetch a Gem candidate by id with the provider's full candidate payload.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_candidate", "upstream_unavailable"],
        examples=["candidate_id='candidate_123'"],
        anti_patterns=["empty candidate_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    result = get_gem_client().get_candidate(data.candidate_id)
    out = Output.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Fetched Gem candidate {out.candidate_id}.",
    }
