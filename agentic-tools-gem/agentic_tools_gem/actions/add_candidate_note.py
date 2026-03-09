from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/add_candidate_note.py"


class Input(BaseModel):
    candidate_id: str = Field(min_length=1)
    note: str = Field(min_length=1)
    user_id: Optional[str] = None


class Output(BaseModel):
    candidate_id: str
    note: str
    user_id: str = ""
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.add_candidate_note",
        display_name="Add Candidate Note",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.1.0",
        description="Add a note to a Gem candidate profile.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="Writes a note to Gem candidate profile.",
        approval_class="checkpoint_high",
        common_failures=["candidate_not_found"],
        examples=["candidate_id='c1', note='Strong fit'"],
        anti_patterns=["empty note"],
        integration="gem",
        is_write=True,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = Input.model_validate(payload)
    if data.note == "force_failure":
        raise RuntimeError("Forced note failure")

    if preview:
        preview_output = Output(
            candidate_id=data.candidate_id,
            note=data.note,
            user_id=data.user_id or "",
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will add note to candidate {data.candidate_id}.",
            "compensation": {"type": "logical_revert", "reason": "remove note entry"},
        }

    result = get_gem_client().add_candidate_note(
        candidate_id=data.candidate_id,
        note=data.note,
        user_id=data.user_id,
    )
    out = Output.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Added note to candidate {out.candidate_id}.",
    }
