from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/set_custom_value.py"


class Input(BaseModel):
    candidate_id: str = Field(min_length=1)
    key: str = Field(min_length=1)
    value: Any
    project_id: Optional[str] = None


class Output(BaseModel):
    candidate_id: str
    key: str
    custom_field_id: str = ""
    value: Any
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.set_custom_value",
        display_name="Set Candidate Custom Value",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.1.0",
        description="Set a custom value field on a Gem candidate.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="Writes custom field to Gem candidate profile.",
        approval_class="checkpoint_high",
        common_failures=["invalid_key"],
        examples=["key='priority_tier', value='A'"],
        anti_patterns=["empty key"],
        integration="gem",
        is_write=True,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = Input.model_validate(payload)

    if preview:
        preview_output = Output(
            candidate_id=data.candidate_id,
            key=data.key,
            custom_field_id=data.key,
            value=data.value,
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will set custom value '{data.key}' on candidate {data.candidate_id}.",
            "compensation": {"type": "logical_revert", "reason": "restore previous value"},
        }

    result = get_gem_client().set_custom_value(
        candidate_id=data.candidate_id,
        key=data.key,
        value=data.value,
        project_id=data.project_id,
    )
    out = Output.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Set custom value '{out.key}' on candidate {out.candidate_id}.",
    }
