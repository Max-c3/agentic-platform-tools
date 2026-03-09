from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/create_project.py"


class Input(BaseModel):
    project_name: str = Field(min_length=2)
    metadata: dict[str, Any] = Field(default_factory=dict)
    user_id: Optional[str] = None


class Output(BaseModel):
    project_id: str
    name: str
    user_id: str = ""
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.create_project",
        display_name="Create Gem Project",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.1.0",
        description="Create a project in Gem.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="Creates a project in Gem.",
        approval_class="checkpoint_low",
        common_failures=["invalid_project_name"],
        examples=["project_name='Backend Hiring Sprint'"],
        anti_patterns=["project_name too short"],
        integration="gem",
        is_write=True,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = Input.model_validate(payload)
    if preview:
        fake_project_id = f"preview_{data.project_name.lower().replace(' ', '_')[:20]}"
        preview_output = Output(
            project_id=fake_project_id,
            name=data.project_name,
            user_id=data.user_id or "",
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will create Gem project '{data.project_name}'.",
            "compensation": {"type": "logical_revert", "reason": "archive or delete created project"},
        }

    created = get_gem_client().create_project(
        project_name=data.project_name,
        metadata=data.metadata,
        user_id=data.user_id,
    )
    out = Output.model_validate(created)
    return {
        "output": out.model_dump(),
        "summary": f"Created Gem project '{out.name}' ({out.project_id}).",
    }
