from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field, model_validator

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/project_writes.py"
VERSION = "2.1.0"


class UpdateProjectInput(BaseModel):
    project_id: str = Field(min_length=1)
    user_id: Optional[str] = None
    name: Optional[str] = None
    privacy_type: Optional[Literal["confidential", "personal", "shared"]] = None
    description: Optional[str] = None
    is_archived: Optional[bool] = None


class UpdateProjectOutput(BaseModel):
    project_id: str
    project: dict[str, Any] = Field(default_factory=dict)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.update_project",
        display_name="Update Gem Project",
        source_path=SOURCE,
        function_name="run_update_project",
        owner="recruiting-platform",
        version=VERSION,
        description="Update a Gem project using the native project update fields.",
        input_schema=UpdateProjectInput.model_json_schema(),
        output_schema=UpdateProjectOutput.model_json_schema(),
        side_effects="Updates a project in Gem.",
        approval_class="none",
        common_failures=["unknown_project", "validation_error"],
        examples=["project_id='proj_123', is_archived=True"],
        anti_patterns=["empty project_id"],
        integration="gem",
        is_write=True,
    ),
    input_model=UpdateProjectInput,
    output_model=UpdateProjectOutput,
)
def run_update_project(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = UpdateProjectInput.model_validate(payload)
    raw = data.model_dump(exclude_unset=True)
    project_id = str(raw.pop("project_id"))
    if preview:
        preview_project = dict(raw)
        preview_project["project_id"] = project_id
        preview_output = UpdateProjectOutput(
            project_id=project_id,
            project=preview_project,
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will update Gem project {project_id}.",
            "compensation": {"type": "logical_revert", "reason": "restore previous project values"},
        }

    result = get_gem_client().update_project(project_id, raw)
    out = UpdateProjectOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Updated Gem project {out.project_id}.",
    }


class RemoveCandidatesFromProjectInput(BaseModel):
    project_id: str = Field(min_length=1)
    candidate_ids: list[str] = Field(min_length=1)
    user_id: Optional[str] = None


class RemoveCandidatesFromProjectOutput(BaseModel):
    project_id: str
    removed_candidate_ids: list[str] = Field(default_factory=list)
    already_missing_candidate_ids: list[str] = Field(default_factory=list)
    user_id: str = ""
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.remove_candidates_from_project",
        display_name="Remove Candidates From Gem Project",
        source_path=SOURCE,
        function_name="run_remove_candidates_from_project",
        owner="recruiting-platform",
        version=VERSION,
        description="Remove candidates from a Gem project with partial-success reporting.",
        input_schema=RemoveCandidatesFromProjectInput.model_json_schema(),
        output_schema=RemoveCandidatesFromProjectOutput.model_json_schema(),
        side_effects="Removes candidates from a Gem project.",
        approval_class="none",
        common_failures=["unknown_project", "validation_error"],
        examples=["project_id='proj_123', candidate_ids=['cand_1', 'cand_2']"],
        anti_patterns=["empty candidate_ids"],
        integration="gem",
        is_write=True,
    ),
    input_model=RemoveCandidatesFromProjectInput,
    output_model=RemoveCandidatesFromProjectOutput,
)
def run_remove_candidates_from_project(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = RemoveCandidatesFromProjectInput.model_validate(payload)
    if preview:
        preview_output = RemoveCandidatesFromProjectOutput(
            project_id=data.project_id,
            removed_candidate_ids=data.candidate_ids,
            already_missing_candidate_ids=[],
            user_id=data.user_id or "",
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will remove {len(data.candidate_ids)} candidates from Gem project {data.project_id}.",
            "compensation": {"type": "logical_revert", "reason": "re-add removed candidates to the project"},
        }

    result = get_gem_client().remove_candidates_from_project(
        project_id=data.project_id,
        candidate_ids=data.candidate_ids,
        user_id=data.user_id,
    )
    out = RemoveCandidatesFromProjectOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Removed {len(out.removed_candidate_ids)} candidates from Gem project {out.project_id}.",
    }


class SetProjectFieldValueInput(BaseModel):
    project_id: str = Field(min_length=1)
    project_field_id: str = Field(min_length=1)
    operation: Literal["add", "remove"]
    option_ids: list[str] = Field(default_factory=list)
    text: Optional[str] = None

    @model_validator(mode="after")
    def validate_payload(self) -> "SetProjectFieldValueInput":
        if self.operation == "add" and not (self.option_ids or (self.text and self.text.strip())):
            raise ValueError("Provide option_ids or text when operation is add.")
        return self


class SetProjectFieldValueOutput(BaseModel):
    project_id: str
    project_field_id: str
    operation: str
    option_ids: list[str] = Field(default_factory=list)
    text: str = ""
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.set_project_field_value",
        display_name="Set Gem Project Field Value",
        source_path=SOURCE,
        function_name="run_set_project_field_value",
        owner="recruiting-platform",
        version=VERSION,
        description="Set or remove a project field value association on a Gem project.",
        input_schema=SetProjectFieldValueInput.model_json_schema(),
        output_schema=SetProjectFieldValueOutput.model_json_schema(),
        side_effects="Updates a Gem project field value.",
        approval_class="none",
        common_failures=["unknown_project", "unknown_project_field", "validation_error"],
        examples=["project_id='proj_123', project_field_id='pf_1', operation='add', option_ids=['opt_1']"],
        anti_patterns=["missing add payload"],
        integration="gem",
        is_write=True,
    ),
    input_model=SetProjectFieldValueInput,
    output_model=SetProjectFieldValueOutput,
)
def run_set_project_field_value(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = SetProjectFieldValueInput.model_validate(payload)
    if preview:
        preview_output = SetProjectFieldValueOutput(
            project_id=data.project_id,
            project_field_id=data.project_field_id,
            operation=data.operation,
            option_ids=data.option_ids,
            text=data.text or "",
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will {data.operation} a project field value on Gem project {data.project_id}.",
            "compensation": {"type": "logical_revert", "reason": "restore the previous project field value"},
        }

    result = get_gem_client().set_project_field_value(
        project_id=data.project_id,
        project_field_id=data.project_field_id,
        operation=data.operation,
        option_ids=data.option_ids,
        text=data.text,
    )
    out = SetProjectFieldValueOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Updated Gem project field {out.project_field_id} on project {out.project_id}.",
    }
