from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/admin_writes.py"
VERSION = "2.1.0"


class CreateCustomFieldInput(BaseModel):
    name: str = Field(min_length=1, max_length=50)
    value_type: Literal["date", "text", "single_select", "multi_select"]
    scope: Literal["team", "project"]
    project_id: Optional[str] = None
    option_values: list[str] = Field(default_factory=list)


class CreateCustomFieldOutput(BaseModel):
    custom_field_id: str
    custom_field: dict[str, Any] = Field(default_factory=dict)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.create_custom_field",
        display_name="Create Gem Custom Field",
        source_path=SOURCE,
        function_name="run_create_custom_field",
        owner="recruiting-platform",
        version=VERSION,
        description="Create a Gem custom field.",
        input_schema=CreateCustomFieldInput.model_json_schema(),
        output_schema=CreateCustomFieldOutput.model_json_schema(),
        side_effects="Creates a custom field in Gem.",
        approval_class="none",
        common_failures=["duplicate_custom_field", "validation_error"],
        examples=["name='Priority', value_type='single_select', scope='team'"],
        anti_patterns=["empty custom field name"],
        integration="gem",
        is_write=True,
    ),
    input_model=CreateCustomFieldInput,
    output_model=CreateCustomFieldOutput,
)
def run_create_custom_field(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = CreateCustomFieldInput.model_validate(payload)
    if preview:
        preview_output = CreateCustomFieldOutput(
            custom_field_id="preview_custom_field",
            custom_field={
                "custom_field_id": "preview_custom_field",
                "name": data.name,
                "value_type": data.value_type,
                "scope": data.scope,
                "project_id": data.project_id,
                "options": [{"value": item} for item in data.option_values],
            },
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will create Gem custom field {data.name}.",
            "compensation": {"type": "logical_revert", "reason": "hide or remove the created field manually if needed"},
        }

    result = get_gem_client().create_custom_field(
        name=data.name,
        value_type=data.value_type,
        scope=data.scope,
        project_id=data.project_id,
        option_values=data.option_values,
    )
    out = CreateCustomFieldOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Created Gem custom field {out.custom_field_id}.",
    }


class AddCustomFieldOptionsInput(BaseModel):
    custom_field_id: str = Field(min_length=1)
    option_values: list[str] = Field(min_length=1)


class AddCustomFieldOptionsOutput(BaseModel):
    custom_field_id: str
    option_ids: list[str] = Field(default_factory=list)
    options: list[dict[str, Any]] = Field(default_factory=list)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.add_custom_field_options",
        display_name="Add Gem Custom Field Options",
        source_path=SOURCE,
        function_name="run_add_custom_field_options",
        owner="recruiting-platform",
        version=VERSION,
        description="Add options to a Gem custom field.",
        input_schema=AddCustomFieldOptionsInput.model_json_schema(),
        output_schema=AddCustomFieldOptionsOutput.model_json_schema(),
        side_effects="Creates custom field options in Gem.",
        approval_class="none",
        common_failures=["unknown_custom_field", "duplicate_option", "validation_error"],
        examples=["custom_field_id='cf_123', option_values=['High', 'Medium']"],
        anti_patterns=["empty option_values"],
        integration="gem",
        is_write=True,
    ),
    input_model=AddCustomFieldOptionsInput,
    output_model=AddCustomFieldOptionsOutput,
)
def run_add_custom_field_options(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = AddCustomFieldOptionsInput.model_validate(payload)
    if preview:
        preview_output = AddCustomFieldOptionsOutput(
            custom_field_id=data.custom_field_id,
            option_ids=[f"preview_option_{idx + 1}" for idx, _ in enumerate(data.option_values)],
            options=[{"value": value} for value in data.option_values],
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will add {len(data.option_values)} options to Gem custom field {data.custom_field_id}.",
            "compensation": {"type": "logical_revert", "reason": "hide added options if needed"},
        }

    result = get_gem_client().add_custom_field_options(
        custom_field_id=data.custom_field_id,
        option_values=data.option_values,
    )
    out = AddCustomFieldOptionsOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Added {len(out.option_ids)} options to Gem custom field {out.custom_field_id}.",
    }


class UpdateCustomFieldOptionInput(BaseModel):
    custom_field_id: str = Field(min_length=1)
    option_id: str = Field(min_length=1)
    is_hidden: bool


class UpdateCustomFieldOptionOutput(BaseModel):
    custom_field_id: str
    option_id: str
    option: dict[str, Any] = Field(default_factory=dict)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.update_custom_field_option",
        display_name="Update Gem Custom Field Option",
        source_path=SOURCE,
        function_name="run_update_custom_field_option",
        owner="recruiting-platform",
        version=VERSION,
        description="Update the visibility of a Gem custom field option.",
        input_schema=UpdateCustomFieldOptionInput.model_json_schema(),
        output_schema=UpdateCustomFieldOptionOutput.model_json_schema(),
        side_effects="Updates a custom field option in Gem.",
        approval_class="none",
        common_failures=["unknown_option", "validation_error"],
        examples=["custom_field_id='cf_123', option_id='opt_1', is_hidden=True"],
        anti_patterns=["empty option_id"],
        integration="gem",
        is_write=True,
    ),
    input_model=UpdateCustomFieldOptionInput,
    output_model=UpdateCustomFieldOptionOutput,
)
def run_update_custom_field_option(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = UpdateCustomFieldOptionInput.model_validate(payload)
    if preview:
        preview_output = UpdateCustomFieldOptionOutput(
            custom_field_id=data.custom_field_id,
            option_id=data.option_id,
            option={"option_id": data.option_id, "is_hidden": data.is_hidden},
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will update Gem custom field option {data.option_id}.",
            "compensation": {"type": "logical_revert", "reason": "restore previous option visibility"},
        }

    result = get_gem_client().update_custom_field_option(
        custom_field_id=data.custom_field_id,
        option_id=data.option_id,
        is_hidden=data.is_hidden,
    )
    out = UpdateCustomFieldOptionOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Updated Gem custom field option {out.option_id}.",
    }


class CreateProjectFieldInput(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    field_type: Literal["text", "single_select", "multi_select"]
    options: list[str] = Field(default_factory=list)
    is_required: Optional[bool] = None


class CreateProjectFieldOutput(BaseModel):
    project_field_id: str
    project_field: dict[str, Any] = Field(default_factory=dict)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.create_project_field",
        display_name="Create Gem Project Field",
        source_path=SOURCE,
        function_name="run_create_project_field",
        owner="recruiting-platform",
        version=VERSION,
        description="Create a Gem project field.",
        input_schema=CreateProjectFieldInput.model_json_schema(),
        output_schema=CreateProjectFieldOutput.model_json_schema(),
        side_effects="Creates a project field in Gem.",
        approval_class="none",
        common_failures=["duplicate_project_field", "validation_error"],
        examples=["name='Pipeline', field_type='single_select', options=['Sourced']"],
        anti_patterns=["empty project field name"],
        integration="gem",
        is_write=True,
    ),
    input_model=CreateProjectFieldInput,
    output_model=CreateProjectFieldOutput,
)
def run_create_project_field(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = CreateProjectFieldInput.model_validate(payload)
    if preview:
        preview_output = CreateProjectFieldOutput(
            project_field_id="preview_project_field",
            project_field={
                "project_field_id": "preview_project_field",
                "name": data.name,
                "field_type": data.field_type,
                "options": [{"value": item} for item in data.options],
                "is_required": data.is_required,
            },
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will create Gem project field {data.name}.",
            "compensation": {"type": "logical_revert", "reason": "hide or remove the created project field if needed"},
        }

    result = get_gem_client().create_project_field(
        name=data.name,
        field_type=data.field_type,
        options=data.options,
        is_required=data.is_required,
    )
    out = CreateProjectFieldOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Created Gem project field {out.project_field_id}.",
    }


class CreateProjectFieldOptionInput(BaseModel):
    project_field_id: str = Field(min_length=1)
    options: list[str] = Field(min_length=1)


class CreateProjectFieldOptionOutput(BaseModel):
    project_field_id: str
    option_ids: list[str] = Field(default_factory=list)
    options: list[dict[str, Any]] = Field(default_factory=list)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.create_project_field_option",
        display_name="Create Gem Project Field Option",
        source_path=SOURCE,
        function_name="run_create_project_field_option",
        owner="recruiting-platform",
        version=VERSION,
        description="Create options for a Gem project field.",
        input_schema=CreateProjectFieldOptionInput.model_json_schema(),
        output_schema=CreateProjectFieldOptionOutput.model_json_schema(),
        side_effects="Creates project field options in Gem.",
        approval_class="none",
        common_failures=["unknown_project_field", "validation_error"],
        examples=["project_field_id='pf_123', options=['Offer']"],
        anti_patterns=["empty options"],
        integration="gem",
        is_write=True,
    ),
    input_model=CreateProjectFieldOptionInput,
    output_model=CreateProjectFieldOptionOutput,
)
def run_create_project_field_option(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = CreateProjectFieldOptionInput.model_validate(payload)
    if preview:
        preview_output = CreateProjectFieldOptionOutput(
            project_field_id=data.project_field_id,
            option_ids=[f"preview_project_field_option_{idx + 1}" for idx, _ in enumerate(data.options)],
            options=[{"value": value} for value in data.options],
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will add {len(data.options)} options to Gem project field {data.project_field_id}.",
            "compensation": {"type": "logical_revert", "reason": "hide added project field options if needed"},
        }

    result = get_gem_client().create_project_field_option(
        project_field_id=data.project_field_id,
        options=data.options,
    )
    out = CreateProjectFieldOptionOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Added {len(out.option_ids)} options to Gem project field {out.project_field_id}.",
    }


class UpdateProjectFieldOptionInput(BaseModel):
    project_field_id: str = Field(min_length=1)
    project_field_option_id: str = Field(min_length=1)
    is_hidden: bool


class UpdateProjectFieldOptionOutput(BaseModel):
    project_field_id: str
    project_field_option_id: str
    option: dict[str, Any] = Field(default_factory=dict)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.update_project_field_option",
        display_name="Update Gem Project Field Option",
        source_path=SOURCE,
        function_name="run_update_project_field_option",
        owner="recruiting-platform",
        version=VERSION,
        description="Update the visibility of a Gem project field option.",
        input_schema=UpdateProjectFieldOptionInput.model_json_schema(),
        output_schema=UpdateProjectFieldOptionOutput.model_json_schema(),
        side_effects="Updates a project field option in Gem.",
        approval_class="none",
        common_failures=["unknown_project_field_option", "validation_error"],
        examples=["project_field_id='pf_123', project_field_option_id='opt_1', is_hidden=True"],
        anti_patterns=["empty project_field_option_id"],
        integration="gem",
        is_write=True,
    ),
    input_model=UpdateProjectFieldOptionInput,
    output_model=UpdateProjectFieldOptionOutput,
)
def run_update_project_field_option(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = UpdateProjectFieldOptionInput.model_validate(payload)
    if preview:
        preview_output = UpdateProjectFieldOptionOutput(
            project_field_id=data.project_field_id,
            project_field_option_id=data.project_field_option_id,
            option={"project_field_option_id": data.project_field_option_id, "is_hidden": data.is_hidden},
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will update Gem project field option {data.project_field_option_id}.",
            "compensation": {"type": "logical_revert", "reason": "restore previous option visibility"},
        }

    result = get_gem_client().update_project_field_option(
        project_field_id=data.project_field_id,
        project_field_option_id=data.project_field_option_id,
        is_hidden=data.is_hidden,
    )
    out = UpdateProjectFieldOptionOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Updated Gem project field option {out.project_field_option_id}.",
    }
