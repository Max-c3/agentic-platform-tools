from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/admin_reads.py"
VERSION = "2.1.0"


class ListUsersInput(BaseModel):
    email: Optional[str] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListUsersOutput(BaseModel):
    users: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_users",
        display_name="List Gem Users",
        source_path=SOURCE,
        function_name="run_list_users",
        owner="recruiting-platform",
        version=VERSION,
        description="List Gem users with pagination.",
        input_schema=ListUsersInput.model_json_schema(),
        output_schema=ListUsersOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["invalid_pagination", "upstream_unavailable"],
        examples=["email='recruiter@example.com'"],
        anti_patterns=["page_size > 100"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListUsersInput,
    output_model=ListUsersOutput,
)
def run_list_users(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListUsersInput.model_validate(payload)
    result = get_gem_client().list_users(email=data.email, page=data.page, page_size=data.page_size)
    out = ListUsersOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.users)} Gem users on page {out.pagination.get('page', data.page)}.",
    }


class ListCustomFieldsInput(BaseModel):
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    project_id: Optional[str] = None
    scope: Optional[Literal["team", "project"]] = None
    is_hidden: Optional[bool] = None
    name: Optional[str] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListCustomFieldsOutput(BaseModel):
    custom_fields: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_custom_fields",
        display_name="List Gem Custom Fields",
        source_path=SOURCE,
        function_name="run_list_custom_fields",
        owner="recruiting-platform",
        version=VERSION,
        description="List Gem candidate custom fields.",
        input_schema=ListCustomFieldsInput.model_json_schema(),
        output_schema=ListCustomFieldsOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["invalid_pagination", "upstream_unavailable"],
        examples=["scope='team'"],
        anti_patterns=["page_size > 100"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListCustomFieldsInput,
    output_model=ListCustomFieldsOutput,
)
def run_list_custom_fields(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListCustomFieldsInput.model_validate(payload)
    result = get_gem_client().list_custom_fields(
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        project_id=data.project_id,
        scope=data.scope,
        is_hidden=data.is_hidden,
        name=data.name,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListCustomFieldsOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.custom_fields)} Gem custom fields on page {out.pagination.get('page', data.page)}.",
    }


class ListCustomFieldOptionsInput(BaseModel):
    custom_field_id: str = Field(min_length=1)
    value: Optional[str] = None
    is_hidden: Optional[bool] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListCustomFieldOptionsOutput(BaseModel):
    custom_field_id: str
    options: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_custom_field_options",
        display_name="List Gem Custom Field Options",
        source_path=SOURCE,
        function_name="run_list_custom_field_options",
        owner="recruiting-platform",
        version=VERSION,
        description="List options for a Gem custom field.",
        input_schema=ListCustomFieldOptionsInput.model_json_schema(),
        output_schema=ListCustomFieldOptionsOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_custom_field", "invalid_pagination", "upstream_unavailable"],
        examples=["custom_field_id='cf_123'"],
        anti_patterns=["empty custom_field_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListCustomFieldOptionsInput,
    output_model=ListCustomFieldOptionsOutput,
)
def run_list_custom_field_options(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListCustomFieldOptionsInput.model_validate(payload)
    result = get_gem_client().list_custom_field_options(
        custom_field_id=data.custom_field_id,
        value=data.value,
        is_hidden=data.is_hidden,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListCustomFieldOptionsOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.options)} options for Gem custom field {out.custom_field_id}.",
    }


class ListProjectFieldsInput(BaseModel):
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    is_hidden: Optional[bool] = None
    is_required: Optional[bool] = None
    name: Optional[str] = None
    field_type: Optional[Literal["text", "single_select", "multi_select"]] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListProjectFieldsOutput(BaseModel):
    project_fields: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_project_fields",
        display_name="List Gem Project Fields",
        source_path=SOURCE,
        function_name="run_list_project_fields",
        owner="recruiting-platform",
        version=VERSION,
        description="List Gem project fields.",
        input_schema=ListProjectFieldsInput.model_json_schema(),
        output_schema=ListProjectFieldsOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["invalid_pagination", "upstream_unavailable"],
        examples=["field_type='single_select'"],
        anti_patterns=["page_size > 100"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListProjectFieldsInput,
    output_model=ListProjectFieldsOutput,
)
def run_list_project_fields(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListProjectFieldsInput.model_validate(payload)
    result = get_gem_client().list_project_fields(
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        is_hidden=data.is_hidden,
        is_required=data.is_required,
        name=data.name,
        field_type=data.field_type,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListProjectFieldsOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.project_fields)} Gem project fields on page {out.pagination.get('page', data.page)}.",
    }


class ListProjectFieldOptionsInput(BaseModel):
    project_field_id: str = Field(min_length=1)
    value: Optional[str] = None
    is_hidden: Optional[bool] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListProjectFieldOptionsOutput(BaseModel):
    project_field_id: str
    options: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_project_field_options",
        display_name="List Gem Project Field Options",
        source_path=SOURCE,
        function_name="run_list_project_field_options",
        owner="recruiting-platform",
        version=VERSION,
        description="List options for a Gem project field.",
        input_schema=ListProjectFieldOptionsInput.model_json_schema(),
        output_schema=ListProjectFieldOptionsOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_project_field", "invalid_pagination", "upstream_unavailable"],
        examples=["project_field_id='pf_123'"],
        anti_patterns=["empty project_field_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListProjectFieldOptionsInput,
    output_model=ListProjectFieldOptionsOutput,
)
def run_list_project_field_options(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListProjectFieldOptionsInput.model_validate(payload)
    result = get_gem_client().list_project_field_options(
        project_field_id=data.project_field_id,
        value=data.value,
        is_hidden=data.is_hidden,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListProjectFieldOptionsOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.options)} options for Gem project field {out.project_field_id}.",
    }


class ListSequencesInput(BaseModel):
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    user_id: Optional[str] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListSequencesOutput(BaseModel):
    sequences: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_sequences",
        display_name="List Gem Sequences",
        source_path=SOURCE,
        function_name="run_list_sequences",
        owner="recruiting-platform",
        version=VERSION,
        description="List Gem sequences.",
        input_schema=ListSequencesInput.model_json_schema(),
        output_schema=ListSequencesOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["invalid_pagination", "upstream_unavailable"],
        examples=["user_id='user_123'"],
        anti_patterns=["page_size > 100"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListSequencesInput,
    output_model=ListSequencesOutput,
)
def run_list_sequences(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListSequencesInput.model_validate(payload)
    result = get_gem_client().list_sequences(
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        user_id=data.user_id,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListSequencesOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.sequences)} Gem sequences on page {out.pagination.get('page', data.page)}.",
    }


class GetSequenceInput(BaseModel):
    sequence_id: str = Field(min_length=1)


class GetSequenceOutput(BaseModel):
    sequence_id: str
    sequence: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.get_sequence",
        display_name="Get Gem Sequence",
        source_path=SOURCE,
        function_name="run_get_sequence",
        owner="recruiting-platform",
        version=VERSION,
        description="Fetch a Gem sequence by id.",
        input_schema=GetSequenceInput.model_json_schema(),
        output_schema=GetSequenceOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_sequence", "upstream_unavailable"],
        examples=["sequence_id='seq_123'"],
        anti_patterns=["empty sequence_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=GetSequenceInput,
    output_model=GetSequenceOutput,
)
def run_get_sequence(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = GetSequenceInput.model_validate(payload)
    result = get_gem_client().get_sequence(data.sequence_id)
    out = GetSequenceOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Fetched Gem sequence {out.sequence_id}.",
    }
