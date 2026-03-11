from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field, model_validator

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/project_reads.py"
VERSION = "2.1.0"


class GetProjectInput(BaseModel):
    project_id: str = Field(min_length=1)


class GetProjectOutput(BaseModel):
    project_id: str
    project: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.get_project",
        display_name="Get Gem Project",
        source_path=SOURCE,
        function_name="run_get_project",
        owner="recruiting-platform",
        version=VERSION,
        description="Fetch a Gem project by id.",
        input_schema=GetProjectInput.model_json_schema(),
        output_schema=GetProjectOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_project", "upstream_unavailable"],
        examples=["project_id='proj_123'"],
        anti_patterns=["empty project_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=GetProjectInput,
    output_model=GetProjectOutput,
)
def run_get_project(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = GetProjectInput.model_validate(payload)
    result = get_gem_client().get_project(data.project_id)
    out = GetProjectOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Fetched Gem project {out.project_id}.",
    }


class FindProjectsInput(BaseModel):
    name_exact: Optional[str] = None
    name_contains: Optional[str] = None
    owner_user_id: Optional[str] = None
    readable_by_user_id: Optional[str] = None
    writable_by_user_id: Optional[str] = None
    is_archived: Optional[bool] = None
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    max_pages: int = Field(default=5, ge=1, le=50)
    page_size: int = Field(default=100, ge=1, le=100)

    @model_validator(mode="after")
    def validate_name_filters(self) -> "FindProjectsInput":
        if not (
            (self.name_exact and self.name_exact.strip())
            or (self.name_contains and self.name_contains.strip())
        ):
            raise ValueError("Provide name_exact or name_contains.")
        return self


class FindProjectsOutput(BaseModel):
    matches: list[dict[str, Any]] = Field(default_factory=list)
    scan: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.find_projects",
        display_name="Find Gem Projects",
        source_path=SOURCE,
        function_name="run_find_projects",
        owner="recruiting-platform",
        version=VERSION,
        description="Find Gem projects by case-insensitive name matching with bounded scans.",
        input_schema=FindProjectsInput.model_json_schema(),
        output_schema=FindProjectsOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["missing_name_filter", "upstream_unavailable"],
        examples=["name_contains='backend'"],
        anti_patterns=["missing project name filters"],
        integration="gem",
        is_write=False,
    ),
    input_model=FindProjectsInput,
    output_model=FindProjectsOutput,
)
def run_find_projects(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = FindProjectsInput.model_validate(payload)
    result = get_gem_client().find_projects(
        name_exact=data.name_exact or "",
        name_contains=data.name_contains or "",
        owner_user_id=data.owner_user_id,
        readable_by_user_id=data.readable_by_user_id,
        writable_by_user_id=data.writable_by_user_id,
        is_archived=data.is_archived,
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        max_pages=data.max_pages,
        page_size=data.page_size,
    )
    out = FindProjectsOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Found {len(out.matches)} Gem project matches.",
    }


class ListProjectMembershipLogInput(BaseModel):
    changed_after: Optional[int] = Field(default=None, ge=1)
    changed_before: Optional[int] = Field(default=None, ge=1)
    project_id: Optional[str] = None
    candidate_id: Optional[str] = None
    sort: Optional[Literal["asc", "desc"]] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def validate_filters(self) -> "ListProjectMembershipLogInput":
        if not ((self.project_id and self.project_id.strip()) or (self.candidate_id and self.candidate_id.strip())):
            raise ValueError("Provide project_id or candidate_id.")
        return self


class ListProjectMembershipLogOutput(BaseModel):
    entries: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_project_membership_log",
        display_name="List Gem Project Membership Log",
        source_path=SOURCE,
        function_name="run_list_project_membership_log",
        owner="recruiting-platform",
        version=VERSION,
        description="List Gem project membership log entries filtered by project or candidate.",
        input_schema=ListProjectMembershipLogInput.model_json_schema(),
        output_schema=ListProjectMembershipLogOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["missing_filter", "invalid_pagination", "upstream_unavailable"],
        examples=["project_id='proj_123'"],
        anti_patterns=["missing project_id and candidate_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListProjectMembershipLogInput,
    output_model=ListProjectMembershipLogOutput,
)
def run_list_project_membership_log(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListProjectMembershipLogInput.model_validate(payload)
    result = get_gem_client().list_project_membership_log(
        changed_after=data.changed_after,
        changed_before=data.changed_before,
        project_id=data.project_id,
        candidate_id=data.candidate_id,
        sort=data.sort,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListProjectMembershipLogOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.entries)} Gem membership log entries.",
    }
