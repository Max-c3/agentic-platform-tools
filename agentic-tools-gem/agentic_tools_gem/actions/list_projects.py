from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/list_projects.py"


class Input(BaseModel):
    owner_user_id: Optional[str] = None
    readable_by_user_id: Optional[str] = None
    writable_by_user_id: Optional[str] = None
    is_archived: Optional[bool] = None
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class Output(BaseModel):
    projects: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_projects",
        display_name="List Gem Projects",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.0.0",
        description="List existing Gem projects with pagination and optional access filters.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["invalid_pagination", "upstream_unavailable"],
        examples=[
            "page=1, page_size=20",
            "readable_by_user_id='user_123', is_archived=False",
        ],
        anti_patterns=["page_size > 100"],
        integration="gem",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    result = get_gem_client().list_projects(
        owner_user_id=data.owner_user_id,
        readable_by_user_id=data.readable_by_user_id,
        writable_by_user_id=data.writable_by_user_id,
        is_archived=data.is_archived,
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        page=data.page,
        page_size=data.page_size,
    )
    out = Output.model_validate(result)
    pagination = out.pagination
    total = pagination.get("total")
    total_text = f"/{total}" if isinstance(total, int) else ""
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.projects)} Gem projects on page {pagination.get('page', data.page)}{total_text}.",
    }
