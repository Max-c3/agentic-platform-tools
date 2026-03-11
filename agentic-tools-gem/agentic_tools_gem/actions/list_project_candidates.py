from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/list_project_candidates.py"


class Input(BaseModel):
    project_id: str = Field(min_length=1)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    added_after: Optional[int] = Field(default=None, ge=1)
    added_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    include_candidates: bool = True


class Output(BaseModel):
    project_id: str
    project: dict[str, Any] = Field(default_factory=dict)
    entries: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)
    unresolved_candidate_ids: list[str] = Field(default_factory=list)


@tool(
    ToolDefinition(
        tool_id="gem.list_project_candidates",
        display_name="List Gem Project Candidates",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="2.1.0",
        description="List candidates in a Gem project and optionally hydrate each entry with full candidate data.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_project", "invalid_pagination", "upstream_unavailable"],
        examples=[
            "project_id='proj_123'",
            "project_id='proj_123', page=2, page_size=50, include_candidates=True",
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
    result = get_gem_client().list_project_candidates(
        project_id=data.project_id,
        page=data.page,
        page_size=data.page_size,
        added_after=data.added_after,
        added_before=data.added_before,
        sort=data.sort,
        include_candidates=data.include_candidates,
    )
    out = Output.model_validate(result)
    unresolved = len(out.unresolved_candidate_ids)
    unresolved_text = f", unresolved={unresolved}" if unresolved else ""
    return {
        "output": out.model_dump(),
        "summary": (
            f"Returned {len(out.entries)} candidates for Gem project {out.project_id} "
            f"(page {out.pagination.get('page', data.page)}{unresolved_text})."
        ),
    }
