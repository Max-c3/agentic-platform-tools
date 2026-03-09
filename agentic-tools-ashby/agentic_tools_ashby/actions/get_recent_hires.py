from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_ashby.bootstrap import tool
from agentic_tools_ashby.runtime_clients import get_ashby_client

SOURCE = "app/tools/ashby/actions/get_recent_hires.py"


class Input(BaseModel):
    count: int = Field(default=10, ge=1, le=100)
    role_context: str = Field(default="")
    keywords: list[str] = Field(default_factory=list)
    selection_mode: Literal["global_latest_exact", "global_latest_best_effort", "fast_sample"] = Field(
        default="global_latest_exact"
    )
    sort_by: Literal["hired_at", "created_at", "updated_at"] = Field(default="hired_at")
    sort_order: Literal["desc", "asc"] = Field(default="desc")
    retrieval_policy: Literal["strict_count", "fast_sample"] = Field(default="strict_count")
    max_scan_pages: int | None = Field(default=None, ge=1, le=200)
    require_fields: list[Literal["candidate_id", "name", "email", "linkedin", "job_title"]] = Field(
        default_factory=lambda: ["candidate_id", "name"]
    )
    status: list[Literal["hired"]] = Field(default_factory=lambda: ["hired"])
    department_ids: list[str] = Field(default_factory=list)
    location_ids: list[str] = Field(default_factory=list)
    candidate_ids: list[str] = Field(default_factory=list)


class Output(BaseModel):
    hires: list[dict[str, Any]]
    diagnostics: dict[str, Any]
    confidence: float = Field(ge=0.0, le=1.0)


@tool(
    ToolDefinition(
        tool_id="ashby.get_recent_hires",
        display_name="Get Recent Hires",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.2.0",
        description=(
            "Pull recent hires from Ashby with deterministic default semantics "
            "(global_latest_exact, hired_at desc, strict_count) and bounded strategy knobs."
        ),
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["rate_limit", "upstream_unavailable"],
        examples=[
            "count=10",
            "count=25, keywords=['kernel', 'ml'], retrieval_policy='strict_count'",
            "count=10, department_ids=['engineering'], location_ids=['sf']",
        ],
        anti_patterns=["count > 100"],
        integration="ashby",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    result = get_ashby_client().search_hires(
        count=data.count,
        selection_mode=data.selection_mode,
        sort_by=data.sort_by,
        sort_order=data.sort_order,
        filters={
            "status": data.status,
            "keywords": data.keywords,
            "department_ids": data.department_ids,
            "location_ids": data.location_ids,
            "candidate_ids": data.candidate_ids,
        },
        retrieval_policy=data.retrieval_policy,
        max_scan_pages=data.max_scan_pages,
        require_fields=data.require_fields,
    )
    normalized = Output.model_validate(result)
    diagnostics = normalized.diagnostics
    return {
        "output": normalized.model_dump(),
        "summary": (
            f"Loaded {len(normalized.hires)} recent hires from Ashby "
            f"(requested={diagnostics.get('requested_count')}, stop_reason={diagnostics.get('stop_reason')})."
        ),
    }
