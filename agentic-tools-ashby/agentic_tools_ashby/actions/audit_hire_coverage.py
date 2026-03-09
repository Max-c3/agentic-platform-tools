from __future__ import annotations

from typing import Any
from typing import Literal

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_ashby.bootstrap import tool
from agentic_tools_ashby.runtime_clients import get_ashby_client

SOURCE = "app/tools/ashby/actions/audit_hire_coverage.py"


class Filters(BaseModel):
    status: list[Literal["hired"]] = Field(default_factory=lambda: ["hired"])
    keywords: list[str] = Field(default_factory=list)
    department_ids: list[str] = Field(default_factory=list)
    location_ids: list[str] = Field(default_factory=list)
    candidate_ids: list[str] = Field(default_factory=list)
    technical_only: bool = Field(default=False)


class Input(BaseModel):
    sample_size: int = Field(default=50, ge=1, le=200)
    max_scan_pages: int | None = Field(default=None, ge=1, le=200)
    require_fields: list[
        Literal[
            "candidate_id",
            "name",
            "email",
            "linkedin",
            "job_title",
            "status",
            "hired_at",
            "created_at",
            "updated_at",
            "department_id",
            "location_id",
        ]
    ] = Field(default_factory=list)
    filters: Filters = Field(default_factory=Filters)


class Coverage(BaseModel):
    sample_size: int
    returned_count: int
    missing_email_count: int
    missing_linkedin_count: int
    by_department: dict[str, int]
    by_location: dict[str, int]


class Output(BaseModel):
    diagnostics: dict[str, Any]
    coverage: Coverage
    confidence: float = Field(ge=0.0, le=1.0)
    sample_hires: list[dict[str, Any]]


@tool(
    ToolDefinition(
        tool_id="ashby.audit_hire_coverage",
        display_name="Audit Hire Coverage",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.0.0",
        description=(
            "Audit hire coverage quality and data completeness from Ashby. "
            "Returns diagnostics, confidence, and aggregate missing-field signals."
        ),
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["rate_limit", "upstream_unavailable"],
        examples=[
            "sample_size=50",
            "sample_size=100, filters={technical_only:true}",
            "sample_size=25, require_fields=['candidate_id','email']",
        ],
        anti_patterns=["sample_size > 200"],
        integration="ashby",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    result = get_ashby_client().audit_hire_coverage(
        sample_size=data.sample_size,
        filters=data.filters.model_dump(),
        require_fields=list(data.require_fields),
        max_scan_pages=data.max_scan_pages,
    )
    normalized = Output.model_validate(result)
    return {
        "output": normalized.model_dump(),
        "summary": (
            f"Audited Ashby hire coverage over {normalized.coverage.returned_count} records "
            f"(sample_size={normalized.coverage.sample_size})."
        ),
    }
