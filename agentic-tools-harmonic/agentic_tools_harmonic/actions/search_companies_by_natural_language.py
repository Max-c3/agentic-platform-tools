from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_harmonic.bootstrap import tool
from agentic_tools_harmonic.runtime_clients import get_harmonic_client

SOURCE = "app/tools/harmonic/actions/search_companies_by_natural_language.py"


class Input(BaseModel):
    query: str = Field(min_length=1)
    similarity_threshold: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    size: int = Field(default=25, ge=1, le=1000)
    cursor: Optional[str] = None


class Output(BaseModel):
    query: str
    count: int = 0
    companies: list[dict[str, Any]] = Field(default_factory=list)
    page_info: dict[str, Any] = Field(default_factory=dict)
    query_interpretation: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="harmonic.search_companies_by_natural_language",
        display_name="Search Companies By Natural Language",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.0.0",
        description="Run Harmonic natural-language company search via search_agent.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["upstream_unavailable", "invalid_query"],
        examples=["query='Series B fintech companies with strong engineering teams'"],
        anti_patterns=["empty query"],
        integration="harmonic",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    result = get_harmonic_client().search_companies_by_natural_language(
        data.query,
        size=data.size,
        cursor=data.cursor,
        similarity_threshold=data.similarity_threshold,
    )
    normalized = Output.model_validate(result)
    return {
        "output": normalized.model_dump(),
        "summary": f"Found {len(normalized.companies)} companies for Harmonic query '{normalized.query}'.",
    }
