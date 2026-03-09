from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from agentic_tools_core.models import ToolDefinition
from agentic_tools_harmonic.bootstrap import tool
from agentic_tools_harmonic.runtime_clients import get_harmonic_client

SOURCE = "app/tools/harmonic/actions/get_team_network_connections_to_company.py"


class Input(BaseModel):
    company_id_or_urn: str = Field(min_length=1)
    size: int = Field(default=100, ge=1, le=1000)
    cursor: Optional[str] = None


class Output(BaseModel):
    company_id_or_urn: str
    count: int = 0
    connections: list[dict[str, Any]] = Field(default_factory=list)
    page_info: dict[str, Any] = Field(default_factory=dict)
    source_endpoint: str = ""


@tool(
    ToolDefinition(
        tool_id="harmonic.get_team_network_connections_to_company",
        display_name="Get Team Network Connections To Company",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.0.0",
        description="Fetch team network connections to a company from Harmonic.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["company_not_found", "upstream_unavailable"],
        examples=["company_id_or_urn='urn:harmonic:company:123'"],
        anti_patterns=["size > 1000"],
        integration="harmonic",
        is_write=False,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = Input.model_validate(payload)
    result = get_harmonic_client().get_team_network_connections_to_company(
        data.company_id_or_urn,
        size=data.size,
        cursor=data.cursor,
    )
    normalized = Output.model_validate(result)
    return {
        "output": normalized.model_dump(),
        "summary": (
            f"Loaded {len(normalized.connections)} team network connections for {normalized.company_id_or_urn} "
            f"via {normalized.source_endpoint}."
        ),
    }
