from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from agentic_tools_core.models import ToolDefinition
from agentic_tools_harmonic.bootstrap import tool
from agentic_tools_harmonic.runtime_clients import get_harmonic_client

SOURCE = "app/tools/harmonic/actions/enrich_company.py"


class Input(BaseModel):
    company_urn: Optional[str] = None
    domain: Optional[str] = None
    name: Optional[str] = None
    website_url: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_identifier(self) -> "Input":
        if any([self.company_urn, self.domain, self.name, self.website_url]):
            return self
        if self.payload:
            return self
        raise ValueError("Provide at least one company identifier or payload")


class Output(BaseModel):
    status: str = ""
    message: str = ""
    enrichment_urn: str = ""
    enriched_company_urn: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="harmonic.enrich_company",
        display_name="Enrich Company",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.0.0",
        description="Trigger Harmonic company enrichment from identity hints.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="Triggers enrichment process in Harmonic.",
        approval_class="none",
        common_failures=["upstream_unavailable", "invalid_identifier"],
        examples=["domain='example.com'"],
        anti_patterns=["empty payload and no identifiers"],
        integration="harmonic",
        is_write=True,
    ),
    input_model=Input,
    output_model=Output,
)
def run(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = Input.model_validate(payload)
    request_payload = dict(data.payload)
    if data.company_urn:
        request_payload["company_urn"] = data.company_urn
    if data.domain:
        request_payload["domain"] = data.domain
    if data.name:
        request_payload["name"] = data.name
    if data.website_url:
        request_payload["website_url"] = data.website_url

    if preview:
        preview_output = Output(
            status="preview",
            message="Company enrichment will be triggered on approval.",
            enrichment_urn="preview:company-enrichment",
            enriched_company_urn=request_payload.get("company_urn", ""),
            raw={"preview": True, "request_payload": request_payload},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": "Will trigger Harmonic company enrichment.",
            "compensation": {"type": "logical_revert", "reason": "no remote compensation available"},
        }

    response = get_harmonic_client().enrich_company(request_payload)
    out = Output.model_validate(response)
    return {
        "output": out.model_dump(),
        "summary": f"Triggered company enrichment ({out.status}) for {out.enriched_company_urn or 'requested company'}.",
    }
