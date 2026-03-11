from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from agentic_tools_core.models import ToolDefinition
from agentic_tools_harmonic.bootstrap import tool
from agentic_tools_harmonic.runtime_clients import get_harmonic_client

SOURCE = "app/tools/harmonic/actions/enrich_person.py"


class Input(BaseModel):
    person_urn: Optional[str] = None
    linkedin_url: Optional[str] = None
    email: Optional[str] = None
    full_name: Optional[str] = None
    company_name: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_identifier(self) -> "Input":
        if any([self.person_urn, self.linkedin_url, self.email, self.full_name, self.company_name]):
            return self
        if self.payload:
            return self
        raise ValueError("Provide at least one person identifier or payload")


class Output(BaseModel):
    status: str = ""
    message: str = ""
    enrichment_urn: str = ""
    enriched_person_urn: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="harmonic.enrich_person",
        display_name="Enrich Person",
        source_path=SOURCE,
        function_name="run",
        owner="recruiting-platform",
        version="1.0.0",
        description="Trigger Harmonic person enrichment from identity hints.",
        input_schema=Input.model_json_schema(),
        output_schema=Output.model_json_schema(),
        side_effects="Triggers enrichment process in Harmonic.",
        approval_class="none",
        common_failures=["upstream_unavailable", "invalid_identifier"],
        examples=["linkedin_url='https://linkedin.com/in/example'"],
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
    if data.person_urn:
        request_payload["person_urn"] = data.person_urn
    if data.linkedin_url:
        request_payload["linkedin_url"] = data.linkedin_url
    if data.email:
        request_payload["email"] = data.email
    if data.full_name:
        request_payload["full_name"] = data.full_name
    if data.company_name:
        request_payload["company_name"] = data.company_name

    if preview:
        preview_output = Output(
            status="preview",
            message="Person enrichment will be triggered on approval.",
            enrichment_urn="preview:person-enrichment",
            enriched_person_urn=request_payload.get("person_urn", ""),
            raw={"preview": True, "request_payload": request_payload},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": "Will trigger Harmonic person enrichment.",
            "compensation": {"type": "logical_revert", "reason": "no remote compensation available"},
        }

    response = get_harmonic_client().enrich_person(request_payload)
    out = Output.model_validate(response)
    return {
        "output": out.model_dump(),
        "summary": f"Triggered person enrichment ({out.status}) for {out.enriched_person_urn or 'requested person'}.",
    }
