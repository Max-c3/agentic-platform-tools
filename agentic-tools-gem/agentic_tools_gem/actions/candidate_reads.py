from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field, model_validator

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/candidate_reads.py"
VERSION = "2.1.0"


class ListCandidatesInput(BaseModel):
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    created_by: Optional[str] = None
    email: Optional[str] = None
    linked_in_handle: Optional[str] = None
    updated_after: Optional[int] = Field(default=None, ge=1)
    updated_before: Optional[int] = Field(default=None, ge=1)
    candidate_ids: list[str] = Field(default_factory=list)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListCandidatesOutput(BaseModel):
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_candidates",
        display_name="List Gem Candidates",
        source_path=SOURCE,
        function_name="run_list_candidates",
        owner="recruiting-platform",
        version=VERSION,
        description="List Gem candidates with API-backed filters and pagination.",
        input_schema=ListCandidatesInput.model_json_schema(),
        output_schema=ListCandidatesOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["invalid_pagination", "upstream_unavailable"],
        examples=["email='ada@example.com'", "candidate_ids=['cand_123']"],
        anti_patterns=["page_size > 100"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListCandidatesInput,
    output_model=ListCandidatesOutput,
)
def run_list_candidates(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListCandidatesInput.model_validate(payload)
    result = get_gem_client().list_candidates(
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        created_by=data.created_by,
        email=data.email,
        linked_in_handle=data.linked_in_handle,
        updated_after=data.updated_after,
        updated_before=data.updated_before,
        candidate_ids=data.candidate_ids,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListCandidatesOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.candidates)} Gem candidates on page {out.pagination.get('page', data.page)}.",
    }


class FindCandidatesInput(BaseModel):
    email: Optional[str] = None
    linked_in_handle: Optional[str] = None
    linkedin_url: Optional[str] = None
    candidate_ids: list[str] = Field(default_factory=list)
    created_by: Optional[str] = None
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    updated_after: Optional[int] = Field(default=None, ge=1)
    updated_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    max_pages: int = Field(default=5, ge=1, le=50)
    page_size: int = Field(default=100, ge=1, le=100)

    @model_validator(mode="after")
    def validate_identity_filters(self) -> "FindCandidatesInput":
        if not (
            (self.email and self.email.strip())
            or (self.linked_in_handle and self.linked_in_handle.strip())
            or (self.linkedin_url and self.linkedin_url.strip())
            or self.candidate_ids
        ):
            raise ValueError("Provide email, linked_in_handle/linkedin_url, or candidate_ids.")
        return self


class FindCandidatesOutput(BaseModel):
    matches: list[dict[str, Any]] = Field(default_factory=list)
    scan: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.find_candidates",
        display_name="Find Gem Candidates",
        source_path=SOURCE,
        function_name="run_find_candidates",
        owner="recruiting-platform",
        version=VERSION,
        description="Find Gem candidates by identity filters with bounded scanning metadata.",
        input_schema=FindCandidatesInput.model_json_schema(),
        output_schema=FindCandidatesOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["missing_identity_filter", "upstream_unavailable"],
        examples=["email='ada@example.com'", "linkedin_url='https://linkedin.com/in/ada'"],
        anti_patterns=["missing identity filters"],
        integration="gem",
        is_write=False,
    ),
    input_model=FindCandidatesInput,
    output_model=FindCandidatesOutput,
)
def run_find_candidates(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = FindCandidatesInput.model_validate(payload)
    result = get_gem_client().find_candidates(
        email=data.email or "",
        linked_in_handle=data.linked_in_handle or "",
        linkedin_url=data.linkedin_url or "",
        candidate_ids=data.candidate_ids,
        created_by=data.created_by,
        created_after=data.created_after,
        created_before=data.created_before,
        updated_after=data.updated_after,
        updated_before=data.updated_before,
        sort=data.sort,
        max_pages=data.max_pages,
        page_size=data.page_size,
    )
    out = FindCandidatesOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Found {len(out.matches)} Gem candidate matches.",
    }


class ListCandidateNotesInput(BaseModel):
    candidate_id: str = Field(min_length=1)
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListCandidateNotesOutput(BaseModel):
    candidate_id: str
    notes: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_candidate_notes",
        display_name="List Gem Candidate Notes",
        source_path=SOURCE,
        function_name="run_list_candidate_notes",
        owner="recruiting-platform",
        version=VERSION,
        description="List notes that belong to a Gem candidate.",
        input_schema=ListCandidateNotesInput.model_json_schema(),
        output_schema=ListCandidateNotesOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_candidate", "invalid_pagination", "upstream_unavailable"],
        examples=["candidate_id='cand_123'"],
        anti_patterns=["empty candidate_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListCandidateNotesInput,
    output_model=ListCandidateNotesOutput,
)
def run_list_candidate_notes(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListCandidateNotesInput.model_validate(payload)
    result = get_gem_client().list_candidate_notes(
        candidate_id=data.candidate_id,
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListCandidateNotesOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.notes)} notes for Gem candidate {out.candidate_id}.",
    }


class ListUploadedResumesInput(BaseModel):
    candidate_id: str = Field(min_length=1)
    created_after: Optional[int] = Field(default=None, ge=1)
    created_before: Optional[int] = Field(default=None, ge=1)
    sort: Optional[Literal["asc", "desc"]] = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ListUploadedResumesOutput(BaseModel):
    candidate_id: str
    resumes: list[dict[str, Any]] = Field(default_factory=list)
    pagination: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.list_uploaded_resumes",
        display_name="List Gem Uploaded Resumes",
        source_path=SOURCE,
        function_name="run_list_uploaded_resumes",
        owner="recruiting-platform",
        version=VERSION,
        description="List uploaded resumes for a Gem candidate.",
        input_schema=ListUploadedResumesInput.model_json_schema(),
        output_schema=ListUploadedResumesOutput.model_json_schema(),
        side_effects="None",
        approval_class="none",
        common_failures=["unknown_candidate", "invalid_pagination", "upstream_unavailable"],
        examples=["candidate_id='cand_123'"],
        anti_patterns=["empty candidate_id"],
        integration="gem",
        is_write=False,
    ),
    input_model=ListUploadedResumesInput,
    output_model=ListUploadedResumesOutput,
)
def run_list_uploaded_resumes(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    del preview
    data = ListUploadedResumesInput.model_validate(payload)
    result = get_gem_client().list_uploaded_resumes(
        candidate_id=data.candidate_id,
        created_after=data.created_after,
        created_before=data.created_before,
        sort=data.sort,
        page=data.page,
        page_size=data.page_size,
    )
    out = ListUploadedResumesOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Returned {len(out.resumes)} uploaded resumes for Gem candidate {out.candidate_id}.",
    }
