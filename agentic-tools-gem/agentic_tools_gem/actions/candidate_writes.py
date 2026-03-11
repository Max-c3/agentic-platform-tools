from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Optional

from pydantic import BaseModel, Field, model_validator

from agentic_tools_core.models import ToolDefinition
from agentic_tools_gem.bootstrap import tool
from agentic_tools_gem.runtime_clients import get_gem_client

SOURCE = "app/tools/gem/actions/candidate_writes.py"
VERSION = "2.1.0"
ALLOWED_RESUME_EXTENSIONS = {".pdf", ".doc", ".docx"}
MAX_RESUME_BYTES = 10 * 1024 * 1024


class CandidateFields(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    nickname: Optional[str] = None
    emails: Optional[list[dict[str, Any]]] = None
    linked_in_handle: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    location: Optional[str] = None
    school: Optional[str] = None
    education_info: Optional[list[dict[str, Any]]] = None
    work_info: Optional[list[dict[str, Any]]] = None
    profile_urls: Optional[list[str]] = None
    phone_number: Optional[str] = None
    project_ids: Optional[list[str]] = None
    custom_fields: Optional[list[dict[str, Any]]] = None
    sourced_from: Optional[str] = None
    autofill: Optional[bool] = None


class CreateCandidateInput(CandidateFields):
    user_id: Optional[str] = None


class CreateCandidateOutput(BaseModel):
    candidate_id: str
    candidate: dict[str, Any] = Field(default_factory=dict)
    user_id: str = ""
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.create_candidate",
        display_name="Create Gem Candidate",
        source_path=SOURCE,
        function_name="run_create_candidate",
        owner="recruiting-platform",
        version=VERSION,
        description="Create a Gem candidate with the native candidate creation fields.",
        input_schema=CreateCandidateInput.model_json_schema(),
        output_schema=CreateCandidateOutput.model_json_schema(),
        side_effects="Creates a candidate in Gem.",
        approval_class="none",
        common_failures=["duplicate_candidate", "validation_error"],
        examples=["first_name='Ada', last_name='Lovelace'"],
        anti_patterns=["empty candidate payload"],
        integration="gem",
        is_write=True,
    ),
    input_model=CreateCandidateInput,
    output_model=CreateCandidateOutput,
)
def run_create_candidate(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = CreateCandidateInput.model_validate(payload)
    raw = data.model_dump(exclude_unset=True)
    user_id = str(raw.pop("user_id", "") or "")
    if preview:
        preview_candidate = dict(raw)
        preview_candidate["candidate_id"] = "preview_candidate"
        preview_output = CreateCandidateOutput(
            candidate_id="preview_candidate",
            candidate=preview_candidate,
            user_id=user_id,
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": "Will create a Gem candidate.",
            "compensation": {"type": "logical_revert", "reason": "archive or delete created candidate manually if needed"},
        }

    result = get_gem_client().create_candidate(raw, user_id=user_id or None)
    out = CreateCandidateOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Created Gem candidate {out.candidate_id}.",
    }


class UpdateCandidateInput(CandidateFields):
    candidate_id: str = Field(min_length=1)
    due_date: Optional[dict[str, Any]] = None


class UpdateCandidateOutput(BaseModel):
    candidate_id: str
    candidate: dict[str, Any] = Field(default_factory=dict)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.update_candidate",
        display_name="Update Gem Candidate",
        source_path=SOURCE,
        function_name="run_update_candidate",
        owner="recruiting-platform",
        version=VERSION,
        description="Update a Gem candidate using the native candidate update fields.",
        input_schema=UpdateCandidateInput.model_json_schema(),
        output_schema=UpdateCandidateOutput.model_json_schema(),
        side_effects="Updates a candidate in Gem.",
        approval_class="none",
        common_failures=["unknown_candidate", "validation_error"],
        examples=["candidate_id='cand_123', title='Principal Engineer'"],
        anti_patterns=["empty candidate_id"],
        integration="gem",
        is_write=True,
    ),
    input_model=UpdateCandidateInput,
    output_model=UpdateCandidateOutput,
)
def run_update_candidate(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = UpdateCandidateInput.model_validate(payload)
    raw = data.model_dump(exclude_unset=True)
    candidate_id = str(raw.pop("candidate_id"))
    if preview:
        preview_candidate = dict(raw)
        preview_candidate["candidate_id"] = candidate_id
        preview_output = UpdateCandidateOutput(
            candidate_id=candidate_id,
            candidate=preview_candidate,
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will update Gem candidate {candidate_id}.",
            "compensation": {"type": "logical_revert", "reason": "restore previous candidate values"},
        }

    result = get_gem_client().update_candidate(candidate_id, raw)
    out = UpdateCandidateOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Updated Gem candidate {out.candidate_id}.",
    }


class UploadResumeInput(BaseModel):
    candidate_id: str = Field(min_length=1)
    file_path: str = Field(min_length=1)
    user_id: Optional[str] = None

    @model_validator(mode="after")
    def validate_resume_file(self) -> "UploadResumeInput":
        _validate_resume_path(self.file_path)
        return self


class UploadResumeOutput(BaseModel):
    candidate_id: str
    user_id: str = ""
    uploaded_resume: dict[str, Any] = Field(default_factory=dict)
    provider_response: dict[str, Any] = Field(default_factory=dict)


@tool(
    ToolDefinition(
        tool_id="gem.upload_resume",
        display_name="Upload Gem Resume",
        source_path=SOURCE,
        function_name="run_upload_resume",
        owner="recruiting-platform",
        version=VERSION,
        description="Upload a resume file for a Gem candidate.",
        input_schema=UploadResumeInput.model_json_schema(),
        output_schema=UploadResumeOutput.model_json_schema(),
        side_effects="Uploads a resume file to Gem.",
        approval_class="none",
        common_failures=["resume_not_found", "invalid_resume_file", "unknown_candidate"],
        examples=["candidate_id='cand_123', file_path='/tmp/resume.pdf'"],
        anti_patterns=["missing local file"],
        integration="gem",
        is_write=True,
    ),
    input_model=UploadResumeInput,
    output_model=UploadResumeOutput,
)
def run_upload_resume(payload: dict[str, Any], preview: bool = False) -> dict[str, Any]:
    data = UploadResumeInput.model_validate(payload)
    resume_path = _validate_resume_path(data.file_path)
    if preview:
        preview_output = UploadResumeOutput(
            candidate_id=data.candidate_id,
            user_id=data.user_id or "",
            uploaded_resume={
                "candidate_id": data.candidate_id,
                "filename": resume_path.name,
                "download_url": "",
            },
            provider_response={"preview": True},
        )
        return {
            "output": preview_output.model_dump(),
            "summary": f"Will upload {resume_path.name} to Gem candidate {data.candidate_id}.",
            "compensation": {"type": "logical_revert", "reason": "hide or replace uploaded resume if needed"},
        }

    result = get_gem_client().upload_resume(
        candidate_id=data.candidate_id,
        file_path=str(resume_path),
        user_id=data.user_id,
    )
    out = UploadResumeOutput.model_validate(result)
    return {
        "output": out.model_dump(),
        "summary": f"Uploaded a resume for Gem candidate {out.candidate_id}.",
    }


def _validate_resume_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_file():
        raise ValueError(f"Resume file not found: {path}")
    if path.suffix.lower() not in ALLOWED_RESUME_EXTENSIONS:
        raise ValueError("Resume file must be .pdf, .doc, or .docx.")
    if path.stat().st_size > MAX_RESUME_BYTES:
        raise ValueError("Resume file must be 10MB or smaller.")
    return path
