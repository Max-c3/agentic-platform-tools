from __future__ import annotations

import json

import httpx

from agentic_tools_gem.client import GemClient
from agentic_tools_gem.client import MockGemClient


def test_list_projects_paginates_mock_projects() -> None:
    client = MockGemClient()
    client.create_project("Alpha")
    client.create_project("Beta")

    result = client.list_projects(page=1, page_size=1)

    assert len(result["projects"]) == 1
    assert result["pagination"]["total"] == 2
    assert result["pagination"]["page"] == 1
    assert result["pagination"]["next_page"] == 2


def test_find_projects_respects_max_pages_and_case_insensitive_matching() -> None:
    client = MockGemClient()
    client.create_project("Alpha")
    client.create_project("Backend One")
    client.create_project("Backend Two")

    result = client.find_projects(name_contains="BACKEND", sort="asc", page_size=1, max_pages=2)

    assert [item["name"] for item in result["matches"]] == ["Backend One"]
    assert result["scan"]["scanned_pages"] == 2
    assert result["scan"]["source_exhausted"] is False
    assert result["scan"]["stop_reason"] == "max_pages_reached"


def test_list_project_candidates_hydrates_candidate_payloads() -> None:
    client = MockGemClient()
    project = client.create_project("Backend Hiring")
    client.add_profiles_to_project(
        project_id=project["project_id"],
        profiles=[
            {"name": "Ada Lovelace", "email": "ada@example.com"},
            {"name": "Grace Hopper", "email": "grace@example.com"},
        ],
    )

    result = client.list_project_candidates(project_id=project["project_id"], include_candidates=True)

    assert result["project_id"] == project["project_id"]
    assert len(result["entries"]) == 2
    assert result["unresolved_candidate_ids"] == []
    assert result["entries"][0]["candidate"]["candidate_id"] == result["entries"][0]["candidate_id"]

    candidate_id = result["entries"][0]["candidate_id"]
    fetched = client.get_candidate(candidate_id)
    assert fetched["candidate_id"] == candidate_id
    assert fetched["candidate"]["candidate_id"] == candidate_id


def test_find_candidates_batches_candidate_ids_and_normalizes_linkedin_url() -> None:
    client = MockGemClient()
    candidate_ids = []
    for idx in range(21):
        created = client.create_candidate(
            {
                "first_name": f"User{idx}",
                "emails": [{"email_address": f"user{idx}@example.com", "is_primary": True}],
                "linked_in_handle": f"user-{idx}",
            }
        )
        candidate_ids.append(created["candidate_id"])

    result = client.find_candidates(
        candidate_ids=candidate_ids,
        linkedin_url="https://www.linkedin.com/in/user-0/?trk=public",
    )

    assert [item["candidate_id"] for item in result["matches"]] == [candidate_ids[0]]
    assert result["scan"]["scanned_batches"] == 2
    assert result["scan"]["linked_in_handle"] == "user-0"


def test_remove_candidates_from_project_reports_missing_candidates_and_logs_changes() -> None:
    client = MockGemClient()
    project = client.create_project("Ops Hiring")
    candidate = client.create_candidate({"first_name": "Ada"})
    client.add_profiles_to_project(project_id=project["project_id"], profiles=[{"candidate_id": candidate["candidate_id"]}])

    result = client.remove_candidates_from_project(
        project_id=project["project_id"],
        candidate_ids=[candidate["candidate_id"], "cand_missing"],
    )

    assert result["removed_candidate_ids"] == [candidate["candidate_id"]]
    assert result["already_missing_candidate_ids"] == ["cand_missing"]
    log = client.list_project_membership_log(project_id=project["project_id"])
    assert [entry["action"] for entry in log["entries"]] == ["removed", "added"]


def test_mock_schema_admin_and_project_field_value_flows() -> None:
    client = MockGemClient()
    custom_field = client.create_custom_field(
        name="Priority",
        value_type="single_select",
        scope="team",
        option_values=["High"],
    )
    added_options = client.add_custom_field_options(custom_field_id=custom_field["custom_field_id"], option_values=["Medium"])
    updated_option = client.update_custom_field_option(
        custom_field_id=custom_field["custom_field_id"],
        option_id=added_options["option_ids"][0],
        is_hidden=True,
    )

    project_field = client.create_project_field(name="Stage", field_type="single_select", options=["Sourced"])
    added_project_options = client.create_project_field_option(
        project_field_id=project_field["project_field_id"],
        options=["Interview"],
    )
    updated_project_option = client.update_project_field_option(
        project_field_id=project_field["project_field_id"],
        project_field_option_id=added_project_options["option_ids"][0],
        is_hidden=True,
    )

    project = client.create_project("Platform Hiring")
    field_value = client.set_project_field_value(
        project_id=project["project_id"],
        project_field_id=project_field["project_field_id"],
        operation="add",
        option_ids=added_project_options["option_ids"],
    )

    assert updated_option["option"]["is_hidden"] is True
    assert updated_project_option["option"]["is_hidden"] is True
    assert field_value["option_ids"] == added_project_options["option_ids"]
    listed_custom_fields = client.list_custom_fields()
    assert listed_custom_fields["custom_fields"][0]["custom_field_id"] == custom_field["custom_field_id"]
    listed_project_fields = client.list_project_fields()
    assert listed_project_fields["project_fields"][0]["project_field_id"] == project_field["project_field_id"]


def test_mock_candidate_notes_and_uploaded_resumes_roundtrip(tmp_path) -> None:
    client = MockGemClient()
    candidate = client.create_candidate({"first_name": "Ada"})
    client.add_candidate_note(candidate["candidate_id"], "Strong fit")
    resume_path = tmp_path / "resume.pdf"
    resume_path.write_bytes(b"%PDF-1.4 mock")
    client.upload_resume(candidate_id=candidate["candidate_id"], file_path=str(resume_path))

    notes = client.list_candidate_notes(candidate_id=candidate["candidate_id"])
    resumes = client.list_uploaded_resumes(candidate_id=candidate["candidate_id"])

    assert notes["notes"][0]["content"] == "Strong fit"
    assert resumes["resumes"][0]["filename"] == "resume.pdf"


def test_create_candidate_live_payload_uses_created_by_alias() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["json"] = json.loads(request.content.decode())
        return httpx.Response(
            201,
            json={"id": "cand_live_1", "created_by": "user_live_1", "first_name": "Ada"},
        )

    client = GemClient(api_key="test-key", transport=httpx.MockTransport(handler))
    result = client.create_candidate({"first_name": "Ada"}, user_id="user_live_1")

    assert captured["method"] == "POST"
    assert captured["path"] == "/v0/candidates"
    assert captured["json"] == {"first_name": "Ada", "created_by": "user_live_1"}
    assert result["candidate_id"] == "cand_live_1"
    assert result["user_id"] == "user_live_1"


def test_update_project_live_uses_patch_payload() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["json"] = json.loads(request.content.decode())
        return httpx.Response(
            200,
            json={"id": "proj_live_1", "name": "Renamed", "is_archived": True},
        )

    client = GemClient(api_key="test-key", transport=httpx.MockTransport(handler))
    result = client.update_project("proj_live_1", {"name": "Renamed", "is_archived": True})

    assert captured["method"] == "PATCH"
    assert captured["path"] == "/v0/projects/proj_live_1"
    assert captured["json"] == {"name": "Renamed", "is_archived": True}
    assert result["project"]["name"] == "Renamed"
    assert result["project"]["is_archived"] is True


def test_upload_resume_live_sends_multipart_resume_file(tmp_path) -> None:
    resume_path = tmp_path / "resume.pdf"
    resume_path.write_bytes(b"%PDF-1.4 live")
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["content_type"] = request.headers.get("content-type", "")
        captured["body"] = request.read()
        return httpx.Response(
            201,
            json={
                "id": "resume_live_1",
                "candidate_id": "cand_live_1",
                "filename": "resume.pdf",
                "download_url": "https://example.com/resume.pdf",
            },
        )

    client = GemClient(api_key="test-key", transport=httpx.MockTransport(handler))
    result = client.upload_resume(
        candidate_id="cand_live_1",
        file_path=str(resume_path),
        user_id="user_live_1",
    )

    assert captured["method"] == "POST"
    assert captured["path"] == "/v0/candidates/cand_live_1/uploaded_resumes/user_live_1"
    assert "multipart/form-data" in str(captured["content_type"])
    assert b'name="resume_file"' in captured["body"]
    assert b'filename="resume.pdf"' in captured["body"]
    assert result["uploaded_resume"]["filename"] == "resume.pdf"
