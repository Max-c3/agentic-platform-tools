from __future__ import annotations

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
