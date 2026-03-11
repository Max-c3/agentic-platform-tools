from __future__ import annotations

import json

import httpx

from agentic_tools_ashby.client import AshbyClient


def test_search_hires_exact_mode_scans_past_default_cap_and_uses_hired_stage_dates(monkeypatch) -> None:
    monkeypatch.setenv("ASHBY_MAX_PAGES", "2")
    monkeypatch.setenv("ASHBY_EXACT_MAX_PAGES", "10")

    list_calls: list[dict[str, object]] = []
    history_calls: list[str] = []

    list_pages = {
        "": {
            "success": True,
            "results": [
                _application(
                    application_id="app-old",
                    candidate_id="cand-old",
                    candidate_name="Old Hire",
                    status="Hired",
                    created_at="2023-01-01T00:00:00Z",
                    updated_at="2027-01-01T00:00:00Z",
                    job_title="Generalist",
                ),
                _application(
                    application_id="app-open",
                    candidate_id="cand-open",
                    candidate_name="Open Candidate",
                    status="Interviewing",
                    created_at="2023-01-02T00:00:00Z",
                    updated_at="2023-01-03T00:00:00Z",
                    job_title="Generalist",
                ),
            ],
            "nextCursor": "page-2",
            "moreDataAvailable": True,
        },
        "page-2": {
            "success": True,
            "results": [
                _application(
                    application_id="app-mid",
                    candidate_id="cand-mid",
                    candidate_name="Mid Candidate",
                    status="Rejected",
                    created_at="2024-01-01T00:00:00Z",
                    updated_at="2024-01-02T00:00:00Z",
                    job_title="Generalist",
                )
            ],
            "nextCursor": "page-3",
            "moreDataAvailable": True,
        },
        "page-3": {
            "success": True,
            "results": [
                _application(
                    application_id="app-newest",
                    candidate_id="cand-newest",
                    candidate_name="Newest Hire",
                    status="Hired",
                    created_at="2025-12-01T00:00:00Z",
                    updated_at="2025-12-02T00:00:00Z",
                    job_title="Platform Engineer",
                ),
                _application(
                    application_id="app-recent",
                    candidate_id="cand-recent",
                    candidate_name="Recent Hire",
                    status="Hired",
                    created_at="2025-08-01T00:00:00Z",
                    updated_at="2025-08-02T00:00:00Z",
                    job_title="Product Designer",
                ),
            ],
            "moreDataAvailable": False,
        },
    }

    history_by_application = {
        "app-old": _history("2023-05-09T23:47:37.639Z"),
        "app-newest": _history("2026-02-19T22:56:24.307Z"),
        "app-recent": _history("2025-08-20T21:20:26.311Z"),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode() or "{}")
        if request.url.path == "/application.list":
            cursor = str(body.get("cursor") or "")
            list_calls.append(body)
            return httpx.Response(200, json=list_pages[cursor])
        if request.url.path == "/application.listHistory":
            application_id = str(body["applicationId"])
            history_calls.append(application_id)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "results": history_by_application[application_id],
                    "moreDataAvailable": False,
                },
            )
        raise AssertionError(f"unexpected request path: {request.url.path}")

    client = AshbyClient(
        base_url="https://ashby.example",
        api_key="test-key",
        transport=httpx.MockTransport(handler),
    )

    result = client.search_hires(
        count=2,
        selection_mode="global_latest_exact",
        sort_by="hired_at",
        sort_order="desc",
        filters={"status": ["hired"]},
        retrieval_policy="strict_count",
        max_scan_pages=None,
        require_fields=["candidate_id", "name"],
    )

    assert [item["name"] for item in result["hires"]] == ["Newest Hire", "Recent Hire"]
    assert [item["hired_at"] for item in result["hires"]] == [
        "2026-02-19T22:56:24.307Z",
        "2025-08-20T21:20:26.311Z",
    ]
    assert result["diagnostics"]["scanned_pages"] == 3
    assert result["diagnostics"]["stop_reason"] == "source_exhausted"
    assert len(list_calls) == 3
    assert history_calls == ["app-old", "app-newest", "app-recent"]


def test_search_hires_retries_transient_timeouts(monkeypatch) -> None:
    monkeypatch.setenv("ASHBY_REQUEST_RETRIES", "2")

    call_count = {"application_list": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode() or "{}")
        if request.url.path == "/application.list":
            call_count["application_list"] += 1
            if call_count["application_list"] == 1:
                raise httpx.ReadTimeout("timed out", request=request)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "results": [
                        _application(
                            application_id="app-timeout",
                            candidate_id="cand-timeout",
                            candidate_name="Recovered Hire",
                            status="Hired",
                            created_at="2024-01-01T00:00:00Z",
                            updated_at="2024-01-02T00:00:00Z",
                            job_title="Software Engineer",
                        )
                    ],
                    "moreDataAvailable": False,
                },
            )
        if request.url.path == "/application.listHistory":
            assert body["applicationId"] == "app-timeout"
            return httpx.Response(
                200,
                json={"success": True, "results": _history("2024-02-01T00:00:00Z"), "moreDataAvailable": False},
            )
        raise AssertionError(f"unexpected request path: {request.url.path}")

    client = AshbyClient(
        base_url="https://ashby.example",
        api_key="test-key",
        transport=httpx.MockTransport(handler),
    )

    result = client.search_hires(
        count=1,
        selection_mode="global_latest_exact",
        sort_by="hired_at",
        sort_order="desc",
        filters={"status": ["hired"]},
        retrieval_policy="strict_count",
        max_scan_pages=1,
        require_fields=["candidate_id", "name"],
    )

    assert call_count["application_list"] == 2
    assert result["hires"][0]["name"] == "Recovered Hire"
    assert result["hires"][0]["hired_at"] == "2024-02-01T00:00:00Z"


def _application(
    *,
    application_id: str,
    candidate_id: str,
    candidate_name: str,
    status: str,
    created_at: str,
    updated_at: str,
    job_title: str,
) -> dict[str, object]:
    return {
        "id": application_id,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "status": status,
        "candidate": {
            "id": candidate_id,
            "name": candidate_name,
            "primaryEmailAddress": {"value": f"{candidate_id}@example.com"},
        },
        "currentInterviewStage": {"title": status, "type": status},
        "job": {
            "id": f"job-{application_id}",
            "title": job_title,
            "locationId": "loc-1",
            "departmentId": "dept-1",
        },
    }


def _history(hired_at: str) -> list[dict[str, object]]:
    return [
        {
            "id": "stage-review",
            "title": "Application Review",
            "enteredStageAt": "2023-01-01T00:00:00Z",
            "leftStageAt": "2023-01-02T00:00:00Z",
        },
        {
            "id": "stage-hired",
            "title": "Hired",
            "enteredStageAt": hired_at,
        },
    ]
