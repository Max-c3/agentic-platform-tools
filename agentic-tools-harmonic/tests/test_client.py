from __future__ import annotations

import httpx

from agentic_tools_harmonic.client import HarmonicClient


def test_get_employees_by_company_hydrates_person_urn_results(monkeypatch) -> None:
    monkeypatch.setenv("HARMONIC_PERSON_BATCH_SIZE", "2")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/companies/urn:harmonic:company:123/employees":
            return httpx.Response(
                200,
                json={
                    "count": 2,
                    "results": [
                        "urn:harmonic:person:1",
                        "urn:harmonic:person:2",
                    ],
                },
            )

        if request.url.path == "/persons":
            assert request.url.params.get_list("urns") == [
                "urn:harmonic:person:1",
                "urn:harmonic:person:2",
            ]
            return httpx.Response(
                200,
                json=[
                    {
                        "entity_urn": "urn:harmonic:person:2",
                        "id": 2,
                        "full_name": "Employee Two",
                        "contact": {"email": "two@example.com"},
                        "linkedin_url": "https://linkedin.com/in/two",
                        "headline": "Designer",
                    },
                    {
                        "entity_urn": "urn:harmonic:person:1",
                        "id": 1,
                        "full_name": "Employee One",
                        "contact": {"email": "one@example.com"},
                        "linkedin_url": "https://linkedin.com/in/one",
                        "headline": "Engineer",
                    },
                ],
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = HarmonicClient(api_key="test-key", transport=httpx.MockTransport(handler))

    result = client.get_employees_by_company("urn:harmonic:company:123", size=2)

    assert result["count"] == 2
    assert [item["person_urn"] for item in result["employees"]] == [
        "urn:harmonic:person:1",
        "urn:harmonic:person:2",
    ]
    assert [item["name"] for item in result["employees"]] == ["Employee One", "Employee Two"]
    assert result["employees"][0]["raw"]["headline"] == "Engineer"
    assert result["employees"][1]["raw"]["headline"] == "Designer"


def test_get_employees_by_company_falls_back_to_person_reference_when_lookup_misses(monkeypatch) -> None:
    monkeypatch.setenv("HARMONIC_PERSON_BATCH_SIZE", "2")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/companies/urn:harmonic:company:123/employees":
            return httpx.Response(
                200,
                json={
                    "count": 2,
                    "results": [
                        "urn:harmonic:person:1",
                        "urn:harmonic:person:2",
                    ],
                },
            )

        if request.url.path == "/persons":
            return httpx.Response(
                200,
                json=[
                    {
                        "entity_urn": "urn:harmonic:person:1",
                        "id": 1,
                        "full_name": "Employee One",
                    }
                ],
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = HarmonicClient(api_key="test-key", transport=httpx.MockTransport(handler))

    result = client.get_employees_by_company("urn:harmonic:company:123", size=2)

    assert [item["person_urn"] for item in result["employees"]] == [
        "urn:harmonic:person:1",
        "urn:harmonic:person:2",
    ]
    assert result["employees"][1]["candidate_id"] == "urn:harmonic:person:2"
    assert result["employees"][1]["raw"] == {"entity_urn": "urn:harmonic:person:2"}


def test_get_employees_by_company_batches_person_lookups(monkeypatch) -> None:
    monkeypatch.setenv("HARMONIC_PERSON_BATCH_SIZE", "2")
    requested_batches: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/companies/urn:harmonic:company:123/employees":
            return httpx.Response(
                200,
                json={
                    "count": 3,
                    "results": [
                        "urn:harmonic:person:1",
                        "urn:harmonic:person:2",
                        "urn:harmonic:person:3",
                    ],
                },
            )

        if request.url.path == "/persons":
            batch = request.url.params.get_list("urns")
            requested_batches.append(batch)
            return httpx.Response(
                200,
                json=[
                    {
                        "entity_urn": urn,
                        "id": urn.rsplit(":", 1)[-1],
                        "full_name": f"Employee {urn.rsplit(':', 1)[-1]}",
                    }
                    for urn in batch
                ],
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = HarmonicClient(api_key="test-key", transport=httpx.MockTransport(handler))

    result = client.get_employees_by_company("urn:harmonic:company:123", size=3)

    assert requested_batches == [
        ["urn:harmonic:person:1", "urn:harmonic:person:2"],
        ["urn:harmonic:person:3"],
    ]
    assert [item["person_urn"] for item in result["employees"]] == [
        "urn:harmonic:person:1",
        "urn:harmonic:person:2",
        "urn:harmonic:person:3",
    ]


def test_get_employees_by_company_uses_provider_safe_default_batch_size(monkeypatch) -> None:
    monkeypatch.delenv("HARMONIC_PERSON_BATCH_SIZE", raising=False)
    requested_batches: list[list[str]] = []
    person_urns = [f"urn:harmonic:person:{idx}" for idx in range(1, 76)]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/companies/urn:harmonic:company:123/employees":
            return httpx.Response(
                200,
                json={
                    "count": 75,
                    "results": person_urns,
                },
            )

        if request.url.path == "/persons":
            batch = request.url.params.get_list("urns")
            requested_batches.append(batch)
            return httpx.Response(
                200,
                json=[
                    {
                        "entity_urn": urn,
                        "id": urn.rsplit(":", 1)[-1],
                        "full_name": f"Employee {urn.rsplit(':', 1)[-1]}",
                    }
                    for urn in batch
                ],
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = HarmonicClient(api_key="test-key", transport=httpx.MockTransport(handler))

    result = client.get_employees_by_company("urn:harmonic:company:123", size=75)

    assert [len(batch) for batch in requested_batches] == [50, 25]
    assert len(result["employees"]) == 75


def test_get_employees_by_company_uses_cursor_as_numeric_offset_token(monkeypatch) -> None:
    monkeypatch.setenv("HARMONIC_PERSON_BATCH_SIZE", "2")
    seen_pages: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/companies/urn:harmonic:company:123/employees":
            seen_pages.append(request.url.params.get("page"))
            if request.url.params.get("page") == "2":
                return httpx.Response(
                    200,
                    json={
                        "count": 5,
                        "results": [
                            "urn:harmonic:person:3",
                            "urn:harmonic:person:4",
                        ],
                    },
                )
            return httpx.Response(200, json={"count": 5, "results": []})

        if request.url.path == "/persons":
            batch = request.url.params.get_list("urns")
            return httpx.Response(
                200,
                json=[
                    {
                        "entity_urn": urn,
                        "id": urn.rsplit(":", 1)[-1],
                        "full_name": f"Employee {urn.rsplit(':', 1)[-1]}",
                    }
                    for urn in batch
                ],
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = HarmonicClient(api_key="test-key", transport=httpx.MockTransport(handler))

    result = client.get_employees_by_company("urn:harmonic:company:123", size=2, cursor="2")

    assert seen_pages == ["2"]
    assert result["page_info"] == {"has_next": True, "next": "4", "current": "2"}
    assert [item["person_urn"] for item in result["employees"]] == [
        "urn:harmonic:person:3",
        "urn:harmonic:person:4",
    ]


def test_get_employees_by_company_synthesizes_page_info_when_provider_omits_it(monkeypatch) -> None:
    monkeypatch.setenv("HARMONIC_PERSON_BATCH_SIZE", "2")
    person_urns = [f"urn:harmonic:person:{idx}" for idx in range(1, 11)]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/companies/urn:harmonic:company:123/employees":
            return httpx.Response(
                200,
                json={
                    "count": 12,
                    "page_info": None,
                    "results": person_urns,
                },
            )

        if request.url.path == "/persons":
            batch = request.url.params.get_list("urns")
            return httpx.Response(
                200,
                json=[
                    {
                        "entity_urn": urn,
                        "id": urn.rsplit(":", 1)[-1],
                        "full_name": f"Employee {urn.rsplit(':', 1)[-1]}",
                    }
                    for urn in batch
                ],
            )

        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = HarmonicClient(api_key="test-key", transport=httpx.MockTransport(handler))

    result = client.get_employees_by_company("urn:harmonic:company:123", size=10)

    assert result["page_info"] == {"has_next": True, "next": "10", "current": "0"}
