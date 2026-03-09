from __future__ import annotations

import os
from typing import Any, Optional

import httpx

from agentic_tools_core.integration_clients.exceptions import IntegrationConfigError
from agentic_tools_core.integration_clients.exceptions import IntegrationRequestError
from agentic_tools_core.integration_clients.http_client import IntegrationHttpClient


class HarmonicClient:
    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        self.base_url = base_url or os.getenv("HARMONIC_API_BASE_URL", "https://api.harmonic.ai")
        self.api_key = api_key or os.getenv("HARMONIC_API_KEY", "")
        auth_header = os.getenv("HARMONIC_AUTH_HEADER", "Authorization")
        auth_scheme = os.getenv("HARMONIC_AUTH_SCHEME", "Bearer")
        timeout_seconds = float(os.getenv("HARMONIC_API_TIMEOUT_SECONDS", "30"))

        self._similar_path = os.getenv("HARMONIC_ENDPOINT_SIMILAR_CANDIDATES", "/v1/people/similar")
        self._enrich_person_path = os.getenv("HARMONIC_ENDPOINT_ENRICH_PERSON", "/persons")
        self._enrich_company_path = os.getenv("HARMONIC_ENDPOINT_ENRICH_COMPANY", "/companies")
        self._people_saved_search_results_path = os.getenv(
            "HARMONIC_ENDPOINT_PEOPLE_SAVED_SEARCH_RESULTS",
            "/savedSearches:results/{id_or_urn}",
        )
        self._search_agent_path = os.getenv("HARMONIC_ENDPOINT_SEARCH_AGENT", "/search/search_agent")
        self._company_employees_path = os.getenv("HARMONIC_ENDPOINT_COMPANY_EMPLOYEES", "/companies/{id_or_urn}/employees")
        self._team_connections_company_path = os.getenv(
            "HARMONIC_ENDPOINT_TEAM_CONNECTIONS_COMPANY",
            "/companies/{id_or_urn}/userConnections",
        )
        self._team_connections_search_path = os.getenv(
            "HARMONIC_ENDPOINT_TEAM_CONNECTIONS_SEARCH",
            "/search/team_connections",
        )

        self.http = IntegrationHttpClient(
            name="harmonic",
            base_url=self.base_url,
            api_key=self.api_key,
            auth_header_name=auth_header,
            auth_scheme=auth_scheme,
            timeout_seconds=timeout_seconds,
            transport=transport,
        )

    def find_similar_profiles(self, seed_profiles: list[dict[str, Any]], per_seed: int = 10) -> list[dict[str, Any]]:
        payload = {
            "seed_profiles": seed_profiles,
            "per_seed": per_seed,
        }
        data = self.http.request("POST", self._similar_path, json_body=payload)

        raw_candidates: Any
        if isinstance(data, dict):
            raw_candidates = data.get("candidates") or data.get("results") or data.get("data") or []
            if isinstance(raw_candidates, dict):
                raw_candidates = raw_candidates.get("items") or raw_candidates.get("results") or []
        elif isinstance(data, list):
            raw_candidates = data
        else:
            raw_candidates = []

        out: list[dict[str, Any]] = []
        for item in raw_candidates:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "candidate_id": str(item.get("candidate_id") or item.get("id") or ""),
                    "name": item.get("name") or "",
                    "email": item.get("email") or "",
                    "linkedin": item.get("linkedin") or item.get("linkedin_url") or "",
                    "skills": item.get("skills") or [],
                    "raw": item,
                }
            )
        return out

    def enrich_person(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = self.http.request("POST", self._enrich_person_path, json_body=payload)
        return _normalize_enrichment_response(data, entity_type="person")

    def enrich_company(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = self.http.request("POST", self._enrich_company_path, json_body=payload)
        return _normalize_enrichment_response(data, entity_type="company")

    def get_people_saved_search_results_with_metadata(
        self,
        saved_search_id_or_urn: str,
        *,
        size: int = 100,
        cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        path = self._people_saved_search_results_path.format(id_or_urn=saved_search_id_or_urn)
        params: dict[str, Any] = {"size": size}
        if cursor:
            params["cursor"] = cursor
        data = self.http.request("GET", path, params=params)

        raw_results = _extract_results(data)
        candidates = [_normalize_person_result(item) for item in raw_results if isinstance(item, dict)]
        return {
            "saved_search_id_or_urn": saved_search_id_or_urn,
            "count": _extract_count(data, default=len(candidates)),
            "candidates": candidates,
            "page_info": _extract_page_info(data),
            "raw_metadata": _extract_raw_metadata(data),
        }

    def search_companies_by_natural_language(
        self,
        query: str,
        *,
        size: int = 25,
        cursor: Optional[str] = None,
        similarity_threshold: Optional[float] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "query": query,
            "size": size,
        }
        if cursor:
            params["cursor"] = cursor
        if similarity_threshold is not None:
            params["similarity_threshold"] = similarity_threshold

        data = self.http.request("GET", self._search_agent_path, params=params)
        raw_results = _extract_results(data)
        companies = [_normalize_company_result(item) for item in raw_results]

        query_interpretation: dict[str, Any] = {}
        if isinstance(data, dict):
            qi = data.get("query_interpretation")
            if isinstance(qi, dict):
                query_interpretation = qi

        return {
            "query": query,
            "count": _extract_count(data, default=len(companies)),
            "companies": companies,
            "page_info": _extract_page_info(data),
            "query_interpretation": query_interpretation,
        }

    def get_employees_by_company(
        self,
        company_id_or_urn: str,
        *,
        size: int = 100,
        cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        path = self._company_employees_path.format(id_or_urn=company_id_or_urn)
        params: dict[str, Any] = {"size": size}
        if cursor:
            params["cursor"] = cursor
        data = self.http.request("GET", path, params=params)
        raw_results = _extract_results(data)
        employees = [_normalize_person_result(item) for item in raw_results if isinstance(item, dict)]
        return {
            "company_id_or_urn": company_id_or_urn,
            "count": _extract_count(data, default=len(employees)),
            "employees": employees,
            "page_info": _extract_page_info(data),
        }

    def get_team_network_connections_to_company(
        self,
        company_id_or_urn: str,
        *,
        size: int = 100,
        cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"size": size}
        if cursor:
            params["cursor"] = cursor

        source_endpoint = self._team_connections_company_path
        try:
            path = self._team_connections_company_path.format(id_or_urn=company_id_or_urn)
            data = self.http.request("GET", path, params=params)
        except IntegrationRequestError:
            payload: dict[str, Any] = {
                "company_id_or_urn": company_id_or_urn,
                "size": size,
            }
            if cursor:
                payload["cursor"] = cursor
            data = self.http.request("POST", self._team_connections_search_path, json_body=payload)
            source_endpoint = self._team_connections_search_path

        raw_results = _extract_results(data)
        connections = [_normalize_connection_result(item) for item in raw_results]
        return {
            "company_id_or_urn": company_id_or_urn,
            "count": _extract_count(data, default=len(connections)),
            "connections": connections,
            "page_info": _extract_page_info(data),
            "source_endpoint": source_endpoint,
        }


class MockHarmonicClient:
    def find_similar_profiles(self, seed_profiles: list[dict[str, Any]], per_seed: int = 10) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for seed in seed_profiles:
            seed_id = seed.get("candidate_id", "unknown")
            for idx in range(1, per_seed + 1):
                out.append(
                    {
                        "candidate_id": f"sim_{seed_id}_{idx}",
                        "name": f"Similar {seed_id} #{idx}",
                        "email": f"sim_{seed_id}_{idx}@example.com",
                        "linkedin": f"https://linkedin.com/in/sim_{seed_id}_{idx}",
                        "skills": ["python", "systems"],
                    }
                )
        return out

    def enrich_person(self, payload: dict[str, Any]) -> dict[str, Any]:
        person_id = payload.get("person_urn") or payload.get("linkedin_url") or payload.get("email") or "unknown-person"
        return {
            "status": "QUEUED",
            "message": "Enrichment queued",
            "enrichment_urn": "urn:harmonic:enrichment:person-mock-1",
            "enriched_person_urn": f"urn:harmonic:person:{person_id}",
            "raw": payload,
        }

    def enrich_company(self, payload: dict[str, Any]) -> dict[str, Any]:
        company_ref = payload.get("company_urn") or payload.get("domain") or payload.get("name") or "unknown-company"
        return {
            "status": "QUEUED",
            "message": "Enrichment queued",
            "enrichment_urn": "urn:harmonic:enrichment:company-mock-1",
            "enriched_company_urn": f"urn:harmonic:company:{company_ref}",
            "raw": payload,
        }

    def get_people_saved_search_results_with_metadata(
        self,
        saved_search_id_or_urn: str,
        *,
        size: int = 100,
        cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        del cursor
        candidates = [
            {
                "candidate_id": f"urn:harmonic:person:{saved_search_id_or_urn}:1",
                "name": "Saved Search Candidate 1",
                "email": "saved1@example.com",
                "linkedin": "https://linkedin.com/in/saved1",
                "raw": {"entity_urn": f"urn:harmonic:person:{saved_search_id_or_urn}:1"},
            },
            {
                "candidate_id": f"urn:harmonic:person:{saved_search_id_or_urn}:2",
                "name": "Saved Search Candidate 2",
                "email": "saved2@example.com",
                "linkedin": "https://linkedin.com/in/saved2",
                "raw": {"entity_urn": f"urn:harmonic:person:{saved_search_id_or_urn}:2"},
            },
        ][: max(1, min(size, 2))]
        return {
            "saved_search_id_or_urn": saved_search_id_or_urn,
            "count": len(candidates),
            "candidates": candidates,
            "page_info": {"has_next": False, "next": None, "current": None},
            "raw_metadata": {"source": "mock"},
        }

    def search_companies_by_natural_language(
        self,
        query: str,
        *,
        size: int = 25,
        cursor: Optional[str] = None,
        similarity_threshold: Optional[float] = None,
    ) -> dict[str, Any]:
        del cursor, similarity_threshold
        companies = [
            {
                "company_id": "123456",
                "company_urn": "urn:harmonic:company:123456",
                "name": "Mock Company A",
                "raw": {"urn": "urn:harmonic:company:123456"},
            },
            {
                "company_id": "234567",
                "company_urn": "urn:harmonic:company:234567",
                "name": "Mock Company B",
                "raw": {"urn": "urn:harmonic:company:234567"},
            },
        ][: max(1, min(size, 2))]
        return {
            "query": query,
            "count": len(companies),
            "companies": companies,
            "page_info": {"has_next": False, "next": None, "current": None},
            "query_interpretation": {"semantic": query},
        }

    def get_employees_by_company(
        self,
        company_id_or_urn: str,
        *,
        size: int = 100,
        cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        del cursor
        employees = [
            {
                "candidate_id": f"urn:harmonic:person:{company_id_or_urn}:1",
                "name": "Employee 1",
                "email": "employee1@example.com",
                "linkedin": "https://linkedin.com/in/employee1",
                "raw": {"entity_urn": f"urn:harmonic:person:{company_id_or_urn}:1"},
            },
            {
                "candidate_id": f"urn:harmonic:person:{company_id_or_urn}:2",
                "name": "Employee 2",
                "email": "employee2@example.com",
                "linkedin": "https://linkedin.com/in/employee2",
                "raw": {"entity_urn": f"urn:harmonic:person:{company_id_or_urn}:2"},
            },
        ][: max(1, min(size, 2))]
        return {
            "company_id_or_urn": company_id_or_urn,
            "count": len(employees),
            "employees": employees,
            "page_info": {"has_next": False, "next": None, "current": None},
        }

    def get_team_network_connections_to_company(
        self,
        company_id_or_urn: str,
        *,
        size: int = 100,
        cursor: Optional[str] = None,
    ) -> dict[str, Any]:
        del cursor
        connections = [
            {
                "connection_id": "conn_1",
                "candidate_id": f"urn:harmonic:person:{company_id_or_urn}:1",
                "connector_name": "Alice Recruiter",
                "strength": "strong",
                "raw": {"id": "conn_1"},
            },
            {
                "connection_id": "conn_2",
                "candidate_id": f"urn:harmonic:person:{company_id_or_urn}:2",
                "connector_name": "Bob Hiring Manager",
                "strength": "medium",
                "raw": {"id": "conn_2"},
            },
        ][: max(1, min(size, 2))]
        return {
            "company_id_or_urn": company_id_or_urn,
            "count": len(connections),
            "connections": connections,
            "page_info": {"has_next": False, "next": None, "current": None},
            "source_endpoint": "/companies/{id_or_urn}/userConnections",
        }


def build_harmonic_client(mode: str):
    if mode == "mock":
        return MockHarmonicClient()
    if mode == "live":
        return HarmonicClient()
    raise IntegrationConfigError("AR_INTEGRATION_MODE must be one of: mock, live")


def _extract_results(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []

    results = data.get("results") or data.get("data") or data.get("items") or []
    if isinstance(results, list):
        return results
    if isinstance(results, dict):
        nested = results.get("results") or results.get("items") or []
        return nested if isinstance(nested, list) else []
    return []


def _extract_count(data: Any, *, default: int) -> int:
    if isinstance(data, dict):
        value = data.get("count")
        if isinstance(value, int):
            return value
    return default


def _extract_page_info(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {"has_next": False, "next": None, "current": None}

    page_info = data.get("page_info") or data.get("pageInfo")
    if isinstance(page_info, dict):
        return {
            "has_next": page_info.get("has_next") if "has_next" in page_info else page_info.get("hasNext"),
            "next": page_info.get("next"),
            "current": page_info.get("current"),
        }

    has_next = data.get("has_next") if "has_next" in data else data.get("hasNext")
    next_cursor = data.get("next") or data.get("next_cursor")
    current_cursor = data.get("current") or data.get("cursor")
    return {"has_next": bool(has_next), "next": next_cursor, "current": current_cursor}


def _extract_raw_metadata(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    keep_keys = {"query", "type", "name", "query_interpretation", "creator", "is_private", "entity_urn"}
    return {key: value for key, value in data.items() if key in keep_keys}


def _normalize_enrichment_response(data: Any, *, entity_type: str) -> dict[str, Any]:
    if isinstance(data, dict):
        status = str(data.get("status") or "")
        message = str(data.get("message") or "")
        enrichment_urn = str(data.get("entity_urn") or data.get("enrichment_urn") or "")
        enriched_entity_urn = str(data.get("enriched_entity_urn") or data.get("enrichedEntityUrn") or "")
    else:
        status = ""
        message = ""
        enrichment_urn = ""
        enriched_entity_urn = ""

    enriched_person_urn = enriched_entity_urn if entity_type == "person" else ""
    enriched_company_urn = enriched_entity_urn if entity_type == "company" else ""
    return {
        "status": status,
        "message": message,
        "enrichment_urn": enrichment_urn,
        "enriched_person_urn": enriched_person_urn,
        "enriched_company_urn": enriched_company_urn,
        "raw": data if isinstance(data, dict) else {"value": data},
    }


def _normalize_person_result(item: dict[str, Any]) -> dict[str, Any]:
    urn = str(item.get("entity_urn") or item.get("urn") or item.get("person_urn") or item.get("id") or "")
    profile = item.get("profile") if isinstance(item.get("profile"), dict) else item
    return {
        "candidate_id": str(profile.get("id") or urn),
        "person_urn": urn,
        "name": profile.get("full_name") or profile.get("name") or "",
        "email": _extract_email(profile),
        "linkedin": _extract_linkedin(profile),
        "raw": item,
    }


def _normalize_company_result(item: Any) -> dict[str, Any]:
    if isinstance(item, str):
        return {
            "company_id": "",
            "company_urn": item,
            "name": "",
            "raw": {"urn": item},
        }
    if not isinstance(item, dict):
        return {"company_id": "", "company_urn": "", "name": "", "raw": {"value": item}}

    company_urn = str(item.get("entity_urn") or item.get("urn") or item.get("company_urn") or "")
    company_id = str(item.get("id") or "")
    name = item.get("name") or item.get("text") or item.get("legal_name") or ""
    return {
        "company_id": company_id,
        "company_urn": company_urn,
        "name": name,
        "raw": item,
    }


def _normalize_connection_result(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        return {"connection_id": "", "candidate_id": "", "connector_name": "", "strength": "", "raw": {"value": item}}
    return {
        "connection_id": str(item.get("id") or item.get("connection_id") or ""),
        "candidate_id": str(item.get("candidate_id") or item.get("person_urn") or item.get("entity_urn") or ""),
        "connector_name": item.get("connector_name") or item.get("name") or "",
        "strength": item.get("strength") or item.get("score") or "",
        "raw": item,
    }


def _extract_email(item: dict[str, Any]) -> str:
    value = item.get("email") or item.get("primary_email")
    if isinstance(value, str):
        return value
    contact = item.get("contact")
    if isinstance(contact, dict):
        email = contact.get("email") or contact.get("primary_email")
        if isinstance(email, str):
            return email
    return ""


def _extract_linkedin(item: dict[str, Any]) -> str:
    direct = item.get("linkedin") or item.get("linkedin_url") or item.get("linkedin_profile_url")
    if isinstance(direct, str):
        return direct
    socials = item.get("socials")
    if isinstance(socials, dict):
        value = socials.get("linkedin")
        if isinstance(value, str):
            return value
    return ""
