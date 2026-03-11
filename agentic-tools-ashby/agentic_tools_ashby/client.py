from __future__ import annotations

from datetime import datetime
import json
import os
import re
import time
from typing import Any, Optional

import httpx

from agentic_tools_core.integration_clients.exceptions import IntegrationConfigError
from agentic_tools_core.integration_clients.exceptions import IntegrationRequestError
from agentic_tools_core.integration_clients.http_client import IntegrationHttpClient

_ALLOWED_SORT_BY = {"hired_at", "created_at", "updated_at"}
_ALLOWED_SORT_ORDER = {"asc", "desc"}
_ALLOWED_RETRIEVAL_POLICY = {"strict_count", "fast_sample"}
_ALLOWED_SELECTION_MODE = {"global_latest_exact", "global_latest_best_effort", "fast_sample"}
_ALLOWED_REQUIRED_FIELDS = {
    "candidate_id",
    "name",
    "email",
    "linkedin",
    "job_title",
    "status",
    "hired_at",
    "created_at",
    "updated_at",
    "department_id",
    "location_id",
}


class AshbyClient:
    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        self.base_url = base_url or os.getenv("ASHBY_API_BASE_URL", "https://api.ashbyhq.com")
        self.api_key = api_key or os.getenv("ASHBY_API_KEY", "")
        auth_mode = os.getenv("ASHBY_AUTH_MODE", "basic")
        auth_header = os.getenv("ASHBY_AUTH_HEADER", "Authorization")
        auth_scheme = os.getenv("ASHBY_AUTH_SCHEME", "Bearer")
        timeout_seconds = float(os.getenv("ASHBY_API_TIMEOUT_SECONDS", "30"))

        self._candidate_endpoint = os.getenv("ASHBY_ENDPOINT_CANDIDATE_LIST", "/candidate.list")
        self._application_endpoint = os.getenv("ASHBY_ENDPOINT_APPLICATION_LIST", "/application.list")
        self._page_size = int(os.getenv("ASHBY_PAGE_SIZE", "100"))
        self._max_pages = int(os.getenv("ASHBY_MAX_PAGES", "20"))
        self._exact_max_pages = int(os.getenv("ASHBY_EXACT_MAX_PAGES", "1000"))
        self._history_page_size = int(os.getenv("ASHBY_APPLICATION_HISTORY_PAGE_SIZE", "100"))
        self._history_max_pages = int(os.getenv("ASHBY_APPLICATION_HISTORY_MAX_PAGES", "10"))
        self._request_retries = int(os.getenv("ASHBY_REQUEST_RETRIES", "3"))
        self._retry_backoff_seconds = float(os.getenv("ASHBY_RETRY_BACKOFF_SECONDS", "1.0"))
        self._server_filters = _load_server_filters()
        self._expand = _load_expand_fields()
        self._application_history_cache: dict[str, list[dict[str, Any]]] = {}
        accept_header = os.getenv("ASHBY_ACCEPT_HEADER", "application/json; version=1")

        self.http = IntegrationHttpClient(
            name="ashby",
            base_url=self.base_url,
            api_key=self.api_key,
            auth_mode=auth_mode,
            auth_header_name=auth_header,
            auth_scheme=auth_scheme,
            timeout_seconds=timeout_seconds,
            transport=transport,
            static_headers={"Accept": accept_header},
        )

    def get_recent_technical_hires(
        self,
        count: int = 10,
        role_context: str = "",
        keywords: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        effective_keywords = _resolve_technical_keywords(role_context=role_context, explicit_keywords=keywords)
        result = self.search_hires(
            count=count,
            selection_mode="global_latest_exact",
            sort_by="hired_at",
            sort_order="desc",
            filters={"status": ["hired"], "keywords": effective_keywords},
            retrieval_policy="strict_count",
            max_scan_pages=None,
            require_fields=["candidate_id", "name"],
        )
        return result["hires"]

    def get_recent_hires(
        self,
        count: int = 10,
        role_context: str = "",
        keywords: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        del role_context
        result = self.search_hires(
            count=count,
            selection_mode="global_latest_exact",
            sort_by="hired_at",
            sort_order="desc",
            filters={"status": ["hired"], "keywords": keywords or []},
            retrieval_policy="strict_count",
            max_scan_pages=None,
            require_fields=["candidate_id", "name"],
        )
        return result["hires"]

    def search_hires(
        self,
        *,
        count: int,
        selection_mode: str = "global_latest_best_effort",
        sort_by: str = "hired_at",
        sort_order: str = "desc",
        filters: Optional[dict[str, Any]] = None,
        retrieval_policy: str = "strict_count",
        max_scan_pages: Optional[int] = None,
        require_fields: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        normalized_selection_mode = _normalize_selection_mode(selection_mode)
        normalized_sort_by = _normalize_sort_by(sort_by)
        normalized_sort_order = _normalize_sort_order(sort_order)
        normalized_policy = _normalize_retrieval_policy(retrieval_policy)
        if normalized_selection_mode == "fast_sample":
            normalized_policy = "fast_sample"
        normalized_filters = _normalize_hire_filters(filters or {})
        required = _normalize_required_fields(require_fields)
        target_count = max(1, count)
        hard_max_pages = _resolve_max_scan_pages(
            configured_default=self._max_pages,
            exact_default=self._exact_max_pages,
            explicit_value=max_scan_pages,
            selection_mode=normalized_selection_mode,
            retrieval_policy=normalized_policy,
        )
        if normalized_policy == "fast_sample":
            hard_max_pages = min(hard_max_pages, 3)

        hires: list[dict[str, Any]] = []
        seen_identities: set[str] = set()
        seen_cursors: set[str] = set()
        cursor: Optional[str] = None
        scanned_pages = 0
        scanned_records = 0
        stop_reason = "max_scan_pages_reached"
        missing_required_count = 0
        source_exhausted = False
        should_resolve_hired_at = normalized_sort_by == "hired_at" or "hired_at" in required

        for _ in range(hard_max_pages):
            payload: dict[str, Any] = {"limit": self._page_size}
            if cursor:
                payload["cursor"] = cursor
            if self._server_filters:
                payload["filters"] = self._server_filters
            if self._expand:
                payload["expand"] = self._expand

            data = self._request_json(self._application_endpoint, payload, allow_filter_fallback=True)
            scanned_pages += 1
            for item in _extract_results(data):
                scanned_records += 1
                if not isinstance(item, dict):
                    continue
                if not _is_hired_application(item):
                    continue
                history_hired_at = ""
                if should_resolve_hired_at:
                    history_hired_at = self._lookup_hired_stage_timestamp(_as_text(item.get("id")))
                normalized = _normalize_application_candidate(item, history_hired_at=history_hired_at)
                if not _matches_hire_filters(normalized, normalized_filters):
                    continue
                identity = _identity_key(normalized)
                if not identity or identity in seen_identities:
                    continue
                if required and any(not _field_present(normalized, field) for field in required):
                    missing_required_count += 1
                    continue
                hires.append(normalized)
                seen_identities.add(identity)
                if normalized_selection_mode == "global_latest_best_effort" and normalized_policy == "strict_count" and len(hires) >= target_count:
                    stop_reason = "target_reached"
                    break

            if stop_reason == "target_reached":
                break
            if normalized_policy == "fast_sample" and hires:
                stop_reason = "fast_sample_budget_reached"
                break

            cursor = _extract_next_cursor(data)
            if not cursor:
                source_exhausted = True
                stop_reason = "source_exhausted"
                break
            if cursor in seen_cursors:
                stop_reason = "cursor_loop_detected"
                break
            seen_cursors.add(cursor)

        sorted_hires = _sort_hires(hires, sort_by=normalized_sort_by, sort_order=normalized_sort_order)
        final_hires = sorted_hires[:target_count]
        if normalized_selection_mode == "global_latest_best_effort" and len(final_hires) >= target_count:
            stop_reason = "target_reached"
        elif source_exhausted:
            stop_reason = "source_exhausted"
        elif normalized_selection_mode == "global_latest_exact":
            stop_reason = "max_scan_pages_reached"

        proof_flags = {
            "global_latest_proven": bool(normalized_selection_mode == "global_latest_exact" and source_exhausted),
            "source_exhausted": source_exhausted,
        }

        quality_flags = _quality_flags(
            requested_count=target_count,
            returned_count=len(final_hires),
            missing_required_count=missing_required_count,
            source_exhausted=source_exhausted,
            selection_mode=normalized_selection_mode,
            global_latest_proven=proof_flags["global_latest_proven"],
        )
        confidence = _estimate_confidence(
            requested_count=target_count,
            returned_count=len(final_hires),
            missing_required_count=missing_required_count,
            scanned_pages=scanned_pages,
            source_exhausted=source_exhausted,
        )

        return {
            "hires": final_hires,
            "diagnostics": {
                "requested_count": target_count,
                "returned_count": len(final_hires),
                "sort_by": normalized_sort_by,
                "sort_order": normalized_sort_order,
                "filters": normalized_filters,
                "selection_mode": normalized_selection_mode,
                "retrieval_policy": normalized_policy,
                "max_scan_pages": hard_max_pages,
                "scanned_pages": scanned_pages,
                "scanned_records": scanned_records,
                "stop_reason": stop_reason,
                "missing_required_count": missing_required_count,
                "quality_flags": quality_flags,
                "proof_flags": proof_flags,
                "guarantee": (
                    "global_latest_exact scans until source exhaustion or configured exact-mode safety cap. "
                    "global_latest_best_effort scans until requested count or source exhaustion."
                ),
            },
            "confidence": confidence,
        }

    def _lookup_hired_stage_timestamp(self, application_id: str) -> str:
        if not application_id:
            return ""
        cached = self._application_history_cache.get(application_id)
        if cached is None:
            cached = self._fetch_application_history(application_id)
            self._application_history_cache[application_id] = cached
        return _extract_hired_stage_timestamp(cached)

    def _fetch_application_history(self, application_id: str) -> list[dict[str, Any]]:
        history: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        for _ in range(self._history_max_pages):
            payload: dict[str, Any] = {
                "applicationId": application_id,
                "limit": self._history_page_size,
            }
            if cursor:
                payload["cursor"] = cursor
            try:
                data = self._request_json("/application.listHistory", payload, allow_filter_fallback=False)
            except IntegrationRequestError:
                return []
            page_items = _extract_results(data)
            history.extend(item for item in page_items if isinstance(item, dict))
            cursor = _extract_next_cursor(data)
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
        return history

    def _request_json(self, path: str, payload: dict[str, Any], *, allow_filter_fallback: bool) -> Any:
        request_payload = dict(payload)
        attempts = max(1, self._request_retries)
        last_error: IntegrationRequestError | None = None

        for attempt in range(attempts):
            try:
                return self.http.request("POST", path, json_body=request_payload)
            except IntegrationRequestError as exc:
                last_error = exc
                if allow_filter_fallback and "filters" in request_payload:
                    request_payload.pop("filters", None)
                    allow_filter_fallback = False
                    continue
            except httpx.HTTPError as exc:
                last_error = IntegrationRequestError(
                    f"ashby: POST {path} failed: {exc}",
                    method="POST",
                    url=f"{self.base_url}/{path.lstrip('/')}",
                )

            if attempt + 1 < attempts:
                time.sleep(self._retry_backoff_seconds * (attempt + 1))

        if last_error is not None:
            raise last_error
        raise IntegrationRequestError(f"ashby: POST {path} failed without a recoverable response.")

    def audit_hire_coverage(
        self,
        *,
        sample_size: int = 50,
        filters: Optional[dict[str, Any]] = None,
        require_fields: Optional[list[str]] = None,
        max_scan_pages: Optional[int] = None,
    ) -> dict[str, Any]:
        result = self.search_hires(
            count=max(1, sample_size),
            selection_mode="global_latest_exact",
            sort_by="hired_at",
            sort_order="desc",
            filters=filters or {"status": ["hired"]},
            retrieval_policy="strict_count",
            max_scan_pages=max_scan_pages,
            require_fields=require_fields or [],
        )
        hires = result["hires"]
        by_department: dict[str, int] = {}
        by_location: dict[str, int] = {}
        missing_email = 0
        missing_linkedin = 0
        for hire in hires:
            department = _as_text(hire.get("department_id")) or "unknown"
            location = _as_text(hire.get("location_id")) or "unknown"
            by_department[department] = by_department.get(department, 0) + 1
            by_location[location] = by_location.get(location, 0) + 1
            if not _as_text(hire.get("email")):
                missing_email += 1
            if not _as_text(hire.get("linkedin")):
                missing_linkedin += 1
        return {
            "diagnostics": result["diagnostics"],
            "coverage": {
                "sample_size": sample_size,
                "returned_count": len(hires),
                "missing_email_count": missing_email,
                "missing_linkedin_count": missing_linkedin,
                "by_department": by_department,
                "by_location": by_location,
            },
            "confidence": result["confidence"],
            "sample_hires": hires[: min(10, len(hires))],
        }


class MockAshbyClient:
    def get_recent_technical_hires(
        self,
        count: int = 10,
        role_context: str = "",
        keywords: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        result = self.search_hires(
            count=count,
            selection_mode="global_latest_exact",
            sort_by="hired_at",
            sort_order="desc",
            filters={"status": ["hired"], "keywords": keywords or _resolve_technical_keywords(role_context, None)},
            retrieval_policy="strict_count",
            max_scan_pages=5,
            require_fields=["candidate_id", "name"],
        )
        return result["hires"]

    def get_recent_hires(
        self,
        count: int = 10,
        role_context: str = "",
        keywords: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        del role_context
        result = self.search_hires(
            count=count,
            selection_mode="global_latest_exact",
            sort_by="hired_at",
            sort_order="desc",
            filters={"status": ["hired"], "keywords": keywords or []},
            retrieval_policy="strict_count",
            max_scan_pages=5,
            require_fields=["candidate_id", "name"],
        )
        return result["hires"]

    def search_hires(
        self,
        *,
        count: int,
        selection_mode: str = "global_latest_best_effort",
        sort_by: str = "hired_at",
        sort_order: str = "desc",
        filters: Optional[dict[str, Any]] = None,
        retrieval_policy: str = "strict_count",
        max_scan_pages: Optional[int] = None,
        require_fields: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        normalized_selection_mode = _normalize_selection_mode(selection_mode)
        normalized_sort_by = _normalize_sort_by(sort_by)
        normalized_sort_order = _normalize_sort_order(sort_order)
        normalized_policy = _normalize_retrieval_policy(retrieval_policy)
        if normalized_selection_mode == "fast_sample":
            normalized_policy = "fast_sample"
        normalized_filters = _normalize_hire_filters(filters or {})
        required = _normalize_required_fields(require_fields)
        roles = ["Research Engineer", "Recruiter", "People Ops", "MTS, Kernels", "Product Manager", "Designer"]
        base: list[dict[str, Any]] = []
        for i in range(1, 121):
            base.append(
                {
                    "candidate_id": f"h{i}",
                    "name": f"Hire {i}",
                    "email": f"hire{i}@example.com",
                    "linkedin": f"https://linkedin.com/in/hire{i}",
                    "skills": ["python", "systems"] if i % 2 == 0 else ["people", "operations"],
                    "job_title": roles[(i - 1) % len(roles)],
                    "status": "Hired",
                    "hired_at": f"2026-01-{(i % 28) + 1:02d}T00:00:00+00:00",
                    "created_at": f"2025-12-{(i % 28) + 1:02d}T00:00:00+00:00",
                    "updated_at": f"2026-01-{(i % 28) + 1:02d}T10:00:00+00:00",
                    "department_id": f"dept_{(i % 3) + 1}",
                    "location_id": f"loc_{(i % 4) + 1}",
                    "raw": {"mock": True},
                }
            )
        filtered: list[dict[str, Any]] = []
        missing_required_count = 0
        for item in base:
            if not _matches_hire_filters(item, normalized_filters):
                continue
            if required and any(not _field_present(item, field) for field in required):
                missing_required_count += 1
                continue
            filtered.append(item)
        hard_max_pages = max_scan_pages if isinstance(max_scan_pages, int) and max_scan_pages > 0 else 5
        page_size = 25
        total_pages = max(1, (len(filtered) + page_size - 1) // page_size)
        source_exhausted = hard_max_pages >= total_pages
        considered: list[dict[str, Any]]
        if source_exhausted:
            considered = filtered
        else:
            considered = filtered[: hard_max_pages * page_size]

        sorted_hires = _sort_hires(considered, sort_by=normalized_sort_by, sort_order=normalized_sort_order)
        target = max(1, count)
        if normalized_selection_mode == "fast_sample" or normalized_policy == "fast_sample":
            out = sorted_hires[: min(target, max(1, min(5, len(sorted_hires))))]
            stop_reason = "fast_sample_budget_reached"
        elif normalized_selection_mode == "global_latest_exact":
            out = sorted_hires[:target]
            stop_reason = "source_exhausted" if source_exhausted else "max_scan_pages_reached"
        else:
            out = sorted_hires[:target]
            stop_reason = "target_reached" if len(out) >= target else ("source_exhausted" if source_exhausted else "max_scan_pages_reached")
        proof_flags = {
            "global_latest_proven": bool(normalized_selection_mode == "global_latest_exact" and source_exhausted),
            "source_exhausted": source_exhausted,
        }
        return {
            "hires": out,
            "diagnostics": {
                "requested_count": target,
                "returned_count": len(out),
                "sort_by": normalized_sort_by,
                "sort_order": normalized_sort_order,
                "filters": normalized_filters,
                "selection_mode": normalized_selection_mode,
                "retrieval_policy": normalized_policy,
                "max_scan_pages": hard_max_pages,
                "scanned_pages": min(hard_max_pages, total_pages),
                "scanned_records": len(considered),
                "stop_reason": stop_reason,
                "missing_required_count": missing_required_count,
                "quality_flags": _quality_flags(
                    requested_count=target,
                    returned_count=len(out),
                    missing_required_count=missing_required_count,
                    source_exhausted=source_exhausted,
                    selection_mode=normalized_selection_mode,
                    global_latest_proven=proof_flags["global_latest_proven"],
                ),
                "proof_flags": proof_flags,
                "guarantee": (
                    "global_latest_exact scans until source exhaustion or page budget. "
                    "global_latest_best_effort scans until requested count or source exhaustion."
                ),
            },
            "confidence": _estimate_confidence(
                requested_count=target,
                returned_count=len(out),
                missing_required_count=missing_required_count,
                scanned_pages=min(hard_max_pages, total_pages),
                source_exhausted=source_exhausted,
            ),
        }

    def audit_hire_coverage(
        self,
        *,
        sample_size: int = 50,
        filters: Optional[dict[str, Any]] = None,
        require_fields: Optional[list[str]] = None,
        max_scan_pages: Optional[int] = None,
    ) -> dict[str, Any]:
        del max_scan_pages
        result = self.search_hires(
            count=sample_size,
            selection_mode="global_latest_exact",
            sort_by="hired_at",
            sort_order="desc",
            filters=filters or {"status": ["hired"]},
            retrieval_policy="strict_count",
            require_fields=require_fields or [],
        )
        hires = result["hires"]
        return {
            "diagnostics": result["diagnostics"],
            "coverage": {
                "sample_size": sample_size,
                "returned_count": len(hires),
                "missing_email_count": sum(1 for item in hires if not _as_text(item.get("email"))),
                "missing_linkedin_count": sum(1 for item in hires if not _as_text(item.get("linkedin"))),
                "by_department": {},
                "by_location": {},
            },
            "confidence": result["confidence"],
            "sample_hires": hires[: min(10, len(hires))],
        }


def build_ashby_client(mode: str):
    if mode == "mock":
        return MockAshbyClient()
    if mode == "live":
        return AshbyClient()
    raise IntegrationConfigError("AR_INTEGRATION_MODE must be one of: mock, live")


def _load_server_filters() -> dict[str, Any]:
    raw = os.getenv("ASHBY_RECENT_HIRES_SERVER_FILTERS_JSON", "{}")
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    return {}


def _load_expand_fields() -> list[str]:
    raw = os.getenv("ASHBY_EXPAND", "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _extract_results(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []

    results = data.get("results") or data.get("candidates") or data.get("data") or []
    if isinstance(results, list):
        return results
    if isinstance(results, dict):
        nested = results.get("results") or results.get("items") or results.get("candidates") or []
        return nested if isinstance(nested, list) else []
    return []


def _extract_next_cursor(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return None

    more_data = data.get("moreDataAvailable")
    if more_data is False:
        return None

    cursor = data.get("nextCursor") or data.get("next_cursor")
    if isinstance(cursor, str) and cursor:
        return cursor

    pagination = data.get("pagination")
    if isinstance(pagination, dict):
        nested = pagination.get("nextCursor") or pagination.get("next_cursor")
        if isinstance(nested, str) and nested:
            return nested

    return None


def _is_hired_candidate(candidate: dict[str, Any]) -> bool:
    # Prefer explicit hire indicators if present.
    if _truthy(candidate.get("isHired")) or _truthy(candidate.get("hired")):
        return True
    if _nonempty(candidate.get("hiredAt")) or _nonempty(candidate.get("hiredDate")):
        return True
    status = _normalize_text(candidate.get("status"))
    if status and "hired" in status:
        return True

    for app in _candidate_applications(candidate):
        if _truthy(app.get("isHired")) or _truthy(app.get("hired")):
            return True
        if _nonempty(app.get("hiredAt")) or _nonempty(app.get("hiredDate")):
            return True
        app_status = _normalize_text(
            app.get("status") or app.get("applicationStatus") or app.get("stage") or app.get("currentStage")
        )
        if app_status and "hired" in app_status:
            return True

    return False


def _is_technical_candidate(candidate: dict[str, Any], keywords: list[str]) -> bool:
    if not keywords:
        return True
    corpus = " ".join(_collect_text(candidate)).lower()
    return any(keyword in corpus for keyword in keywords)


def _normalize_candidate(item: dict[str, Any]) -> dict[str, Any]:
    first_name = _normalize_text(item.get("firstName") or item.get("first_name"))
    last_name = _normalize_text(item.get("lastName") or item.get("last_name"))
    derived_name = f"{first_name} {last_name}".strip()
    name = _normalize_text(item.get("name") or item.get("fullName") or item.get("full_name")) or derived_name

    skills = item.get("skills")
    if not isinstance(skills, list):
        skills = []

    return {
        "candidate_id": str(item.get("candidate_id") or item.get("candidateId") or item.get("id") or ""),
        "name": name,
        "email": _extract_email(item.get("primaryEmailAddress")) or _normalize_text(item.get("email")),
        "linkedin": _normalize_text(item.get("linkedinUrl") or item.get("linkedInUrl") or item.get("linkedin")),
        "skills": [str(skill) for skill in skills if isinstance(skill, str)],
        "raw": item,
    }


def _candidate_applications(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("applications", "applicationHistory", "jobs"):
        value = candidate.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _collect_text(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(_collect_text(item))
        return out
    if isinstance(value, dict):
        out: list[str] = []
        for nested in value.values():
            out.extend(_collect_text(nested))
        return out
    return []


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1"}
    return False


def _nonempty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _normalize_text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _normalize_application_candidate(application: dict[str, Any], *, history_hired_at: str = "") -> dict[str, Any]:
    candidate = application.get("candidate") if isinstance(application.get("candidate"), dict) else {}
    job = application.get("job") if isinstance(application.get("job"), dict) else {}
    stage = application.get("currentInterviewStage") if isinstance(application.get("currentInterviewStage"), dict) else {}
    raw = {"application": application, "candidate": candidate, "job": job}
    stage_title = _normalize_text(stage.get("title") or stage.get("name"))
    status = _normalize_text(application.get("status") or stage_title)
    hired_at = _first_nonempty_text(
        application.get("hiredAt"),
        application.get("hiredDate"),
        history_hired_at,
    )
    created_at = _first_nonempty_text(
        application.get("createdAt"),
        application.get("submittedAt"),
        application.get("updatedAt"),
    )
    updated_at = _first_nonempty_text(
        application.get("updatedAt"),
        application.get("lastActivityAt"),
        application.get("createdAt"),
    )

    return {
        "candidate_id": str(candidate.get("id") or application.get("candidateId") or ""),
        "name": _normalize_text(candidate.get("name")),
        "email": _extract_email(candidate.get("primaryEmailAddress")) or _normalize_text(candidate.get("email")),
        "linkedin": _extract_linkedin(candidate),
        "skills": _collect_candidate_skills(candidate),
        "job_title": _normalize_text(job.get("title")),
        "status": status,
        "hired_at": hired_at,
        "created_at": created_at,
        "updated_at": updated_at,
        "department_id": _normalize_text(job.get("departmentId")),
        "location_id": _normalize_text(job.get("locationId")),
        "raw": raw,
    }


def _is_hired_application(application: dict[str, Any]) -> bool:
    status = _normalize_text(application.get("status")).lower()
    if "hired" in status:
        return True
    stage = application.get("currentInterviewStage")
    if isinstance(stage, dict):
        stage_text = _normalize_text(stage.get("title") or stage.get("name")).lower()
        if "hired" in stage_text:
            return True
    return False


def _extract_email(value: Any) -> str:
    if isinstance(value, str):
        return _normalize_text(value)
    if isinstance(value, dict):
        return _normalize_text(value.get("value"))
    return ""


def _extract_linkedin(candidate: dict[str, Any]) -> str:
    direct = _normalize_text(candidate.get("linkedin") or candidate.get("linkedinUrl") or candidate.get("linkedInUrl"))
    if direct:
        return direct
    links = candidate.get("socialLinks")
    if isinstance(links, list):
        for item in links:
            if not isinstance(item, dict):
                continue
            value = _normalize_text(item.get("url") or item.get("value") or "")
            if "linkedin.com" in value:
                return value
    return ""


def _collect_candidate_skills(candidate: dict[str, Any]) -> list[str]:
    raw = candidate.get("skills")
    if isinstance(raw, list):
        return [str(item) for item in raw if isinstance(item, str)]
    return []


def _normalize_sort_by(value: str) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in _ALLOWED_SORT_BY else "hired_at"


def _normalize_sort_order(value: str) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in _ALLOWED_SORT_ORDER else "desc"


def _normalize_retrieval_policy(value: str) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in _ALLOWED_RETRIEVAL_POLICY else "strict_count"


def _normalize_selection_mode(value: str) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in _ALLOWED_SELECTION_MODE else "global_latest_best_effort"


def _normalize_required_fields(require_fields: Optional[list[str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for field in require_fields or []:
        normalized = _normalize_text(field)
        if not normalized or normalized in seen:
            continue
        if normalized in _ALLOWED_REQUIRED_FIELDS:
            seen.add(normalized)
            out.append(normalized)
    return out


def _normalize_hire_filters(filters: dict[str, Any]) -> dict[str, Any]:
    statuses = {_normalize_text(item).lower() for item in _as_list(filters.get("status")) if _normalize_text(item)}
    keywords = {_normalize_text(item).lower() for item in _as_list(filters.get("keywords")) if _normalize_text(item)}
    department_ids = {_normalize_text(item) for item in _as_list(filters.get("department_ids")) if _normalize_text(item)}
    location_ids = {_normalize_text(item) for item in _as_list(filters.get("location_ids")) if _normalize_text(item)}
    candidate_ids = {_normalize_text(item) for item in _as_list(filters.get("candidate_ids")) if _normalize_text(item)}
    technical_only = bool(filters.get("technical_only", False))
    return {
        "status": sorted(statuses),
        "keywords": sorted(keywords),
        "department_ids": sorted(department_ids),
        "location_ids": sorted(location_ids),
        "candidate_ids": sorted(candidate_ids),
        "technical_only": technical_only,
    }


def _matches_hire_filters(hire: dict[str, Any], filters: dict[str, Any]) -> bool:
    if not isinstance(hire, dict):
        return False
    status_filters = {item.lower() for item in _as_list(filters.get("status")) if _as_text(item)}
    if status_filters:
        status = _normalize_text(hire.get("status")).lower()
        if status not in status_filters:
            return False

    dept_filters = {item for item in _as_list(filters.get("department_ids")) if _as_text(item)}
    if dept_filters and _as_text(hire.get("department_id")) not in dept_filters:
        return False

    location_filters = {item for item in _as_list(filters.get("location_ids")) if _as_text(item)}
    if location_filters and _as_text(hire.get("location_id")) not in location_filters:
        return False

    candidate_filters = {item for item in _as_list(filters.get("candidate_ids")) if _as_text(item)}
    if candidate_filters and _as_text(hire.get("candidate_id")) not in candidate_filters:
        return False

    if bool(filters.get("technical_only", False)) and not _has_technical_signal(hire):
        return False

    keywords = [str(item).lower() for item in _as_list(filters.get("keywords")) if _as_text(item)]
    if keywords:
        corpus = " ".join(
            [
                _as_text(hire.get("name")),
                _as_text(hire.get("job_title")),
                _as_text(hire.get("email")),
                _as_text(hire.get("linkedin")),
                " ".join(_as_text(skill) for skill in _as_list(hire.get("skills"))),
            ]
        ).lower()
        if not any(keyword in corpus for keyword in keywords):
            return False

    return True


def _field_present(hire: dict[str, Any], field: str) -> bool:
    if not isinstance(hire, dict):
        return False
    value = hire.get(field)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return len(value) > 0
    return True


def _sort_hires(hires: list[dict[str, Any]], *, sort_by: str, sort_order: str) -> list[dict[str, Any]]:
    effective_sort_by = _normalize_sort_by(sort_by)
    effective_sort_order = _normalize_sort_order(sort_order)

    def key(item: dict[str, Any]) -> tuple[bool, float, str]:
        ts = _sort_timestamp(item, effective_sort_by)
        if effective_sort_order == "desc":
            return (ts is None, -(ts or 0.0), _as_text(item.get("candidate_id")))
        return (ts is None, ts or 0.0, _as_text(item.get("candidate_id")))

    return sorted(hires, key=key)


def _sort_timestamp(hire: dict[str, Any], sort_by: str) -> Optional[float]:
    if not isinstance(hire, dict):
        return None
    candidate_fields: list[str]
    if sort_by == "created_at":
        candidate_fields = ["created_at", "hired_at", "updated_at"]
    elif sort_by == "updated_at":
        candidate_fields = ["updated_at", "hired_at", "created_at"]
    else:
        candidate_fields = ["hired_at"]

    for field in candidate_fields:
        parsed = _parse_datetime(hire.get(field))
        if parsed is not None:
            return parsed.timestamp()
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = _as_text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _identity_key(item: dict[str, Any]) -> str:
    for field in ("candidate_id", "email", "linkedin", "name"):
        value = _as_text(item.get(field))
        if value:
            return value.lower()
    return ""


def _quality_flags(
    *,
    requested_count: int,
    returned_count: int,
    missing_required_count: int,
    source_exhausted: bool,
    selection_mode: str,
    global_latest_proven: bool,
) -> list[str]:
    flags: list[str] = []
    if returned_count == 0:
        flags.append("empty_result")
    if returned_count < requested_count and source_exhausted:
        flags.append("source_exhausted_before_target")
    if missing_required_count > 0:
        flags.append("required_fields_filtered")
    if selection_mode == "global_latest_exact" and not global_latest_proven:
        flags.append("global_latest_unproven")
    if not flags:
        flags.append("ok")
    return flags


def _estimate_confidence(
    *,
    requested_count: int,
    returned_count: int,
    missing_required_count: int,
    scanned_pages: int,
    source_exhausted: bool,
) -> float:
    confidence = 1.0
    if requested_count > 0 and returned_count < requested_count:
        shortfall_ratio = (requested_count - returned_count) / requested_count
        confidence -= min(0.6, shortfall_ratio * 0.6)
    if missing_required_count > 0:
        denominator = max(1, returned_count + missing_required_count)
        confidence -= min(0.2, (missing_required_count / denominator) * 0.2)
    if scanned_pages <= 0:
        confidence -= 0.2
    if returned_count < requested_count and not source_exhausted:
        confidence -= 0.1
    return round(max(0.0, min(1.0, confidence)), 3)


def _has_technical_signal(hire: dict[str, Any]) -> bool:
    corpus = " ".join(
        [
            _as_text(hire.get("job_title")),
            _as_text(hire.get("name")),
            " ".join(_as_text(skill) for skill in _as_list(hire.get("skills"))),
        ]
    ).lower()
    technical_terms = [
        "engineer",
        "engineering",
        "software",
        "platform",
        "infrastructure",
        "technical",
        "developer",
        "kernel",
        "ml",
        "machine learning",
        "ai",
        "data",
        "devops",
        "sre",
        "security",
    ]
    return any(term in corpus for term in technical_terms)


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    if isinstance(value, (int, float, bool)):
        return str(value)
    return ""


def _first_nonempty_text(*values: Any) -> str:
    for value in values:
        text = _as_text(value)
        if text:
            return text
    return ""


def _resolve_max_scan_pages(
    *,
    configured_default: int,
    exact_default: int,
    explicit_value: Optional[int],
    selection_mode: str,
    retrieval_policy: str,
) -> int:
    if isinstance(explicit_value, int) and explicit_value > 0:
        return explicit_value
    if selection_mode == "global_latest_exact" and retrieval_policy == "strict_count":
        return max(1, exact_default)
    return max(1, configured_default)


def _extract_hired_stage_timestamp(history: list[dict[str, Any]]) -> str:
    hired_stage_at = ""
    hired_stage_ts: float | None = None
    for item in history:
        if not isinstance(item, dict):
            continue
        title = _normalize_text(item.get("title")).lower()
        if title != "hired":
            continue
        entered_at = _as_text(item.get("enteredStageAt"))
        parsed = _parse_datetime(entered_at)
        if parsed is None:
            continue
        ts = parsed.timestamp()
        if hired_stage_ts is None or ts > hired_stage_ts:
            hired_stage_ts = ts
            hired_stage_at = entered_at
    return hired_stage_at


def _resolve_technical_keywords(role_context: str, explicit_keywords: Optional[list[str]]) -> list[str]:
    cleaned_explicit = [item.strip().lower() for item in (explicit_keywords or []) if isinstance(item, str) and item.strip()]
    if cleaned_explicit:
        return cleaned_explicit

    inferred: list[str] = []
    context = role_context.lower()

    if any(token in context for token in ("backend", "api", "distributed systems", "platform")):
        inferred.extend(["backend", "software", "platform", "infrastructure"])
    if any(token in context for token in ("frontend", "design system", "react", "ui")):
        inferred.extend(["frontend", "ui", "web", "software"])
    if any(token in context for token in ("machine learning", "ml", "ai", "llm", "data science")):
        inferred.extend(["machine learning", "ml", "ai", "data"])
    if any(token in context for token in ("data engineer", "analytics", "warehouse")):
        inferred.extend(["data", "analytics", "etl"])
    if any(token in context for token in ("devops", "sre", "reliability", "infrastructure")):
        inferred.extend(["sre", "devops", "infrastructure"])
    if any(token in context for token in ("security", "appsec", "infosec")):
        inferred.extend(["security", "application security"])

    base = [
        "engineering",
        "engineer",
        "software",
        "technical",
        "tech",
        "backend",
        "frontend",
        "platform",
        "infrastructure",
        "python",
        "java",
        "javascript",
        "typescript",
        "ai",
        "ml",
        "machine learning",
        "data",
        "devops",
        "sre",
        "security",
    ]
    merged = base + inferred
    deduped: list[str] = []
    seen: set[str] = set()
    for keyword in merged:
        normalized = re.sub(r"\s+", " ", keyword.strip().lower())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped
