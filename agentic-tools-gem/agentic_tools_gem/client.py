from __future__ import annotations

import json
import mimetypes
import os
import re
import uuid
import base64
from pathlib import Path
from typing import Any, Optional

import httpx

from agentic_tools_core.integration_clients.exceptions import IntegrationConfigError
from agentic_tools_core.integration_clients.exceptions import IntegrationRequestError
from agentic_tools_core.integration_clients.http_client import IntegrationHttpClient


def _as_dict(payload: Any) -> dict[str, Any]:
    return payload if isinstance(payload, dict) else {"value": payload}


def _extract_id(payload: dict[str, Any]) -> str:
    return str(payload.get("id") or payload.get("candidate_id") or payload.get("project_id") or "").strip()


def _normalize_object(payload: dict[str, Any], alias: str) -> dict[str, Any]:
    item = dict(payload)
    item_id = str(item.get(alias) or item.get("id") or "").strip()
    if item_id:
        item[alias] = item_id
    return item


def _normalize_project(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "project_id")


def _normalize_candidate(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "candidate_id")


def _normalize_user(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "user_id")


def _normalize_note(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "note_id")


def _normalize_custom_field(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "custom_field_id")


def _normalize_custom_field_option(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "option_id")


def _normalize_project_field(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "project_field_id")


def _normalize_project_field_option(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "project_field_option_id")


def _normalize_sequence(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "sequence_id")


def _normalize_uploaded_resume(payload: dict[str, Any]) -> dict[str, Any]:
    return _normalize_object(payload, "uploaded_resume_id")


def _normalize_name(profile: dict[str, Any]) -> tuple[str, str]:
    first_name = str(profile.get("first_name") or "").strip()
    last_name = str(profile.get("last_name") or "").strip()
    if first_name or last_name:
        return first_name, last_name
    full_name = str(profile.get("name") or "").strip()
    if not full_name:
        return "", ""
    parts = full_name.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _extract_email(profile: dict[str, Any]) -> str:
    email = str(profile.get("email") or "").strip().lower()
    return email


def _extract_linkedin_url(profile: dict[str, Any]) -> str:
    return str(profile.get("linkedin") or profile.get("linkedin_url") or "").strip()


def _extract_linkedin_handle(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        # Handles common Linkedin profile/company URL styles and strips query params.
        match = re.search(r"linkedin\.com/(?:in|pub|company)/([^/?#]+)", value, flags=re.IGNORECASE)
        return (match.group(1).strip() if match else "").strip()
    value = value.replace("@", "").strip("/")
    return value


def _candidate_email_values(candidate: dict[str, Any]) -> list[str]:
    emails = candidate.get("emails", [])
    out: list[str] = []
    if isinstance(emails, list):
        for item in emails:
            if not isinstance(item, dict):
                continue
            value = str(item.get("email_address") or "").strip().lower()
            if value:
                out.append(value)
    single = str(candidate.get("email") or "").strip().lower()
    if single:
        out.append(single)
    return out


def _candidate_linkedin_handles(candidate: dict[str, Any]) -> list[str]:
    out: list[str] = []
    handle = str(candidate.get("linked_in_handle") or "").strip()
    if handle:
        out.append(handle)
    profiles = candidate.get("profiles", [])
    if isinstance(profiles, list):
        for item in profiles:
            if not isinstance(item, dict):
                continue
            username = str(item.get("username") or "").strip()
            if username:
                out.append(username)
            url = str(item.get("url") or "").strip()
            parsed = _extract_linkedin_handle(url)
            if parsed:
                out.append(parsed)
    return [item for item in dict.fromkeys(out)]


def _unique_non_empty(items: list[str]) -> list[str]:
    return [item for item in dict.fromkeys(str(value).strip() for value in items) if item]


def _dedupe_objects(items: list[dict[str, Any]], id_key: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        item_id = str(item.get(id_key) or item.get("id") or "").strip()
        if item_id and item_id in seen:
            continue
        if item_id:
            seen.add(item_id)
        out.append(item)
    return out


def _matches_project_name(project: dict[str, Any], *, name_exact: str, name_contains: str) -> bool:
    project_name = str(project.get("name") or "").strip().casefold()
    if name_exact and project_name != name_exact.casefold():
        return False
    if name_contains and name_contains.casefold() not in project_name:
        return False
    return True


def _matches_candidate_identity(
    candidate: dict[str, Any],
    *,
    email: str,
    linked_in_handle: str,
    candidate_ids: set[str],
) -> bool:
    candidate_id = str(candidate.get("candidate_id") or candidate.get("id") or "").strip()
    if candidate_ids and candidate_id not in candidate_ids:
        return False
    if email and email not in _candidate_email_values(candidate):
        return False
    if linked_in_handle and linked_in_handle not in _candidate_linkedin_handles(candidate):
        return False
    return True


def _extract_duplicate_candidate_id(exc: IntegrationRequestError) -> str:
    if not isinstance(exc.response_json, dict):
        return ""
    errors = exc.response_json.get("errors")
    if isinstance(errors, dict):
        direct = errors.get("duplicate_candidate")
        if isinstance(direct, dict):
            return str(direct.get("id") or "").strip()
        nested = errors.get("json")
        if isinstance(nested, dict):
            dup = nested.get("duplicate_candidate")
            if isinstance(dup, dict):
                return str(dup.get("id") or "").strip()
    return ""


def _extract_project_membership_conflicts(exc: IntegrationRequestError) -> list[str]:
    if not isinstance(exc.response_json, dict):
        return []
    errors = exc.response_json.get("errors")
    if not isinstance(errors, dict):
        return []
    nested = errors.get("json")
    if not isinstance(nested, dict):
        return []
    raw = nested.get("candidate_ids")
    if not isinstance(raw, list):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]


def _parse_pagination_header(raw: str, *, page: int, page_size: int, returned_count: int) -> dict[str, Any]:
    pagination: dict[str, Any] = {
        "page": page,
        "page_size": page_size,
        "returned_count": returned_count,
    }
    if not raw.strip():
        return pagination
    try:
        parsed = json.loads(raw)
    except Exception:
        pagination["raw"] = raw
        return pagination
    if isinstance(parsed, dict):
        pagination.update(parsed)
    else:
        pagination["raw"] = raw
    pagination["page"] = int(pagination.get("page") or page)
    pagination["page_size"] = int(pagination.get("page_size") or page_size)
    pagination["returned_count"] = returned_count
    return pagination


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


class GemClient:
    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        self.base_url = base_url or os.getenv("GEM_API_BASE_URL", "https://api.gem.com")
        self.api_key = api_key or os.getenv("GEM_API_KEY", "")
        auth_header = os.getenv("GEM_AUTH_HEADER", "X-API-Key")
        auth_scheme = os.getenv("GEM_AUTH_SCHEME", "")
        timeout_seconds = float(os.getenv("GEM_API_TIMEOUT_SECONDS", "30"))

        self._users_path = os.getenv("GEM_ENDPOINT_USERS", "/v0/users")
        self._candidates_path = os.getenv("GEM_ENDPOINT_CANDIDATES", "/v0/candidates")
        self._projects_path = os.getenv(
            "GEM_ENDPOINT_PROJECTS",
            os.getenv("GEM_ENDPOINT_CREATE_PROJECT", "/v0/projects"),
        )
        self._project_path = os.getenv("GEM_ENDPOINT_PROJECT", "/v0/projects/{project_id}")
        self._project_candidates_path = os.getenv(
            "GEM_ENDPOINT_PROJECT_CANDIDATES",
            os.getenv(
                "GEM_ENDPOINT_ADD_PROFILES_TO_PROJECT",
                "/v0/projects/{project_id}/candidates",
            ),
        )
        self._create_project_path = self._projects_path
        self._add_profiles_to_project_path = self._project_candidates_path
        self._add_note_path = os.getenv("GEM_ENDPOINT_ADD_NOTE", "/v0/notes")
        self._candidate_path = os.getenv("GEM_ENDPOINT_CANDIDATE", "/v0/candidates/{candidate_id}")
        self._candidate_notes_path = os.getenv(
            "GEM_ENDPOINT_CANDIDATE_NOTES",
            "/v0/candidates/{candidate_id}/notes",
        )
        self._candidate_uploaded_resumes_path = os.getenv(
            "GEM_ENDPOINT_CANDIDATE_UPLOADED_RESUMES",
            "/v0/candidates/{candidate_id}/uploaded_resumes",
        )
        self._candidate_upload_resume_path = os.getenv(
            "GEM_ENDPOINT_CANDIDATE_UPLOAD_RESUME",
            "/v0/candidates/{candidate_id}/uploaded_resumes/{user_id}",
        )
        self._set_custom_value_path = os.getenv("GEM_ENDPOINT_SET_CUSTOM_VALUE", self._candidate_path)
        self._custom_fields_path = os.getenv("GEM_ENDPOINT_CUSTOM_FIELDS", "/v0/custom_fields")
        self._custom_field_options_path = os.getenv(
            "GEM_ENDPOINT_CUSTOM_FIELD_OPTIONS",
            "/v0/custom_fields/{custom_field_id}/options",
        )
        self._custom_field_option_path = os.getenv(
            "GEM_ENDPOINT_CUSTOM_FIELD_OPTION",
            "/v0/custom_fields/{custom_field_id}/options/{option_id}",
        )
        self._project_fields_path = os.getenv("GEM_ENDPOINT_PROJECT_FIELDS", "/v0/project_fields")
        self._project_field_options_path = os.getenv(
            "GEM_ENDPOINT_PROJECT_FIELD_OPTIONS",
            "/v0/project_fields/{project_field_id}/options",
        )
        self._project_field_option_path = os.getenv(
            "GEM_ENDPOINT_PROJECT_FIELD_OPTION",
            "/v0/project_fields/{project_field_id}/options/{project_field_option_id}",
        )
        self._project_field_values_path = os.getenv(
            "GEM_ENDPOINT_PROJECT_FIELD_VALUES",
            "/v0/projects/{project_id}/project_field_options",
        )
        self._sequences_path = os.getenv("GEM_ENDPOINT_SEQUENCES", "/v0/sequences")
        self._sequence_path = os.getenv("GEM_ENDPOINT_SEQUENCE", "/v0/sequences/{sequence_id}")
        self._project_membership_log_path = os.getenv(
            "GEM_ENDPOINT_PROJECT_MEMBERSHIP_LOG",
            "/v0/project_candidate_membership_log",
        )

        app_secret = os.getenv("GEM_APPLICATION_SECRET", "").strip()
        static_headers: dict[str, str] = {}
        if app_secret:
            static_headers["X-Application-Secret"] = app_secret

        self._default_user_id: str = ""
        self.http = IntegrationHttpClient(
            name="gem",
            base_url=self.base_url,
            api_key=self.api_key,
            auth_header_name=auth_header,
            auth_scheme=auth_scheme,
            timeout_seconds=timeout_seconds,
            transport=transport,
            static_headers=static_headers,
        )

    def resolve_user_id(self, user_id: Optional[str] = None) -> str:
        if user_id and user_id.strip():
            return user_id.strip()
        if self._default_user_id:
            return self._default_user_id

        configured = os.getenv("GEM_DEFAULT_USER_ID", "").strip()
        if configured:
            self._default_user_id = configured
            return configured

        email = os.getenv("GEM_DEFAULT_USER_EMAIL", "").strip()
        params: dict[str, Any] = {"page_size": 20}
        if email:
            params["email"] = email
        users = self.http.request("GET", self._users_path, params=params)
        if not isinstance(users, list) or not users:
            raise IntegrationConfigError(
                "gem: could not resolve default user id. Set GEM_DEFAULT_USER_ID or GEM_DEFAULT_USER_EMAIL."
            )
        resolved = _extract_id(users[0]) or str(users[0].get("id") or "").strip()
        if not resolved:
            raise IntegrationConfigError("gem: could not parse user id from /users response")
        self._default_user_id = resolved
        return resolved

    def list_users(
        self,
        *,
        email: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if email:
            params["email"] = email
        data, pagination = self._request_with_pagination("GET", self._users_path, params=params)
        users = [_normalize_user(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "users": users,
            "pagination": pagination,
        }

    def list_projects(
        self,
        *,
        owner_user_id: Optional[str] = None,
        readable_by_user_id: Optional[str] = None,
        writable_by_user_id: Optional[str] = None,
        is_archived: Optional[bool] = None,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if owner_user_id:
            params["user_id"] = owner_user_id
        if readable_by_user_id:
            params["readable_by"] = readable_by_user_id
        if writable_by_user_id:
            params["writable_by"] = writable_by_user_id
        if is_archived is not None:
            params["is_archived"] = is_archived
        if created_after is not None:
            params["created_after"] = created_after
        if created_before is not None:
            params["created_before"] = created_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort

        data, pagination = self._request_with_pagination("GET", self._projects_path, params=params)
        projects = [_normalize_project(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "projects": projects,
            "pagination": pagination,
        }

    def find_projects(
        self,
        *,
        name_exact: str = "",
        name_contains: str = "",
        owner_user_id: Optional[str] = None,
        readable_by_user_id: Optional[str] = None,
        writable_by_user_id: Optional[str] = None,
        is_archived: Optional[bool] = None,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        max_pages: int = 5,
        page_size: int = 100,
    ) -> dict[str, Any]:
        matches: list[dict[str, Any]] = []
        scanned_pages = 0
        scanned_projects = 0
        source_exhausted = False
        next_page = 1
        stop_reason = "max_pages_reached"

        while next_page and scanned_pages < max_pages:
            result = self.list_projects(
                owner_user_id=owner_user_id,
                readable_by_user_id=readable_by_user_id,
                writable_by_user_id=writable_by_user_id,
                is_archived=is_archived,
                created_after=created_after,
                created_before=created_before,
                sort=sort,
                page=next_page,
                page_size=page_size,
            )
            scanned_pages += 1
            projects = result["projects"]
            scanned_projects += len(projects)
            matches.extend(
                item
                for item in projects
                if _matches_project_name(
                    item,
                    name_exact=name_exact.strip(),
                    name_contains=name_contains.strip(),
                )
            )
            next_page = result["pagination"].get("next_page")
            if not next_page:
                source_exhausted = True
                stop_reason = "source_exhausted"
                break

        deduped_matches = _dedupe_objects(matches, "project_id")
        return {
            "matches": deduped_matches,
            "scan": {
                "name_exact": name_exact.strip(),
                "name_contains": name_contains.strip(),
                "filters": {
                    "owner_user_id": owner_user_id or "",
                    "readable_by_user_id": readable_by_user_id or "",
                    "writable_by_user_id": writable_by_user_id or "",
                    "is_archived": is_archived,
                    "created_after": created_after,
                    "created_before": created_before,
                    "sort": sort or "",
                },
                "max_pages": max_pages,
                "page_size": page_size,
                "scanned_pages": scanned_pages,
                "scanned_projects": scanned_projects,
                "returned_count": len(deduped_matches),
                "source_exhausted": source_exhausted,
                "stop_reason": stop_reason,
            },
        }

    def get_project(self, project_id: str) -> dict[str, Any]:
        path = self._project_path.format(project_id=project_id)
        data = self.http.request("GET", path)
        body = _as_dict(data)
        project = _normalize_project(body)
        resolved_project_id = _extract_id(project) or project_id
        return {
            "project_id": resolved_project_id,
            "project": project,
        }

    def list_project_candidates(
        self,
        *,
        project_id: str,
        page: int = 1,
        page_size: int = 20,
        added_after: Optional[int] = None,
        added_before: Optional[int] = None,
        sort: Optional[str] = None,
        include_candidates: bool = True,
    ) -> dict[str, Any]:
        project_result = self.get_project(project_id)
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if added_after is not None:
            params["added_after"] = added_after
        if added_before is not None:
            params["added_before"] = added_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort

        path = self._project_candidates_path.format(project_id=project_id)
        data, pagination = self._request_with_pagination("GET", path, params=params)
        memberships = [dict(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        candidate_ids = [str(item.get("candidate_id") or "").strip() for item in memberships]
        candidate_ids = [item for item in dict.fromkeys(candidate_ids) if item]
        candidate_map = self._list_candidates_by_ids(candidate_ids) if include_candidates and candidate_ids else {}

        entries: list[dict[str, Any]] = []
        for membership in memberships:
            candidate_id = str(membership.get("candidate_id") or "").strip()
            entry = {
                "candidate_id": candidate_id,
                "added_at": membership.get("added_at"),
                "candidate": candidate_map.get(candidate_id, {}) if include_candidates else {},
            }
            entries.append(entry)

        unresolved_candidate_ids = [item for item in candidate_ids if item not in candidate_map] if include_candidates else []
        return {
            "project_id": project_result["project_id"],
            "project": project_result["project"],
            "entries": entries,
            "pagination": pagination,
            "unresolved_candidate_ids": unresolved_candidate_ids,
        }

    def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        path = self._candidate_path.format(candidate_id=candidate_id)
        data = self.http.request("GET", path)
        body = _as_dict(data)
        candidate = _normalize_candidate(body)
        resolved_candidate_id = _extract_id(candidate) or candidate_id
        return {
            "candidate_id": resolved_candidate_id,
            "candidate": candidate,
        }

    def list_candidates(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        created_by: Optional[str] = None,
        email: Optional[str] = None,
        linked_in_handle: Optional[str] = None,
        updated_after: Optional[int] = None,
        updated_before: Optional[int] = None,
        candidate_ids: Optional[list[str]] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if created_after is not None:
            params["created_after"] = created_after
        if created_before is not None:
            params["created_before"] = created_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort
        if created_by:
            params["created_by"] = created_by
        if email:
            params["email"] = email
        if linked_in_handle:
            params["linked_in_handle"] = linked_in_handle
        if updated_after is not None:
            params["updated_after"] = updated_after
        if updated_before is not None:
            params["updated_before"] = updated_before
        if candidate_ids:
            params["candidate_ids"] = _unique_non_empty(candidate_ids)

        data, pagination = self._request_with_pagination("GET", self._candidates_path, params=params)
        candidates = [_normalize_candidate(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "candidates": candidates,
            "pagination": pagination,
        }

    def find_candidates(
        self,
        *,
        email: str = "",
        linked_in_handle: str = "",
        linkedin_url: str = "",
        candidate_ids: Optional[list[str]] = None,
        created_by: Optional[str] = None,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        updated_after: Optional[int] = None,
        updated_before: Optional[int] = None,
        sort: Optional[str] = None,
        max_pages: int = 5,
        page_size: int = 100,
    ) -> dict[str, Any]:
        normalized_email = email.strip().lower()
        normalized_handle = _extract_linkedin_handle(linked_in_handle or linkedin_url)
        normalized_candidate_ids = _unique_non_empty(candidate_ids or [])
        candidate_id_set = set(normalized_candidate_ids)
        if not (normalized_email or normalized_handle or normalized_candidate_ids):
            raise IntegrationConfigError("gem: find_candidates requires email, linked_in_handle/linkedin_url, or candidate_ids")

        matches: list[dict[str, Any]] = []
        scanned_pages = 0
        scanned_batches = 0
        scanned_candidates = 0

        if normalized_candidate_ids:
            for chunk in _chunked(normalized_candidate_ids, 20):
                scanned_batches += 1
                result = self.list_candidates(
                    created_after=created_after,
                    created_before=created_before,
                    sort=sort,
                    created_by=created_by,
                    email=normalized_email or None,
                    linked_in_handle=normalized_handle or None,
                    updated_after=updated_after,
                    updated_before=updated_before,
                    candidate_ids=chunk,
                    page=1,
                    page_size=max(1, min(len(chunk), 100)),
                )
                candidates = result["candidates"]
                scanned_candidates += len(candidates)
                matches.extend(
                    item
                    for item in candidates
                    if _matches_candidate_identity(
                        item,
                        email=normalized_email,
                        linked_in_handle=normalized_handle,
                        candidate_ids=candidate_id_set,
                    )
                )
            source_exhausted = True
            stop_reason = "all_candidate_ids_scanned"
        else:
            source_exhausted = False
            stop_reason = "max_pages_reached"
            next_page = 1
            while next_page and scanned_pages < max_pages:
                result = self.list_candidates(
                    created_after=created_after,
                    created_before=created_before,
                    sort=sort,
                    created_by=created_by,
                    email=normalized_email or None,
                    linked_in_handle=normalized_handle or None,
                    updated_after=updated_after,
                    updated_before=updated_before,
                    page=next_page,
                    page_size=page_size,
                )
                scanned_pages += 1
                candidates = result["candidates"]
                scanned_candidates += len(candidates)
                matches.extend(
                    item
                    for item in candidates
                    if _matches_candidate_identity(
                        item,
                        email=normalized_email,
                        linked_in_handle=normalized_handle,
                        candidate_ids=candidate_id_set,
                    )
                )
                next_page = result["pagination"].get("next_page")
                if not next_page:
                    source_exhausted = True
                    stop_reason = "source_exhausted"
                    break

        deduped_matches = _dedupe_objects(matches, "candidate_id")
        return {
            "matches": deduped_matches,
            "scan": {
                "email": normalized_email,
                "linked_in_handle": normalized_handle,
                "candidate_ids": normalized_candidate_ids,
                "filters": {
                    "created_by": created_by or "",
                    "created_after": created_after,
                    "created_before": created_before,
                    "updated_after": updated_after,
                    "updated_before": updated_before,
                    "sort": sort or "",
                },
                "max_pages": max_pages,
                "page_size": page_size,
                "scanned_pages": scanned_pages,
                "scanned_batches": scanned_batches,
                "scanned_candidates": scanned_candidates,
                "returned_count": len(deduped_matches),
                "source_exhausted": source_exhausted,
                "stop_reason": stop_reason,
            },
        }

    def list_candidate_notes(
        self,
        *,
        candidate_id: str,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if created_after is not None:
            params["created_after"] = created_after
        if created_before is not None:
            params["created_before"] = created_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort

        path = self._candidate_notes_path.format(candidate_id=candidate_id)
        data, pagination = self._request_with_pagination("GET", path, params=params)
        notes = [_normalize_note(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "candidate_id": candidate_id,
            "notes": notes,
            "pagination": pagination,
        }

    def list_uploaded_resumes(
        self,
        *,
        candidate_id: str,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if created_after is not None:
            params["created_after"] = created_after
        if created_before is not None:
            params["created_before"] = created_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort

        path = self._candidate_uploaded_resumes_path.format(candidate_id=candidate_id)
        data, pagination = self._request_with_pagination("GET", path, params=params)
        resumes = [_normalize_uploaded_resume(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "candidate_id": candidate_id,
            "resumes": resumes,
            "pagination": pagination,
        }

    def list_custom_fields(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        project_id: Optional[str] = None,
        scope: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        name: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if created_after is not None:
            params["created_after"] = created_after
        if created_before is not None:
            params["created_before"] = created_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort
        if project_id:
            params["project_id"] = project_id
        if scope in {"team", "project"}:
            params["scope"] = scope
        if is_hidden is not None:
            params["is_hidden"] = is_hidden
        if name:
            params["name"] = name.strip()

        data, pagination = self._request_with_pagination("GET", self._custom_fields_path, params=params)
        fields = [_normalize_custom_field(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "custom_fields": fields,
            "pagination": pagination,
        }

    def list_custom_field_options(
        self,
        *,
        custom_field_id: str,
        value: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if value:
            params["value"] = value.strip()
        if is_hidden is not None:
            params["is_hidden"] = is_hidden
        path = self._custom_field_options_path.format(custom_field_id=custom_field_id)
        data, pagination = self._request_with_pagination("GET", path, params=params)
        options = [
            _normalize_custom_field_option(item)
            for item in data
            if isinstance(item, dict)
        ] if isinstance(data, list) else []
        return {
            "custom_field_id": custom_field_id,
            "options": options,
            "pagination": pagination,
        }

    def list_project_fields(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        is_required: Optional[bool] = None,
        name: Optional[str] = None,
        field_type: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if created_after is not None:
            params["created_after"] = created_after
        if created_before is not None:
            params["created_before"] = created_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort
        if is_hidden is not None:
            params["is_hidden"] = is_hidden
        if is_required is not None:
            params["is_required"] = is_required
        if name:
            params["name"] = name.strip()
        if field_type in {"text", "single_select", "multi_select"}:
            params["field_type"] = field_type

        data, pagination = self._request_with_pagination("GET", self._project_fields_path, params=params)
        fields = [_normalize_project_field(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "project_fields": fields,
            "pagination": pagination,
        }

    def list_project_field_options(
        self,
        *,
        project_field_id: str,
        value: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if value:
            params["value"] = value.strip()
        if is_hidden is not None:
            params["is_hidden"] = is_hidden
        path = self._project_field_options_path.format(project_field_id=project_field_id)
        data, pagination = self._request_with_pagination("GET", path, params=params)
        options = [
            _normalize_project_field_option(item)
            for item in data
            if isinstance(item, dict)
        ] if isinstance(data, list) else []
        return {
            "project_field_id": project_field_id,
            "options": options,
            "pagination": pagination,
        }

    def list_sequences(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        user_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if created_after is not None:
            params["created_after"] = created_after
        if created_before is not None:
            params["created_before"] = created_before
        if sort in {"asc", "desc"}:
            params["sort"] = sort
        if user_id:
            params["user_id"] = user_id
        data, pagination = self._request_with_pagination("GET", self._sequences_path, params=params)
        sequences = [_normalize_sequence(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "sequences": sequences,
            "pagination": pagination,
        }

    def get_sequence(self, sequence_id: str) -> dict[str, Any]:
        path = self._sequence_path.format(sequence_id=sequence_id)
        data = self.http.request("GET", path)
        body = _as_dict(data)
        sequence = _normalize_sequence(body)
        resolved_sequence_id = str(sequence.get("sequence_id") or sequence_id)
        return {
            "sequence_id": resolved_sequence_id,
            "sequence": sequence,
        }

    def list_project_membership_log(
        self,
        *,
        changed_after: Optional[int] = None,
        changed_before: Optional[int] = None,
        project_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if changed_after is not None:
            params["changed_after"] = changed_after
        if changed_before is not None:
            params["changed_before"] = changed_before
        if project_id:
            params["project_id"] = project_id
        if candidate_id:
            params["candidate_id"] = candidate_id
        if sort in {"asc", "desc"}:
            params["sort"] = sort

        data, pagination = self._request_with_pagination("GET", self._project_membership_log_path, params=params)
        entries = [dict(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        return {
            "entries": entries,
            "pagination": pagination,
        }

    def create_project(
        self,
        project_name: str,
        metadata: Optional[dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        payload: dict[str, Any] = {"name": project_name, "user_id": owner_id}
        if metadata:
            privacy = metadata.get("privacy_type")
            if privacy in {"confidential", "personal", "shared"}:
                payload["privacy_type"] = privacy
            description = metadata.get("description")
            if isinstance(description, str) and description.strip():
                payload["description"] = description.strip()
        data = self.http.request("POST", self._create_project_path, json_body=payload)
        body = _as_dict(data)
        project_id = _extract_id(body)
        return {
            "project_id": project_id,
            "name": str(body.get("name") or project_name),
            "user_id": owner_id,
            "provider_response": body,
        }

    def update_project(self, project_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        path = self._project_path.format(project_id=project_id)
        payload = {key: value for key, value in fields.items() if key in {"user_id", "name", "privacy_type", "description", "is_archived"}}
        data = self.http.request("PATCH", path, json_body=payload)
        body = _as_dict(data)
        project = _normalize_project(body)
        resolved_project_id = str(project.get("project_id") or project_id)
        return {
            "project_id": resolved_project_id,
            "project": project,
            "provider_response": body,
        }

    def create_candidate(self, fields: dict[str, Any], user_id: Optional[str] = None) -> dict[str, Any]:
        payload = {key: value for key, value in fields.items()}
        payload["created_by"] = self.resolve_user_id(user_id)
        data = self.http.request("POST", self._candidates_path, json_body=payload)
        body = _as_dict(data)
        candidate = _normalize_candidate(body)
        candidate_id = str(candidate.get("candidate_id") or _extract_id(candidate))
        return {
            "candidate_id": candidate_id,
            "candidate": candidate,
            "user_id": payload["created_by"],
            "provider_response": body,
        }

    def update_candidate(self, candidate_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        path = self._candidate_path.format(candidate_id=candidate_id)
        data = self.http.request("PUT", path, json_body=fields)
        body = _as_dict(data)
        candidate = _normalize_candidate(body)
        resolved_candidate_id = str(candidate.get("candidate_id") or candidate_id)
        return {
            "candidate_id": resolved_candidate_id,
            "candidate": candidate,
            "provider_response": body,
        }

    def add_profiles_to_project(
        self,
        project_id: str,
        profiles: list[dict[str, Any]],
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        candidate_ids: list[str] = []
        mapping: list[dict[str, str]] = []

        for profile in profiles:
            candidate_id = self._resolve_or_create_candidate(profile, owner_id)
            if not candidate_id:
                continue
            candidate_ids.append(candidate_id)
            source_candidate_id = _source_reference(profile)
            mapping.append({"source_candidate_id": source_candidate_id, "gem_candidate_id": candidate_id})

        deduped_ids = [item for item in dict.fromkeys(candidate_ids) if item]
        membership = self._add_candidates_to_project(project_id=project_id, candidate_ids=deduped_ids, user_id=owner_id)
        return {
            "project_id": project_id,
            "added_candidate_ids": deduped_ids,
            "mapping": mapping,
            "user_id": owner_id,
            "provider_response": {
                "membership": membership,
            },
        }

    def add_candidate_note(self, candidate_id: str, note: str, user_id: Optional[str] = None) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        payload = {
            "candidate_id": candidate_id,
            "user_id": owner_id,
            "content": note,
        }
        data = self.http.request("POST", self._add_note_path, json_body=payload)
        body = _as_dict(data)
        return {
            "candidate_id": candidate_id,
            "note": note,
            "user_id": owner_id,
            "provider_response": body,
        }

    def set_custom_value(
        self,
        candidate_id: str,
        key: str,
        value: Any,
        project_id: Optional[str] = None,
    ) -> dict[str, Any]:
        custom_field_id = self._resolve_custom_field_id(key=key, project_id=project_id)
        path = self._set_custom_value_path.format(candidate_id=candidate_id)
        payload = {
            "custom_fields": [
                {
                    "custom_field_id": custom_field_id,
                    "value": value,
                }
            ]
        }
        data = self.http.request("PUT", path, json_body=payload)
        body = _as_dict(data)
        return {
            "candidate_id": candidate_id,
            "key": key,
            "custom_field_id": custom_field_id,
            "value": value,
            "provider_response": body,
        }

    def remove_candidates_from_project(
        self,
        *,
        project_id: str,
        candidate_ids: list[str],
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        normalized_candidate_ids = _unique_non_empty(candidate_ids)
        path = self._project_candidates_path.format(project_id=project_id)
        payload = {"candidate_ids": normalized_candidate_ids, "user_id": owner_id}
        try:
            data = self.http.request("DELETE", path, json_body=payload)
            body = _as_dict(data)
            return {
                "project_id": project_id,
                "removed_candidate_ids": normalized_candidate_ids,
                "already_missing_candidate_ids": [],
                "user_id": owner_id,
                "provider_response": body,
            }
        except IntegrationRequestError as exc:
            conflicts = _extract_project_membership_conflicts(exc)
            if not conflicts:
                raise
            remaining = [item for item in normalized_candidate_ids if item not in set(conflicts)]
            if remaining:
                data = self.http.request("DELETE", path, json_body={"candidate_ids": remaining, "user_id": owner_id})
                body = _as_dict(data)
            else:
                body = _as_dict(exc.response_json or {})
            return {
                "project_id": project_id,
                "removed_candidate_ids": remaining,
                "already_missing_candidate_ids": conflicts,
                "user_id": owner_id,
                "provider_response": body,
            }

    def set_project_field_value(
        self,
        *,
        project_id: str,
        project_field_id: str,
        operation: str,
        option_ids: Optional[list[str]] = None,
        text: Optional[str] = None,
    ) -> dict[str, Any]:
        path = self._project_field_values_path.format(project_id=project_id)
        payload: dict[str, Any] = {
            "project_field_id": project_field_id,
            "operation": operation,
        }
        normalized_option_ids = _unique_non_empty(option_ids or [])
        if normalized_option_ids:
            payload["options"] = normalized_option_ids
        if text is not None:
            payload["text"] = text
        data = self.http.request("POST", path, json_body=payload)
        body = _as_dict(data)
        return {
            "project_id": project_id,
            "project_field_id": project_field_id,
            "operation": operation,
            "option_ids": normalized_option_ids,
            "text": text or "",
            "provider_response": body,
        }

    def create_custom_field(
        self,
        *,
        name: str,
        value_type: str,
        scope: str,
        project_id: Optional[str] = None,
        option_values: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": name,
            "value_type": value_type,
            "scope": scope,
        }
        if project_id:
            payload["project_id"] = project_id
        normalized_option_values = _unique_non_empty(option_values or [])
        if normalized_option_values:
            payload["option_values"] = normalized_option_values
        data = self.http.request("POST", self._custom_fields_path, json_body=payload)
        body = _as_dict(data)
        custom_field = _normalize_custom_field(body)
        custom_field_id = str(custom_field.get("custom_field_id") or _extract_id(custom_field))
        return {
            "custom_field_id": custom_field_id,
            "custom_field": custom_field,
            "provider_response": body,
        }

    def add_custom_field_options(self, *, custom_field_id: str, option_values: list[str]) -> dict[str, Any]:
        path = self._custom_field_options_path.format(custom_field_id=custom_field_id)
        payload = {"option_values": _unique_non_empty(option_values)}
        data = self.http.request("POST", path, json_body=payload)
        body = data if isinstance(data, list) else []
        options = [_normalize_custom_field_option(item) for item in body if isinstance(item, dict)]
        option_ids = [str(item.get("option_id") or item.get("id") or "").strip() for item in options]
        return {
            "custom_field_id": custom_field_id,
            "option_ids": [item for item in option_ids if item],
            "options": options,
            "provider_response": {"options": options},
        }

    def update_custom_field_option(
        self,
        *,
        custom_field_id: str,
        option_id: str,
        is_hidden: bool,
    ) -> dict[str, Any]:
        path = self._custom_field_option_path.format(custom_field_id=custom_field_id, option_id=option_id)
        data = self.http.request("PATCH", path, json_body={"is_hidden": is_hidden})
        body = _as_dict(data)
        option = _normalize_custom_field_option(body)
        resolved_option_id = str(option.get("option_id") or option_id)
        return {
            "custom_field_id": custom_field_id,
            "option_id": resolved_option_id,
            "option": option,
            "provider_response": body,
        }

    def create_project_field(
        self,
        *,
        name: str,
        field_type: str,
        options: Optional[list[str]] = None,
        is_required: Optional[bool] = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": name, "field_type": field_type}
        normalized_options = _unique_non_empty(options or [])
        if normalized_options:
            payload["options"] = normalized_options
        if is_required is not None:
            payload["is_required"] = is_required
        data = self.http.request("POST", self._project_fields_path, json_body=payload)
        body = _as_dict(data)
        project_field = _normalize_project_field(body)
        project_field_id = str(project_field.get("project_field_id") or _extract_id(project_field))
        return {
            "project_field_id": project_field_id,
            "project_field": project_field,
            "provider_response": body,
        }

    def create_project_field_option(self, *, project_field_id: str, options: list[str]) -> dict[str, Any]:
        path = self._project_field_options_path.format(project_field_id=project_field_id)
        payload = {"options": _unique_non_empty(options)}
        data = self.http.request("POST", path, json_body=payload)
        body = data if isinstance(data, list) else []
        options_payload = [_normalize_project_field_option(item) for item in body if isinstance(item, dict)]
        option_ids = [str(item.get("project_field_option_id") or item.get("id") or "").strip() for item in options_payload]
        return {
            "project_field_id": project_field_id,
            "option_ids": [item for item in option_ids if item],
            "options": options_payload,
            "provider_response": {"options": options_payload},
        }

    def update_project_field_option(
        self,
        *,
        project_field_id: str,
        project_field_option_id: str,
        is_hidden: bool,
    ) -> dict[str, Any]:
        path = self._project_field_option_path.format(
            project_field_id=project_field_id,
            project_field_option_id=project_field_option_id,
        )
        data = self.http.request("PATCH", path, json_body={"is_hidden": is_hidden})
        body = _as_dict(data)
        option = _normalize_project_field_option(body)
        resolved_option_id = str(option.get("project_field_option_id") or project_field_option_id)
        return {
            "project_field_id": project_field_id,
            "project_field_option_id": resolved_option_id,
            "option": option,
            "provider_response": body,
        }

    def upload_resume(self, *, candidate_id: str, file_path: str, user_id: Optional[str] = None) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        path = self._candidate_upload_resume_path.format(candidate_id=candidate_id, user_id=owner_id)
        resume_path = Path(file_path).expanduser()
        if not resume_path.is_file():
            raise IntegrationConfigError(f"gem: resume file not found: {resume_path}")
        mime_type = mimetypes.guess_type(resume_path.name)[0] or "application/octet-stream"
        with resume_path.open("rb") as handle:
            data = self._request_multipart(
                "POST",
                path,
                files={"resume_file": (resume_path.name, handle, mime_type)},
            )
        body = _as_dict(data)
        uploaded_resume = _normalize_uploaded_resume(body)
        return {
            "candidate_id": candidate_id,
            "user_id": owner_id,
            "uploaded_resume": uploaded_resume,
            "provider_response": body,
        }

    def _resolve_or_create_candidate(self, profile: dict[str, Any], user_id: str) -> str:
        direct_id = str(profile.get("candidate_id") or "").strip()
        if direct_id and _is_probably_gem_candidate_id(direct_id):
            return direct_id

        email = _extract_email(profile)
        if email:
            matches = self._find_candidates(email=email)
            for item in matches:
                if email in _candidate_email_values(item):
                    return _extract_id(item)

        linkedin_handle = _extract_linkedin_handle(
            str(profile.get("linked_in_handle") or "") or _extract_linkedin_url(profile)
        )
        if linkedin_handle:
            matches = self._find_candidates(linked_in_handle=linkedin_handle)
            for item in matches:
                if linkedin_handle in _candidate_linkedin_handles(item):
                    return _extract_id(item)

        return self._create_candidate_from_profile(profile, user_id=user_id)

    def _find_candidates(
        self,
        *,
        email: Optional[str] = None,
        linked_in_handle: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"page_size": 20}
        if email:
            params["email"] = email
        if linked_in_handle:
            params["linked_in_handle"] = linked_in_handle
        data = self.http.request("GET", self._candidates_path, params=params)
        return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []

    def _create_candidate_from_profile(self, profile: dict[str, Any], user_id: str) -> str:
        first_name, last_name = _normalize_name(profile)
        email = _extract_email(profile)
        linkedin_url = _extract_linkedin_url(profile)
        linkedin_handle = _extract_linkedin_handle(str(profile.get("linked_in_handle") or "") or linkedin_url)

        payload: dict[str, Any] = {
            "created_by": user_id,
        }
        if first_name:
            payload["first_name"] = first_name
        if last_name:
            payload["last_name"] = last_name
        if email:
            payload["emails"] = [{"email_address": email, "is_primary": True}]
        if linkedin_handle:
            payload["linked_in_handle"] = linkedin_handle
        if linkedin_url:
            payload["profile_urls"] = [linkedin_url]
        for field in ("title", "company", "location", "school"):
            value = str(profile.get(field) or "").strip()
            if value:
                payload[field] = value

        try:
            data = self.http.request("POST", self._candidates_path, json_body=payload)
            body = _as_dict(data)
            candidate_id = _extract_id(body)
            if candidate_id:
                return candidate_id
        except IntegrationRequestError as exc:
            duplicate_id = _extract_duplicate_candidate_id(exc)
            if duplicate_id:
                return duplicate_id
            raise
        raise IntegrationRequestError("gem: could not resolve candidate id after create")

    def _add_candidates_to_project(self, project_id: str, candidate_ids: list[str], user_id: str) -> dict[str, Any]:
        if not candidate_ids:
            return {"added_candidate_ids": [], "already_in_project_ids": []}
        path = self._add_profiles_to_project_path.format(project_id=project_id)
        payload = {"candidate_ids": candidate_ids, "user_id": user_id}
        try:
            data = self.http.request("PUT", path, json_body=payload)
            return {
                "added_candidate_ids": candidate_ids,
                "already_in_project_ids": [],
                "provider_response": _as_dict(data),
            }
        except IntegrationRequestError as exc:
            conflicts = _extract_project_membership_conflicts(exc)
            if not conflicts:
                raise
            remaining = [item for item in candidate_ids if item not in set(conflicts)]
            if remaining:
                data = self.http.request("PUT", path, json_body={"candidate_ids": remaining, "user_id": user_id})
                body = _as_dict(data)
            else:
                body = _as_dict(exc.response_json or {})
            return {
                "added_candidate_ids": remaining,
                "already_in_project_ids": conflicts,
                "provider_response": body,
            }

    def _resolve_custom_field_id(self, *, key: str, project_id: Optional[str]) -> str:
        if not key.strip():
            raise IntegrationConfigError("gem: custom field key is required")

        params: dict[str, Any] = {"name": key.strip(), "page_size": 100}
        if project_id:
            params["scope"] = "project"
            params["project_id"] = project_id
        fields = self.http.request("GET", self._custom_fields_path, params=params)
        if isinstance(fields, list):
            for field in fields:
                if not isinstance(field, dict):
                    continue
                if str(field.get("name") or "").strip().lower() == key.strip().lower():
                    field_id = _extract_id(field)
                    if field_id:
                        return field_id
            if fields:
                maybe = _extract_id(fields[0]) if isinstance(fields[0], dict) else ""
                if maybe:
                    return maybe

        # Allow direct IDs when callers already know the Gem custom field id.
        return key.strip()

    def _list_candidates_by_ids(self, candidate_ids: list[str]) -> dict[str, dict[str, Any]]:
        candidates: dict[str, dict[str, Any]] = {}
        for chunk in _chunked([item for item in dict.fromkeys(candidate_ids) if item], 20):
            data = self.http.request(
                "GET",
                self._candidates_path,
                params={
                    "candidate_ids": chunk,
                    "page": 1,
                    "page_size": len(chunk),
                },
            )
            if not isinstance(data, list):
                continue
            for item in data:
                if not isinstance(item, dict):
                    continue
                candidate = _normalize_candidate(item)
                candidate_id = _extract_id(candidate)
                if candidate_id:
                    candidates[candidate_id] = candidate
        return candidates

    def _request_with_pagination(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[Any, dict[str, Any]]:
        url = f"{self.http.base_url}/{path.lstrip('/')}"
        request_headers = self.http._build_headers()
        if headers:
            request_headers.update(headers)
        auth = httpx.BasicAuth(self.http.api_key, "") if self.http.auth_mode == "basic" else None
        response = self.http.client.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            headers=request_headers,
            auth=auth,
        )

        if response.status_code >= 400:
            parsed_json: dict[str, Any] | list[Any] | None = None
            try:
                parsed = response.json()
                if isinstance(parsed, (dict, list)):
                    parsed_json = parsed
            except Exception:
                parsed_json = None
            raise IntegrationRequestError(
                f"gem: {method} {url} failed with {response.status_code}: {response.text[:300]}",
                status_code=response.status_code,
                method=method,
                url=url,
                response_text=response.text,
                response_json=parsed_json,
            )

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            if not response.text or not response.text.strip():
                payload: Any = {}
            else:
                payload = response.json()
        elif response.status_code == 204:
            payload = {}
        else:
            payload = response.text

        pagination = _parse_pagination_header(
            response.headers.get("x-pagination", ""),
            page=int((params or {}).get("page") or 1),
            page_size=int((params or {}).get("page_size") or 20),
            returned_count=len(payload) if isinstance(payload, list) else 1 if payload else 0,
        )
        return payload, pagination

    def _request_multipart(
        self,
        method: str,
        path: str,
        *,
        data: Optional[dict[str, Any]] = None,
        files: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        url = f"{self.http.base_url}/{path.lstrip('/')}"
        request_headers = self.http._build_headers()
        request_headers.pop("Content-Type", None)
        if headers:
            request_headers.update(headers)
        auth = httpx.BasicAuth(self.http.api_key, "") if self.http.auth_mode == "basic" else None
        response = self.http.client.request(
            method=method,
            url=url,
            data=data,
            files=files,
            headers=request_headers,
            auth=auth,
        )
        if response.status_code >= 400:
            parsed_json: dict[str, Any] | list[Any] | None = None
            try:
                parsed = response.json()
                if isinstance(parsed, (dict, list)):
                    parsed_json = parsed
            except Exception:
                parsed_json = None
            raise IntegrationRequestError(
                f"gem: {method} {url} failed with {response.status_code}: {response.text[:300]}",
                status_code=response.status_code,
                method=method,
                url=url,
                response_text=response.text,
                response_json=parsed_json,
            )

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            if not response.text or not response.text.strip():
                return {}
            return response.json()
        if response.status_code == 204:
            return {}
        return response.text


class MockGemClient:
    def __init__(self) -> None:
        self.users = [
            {"id": "user_mock_1", "email": "mock@example.com", "name": "Mock User"},
            {"id": "user_mock_2", "email": "recruiter@example.com", "name": "Recruiter User"},
        ]
        self.projects: dict[str, dict[str, Any]] = {}
        self.project_candidates: dict[str, list[str]] = {}
        self.project_candidate_added_at: dict[str, dict[str, int]] = {}
        self.candidates: dict[str, dict[str, Any]] = {}
        self.notes: dict[str, list[dict[str, Any]]] = {}
        self.custom_values: dict[str, dict[str, Any]] = {}
        self.custom_fields: dict[str, dict[str, Any]] = {}
        self.project_fields: dict[str, dict[str, Any]] = {}
        self.project_field_values: dict[str, dict[str, Any]] = {}
        self.sequences: dict[str, dict[str, Any]] = {
            "seq_mock_1": {
                "id": "seq_mock_1",
                "name": "Default Sequence",
                "user_id": "user_mock_1",
                "created_at": 1,
            }
        }
        self.uploaded_resumes: dict[str, list[dict[str, Any]]] = {}
        self.membership_log: list[dict[str, Any]] = []
        self._timestamp = 1

    def _next_timestamp(self) -> int:
        self._timestamp += 1
        return self._timestamp

    def _record_membership(self, *, candidate_id: str, project_id: str, action: str) -> None:
        self.membership_log.append(
            {
                "candidate_id": candidate_id,
                "project_id": project_id,
                "action": action,
                "timestamp": self._next_timestamp(),
            }
        )

    def resolve_user_id(self, user_id: Optional[str] = None) -> str:
        if user_id and user_id.strip():
            return user_id.strip()
        return "user_mock_1"

    def list_users(
        self,
        *,
        email: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        users = [_normalize_user(item) for item in self.users]
        if email:
            query = email.strip().lower()
            users = [item for item in users if query in str(item.get("email") or "").lower()]
        page_items, pagination = _paginate_items(users, page=page, page_size=page_size)
        return {
            "users": page_items,
            "pagination": pagination,
        }

    def list_projects(
        self,
        *,
        owner_user_id: Optional[str] = None,
        readable_by_user_id: Optional[str] = None,
        writable_by_user_id: Optional[str] = None,
        is_archived: Optional[bool] = None,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        del readable_by_user_id, writable_by_user_id
        projects = [_normalize_project(item) for item in self.projects.values()]
        if owner_user_id:
            projects = [item for item in projects if str(item.get("user_id") or "") == owner_user_id]
        if is_archived is not None:
            projects = [item for item in projects if bool(item.get("is_archived", False)) is is_archived]
        if created_after is not None:
            projects = [item for item in projects if int(item.get("created_at") or 0) > created_after]
        if created_before is not None:
            projects = [item for item in projects if int(item.get("created_at") or 0) < created_before]
        projects.sort(key=lambda item: int(item.get("created_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(projects, page=page, page_size=page_size)
        return {
            "projects": page_items,
            "pagination": pagination,
        }

    def find_projects(
        self,
        *,
        name_exact: str = "",
        name_contains: str = "",
        owner_user_id: Optional[str] = None,
        readable_by_user_id: Optional[str] = None,
        writable_by_user_id: Optional[str] = None,
        is_archived: Optional[bool] = None,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        max_pages: int = 5,
        page_size: int = 100,
    ) -> dict[str, Any]:
        matches: list[dict[str, Any]] = []
        scanned_pages = 0
        scanned_projects = 0
        next_page = 1
        source_exhausted = False
        stop_reason = "max_pages_reached"
        while next_page and scanned_pages < max_pages:
            result = self.list_projects(
                owner_user_id=owner_user_id,
                readable_by_user_id=readable_by_user_id,
                writable_by_user_id=writable_by_user_id,
                is_archived=is_archived,
                created_after=created_after,
                created_before=created_before,
                sort=sort,
                page=next_page,
                page_size=page_size,
            )
            scanned_pages += 1
            projects = result["projects"]
            scanned_projects += len(projects)
            matches.extend(
                item
                for item in projects
                if _matches_project_name(item, name_exact=name_exact.strip(), name_contains=name_contains.strip())
            )
            next_page = result["pagination"].get("next_page")
            if not next_page:
                source_exhausted = True
                stop_reason = "source_exhausted"
                break
        deduped_matches = _dedupe_objects(matches, "project_id")
        return {
            "matches": deduped_matches,
            "scan": {
                "name_exact": name_exact.strip(),
                "name_contains": name_contains.strip(),
                "filters": {
                    "owner_user_id": owner_user_id or "",
                    "readable_by_user_id": readable_by_user_id or "",
                    "writable_by_user_id": writable_by_user_id or "",
                    "is_archived": is_archived,
                    "created_after": created_after,
                    "created_before": created_before,
                    "sort": sort or "",
                },
                "max_pages": max_pages,
                "page_size": page_size,
                "scanned_pages": scanned_pages,
                "scanned_projects": scanned_projects,
                "returned_count": len(deduped_matches),
                "source_exhausted": source_exhausted,
                "stop_reason": stop_reason,
            },
        }

    def get_project(self, project_id: str) -> dict[str, Any]:
        project = self.projects.get(project_id)
        if not isinstance(project, dict):
            raise IntegrationRequestError(
                f"gem: project not found: {project_id}",
                status_code=404,
                method="GET",
                url=f"/v0/projects/{project_id}",
            )
        normalized = _normalize_project(project)
        return {
            "project_id": _extract_id(normalized) or project_id,
            "project": normalized,
        }

    def list_project_candidates(
        self,
        *,
        project_id: str,
        page: int = 1,
        page_size: int = 20,
        added_after: Optional[int] = None,
        added_before: Optional[int] = None,
        sort: Optional[str] = None,
        include_candidates: bool = True,
    ) -> dict[str, Any]:
        project_result = self.get_project(project_id)
        memberships: list[dict[str, Any]] = []
        added_at_lookup = self.project_candidate_added_at.get(project_id, {})
        for candidate_id in self.project_candidates.get(project_id, []):
            added_at = int(added_at_lookup.get(candidate_id) or 0)
            if added_after is not None and added_at <= added_after:
                continue
            if added_before is not None and added_at >= added_before:
                continue
            memberships.append({"candidate_id": candidate_id, "added_at": added_at})
        memberships.sort(key=lambda item: int(item.get("added_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(memberships, page=page, page_size=page_size)

        entries: list[dict[str, Any]] = []
        unresolved_candidate_ids: list[str] = []
        for membership in page_items:
            candidate_id = str(membership.get("candidate_id") or "").strip()
            candidate = _normalize_candidate(self.candidates.get(candidate_id, {})) if include_candidates else {}
            if include_candidates and not candidate:
                unresolved_candidate_ids.append(candidate_id)
            entries.append(
                {
                    "candidate_id": candidate_id,
                    "added_at": membership.get("added_at"),
                    "candidate": candidate,
                }
            )

        return {
            "project_id": project_result["project_id"],
            "project": project_result["project"],
            "entries": entries,
            "pagination": pagination,
            "unresolved_candidate_ids": unresolved_candidate_ids,
        }

    def list_candidates(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        created_by: Optional[str] = None,
        email: Optional[str] = None,
        linked_in_handle: Optional[str] = None,
        updated_after: Optional[int] = None,
        updated_before: Optional[int] = None,
        candidate_ids: Optional[list[str]] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        candidates = [_normalize_candidate(item) for item in self.candidates.values()]
        normalized_ids = set(_unique_non_empty(candidate_ids or []))
        if created_after is not None:
            candidates = [item for item in candidates if int(item.get("created_at") or 0) > created_after]
        if created_before is not None:
            candidates = [item for item in candidates if int(item.get("created_at") or 0) < created_before]
        if updated_after is not None:
            candidates = [item for item in candidates if int(item.get("last_updated_at") or item.get("created_at") or 0) > updated_after]
        if updated_before is not None:
            candidates = [item for item in candidates if int(item.get("last_updated_at") or item.get("created_at") or 0) < updated_before]
        if created_by:
            candidates = [item for item in candidates if str(item.get("created_by") or "") == created_by]
        if email:
            query = email.strip().lower()
            candidates = [item for item in candidates if any(query in value for value in _candidate_email_values(item))]
        if linked_in_handle:
            handle = _extract_linkedin_handle(linked_in_handle)
            candidates = [item for item in candidates if handle in _candidate_linkedin_handles(item)]
        if normalized_ids:
            candidates = [item for item in candidates if str(item.get("candidate_id") or "") in normalized_ids]
        candidates.sort(key=lambda item: int(item.get("created_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(candidates, page=page, page_size=page_size)
        return {
            "candidates": page_items,
            "pagination": pagination,
        }

    def find_candidates(
        self,
        *,
        email: str = "",
        linked_in_handle: str = "",
        linkedin_url: str = "",
        candidate_ids: Optional[list[str]] = None,
        created_by: Optional[str] = None,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        updated_after: Optional[int] = None,
        updated_before: Optional[int] = None,
        sort: Optional[str] = None,
        max_pages: int = 5,
        page_size: int = 100,
    ) -> dict[str, Any]:
        normalized_email = email.strip().lower()
        normalized_handle = _extract_linkedin_handle(linked_in_handle or linkedin_url)
        normalized_candidate_ids = _unique_non_empty(candidate_ids or [])
        candidate_id_set = set(normalized_candidate_ids)
        if not (normalized_email or normalized_handle or normalized_candidate_ids):
            raise IntegrationConfigError("gem: find_candidates requires email, linked_in_handle/linkedin_url, or candidate_ids")

        matches: list[dict[str, Any]] = []
        scanned_pages = 0
        scanned_batches = 0
        scanned_candidates = 0
        if normalized_candidate_ids:
            for chunk in _chunked(normalized_candidate_ids, 20):
                scanned_batches += 1
                result = self.list_candidates(
                    created_after=created_after,
                    created_before=created_before,
                    sort=sort,
                    created_by=created_by,
                    email=normalized_email or None,
                    linked_in_handle=normalized_handle or None,
                    updated_after=updated_after,
                    updated_before=updated_before,
                    candidate_ids=chunk,
                    page=1,
                    page_size=max(1, len(chunk)),
                )
                candidates = result["candidates"]
                scanned_candidates += len(candidates)
                matches.extend(
                    item
                    for item in candidates
                    if _matches_candidate_identity(
                        item,
                        email=normalized_email,
                        linked_in_handle=normalized_handle,
                        candidate_ids=candidate_id_set,
                    )
                )
            source_exhausted = True
            stop_reason = "all_candidate_ids_scanned"
        else:
            next_page = 1
            source_exhausted = False
            stop_reason = "max_pages_reached"
            while next_page and scanned_pages < max_pages:
                result = self.list_candidates(
                    created_after=created_after,
                    created_before=created_before,
                    sort=sort,
                    created_by=created_by,
                    email=normalized_email or None,
                    linked_in_handle=normalized_handle or None,
                    updated_after=updated_after,
                    updated_before=updated_before,
                    page=next_page,
                    page_size=page_size,
                )
                scanned_pages += 1
                candidates = result["candidates"]
                scanned_candidates += len(candidates)
                matches.extend(
                    item
                    for item in candidates
                    if _matches_candidate_identity(
                        item,
                        email=normalized_email,
                        linked_in_handle=normalized_handle,
                        candidate_ids=candidate_id_set,
                    )
                )
                next_page = result["pagination"].get("next_page")
                if not next_page:
                    source_exhausted = True
                    stop_reason = "source_exhausted"
                    break
        deduped_matches = _dedupe_objects(matches, "candidate_id")
        return {
            "matches": deduped_matches,
            "scan": {
                "email": normalized_email,
                "linked_in_handle": normalized_handle,
                "candidate_ids": normalized_candidate_ids,
                "filters": {
                    "created_by": created_by or "",
                    "created_after": created_after,
                    "created_before": created_before,
                    "updated_after": updated_after,
                    "updated_before": updated_before,
                    "sort": sort or "",
                },
                "max_pages": max_pages,
                "page_size": page_size,
                "scanned_pages": scanned_pages,
                "scanned_batches": scanned_batches,
                "scanned_candidates": scanned_candidates,
                "returned_count": len(deduped_matches),
                "source_exhausted": source_exhausted,
                "stop_reason": stop_reason,
            },
        }

    def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        candidate = self.candidates.get(candidate_id)
        if not isinstance(candidate, dict):
            raise IntegrationRequestError(
                f"gem: candidate not found: {candidate_id}",
                status_code=404,
                method="GET",
                url=f"/v0/candidates/{candidate_id}",
            )
        normalized = _normalize_candidate(candidate)
        return {
            "candidate_id": _extract_id(normalized) or candidate_id,
            "candidate": normalized,
        }

    def list_candidate_notes(
        self,
        *,
        candidate_id: str,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        notes = [_normalize_note(item) for item in self.notes.get(candidate_id, [])]
        if created_after is not None:
            notes = [item for item in notes if int(item.get("created_at") or 0) > created_after]
        if created_before is not None:
            notes = [item for item in notes if int(item.get("created_at") or 0) < created_before]
        notes.sort(key=lambda item: int(item.get("created_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(notes, page=page, page_size=page_size)
        return {
            "candidate_id": candidate_id,
            "notes": page_items,
            "pagination": pagination,
        }

    def list_uploaded_resumes(
        self,
        *,
        candidate_id: str,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        resumes = [_normalize_uploaded_resume(item) for item in self.uploaded_resumes.get(candidate_id, [])]
        if created_after is not None:
            resumes = [item for item in resumes if int(item.get("created_at") or 0) > created_after]
        if created_before is not None:
            resumes = [item for item in resumes if int(item.get("created_at") or 0) < created_before]
        resumes.sort(key=lambda item: int(item.get("created_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(resumes, page=page, page_size=page_size)
        return {
            "candidate_id": candidate_id,
            "resumes": page_items,
            "pagination": pagination,
        }

    def list_custom_fields(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        project_id: Optional[str] = None,
        scope: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        name: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        fields = [_normalize_custom_field(item) for item in self.custom_fields.values()]
        if created_after is not None:
            fields = [item for item in fields if int(item.get("created_at") or 0) > created_after]
        if created_before is not None:
            fields = [item for item in fields if int(item.get("created_at") or 0) < created_before]
        if project_id:
            fields = [item for item in fields if str(item.get("project_id") or "") == project_id]
        if scope:
            fields = [item for item in fields if str(item.get("scope") or "") == scope]
        if is_hidden is not None:
            fields = [item for item in fields if bool(item.get("is_hidden", False)) is is_hidden]
        if name:
            query = name.strip().casefold()
            fields = [item for item in fields if query in str(item.get("name") or "").casefold()]
        fields.sort(key=lambda item: int(item.get("created_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(fields, page=page, page_size=page_size)
        return {
            "custom_fields": page_items,
            "pagination": pagination,
        }

    def list_custom_field_options(
        self,
        *,
        custom_field_id: str,
        value: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        custom_field = self.custom_fields.get(custom_field_id, {})
        options = [_normalize_custom_field_option(item) for item in custom_field.get("options", [])]
        if value:
            query = value.strip().casefold()
            options = [item for item in options if query in str(item.get("value") or "").casefold()]
        if is_hidden is not None:
            options = [item for item in options if bool(item.get("is_hidden", False)) is is_hidden]
        page_items, pagination = _paginate_items(options, page=page, page_size=page_size)
        return {
            "custom_field_id": custom_field_id,
            "options": page_items,
            "pagination": pagination,
        }

    def list_project_fields(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        is_required: Optional[bool] = None,
        name: Optional[str] = None,
        field_type: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        fields = [_normalize_project_field(item) for item in self.project_fields.values()]
        if created_after is not None:
            fields = [item for item in fields if int(item.get("created_at") or 0) > created_after]
        if created_before is not None:
            fields = [item for item in fields if int(item.get("created_at") or 0) < created_before]
        if is_hidden is not None:
            fields = [item for item in fields if bool(item.get("is_hidden", False)) is is_hidden]
        if is_required is not None:
            fields = [item for item in fields if bool(item.get("is_required", False)) is is_required]
        if name:
            query = name.strip().casefold()
            fields = [item for item in fields if query in str(item.get("name") or "").casefold()]
        if field_type:
            fields = [item for item in fields if str(item.get("field_type") or "") == field_type]
        fields.sort(key=lambda item: int(item.get("created_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(fields, page=page, page_size=page_size)
        return {
            "project_fields": page_items,
            "pagination": pagination,
        }

    def list_project_field_options(
        self,
        *,
        project_field_id: str,
        value: Optional[str] = None,
        is_hidden: Optional[bool] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        project_field = self.project_fields.get(project_field_id, {})
        options = [_normalize_project_field_option(item) for item in project_field.get("options", [])]
        if value:
            query = value.strip().casefold()
            options = [item for item in options if query in str(item.get("value") or "").casefold()]
        if is_hidden is not None:
            options = [item for item in options if bool(item.get("is_hidden", False)) is is_hidden]
        page_items, pagination = _paginate_items(options, page=page, page_size=page_size)
        return {
            "project_field_id": project_field_id,
            "options": page_items,
            "pagination": pagination,
        }

    def list_sequences(
        self,
        *,
        created_after: Optional[int] = None,
        created_before: Optional[int] = None,
        sort: Optional[str] = None,
        user_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        sequences = [_normalize_sequence(item) for item in self.sequences.values()]
        if created_after is not None:
            sequences = [item for item in sequences if int(item.get("created_at") or 0) > created_after]
        if created_before is not None:
            sequences = [item for item in sequences if int(item.get("created_at") or 0) < created_before]
        if user_id:
            sequences = [item for item in sequences if str(item.get("user_id") or "") == user_id]
        sequences.sort(key=lambda item: int(item.get("created_at") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(sequences, page=page, page_size=page_size)
        return {
            "sequences": page_items,
            "pagination": pagination,
        }

    def get_sequence(self, sequence_id: str) -> dict[str, Any]:
        sequence = self.sequences.get(sequence_id)
        if not isinstance(sequence, dict):
            raise IntegrationRequestError(
                f"gem: sequence not found: {sequence_id}",
                status_code=404,
                method="GET",
                url=f"/v0/sequences/{sequence_id}",
            )
        normalized = _normalize_sequence(sequence)
        return {
            "sequence_id": str(normalized.get("sequence_id") or sequence_id),
            "sequence": normalized,
        }

    def list_project_membership_log(
        self,
        *,
        changed_after: Optional[int] = None,
        changed_before: Optional[int] = None,
        project_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        entries = [dict(item) for item in self.membership_log]
        if changed_after is not None:
            entries = [item for item in entries if int(item.get("timestamp") or 0) > changed_after]
        if changed_before is not None:
            entries = [item for item in entries if int(item.get("timestamp") or 0) < changed_before]
        if project_id:
            entries = [item for item in entries if str(item.get("project_id") or "") == project_id]
        if candidate_id:
            entries = [item for item in entries if str(item.get("candidate_id") or "") == candidate_id]
        entries.sort(key=lambda item: int(item.get("timestamp") or 0), reverse=(sort != "asc"))
        page_items, pagination = _paginate_items(entries, page=page, page_size=page_size)
        return {
            "entries": page_items,
            "pagination": pagination,
        }

    def create_project(
        self,
        project_name: str,
        metadata: Optional[dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        project_id = f"proj_{uuid.uuid4().hex[:8]}"
        project = {
            "id": project_id,
            "name": project_name,
            "user_id": owner_id,
            "privacy_type": str((metadata or {}).get("privacy_type") or "personal"),
            "description": (metadata or {}).get("description"),
            "created_at": self._next_timestamp(),
            "is_archived": False,
        }
        self.projects[project_id] = project
        self.project_candidates.setdefault(project_id, [])
        self.project_candidate_added_at.setdefault(project_id, {})
        return {"project_id": project_id, "name": project_name, "user_id": owner_id, "provider_response": project}

    def update_project(self, project_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        project = self.projects.get(project_id)
        if not isinstance(project, dict):
            raise IntegrationRequestError(
                f"gem: project not found: {project_id}",
                status_code=404,
                method="PATCH",
                url=f"/v0/projects/{project_id}",
            )
        for key in ("user_id", "name", "privacy_type", "description", "is_archived"):
            if key in fields:
                project[key] = fields[key]
        project["last_updated_at"] = self._next_timestamp()
        normalized = _normalize_project(project)
        return {
            "project_id": str(normalized.get("project_id") or project_id),
            "project": normalized,
            "provider_response": normalized,
        }

    def create_candidate(self, fields: dict[str, Any], user_id: Optional[str] = None) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        candidate_id = f"cand_{uuid.uuid4().hex[:8]}"
        candidate = dict(fields)
        candidate["id"] = candidate_id
        candidate["created_by"] = owner_id
        candidate.setdefault("created_at", self._next_timestamp())
        candidate["last_updated_at"] = candidate["created_at"]
        self.candidates[candidate_id] = candidate
        normalized = _normalize_candidate(candidate)
        return {
            "candidate_id": candidate_id,
            "candidate": normalized,
            "user_id": owner_id,
            "provider_response": normalized,
        }

    def update_candidate(self, candidate_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        candidate = self.candidates.get(candidate_id)
        if not isinstance(candidate, dict):
            raise IntegrationRequestError(
                f"gem: candidate not found: {candidate_id}",
                status_code=404,
                method="PUT",
                url=f"/v0/candidates/{candidate_id}",
            )
        candidate.update(fields)
        candidate["last_updated_at"] = self._next_timestamp()
        normalized = _normalize_candidate(candidate)
        return {
            "candidate_id": str(normalized.get("candidate_id") or candidate_id),
            "candidate": normalized,
            "provider_response": normalized,
        }

    def _resolve_or_create_candidate_from_profile(self, profile: dict[str, Any], user_id: str) -> str:
        direct_id = str(profile.get("candidate_id") or "").strip()
        if direct_id and direct_id in self.candidates:
            return direct_id
        email = _extract_email(profile)
        linkedin_handle = _extract_linkedin_handle(str(profile.get("linked_in_handle") or "") or _extract_linkedin_url(profile))
        matches = self.find_candidates(
            email=email,
            linked_in_handle=linkedin_handle,
            candidate_ids=[direct_id] if direct_id else None,
        )["matches"] if (email or linkedin_handle or direct_id) else []
        if matches:
            return str(matches[0].get("candidate_id") or "")
        first_name, last_name = _normalize_name(profile)
        payload: dict[str, Any] = {}
        if first_name:
            payload["first_name"] = first_name
        if last_name:
            payload["last_name"] = last_name
        if email:
            payload["emails"] = [{"email_address": email, "is_primary": True}]
        if linkedin_handle:
            payload["linked_in_handle"] = linkedin_handle
        linkedin_url = _extract_linkedin_url(profile)
        if linkedin_url:
            payload["profile_urls"] = [linkedin_url]
        for field in ("title", "company", "location", "school"):
            value = str(profile.get(field) or "").strip()
            if value:
                payload[field] = value
        return self.create_candidate(payload, user_id=user_id)["candidate_id"]

    def add_profiles_to_project(
        self,
        project_id: str,
        profiles: list[dict[str, Any]],
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        if project_id not in self.projects:
            self.projects[project_id] = {
                "id": project_id,
                "name": project_id,
                "user_id": owner_id,
                "created_at": self._next_timestamp(),
                "is_archived": False,
            }
            self.project_candidates.setdefault(project_id, [])
            self.project_candidate_added_at.setdefault(project_id, {})

        mapping: list[dict[str, str]] = []
        resolved_ids: list[str] = []
        for profile in profiles:
            candidate_id = self._resolve_or_create_candidate_from_profile(profile, owner_id)
            resolved_ids.append(candidate_id)
            mapping.append(
                {
                    "source_candidate_id": _source_reference(profile),
                    "gem_candidate_id": candidate_id,
                }
            )

        current = self.project_candidates.setdefault(project_id, [])
        added_at_lookup = self.project_candidate_added_at.setdefault(project_id, {})
        for candidate_id in resolved_ids:
            if candidate_id not in current:
                current.append(candidate_id)
                added_at_lookup[candidate_id] = self._next_timestamp()
                self._record_membership(candidate_id=candidate_id, project_id=project_id, action="added")

        return {
            "project_id": project_id,
            "added_candidate_ids": resolved_ids,
            "mapping": mapping,
            "user_id": owner_id,
            "provider_response": {"membership": {"added_candidate_ids": resolved_ids}},
        }

    def add_candidate_note(self, candidate_id: str, note: str, user_id: Optional[str] = None) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        notes = self.notes.setdefault(candidate_id, [])
        note_payload = {
            "id": f"note_{uuid.uuid4().hex[:8]}",
            "candidate_id": candidate_id,
            "user_id": owner_id,
            "content": note,
            "created_at": self._next_timestamp(),
            "is_private": False,
        }
        notes.append(note_payload)
        return {
            "candidate_id": candidate_id,
            "note": note,
            "user_id": owner_id,
            "provider_response": note_payload,
        }

    def set_custom_value(
        self,
        candidate_id: str,
        key: str,
        value: Any,
        project_id: Optional[str] = None,
    ) -> dict[str, Any]:
        custom_field_id = key
        for field in self.custom_fields.values():
            normalized = _normalize_custom_field(field)
            if str(normalized.get("name") or "").strip().lower() == key.strip().lower():
                if project_id and str(normalized.get("project_id") or "") != project_id:
                    continue
                custom_field_id = str(normalized.get("custom_field_id") or key)
                break
        values = self.custom_values.setdefault(candidate_id, {})
        values[key] = value
        return {
            "candidate_id": candidate_id,
            "key": key,
            "custom_field_id": custom_field_id,
            "value": value,
            "provider_response": {"ok": True},
        }

    def remove_candidates_from_project(
        self,
        *,
        project_id: str,
        candidate_ids: list[str],
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        normalized_candidate_ids = _unique_non_empty(candidate_ids)
        current = self.project_candidates.setdefault(project_id, [])
        missing = [item for item in normalized_candidate_ids if item not in current]
        removed = [item for item in normalized_candidate_ids if item in current]
        self.project_candidates[project_id] = [item for item in current if item not in set(removed)]
        for candidate_id in removed:
            self.project_candidate_added_at.setdefault(project_id, {}).pop(candidate_id, None)
            self._record_membership(candidate_id=candidate_id, project_id=project_id, action="removed")
        return {
            "project_id": project_id,
            "removed_candidate_ids": removed,
            "already_missing_candidate_ids": missing,
            "user_id": owner_id,
            "provider_response": {"ok": True},
        }

    def set_project_field_value(
        self,
        *,
        project_id: str,
        project_field_id: str,
        operation: str,
        option_ids: Optional[list[str]] = None,
        text: Optional[str] = None,
    ) -> dict[str, Any]:
        field_values = self.project_field_values.setdefault(project_id, {})
        normalized_option_ids = _unique_non_empty(option_ids or [])
        if operation == "remove":
            if normalized_option_ids:
                existing = field_values.get(project_field_id, [])
                if isinstance(existing, list):
                    field_values[project_field_id] = [item for item in existing if item not in set(normalized_option_ids)]
                else:
                    field_values.pop(project_field_id, None)
            else:
                field_values.pop(project_field_id, None)
        elif text is not None:
            field_values[project_field_id] = text
        else:
            existing = field_values.get(project_field_id, [])
            if not isinstance(existing, list):
                existing = []
            field_values[project_field_id] = _unique_non_empty(existing + normalized_option_ids)
        return {
            "project_id": project_id,
            "project_field_id": project_field_id,
            "operation": operation,
            "option_ids": normalized_option_ids,
            "text": text or "",
            "provider_response": {"value": field_values.get(project_field_id)},
        }

    def create_custom_field(
        self,
        *,
        name: str,
        value_type: str,
        scope: str,
        project_id: Optional[str] = None,
        option_values: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        custom_field_id = f"cf_{uuid.uuid4().hex[:8]}"
        options = [
            {"id": f"cfo_{uuid.uuid4().hex[:8]}", "value": option_value, "is_hidden": False}
            for option_value in _unique_non_empty(option_values or [])
        ]
        custom_field = {
            "id": custom_field_id,
            "name": name,
            "value_type": value_type,
            "scope": scope,
            "project_id": project_id,
            "is_hidden": False,
            "options": options,
            "created_at": self._next_timestamp(),
        }
        self.custom_fields[custom_field_id] = custom_field
        normalized = _normalize_custom_field(custom_field)
        return {
            "custom_field_id": custom_field_id,
            "custom_field": normalized,
            "provider_response": normalized,
        }

    def add_custom_field_options(self, *, custom_field_id: str, option_values: list[str]) -> dict[str, Any]:
        custom_field = self.custom_fields.setdefault(custom_field_id, {"id": custom_field_id, "options": []})
        options = custom_field.setdefault("options", [])
        created_options = []
        for option_value in _unique_non_empty(option_values):
            option = {"id": f"cfo_{uuid.uuid4().hex[:8]}", "value": option_value, "is_hidden": False}
            options.append(option)
            created_options.append(_normalize_custom_field_option(option))
        return {
            "custom_field_id": custom_field_id,
            "option_ids": [str(item.get("option_id") or "") for item in created_options if item.get("option_id")],
            "options": created_options,
            "provider_response": {"options": created_options},
        }

    def update_custom_field_option(
        self,
        *,
        custom_field_id: str,
        option_id: str,
        is_hidden: bool,
    ) -> dict[str, Any]:
        custom_field = self.custom_fields.get(custom_field_id, {})
        for option in custom_field.get("options", []):
            if str(option.get("id") or "") == option_id:
                option["is_hidden"] = is_hidden
                normalized = _normalize_custom_field_option(option)
                return {
                    "custom_field_id": custom_field_id,
                    "option_id": option_id,
                    "option": normalized,
                    "provider_response": normalized,
                }
        raise IntegrationRequestError(
            f"gem: custom field option not found: {option_id}",
            status_code=404,
            method="PATCH",
            url=f"/v0/custom_fields/{custom_field_id}/options/{option_id}",
        )

    def create_project_field(
        self,
        *,
        name: str,
        field_type: str,
        options: Optional[list[str]] = None,
        is_required: Optional[bool] = None,
    ) -> dict[str, Any]:
        project_field_id = f"pf_{uuid.uuid4().hex[:8]}"
        option_payloads = [
            {"id": f"pfo_{uuid.uuid4().hex[:8]}", "value": option_value, "is_hidden": False}
            for option_value in _unique_non_empty(options or [])
        ]
        project_field = {
            "id": project_field_id,
            "name": name,
            "field_type": field_type,
            "user_id": "user_mock_1",
            "options": option_payloads,
            "is_required": bool(is_required),
            "is_hidden": False,
            "created_at": self._next_timestamp(),
        }
        self.project_fields[project_field_id] = project_field
        normalized = _normalize_project_field(project_field)
        return {
            "project_field_id": project_field_id,
            "project_field": normalized,
            "provider_response": normalized,
        }

    def create_project_field_option(self, *, project_field_id: str, options: list[str]) -> dict[str, Any]:
        project_field = self.project_fields.setdefault(project_field_id, {"id": project_field_id, "options": []})
        option_list = project_field.setdefault("options", [])
        created_options = []
        for option_value in _unique_non_empty(options):
            option = {"id": f"pfo_{uuid.uuid4().hex[:8]}", "value": option_value, "is_hidden": False}
            option_list.append(option)
            created_options.append(_normalize_project_field_option(option))
        return {
            "project_field_id": project_field_id,
            "option_ids": [str(item.get("project_field_option_id") or "") for item in created_options if item.get("project_field_option_id")],
            "options": created_options,
            "provider_response": {"options": created_options},
        }

    def update_project_field_option(
        self,
        *,
        project_field_id: str,
        project_field_option_id: str,
        is_hidden: bool,
    ) -> dict[str, Any]:
        project_field = self.project_fields.get(project_field_id, {})
        for option in project_field.get("options", []):
            if str(option.get("id") or "") == project_field_option_id:
                option["is_hidden"] = is_hidden
                normalized = _normalize_project_field_option(option)
                return {
                    "project_field_id": project_field_id,
                    "project_field_option_id": project_field_option_id,
                    "option": normalized,
                    "provider_response": normalized,
                }
        raise IntegrationRequestError(
            f"gem: project field option not found: {project_field_option_id}",
            status_code=404,
            method="PATCH",
            url=f"/v0/project_fields/{project_field_id}/options/{project_field_option_id}",
        )

    def upload_resume(self, *, candidate_id: str, file_path: str, user_id: Optional[str] = None) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        resume_path = Path(file_path).expanduser()
        if not resume_path.is_file():
            raise IntegrationConfigError(f"gem: resume file not found: {resume_path}")
        resume = {
            "id": f"resume_{uuid.uuid4().hex[:8]}",
            "candidate_id": candidate_id,
            "user_id": owner_id,
            "filename": resume_path.name,
            "download_url": f"mock://download/{resume_path.name}",
            "created_at": self._next_timestamp(),
        }
        self.uploaded_resumes.setdefault(candidate_id, []).append(resume)
        normalized = _normalize_uploaded_resume(resume)
        return {
            "candidate_id": candidate_id,
            "user_id": owner_id,
            "uploaded_resume": normalized,
            "provider_response": normalized,
        }


def build_gem_client(mode: str):
    if mode == "mock":
        return MockGemClient()
    if mode == "live":
        return GemClient()
    raise IntegrationConfigError("AR_INTEGRATION_MODE must be one of: mock, live")


def _source_reference(profile: dict[str, Any]) -> str:
    for key in ("candidate_id", "email", "linkedin", "linked_in_handle", "name"):
        value = str(profile.get(key) or "").strip()
        if value:
            return value
    return ""


def _is_probably_gem_candidate_id(value: str) -> bool:
    candidate_id = value.strip()
    if not candidate_id:
        return False
    if candidate_id.startswith("candidates:"):
        return True
    # Gem GraphQL IDs are base64-encoded strings like "Y2FuZGlkYXRlczoxMjM=" -> "candidates:123"
    if re.fullmatch(r"[A-Za-z0-9+/=]+", candidate_id):
        try:
            decoded = base64.b64decode(candidate_id, validate=True).decode("utf-8", errors="ignore")
        except Exception:
            decoded = ""
        if decoded.startswith("candidates:"):
            return True
    return False


def _paginate_items(items: list[dict[str, Any]], *, page: int, page_size: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    total = len(items)
    total_pages = max((total + safe_page_size - 1) // safe_page_size, 1)
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    page_items = items[start:end]
    pagination = {
        "total": total,
        "total_pages": total_pages,
        "first_page": 1,
        "last_page": total_pages,
        "page": safe_page,
        "next_page": safe_page + 1 if safe_page < total_pages else None,
        "page_size": safe_page_size,
        "returned_count": len(page_items),
    }
    return page_items, pagination
