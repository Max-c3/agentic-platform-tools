from __future__ import annotations

import json
import os
import re
import uuid
import base64
from typing import Any, Optional

import httpx

from agentic_tools_core.integration_clients.exceptions import IntegrationConfigError
from agentic_tools_core.integration_clients.exceptions import IntegrationRequestError
from agentic_tools_core.integration_clients.http_client import IntegrationHttpClient


def _as_dict(payload: Any) -> dict[str, Any]:
    return payload if isinstance(payload, dict) else {"value": payload}


def _extract_id(payload: dict[str, Any]) -> str:
    return str(payload.get("id") or payload.get("candidate_id") or payload.get("project_id") or "").strip()


def _normalize_project(payload: dict[str, Any]) -> dict[str, Any]:
    project = dict(payload)
    project_id = _extract_id(project)
    if project_id:
        project["project_id"] = project_id
    return project


def _normalize_candidate(payload: dict[str, Any]) -> dict[str, Any]:
    candidate = dict(payload)
    candidate_id = _extract_id(candidate)
    if candidate_id:
        candidate["candidate_id"] = candidate_id
    return candidate


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
        self._set_custom_value_path = os.getenv("GEM_ENDPOINT_SET_CUSTOM_VALUE", self._candidate_path)
        self._custom_fields_path = os.getenv("GEM_ENDPOINT_CUSTOM_FIELDS", "/v0/custom_fields")

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


class MockGemClient:
    def __init__(self) -> None:
        self.users = [{"id": "user_mock_1", "email": "mock@example.com", "name": "Mock User"}]
        self.projects: dict[str, dict[str, Any]] = {}
        self.project_candidates: dict[str, list[str]] = {}
        self.candidates: dict[str, dict[str, Any]] = {}
        self.notes: dict[str, list[str]] = {}
        self.custom_values: dict[str, dict[str, Any]] = {}

    def resolve_user_id(self, user_id: Optional[str] = None) -> str:
        if user_id and user_id.strip():
            return user_id.strip()
        return "user_mock_1"

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
        for idx, candidate_id in enumerate(self.project_candidates.get(project_id, []), start=1):
            added_at = idx
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
            "metadata": metadata or {},
            "created_at": len(self.projects) + 1,
            "is_archived": False,
        }
        self.projects[project_id] = project
        self.project_candidates.setdefault(project_id, [])
        return {"project_id": project_id, "name": project_name, "user_id": owner_id, "provider_response": project}

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
                "created_at": len(self.projects) + 1,
                "is_archived": False,
            }
            self.project_candidates.setdefault(project_id, [])

        mapping: list[dict[str, str]] = []
        resolved_ids: list[str] = []
        for idx, profile in enumerate(profiles, start=1):
            candidate_id = str(profile.get("candidate_id") or "").strip()
            if not candidate_id:
                candidate_id = f"cand_{idx}_{uuid.uuid4().hex[:6]}"
            candidate = dict(profile)
            candidate["id"] = candidate_id
            candidate.setdefault("created_at", len(self.candidates) + 1)
            self.candidates[candidate_id] = candidate
            resolved_ids.append(candidate_id)
            mapping.append(
                {
                    "source_candidate_id": _source_reference(profile),
                    "gem_candidate_id": candidate_id,
                }
            )

        current = self.project_candidates.setdefault(project_id, [])
        for candidate_id in resolved_ids:
            if candidate_id not in current:
                current.append(candidate_id)

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
        notes.append(note)
        return {
            "candidate_id": candidate_id,
            "note": note,
            "user_id": owner_id,
            "provider_response": {"ok": True},
        }

    def set_custom_value(
        self,
        candidate_id: str,
        key: str,
        value: Any,
        project_id: Optional[str] = None,
    ) -> dict[str, Any]:
        del project_id
        values = self.custom_values.setdefault(candidate_id, {})
        values[key] = value
        return {
            "candidate_id": candidate_id,
            "key": key,
            "custom_field_id": key,
            "value": value,
            "provider_response": {"ok": True},
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
