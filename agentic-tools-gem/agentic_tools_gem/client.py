from __future__ import annotations

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
        self._create_project_path = os.getenv("GEM_ENDPOINT_CREATE_PROJECT", "/v0/projects")
        self._add_profiles_to_project_path = os.getenv(
            "GEM_ENDPOINT_ADD_PROFILES_TO_PROJECT",
            "/v0/projects/{project_id}/candidates",
        )
        self._add_note_path = os.getenv("GEM_ENDPOINT_ADD_NOTE", "/v0/notes")
        self._set_custom_value_path = os.getenv("GEM_ENDPOINT_SET_CUSTOM_VALUE", "/v0/candidates/{candidate_id}")
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

    def create_project(
        self,
        project_name: str,
        metadata: Optional[dict[str, Any]] = None,
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        owner_id = self.resolve_user_id(user_id)
        project_id = f"proj_{uuid.uuid4().hex[:8]}"
        project = {"id": project_id, "name": project_name, "user_id": owner_id, "metadata": metadata or {}}
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
            self.projects[project_id] = {"id": project_id, "name": project_id, "user_id": owner_id}
            self.project_candidates.setdefault(project_id, [])

        mapping: list[dict[str, str]] = []
        resolved_ids: list[str] = []
        for idx, profile in enumerate(profiles, start=1):
            candidate_id = str(profile.get("candidate_id") or "").strip()
            if not candidate_id:
                candidate_id = f"cand_{idx}_{uuid.uuid4().hex[:6]}"
            candidate = dict(profile)
            candidate["id"] = candidate_id
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
