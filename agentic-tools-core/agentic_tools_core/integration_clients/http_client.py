from __future__ import annotations

from typing import Any, Optional

import httpx

from agentic_tools_core.integration_clients.exceptions import IntegrationConfigError, IntegrationRequestError


class IntegrationHttpClient:
    def __init__(
        self,
        *,
        name: str,
        base_url: str,
        api_key: str,
        auth_mode: str = "header",
        auth_header_name: str = "Authorization",
        auth_scheme: str = "Bearer",
        timeout_seconds: float = 30.0,
        transport: Optional[httpx.BaseTransport] = None,
        static_headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.auth_mode = auth_mode.strip().lower()
        self.auth_header_name = auth_header_name
        self.auth_scheme = auth_scheme
        self.static_headers = static_headers or {}

        if not self.base_url:
            raise IntegrationConfigError(f"{name}: base URL is required")
        if not self.api_key:
            raise IntegrationConfigError(f"{name}: API key is required")
        if self.auth_mode not in {"header", "basic"}:
            raise IntegrationConfigError(f"{name}: unsupported auth_mode '{self.auth_mode}'")

        self.client = httpx.Client(timeout=timeout_seconds, transport=transport)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_headers = self._build_headers()
        if headers:
            request_headers.update(headers)
        auth = httpx.BasicAuth(self.api_key, "") if self.auth_mode == "basic" else None

        response = self.client.request(
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
                f"{self.name}: {method} {url} failed with {response.status_code}: {response.text[:300]}",
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

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.auth_mode == "header":
            auth_value = self.api_key if not self.auth_scheme else f"{self.auth_scheme} {self.api_key}"
            headers[self.auth_header_name] = auth_value
        headers.update(self.static_headers)
        return headers
