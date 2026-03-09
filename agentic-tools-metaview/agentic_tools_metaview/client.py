from __future__ import annotations

import os
from typing import Any, Optional

import httpx

from agentic_tools_core.integration_clients.exceptions import IntegrationConfigError
from agentic_tools_core.integration_clients.http_client import IntegrationHttpClient


class MetaviewClient:
    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        self.base_url = base_url or os.getenv("METAVIEW_API_BASE_URL", "https://api.metaview.ai")
        self.api_key = api_key or os.getenv("METAVIEW_API_KEY", "")
        auth_header = os.getenv("METAVIEW_AUTH_HEADER", "Authorization")
        auth_scheme = os.getenv("METAVIEW_AUTH_SCHEME", "Bearer")
        timeout_seconds = float(os.getenv("METAVIEW_API_TIMEOUT_SECONDS", "30"))

        self._enrich_path = os.getenv("METAVIEW_ENDPOINT_ENRICH_PROFILES", "/v1/candidates/enrich")

        self.http = IntegrationHttpClient(
            name="metaview",
            base_url=self.base_url,
            api_key=self.api_key,
            auth_header_name=auth_header,
            auth_scheme=auth_scheme,
            timeout_seconds=timeout_seconds,
            transport=transport,
        )

    def enrich_candidate_profiles(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = {"candidates": profiles}
        data = self.http.request("POST", self._enrich_path, json_body=payload)

        raw_candidates: Any
        if isinstance(data, dict):
            raw_candidates = data.get("candidates") or data.get("results") or data.get("data") or []
            if isinstance(raw_candidates, dict):
                raw_candidates = raw_candidates.get("items") or raw_candidates.get("results") or []
        elif isinstance(data, list):
            raw_candidates = data
        else:
            raw_candidates = []

        enriched: list[dict[str, Any]] = []
        for idx, profile in enumerate(profiles):
            remote = raw_candidates[idx] if idx < len(raw_candidates) and isinstance(raw_candidates[idx], dict) else {}
            merged = {
                **profile,
                **remote,
            }
            enriched.append(merged)
        return enriched


class MockMetaviewClient:
    def enrich_candidate_profiles(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                **profile,
                "metaview_signal": "strong_communication" if idx % 2 == 0 else "strong_ownership",
            }
            for idx, profile in enumerate(profiles)
        ]


def build_metaview_client(mode: str):
    if mode == "mock":
        return MockMetaviewClient()
    if mode == "live":
        return MetaviewClient()
    raise IntegrationConfigError("AR_INTEGRATION_MODE must be one of: mock, live")
