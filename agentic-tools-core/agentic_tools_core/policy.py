from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


def _resolve_policy_path(policy_path: Optional[Path]) -> Path:
    if policy_path is not None:
        return Path(policy_path)
    return Path.cwd() / "policy" / "capabilities.yaml"


@dataclass(frozen=True)
class ToolLimit:
    rate_per_minute: int
    batch_size: int
    concurrency: int


@dataclass(frozen=True)
class ToolPolicy:
    tool_id: str
    read_write: str
    allowed_entities: list[str]
    integration: str
    approval_mode: str
    risk_tier: str
    limits: ToolLimit
    forbidden_actions: list[str]
    required_audit_fields: list[str]


class PolicyStore:
    def __init__(self, policy_path: Optional[Path] = None) -> None:
        self.policy_path = _resolve_policy_path(policy_path)
        self._policies = self._load()

    def _load(self) -> dict[str, ToolPolicy]:
        raw = yaml.safe_load(self.policy_path.read_text())
        tools = raw.get("tools", {})
        out: dict[str, ToolPolicy] = {}
        for tool_id, conf in tools.items():
            limits = conf.get("limits", {})
            out[tool_id] = ToolPolicy(
                tool_id=tool_id,
                read_write=conf.get("read_write", "read"),
                allowed_entities=list(conf.get("allowed_entities", [])),
                integration=conf.get("integration", "internal"),
                approval_mode=conf.get("approval_mode", "none"),
                risk_tier=conf.get("risk_tier", "low"),
                limits=ToolLimit(
                    rate_per_minute=int(limits.get("rate_per_minute", 60)),
                    batch_size=int(limits.get("batch_size", 20)),
                    concurrency=int(limits.get("concurrency", 3)),
                ),
                forbidden_actions=list(conf.get("forbidden_actions", [])),
                required_audit_fields=list(conf.get("required_audit_fields", [])),
            )
        return out

    def get(self, tool_id: str) -> ToolPolicy:
        return self._policies[tool_id]

    def has(self, tool_id: str) -> bool:
        return tool_id in self._policies

    def all(self) -> dict[str, ToolPolicy]:
        return dict(self._policies)


def ensure_audit_fields(policy: ToolPolicy, payload: dict[str, Any]) -> None:
    missing = [field for field in policy.required_audit_fields if field not in payload]
    if missing:
        raise ValueError(f"Missing required audit fields: {', '.join(missing)}")
