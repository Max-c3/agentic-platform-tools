from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"


class RiskTier(str, Enum):
    LOW = "low"
    HIGH = "high"


class RunRequest(BaseModel):
    objective: str = Field(min_length=3)
    constraints: list[str] = Field(default_factory=list)


class ToolCallContext(BaseModel):
    run_id: str
    step_id: str
    subtask_id: str
    allowed_tool_ids: list[str]


class PlanStep(BaseModel):
    id: str
    name: str
    subtask_id: str
    tool_id: str
    tool_input: dict[str, Any] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)
    is_write: bool = False


class RunPlan(BaseModel):
    steps: list[PlanStep] = Field(default_factory=list)


class WriteAction(BaseModel):
    action_id: str
    run_id: str
    step_id: str
    tool_id: str
    idempotency_key: str
    input_payload: dict[str, Any]
    risk_tier: RiskTier
    summary: str
    preview_output: Optional[dict[str, Any]] = None
    compensation: Optional[dict[str, Any]] = None
    verification: dict[str, Any] = Field(default_factory=dict)


class Checkpoint(BaseModel):
    checkpoint_id: str
    run_id: str
    status: str
    created_at: str = Field(default_factory=utcnow_iso)
    risk_tier: RiskTier
    actions: list[WriteAction] = Field(default_factory=list)


class ReceiptStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    COMPENSATED = "compensated"
    DUPLICATE = "duplicate"


class WriteReceipt(BaseModel):
    receipt_id: str
    checkpoint_id: str
    run_id: str
    action_id: str
    tool_id: str
    idempotency_key: str
    status: ReceiptStatus
    result: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utcnow_iso)


class RunEvent(BaseModel):
    run_id: str
    level: str
    message: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utcnow_iso)


class RunRecord(BaseModel):
    run_id: str
    objective: str
    status: RunStatus
    constraints: list[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=utcnow_iso)
    updated_at: str = Field(default_factory=utcnow_iso)
    error: Optional[str] = None
    plan: Optional[RunPlan] = None
    checkpoints: list[Checkpoint] = Field(default_factory=list)
    report: Optional[dict[str, Any]] = None


class ToolResult(BaseModel):
    output: dict[str, Any] = Field(default_factory=dict)
    summary: str = ""
    compensation: Optional[dict[str, Any]] = None


class ToolDefinition(BaseModel):
    tool_id: str
    display_name: str
    source_path: str
    function_name: str
    owner: str
    version: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    side_effects: str
    approval_class: str
    common_failures: list[str] = Field(default_factory=list)
    examples: list[str] = Field(default_factory=list)
    anti_patterns: list[str] = Field(default_factory=list)
    integration: str
    is_write: bool = False


class VerificationIssue(BaseModel):
    code: str
    severity: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class VerificationResult(BaseModel):
    tool_id: str
    status: str
    preview: bool = False
    issues: list[VerificationIssue] = Field(default_factory=list)
    stats: dict[str, Any] = Field(default_factory=dict)
    retry_hints: list[dict[str, Any]] = Field(default_factory=list)
    goal_impact: dict[str, Any] = Field(default_factory=dict)
