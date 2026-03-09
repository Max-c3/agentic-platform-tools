from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass
from typing import Any

from agentic_tools_core.errors import PolicyError, RateLimitError, ScopeError, ToolVerificationError
from agentic_tools_core.models import Checkpoint, ReceiptStatus, RiskTier, ToolCallContext, WriteAction, WriteReceipt
from agentic_tools_core.policy import PolicyStore, ensure_audit_fields
from agentic_tools_core.run_store import RunStore
from agentic_tools_core.runtime.rate_control import RateController
from agentic_tools_core.runtime.tool_verifier import ToolOutputVerifier
from agentic_tools_core.registry import ToolRegistry


@dataclass
class StepScope:
    subtask_id: str
    allowed_tool_ids: list[str]


class ToolGateway:
    def __init__(self, policy_store: PolicyStore, registry: ToolRegistry, run_store: RunStore) -> None:
        self.policy_store = policy_store
        self.registry = registry
        self.run_store = run_store
        self.rate_control = RateController()
        self.verifier = ToolOutputVerifier()
        self._configure_controls()

    def _configure_controls(self) -> None:
        for tool_id, policy in self.policy_store.all().items():
            self.rate_control.configure_tool(
                tool_id=tool_id,
                rate_per_minute=policy.limits.rate_per_minute,
                concurrency=policy.limits.concurrency,
            )

    def _validate_access(self, tool_id: str, context: ToolCallContext, expected_mode: str) -> None:
        if tool_id not in context.allowed_tool_ids:
            raise ScopeError(f"Tool '{tool_id}' is not allowed for subtask '{context.subtask_id}'")
        if not self.policy_store.has(tool_id):
            raise PolicyError(f"Tool '{tool_id}' missing from capabilities policy")
        policy = self.policy_store.get(tool_id)
        if policy.read_write != expected_mode:
            raise PolicyError(f"Tool '{tool_id}' expected mode '{expected_mode}' but policy says '{policy.read_write}'")
        if not self.rate_control.allow(tool_id):
            raise RateLimitError(f"Tool '{tool_id}' exceeded token bucket limit")

    def execute_read(
        self,
        tool_id: str,
        tool_input: dict[str, Any],
        context: ToolCallContext,
        goal_contract: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_access(tool_id, context, expected_mode="read")
        policy = self.policy_store.get(tool_id)
        audit_payload = {
            "run_id": context.run_id,
            "step_id": context.step_id,
            "tool_id": tool_id,
        }
        ensure_audit_fields(policy, audit_payload)

        semaphore = self.rate_control.semaphore(tool_id)
        with semaphore:
            result = self.registry.execute(tool_id=tool_id, tool_input=tool_input, preview=False)
        verification = self.verifier.verify(
            tool_id=tool_id,
            tool_input=tool_input,
            output=result.output,
            preview=False,
            goal_contract=goal_contract,
        )
        if verification.status == "fail":
            raise ToolVerificationError(
                _verification_failure_message(verification),
                verification=verification.model_dump(),
                output=result.output,
            )
        payload = result.model_dump()
        payload["verification"] = verification.model_dump()
        return payload

    def stage_write(self, tool_id: str, tool_input: dict[str, Any], context: ToolCallContext) -> WriteAction:
        self._validate_access(tool_id, context, expected_mode="write")
        policy = self.policy_store.get(tool_id)
        preview = self.registry.execute(tool_id=tool_id, tool_input=tool_input, preview=True)
        verification = self.verifier.verify(
            tool_id=tool_id,
            tool_input=tool_input,
            output=preview.output,
            preview=True,
            goal_contract=None,
        )
        if verification.status == "fail":
            raise ToolVerificationError(
                _verification_failure_message(verification),
                verification=verification.model_dump(),
                output=preview.output,
            )
        action = WriteAction(
            action_id=str(uuid.uuid4()),
            run_id=context.run_id,
            step_id=context.step_id,
            tool_id=tool_id,
            idempotency_key=f"{context.run_id}:{context.step_id}:{tool_id}",
            input_payload=tool_input,
            risk_tier=RiskTier(policy.risk_tier),
            summary=preview.summary,
            preview_output=preview.output,
            compensation=preview.compensation,
            verification=verification.model_dump(),
        )
        return action

    def execute_checkpoint(self, checkpoint: Checkpoint) -> list[WriteReceipt]:
        receipts: list[WriteReceipt] = []
        succeeded_receipts: list[WriteReceipt] = []
        replacements: dict[str, Any] = {}

        for action in checkpoint.actions:
            resolved_payload = _apply_replacements(action.input_payload, replacements)
            policy = self.policy_store.get(action.tool_id)
            audit_payload = {
                "run_id": action.run_id,
                "step_id": action.step_id,
                "tool_id": action.tool_id,
                "idempotency_key": action.idempotency_key,
            }
            ensure_audit_fields(policy, audit_payload)

            existing_receipt_id = self.run_store.find_idempotent_receipt(action.idempotency_key, resolved_payload)
            if existing_receipt_id:
                existing = self.run_store.get_receipt_by_id(existing_receipt_id)
                if existing is not None:
                    duplicate = WriteReceipt(
                        receipt_id=str(uuid.uuid4()),
                        checkpoint_id=checkpoint.checkpoint_id,
                        run_id=checkpoint.run_id,
                        action_id=action.action_id,
                        tool_id=action.tool_id,
                        idempotency_key=action.idempotency_key,
                        status=ReceiptStatus.DUPLICATE,
                        result={"existing_receipt_id": existing_receipt_id},
                    )
                    self.run_store.put_receipt(duplicate)
                    receipts.append(duplicate)
                    if existing.status == ReceiptStatus.SUCCESS:
                        _collect_replacements(action.preview_output or {}, existing.result, replacements)
                    continue

            try:
                semaphore = self.rate_control.semaphore(action.tool_id)
                with semaphore:
                    result = self.registry.execute(tool_id=action.tool_id, tool_input=resolved_payload, preview=False)
                verification = self.verifier.verify(
                    tool_id=action.tool_id,
                    tool_input=resolved_payload,
                    output=result.output,
                    preview=False,
                    goal_contract=None,
                )
                if verification.status == "fail":
                    raise ToolVerificationError(
                        _verification_failure_message(verification),
                        verification=verification.model_dump(),
                        output=result.output,
                    )
                receipt_result = dict(result.output)
                receipt_result["_verification"] = verification.model_dump()
                receipt = WriteReceipt(
                    receipt_id=str(uuid.uuid4()),
                    checkpoint_id=checkpoint.checkpoint_id,
                    run_id=checkpoint.run_id,
                    action_id=action.action_id,
                    tool_id=action.tool_id,
                    idempotency_key=action.idempotency_key,
                    status=ReceiptStatus.SUCCESS,
                    result=receipt_result,
                )
                self.run_store.put_receipt(receipt)
                self.run_store.remember_idempotency(action.idempotency_key, receipt.receipt_id, resolved_payload)
                receipts.append(receipt)
                succeeded_receipts.append(receipt)
                _collect_replacements(action.preview_output or {}, result.output, replacements)
            except Exception as exc:
                failed = WriteReceipt(
                    receipt_id=str(uuid.uuid4()),
                    checkpoint_id=checkpoint.checkpoint_id,
                    run_id=checkpoint.run_id,
                    action_id=action.action_id,
                    tool_id=action.tool_id,
                    idempotency_key=action.idempotency_key,
                    status=ReceiptStatus.FAILED,
                    result={"error": str(exc)},
                )
                self.run_store.put_receipt(failed)
                receipts.append(failed)
                receipts.extend(self._compensate(checkpoint, succeeded_receipts))
                break

        return receipts

    def _compensate(self, checkpoint: Checkpoint, succeeded_receipts: list[WriteReceipt]) -> list[WriteReceipt]:
        compensated: list[WriteReceipt] = []
        for prior in reversed(succeeded_receipts):
            receipt = WriteReceipt(
                receipt_id=str(uuid.uuid4()),
                checkpoint_id=checkpoint.checkpoint_id,
                run_id=checkpoint.run_id,
                action_id=prior.action_id,
                tool_id=prior.tool_id,
                idempotency_key=prior.idempotency_key,
                status=ReceiptStatus.COMPENSATED,
                result={"message": "Logical compensation recorded"},
            )
            self.run_store.put_receipt(receipt)
            compensated.append(receipt)
        return compensated


def _collect_replacements(preview: Any, actual: Any, replacements: dict[str, Any]) -> None:
    if isinstance(preview, dict) and isinstance(actual, dict):
        for key in preview.keys() & actual.keys():
            _collect_replacements(preview[key], actual[key], replacements)
        return
    if isinstance(preview, list) and isinstance(actual, list):
        for p_item, a_item in zip(preview, actual):
            _collect_replacements(p_item, a_item, replacements)
        return
    if isinstance(preview, str) and preview and preview != str(actual):
        replacements[preview] = actual


def _apply_replacements(value: Any, replacements: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {key: _apply_replacements(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [_apply_replacements(item, replacements) for item in value]
    if isinstance(value, str) and value in replacements:
        return replacements[value]
    return value


def _verification_failure_message(verification: Any) -> str:
    issues = verification.issues if hasattr(verification, "issues") else []
    first = issues[0] if issues else None
    first_message = f"{first.code}: {first.message}" if first else "unknown verification failure"
    hints = verification.retry_hints if hasattr(verification, "retry_hints") else []
    hint_text = ""
    if hints:
        first_hint = hints[0]
        if isinstance(first_hint, dict):
            patch = first_hint.get("suggested_input_patch", {})
            hint_text = f" Retry hint: {json.dumps(patch, sort_keys=True)}."
    return f"Tool output verification failed for {verification.tool_id}: {first_message}.{hint_text}"
