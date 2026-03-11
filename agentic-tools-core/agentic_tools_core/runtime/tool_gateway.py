from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from agentic_tools_core.errors import PolicyError, RateLimitError, ScopeError, ToolVerificationError
from agentic_tools_core.models import ReceiptStatus, ToolCallContext, WriteReceipt
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

    def execute_write(self, tool_id: str, tool_input: dict[str, Any], context: ToolCallContext) -> dict[str, Any]:
        self._validate_access(tool_id, context, expected_mode="write")
        policy = self.policy_store.get(tool_id)
        idempotency_key = _idempotency_key(context=context, tool_id=tool_id)
        audit_payload = {
            "run_id": context.run_id,
            "step_id": context.step_id,
            "tool_id": tool_id,
            "idempotency_key": idempotency_key,
        }
        ensure_audit_fields(policy, audit_payload)

        existing_receipt_id = self.run_store.find_idempotent_receipt(idempotency_key, tool_input)
        if existing_receipt_id:
            existing = self.run_store.get_receipt_by_id(existing_receipt_id)
            if existing is not None:
                duplicate = WriteReceipt(
                    receipt_id=str(uuid.uuid4()),
                    checkpoint_id="",
                    run_id=context.run_id,
                    action_id=context.step_id,
                    tool_id=tool_id,
                    idempotency_key=idempotency_key,
                    status=ReceiptStatus.DUPLICATE,
                    result={"existing_receipt_id": existing_receipt_id},
                )
                self.run_store.put_receipt(duplicate)
                prior_result = dict(existing.result)
                verification = prior_result.pop("_verification", {})
                return {
                    "output": prior_result,
                    "summary": f"Skipped duplicate execution for {tool_id}.",
                    "verification": verification,
                    "receipt": _receipt_payload(duplicate),
                }

        try:
            preview = self.registry.execute(tool_id=tool_id, tool_input=tool_input, preview=True)
            preview_verification = self.verifier.verify(
                tool_id=tool_id,
                tool_input=tool_input,
                output=preview.output,
                preview=True,
                goal_contract=None,
            )
            if preview_verification.status == "fail":
                raise ToolVerificationError(
                    _verification_failure_message(preview_verification),
                    verification=preview_verification.model_dump(),
                    output=preview.output,
                )

            semaphore = self.rate_control.semaphore(tool_id)
            with semaphore:
                result = self.registry.execute(tool_id=tool_id, tool_input=tool_input, preview=False)
            verification = self.verifier.verify(
                tool_id=tool_id,
                tool_input=tool_input,
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
                checkpoint_id="",
                run_id=context.run_id,
                action_id=context.step_id,
                tool_id=tool_id,
                idempotency_key=idempotency_key,
                status=ReceiptStatus.SUCCESS,
                result=receipt_result,
            )
            self.run_store.put_receipt(receipt)
            self.run_store.remember_idempotency(idempotency_key, receipt.receipt_id, tool_input)
            return {
                "output": result.output,
                "summary": result.summary,
                "verification": verification.model_dump(),
                "receipt": _receipt_payload(receipt),
            }
        except Exception as exc:
            failed_result = {"error": str(exc)}
            if isinstance(exc, ToolVerificationError):
                failed_result["verification"] = exc.verification
                failed_result["output"] = exc.output
            failed = WriteReceipt(
                receipt_id=str(uuid.uuid4()),
                checkpoint_id="",
                run_id=context.run_id,
                action_id=context.step_id,
                tool_id=tool_id,
                idempotency_key=idempotency_key,
                status=ReceiptStatus.FAILED,
                result=failed_result,
            )
            self.run_store.put_receipt(failed)
            raise


def _idempotency_key(*, context: ToolCallContext, tool_id: str) -> str:
    request_id = context.request_id or context.step_id
    return f"{request_id}:{tool_id}"


def _receipt_payload(receipt: WriteReceipt) -> dict[str, Any]:
    return {
        "receipt_id": receipt.receipt_id,
        "tool_id": receipt.tool_id,
        "status": receipt.status.value,
        "idempotency_key": receipt.idempotency_key,
        "created_at": receipt.created_at,
    }


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
