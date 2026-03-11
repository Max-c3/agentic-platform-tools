from __future__ import annotations

from pathlib import Path

import pytest

from agentic_tools_core.models import ToolCallContext, ToolDefinition
from agentic_tools_core.policy import PolicyStore
from agentic_tools_core.registry import ToolRegistry
from agentic_tools_core.run_store import RunStore
from agentic_tools_core.runtime.tool_gateway import ToolGateway


def _write_policy(path: Path, tool_id: str) -> PolicyStore:
    path.write_text(
        f"""tools:
  {tool_id}:
    read_write: write
    allowed_entities: [test]
    integration: test
    approval_mode: none
    risk_tier: low
    limits:
      rate_per_minute: 60
      batch_size: 20
      concurrency: 1
    forbidden_actions: []
    required_audit_fields: [run_id, step_id, tool_id, idempotency_key]
""",
        encoding="utf-8",
    )
    return PolicyStore(path)


def _context(tool_id: str, *, request_id: str = "req-1") -> ToolCallContext:
    return ToolCallContext(
        run_id="run-1",
        step_id=request_id,
        request_id=request_id,
        subtask_id=tool_id,
        allowed_tool_ids=[tool_id],
    )


def test_execute_write_returns_direct_envelope_and_persists_receipt(tmp_path: Path) -> None:
    registry = ToolRegistry()
    calls = {"preview": 0, "live": 0}
    tool_id = "gem.create_project"

    def handler(payload: dict[str, object], preview: bool = False) -> dict[str, object]:
        if preview:
            calls["preview"] += 1
            return {
                "output": {"project_id": "preview_project_123", "name": payload["project_name"]},
                "summary": "Will create project.",
            }
        calls["live"] += 1
        return {
            "output": {"project_id": "proj_123", "name": payload["project_name"]},
            "summary": "Created project.",
        }

    registry.register(
        ToolDefinition(
            tool_id=tool_id,
            display_name="Create Gem Project",
            source_path="tests/test_tool_gateway.py",
            function_name="handler",
            owner="tests",
            version="1.0.0",
            description="Create a project.",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            side_effects="Creates a project.",
            approval_class="none",
            integration="test",
            is_write=True,
        ),
        handler,
    )

    policy_store = _write_policy(tmp_path / "capabilities.yaml", tool_id)
    run_store = RunStore(tmp_path / "runs.db")
    gateway = ToolGateway(policy_store=policy_store, registry=registry, run_store=run_store)

    result = gateway.execute_write(tool_id=tool_id, tool_input={"project_name": "Backend Sprint"}, context=_context(tool_id))

    assert result["output"]["project_id"] == "proj_123"
    assert result["summary"] == "Created project."
    assert result["verification"]["status"] == "pass"
    assert result["receipt"]["status"] == "success"
    assert calls == {"preview": 1, "live": 1}

    receipts = run_store.list_receipts("run-1")
    assert len(receipts) == 1
    assert receipts[0].status.value == "success"


def test_execute_write_deduplicates_same_request_id(tmp_path: Path) -> None:
    registry = ToolRegistry()
    calls = {"preview": 0, "live": 0}
    tool_id = "gem.create_project"

    def handler(payload: dict[str, object], preview: bool = False) -> dict[str, object]:
        if preview:
            calls["preview"] += 1
            return {
                "output": {"project_id": "preview_project_123", "name": payload["project_name"]},
                "summary": "Will create project.",
            }
        calls["live"] += 1
        return {
            "output": {"project_id": "proj_123", "name": payload["project_name"]},
            "summary": "Created project.",
        }

    registry.register(
        ToolDefinition(
            tool_id=tool_id,
            display_name="Create Gem Project",
            source_path="tests/test_tool_gateway.py",
            function_name="handler",
            owner="tests",
            version="1.0.0",
            description="Create a project.",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            side_effects="Creates a project.",
            approval_class="none",
            integration="test",
            is_write=True,
        ),
        handler,
    )

    policy_store = _write_policy(tmp_path / "capabilities.yaml", tool_id)
    run_store = RunStore(tmp_path / "runs.db")
    gateway = ToolGateway(policy_store=policy_store, registry=registry, run_store=run_store)
    context = _context(tool_id)

    first = gateway.execute_write(tool_id=tool_id, tool_input={"project_name": "Backend Sprint"}, context=context)
    second = gateway.execute_write(tool_id=tool_id, tool_input={"project_name": "Backend Sprint"}, context=context)

    assert first["receipt"]["status"] == "success"
    assert second["receipt"]["status"] == "duplicate"
    assert second["output"]["project_id"] == "proj_123"
    assert calls == {"preview": 1, "live": 1}

    receipts = run_store.list_receipts("run-1")
    assert [receipt.status.value for receipt in receipts] == ["success", "duplicate"]


def test_execute_write_persists_failed_receipt_on_preview_verification_error(tmp_path: Path) -> None:
    registry = ToolRegistry()
    tool_id = "gem.create_project"

    def handler(payload: dict[str, object], preview: bool = False) -> dict[str, object]:
        del payload, preview
        return {"output": {}, "summary": "bad preview"}

    registry.register(
        ToolDefinition(
            tool_id=tool_id,
            display_name="Create Gem Project",
            source_path="tests/test_tool_gateway.py",
            function_name="handler",
            owner="tests",
            version="1.0.0",
            description="Create a project.",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            side_effects="Creates a project.",
            approval_class="none",
            integration="test",
            is_write=True,
        ),
        handler,
    )

    policy_store = _write_policy(tmp_path / "capabilities.yaml", tool_id)
    run_store = RunStore(tmp_path / "runs.db")
    gateway = ToolGateway(policy_store=policy_store, registry=registry, run_store=run_store)

    with pytest.raises(Exception):
        gateway.execute_write(tool_id=tool_id, tool_input={"project_name": "Backend Sprint"}, context=_context(tool_id))

    receipts = run_store.list_receipts("run-1")
    assert len(receipts) == 1
    assert receipts[0].status.value == "failed"
    assert receipts[0].result["verification"]["status"] == "fail"


def test_execute_write_persists_failed_receipt_on_provider_error(tmp_path: Path) -> None:
    registry = ToolRegistry()
    calls = {"preview": 0, "live": 0}
    tool_id = "gem.create_project"

    def handler(payload: dict[str, object], preview: bool = False) -> dict[str, object]:
        if preview:
            calls["preview"] += 1
            return {
                "output": {"project_id": "preview_project_123", "name": payload["project_name"]},
                "summary": "Will create project.",
            }
        calls["live"] += 1
        raise RuntimeError("provider exploded")

    registry.register(
        ToolDefinition(
            tool_id=tool_id,
            display_name="Create Gem Project",
            source_path="tests/test_tool_gateway.py",
            function_name="handler",
            owner="tests",
            version="1.0.0",
            description="Create a project.",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            side_effects="Creates a project.",
            approval_class="none",
            integration="test",
            is_write=True,
        ),
        handler,
    )

    policy_store = _write_policy(tmp_path / "capabilities.yaml", tool_id)
    run_store = RunStore(tmp_path / "runs.db")
    gateway = ToolGateway(policy_store=policy_store, registry=registry, run_store=run_store)

    with pytest.raises(RuntimeError, match="provider exploded"):
        gateway.execute_write(tool_id=tool_id, tool_input={"project_name": "Backend Sprint"}, context=_context(tool_id))

    receipts = run_store.list_receipts("run-1")
    assert len(receipts) == 1
    assert receipts[0].status.value == "failed"
    assert "provider exploded" in receipts[0].result["error"]
    assert calls == {"preview": 1, "live": 1}
