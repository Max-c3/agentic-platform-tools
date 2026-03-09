from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from hashlib import sha256
from pathlib import Path
from typing import Any, Optional

from agentic_tools_core.models import Checkpoint, RunEvent, RunPlan, RunRecord, RunRequest, RunStatus, WriteReceipt, utcnow_iso


def _default_db_path() -> Path:
    configured = os.getenv("AGENTIC_TOOLS_DB_PATH", "").strip()
    if configured:
        target = Path(configured)
    else:
        target = Path.cwd() / ".agentic-tools" / "runs.db"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


class RunStore:
    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = Path(db_path) if db_path is not None else _default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._ensure_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    objective TEXT NOT NULL,
                    constraints_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    plan_json TEXT,
                    report_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    checkpoint_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    risk_tier TEXT NOT NULL,
                    actions_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS receipts (
                    receipt_id TEXT PRIMARY KEY,
                    checkpoint_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    tool_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS idempotency (
                    idempotency_key TEXT PRIMARY KEY,
                    receipt_id TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    path TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS session_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def create_run(self, req: RunRequest) -> RunRecord:
        run = RunRecord(
            run_id=str(uuid.uuid4()),
            objective=req.objective,
            constraints=req.constraints,
            status=RunStatus.PENDING,
        )
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO runs (run_id, objective, constraints_json, status, error, plan_json, report_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                """,
                (
                    run.run_id,
                    run.objective,
                    json.dumps(run.constraints),
                    run.status.value,
                    run.created_at,
                    run.updated_at,
                ),
            )
            conn.commit()
        return run

    def get_run(self, run_id: str) -> Optional[RunRecord]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            checkpoints = self.list_checkpoints(run_id)
            plan = RunPlan.model_validate(json.loads(row["plan_json"])) if row["plan_json"] else None
            report = json.loads(row["report_json"]) if row["report_json"] else None
            return RunRecord(
                run_id=row["run_id"],
                objective=row["objective"],
                constraints=json.loads(row["constraints_json"]),
                status=RunStatus(row["status"]),
                error=row["error"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                plan=plan,
                checkpoints=checkpoints,
                report=report,
            )

    def set_status(self, run_id: str, status: RunStatus, error: Optional[str] = None) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                "UPDATE runs SET status = ?, error = ?, updated_at = ? WHERE run_id = ?",
                (status.value, error, utcnow_iso(), run_id),
            )
            conn.commit()

    def save_plan(self, run_id: str, plan: RunPlan) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                "UPDATE runs SET plan_json = ?, updated_at = ? WHERE run_id = ?",
                (plan.model_dump_json(), utcnow_iso(), run_id),
            )
            conn.commit()

    def save_report(self, run_id: str, report: dict[str, Any]) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                "UPDATE runs SET report_json = ?, updated_at = ? WHERE run_id = ?",
                (json.dumps(report), utcnow_iso(), run_id),
            )
            conn.commit()

    def add_event(self, event: RunEvent) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO events (run_id, level, message, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.run_id,
                    event.level,
                    event.message,
                    json.dumps(event.payload),
                    event.created_at,
                ),
            )
            conn.commit()

    def list_events(self, run_id: str) -> list[RunEvent]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT run_id, level, message, payload_json, created_at FROM events WHERE run_id = ? ORDER BY event_id ASC",
                (run_id,),
            ).fetchall()
        return [
            RunEvent(
                run_id=row["run_id"],
                level=row["level"],
                message=row["message"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def put_checkpoint(self, checkpoint: Checkpoint) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO checkpoints (checkpoint_id, run_id, status, risk_tier, actions_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    checkpoint.checkpoint_id,
                    checkpoint.run_id,
                    checkpoint.status,
                    checkpoint.risk_tier.value,
                    checkpoint.model_dump_json(exclude={"run_id", "checkpoint_id", "status", "created_at", "risk_tier"}),
                    checkpoint.created_at,
                ),
            )
            conn.commit()

    def list_checkpoints(self, run_id: str) -> list[Checkpoint]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints WHERE run_id = ? ORDER BY created_at ASC",
                (run_id,),
            ).fetchall()
        out: list[Checkpoint] = []
        for row in rows:
            payload = json.loads(row["actions_json"])
            out.append(
                Checkpoint(
                    checkpoint_id=row["checkpoint_id"],
                    run_id=row["run_id"],
                    status=row["status"],
                    created_at=row["created_at"],
                    risk_tier=row["risk_tier"],
                    actions=payload.get("actions", []),
                )
            )
        return out

    def list_all_checkpoints(self, status: Optional[str] = None) -> list[Checkpoint]:
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM checkpoints WHERE status = ? ORDER BY created_at ASC",
                    (status,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM checkpoints ORDER BY created_at ASC").fetchall()
        out: list[Checkpoint] = []
        for row in rows:
            payload = json.loads(row["actions_json"])
            out.append(
                Checkpoint(
                    checkpoint_id=row["checkpoint_id"],
                    run_id=row["run_id"],
                    status=row["status"],
                    created_at=row["created_at"],
                    risk_tier=row["risk_tier"],
                    actions=payload.get("actions", []),
                )
            )
        return out

    def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM checkpoints WHERE checkpoint_id = ?", (checkpoint_id,)).fetchone()
            if row is None:
                return None
            payload = json.loads(row["actions_json"])
            return Checkpoint(
                checkpoint_id=row["checkpoint_id"],
                run_id=row["run_id"],
                status=row["status"],
                created_at=row["created_at"],
                risk_tier=row["risk_tier"],
                actions=payload.get("actions", []),
            )

    def update_checkpoint_status(self, checkpoint_id: str, status: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute("UPDATE checkpoints SET status = ? WHERE checkpoint_id = ?", (status, checkpoint_id))
            conn.commit()

    def put_receipt(self, receipt: WriteReceipt) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO receipts (receipt_id, checkpoint_id, run_id, action_id, tool_id, idempotency_key, status, result_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt.receipt_id,
                    receipt.checkpoint_id,
                    receipt.run_id,
                    receipt.action_id,
                    receipt.tool_id,
                    receipt.idempotency_key,
                    receipt.status.value,
                    json.dumps(receipt.result),
                    receipt.created_at,
                ),
            )
            conn.commit()

    def list_receipts(self, run_id: str) -> list[WriteReceipt]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM receipts WHERE run_id = ? ORDER BY created_at ASC", (run_id,)).fetchall()
        return [
            WriteReceipt(
                receipt_id=row["receipt_id"],
                checkpoint_id=row["checkpoint_id"],
                run_id=row["run_id"],
                action_id=row["action_id"],
                tool_id=row["tool_id"],
                idempotency_key=row["idempotency_key"],
                status=row["status"],
                result=json.loads(row["result_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def list_receipts_for_checkpoint(self, checkpoint_id: str) -> list[WriteReceipt]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM receipts WHERE checkpoint_id = ? ORDER BY created_at ASC",
                (checkpoint_id,),
            ).fetchall()
        return [
            WriteReceipt(
                receipt_id=row["receipt_id"],
                checkpoint_id=row["checkpoint_id"],
                run_id=row["run_id"],
                action_id=row["action_id"],
                tool_id=row["tool_id"],
                idempotency_key=row["idempotency_key"],
                status=row["status"],
                result=json.loads(row["result_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def find_idempotent_receipt(self, idempotency_key: str, payload: dict[str, Any]) -> Optional[str]:
        payload_hash = sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT receipt_id, payload_hash FROM idempotency WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row is None:
                return None
            if row["payload_hash"] != payload_hash:
                raise ValueError("Idempotency key reuse with different payload")
            return row["receipt_id"]

    def remember_idempotency(self, idempotency_key: str, receipt_id: str, payload: dict[str, Any]) -> None:
        payload_hash = sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        with self._lock, self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO idempotency (idempotency_key, receipt_id, payload_hash) VALUES (?, ?, ?)",
                (idempotency_key, receipt_id, payload_hash),
            )
            conn.commit()

    def get_receipt_by_id(self, receipt_id: str) -> Optional[WriteReceipt]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)).fetchone()
            if row is None:
                return None
            return WriteReceipt(
                receipt_id=row["receipt_id"],
                checkpoint_id=row["checkpoint_id"],
                run_id=row["run_id"],
                action_id=row["action_id"],
                tool_id=row["tool_id"],
                idempotency_key=row["idempotency_key"],
                status=row["status"],
                result=json.loads(row["result_json"]),
                created_at=row["created_at"],
            )

    def put_artifact(self, run_id: str, kind: str, path: str, metadata: dict[str, Any]) -> str:
        artifact_id = str(uuid.uuid4())
        with self._lock, self._conn() as conn:
            conn.execute(
                "INSERT INTO artifacts (artifact_id, run_id, kind, path, metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (artifact_id, run_id, kind, path, json.dumps(metadata), utcnow_iso()),
            )
            conn.commit()
        return artifact_id

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM artifacts WHERE run_id = ? ORDER BY created_at ASC", (run_id,)).fetchall()
        return [
            {
                "artifact_id": row["artifact_id"],
                "run_id": row["run_id"],
                "kind": row["kind"],
                "path": row["path"],
                "metadata": json.loads(row["metadata_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def add_session_log(
        self,
        run_id: str,
        channel: str,
        message: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO session_logs (run_id, channel, message, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    channel,
                    message,
                    json.dumps(payload or {}),
                    utcnow_iso(),
                ),
            )
            conn.commit()

    def list_session_logs(self, run_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT run_id, channel, message, payload_json, created_at
                FROM session_logs
                WHERE run_id = ?
                ORDER BY log_id ASC
                """,
                (run_id,),
            ).fetchall()
        return [
            {
                "run_id": row["run_id"],
                "channel": row["channel"],
                "message": row["message"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]
