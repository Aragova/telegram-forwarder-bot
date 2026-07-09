from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

from app.postgres_repository import PostgresRepository
from app.repository import RepositoryProtocol


class _PlacementCursor:
    def __init__(self, runs: list[dict[str, Any]], messages: list[dict[str, Any]]):
        self._runs = runs
        self._messages = messages
        self._rows: list[dict[str, Any]] = []
        self._row: dict[str, Any] | None = None
        self.last_sql = ""
        self.last_params: tuple[Any, ...] | None = None
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None):
        self.last_sql = sql
        self.last_params = params
        if "UPDATE campaign_run_messages" in sql and "manual_resolved_orphaned_target" in sql:
            run_id = int(params[0]) if params else 0
            rule_id = int(params[1]) if params and len(params) > 1 else 0
            changed = 0
            for message in self._messages:
                message_rule_id = int(message.get("rule_id") or next((run.get("rule_id") for run in self._runs if int(run["id"]) == int(message["run_id"])), 0))
                if int(message.get("run_id") or 0) == run_id and message_rule_id == rule_id and message.get("send_status") == "sent" and message.get("delete_status") == "failed":
                    message["delete_status"] = "deleted"
                    message["delete_error_text"] = "[manual_resolved_orphaned_target] " + str(message.get("delete_error_text") or "")
                    changed += 1
            self.rowcount = changed
            self._row = None
            self._rows = []
            return
        if "remaining_pending" in sql and "remaining_processing" in sql and "remaining_failed" in sql:
            run_id = int(params[0]) if params else 0
            rule_id = int(params[1]) if params and len(params) > 1 else 0
            scoped = []
            for message in self._messages:
                message_rule_id = int(message.get("rule_id") or next((run.get("rule_id") for run in self._runs if int(run["id"]) == int(message["run_id"])), 0))
                if int(message.get("run_id") or 0) == run_id and message_rule_id == rule_id and message.get("send_status") == "sent":
                    scoped.append(message)
            self._row = {
                "remaining_pending": sum(1 for m in scoped if m.get("delete_status") == "pending"),
                "remaining_processing": sum(1 for m in scoped if m.get("delete_status") == "processing"),
                "remaining_failed": sum(1 for m in scoped if m.get("delete_status") == "failed"),
            }
            self._rows = []
            return
        rule_id = int(params[0]) if params else 0
        is_summary = "WITH placements AS" in sql
        limit = None if is_summary else (int(params[-1]) if params else 20)
        basic_only = "r.scheduled_post_id IS NULL" in sql
        rows: list[dict[str, Any]] = []

        for run in self._runs:
            if int(run["rule_id"]) != rule_id:
                continue
            if basic_only and run.get("scheduled_post_id") is not None:
                continue
            if basic_only and run.get("run_type") not in {"manual", "scheduled"}:
                continue

            active_messages = [
                message
                for message in self._messages
                if int(message["run_id"]) == int(run["id"])
                and message.get("send_status") == "sent"
                and message.get("delete_status") in {"pending", "processing", "failed"}
            ]
            if not active_messages:
                continue

            delete_after_values = [m.get("delete_after_at") for m in active_messages if m.get("delete_after_at") is not None]
            sent_at_values = [m.get("sent_at") for m in active_messages if m.get("sent_at") is not None]
            rows.append(
                {
                    "run_id": run["id"],
                    "rule_id": run["rule_id"],
                    "saved_post_id": run.get("saved_post_id", 100 + int(run["id"])),
                    "run_type": run.get("run_type", "manual"),
                    "run_status": run.get("status", "sent"),
                    "scheduled_post_id": run.get("scheduled_post_id"),
                    "created_at": run.get("created_at"),
                    "started_at": run.get("started_at"),
                    "finished_at": run.get("finished_at"),
                    "targets_total": run.get("targets_total", len(active_messages)),
                    "targets_success": run.get("targets_success", len(active_messages)),
                    "targets_failed": run.get("targets_failed", 0),
                    "sent": len(active_messages),
                    "delete_pending": sum(1 for m in active_messages if m.get("delete_status") == "pending"),
                    "delete_processing": sum(1 for m in active_messages if m.get("delete_status") == "processing"),
                    "delete_failed": sum(1 for m in active_messages if m.get("delete_status") == "failed"),
                    "delete_after_at_min": min(delete_after_values) if delete_after_values else None,
                    "delete_after_at_max": max(delete_after_values) if delete_after_values else None,
                    "last_sent_at": max(sent_at_values) if sent_at_values else None,
                }
            )

        def _sort_key(row: dict[str, Any]):
            problem_rank = 0 if int(row.get("delete_failed") or 0) > 0 else 1
            delete_after = row.get("delete_after_at_min")
            null_rank = 1 if delete_after is None else 0
            return (problem_rank, null_rank, delete_after or "", -int(row["run_id"]))

        sorted_rows = sorted(rows, key=_sort_key)
        if is_summary:
            self._row = {
                "placements_total": len(sorted_rows),
                "active_total": sum(1 for row in sorted_rows if int(row.get("delete_pending") or 0) + int(row.get("delete_processing") or 0) > 0),
                "delete_problem_total": sum(1 for row in sorted_rows if int(row.get("delete_failed") or 0) > 0),
                "mixed_total": sum(1 for row in sorted_rows if int(row.get("delete_failed") or 0) > 0 and int(row.get("delete_pending") or 0) + int(row.get("delete_processing") or 0) > 0),
                "delete_pending_total": sum(int(row.get("delete_pending") or 0) for row in sorted_rows),
                "delete_processing_total": sum(int(row.get("delete_processing") or 0) for row in sorted_rows),
                "delete_failed_total": sum(int(row.get("delete_failed") or 0) for row in sorted_rows),
            }
            self._rows = []
            return
        self._row = None
        self._rows = sorted_rows[:limit]

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


class _PlacementConn:
    def __init__(self, runs: list[dict[str, Any]], messages: list[dict[str, Any]]):
        self._cursor = _PlacementCursor(runs, messages)

    def cursor(self):
        return self._cursor

    def commit(self):
        return None


def _repo_with_data(runs: list[dict[str, Any]], messages: list[dict[str, Any]]) -> tuple[PostgresRepository, dict[str, _PlacementConn | None]]:
    repo = PostgresRepository()
    captured: dict[str, _PlacementConn | None] = {"conn": None}

    @contextmanager
    def _connect():
        conn = _PlacementConn(runs, messages)
        captured["conn"] = conn
        yield conn

    repo.connect = _connect  # type: ignore[method-assign]
    return repo, captured


def _run(run_id: int, *, rule_id: int = 10, run_type: str = "manual", scheduled_post_id: int | None = None) -> dict[str, Any]:
    return {
        "id": run_id,
        "rule_id": rule_id,
        "saved_post_id": 1000 + run_id,
        "run_type": run_type,
        "status": "sent",
        "scheduled_post_id": scheduled_post_id,
        "created_at": f"2026-05-01T00:0{run_id}:00+00:00",
        "started_at": f"2026-05-01T00:0{run_id}:01+00:00",
        "finished_at": f"2026-05-01T00:0{run_id}:10+00:00",
        "targets_total": 1,
        "targets_success": 1,
        "targets_failed": 0,
    }


def _message(message_id: int, run_id: int, delete_status: str, *, send_status: str = "sent", delete_after_at: str | None = "2026-05-01T01:00:00+00:00", rule_id: int | None = None) -> dict[str, Any]:
    return {
        "id": message_id,
        "run_id": run_id,
        "send_status": send_status,
        "delete_status": delete_status,
        "delete_after_at": delete_after_at,
        "sent_at": f"2026-05-01T00:{message_id:02d}:00+00:00",
        **({"rule_id": rule_id} if rule_id is not None else {}),
    }


def test_repository_protocol_exposes_active_placement_methods():
    assert hasattr(RepositoryProtocol, "list_active_campaign_placements_for_rule")
    assert hasattr(RepositoryProtocol, "get_active_campaign_placements_summary_for_rule")
    assert hasattr(PostgresRepository, "list_active_campaign_placements_for_rule")
    assert hasattr(PostgresRepository, "get_active_campaign_placements_summary_for_rule")


def test_one_active_manual_run_returns_active_pending_placement():
    repo, _ = _repo_with_data([_run(1, run_type="manual")], [_message(1, 1, "pending")])

    placements = repo.list_active_campaign_placements_for_rule(10)

    assert len(placements) == 1
    assert placements[0]["placement_status"] == "active"
    assert placements[0]["delete_pending"] == 1
    assert placements[0]["delete_failed"] == 0
    assert placements[0]["active_messages_total"] == 1


def test_scheduled_basic_run_is_included_with_processing_delete():
    repo, _ = _repo_with_data([_run(1, run_type="scheduled")], [_message(1, 1, "processing")])

    placements = repo.list_active_campaign_placements_for_rule(10, basic_only=True)

    assert len(placements) == 1
    assert placements[0]["run_type"] == "scheduled"
    assert placements[0]["placement_status"] == "active"
    assert placements[0]["delete_processing"] == 1


def test_delete_failed_is_problem_placement_and_summary_counts_it():
    repo, _ = _repo_with_data([_run(1, run_type="manual")], [_message(1, 1, "failed")])

    placements = repo.list_active_campaign_placements_for_rule(10)
    summary = repo.get_active_campaign_placements_summary_for_rule(10)

    assert placements[0]["placement_status"] == "delete_problem"
    assert placements[0]["delete_failed"] == 1
    assert summary["delete_problem_total"] == 1
    assert summary["delete_failed_total"] == 1


def test_mixed_status_counts_as_active_and_problem():
    repo, _ = _repo_with_data(
        [_run(1, run_type="manual")],
        [_message(1, 1, "pending"), _message(2, 1, "failed")],
    )

    placements = repo.list_active_campaign_placements_for_rule(10)
    summary = repo.get_active_campaign_placements_summary_for_rule(10)

    assert placements[0]["placement_status"] == "mixed"
    assert summary["active_total"] == 1
    assert summary["delete_problem_total"] == 1
    assert summary["mixed_total"] == 1


def test_fully_deleted_run_is_not_returned():
    repo, _ = _repo_with_data([_run(1, run_type="manual")], [_message(1, 1, "deleted")])

    assert repo.list_active_campaign_placements_for_rule(10) == []


def test_vip_scheduled_post_is_excluded_only_for_basic_placements():
    repo, basic = _repo_with_data([_run(1, run_type="scheduled", scheduled_post_id=55)], [_message(1, 1, "pending")])

    assert repo.list_active_campaign_placements_for_rule(10, basic_only=True) == []
    basic_sql = basic["conn"]._cursor.last_sql  # type: ignore[union-attr]

    placements = repo.list_active_campaign_placements_for_rule(10, basic_only=False)
    all_sql = basic["conn"]._cursor.last_sql  # type: ignore[union-attr]

    assert len(placements) == 1
    assert "r.scheduled_post_id IS NULL" in basic_sql
    assert "r.run_type IN ('manual', 'scheduled')" in basic_sql
    assert "r.scheduled_post_id IS NULL" not in all_sql


def test_multiple_active_placements_are_returned_with_problem_first_and_summary_total():
    repo, _ = _repo_with_data(
        [_run(1), _run(2), _run(3)],
        [
            _message(1, 1, "pending", delete_after_at="2026-05-01T01:00:00+00:00"),
            _message(2, 2, "failed", delete_after_at="2026-05-02T01:00:00+00:00"),
            _message(3, 3, "processing", delete_after_at="2026-05-01T00:30:00+00:00"),
        ],
    )

    placements = repo.list_active_campaign_placements_for_rule(10)
    summary = repo.get_active_campaign_placements_summary_for_rule(10)

    assert [row["run_id"] for row in placements] == [2, 3, 1]
    assert placements[0]["placement_status"] == "delete_problem"
    assert summary["placements_total"] == 3


def test_summary_uses_unlimited_aggregate_sql_not_limited_list():
    repo, captured = _repo_with_data(
        [_run(run_id) for run_id in range(1, 4)],
        [_message(run_id, run_id, "pending") for run_id in range(1, 4)],
    )

    summary = repo.get_active_campaign_placements_summary_for_rule(10)
    sql = captured["conn"]._cursor.last_sql  # type: ignore[union-attr]
    params = captured["conn"]._cursor.last_params  # type: ignore[union-attr]

    assert summary["placements_total"] == 3
    assert "WITH placements AS" in sql
    assert "LIMIT" not in sql
    assert params == (10,)


def test_active_placements_indexes_are_created_safely_in_init_sql():
    sql = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "CREATE INDEX IF NOT EXISTS idx_campaign_run_messages_active_by_rule" in sql
    assert "ON campaign_run_messages(rule_id, delete_status, send_status, delete_after_at)" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_campaign_run_messages_active_by_run" in sql
    assert "ON campaign_run_messages(run_id, delete_status, send_status)" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_campaign_runs_rule_basic" in sql
    assert "ON campaign_runs(rule_id, scheduled_post_id, run_type, created_at DESC)" in sql


def test_scheduled_post_service_does_not_use_clean_channel_active_placements_yet():
    source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")

    assert "list_active_campaign_placements_for_rule" not in source
    assert "clean_channel" not in source
    assert "waiting_clean_channel" not in source


def test_resolve_campaign_run_delete_failures_marks_failed_as_deleted():
    messages = [
        _message(1, 130, "failed", rule_id=4),
        _message(2, 130, "pending", rule_id=4),
        _message(3, 130, "processing", rule_id=4),
        _message(4, 130, "failed", rule_id=5),
    ]
    repo, _ = _repo_with_data([_run(130, rule_id=4), _run(131, rule_id=5)], messages)

    result = repo.resolve_campaign_run_delete_failures(run_id=130, rule_id=4)

    assert messages[0]["delete_status"] == "deleted"
    assert messages[1]["delete_status"] == "pending"
    assert messages[2]["delete_status"] == "processing"
    assert messages[3]["delete_status"] == "failed"
    assert result["resolved"] == 1
    assert result["remaining_pending"] == 1
    assert result["remaining_processing"] == 1
    assert result["remaining_failed"] == 0
