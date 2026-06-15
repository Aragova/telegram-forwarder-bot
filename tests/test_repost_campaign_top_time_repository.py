from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

from app.postgres_repository import PostgresRepository
from app.repository import RepositoryProtocol
from app.repository_models import Rule


class _TopTimeCursor:
    def __init__(self, rows: dict[int, dict[str, Any]]):
        self.rows = rows
        self.rowcount = 0
        self._row: dict[str, Any] | None = None
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None):
        self.executed.append((sql, params))
        normalized = " ".join(sql.split()).upper()
        rule_id = int(params[-1]) if params else 0
        if normalized.startswith("SELECT"):
            data = self.rows.get(rule_id)
            self._row = None if data is None else {
                "id": rule_id,
                "repost_campaign_top_time_enabled": data.get("enabled"),
                "repost_campaign_top_time_seconds": data.get("seconds"),
            }
            return
        if normalized.startswith("UPDATE ROUTING"):
            enabled = bool(params[0]) if params else False
            seconds = int(params[1]) if params else 0
            if rule_id in self.rows:
                self.rows[rule_id] = {"enabled": enabled, "seconds": seconds}
                self.rowcount = 1
            else:
                self.rowcount = 0
            self._row = None
            return
        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self._row


class _TopTimeConn:
    def __init__(self, rows: dict[int, dict[str, Any]]):
        self.cursor_obj = _TopTimeCursor(rows)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


def _repo_with_top_time(rows: dict[int, dict[str, Any]]) -> tuple[PostgresRepository, _TopTimeConn]:
    repo = PostgresRepository()
    conn = _TopTimeConn(rows)

    @contextmanager
    def _connect():
        yield conn

    repo.connect = _connect  # type: ignore[method-assign]
    return repo, conn


def test_rule_dataclass_top_time_defaults():
    rule = Rule(id=1, source_id="s", source_thread_id=None, target_id="t", target_thread_id=None, interval=60)

    assert rule.repost_campaign_top_time_enabled is False
    assert rule.repost_campaign_top_time_seconds == 0


def test_repository_protocol_exposes_top_time_settings_methods():
    assert hasattr(RepositoryProtocol, "get_rule_repost_campaign_top_time_settings")
    assert hasattr(RepositoryProtocol, "set_rule_repost_campaign_top_time_settings")
    assert hasattr(PostgresRepository, "get_rule_repost_campaign_top_time_settings")
    assert hasattr(PostgresRepository, "set_rule_repost_campaign_top_time_settings")


def test_init_sql_adds_top_time_columns_safely():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_top_time_enabled BOOLEAN NOT NULL DEFAULT FALSE" in source
    assert "ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_top_time_seconds INTEGER NOT NULL DEFAULT 0" in source


def test_get_rule_repost_campaign_top_time_settings_default_for_missing_rule():
    repo, _ = _repo_with_top_time({})

    assert repo.get_rule_repost_campaign_top_time_settings(404) == {"rule_id": 404, "enabled": False, "seconds": 0}


def test_set_rule_repost_campaign_top_time_settings_saves_enabled_preset():
    rows = {10: {"enabled": False, "seconds": 0}}
    repo, conn = _repo_with_top_time(rows)

    assert repo.set_rule_repost_campaign_top_time_settings(10, enabled=True, seconds=7200, actor_id=123) is True
    assert rows[10] == {"enabled": True, "seconds": 7200}
    assert conn.commits == 1

    assert repo.get_rule_repost_campaign_top_time_settings(10) == {"rule_id": 10, "enabled": True, "seconds": 7200}


def test_set_rule_repost_campaign_top_time_settings_disables_with_zero_seconds():
    rows = {10: {"enabled": True, "seconds": 7200}}
    repo, _ = _repo_with_top_time(rows)

    assert repo.set_rule_repost_campaign_top_time_settings(10, enabled=False, seconds=7200, actor_id=123) is True
    assert rows[10] == {"enabled": False, "seconds": 0}


def test_set_rule_repost_campaign_top_time_settings_normalizes_invalid_enabled_seconds():
    rows = {10: {"enabled": False, "seconds": 0}}
    repo, _ = _repo_with_top_time(rows)

    assert repo.set_rule_repost_campaign_top_time_settings(10, enabled=True, seconds=111, actor_id=123) is True
    assert rows[10] == {"enabled": True, "seconds": 7200}


def test_get_rule_repost_campaign_summary_source_contains_top_time_fields():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert '"top_time_enabled"' in source
    assert '"top_time_seconds"' in source
    assert "COALESCE(repost_campaign_top_time_enabled, FALSE) AS top_time_enabled" in source
    assert "COALESCE(repost_campaign_top_time_seconds, 0) AS top_time_seconds" in source
