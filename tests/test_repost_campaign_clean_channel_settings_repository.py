from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

from app.postgres_repository import PostgresRepository
from app.repository import RepositoryProtocol
from app.repository_models import Rule


def _base_rule_row(**extra: Any) -> dict[str, Any]:
    row = {
        "id": 10,
        "source_id": "-1001",
        "source_thread_id": None,
        "target_id": "-1002",
        "target_thread_id": None,
        "interval": 3600,
        "schedule_mode": "interval",
        "fixed_times_json": None,
        "is_active": True,
        "created_date": "2026-01-01T00:00:00+00:00",
        "next_run_at": None,
        "last_sent_at": None,
        "source_title": "Source",
        "target_title": "Target",
        "mode": "repost",
        "repost_campaign_enabled": True,
        "repost_campaign_show_seconds": 120,
        "repost_campaign_saved_post_id": 55,
    }
    row.update(extra)
    return row


class _SettingsCursor:
    def __init__(self, rows: dict[int, bool]):
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
            if rule_id in self.rows:
                self._row = {"id": rule_id, "repost_campaign_clean_channel_enabled": self.rows[rule_id]}
            else:
                self._row = None
            return
        if normalized.startswith("UPDATE ROUTING"):
            enabled = bool(params[0]) if params else False
            if rule_id in self.rows:
                self.rows[rule_id] = enabled
                self.rowcount = 1
            else:
                self.rowcount = 0
            self._row = None
            return
        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self._row


class _SettingsConn:
    def __init__(self, rows: dict[int, bool]):
        self.cursor_obj = _SettingsCursor(rows)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


def _repo_with_settings(rows: dict[int, bool]) -> tuple[PostgresRepository, _SettingsConn]:
    repo = PostgresRepository()
    conn = _SettingsConn(rows)

    @contextmanager
    def _connect():
        yield conn

    repo.connect = _connect  # type: ignore[method-assign]
    return repo, conn


def test_rule_dataclass_clean_channel_default_is_true():
    rule = Rule(id=1, source_id="s", source_thread_id=None, target_id="t", target_thread_id=None, interval=60)

    assert rule.repost_campaign_clean_channel_enabled is True


def test_row_to_rule_maps_clean_channel_enabled_true_false_and_fallback():
    repo = PostgresRepository()

    assert repo._row_to_rule(_base_rule_row(repost_campaign_clean_channel_enabled=True)).repost_campaign_clean_channel_enabled is True
    assert repo._row_to_rule(_base_rule_row(repost_campaign_clean_channel_enabled=False)).repost_campaign_clean_channel_enabled is False
    assert repo._row_to_rule(_base_rule_row(repost_campaign_clean_channel_enabled=None)).repost_campaign_clean_channel_enabled is True
    assert repo._row_to_rule(_base_rule_row()).repost_campaign_clean_channel_enabled is True


def test_init_sql_adds_clean_channel_column_safely():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "ALTER TABLE routing ADD COLUMN IF NOT EXISTS repost_campaign_clean_channel_enabled BOOLEAN NOT NULL DEFAULT TRUE" in source


def test_repository_protocol_exposes_clean_channel_settings_methods():
    assert hasattr(RepositoryProtocol, "get_rule_repost_campaign_clean_channel_settings")
    assert hasattr(RepositoryProtocol, "set_rule_repost_campaign_clean_channel_enabled")
    assert hasattr(PostgresRepository, "get_rule_repost_campaign_clean_channel_settings")
    assert hasattr(PostgresRepository, "set_rule_repost_campaign_clean_channel_enabled")


def test_get_rule_repost_campaign_clean_channel_settings_existing_enabled_true_false():
    repo, conn = _repo_with_settings({10: True, 11: False})

    assert repo.get_rule_repost_campaign_clean_channel_settings(10) == {"ok": True, "rule_id": 10, "enabled": True}
    assert repo.get_rule_repost_campaign_clean_channel_settings(11) == {"ok": True, "rule_id": 11, "enabled": False}
    assert "SELECT id, repost_campaign_clean_channel_enabled" in conn.cursor_obj.executed[-1][0]


def test_get_rule_repost_campaign_clean_channel_settings_missing_rule_falls_back_to_enabled_true():
    repo, _ = _repo_with_settings({})

    assert repo.get_rule_repost_campaign_clean_channel_settings(404) == {
        "ok": False,
        "rule_id": 404,
        "enabled": True,
        "error_text": "Правило не найдено",
    }


def test_set_rule_repost_campaign_clean_channel_enabled_updates_true_false_and_missing():
    rows = {10: True}
    repo, conn = _repo_with_settings(rows)

    assert repo.set_rule_repost_campaign_clean_channel_enabled(10, False, actor_id=123) is True
    assert rows[10] is False
    assert conn.commits == 1

    assert repo.set_rule_repost_campaign_clean_channel_enabled(10, True, actor_id=123) is True
    assert rows[10] is True
    assert conn.commits == 2

    assert repo.set_rule_repost_campaign_clean_channel_enabled(404, False, actor_id=123) is False
    assert conn.commits == 3
