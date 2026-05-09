from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.postgres_repository import PostgresRepository


class _Cursor:
    def __init__(self, executed: list[str]) -> None:
        self._executed = executed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._executed.append(" ".join(str(sql).split()))

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _Conn:
    def __init__(self, executed: list[str]) -> None:
        self._executed = executed

    def cursor(self):
        return _Cursor(self._executed)

    def commit(self):
        return None


def test_campaign_runs_scheduled_post_index_created_only_after_alter(monkeypatch):
    repo = PostgresRepository()
    init_sql_payload: list[str] = []
    executed_sql: list[str] = []

    monkeypatch.setattr(repo.client, "execute_script", lambda sql: init_sql_payload.append(sql))

    @contextmanager
    def _fake_connect():
        yield _Conn(executed_sql)

    monkeypatch.setattr(repo, "connect", _fake_connect)
    monkeypatch.setattr(repo, "_ensure_default_plans", lambda: None)
    monkeypatch.setattr(repo, "ensure_configured", lambda: None)

    repo.init()

    assert len(init_sql_payload) == 1
    assert "idx_campaign_runs_scheduled_post" not in init_sql_payload[0]

    alter_stmt = "ALTER TABLE campaign_runs ADD COLUMN IF NOT EXISTS scheduled_post_id BIGINT NULL"
    create_idx_stmt = "CREATE INDEX IF NOT EXISTS idx_campaign_runs_scheduled_post ON campaign_runs(scheduled_post_id)"

    assert alter_stmt in executed_sql
    assert create_idx_stmt in executed_sql
    assert executed_sql.index(alter_stmt) < executed_sql.index(create_idx_stmt)
