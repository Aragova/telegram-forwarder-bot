from pathlib import Path

from app.postgres_repository import PostgresRepository
from app.repository import RepositoryProtocol


def test_campaign_top_time_pause_schema_and_indexes_exist():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS campaign_top_time_pauses" in source
    assert "campaign_run_message_id BIGINT NOT NULL" in source
    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_top_time_pauses_run_message_unique" in source
    assert "ON campaign_top_time_pauses (campaign_run_message_id)" in source
    assert "CREATE INDEX IF NOT EXISTS idx_campaign_top_time_pauses_active_target" in source
    assert "ON campaign_top_time_pauses (target_id, target_thread_id, ends_at)" in source
    assert "CREATE INDEX IF NOT EXISTS idx_campaign_top_time_pauses_active_due" in source


def test_campaign_top_time_pause_repository_methods_exist():
    for name in [
        "create_campaign_top_time_pause",
        "get_campaign_top_time_pause_by_run_message",
        "list_active_campaign_top_time_pauses_for_rule",
        "get_active_campaign_top_time_pause_for_target",
    ]:
        assert hasattr(RepositoryProtocol, name)
        assert hasattr(PostgresRepository, name)


def test_create_campaign_top_time_pause_is_idempotent_sql():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "INSERT INTO campaign_top_time_pauses" in source
    assert "ON CONFLICT (campaign_run_message_id)" in source
    assert "RETURNING id" in source


def test_list_active_campaign_top_time_pauses_for_rule_sql_filters_active_future():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "def list_active_campaign_top_time_pauses_for_rule" in source
    assert "WHERE rule_id = %s" in source
    assert "AND status = 'active'" in source
    assert "AND ends_at > NOW()" in source
    assert "ORDER BY ends_at ASC, id ASC" in source


def test_get_active_campaign_top_time_pause_for_target_sql_returns_latest_end_and_respects_thread():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "def get_active_campaign_top_time_pause_for_target" in source
    assert "target_thread_id IS NULL AND %s IS NULL" in source
    assert "OR target_thread_id = %s" in source
    assert "AND starts_at <= %s" in source
    assert "AND ends_at > %s" in source
    assert "ORDER BY ends_at DESC, id DESC" in source


def test_mark_expired_campaign_top_time_pauses_completed_sql_uses_limited_cte():
    source = Path("app/postgres_repository.py").read_text()
    assert "def mark_expired_campaign_top_time_pauses_completed" in source
    assert "WITH due AS" in source
    assert "WHERE status = 'active'" in source
    assert "ends_at <= COALESCE(%s::timestamptz, NOW())" in source
    assert "LIMIT %s" in source
    assert "SET status = 'completed'" in source
    assert "RETURNING p.id" in source
