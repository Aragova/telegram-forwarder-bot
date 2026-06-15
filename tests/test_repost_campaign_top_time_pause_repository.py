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
        "get_campaign_top_time_pause",
        "get_campaign_top_time_pause_by_run_message",
        "cancel_campaign_top_time_pause",
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
    assert "if target_thread_id is None:" in source
    assert "AND target_thread_id IS NULL" in source
    assert "AND target_thread_id = %s" in source
    assert "AND starts_at <= %s::timestamptz" in source
    assert "AND ends_at > %s::timestamptz" in source
    assert "ORDER BY ends_at DESC, id DESC" in source


def test_campaign_top_time_pauses_schema_has_updated_at():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in source
    assert "ADD COLUMN IF NOT EXISTS updated_at" in source


def test_get_active_pause_lookup_does_not_use_untyped_null_parameter():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    method = source[source.index("def get_active_campaign_top_time_pause_for_target") :]
    method = method.split("\n    def ", 1)[0]

    assert "%s IS NULL" not in method


def test_get_active_campaign_top_time_pause_for_target_null_thread_sql_branch():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    method = source[source.index("def get_active_campaign_top_time_pause_for_target") :]
    method = method.split("\n    def ", 1)[0]

    assert "if target_thread_id is None:" in method
    assert "AND target_thread_id IS NULL" in method
    assert "(str(target_id), at_expr, at_expr)" in method


def test_get_active_campaign_top_time_pause_for_target_non_null_thread_sql_branch():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    method = source[source.index("def get_active_campaign_top_time_pause_for_target") :]
    method = method.split("\n    def ", 1)[0]

    assert "else:" in method
    assert "AND target_thread_id = %s" in method
    assert "(str(target_id), int(target_thread_id), at_expr, at_expr)" in method


def test_mark_expired_campaign_top_time_pauses_completed_sql_uses_limited_cte():
    source = Path("app/postgres_repository.py").read_text()
    assert "def mark_expired_campaign_top_time_pauses_completed" in source
    assert "WITH due AS" in source
    assert "WHERE status = 'active'" in source
    assert "ends_at <= COALESCE(%s::timestamptz, NOW())" in source
    assert "LIMIT %s" in source
    assert "SET status = 'completed'" in source
    assert "RETURNING p.id" in source


def test_list_campaign_top_time_pauses_for_run_returns_all_statuses():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    assert "def list_campaign_top_time_pauses_for_run" in source
    assert "WHERE campaign_run_id = %s" in source
    assert "WHEN 'active' THEN 0" in source
    assert "WHEN 'completed' THEN 1" in source
    assert "WHEN 'cancelled' THEN 2" in source
    assert "ends_at DESC" in source
    assert "id DESC" in source
    assert hasattr(RepositoryProtocol, "list_campaign_top_time_pauses_for_run")
    assert hasattr(PostgresRepository, "list_campaign_top_time_pauses_for_run")


def test_get_campaign_top_time_pause_by_id_sql():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    assert "def get_campaign_top_time_pause" in source
    assert "SELECT * FROM campaign_top_time_pauses WHERE id = %s LIMIT 1" in source


def test_cancel_campaign_top_time_pause_changes_active_to_cancelled_sql():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    assert "def cancel_campaign_top_time_pause" in source
    assert "status = 'cancelled'" in source
    assert "cancelled_at = COALESCE(%s::timestamptz, NOW())" in source
    assert "updated_at = NOW()" in source
    assert "cancel_reason = COALESCE(%s, 'manual_admin_cancel')" in source
    assert "AND status = 'active'" in source
    assert "RETURNING id" in source


def test_cancel_campaign_top_time_pause_does_not_cancel_completed_source_guard():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    method = source.split("def cancel_campaign_top_time_pause", 1)[1].split("def mark_expired_campaign_top_time_pauses_completed", 1)[0]
    assert "AND status = 'active'" in method
    assert "status = 'completed'" not in method
