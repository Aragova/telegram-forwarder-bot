from app.postgres_repository import PostgresRepository
from contextlib import contextmanager
from pathlib import Path


class _FakeCursor:
    def __init__(self, rowcount: int, row: dict | None = None, rows: list[dict] | None = None):
        self.rowcount = rowcount
        self._row = row
        self._rows = rows or []
        self.last_sql = ""
        self.last_params = None
        self.executed_sql: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *_args, **_kwargs):
        if _args:
            self.last_sql = _args[0]
            self.last_params = _args[1] if len(_args) > 1 else None
            self.executed_sql.append(str(_args[0]))
        return None

    def fetchone(self):
        return self._row
    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rowcount: int, row: dict | None = None, rows: list[dict] | None = None):
        self._cursor = _FakeCursor(rowcount, row=row, rows=rows)

    def cursor(self):
        return self._cursor

    def commit(self):
        return None


def test_repost_campaign_repository_methods_exist():
    repo = PostgresRepository()

    assert hasattr(repo, "get_rule_repost_campaign_summary")
    assert hasattr(repo, "update_rule_repost_campaign_settings")
    assert hasattr(repo, "add_rule_repost_campaign_target")
    assert hasattr(repo, "list_rule_repost_campaign_targets")
    assert hasattr(repo, "remove_rule_repost_campaign_target")
    assert hasattr(repo, "set_rule_repost_campaign_target_active")
    assert hasattr(repo, "update_rule_repost_campaign_target_check_result")
    required = [
        "create_campaign_run",
        "update_campaign_run_status",
        "create_campaign_run_message",
        "mark_campaign_run_message_sending",
        "mark_campaign_run_message_sent",
        "mark_campaign_run_message_failed",
        "claim_due_campaign_run_messages_for_delete",
        "mark_campaign_run_message_deleted",
        "mark_campaign_run_message_delete_failed",
        "reset_stuck_campaign_delete_processing",
        "get_campaign_run",
        "get_campaign_run_message",
        "list_campaign_runs_for_rule",
        "list_campaign_run_messages",
        "create_repost_campaign_launch_job",
        "get_active_repost_campaign_launch_job_for_rule",
        "get_repost_campaign_launch_job",
        "get_due_repost_campaign_launch_jobs",
        "lease_repost_campaign_launch_job",
        "mark_repost_campaign_launch_job_processing",
        "set_repost_campaign_launch_job_campaign_run_id",
        "mark_repost_campaign_launch_job_sent",
        "mark_repost_campaign_launch_job_failed",
        "mark_repost_campaign_launch_job_needs_review",
        "mark_repost_campaign_launch_job_cancelled",
        "recover_stale_repost_campaign_launch_jobs",
    ]
    for name in required:
        assert hasattr(repo, name)


def test_row_to_rule_maps_repost_campaign_fields():
    repo = PostgresRepository()
    row = {
        "id": 1,
        "source_id": "-1001",
        "source_thread_id": None,
        "target_id": "-1002",
        "target_thread_id": None,
        "interval": 3600,
        "schedule_mode": "interval",
        "fixed_times_json": None,
        "is_active": True,
        "created_date": "2026-05-04T00:00:00+00:00",
        "next_run_at": None,
        "last_sent_at": None,
        "source_title": "Source",
        "target_title": "Target",
        "mode": "repost",
        "video_trim_seconds": 120,
        "video_clip_duration_seconds": 118,
        "video_add_intro": False,
        "video_intro_horizontal": None,
        "video_intro_vertical": None,
        "video_intro_horizontal_id": None,
        "video_intro_vertical_id": None,
        "video_caption": None,
        "video_caption_entities_json": None,
        "caption_delivery_mode": "auto",
        "video_caption_delivery_mode": "auto",
        "repost_campaign_enabled": True,
        "repost_campaign_show_seconds": 86400,
    }

    rule = repo._row_to_rule(row)

    assert rule.repost_campaign_enabled is True
    assert rule.repost_campaign_show_seconds == 86400


def test_saved_posts_repository_methods_exist():
    repo = PostgresRepository()

    assert hasattr(repo, "create_saved_post")
    assert hasattr(repo, "get_saved_post")
    assert hasattr(repo, "list_saved_posts")
    assert hasattr(repo, "update_saved_post_content")
    assert hasattr(repo, "archive_saved_post")
    assert hasattr(repo, "set_rule_repost_campaign_saved_post")


def test_campaign_target_update_methods_use_returning_row_presence_as_bool():
    repo = PostgresRepository()

    @contextmanager
    def _connect_ok():
        yield _FakeConn(0, row={"id": 1, "is_active": False})

    @contextmanager
    def _connect_miss():
        yield _FakeConn(1, row=None)

    repo.connect = _connect_ok
    assert repo.set_rule_repost_campaign_target_active(1, False) is True
    assert repo.set_rule_repost_campaign_target_active(1, True) is True
    assert repo.remove_rule_repost_campaign_target(1) is True
    assert repo.update_rule_repost_campaign_target_check_result(1, title="Updated", last_check_error=None) is True

    repo.connect = _connect_miss
    assert repo.set_rule_repost_campaign_target_active(999999999, False) is False
    assert repo.remove_rule_repost_campaign_target(999999999) is False
    assert repo.update_rule_repost_campaign_target_check_result(999999999, title="Missing", last_check_error=None) is False


def test_campaign_run_messages_schema_contains_views_final_columns():
    with open("app/postgres_repository.py", "r", encoding="utf-8") as f:
        sql = f.read()
    assert "views_final_count" in sql
    assert "views_final_status" in sql
    assert "views_final_collected_at" in sql
    assert "views_final_error_text" in sql
    assert "views_final_attempt_count" in sql
    assert "views_final_next_retry_at" in sql


def test_mark_campaign_run_message_views_processing_sql_contract():
    repo = PostgresRepository()
    @contextmanager
    def _connect():
        yield _FakeConn(1, row=None)
    repo.connect = _connect
    assert repo.mark_campaign_run_message_views_processing(77) is True


def test_mark_campaign_run_message_views_collected_sql_contract():
    repo = PostgresRepository()
    @contextmanager
    def _connect():
        yield _FakeConn(1, row=None)
    repo.connect = _connect
    assert repo.mark_campaign_run_message_views_collected(77, views_count=555, collected_at="2026-05-07T00:00:00+00:00") is True


def test_mark_campaign_run_message_views_unavailable_sql_contract():
    repo = PostgresRepository()
    @contextmanager
    def _connect():
        yield _FakeConn(1, row=None)
    repo.connect = _connect
    assert repo.mark_campaign_run_message_views_unavailable(77, error_text="e", collected_at="2026-05-07T00:00:00+00:00") is True


def test_mark_campaign_run_message_views_failed_sql_contract():
    repo = PostgresRepository()
    @contextmanager
    def _connect():
        yield _FakeConn(1, row=None)
    repo.connect = _connect
    assert repo.mark_campaign_run_message_views_failed(77, error_text="e", next_retry_at="2026-05-07T00:01:00+00:00") is True


def test_claim_due_campaign_run_messages_for_delete_returns_views_final_fields():
    repo = PostgresRepository()
    rows = [{
        "id": 1, "views_final_count": 10, "views_final_status": "collected",
        "views_final_collected_at": "2026-05-07T00:00:00+00:00", "views_final_error_text": None,
        "views_final_attempt_count": 1, "views_final_next_retry_at": None,
    }]
    @contextmanager
    def _connect():
        yield _FakeConn(1, row=None, rows=rows)
    repo.connect = _connect
    result = repo.claim_due_campaign_run_messages_for_delete(limit=5)
    assert result[0]["views_final_count"] == 10
    assert result[0]["views_final_status"] == "collected"
    assert "views_final_next_retry_at" in result[0]


def test_claim_due_campaign_run_messages_for_delete_skips_failed_with_future_retry_filter():
    repo = PostgresRepository()
    captured = {"conn": None}
    @contextmanager
    def _connect():
        conn = _FakeConn(0, row=None, rows=[])
        captured["conn"] = conn
        yield conn
    repo.connect = _connect
    repo.claim_due_campaign_run_messages_for_delete(limit=5)
    sql = captured["conn"]._cursor.last_sql
    assert "views_final_status = 'failed'" in sql
    assert "views_final_next_retry_at > NOW()" in sql


def test_mark_campaign_run_message_failed_sanitizes_null_delete_status():
    repo = PostgresRepository()
    captured = {"conn": None}

    @contextmanager
    def _connect():
        conn = _FakeConn(1, row=None)
        captured["conn"] = conn
        yield conn

    repo.connect = _connect

    assert repo.mark_campaign_run_message_failed(
        77,
        error_text="x",
        render_mode="telethon_source_unverified",
        delete_status=None,
    ) is True

    params = captured["conn"]._cursor.last_params
    assert params[2] == "failed"
    assert params[2] is not None
    assert "требуется ручная проверка" in params[3]
    assert "delete_error_text" in captured["conn"]._cursor.last_sql


def test_campaign_schedule_repository_methods_exist():
    from app.repository import RepositoryProtocol
    from app.postgres_repository import PostgresRepository
    for name in [
        'create_campaign_scheduled_launch','get_campaign_scheduled_launch','list_rule_campaign_scheduled_launches',
        'list_due_campaign_scheduled_launches','claim_due_campaign_scheduled_launches','set_campaign_scheduled_launch_campaign_run_id','mark_campaign_scheduled_launch_launched',
        'mark_campaign_scheduled_launch_failed','mark_campaign_scheduled_launch_needs_review',
        'mark_campaign_scheduled_launch_waiting_clean_channel','mark_campaign_scheduled_launch_scheduled_again',
        'cancel_campaign_scheduled_launch','reset_stuck_campaign_scheduled_launches']:
        assert hasattr(RepositoryProtocol, name)
        assert hasattr(PostgresRepository, name)


def test_campaign_scheduled_launch_clean_channel_methods_exist():
    repo = PostgresRepository()

    assert hasattr(repo, "mark_campaign_scheduled_launch_waiting_clean_channel")
    assert hasattr(repo, "mark_campaign_scheduled_launch_scheduled_again")


def test_campaign_scheduled_launches_schema_contains_clean_channel_wait_columns():
    sql = Path("app/postgres_repository.py").read_text(encoding="utf-8")

    assert "clean_channel_next_retry_at" in sql
    assert "clean_channel_wait_attempt_count" in sql
    assert "clean_channel_last_wait_at" in sql
    assert "clean_channel_last_reason" in sql
    assert "clean_channel_policy_json" in sql
    assert "waiting_clean_channel" in sql
