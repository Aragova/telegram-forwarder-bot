from contextlib import contextmanager

from app.postgres_repository import PostgresRepository


class _FakeCursor:
    def __init__(self, rowcounts):
        self._rowcounts = list(rowcounts)
        self.execute_calls = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.execute_calls.append((query, params))
        self.rowcount = int(self._rowcounts.pop(0)) if self._rowcounts else 0


class _FakeConn:
    def __init__(self, rowcounts):
        self.cursor_obj = _FakeCursor(rowcounts)
        self.commit_count = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1


class _RepoWithFakeConn(PostgresRepository):
    def __init__(self, rowcounts):
        self.fake_conn = _FakeConn(rowcounts)

    @contextmanager
    def connect(self):
        yield self.fake_conn


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


def test_repost_campaign_target_active_update_returns_rowcount_contract():
    repo = _RepoWithFakeConn([1, 1, 0])

    assert repo.set_rule_repost_campaign_target_active(10, False) is True
    assert repo.set_rule_repost_campaign_target_active(10, True) is True
    assert repo.set_rule_repost_campaign_target_active(999999999, False) is False

    assert repo.fake_conn.commit_count == 3
    first_query, first_params = repo.fake_conn.cursor_obj.execute_calls[0]
    assert "UPDATE rule_repost_campaign_targets" in first_query
    assert "SET is_active = %s" in first_query
    assert "WHERE id = %s" in first_query
    assert first_params == (False, 10)


def test_remove_repost_campaign_target_returns_rowcount_contract():
    repo = _RepoWithFakeConn([1, 0])

    assert repo.remove_rule_repost_campaign_target(10) is True
    assert repo.remove_rule_repost_campaign_target(999999999) is False

    assert repo.fake_conn.commit_count == 2
    first_query, first_params = repo.fake_conn.cursor_obj.execute_calls[0]
    assert "DELETE FROM rule_repost_campaign_targets WHERE id=%s" in first_query
    assert first_params == (10,)


def test_update_repost_campaign_target_check_result_returns_rowcount_contract():
    repo = _RepoWithFakeConn([1, 0])

    assert repo.update_rule_repost_campaign_target_check_result(10, title="Chan", last_check_error=None) is True
    assert repo.update_rule_repost_campaign_target_check_result(999999999, title="Missing", last_check_error="err") is False

    assert repo.fake_conn.commit_count == 2
    first_query, first_params = repo.fake_conn.cursor_obj.execute_calls[0]
    assert "UPDATE rule_repost_campaign_targets" in first_query
    assert "last_check_error = %s" in first_query
    assert "WHERE id = %s" in first_query
    assert first_params == ("Chan", None, 10)
