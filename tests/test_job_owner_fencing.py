"""The repository must fence heartbeat and terminal writes to the active lease owner."""
from __future__ import annotations

from app.postgres_repository import PostgresRepository


class _Cursor:
    def __init__(self, *, rowcount=1):
        self.rowcount = rowcount
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Connection:
    def __init__(self, rowcount=1):
        self.cursor_obj = _Cursor(rowcount=rowcount)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Repo:
    heartbeat_job_processing = PostgresRepository.heartbeat_job_processing
    finalize_owned_job = PostgresRepository.finalize_owned_job

    def __init__(self, rowcount=1):
        self.conn = _Connection(rowcount=rowcount)

    def connect(self):
        return self.conn


def test_heartbeat_updates_only_current_processing_owner():
    repo = _Repo()
    assert repo.heartbeat_job_processing(61923, "heavy:lease1") is True
    sql, params = repo.conn.cursor_obj.calls[0]
    assert "status = 'processing'" in sql
    assert "locked_by = %s" in sql
    assert params == (61923, "heavy:lease1")
    assert repo.conn.commits == 1


def test_stale_owner_cannot_complete_retry_fail_or_defer():
    for action in ("complete", "retry", "fail", "defer"):
        repo = _Repo(rowcount=0)
        assert repo.finalize_owned_job(
            61923, "old:owner", action, "timeout", 20
        ) is False
        sql, params = repo.conn.cursor_obj.calls[0]
        assert "status = 'processing'" in sql
        assert "locked_by = %s" in sql
        assert params[-2:] == (61923, "old:owner")


def test_retry_increments_attempts_but_rate_limit_defer_does_not():
    repo = _Repo()
    assert repo.finalize_owned_job(21, "w1", "retry", "again", 10)
    sql, args = repo.conn.cursor_obj.calls[0]
    assert args[0] == 1
    assert "attempts = attempts + %s" in sql

    repo = _Repo()
    assert repo.finalize_owned_job(21, "w1", "defer", "flood", 30)
    _, args = repo.conn.cursor_obj.calls[0]
    assert args[0] == 0
