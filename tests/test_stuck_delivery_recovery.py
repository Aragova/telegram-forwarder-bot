from __future__ import annotations

import inspect
from contextlib import contextmanager

from app.postgres_repository import PostgresRepository


def test_stuck_diagnostics_is_read_only_and_uses_liveness():
    source = inspect.getsource(PostgresRepository.get_stuck_processing_deliveries).upper()
    predicate = PostgresRepository._stale_delivery_job_predicate().upper()
    assert "SELECT" in source
    assert "D.STATUS='PROCESSING'" in source
    assert "J.UPDATED_AT" in predicate
    assert "J.LEASE_UNTIL < NOW()" in predicate
    assert "J.CREATED_AT" not in source + predicate
    for forbidden in ("UPDATE JOBS", "UPDATE DELIVERIES", "INSERT INTO", "DELETE FROM"):
        assert forbidden not in source


def test_manual_recovery_reuses_existing_job_and_never_reopens_delivery():
    class Cursor:
        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def fetchall(self):
            return [{"id": 123}]

    class Connection:
        def __init__(self):
            self.cur = Cursor()
            self.commits = 0

        @contextmanager
        def cursor(self):
            yield self.cur

        def commit(self):
            self.commits += 1

    repo = PostgresRepository.__new__(PostgresRepository)
    conn = Connection()

    @contextmanager
    def connect():
        yield conn

    repo.connect = connect
    result = repo.recover_stale_delivery_jobs(older_than_seconds=1, limit=100, rule_id=9)
    assert result == {"updated_count": 1, "job_ids": [123], "limit": 50}
    sql = conn.cur.sql.upper()
    assert "UPDATE JOBS" in sql
    assert "FOR UPDATE OF J SKIP LOCKED" in sql
    assert "UPDATE DELIVERIES" not in sql
    assert "RESCAN_OWNER_TOKEN IS NULL" in sql
    assert "RESCAN_CANCELLED" in sql
    assert "J.UPDATED_AT" in sql and "J.LEASE_UNTIL" in sql
    assert conn.cur.params == (600, 9, 9, 50, 600)
    assert conn.commits == 1


def test_legacy_manual_reset_is_only_a_compatibility_alias():
    source = inspect.getsource(PostgresRepository.reset_stuck_processing_deliveries_to_pending)
    assert "recover_stale_delivery_jobs" in source
    assert "UPDATE" not in source
