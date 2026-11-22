"""No stale delivery failure may overwrite a concurrent confirmed send."""
from contextlib import contextmanager

from app.postgres_repository import PostgresRepository


class FakeCursor:
    def __init__(self, attempt_status, delivery_status):
        self.attempt_status = attempt_status
        self.delivery_status = delivery_status
        self.statements = []
        self.rowcount = 0

    def execute(self, sql, params=()):
        self.statements.append((sql, params))
        if sql.lstrip().startswith("UPDATE deliveries"):
            assert self.attempt_status in {"sending", "accepted", "failed_after_send"}
            assert "status NOT IN ('sent', 'faulty')" in sql
            self.rowcount = int(self.delivery_status not in {"sent", "faulty"})
            if self.rowcount:
                self.delivery_status = "faulty"

    def fetchone(self):
        return {"status": self.attempt_status} if self.attempt_status else None


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.committed = False

    @contextmanager
    def cursor(self):
        yield self.fake_cursor

    def commit(self):
        self.committed = True


def run_case(attempt_status, delivery_status):
    cur = FakeCursor(attempt_status, delivery_status)
    conn = FakeConnection(cur)
    repo = PostgresRepository.__new__(PostgresRepository)

    @contextmanager
    def connect():
        yield conn

    repo.connect = connect
    updated = repo.mark_delivery_faulty_if_attempt_unverified(
        141, "delivery:141:target:-100:single", "confirmation_missing"
    )
    assert "FOR UPDATE" in cur.statements[0][0]
    assert conn.committed or not updated
    return updated, cur


def test_verified_attempt_never_becomes_faulty():
    updated, cur = run_case("verified", "processing")
    assert updated is False
    assert cur.delivery_status == "processing"
    assert len(cur.statements) == 1


def test_already_sent_delivery_never_becomes_faulty():
    updated, cur = run_case("accepted", "sent")
    assert updated is False
    assert cur.delivery_status == "sent"


def test_accepted_unverified_processing_delivery_can_be_fenced_faulty():
    updated, cur = run_case("accepted", "processing")
    assert updated is True
    assert cur.delivery_status == "faulty"
