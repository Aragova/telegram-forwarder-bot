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


class OrphanCursor:
    def __init__(self, *, status="sending", has_live_owner=False, process_count=2):
        self.status = status
        self.has_live_owner = has_live_owner
        self.process_count = process_count
        self.phase = ""
        self.rowcount = 0
        self.queries = []

    def execute(self, sql, params=()):
        self.queries.append((sql, params))
        if "FROM delivery_attempts" in sql and "FOR UPDATE" in sql:
            self.phase = "attempt"
        elif "has_live_owner" in sql:
            self.phase = "owner"
            assert "lease_until > NOW()" in sql
            assert "j.payload_json->'delivery_ids'" in sql
            assert "j.id <> %s" in sql
        elif "UPDATE deliveries" in sql:
            self.phase = "deliveries"
            assert "status='processing'" in sql
            self.rowcount = self.process_count
        elif "UPDATE delivery_attempts" in sql:
            self.phase = "finalize"
            self.status = "failed_after_send"
            self.rowcount = 1

    def fetchone(self):
        if self.phase == "attempt":
            return {"status": self.status}
        if self.phase == "owner":
            return {"has_live_owner": self.has_live_owner}
        return None


def orphan_case(status, has_live_owner=False, process_count=2):
    cur = OrphanCursor(status=status, has_live_owner=has_live_owner, process_count=process_count)
    conn = FakeConnection(cur)
    repo = PostgresRepository.__new__(PostgresRepository)

    @contextmanager
    def connect():
        yield conn

    repo.connect = connect
    count = repo.finalize_orphaned_repost_attempt(
        idempotency_key="rule:214:target:-100:album",
        job_id=62102,
        rule_id=214,
        delivery_ids=[171, 172],
        error_text="manual review after unresolved send",
    )
    return count, cur


def test_orphan_recovery_does_not_fault_verified_attempt():
    count, cur = orphan_case("verified")
    assert count == 0
    assert not any("UPDATE deliveries" in sql for sql, _ in cur.queries)


def test_orphan_recovery_never_faults_active_other_worker():
    count, cur = orphan_case("sending", has_live_owner=True)
    assert count == 0
    assert not any("UPDATE deliveries" in sql for sql, _ in cur.queries)


def test_orphan_recovery_finalizes_orphan_after_owner_lease_expires():
    count, cur = orphan_case("sending")
    assert count == 2
    assert cur.status == "failed_after_send"


def test_orphan_recovery_does_not_downgrade_already_sent_deliveries():
    count, cur = orphan_case("accepted", process_count=0)
    assert count == 0
    assert cur.status == "accepted"
