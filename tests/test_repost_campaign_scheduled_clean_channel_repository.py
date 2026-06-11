from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from app.postgres_repository import PostgresRepository


class _FakeCursor:
    def __init__(self, rowcount: int = 1, row: dict | None = None, rows: list[dict] | None = None):
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

    def execute(self, *args, **kwargs):
        if args:
            self.last_sql = str(args[0])
            self.last_params = args[1] if len(args) > 1 else None
            self.executed_sql.append(str(args[0]))
        return None

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rowcount: int = 1, row: dict | None = None, rows: list[dict] | None = None):
        self._cursor = _FakeCursor(rowcount=rowcount, row=row, rows=rows)

    def cursor(self):
        return self._cursor

    def commit(self):
        return None


def _repo_with_conn(conn: _FakeConn) -> PostgresRepository:
    repo = PostgresRepository()

    @contextmanager
    def _connect():
        yield conn

    repo.connect = _connect
    return repo


def _method_source(source: str, method_name: str) -> str:
    markers = (f"    def {method_name}(", f"    async def {method_name}(")
    starts = [source.find(marker) for marker in markers]
    start = min(pos for pos in starts if pos != -1)
    next_def = source.find("\n    def ", start + 1)
    next_async_def = source.find("\n    async def ", start + 1)
    candidates = [pos for pos in (next_def, next_async_def) if pos != -1]
    if not candidates:
        return source[start:]
    return source[start:min(candidates)]


def _clean_channel_row() -> dict:
    return {
        "id": 123,
        "rule_id": 10,
        "status": "waiting_clean_channel",
        "scheduled_at": "2026-06-10T10:00:00+00:00",
        "preview_json": '{"scheduled_policy": {"action": "schedule_with_clean_channel_wait"}}',
        "clean_channel_next_retry_at": "2026-06-10T10:05:00+00:00",
        "clean_channel_wait_attempt_count": 2,
        "clean_channel_last_wait_at": "2026-06-10T10:00:00+00:00",
        "clean_channel_last_reason": "Чистый канал занят",
        "clean_channel_policy_json": '{"action": "schedule_with_clean_channel_wait"}',
    }


def test_mark_waiting_clean_channel_sql_contract():
    conn = _FakeConn(row={"id": 123})
    repo = _repo_with_conn(conn)

    ok = repo.mark_campaign_scheduled_launch_waiting_clean_channel(
        123,
        next_retry_at="2026-06-10T10:05:00+00:00",
        reason="Чистый канал занят",
        policy_snapshot={"action": "schedule_with_clean_channel_wait"},
    )

    assert ok is True
    sql = conn._cursor.last_sql
    assert "status = 'waiting_clean_channel'" in sql
    assert "clean_channel_next_retry_at" in sql
    assert "clean_channel_wait_attempt_count" in sql
    assert "clean_channel_last_wait_at" in sql
    assert "clean_channel_last_reason" in sql
    assert "clean_channel_policy_json" in sql
    assert "campaign_run_id IS NULL" in sql
    assert "Traceback" not in str(conn._cursor.last_params)


def test_mark_waiting_clean_channel_returns_false_without_row():
    conn = _FakeConn(row=None)
    repo = _repo_with_conn(conn)

    assert repo.mark_campaign_scheduled_launch_waiting_clean_channel(
        123,
        next_retry_at="2026-06-10T10:05:00+00:00",
    ) is False


def test_mark_scheduled_again_sql_contract():
    conn = _FakeConn(row={"id": 123})
    repo = _repo_with_conn(conn)

    assert repo.mark_campaign_scheduled_launch_scheduled_again(
        123,
        next_retry_at="2026-06-10T10:10:00+00:00",
        reason="Возврат в расписание",
    ) is True

    sql = conn._cursor.last_sql
    assert "status = 'scheduled'" in sql
    assert "status = 'waiting_clean_channel'" in sql
    assert "campaign_run_id IS NULL" in sql


def test_claim_due_campaign_scheduled_launches_includes_waiting_clean_channel():
    conn = _FakeConn(rows=[_clean_channel_row()])
    repo = _repo_with_conn(conn)

    rows = repo.claim_due_campaign_scheduled_launches(
        now_iso="2026-06-10T10:05:00+00:00",
        worker_id="worker-1",
        limit=5,
    )

    sql = conn._cursor.last_sql
    assert rows[0]["clean_channel_wait_attempt_count"] == 2
    assert "waiting_clean_channel" in sql
    assert "clean_channel_next_retry_at" in sql
    assert "COALESCE(clean_channel_next_retry_at, scheduled_at)" in sql
    assert "status = 'processing'" in sql
    assert "campaign_run_id IS NULL" in sql


def test_list_due_campaign_scheduled_launches_includes_waiting_clean_channel():
    conn = _FakeConn(rows=[_clean_channel_row()])
    repo = _repo_with_conn(conn)

    rows = repo.list_due_campaign_scheduled_launches(
        now_iso="2026-06-10T10:05:00+00:00",
        limit=5,
    )

    sql = conn._cursor.last_sql
    assert rows[0]["clean_channel_next_retry_at"] == "2026-06-10T10:05:00+00:00"
    assert "waiting_clean_channel" in sql
    assert "clean_channel_next_retry_at" in sql
    assert "COALESCE(clean_channel_next_retry_at, scheduled_at)" in sql


def test_cancel_campaign_scheduled_launch_allows_waiting_clean_channel_only_with_scheduled():
    conn = _FakeConn(rowcount=1)
    repo = _repo_with_conn(conn)

    assert repo.cancel_campaign_scheduled_launch(123, cancelled_by=55) is True

    sql = conn._cursor.last_sql
    assert "waiting_clean_channel" in sql
    assert "scheduled" in sql
    assert "launched" not in sql
    assert "failed" not in sql
    assert "processing" not in sql


def test_returned_scheduled_launch_rows_include_clean_channel_fields_and_normalized_policy():
    row = _clean_channel_row()

    get_conn = _FakeConn(row=row)
    get_repo = _repo_with_conn(get_conn)
    got = get_repo.get_campaign_scheduled_launch(123)
    assert got is not None
    assert got["clean_channel_next_retry_at"] == row["clean_channel_next_retry_at"]
    assert got["clean_channel_wait_attempt_count"] == 2
    assert got["clean_channel_last_wait_at"] == row["clean_channel_last_wait_at"]
    assert got["clean_channel_last_reason"] == "Чистый канал занят"
    assert got["clean_channel_policy_json"] == {"action": "schedule_with_clean_channel_wait"}

    list_conn = _FakeConn(rows=[row])
    list_repo = _repo_with_conn(list_conn)
    listed = list_repo.list_rule_campaign_scheduled_launches(10)
    assert listed[0]["clean_channel_policy_json"] == {"action": "schedule_with_clean_channel_wait"}

    claim_conn = _FakeConn(rows=[row])
    claim_repo = _repo_with_conn(claim_conn)
    claimed = claim_repo.claim_due_campaign_scheduled_launches(
        now_iso="2026-06-10T10:05:00+00:00",
        worker_id="worker-1",
        limit=5,
    )
    assert claimed[0]["clean_channel_policy_json"] == {"action": "schedule_with_clean_channel_wait"}


def test_source_guards_worker_enforcement_stays_out_of_handlers_and_vip_posts():
    schedule_source = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")
    process_due_source = _method_source(schedule_source, "process_due_scheduled_launches")
    for token in (
        "mark_campaign_scheduled_launch_waiting_clean_channel",
        "schedule_with_clean_channel_wait",
        "schedule_with_overlap_warning",
    ):
        assert token in process_due_source

    handlers_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    for token in ("mark_campaign_scheduled_launch_waiting_clean_channel", "waiting_clean_channel"):
        assert token not in handlers_source

    scheduled_post_source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")
    for token in (
        "mark_campaign_scheduled_launch_waiting_clean_channel",
        "waiting_clean_channel",
        "clean_channel_next_retry_at",
    ):
        assert token not in scheduled_post_source
