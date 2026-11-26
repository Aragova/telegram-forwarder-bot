from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.postgres_repository import PostgresRepository
from scripts.reconcile_rule_album_ledger import build_candidates, validate_candidate


def _row(did, mid, *, aid=20, status="accepted", ids=None):
    return {
        "delivery_id": did,
        "message_id": mid,
        "attempt_id": aid,
        "attempt_status": status,
        "sent_message_ids_json": ids if ids is not None else [701, 702, 703],
        "source_channel": "-100123",
        "source_thread_id": None,
        "target_id": "-100456",
        "target_thread_id": None,
        "media_group_id": "album-a",
    }


def test_recovery_groups_only_complete_album_attempts():
    rows = [_row(1, 101), _row(2, 102), _row(3, 103)]
    candidates = build_candidates(rows)
    assert len(candidates) == 1
    assert candidates[0]["source_ids"] == [101, 102, 103]
    assert candidates[0]["target_ids"] == [701, 702, 703]
    assert build_candidates(rows[:2]) == []


def test_recovery_rejects_unsorted_or_duplicate_target_ids():
    assert build_candidates([_row(1, 101, ids=[701, 701])]) == []
    assert build_candidates([_row(1, 101, ids=[702, 701])]) == []


def _message(mid, *, grouped_id=50, caption="", photo=True):
    return SimpleNamespace(
        id=mid, grouped_id=grouped_id, message=caption,
        photo=object() if photo else None, document=None,
    )


def test_recovery_checks_telegram_group_and_captions():
    candidate = build_candidates([_row(1, 101, ids=[701]),])[0]
    src = {101: _message(101, caption="hello")}
    dst = {701: _message(701, caption="hello")}
    assert validate_candidate(candidate, src, dst) == "STRUCTURE_OK"
    assert validate_candidate(candidate, src, {}) == "TARGET_MISSING"
    assert validate_candidate(candidate, src, {701: _message(701, grouped_id=None, caption="hello")}) == "TARGET_GROUP_MISMATCH"
    assert validate_candidate(candidate, src, {701: _message(701, caption="wrong")}) == "CAPTION_MISMATCH"
    assert validate_candidate(candidate, src, {701: _message(701, caption="hello", photo=False)}) == "MEDIA_MISMATCH"


class Cursor:
    def __init__(self, *, busy=False, attempt_status="accepted", expected_ids=None,
                 row_statuses=None):
        self.busy = busy
        self.attempt_status = attempt_status
        self.expected_ids = expected_ids or [701, 702, 703]
        self.row_statuses = row_statuses or ["pending"] * 3
        self.executed = []
        self.result = None
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args=None):
        query = " ".join(str(sql).split())
        self.executed.append((query, args))
        self.rowcount = 1
        if query.startswith("SELECT target_id, mode, tenant_id FROM routing"):
            self.result = {"target_id": "-100456", "mode": "repost", "tenant_id": 1}
        elif "FROM delivery_attempts WHERE id =" in query:
            self.result = {
                "id": 20, "status": self.attempt_status,
                "operation_kind": "album", "target_id": "-100456",
                "idempotency_key": "rule:214:target:-100456:media_group:album-a:album",
                "sent_message_ids_json": self.expected_ids,
            }
        elif "AS busy" in query:
            self.result = {"busy": self.busy}
        elif "FROM deliveries d JOIN posts p" in query:
            self.result = [
                {"id": did, "status": status, "message_id": mid}
                for did, status, mid in zip(
                    (1, 2, 3), self.row_statuses, (101, 102, 103)
                )
            ]
        else:
            self.result = None

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield self.cursor_obj

    def commit(self):
        self.commits += 1


def _repository(**kwargs):
    cursor = Cursor(**kwargs)
    connection = FakeConnection(cursor)
    repo = object.__new__(PostgresRepository)

    @contextmanager
    def connect():
        yield connection

    repo.connect = connect
    return repo, cursor, connection


def _apply(repo, **changes):
    data = {
        "rule_id": 214,
        "attempt_id": 20,
        "source_channel": "-100123",
        "source_thread_id": None,
        "media_group_id": "album-a",
        "source_message_ids": [101, 102, 103],
        "target_message_ids": [701, 702, 703],
    }
    data.update(changes)
    return repo.reconcile_confirmed_album_ledger(**data)


def test_recovery_atomically_promotes_all_three_deliveries():
    repo, cursor, connection = _repository()
    assert _apply(repo) == 3
    assert connection.commits == 1
    queries = [query for query, _ in cursor.executed]
    assert any("UPDATE delivery_attempts SET status = 'verified'" in q for q in queries)
    assert sum("UPDATE deliveries SET status = 'sent'" in q for q in queries) == 3
    assert any("FOR UPDATE OF d" in q for q in queries)
    assert all(
        "WHERE id = %s AND status = 'pending'" in q
        for q in queries if "UPDATE deliveries SET status = 'sent'" in q
    )


@pytest.mark.parametrize("variant", [
    {"busy": True},
    {"attempt_status": "sending"},
    {"expected_ids": [801, 802, 803]},
    {"row_statuses": ["pending", "sent", "pending"]},
])
def test_recovery_refuses_ambiguous_or_concurrent_state(variant):
    repo, cursor, connection = _repository(**variant)
    with pytest.raises(RuntimeError):
        _apply(repo)
    assert connection.commits == 0
    assert not any("UPDATE deliveries SET status = 'sent'" in q for q, _ in cursor.executed)


def test_recovery_refuses_partial_verification_without_opening_transaction():
    repo, cursor, connection = _repository()
    with pytest.raises(ValueError):
        _apply(repo, target_message_ids=[701, 702])
    assert connection.commits == 0
    assert cursor.executed == []


def test_rescan_never_deletes_shared_source_posts_and_keep_does_not_reset_queue():
    source = Path("bot.py").read_text(encoding="utf-8")
    keep = source.split("async def _run_rescan_rule_keep_job(", 1)[1].split(
        "\n@dataclass(slots=True)", 1
    )[0]
    fresh = source.split("async def _run_rescan_rule_fresh_job(", 1)[1].split(
        "async def _run_rescan_rule_keep_job(", 1
    )[0]
    assert "db.clear_rule_deliveries" not in keep + fresh
    assert "clean_start=True" not in keep + fresh
    assert "db.begin_rule_rescan" in keep and "db.begin_rule_rescan" in fresh
    assert "db.restart_rule_queue" in fresh
    assert "db.restart_rule_queue" not in keep
    assert "await _scan_rule_source(rule)" in keep and "await _scan_rule_source(rule)" in fresh
    assert "db.mark_rule_messages_sent" not in keep
    assert "db.rebuild_rule_keep_position" in keep
    assert "saved_first_pending_message_id" in keep
    assert "previous_sent" in keep


def test_repository_archives_attempt_keys_instead_of_deleting_history():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    fn = source.split("def restart_rule_queue(", 1)[1].split(
        "def clear_rule_deliveries(", 1
    )[0]
    assert "idempotency_key || ':rescan:' || id::text" in fn
    assert "DELETE FROM delivery_attempts" not in fn
    assert "DELETE FROM deliveries WHERE rule_id=%s" in fn
    assert "is_active = FALSE" in source
    assert "UPDATE posts" in source


def test_history_rescan_preserves_shared_existing_deliveries():
    import asyncio

    from app.parser import parse_channel_history

    class Client:
        async def get_entity(self, entity):
            return entity

        async def iter_messages(self, *_args, **_kwargs):
            yield SimpleNamespace(
                id=101, text="hello", message="hello", raw_text="hello",
                entities=[], media=None, date=None, grouped_id=None,
            )

    class Database:
        def __init__(self):
            self.calls = []

        def save_post_batch(self, messages, **kwargs):
            self.calls.append((messages, kwargs))
            return len(messages)

        def delete_channel_posts(self, *_args):
            raise AssertionError("Shared source deletion is forbidden")

    db = Database()
    count = asyncio.run(parse_channel_history(
        Client(), db, "-100123", clean_start=False,
        preserve_existing_deliveries=True,
    ))
    assert count == 1
    assert len(db.calls) == 1
    assert db.calls[0][1] == {"preserve_existing_deliveries": True}


def test_normal_parser_does_not_require_extra_repository_keyword():
    import asyncio

    from app.parser import parse_channel_history

    class Client:
        async def get_entity(self, entity):
            return entity

        async def iter_messages(self, *_args, **_kwargs):
            yield SimpleNamespace(
                id=102, text="normal", message="normal", raw_text="normal",
                entities=[], media=None, date=None, grouped_id=None,
            )

    class LegacyDatabase:
        def __init__(self):
            self.seen = 0

        def save_post_batch(self, batch):
            self.seen += len(batch)
            return len(batch)

    db = LegacyDatabase()
    assert asyncio.run(parse_channel_history(Client(), db, "-100123")) == 1
    assert db.seen == 1



def test_history_batch_cancellation_waits_for_database_thread():
    import asyncio
    import threading

    from app.parser import _save_history_batch

    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    class BlockingDB:
        def save_post_batch(self, _batch):
            started.set()
            try:
                if not release.wait(timeout=5):
                    raise TimeoutError("test write was never released")
                return 1
            finally:
                finished.set()

    async def scenario():
        task = asyncio.create_task(
            _save_history_batch(BlockingDB(), [(101,)], preserve_existing_deliveries=False)
        )
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    asyncio.run(scenario())


def test_rescan_batch_refresh_skips_recreating_old_delivery_rows():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    code = source.split("def save_post_batch(", 1)[1].split(
        "def delete_channel_posts(", 1
    )[0]
    assert "if inserted or not preserve_existing_deliveries" in code
    assert "content_json IS DISTINCT FROM" in code
    parser = Path("app/parser.py").read_text(encoding="utf-8")
    assert "preserve_existing_deliveries=True" in parser
    bot = Path("bot.py").read_text(encoding="utf-8")
    fresh = bot.split("async def _run_rescan_rule_fresh_job(", 1)[1].split(
        "async def _run_rescan_rule_keep_job(", 1
    )[0]
    keep = bot.split("async def _run_rescan_rule_keep_job(", 1)[1].split(
        "\n@dataclass(slots=True)", 1
    )[0]
    shared = bot.split("async def _scan_rule_source(", 1)[1].split(
        "async def _run_rescan_rule_fresh_job(", 1
    )[0]
    assert shared.count("preserve_existing_deliveries=True") == 2
    assert "await _scan_rule_source(rule)" in fresh and "await _scan_rule_source(rule)" in keep


class RescanCursor:
    def __init__(self, *, active=True, busy=False, owner=None, unresolved=False):
        self.active, self.busy, self.owner = active, busy, owner
        self.unresolved = unresolved
        self.result, self.rowcount, self.sql = None, 1, []

    def execute(self, sql, args=None):
        q = " ".join(str(sql).split())
        self.sql.append(q)
        self.rowcount = 1
        if q.startswith(("SELECT is_active, rescan_owner_token", "SELECT rescan_owner_token")):
            self.result = {"is_active": self.active, "rescan_owner_token": self.owner,
                           "source_id": "-100123", "source_thread_id": None,
                           "target_id": "-100456", "target_thread_id": None, "tenant_id": 1, "mode": "repost"}
        elif "AS busy" in q:
            self.result = {"busy": self.unresolved if "JOIN mtproto_send_intents" in q else self.busy}
        elif "AS unresolved" in q:
            self.result = {"unresolved": self.unresolved}
        elif "SET is_active = FALSE, rescan_owner_token =" in q:
            self.owner, self.active = args[0], False
        elif "SET is_active = %s, rescan_owner_token = NULL" in q:
            if self.owner != args[3]:
                self.rowcount = 0
            else:
                self.active, self.owner = args[0], None
        elif q.startswith("UPDATE delivery_attempts SET idempotency_key"):
            self.rowcount = 12
        elif q.startswith("DELETE FROM deliveries WHERE rule_id"):
            self.rowcount = 33
            self.result = [{"post_id": i} for i in range(1, 34)]
        elif q.startswith("INSERT INTO deliveries"):
            self.rowcount = 33
        else:
            self.result = None

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result


def _rescan_repo(*, active=True, busy=False, owner=None, unresolved=False):
    cursor = RescanCursor(active=active, busy=busy, owner=owner, unresolved=unresolved)
    conn = FakeConnection(cursor)
    repo = object.__new__(PostgresRepository)

    @contextmanager
    def connect():
        yield conn

    repo.connect = connect
    repo._backfill_deliveries_for_rule_conn = lambda *args, **kwargs: 17
    return repo, cursor, conn


def test_rescan_refuses_jobs_already_in_flight():
    repo, cursor, conn = _rescan_repo(busy=True)
    with pytest.raises(RuntimeError, match="незавершённые"):
        repo.begin_rule_rescan(214)
    assert conn.commits == 0
    assert cursor.owner is None


def test_rescan_second_owner_rejected_across_modes():
    repo, cursor, conn = _rescan_repo()
    was_active, owner = repo.begin_rule_rescan(214)
    assert was_active is True and len(owner) == 32
    assert cursor.owner == owner and cursor.active is False
    with pytest.raises(RuntimeError, match="уже выполняется"):
        repo.begin_rule_rescan(214)
    with pytest.raises(RuntimeError, match="Владелец"):
        repo.finish_rule_rescan(214, was_active=True, owner_token="wrong")
    assert cursor.owner == owner and cursor.active is False
    repo.finish_rule_rescan(214, was_active=was_active, owner_token=owner)
    assert cursor.owner is None and cursor.active is True


def test_explicit_restart_archives_attempts_using_scanned_posts():
    repo, cursor, conn = _rescan_repo()
    active, owner = repo.begin_rule_rescan(214, fresh=True)
    assert repo.restart_rule_queue(214, owner_token=owner, allowed_message_ids={101, 103},
                                   include_newer_than_message_id=103) == (12, 33, 33)
    repo.finish_rule_rescan(214, was_active=active, owner_token=owner, queue_rebuilt=True)
    assert conn.commits == 3 and cursor.owner is None and cursor.active
    assert any("UPDATE delivery_attempts SET idempotency_key" in q for q in cursor.sql)
    assert any("DELETE FROM deliveries WHERE rule_id=%s" in q for q in cursor.sql)
    assert not any("DELETE FROM posts" in q for q in cursor.sql)


@pytest.mark.parametrize("options,reason", [
    ({"unresolved": True}, "современная публикация"),
    ({"busy": True}, "незавершённые задачи"),
    ({"owner": "other"}, "уже выполняется"),
])
def test_restart_refuses_live_work_or_another_owner(options, reason):
    repo, cursor, conn = _rescan_repo(**options)
    with pytest.raises(RuntimeError, match=reason):
        repo.begin_rule_rescan(214, fresh=True)
    assert conn.commits == 0
    assert not any("UPDATE delivery_attempts" in q or "DELETE FROM deliveries" in q for q in cursor.sql)


def test_backfill_filters_deleted_source_posts():
    class Cursor:
        rowcount = 1
        def __init__(self):
            self.inserted = []
        def execute(self, sql, params=None):
            if "INSERT INTO deliveries" in str(sql):
                self.inserted.append(params[1])
        def fetchall(self):
            return [{"id": 1, "message_id": 101},
                    {"id": 2, "message_id": 102},
                    {"id": 3, "message_id": 103}]
    class Conn:
        def __init__(self):
            self.cur = Cursor()
        @contextmanager
        def cursor(self):
            yield self.cur
    repo = object.__new__(PostgresRepository)
    repo._is_self_target_echo_post_for_rule_conn = lambda *a, **kw: False
    conn = Conn()
    result = repo._backfill_deliveries_for_rule_conn(
        conn, 214, "-100123", None, allowed_message_ids={101, 103},
    )
    assert result == 2 and conn.cur.inserted == [1, 3]


def test_stale_pending_prune_is_rule_scoped():
    class PruneCursor:
        rowcount = 3
        def __init__(self):
            self.calls = []
        def execute(self, sql, params=None):
            self.calls.append((" ".join(str(sql).split()), params))
    cur = PruneCursor()
    connection = FakeConnection(cur)
    repo = object.__new__(PostgresRepository)
    @contextmanager
    def connect():
        yield connection
    repo.connect = connect
    assert repo.drop_rule_pending_not_in_message_ids(
        214, source_channel="-100123", source_thread_id=None,
        seen_message_ids={101, 103},
        scan_start_max_message_id=103,
    ) == 3
    query, params = cur.calls[0]
    assert "DELETE FROM deliveries d" in query
    assert "d.rule_id = %s" in query and "d.status = 'pending'" in query
    assert "p.source_thread_id IS NOT DISTINCT FROM %s" in query
    assert params == (214, "-100123", None, None, None, None, None, None, [101, 103], 103)
    assert "p.message_id <= %s" in query


def test_parser_seen_ids_ignore_deleted_source_and_fail_closed(monkeypatch):
    import asyncio
    from app import parser
    class Client:
        async def get_entity(self, value):
            return value
        async def iter_messages(self, *_args, **_kwargs):
            for mid in (101, 103):
                yield SimpleNamespace(
                    id=mid, text="ok", message="ok", raw_text="ok",
                    entities=[], media=None, date=None, grouped_id=None,
                )
    class Database:
        def save_post_batch(self, batch, **_kwargs):
            return len(batch)
    count, seen = asyncio.run(parser.parse_channel_history(
        Client(), Database(), "-100123",
        preserve_existing_deliveries=True, collect_seen_message_ids=True,
    ))
    assert count == 2 and seen == {101, 103}
    original = parser.message_to_content_dict
    def fail_one(message):
        if message.id == 103:
            raise RuntimeError("bad message")
        return original(message)
    monkeypatch.setattr(parser, "message_to_content_dict", fail_one)
    with pytest.raises(RuntimeError, match="неполного пересканирования"):
        asyncio.run(parser.parse_channel_history(
            Client(), Database(), "-100123",
            preserve_existing_deliveries=True, collect_seen_message_ids=True,
        ))


def test_user_cannot_manually_reactivate_rule_during_rescan():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    toggle = source.split("def set_rule_active(", 1)[1].split(
        "def activate_rule_with_backfill(", 1
    )[0]
    activate = source.split("def activate_rule_with_backfill(", 1)[1].split(
        "def get_rule_source_scope(", 1
    )[0]
    assert "WHERE id = %s AND rescan_owner_token IS NULL" in toggle
    assert "WHERE id = %s AND rescan_owner_token IS NULL" in activate


@pytest.mark.parametrize("completed_scan,expected_backfill", [(True, 0), (False, 1)])
def test_reactivation_does_not_reintroduce_deleted_cached_posts(
    completed_scan, expected_backfill,
):
    class ActivationCursor:
        rowcount = 1
        def __init__(self):
            self.row = None
        def execute(self, sql, params=None):
            query = " ".join(str(sql).split())
            if "RETURNING id, source_id, source_thread_id, rescan_queue_built" in query:
                self.row = {
                    "id": 214, "source_id": "-100123",
                    "source_thread_id": None,
                    "rescan_queue_built": completed_scan,
                }
            else:
                self.row = None
        def fetchone(self):
            return self.row

    cursor = ActivationCursor()
    connection = FakeConnection(cursor)
    repo = object.__new__(PostgresRepository)
    backfilled = []
    repo._backfill_deliveries_for_rule_conn = lambda *args, **kw: backfilled.append(args)
    repo._compute_next_run_for_rule_conn = lambda *_args: "2026-11-23T00:00:00+00:00"

    @contextmanager
    def connect():
        yield connection

    repo.connect = connect
    assert repo.activate_rule_with_backfill(214) is True
    assert len(backfilled) == expected_backfill
    assert connection.commits == 1


def test_scan_finish_sets_persistent_queue_marker_before_resuming():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    finish = source.split("def finish_rule_rescan(", 1)[1].split(
        "def restart_rule_queue(", 1
    )[0]
    assert "rescan_queue_built = rescan_queue_built OR %s" in finish
    assert "WHERE id = %s AND rescan_owner_token = %s" in finish
    bot = Path("bot.py").read_text(encoding="utf-8")
    assert "db.restart_rule_queue" in bot
    assert "queue_rebuilt=scan_complete" in bot
    assert "rescan_queue_built BOOLEAN NOT NULL DEFAULT FALSE" in source
    activate = source.split("def activate_rule_with_backfill(", 1)[1].split(
        "def get_rule_source_scope(", 1
    )[0]
    assert "RETURNING id, source_id, source_thread_id, rescan_queue_built" in activate
    assert "SELECT id, source_id, source_thread_id, rescan_queue_built" not in activate


def test_mid_scan_live_arrival_survives_replay_backfill():
    class Rows:
        rowcount = 1
        def __init__(self):
            self.inserted = []
        def execute(self, sql, params=None):
            if "INSERT INTO deliveries" in str(sql):
                self.inserted.append(params[1])
        def fetchall(self):
            return [{"id": 1, "message_id": 101},
                    {"id": 2, "message_id": 102},  # stale before scan
                    {"id": 3, "message_id": 103},
                    {"id": 4, "message_id": 104}]  # live arrival after scan start
    class Conn:
        def __init__(self):
            self.cur = Rows()
        @contextmanager
        def cursor(self):
            yield self.cur
    repo = object.__new__(PostgresRepository)
    repo._is_self_target_echo_post_for_rule_conn = lambda *a, **kw: False
    conn = Conn()
    result = repo._backfill_deliveries_for_rule_conn(
        conn, 214, "-100123", None, allowed_message_ids={101, 103},
        include_newer_than_message_id=103,
    )
    assert result == 3
    assert conn.cur.inserted == [1, 3, 4]


def test_scan_high_water_is_captured_inside_keep_owner_finally_scope():
    source = Path("bot.py").read_text(encoding="utf-8")
    block = source.split("async def _run_rescan_rule_keep_job(", 1)[1].split(
        "\n@dataclass(slots=True)", 1
    )[0]
    assert "db.get_rule_scan_start_max_message_id" in block
    assert block.index("        try:") < block.index("db.get_rule_scan_start_max_message_id")
    assert "include_newer_than_message_id=scan_start_max_message_id" in block


def test_fresh_replay_requires_one_time_db_confirmation():
    code = Path("bot.py").read_text(encoding="utf-8")
    fresh = code.split("async def handle_rescan_rule_fresh(callback: CallbackQuery):", 1)[1].split(
        "async def handle_rescan_rule_fresh_confirm(callback: CallbackQuery):", 1
    )[0]
    confirm = code.split("async def handle_rescan_rule_fresh_confirm(callback: CallbackQuery):", 1)[1].split(
        "async def handle_rescan_rule_keep(callback: CallbackQuery):", 1
    )[0]
    assert "issue_rule_rescan_confirmation" in fresh
    assert "if not edited:" in fresh and "send_message_safe(" in fresh
    assert "consume_rule_rescan_confirmation" in confirm
    assert confirm.index("ensure_rule_callback_access") < confirm.index("consume_rule_rescan_confirmation")
    assert confirm.index("consume_rule_rescan_confirmation") < confirm.index("_schedule_rule_ui_task")





def test_reconciliation_recognizes_modern_fingerprinted_album_key():
    from app.delivery_idempotency import build_delivery_idempotency_key
    repo, cursor, connection = _repository()
    modern_key = build_delivery_idempotency_key(
        operation_kind="album", rule_id=214,
        target_id="-100456", media_group_id="album-a",
        source_message_ids=[11, 12, 13],
    )
    old_execute = cursor.execute

    def execute(sql, args=None):
        old_execute(sql, args)
        if "FROM delivery_attempts WHERE id =" in str(sql):
            cursor.result["idempotency_key"] = modern_key
            cursor.result["source_message_ids_json"] = [11, 12, 13]

    cursor.execute = execute
    assert _apply(repo) == 3
    assert connection.commits == 1
    assert any("UPDATE deliveries SET status = 'sent'" in q
               for q, _ in cursor.executed)


def test_album_recovery_sql_recognizes_modern_keys_without_archived_attempts():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    candidate_sql = source.split("def get_pending_album_ledger_candidates(", 1)[1].split(
        "def reconcile_confirmed_album_ledger(", 1
    )[0]
    orphan_sql = source.split("def _quarantine_rescan_orphans(", 1)[1].split(
        "def rebuild_rule_keep_position(", 1
    )[0]
    assert ":album:members:" in candidate_sql
    assert ":album:members:" in orphan_sql
    assert "a.idempotency_key NOT LIKE '%%:rescan:%%'" in candidate_sql
    assert "a.idempotency_key NOT LIKE '%%:rescan:%%'" in orphan_sql


def test_fingerprinted_album_reconciliation_keeps_original_member_vector():
    import json
    from app.delivery_idempotency import build_delivery_idempotency_key
    repo, cursor, conn = _repository()
    original = cursor.execute
    def execute(sql, args=None):
        original(sql, args)
        if "FROM delivery_attempts WHERE id =" in str(sql):
            cursor.result["delivery_id"] = 11
            cursor.result["source_message_ids_json"] = [11, 12, 13]
            cursor.result["idempotency_key"] = build_delivery_idempotency_key(
                operation_kind="album", rule_id=214, target_id="-100456",
                media_group_id="album-a", source_message_ids=[11, 12, 13])
    cursor.execute = execute
    assert _apply(repo) == 3 and conn.commits == 1
    args = next(args for query, args in cursor.executed if "UPDATE delivery_attempts SET status = 'verified'" in query)
    assert args[0] == 11 and json.loads(args[1]) == [11, 12, 13]


def _load_rescan_handler(name, namespace):
    import ast

    source = Path("bot.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), "bot.py", "exec"), namespace)
    return namespace[name]


def test_rescan_final_screen_retries_throttled_edit():
    import asyncio
    from unittest.mock import AsyncMock

    policy = SimpleNamespace(
        MESSAGE_EDIT_THROTTLE_SECONDS=0,
        edit_text=AsyncMock(side_effect=[
            SimpleNamespace(ok=False, reason="message_edit_throttled"),
            SimpleNamespace(ok=True, reason=None),
        ]),
    )
    send = AsyncMock()
    handler = _load_rescan_handler("_show_rule_processing_screen", {
        "asyncio": asyncio, "ui_policy": policy, "send_message_safe": send,
    })
    asyncio.run(handler(chat_id=1, message_id=2, text="Готово"))
    assert policy.edit_text.await_count == 2
    send.assert_not_awaited()


def test_rescan_final_screen_uses_send_when_edit_cannot_succeed():
    import asyncio
    from unittest.mock import AsyncMock

    policy = SimpleNamespace(edit_text=AsyncMock(
        return_value=SimpleNamespace(ok=False, reason="message_not_found"),
    ))
    send = AsyncMock()
    handler = _load_rescan_handler("_show_rule_processing_screen", {
        "asyncio": asyncio, "ui_policy": policy, "send_message_safe": send,
    })
    asyncio.run(handler(chat_id=1, message_id=2, text="Готово"))
    send.assert_awaited_once_with(chat_id=1, text="Готово", reply_markup=None, parse_mode=None)


def test_fresh_restart_reports_success_even_when_card_refresh_fails():
    import asyncio
    import logging
    from unittest.mock import AsyncMock, Mock

    from app.rescan_safety import RescanBlocked

    rule = SimpleNamespace(source_id="-100123", source_thread_id=None,
                           target_id="-100456", target_thread_id=None, mode="repost")
    repo = SimpleNamespace(restart_rule_queue=Mock(return_value=(240, 525, 530)),
                           log_rule_change=Mock(), get_rule=Mock(return_value=rule),
                           begin_rule_rescan=Mock(return_value=(True, "owner")),
                           get_rule_scan_start_max_message_id=Mock(return_value=525),
                           finish_rule_rescan=Mock())
    scan = AsyncMock(return_value=(5, set(range(1, 531))))

    async def run_db(func, *args, **kwargs):
        return func(*args, **kwargs)

    show = AsyncMock()
    handler = _load_rescan_handler("_run_rescan_rule_fresh_job", {
        "db": repo, "run_db": run_db, "RescanBlocked": RescanBlocked,
        "logger": logging.getLogger(__name__),
        "invalidate_preview_cache": Mock(), "invalidate_rule_card_cache": Mock(),
        "utc_now_iso": lambda: "2026-11-26T10:00:00Z",
        "ensure_rule_workers": AsyncMock(),
        "refresh_rule_card_by_ids": AsyncMock(return_value="failed"),
        "_show_rule_processing_screen": show, "_scan_rule_source": scan,
        "build_rescan_rule_keyboard": Mock(return_value="keyboard"),
    })
    asyncio.run(handler(rule_id=94, chat_id=1, message_id=2, admin_id=3))
    scan.assert_awaited_once_with(rule)
    repo.restart_rule_queue.assert_called_once_with(
        94, owner_token="owner", allowed_message_ids=set(range(1, 531)),
        include_newer_than_message_id=525, expected_scope=("-100123", None, "-100456", None, "repost"),
    )
    repo.finish_rule_rescan.assert_called_once_with(94, was_active=True, owner_token="owner", queue_rebuilt=True)
    assert "очередь начата заново" in show.await_args.kwargs["text"]
    assert "530" in show.await_args.kwargs["text"]
