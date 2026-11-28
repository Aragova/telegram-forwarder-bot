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


def test_rescan_never_deletes_shared_source_posts_and_uses_common_rebuild():
    source = Path("bot.py").read_text(encoding="utf-8")
    common = source.split("async def _run_rescan_rule_job(", 1)[1].split(
        "async def _rescan_recovery_loop(", 1
    )[0]
    assert "db.clear_rule_deliveries" not in common
    assert "clean_start=True" not in common
    assert "db.begin_rule_rescan" in common
    assert "db.restart_rule_queue" in common
    assert "db.rebuild_rule_keep_position" in common
    assert common.count("await _scan_rule_source(rule, rule_id, owner_token)") == 1
    assert "db.mark_rule_messages_sent" not in common
    assert 'operation["before_message_id"]' in common
    assert common.index('operation["phase"] == "rebuilt"') < common.index("await _scan_rule_source")


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
    common = bot.split("async def _run_rescan_rule_job(", 1)[1].split(
        "async def _rescan_recovery_loop(", 1
    )[0]
    shared = bot.split("async def _scan_rule_source(", 1)[1].split(
        "async def _verify_rescan_live_arrivals(", 1
    )[0]
    assert shared.count("preserve_existing_deliveries=True") == 2
    assert "RescanSourceWriter(db, rule_id, owner_token)" in shared
    assert common.count("await _scan_rule_source(rule, rule_id, owner_token)") == 1


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
            self.result = {"busy": self.unresolved if "FROM mtproto_send_intents" in q else self.busy}
        elif "AS unresolved" in q:
            self.result = {"unresolved": self.unresolved}
        elif "SET is_active = FALSE, rescan_owner_token =" in q:
            self.owner, self.active = args[0], False
        elif q.startswith("UPDATE jobs SET status='failed'"):
            self.busy = False
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
            self.rowcount = 2
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


def test_rescan_revokes_jobs_already_in_flight():
    repo, cursor, conn = _rescan_repo(busy=True)
    was_active, owner = repo.begin_rule_rescan(214)
    assert was_active and cursor.owner == owner
    assert conn.commits == 1 and not cursor.busy
    assert any("rescan_cancelled" in q and "UPDATE jobs" in q for q in cursor.sql)


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
                                   include_newer_than_message_id=103) == (12, 33, 2)
    repo.finish_rule_rescan(214, was_active=active, owner_token=owner, queue_rebuilt=True)
    assert conn.commits == 3 and cursor.owner is None and cursor.active
    archive_index = next(i for i, q in enumerate(cursor.sql)
                         if "UPDATE delivery_attempts SET idempotency_key" in q)
    delete_index = next(i for i, q in enumerate(cursor.sql)
                        if "DELETE FROM deliveries WHERE rule_id=%s" in q)
    insert_index = next(i for i, q in enumerate(cursor.sql) if q.startswith("INSERT INTO deliveries"))
    assert archive_index < delete_index < insert_index
    assert not any("DELETE FROM posts" in q for q in cursor.sql)


def test_restart_refuses_another_owner():
    repo, cursor, conn = _rescan_repo(owner="other")
    with pytest.raises(RuntimeError, match="уже выполняется"):
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
    assert params == (214, "-100123", None, None, None, None, None, None, [101, 103], 103, [])
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
    assert "queue_rebuilt=True" in bot and "queue_rebuilt=False" in bot
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


def test_scan_high_water_and_cursor_are_persisted_before_parser():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    block = source.split("def begin_rule_rescan(", 1)[1].split(
        "def _rescan_scope(", 1
    )[0]
    assert block.index("FOR UPDATE") < block.index("self._rule_first_pending_message_id")
    assert block.index("SELECT COALESCE(MAX(message_id),0) AS watermark") < block.index("SET rescan_operation=%s::jsonb")
    assert block.index("SET rescan_operation=%s::jsonb") < block.index("conn.commit()")
    bot = Path("bot.py").read_text(encoding="utf-8")
    assert 'watermark = int(operation["watermark"])' in bot
    assert "include_newer_than_message_id=watermark" in bot


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
    assert ":album:members:" in candidate_sql
    assert "a.idempotency_key NOT LIKE '%%:rescan:%%'" in candidate_sql
    assert "def _quarantine_rescan_orphans(" not in source


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
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lease(*args):
        yield

    async def db_write(run_db, func, *args, **kwargs):
        return await run_db(func, *args, **kwargs)

    namespace.setdefault("maintain_rescan_lease", lease)
    namespace.setdefault("rescan_db", db_write)
    helpers = [n for n in tree.body
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
               and n.name in {"_RescanReleaseFailed", "_finish_rule_rescan_with_retry",
                              "_report_rescan_release_failure", "_complete_rescan_job",
                              "_report_rescan_unknown", "_write_rescan_audit",
                              "_run_rescan_rule_job", "_rule_rescan_scope"}
               and n is not node]
    exec(compile(ast.Module(body=[*helpers, node], type_ignores=[]), "bot.py", "exec"), namespace)
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


@pytest.mark.parametrize("release", ["ok", "retry", "failed", "unknown"])
def test_fresh_restart_reports_success_even_when_card_refresh_fails(release):
    import asyncio
    import logging
    from unittest.mock import AsyncMock, Mock

    from app.rescan_safety import RescanBlocked

    rule = SimpleNamespace(source_id="-100123", source_thread_id=None,
                           target_id="-100456", target_thread_id=None, mode="repost",
                           schedule_mode="interval")
    repo = SimpleNamespace(restart_rule_queue=Mock(
                               return_value=(240, 525, 530),
                               side_effect=RuntimeError("lost commit response") if release == "unknown" else None),
                           log_rule_change=Mock(), get_rule=Mock(return_value=rule),
                           begin_rule_rescan=Mock(return_value=(True, "owner")),
                           get_rule_rescan_operation=Mock(return_value={
                               "mode": "fresh", "phase": "scanning", "attempt": 1,
                               "was_active": True, "actor_id": 3,
                               "scope": ["-100123", None, "-100456", None, "repost"],
                               "watermark": 525, "before_message_id": None, "old_next_run_at": None,
                           }),
                           finish_rule_rescan=Mock(return_value=True, side_effect={
                               "ok": None,
                               "retry": [RuntimeError("connection"), None],
                               "failed": RuntimeError("connection"),
                               "unknown": None,
                           }[release]))
    scan = AsyncMock(return_value=(5, set(range(1, 531))))

    async def run_db(func, *args, **kwargs):
        return func(*args, **kwargs)

    show = AsyncMock()
    sleep = AsyncMock()
    handler = _load_rescan_handler("_run_rescan_rule_fresh_job", {
        "asyncio": SimpleNamespace(sleep=sleep),
        "db": repo, "run_db": run_db, "RescanBlocked": RescanBlocked,
        "cancel_local_rule_delivery_jobs": Mock(),
        "logger": logging.getLogger(__name__),
        "invalidate_preview_cache": Mock(), "invalidate_rule_card_cache": Mock(),
        "utc_now_iso": lambda: "2026-11-26T10:00:00Z",
        "ensure_rule_workers": AsyncMock(),
        "refresh_rule_card_by_ids": AsyncMock(return_value="failed"),
        "_show_rule_processing_screen": show, "_scan_rule_source": scan,
        "_verify_rescan_live_arrivals": AsyncMock(return_value=set()),
        "build_rescan_rule_keyboard": Mock(return_value="rescan"),
        "build_rule_card_keyboard": Mock(return_value="card"),
    })
    asyncio.run(handler(rule_id=94, chat_id=1, message_id=2, admin_id=3))
    if release in {"retry", "failed"}:
        sleep.assert_awaited_once_with(1.0)
    else:
        sleep.assert_not_awaited()
    scan.assert_awaited_once_with(rule, 94, "owner")
    repo.restart_rule_queue.assert_called_once_with(
        94, owner_token="owner", allowed_message_ids=set(range(1, 531)),
        include_newer_than_message_id=525, checked_missing_message_ids=set(),
        expected_scope=("-100123", None, "-100456", None, "repost"),
    )
    if release == "unknown":
        repo.finish_rule_rescan.assert_not_called()
        repo.log_rule_change.assert_not_called()
        assert "неизвестен" in show.await_args.kwargs["text"]
        assert "reply_markup" not in show.await_args.kwargs
        return
    repo.log_rule_change.assert_called_once()
    assert repo.finish_rule_rescan.call_count == (1 if release == "ok" else 2)
    for call in repo.finish_rule_rescan.call_args_list:
        assert call.args == (94,)
        assert call.kwargs == {"was_active": True, "owner_token": "owner", "queue_rebuilt": True}
    if release == "failed":
        assert "Очередь перестроена" in show.await_args.kwargs["text"]
        assert "не запускайте повторно" in show.await_args.kwargs["text"]
        assert "reply_markup" not in show.await_args.kwargs
        return
    assert "очередь начата заново" in show.await_args.kwargs["text"]
    assert "530" in show.await_args.kwargs["text"]
    assert show.await_args.kwargs["reply_markup"] == "card"


@pytest.mark.parametrize("reason", ["same_message_signature", "message_not_modified"])
def test_rescan_final_screen_does_not_duplicate_existing_message(reason):
    import asyncio
    from unittest.mock import AsyncMock

    policy = SimpleNamespace(edit_text=AsyncMock(
        return_value=SimpleNamespace(ok=False, reason=reason),
    ))
    send = AsyncMock()
    handler = _load_rescan_handler("_show_rule_processing_screen", {
        "asyncio": asyncio, "ui_policy": policy, "send_message_safe": send,
    })
    asyncio.run(handler(chat_id=1, message_id=2, text="Готово"))
    send.assert_not_awaited()


def test_rescan_final_screen_handles_missing_ui_result():
    import asyncio
    from unittest.mock import AsyncMock

    policy = SimpleNamespace(edit_text=AsyncMock(return_value=None))
    send = AsyncMock()
    handler = _load_rescan_handler("_show_rule_processing_screen", {
        "asyncio": asyncio, "ui_policy": policy, "send_message_safe": send,
    })
    asyncio.run(handler(chat_id=1, message_id=2, text="Готово"))
    send.assert_awaited_once_with(chat_id=1, text="Готово", reply_markup=None, parse_mode=None)


@pytest.mark.parametrize("failure", ["audit", "workers", "card", "release_retry", "release_failed", "unknown"])
def test_keep_rescan_post_commit_failure_still_reports_success(failure):
    import asyncio
    import logging
    from unittest.mock import AsyncMock, Mock

    from app.rescan_safety import RescanBlocked

    rule = SimpleNamespace(source_id="-100123", source_thread_id=None,
                           target_id="-100456", target_thread_id=None,
                           mode="repost", schedule_mode="interval")
    repo = SimpleNamespace(
        get_rule=Mock(return_value=rule),
        begin_rule_rescan=Mock(return_value=(True, "owner")),
        get_rule_rescan_operation=Mock(return_value={
            "mode": "keep", "phase": "scanning", "attempt": 1,
            "was_active": True, "actor_id": 3,
            "scope": ["-100123", None, "-100456", None, "repost"],
            "watermark": 525, "before_message_id": 100, "old_next_run_at": "2026-11-26",
        }),
        rebuild_rule_keep_position=Mock(
            return_value=(5, 1, 0, 0),
            side_effect=RuntimeError("lost commit response") if failure == "unknown" else None),
        finish_rule_rescan=Mock(return_value=True, side_effect={
            "release_retry": [RuntimeError("connection"), None],
            "release_failed": RuntimeError("connection"),
        }.get(failure)),
        log_rule_change=Mock(side_effect=RuntimeError("audit") if failure == "audit" else None),
    )

    async def run_db(func, *args, **kwargs):
        return func(*args, **kwargs)

    show = AsyncMock()
    workers = AsyncMock(side_effect=RuntimeError("workers") if failure == "workers" else None)
    refresh = AsyncMock(return_value="failed",
                        side_effect=RuntimeError("card") if failure == "card" else None)
    sleep = AsyncMock()
    handler = _load_rescan_handler("_run_rescan_rule_keep_job", {
        "asyncio": SimpleNamespace(sleep=sleep),
        "db": repo, "run_db": run_db, "RescanBlocked": RescanBlocked,
        "cancel_local_rule_delivery_jobs": Mock(),
        "logger": logging.getLogger(__name__),
        "invalidate_preview_cache": Mock(), "invalidate_rule_card_cache": Mock(),
        "ensure_rule_workers": workers, "refresh_rule_card_by_ids": refresh,
        "_show_rule_processing_screen": show,
        "_scan_rule_source": AsyncMock(return_value=(5, {100, 101})),
        "_verify_rescan_live_arrivals": AsyncMock(return_value=set()),
        "build_rule_card_keyboard": Mock(return_value="card"),
        "build_rescan_rule_keyboard": Mock(return_value="rescan"),
    })
    asyncio.run(handler(rule_id=94, chat_id=1, message_id=2, admin_id=3))
    if failure.startswith("release"):
        sleep.assert_awaited_once_with(1.0)
    else:
        sleep.assert_not_awaited()
    if failure == "unknown":
        repo.finish_rule_rescan.assert_not_called()
        repo.log_rule_change.assert_not_called()
        assert "неизвестен" in show.await_args.kwargs["text"]
        assert "reply_markup" not in show.await_args.kwargs
        return
    repo.log_rule_change.assert_called_once()
    assert repo.finish_rule_rescan.call_count == (2 if failure.startswith("release") else 1)
    for call in repo.finish_rule_rescan.call_args_list:
        assert call.args == (94,)
        assert call.kwargs == {"was_active": True, "owner_token": "owner", "queue_rebuilt": True}
    repo.rebuild_rule_keep_position.assert_called_once()
    if failure == "release_failed":
        assert "Очередь перестроена" in show.await_args.kwargs["text"]
        assert "reply_markup" not in show.await_args.kwargs
        workers.assert_not_awaited()
        refresh.assert_not_awaited()
        return
    workers.assert_awaited_once()
    refresh.assert_awaited_once()
    assert "пересканировано с сохранением позиции" in show.await_args.kwargs["text"]
    assert show.await_args.kwargs["reply_markup"] == "card"


@pytest.mark.parametrize("mode", ["fresh", "keep"])
def test_rescan_missing_rule_does_not_offer_retry(mode):
    import asyncio
    import logging
    from unittest.mock import AsyncMock, Mock

    from app.rescan_safety import RescanBlocked

    repo = SimpleNamespace(get_rule=Mock(return_value=None), begin_rule_rescan=Mock())

    async def run_db(func, *args, **kwargs):
        return func(*args, **kwargs)

    show = AsyncMock()
    handler = _load_rescan_handler(f"_run_rescan_rule_{mode}_job", {
        "db": repo, "run_db": run_db, "RescanBlocked": RescanBlocked,
        "cancel_local_rule_delivery_jobs": Mock(),
        "logger": logging.getLogger(__name__), "_show_rule_processing_screen": show,
    })
    asyncio.run(handler(rule_id=94, chat_id=1, message_id=2, admin_id=3))
    show.assert_awaited_once_with(chat_id=1, message_id=2, text="❌ Правило #94 не найдено")
    repo.begin_rule_rescan.assert_not_called()


@pytest.mark.parametrize("tenant_id,expected", [(None, 1), (42, 42)])
def test_restart_preserves_tenant_or_uses_legacy_default(tenant_id, expected):
    repo, cursor, _ = _rescan_repo()
    execute = cursor.execute
    insert_args = []

    def track(sql, args=None):
        execute(sql, args)
        if str(sql).startswith("SELECT is_active, rescan_owner_token"):
            cursor.result["tenant_id"] = tenant_id
        if "INSERT INTO deliveries" in str(sql):
            insert_args.append(args)

    cursor.execute = track
    _, owner = repo.begin_rule_rescan(214, fresh=True)
    repo.restart_rule_queue(214, owner_token=owner, allowed_message_ids={101},
                            include_newer_than_message_id=101)
    assert len(insert_args) == 1
    assert insert_args[0][2] == expected


@pytest.mark.parametrize("reason", ["retry_after", "chat_retry_after_active"])
def test_rescan_terminal_screen_waits_for_chat_flood_lock(reason):
    import asyncio
    from unittest.mock import AsyncMock, Mock

    policy = SimpleNamespace(
        chat_cooldown_left=Mock(return_value=4.0),
        edit_text=AsyncMock(side_effect=[
            SimpleNamespace(ok=False, reason=reason),
            SimpleNamespace(ok=True, reason=None),
        ]),
    )
    sleep = AsyncMock()
    send = AsyncMock()
    handler = _load_rescan_handler("_show_rule_processing_screen", {
        "asyncio": SimpleNamespace(sleep=sleep), "ui_policy": policy,
        "send_message_safe": send,
    })
    asyncio.run(handler(chat_id=1, message_id=2, text="Готово"))
    sleep.assert_awaited_once_with(4.05)
    policy.chat_cooldown_left.assert_called_once_with(1)
    send.assert_not_awaited()


def test_rescan_terminal_screen_retries_fallback_send_after_flood_lock():
    import asyncio
    from unittest.mock import AsyncMock, Mock

    policy = SimpleNamespace(
        chat_cooldown_left=Mock(return_value=4.0),
        edit_text=AsyncMock(return_value=SimpleNamespace(ok=False, reason="message_not_found")),
    )
    sleep = AsyncMock()
    send = AsyncMock(side_effect=[None, object()])
    handler = _load_rescan_handler("_show_rule_processing_screen", {
        "asyncio": SimpleNamespace(sleep=sleep), "ui_policy": policy,
        "send_message_safe": send,
    })
    asyncio.run(handler(chat_id=1, message_id=2, text="Готово"))
    assert send.await_count == 2
    sleep.assert_awaited_once_with(4.05)


def test_rescan_progress_edit_failure_does_not_send_new_message():
    import asyncio
    from unittest.mock import AsyncMock

    policy = SimpleNamespace(edit_text=AsyncMock(
        return_value=SimpleNamespace(ok=False, reason="retry_after"),
    ))
    sleep = AsyncMock()
    send = AsyncMock()
    handler = _load_rescan_handler("_show_rule_processing_screen", {
        "asyncio": SimpleNamespace(sleep=sleep), "ui_policy": policy,
        "send_message_safe": send,
    })
    asyncio.run(handler(chat_id=1, message_id=2, text="Читаем историю", terminal=False))
    send.assert_not_awaited()
    sleep.assert_not_awaited()


@pytest.mark.parametrize("wrong_id", [False, True])
def test_verify_live_candidates_checks_deleted_and_present(wrong_id):
    import asyncio
    from unittest.mock import AsyncMock, Mock

    async def run_db(fn, *args):
        return fn(*args)

    client = SimpleNamespace(
        get_entity=AsyncMock(return_value="peer"),
        get_messages=AsyncMock(return_value=[
            None, SimpleNamespace(id=999 if wrong_id else 104),
        ]),
    )
    repo = SimpleNamespace(get_rule_rescan_live_message_ids=Mock(return_value={103, 104}))
    handler = _load_rescan_handler("_verify_rescan_live_arrivals", {
        "run_db": run_db, "db": repo, "telethon_client": client,
    })
    observed = {101, 103}
    rule = SimpleNamespace(source_id="-100123")
    if wrong_id:
        with pytest.raises(RuntimeError, match="неожиданный ID"):
            asyncio.run(handler(214, rule, 102, observed))
    else:
        assert asyncio.run(handler(214, rule, 102, observed)) == {103}
        assert observed == {101, 104}
    client.get_messages.assert_awaited_once_with("peer", ids=[103, 104])


@pytest.mark.parametrize("phase,attempt,active", [
    ("rebuilt", 2, True), ("rebuilt", 4, False), ("scanning", 4, True),
])
def test_recovered_committed_or_exhausted_operation_never_parses_again(phase, attempt, active):
    import asyncio
    import logging
    from unittest.mock import AsyncMock, Mock
    from app.rescan_safety import RescanBlocked

    operation = {"mode": "fresh", "phase": phase, "attempt": attempt,
                 "actor_id": 3, "was_active": active}
    repo = SimpleNamespace(
        claim_interrupted_rule_rescan=Mock(return_value=("new-owner", operation)),
        finish_rule_rescan=Mock(return_value=active),
        log_rule_change=Mock(), get_rule=Mock(), restart_rule_queue=Mock(),
    )

    async def run_db(func, *args, **kwargs):
        return func(*args, **kwargs)

    scan, show = AsyncMock(), AsyncMock()
    handler = _load_rescan_handler("_run_rescan_rule_job", {
        "asyncio": asyncio, "db": repo, "run_db": run_db, "RescanBlocked": RescanBlocked,
        "logger": logging.getLogger(__name__), "_scan_rule_source": scan,
        "_show_rule_processing_screen": show, "invalidate_preview_cache": Mock(),
        "invalidate_rule_card_cache": Mock(),
    })
    asyncio.run(handler(rule_id=94, recovery=True))
    repo.finish_rule_rescan.assert_called_once_with(
        94, was_active=active if phase == "rebuilt" else False,
        owner_token="new-owner", queue_rebuilt=phase == "rebuilt",
    )
    repo.restart_rule_queue.assert_not_called()
    repo.get_rule.assert_not_called()
    scan.assert_not_awaited()
    show.assert_not_awaited()


def test_recovery_keeps_original_cursor_even_after_partial_history_scan():
    import asyncio
    import logging
    from unittest.mock import AsyncMock, Mock
    from app.rescan_safety import RescanBlocked

    rule = SimpleNamespace(source_id="-100123", source_thread_id=None,
                           target_id="-100456", target_thread_id=None, mode="repost")
    operation = {"mode": "keep", "phase": "scanning", "attempt": 2,
                 "actor_id": 3, "was_active": True,
                 "scope": ["-100123", None, "-100456", None, "repost"],
                 "watermark": 525, "before_message_id": 100, "old_next_run_at": "2026-11-26"}
    repo = SimpleNamespace(
        claim_interrupted_rule_rescan=Mock(return_value=("new-owner", operation)),
        finish_rule_rescan=Mock(return_value=True), log_rule_change=Mock(),
        get_rule=Mock(return_value=rule), rebuild_rule_keep_position=Mock(return_value=(5, 1, 2, 0)),
    )

    async def run_db(func, *args, **kwargs):
        return func(*args, **kwargs)

    scan, show = AsyncMock(return_value=(5, {1, 100, 526})), AsyncMock()
    handler = _load_rescan_handler("_run_rescan_rule_job", {
        "asyncio": asyncio, "db": repo, "run_db": run_db, "RescanBlocked": RescanBlocked,
        "logger": logging.getLogger(__name__), "_scan_rule_source": scan,
        "_verify_rescan_live_arrivals": AsyncMock(return_value=set()),
        "cancel_local_rule_delivery_jobs": Mock(), "_show_rule_processing_screen": show,
        "invalidate_preview_cache": Mock(), "invalidate_rule_card_cache": Mock(),
    })
    asyncio.run(handler(rule_id=94, recovery=True))
    scan.assert_awaited_once_with(rule, 94, "new-owner")
    repo.rebuild_rule_keep_position.assert_called_once_with(
        94, before_message_id=100, owner_token="new-owner", allowed_message_ids={1, 100, 526},
        include_newer_than_message_id=525, checked_missing_message_ids=set(),
        expected_scope=("-100123", None, "-100456", None, "repost"),
    )
    repo.finish_rule_rescan.assert_called_once_with(
        94, was_active=True, owner_token="new-owner", queue_rebuilt=True,
    )
    show.assert_not_awaited()
