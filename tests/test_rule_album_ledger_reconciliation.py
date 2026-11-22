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
    assert "db.reset_rule_history_for_replay" in fresh
    assert "db.reset_rule_history_for_replay" not in keep
    assert "db.mark_rule_messages_sent" not in keep
    assert "db.drop_rule_pending_before_message" in keep
    assert "saved_first_pending_message_id" in keep
    assert "previous_sent" in keep


def test_repository_archives_attempt_keys_instead_of_deleting_history():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    fn = source.split("def reset_rule_history_for_replay(", 1)[1].split(
        "def clear_rule_deliveries(", 1
    )[0]
    assert "idempotency_key || ':rescan:' || id::text" in fn
    assert "DELETE FROM delivery_attempts" not in fn
    assert "DELETE FROM deliveries WHERE rule_id = %s" in fn
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
    assert fresh.count("preserve_existing_deliveries=True") == 2
    assert keep.count("preserve_existing_deliveries=True") == 2


class RescanCursor:
    def __init__(self, *, active=True, busy=False, owner=None):
        self.active, self.busy, self.owner = active, busy, owner
        self.result, self.rowcount, self.sql = None, 1, []

    def execute(self, sql, args=None):
        q = " ".join(str(sql).split())
        self.sql.append(q)
        self.rowcount = 1
        if q.startswith("SELECT is_active, rescan_owner_token FROM routing"):
            self.result = {"is_active": self.active, "rescan_owner_token": self.owner}
        elif "AS busy" in q:
            self.result = {"busy": self.busy}
        elif "SET is_active = FALSE, rescan_owner_token =" in q:
            self.owner, self.active = args[0], False
        elif "SET is_active = %s, rescan_owner_token = NULL" in q:
            if self.owner != args[2]:
                self.rowcount = 0
            else:
                self.active, self.owner = args[0], None
        elif q.startswith("UPDATE delivery_attempts SET idempotency_key"):
            self.rowcount = 12
        elif q.startswith("DELETE FROM deliveries WHERE rule_id"):
            self.rowcount = 33
        else:
            self.result = None

    def fetchone(self):
        return self.result


def _rescan_repo(*, active=True, busy=False, owner=None):
    cursor = RescanCursor(active=active, busy=busy, owner=owner)
    conn = FakeConnection(cursor)
    repo = object.__new__(PostgresRepository)

    @contextmanager
    def connect():
        yield conn

    repo.connect = connect
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


def test_explicit_replay_archives_attempts_with_matching_owner():
    repo, cursor, conn = _rescan_repo(active=False, owner="valid")
    assert repo.reset_rule_history_for_replay(214, owner_token="valid") == (12, 33)
    assert conn.commits == 1
    assert any("UPDATE delivery_attempts SET idempotency_key" in q for q in cursor.sql)
    assert any("DELETE FROM deliveries WHERE rule_id = %s" in q for q in cursor.sql)
    assert not any("DELETE FROM posts" in q for q in cursor.sql)


@pytest.mark.parametrize("active,token", [(True, "valid"), (False, "wrong"), (False, "")])
def test_replay_refuses_active_or_wrong_owner(active, token):
    repo, cursor, conn = _rescan_repo(active=active, owner="valid")
    with pytest.raises(RuntimeError, match="владеть остановленным"):
        repo.reset_rule_history_for_replay(214, owner_token=token)
    assert conn.commits == 0


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
    ) == 3
    query, params = cur.calls[0]
    assert "DELETE FROM deliveries d" in query
    assert "d.rule_id = %s" in query and "d.status = 'pending'" in query
    assert "p.source_thread_id IS NOT DISTINCT FROM %s" in query
    assert params == (214, "-100123", None, [101, 103])


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
