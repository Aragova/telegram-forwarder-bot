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
    def __init__(self, *, active=True, busy=False):
        self.active = active
        self.busy = busy
        self.result = None
        self.rowcount = 1
        self.sql = []

    def execute(self, sql, _args=None):
        query = " ".join(str(sql).split())
        self.sql.append(query)
        self.rowcount = 1
        if query.startswith("SELECT is_active FROM routing"):
            self.result = {"is_active": self.active}
        elif "AS busy" in query:
            self.result = {"busy": self.busy}
        elif query.startswith("UPDATE delivery_attempts SET idempotency_key"):
            self.rowcount = 12
        elif query.startswith("DELETE FROM deliveries WHERE rule_id"):
            self.rowcount = 33
        else:
            self.result = None

    def fetchone(self):
        return self.result


def _rescan_repo(*, active=True, busy=False):
    cursor = RescanCursor(active=active, busy=busy)
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
    assert not any("UPDATE routing" in q for q in cursor.sql)


def test_rescan_pauses_only_selected_rule():
    repo, cursor, conn = _rescan_repo()
    assert repo.begin_rule_rescan(214) is True
    assert conn.commits == 1
    assert any(
        "UPDATE routing SET is_active = FALSE WHERE id = %s" in q
        for q in cursor.sql
    )


def test_explicit_replay_archives_attempts_and_resets_only_its_rule():
    repo, cursor, conn = _rescan_repo(active=False)
    assert repo.reset_rule_history_for_replay(214) == (12, 33)
    assert conn.commits == 1
    assert any(
        "UPDATE delivery_attempts SET idempotency_key" in q
        for q in cursor.sql
    )
    assert any(
        "DELETE FROM deliveries WHERE rule_id = %s" in q
        for q in cursor.sql
    )
    assert not any("DELETE FROM posts" in q for q in cursor.sql)
    assert not any("DELETE FROM delivery_attempts" in q for q in cursor.sql)


def test_explicit_replay_rejects_running_rule():
    repo, cursor, conn = _rescan_repo(active=True)
    with pytest.raises(RuntimeError, match="остановлено"):
        repo.reset_rule_history_for_replay(214)
    assert conn.commits == 0
    assert not any("UPDATE delivery_attempts" in q for q in cursor.sql)
