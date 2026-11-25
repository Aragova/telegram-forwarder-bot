"""Production regression tests: no orphan captions and no deletion of delivery evidence."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from app.postgres_repository import PostgresRepository
from app.repost_single_delivery import RepostSingleDelivery
from app.telethon_authoritative_resolver import telethon_not_attempted


class _Bot:
    def __init__(self):
        self.send_message = AsyncMock(return_value=SimpleNamespace(message_id=900))


class _Owner:
    def __init__(self, media):
        self.db = SimpleNamespace(log_delivery_event=lambda **_kw: None)
        self.bot = _Bot()
        self.media = media
        self.sent = Mock()
        self.failed = AsyncMock()
        self.success = AsyncMock()

    def _get_post_id_by_delivery_sync(self, delivery_id):
        return delivery_id + 100

    def _resolve_repost_caption_delivery_strategy_sync(self, **_kwargs):
        return {"configured_mode": "builder_first", "requires_builder": True, "use_copy_first": False}

    def _is_self_loop_rule(self, _rule):
        return False

    def _get_post_row_for_rule_message_sync(self, *_args):
        return {"content_json": {"text": "original caption", "entities": []}}

    async def _fetch_message(self, *_args):
        return SimpleNamespace(id=10, text="original caption", message="original caption",
                               raw_text="original caption", entities=[], media=self.media)

    def _content_from_message_or_post(self, **_kwargs):
        return {"text": "original caption", "entities": []}

    def _build_text_and_entities_from_content(self, _content):
        return "original caption", []

    async def _reupload_message(self, *_args, **_kwargs):
        return telethon_not_attempted("safe_no_transport_send")

    async def _confirm_target_delivery_message_ids_with_retry(self, **_kwargs):
        return []

    async def _log_delivery_pipeline_step(self, **_kwargs):
        pass

    async def _log_delivery_final_success(self, **kwargs):
        await self.success(**kwargs)

    async def _log_delivery_final_failure(self, **kwargs):
        await self.failed(**kwargs)

    async def _add_reaction_for_rule_if_possible(self, **_kwargs):
        return {"applied": True}

    def _mark_delivery_sent_sync(self, *args, **kwargs):
        self.sent(*args, **kwargs)


def _deliver(owner):
    rule = SimpleNamespace(id=4, mode="repost", repost_caption_enabled=False)
    return asyncio.run(RepostSingleDelivery(owner).deliver(
        rule, 123, 10, "-1001", "-1002", None,
    ))


def test_media_failure_never_publishes_detached_original_caption():
    owner = _Owner(media=object())
    assert _deliver(owner) is False
    owner.bot.send_message.assert_not_awaited()
    owner.sent.assert_not_called()
    owner.failed.assert_awaited_once()


def test_text_only_fallback_saves_actual_message_id_and_transport():
    owner = _Owner(media=None)
    result = _deliver(owner)
    assert result == {"ok": True, "sent_message_ids": [900]}
    owner.bot.send_message.assert_awaited_once()
    owner.sent.assert_called_once_with(
        123, sent_message_id=900, sent_message_ids=[900],
        target_id="-1002", delivery_method="text_fallback",
    )


class _Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=()):
        self.statements.append((sql, params))

    def fetchone(self):
        return {"id": 100, "rule_id": 14, "post_id": 90,
                "status": "faulty", "error_text": "unverified"}

    def fetchall(self):
        return []


class _Conn:
    def __init__(self, cursor):
        self.cur = cursor
        self.committed = False

    @contextmanager
    def cursor(self):
        yield self.cur

    def commit(self):
        self.committed = True


def _repo(cursor):
    repo = object.__new__(PostgresRepository)
    conn = _Conn(cursor)

    @contextmanager
    def connection():
        yield conn

    repo.connect = connection
    return repo, conn


def test_faulty_clear_records_audit_without_deleting_delivery():
    cur = _Cursor()
    repo, conn = _repo(cur)
    assert repo.clear_faulty_delivery_log(100, admin_id=12345) is True
    sqls = "\n".join(sql for sql, _ in cur.statements)
    assert "INSERT INTO audit_log" in sqls
    assert "DELETE FROM deliveries" not in sqls
    assert "UPDATE deliveries" not in sqls
    assert "faulty_log_cleared" in str(cur.statements)
    assert conn.committed


def test_faulty_list_excludes_archived_entries_without_removing_history():
    cur = _Cursor()
    repo, _ = _repo(cur)
    assert repo.get_faulty_deliveries(limit=20) == []
    sql = cur.statements[0][0]
    assert "NOT EXISTS" in sql
    assert "cleared.delivery_id = d.id" in sql
    assert "cleared.event_type = 'faulty_log_cleared'" in sql


def test_live_faulty_count_uses_same_filter_as_faulty_list():
    cur = _Cursor()
    cur.fetchone = lambda: {"cnt": 3}
    repo, _ = _repo(cur)
    assert repo.count_uncleared_faulty_deliveries() == 3
    repo.get_faulty_deliveries(limit=20)
    count_sql, list_sql = (statement[0] for statement in cur.statements)
    for clause in (
        "d.status = 'faulty'",
        "d.error_text NOT LIKE 'Self-loop:",
        "d.error_text <> 'self_target_echo'",
        "JOIN posts p ON p.id = d.post_id",
        "JOIN routing r ON r.id = d.rule_id",
        "NOT EXISTS",
        "cleared.delivery_id = d.id",
        "cleared.event_type = 'faulty_log_cleared'",
    ):
        assert clause in count_sql
        assert clause in list_sql
    assert "LIMIT" not in count_sql
