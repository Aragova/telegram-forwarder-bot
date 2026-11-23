"""Channel-title refresh: immediate startup, fallback, retries and DB-only changes."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.channel_title_sync import (
    TITLE_SYNC_INTERVAL_SECONDS,
    _resolve_title,
    channel_title_sync_loop,
    refresh_channel_titles_once,
    unique_channel_ids,
)
from app.postgres_repository import PostgresRepository


class FakeBot:
    def __init__(self, names=None, errors=None):
        self.names = names or {}
        self.errors = errors or set()
        self.calls = []

    async def get_chat(self, chat_id):
        self.calls.append(str(chat_id))
        if str(chat_id) in self.errors:
            raise ValueError("chat inaccessible to bot")
        return SimpleNamespace(title=self.names.get(str(chat_id)))


class FakeTelethon:
    def __init__(self, names=None, errors=None):
        self.names = names or {}
        self.errors = errors or set()
        self.calls = []

    async def get_entity(self, chat_id):
        self.calls.append(chat_id)
        if str(chat_id) in self.errors:
            raise ValueError("user session cannot resolve entity")
        return SimpleNamespace(title=self.names.get(str(chat_id)))


class FakeDb:
    def __init__(self, channels=None):
        self.channels = channels or []
        self.updated = []
        self.calls = 0

    def get_channels(self):
        self.calls += 1
        return self.channels

    def refresh_channel_title(self, channel_id, title):
        self.updated.append((str(channel_id), title))
        return 1


async def run_db(callable_obj, *args):
    return callable_obj(*args)


def logger():
    import logging
    return logging.getLogger("tests.channel_title_sync")


def test_channel_ids_are_deduplicated_and_named_forum_topics_are_skipped():
    channels = [
        {"channel_id": "-1001", "thread_id": None},
        {"channel_id": "-1001", "thread_id": None},  # source and target
        {"channel_id": "-1002", "thread_id": 59887},  # topic label, not channel title
        {"channel_id": "-1003", "thread_id": None},
        {"channel_id": "  ", "thread_id": None},
    ]
    assert unique_channel_ids(channels) == ["-1001", "-1003"]


def test_rename_both_source_and_recipient_using_current_telegram_names():
    db = FakeDb([
        {"channel_id": "-100214", "thread_id": None, "channel_type": "source"},
        {"channel_id": "-100214", "thread_id": None, "channel_type": "target"},
        {"channel_id": "-100215", "thread_id": None, "channel_type": "target"},
    ])
    bot = FakeBot(names={"-100214": "Тайная", "-100215": "VIP NEW"})
    telethon = FakeTelethon()
    result = asyncio.run(refresh_channel_titles_once(
        db=db, bot=bot, telethon_client=telethon, run_db=run_db, logger=logger(),
    ))
    assert result.channels == 2
    assert result.checked == 2
    assert result.updated == 2
    assert result.inaccessible == 0
    assert len(bot.calls) == 2
    assert telethon.calls == []
    assert sorted(db.updated) == [("-100214", "Тайная"), ("-100215", "VIP NEW")]


def test_private_source_falls_back_to_existing_authorized_telethon():
    db = FakeDb([{"channel_id": "-100333", "thread_id": None}])
    bot = FakeBot(errors={"-100333"})
    telethon = FakeTelethon(names={"-100333": "Тайная"})
    result = asyncio.run(refresh_channel_titles_once(
        db=db, bot=bot, telethon_client=telethon, run_db=run_db, logger=logger(),
    ))
    assert result.checked == 1
    assert result.updated == 1
    assert telethon.calls == [-100333]
    assert db.updated == [("-100333", "Тайная")]


def test_inaccessible_chat_leaves_existing_title_unchanged():
    db = FakeDb([{"channel_id": "-100777", "thread_id": None}])
    bot = FakeBot(errors={"-100777"})
    telethon = FakeTelethon(errors={"-100777"})
    result = asyncio.run(refresh_channel_titles_once(
        db=db, bot=bot, telethon_client=telethon, run_db=run_db, logger=logger(),
    ))
    assert result.inaccessible == 1
    assert result.updated == 0
    assert db.updated == []


def test_blank_title_is_never_written():
    db = FakeDb([{"channel_id": "-100222", "thread_id": None}])
    bot = FakeBot(names={"-100222": " "})
    telethon = FakeTelethon(names={"-100222": ""})
    result = asyncio.run(refresh_channel_titles_once(
        db=db, bot=bot, telethon_client=telethon, run_db=run_db, logger=logger(),
    ))
    assert result.updated == 0
    assert db.updated == []


def test_one_database_failure_does_not_stop_other_channel_updates():
    class FailingDb(FakeDb):
        def refresh_channel_title(self, channel_id, title):
            if str(channel_id) == "-1001":
                raise RuntimeError("db unavailable for first update")
            return super().refresh_channel_title(channel_id, title)

    db = FailingDb([
        {"channel_id": "-1001", "thread_id": None},
        {"channel_id": "-1002", "thread_id": None},
    ])
    bot = FakeBot(names={"-1001": "Source", "-1002": "Target"})
    result = asyncio.run(refresh_channel_titles_once(
        db=db, bot=bot, telethon_client=None, run_db=run_db, logger=logger(),
    ))
    assert result.checked == 2
    assert result.updated == 1
    assert db.updated == [("-1002", "Target")]


def test_loop_starts_without_waiting_six_hours(monkeypatch):
    import app.channel_title_sync as mod
    seen = []

    async def once(**kwargs):
        seen.append("first")

    async def stop_sleep(delay):
        seen.append(("sleep", delay))
        raise asyncio.CancelledError()

    monkeypatch.setattr(mod, "refresh_channel_titles_once", once)
    monkeypatch.setattr(mod.asyncio, "sleep", stop_sleep)
    try:
        asyncio.run(channel_title_sync_loop(
            db=FakeDb(), bot=FakeBot(), telethon_client=None,
            run_db=run_db, logger=logger(),
        ))
    except asyncio.CancelledError:
        pass
    assert seen == ["first", ("sleep", TITLE_SYNC_INTERVAL_SECONDS)]


def test_bot_and_unified_runtime_start_title_refresh_and_shutdown_cleanly():
    source = Path("bot.py").read_text(encoding="utf-8")
    bot_role = source[source.index("async def _start_bot_role()"):source.index("async def _start_scheduler_role()")]
    all_role = source[source.index("async def _start_all_role()"):source.index("async def start_job_workers_runtime()")]
    shutdown = source[source.index("async def _shutdown_runtime("):source.index("async def _run_scheduler_role_loop()")]
    assert "_start_channel_title_sync_runtime()" in bot_role
    assert "_start_channel_title_sync_runtime()" in all_role
    assert "channel_title_sync_task.cancel()" in shutdown
    assert "await asyncio.gather(channel_title_sync_task, return_exceptions=True)" in shutdown
    assert "CHANNEL_TITLE_SYNC | запущена" in source


class FakeCursor:
    def __init__(self):
        self.query = ""
        self.params = None
        self.rowcount = 2

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params):
        self.query = str(query)
        self.params = params


class FakeConn:
    def __init__(self):
        self.cursor_value = FakeCursor()
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1


def test_database_only_updates_changed_root_channel_titles():
    conn = FakeConn()
    repository = object.__new__(PostgresRepository)
    repository.connect = lambda: conn
    count = repository.refresh_channel_title("-10055", " Новое название ")
    assert count == 2
    assert "UPDATE channels" in conn.cursor_value.query
    assert "thread_id IS NULL" in conn.cursor_value.query
    assert "title IS DISTINCT FROM" in conn.cursor_value.query
    assert "routing" not in conn.cursor_value.query
    assert "deliveries" not in conn.cursor_value.query
    assert conn.cursor_value.params == ("Новое название", "-10055", "Новое название")
    assert conn.commits == 1

    assert repository.refresh_channel_title("-10055", " \n ") == 0
    assert conn.commits == 1
