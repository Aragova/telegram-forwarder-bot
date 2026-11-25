import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.telegram_publication import (
    PublicationInbox, publication_pairs, verified_published_message,
)


@pytest.mark.parametrize("old,new", [
    ([1], None), ([1], []), ([1, 2], [3]), ([1, 1], [3, 4]),
    ([1, 2], [3, 3]), ([0], [3]), ([True], [3]), ([1], [-1]),
    (["1"], [3]),
])
def test_invalid_or_deleted_updates_never_create_mapping(old, new):
    assert publication_pairs(old, new) == []


def test_vector_index_not_sorted_numeric_id_determines_correspondence():
    assert publication_pairs([9, 2], [102, 101]) == [(9, 102), (2, 101)]


def test_mapping_is_scoped_by_sender_and_peer_and_can_arrive_before_receipt():
    class Repository:
        def __init__(self):
            self.rows = {}

        def record_telegram_publication_mappings(self, account, peer, pairs):
            for old, new in pairs:
                self.rows[account, peer, old] = new

        def get_telegram_publication_mapping(self, account, peer, old):
            return self.rows.get((account, peer, old))

    async def check():
        repo = Repository()
        inbox = PublicationInbox(repo, 10)
        assert await inbox.record(-100, [9], [100])
        assert await inbox.record(-100, [9], [100])
        assert await inbox.record(-200, [9], [200])
        assert await PublicationInbox(repo, 20).record(-100, [9], [300])
        assert await inbox.lookup(-100, 9) == 100
        assert await inbox.lookup(-200, 9) == 200
        assert await PublicationInbox(repo, 20).lookup(-100, 9) == 300
        assert not await inbox.record(-100, [9], None)
        assert await inbox.lookup(-100, 9) == 100

    asyncio.run(check())


@pytest.mark.parametrize("changes", [
    {"id": 101}, {"chat_id": -200}, {"from_scheduled": False},
    {"video_processing_pending": True}, {"post": False},
    {"reply_to": SimpleNamespace(reply_to_top_id=7)},
])
def test_wrong_peer_id_topic_or_state_cannot_be_reaction_target(changes):
    now = datetime.now(timezone.utc)
    attrs = dict(id=100, chat_id=-100, from_scheduled=True, post=True, date=now,
                 reply_to=SimpleNamespace(reply_to_top_id=5))
    attrs.update(changes)
    assert not verified_published_message(
        SimpleNamespace(**attrs), peer_id=-100, published_id=100,
        thread_id=5, source_ids={9}, before_id=99, started_at=now, broadcast=True,
    )


def test_exact_published_id_passes_but_self_loop_source_and_watermark_do_not():
    now = datetime.now(timezone.utc)
    msg = SimpleNamespace(id=100, chat_id=-100, from_scheduled=True, post=True, date=now)
    kwargs = dict(peer_id=-100, published_id=100, thread_id=None,
                  source_ids={9}, before_id=99, started_at=now, broadcast=True)
    assert verified_published_message(msg, **kwargs)
    assert not verified_published_message(msg, **(kwargs | {"source_ids": {100}}))
    assert not verified_published_message(msg, **(kwargs | {"before_id": 100}))


def test_telethon_schema_supports_publication_mapping_and_plain_deletion():
    import inspect
    from telethon import types
    assert "video_processing_pending" in inspect.signature(types.Message).parameters
    peer = types.PeerChannel(123)
    published = types.UpdateDeleteScheduledMessages(peer=peer,messages=[1],sent_messages=[100])
    deleted = types.UpdateDeleteScheduledMessages(peer=peer,messages=[1])
    from telethon.extensions import BinaryReader
    with BinaryReader(bytes(published)) as reader:
        decoded = reader.tgread_object()
    assert decoded.sent_messages == [100]
    assert publication_pairs(decoded.messages, decoded.sent_messages) == [(1,100)]
    assert publication_pairs(published.messages,published.sent_messages) == [(1,100)]
    assert publication_pairs(deleted.messages,deleted.sent_messages) == []


def test_mixed_namespace_candidates_allow_same_number_only_in_different_namespaces():
    from app.telegram_publication import publication_candidate_mask
    assert publication_candidate_mask([10,10],[False,True]) == [False,True]
    assert publication_candidate_mask([10,10],[True,True]) == []
    assert publication_candidate_mask([10],[False]) == []


def test_inbox_buffers_early_event_until_account_is_authenticated():
    from telethon import types,utils
    from app.telegram_publication import prepare_publication_inbox
    class Repository:
        def __init__(self):
            self.recorded=[]
        def record_telegram_publication_mappings(self,account,peer,pairs):
            self.recorded.append((account,peer,pairs))
    class Client:
        def add_event_handler(self,callback,event):
            self.handler=callback
        async def get_me(self):
            return SimpleNamespace(id=7)
    async def scenario():
        repo=Repository()
        client=Client()
        bind=prepare_publication_inbox(client,repo)
        peer=types.PeerChannel(99)
        await client.handler(types.UpdateDeleteScheduledMessages(peer=peer,messages=[4],sent_messages=[100]))
        assert not repo.recorded
        assert not hasattr(client,"publication_inbox")
        await bind()
        assert repo.recorded==[(7,utils.get_peer_id(peer),[(4,100)])]
        await client.handler(types.UpdateDeleteScheduledMessages(peer=peer,messages=[5]))
        assert len(repo.recorded)==1
    asyncio.run(scenario())



def test_live_inbox_retries_failed_write_without_repeated_telegram_event():
    from telethon import types, utils
    from app.telegram_publication import prepare_publication_inbox

    class Repository:
        def __init__(self):
            self.attempts = 0
            self.recorded = []

        def record_telegram_publication_mappings(self, account, peer, pairs):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("temporary database outage")
            self.recorded.append((account, peer, pairs))

    class Client:
        def add_event_handler(self, callback, event):
            self.handler = callback

        async def get_me(self):
            return SimpleNamespace(id=7)

    async def scenario():
        repo = Repository()
        client = Client()
        await prepare_publication_inbox(client, repo)()
        peer = types.PeerChannel(99)
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=peer, messages=[4], sent_messages=[100],
        ))
        assert not repo.recorded
        for _ in range(100):
            if repo.recorded:
                break
            await asyncio.sleep(0.02)
        assert repo.attempts == 2
        assert repo.recorded == [(7, utils.get_peer_id(peer), [(4, 100)])]

    asyncio.run(scenario())


def test_startup_timeout_preserves_unwritten_events(monkeypatch):
    from app import telegram_publication
    from telethon import types

    class Client:
        def add_event_handler(self, callback, event):
            self.handler = callback

        async def get_me(self):
            return SimpleNamespace(id=7)

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()
        saved = []

        async def record(self, peer, old, new):
            started.set()
            await release.wait()
            saved.append((old, new))
            return True

        monkeypatch.setattr(telegram_publication, "_STARTUP_DRAIN_TIMEOUT_SEC", 0.05)
        monkeypatch.setattr(telegram_publication.PublicationInbox, "record", record)
        client = Client()
        bind = telegram_publication.prepare_publication_inbox(client, object())
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=types.PeerChannel(99), messages=[4], sent_messages=[100],
        ))
        with pytest.raises(asyncio.TimeoutError):
            await bind()
        assert started.is_set() and not hasattr(client, "publication_inbox")
        release.set()
        for _ in range(100):
            if hasattr(client, "publication_inbox"):
                break
            await asyncio.sleep(0.01)
        assert saved == [([4], [100])]
        assert client.publication_inbox.sender_account_id == 7

    asyncio.run(scenario())


def test_startup_retries_failed_writes_after_bind_timeout_without_new_update(monkeypatch):
    from app import telegram_publication
    from telethon import types

    class Client:
        def add_event_handler(self, callback, event):
            self.handler = callback

        async def get_me(self):
            return SimpleNamespace(id=7)

    async def scenario():
        attempts = []
        saved = []
        complete = asyncio.Event()

        async def record(self, peer, old, new):
            attempts.append(old[0])
            if len(attempts) <= 3:
                raise RuntimeError("temporary database outage")
            saved.append((old[0], new[0]))
            if len(saved) == 2:
                complete.set()
            return True

        monkeypatch.setattr(telegram_publication, "_STARTUP_DRAIN_TIMEOUT_SEC", 0.05)
        monkeypatch.setattr(telegram_publication.PublicationInbox, "record", record)
        client = Client()
        bind = telegram_publication.prepare_publication_inbox(client, object())
        peer = types.PeerChannel(99)
        for old, new in [(4, 100), (5, 101)]:
            await client.handler(types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[old], sent_messages=[new],
            ))
        with pytest.raises(asyncio.TimeoutError):
            await bind()
        assert not hasattr(client, "publication_inbox")
        assert saved == []
        await asyncio.wait_for(complete.wait(), timeout=5)
        assert attempts == [4, 4, 4, 4, 5]
        assert saved == [(4, 100), (5, 101)]
        assert client.publication_inbox.sender_account_id == 7

    asyncio.run(scenario())


def test_startup_flood_wait_honors_server_delay_after_bind_timeout(monkeypatch):
    from app import telegram_publication
    from app.publication_difference_recovery import PublicationDifferenceRecovery
    from telethon.errors import FloodWaitError

    class Client:
        def add_event_handler(self, callback, event):
            self.handler = callback

        async def get_me(self):
            return SimpleNamespace(id=7)

    async def scenario():
        loop = asyncio.get_running_loop()
        attempts = []

        async def initialize(self):
            attempts.append(loop.time())
            if len(attempts) == 1:
                raise FloodWaitError(request=None, capture=1)

        monkeypatch.setattr(PublicationDifferenceRecovery, "initialize", initialize)
        monkeypatch.setattr(telegram_publication, "_STARTUP_DRAIN_TIMEOUT_SEC", 0.05)
        client = Client()
        repo = SimpleNamespace(get_publication_recovery_cursor=lambda *_: None)
        bind = telegram_publication.prepare_publication_inbox(client, repo)
        with pytest.raises(asyncio.TimeoutError):
            await bind()
        assert not hasattr(client, "publication_inbox")
        assert not client.publication_inbox_ready.done()
        await asyncio.sleep(0.35)
        assert len(attempts) == 1
        await asyncio.wait_for(asyncio.shield(client.publication_inbox_ready), 3)
        assert len(attempts) == 2
        assert attempts[1] - attempts[0] >= 1
        assert client.publication_inbox.sender_account_id == 7

    asyncio.run(scenario())


def test_sender_identity_timeout_retries_after_bind_timeout(monkeypatch):
    from app import telegram_publication
    from unittest.mock import AsyncMock

    async def scenario():
        client = SimpleNamespace(add_event_handler=lambda *_: None,
                                 get_me=AsyncMock(side_effect=[TimeoutError(), SimpleNamespace(id=7)]))
        monkeypatch.setattr(telegram_publication, "_STARTUP_DRAIN_TIMEOUT_SEC", 0.05)
        bind = telegram_publication.prepare_publication_inbox(client, object())
        with pytest.raises(asyncio.TimeoutError):
            await bind()
        assert not hasattr(client, "publication_inbox")
        inbox = await asyncio.wait_for(asyncio.shield(client.publication_inbox_ready), 3)
        assert inbox.sender_account_id == 7
        assert client.get_me.await_count == 2

    asyncio.run(scenario())


def test_unauthenticated_sender_fails_readiness_without_retry():
    from app.telegram_publication import prepare_publication_inbox
    from unittest.mock import AsyncMock

    async def scenario():
        client = SimpleNamespace(add_event_handler=lambda *_: None,
                                 get_me=AsyncMock(return_value=None))
        with pytest.raises(RuntimeError, match="authenticated sender"):
            await prepare_publication_inbox(client, object())()
        assert client.publication_inbox_ready.done()
        assert not hasattr(client, "publication_inbox")
        assert client.get_me.await_count == 1

    asyncio.run(scenario())


def test_startup_overflow_keeps_live_evidence_without_enabling_writer(monkeypatch):
    from app import telegram_publication
    from telethon import types

    async def scenario():
        saved = []

        async def record(self, peer, old, new):
            saved.append((old[0], new[0]))
            return True

        monkeypatch.setattr(telegram_publication.PublicationInbox, "record", record)
        client = SimpleNamespace(get_me=lambda: asyncio.sleep(0, result=SimpleNamespace(id=7)))
        client.add_event_handler = lambda callback, event: setattr(client, "handler", callback)
        bind = telegram_publication.prepare_publication_inbox(client, object())
        peer = types.PeerChannel(99)
        for mid in range(1, 1026):
            await client.handler(types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[mid], sent_messages=[mid + 2000],
            ))
        with pytest.raises(RuntimeError, match="buffer overflowed"):
            await bind()
        assert client.publication_inbox_ready.done()
        assert not hasattr(client, "publication_inbox")
        # Overflow must never drop a publication mapping, even in the
        # non-session test fallback that has no durable SQLite spool.
        assert len(saved) == 1025
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=peer, messages=[1026], sent_messages=[3026],
        ))
        await client.flush_publication_inbox()
        assert len(saved) == 1026
        assert saved[-1] == (1026, 3026)

    asyncio.run(scenario())


@pytest.mark.parametrize("server_unavailable", [False, True])
def test_difference_total_limit_does_not_discard_recoverable_page(server_unavailable):
    from app.publication_difference_recovery import PublicationDifferenceRecovery
    from telethon import types, functions
    from unittest.mock import AsyncMock

    async def scenario():
        date = datetime.now(timezone.utc)
        cursor = dict(pts=1, qts=0, state_date=date, version=0)
        state = types.updates.State(pts=20000, qts=0, date=date, seq=1, unread_count=0)
        update = types.UpdateDeleteScheduledMessages(
            peer=types.PeerChannel(99), messages=[4], sent_messages=[100],
        )
        page = types.updates.DifferenceSlice(
            new_messages=[], new_encrypted_messages=[], other_updates=[update],
            chats=[], users=[], intermediate_state=state,
        )
        too_long = types.updates.DifferenceTooLong(pts=20000)
        responses = [too_long, too_long] if server_unavailable else [too_long, page]
        client = AsyncMock(side_effect=responses)
        inbox = SimpleNamespace(repository=object(), sender_account_id=7,
                                record=AsyncMock(return_value=True))
        recovery = PublicationDifferenceRecovery(client, inbox)
        recovery._advance = AsyncMock(return_value=True)
        assert await recovery._read_page(cursor) is (not server_unavailable)
        requests = [call.args[0] for call in client.await_args_list]
        assert requests[0].pts_total_limit == 10000
        assert requests[1].pts_total_limit is None
        assert requests[1].pts == requests[0].pts == 1
        assert requests[1].pts_limit == 1000
        if server_unavailable:
            # No third call from the server's newer pts: it could skip
            # exact scheduled-to-published mappings between checkpoints.
            assert len(requests) == 2
            inbox.record.assert_not_awaited()
            recovery._advance.assert_not_awaited()
        else:
            assert len(requests) == 2
            inbox.record.assert_awaited_once()
            recovery._advance.assert_awaited_once_with(cursor, 20000, 0, date)

    asyncio.run(scenario())


def test_spool_append_during_empty_drain_is_not_stranded(monkeypatch, tmp_path):
    """An append while the last pending() read is suspended must cause a new read."""
    import threading
    from telethon import types
    from app import telegram_publication

    class ControlledSpool:
        instance = None

        def __init__(self, _path):
            ControlledSpool.instance = self
            self.rows = []
            self.next_id = 0
            self.armed = False
            self.stall_once = True
            self.empty_read = threading.Event()
            self.release_read = threading.Event()

        def append(self, peer, old, new):
            self.next_id += 1
            self.rows.append((self.next_id, peer, old, new))

        def pending(self, _limit=128):
            snapshot = self.rows[:]
            if self.armed and not snapshot and self.stall_once:
                self.stall_once = False
                self.empty_read.set()
                assert self.release_read.wait(5)
                return snapshot
            return snapshot

        def acknowledge(self, row_id):
            self.rows = [row for row in self.rows if row[0] != row_id]

    monkeypatch.setattr(telegram_publication, "PublicationInbox", PublicationInbox)
    monkeypatch.setattr(
        __import__("app.publication_mapping_spool", fromlist=["PublicationMappingSpool"]),
        "PublicationMappingSpool", ControlledSpool,
    )

    async def scenario():
        recorded = []

        def record(account, peer, pairs):
            recorded.extend(pairs)

        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        repo = SimpleNamespace(record_telegram_publication_mappings=record)
        bind = telegram_publication.prepare_publication_inbox(client, repo)
        await bind()
        spool = ControlledSpool.instance
        spool.armed = True
        peer = types.PeerChannel(99)
        first = asyncio.create_task(client.handler(types.UpdateDeleteScheduledMessages(
            peer=peer, messages=[10], sent_messages=[110],
        )))
        try:
            assert await asyncio.to_thread(spool.empty_read.wait, 5)
            await client.handler(types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[11], sent_messages=[111],
            ))
        finally:
            spool.release_read.set()
        await asyncio.wait_for(first, 5)
        assert recorded == [(10, 110), (11, 111)]
        assert spool.rows == []

    asyncio.run(scenario())


def test_sqlite_spool_failure_persists_to_postgres_without_next_update(monkeypatch, tmp_path):
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as spool_module

    class BrokenSpool:
        def __init__(self, _path):
            pass

        def pending(self, _limit=128):
            return []

        def append(self, *_args):
            raise OSError("disk full")

    monkeypatch.setattr(spool_module, "PublicationMappingSpool", BrokenSpool)

    async def scenario():
        saved = []
        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        repo = SimpleNamespace(record_telegram_publication_mappings=lambda account, peer, pairs: saved.extend(pairs))
        await telegram_publication.prepare_publication_inbox(client, repo)()
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=types.PeerChannel(99), messages=[12], sent_messages=[112],
        ))
        assert saved == [(12, 112)]
        assert client.publication_write_blocked is True

    asyncio.run(scenario())


def test_emergency_mapping_reaches_postgres_when_sqlite_append_and_pending_fail(monkeypatch, tmp_path):
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as spool_module

    class BrokenSpool:
        instance = None

        def __init__(self, _path):
            BrokenSpool.instance = self
            self.read_failed = False

        def pending(self, _limit=128):
            if self.read_failed:
                raise OSError("SQLite corruption prevents pending read")
            return []

        def append(self, *_args):
            raise OSError("SQLite corruption prevents append")

    monkeypatch.setattr(spool_module, "PublicationMappingSpool", BrokenSpool)

    async def scenario():
        recorded = []
        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        repo = SimpleNamespace(
            record_telegram_publication_mappings=lambda _account, _peer, pairs: recorded.extend(pairs),
        )
        await telegram_publication.prepare_publication_inbox(client, repo)()
        BrokenSpool.instance.read_failed = True
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=types.PeerChannel(99), messages=[21], sent_messages=[121],
        ))
        # One update, both SQLite operations failed. The mapping is still
        # persisted directly, before any retry or future Telegram update.
        assert recorded == [(21, 121)]
        assert client.publication_write_blocked is True

    asyncio.run(scenario())


def test_transient_postgres_failure_retries_durable_spool_without_permanent_write_lock(monkeypatch, tmp_path):
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as spool_module

    import threading
    acknowledged = threading.Event()

    class MemorySpool:
        instance = None

        def __init__(self, _path):
            MemorySpool.instance = self
            self.rows = []
            self.next_id = 0

        def append(self, peer, old, new):
            self.next_id += 1
            self.rows.append((self.next_id, peer, old, new))

        def pending(self, _limit=128):
            return self.rows[:]

        def acknowledge(self, row_id):
            self.rows = [row for row in self.rows if row[0] != row_id]
            acknowledged.set()

    monkeypatch.setattr(spool_module, "PublicationMappingSpool", MemorySpool)

    async def scenario():
        calls = []
        persisted = []

        def record(_account, _peer, pairs):
            calls.append(list(pairs))
            if len(calls) == 1:
                raise OSError("temporary PostgreSQL outage")
            persisted.extend(pairs)

        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        repo = SimpleNamespace(record_telegram_publication_mappings=record)
        await telegram_publication.prepare_publication_inbox(client, repo)()
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=types.PeerChannel(99), messages=[33], sent_messages=[133],
        ))
        assert await asyncio.to_thread(acknowledged.wait, 5)
        assert len(calls) == 2
        assert persisted == [(33, 133)]
        assert MemorySpool.instance.rows == []
        assert not getattr(client, "publication_write_blocked", False)

    asyncio.run(scenario())


def test_durable_sqlite_spool_over_1024_does_not_falsely_block_publication(tmp_path):
    """A full memory cache is not evidence loss when every pair is in SQLite."""
    from telethon import types
    from app.telegram_publication import prepare_publication_inbox

    async def scenario():
        state = {"db_up": False}
        persisted = []
        incidents = []

        def record(_account, _peer, pairs):
            if not state["db_up"]:
                raise OSError("temporary PostgreSQL outage")
            persisted.extend(pairs)

        repo = SimpleNamespace(
            record_telegram_publication_mappings=record,
            record_publication_inbox_overflow=lambda *args: incidents.append(args),
        )
        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        await prepare_publication_inbox(client, repo)()
        peer = types.PeerChannel(99)
        for old in range(1, 1026):
            await client.handler(types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[old], sent_messages=[old + 5000],
            ))
        assert not getattr(client, "publication_write_blocked", False)
        assert incidents == []

        state["db_up"] = True
        await client.flush_publication_inbox()
        assert persisted == [(old, old + 5000) for old in range(1, 1026)]
        assert not getattr(client, "publication_write_blocked", False)

    asyncio.run(scenario())


def test_sqlite_outage_coalesces_101_emergency_retries_into_one_task(monkeypatch, tmp_path):
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as spool_module

    class BrokenSpool:
        instance = None

        def __init__(self, _path):
            BrokenSpool.instance = self
            self.broken = False

        def pending(self, _limit=128):
            if self.broken:
                raise OSError("SQLite pending unavailable")
            return []

        def append(self, *_args):
            raise OSError("SQLite append unavailable")

    monkeypatch.setattr(spool_module, "PublicationMappingSpool", BrokenSpool)

    async def scenario():
        def record(_account, _peer, _pairs):
            raise OSError("PostgreSQL unavailable")

        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        await telegram_publication.prepare_publication_inbox(
            client, SimpleNamespace(record_telegram_publication_mappings=record),
        )()
        BrokenSpool.instance.broken = True
        peer = types.PeerChannel(99)
        for old in range(1, 102):
            await client.handler(types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[old], sent_messages=[old + 5000],
            ))
        def active_retries():
            return [
                task for task in asyncio.all_tasks()
                if task.get_name() == "telegram-publication-persist-retry" and not task.done()
            ]

        assert len(active_retries()) == 1
        await asyncio.sleep(0.30)
        assert len(active_retries()) == 1
        assert client.publication_write_blocked is True
        for task in active_retries():
            task.cancel()
        await asyncio.gather(*active_retries(), return_exceptions=True)

    asyncio.run(scenario())



def test_emergency_drain_cannot_strand_concurrent_recovered_spool_append(monkeypatch, tmp_path):
    """SQLite recovers while emergency PostgreSQL write is awaiting: both mappings drain."""
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as spool_module

    class RecoveringSpool:
        instance = None

        def __init__(self, _path):
            RecoveringSpool.instance = self
            self.first_append_fails = True
            self.rows = []
            self.sequence = 0

        def pending(self, _limit=128):
            return self.rows[:]

        def append(self, peer, old, new):
            if self.first_append_fails:
                self.first_append_fails = False
                raise OSError("SQLite append temporarily unavailable")
            self.sequence += 1
            self.rows.append((self.sequence, peer, old, new))

        def acknowledge(self, row_id):
            self.rows = [row for row in self.rows if row[0] != row_id]

    monkeypatch.setattr(spool_module, "PublicationMappingSpool", RecoveringSpool)

    async def scenario():
        persisted = []
        emergency_started = asyncio.Event()
        allow_emergency = asyncio.Event()
        original_record = telegram_publication.PublicationInbox.record

        async def pause_emergency_record(self, peer, old, new):
            if old == [11]:
                emergency_started.set()
                await allow_emergency.wait()
            return await original_record(self, peer, old, new)

        monkeypatch.setattr(
            telegram_publication.PublicationInbox, "record", pause_emergency_record,
        )
        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        repo = SimpleNamespace(
            record_telegram_publication_mappings=lambda _account, _peer, pairs: persisted.extend(pairs),
        )
        await telegram_publication.prepare_publication_inbox(client, repo)()
        peer = types.PeerChannel(99)
        first = asyncio.create_task(client.handler(types.UpdateDeleteScheduledMessages(
            peer=peer, messages=[11], sent_messages=[111],
        )))
        try:
            await asyncio.wait_for(emergency_started.wait(), 5)
            # This append occurs after the last empty spool.pending() read,
            # but before the first emergency mapping reaches PostgreSQL.
            await client.handler(types.UpdateDeleteScheduledMessages(
                peer=peer, messages=[12], sent_messages=[112],
            ))
            assert RecoveringSpool.instance.rows
        finally:
            allow_emergency.set()
        await asyncio.wait_for(first, 5)
        assert persisted == [(11, 111), (12, 112)]
        assert RecoveringSpool.instance.rows == []
        assert not [
            task for task in asyncio.all_tasks()
            if task.get_name() == "telegram-publication-persist-retry" and not task.done()
        ]

    asyncio.run(scenario())



def test_general_topic_valid_reply_message_id_is_not_topic_header():
    now = datetime.now(timezone.utc)
    msg = SimpleNamespace(
        id=100, chat_id=-100, from_scheduled=True, out=True,
        reply_to=SimpleNamespace(reply_to_msg_id=82, reply_to_top_id=None),
        date=now,
    )
    kwargs = dict(
        peer_id=-100, published_id=100, thread_id=1,
        source_ids={2}, before_id=99, started_at=now,
        broadcast=False,
    )
    assert verified_published_message(msg, **kwargs)
    msg.reply_to.forum_topic = True
    assert not verified_published_message(msg, **kwargs)
    msg.reply_to.forum_topic = False
    msg.reply_to.reply_to_top_id = 5
    assert not verified_published_message(msg, **kwargs)


def test_spool_append_failure_retains_incident_evidence_even_if_postgres_fails(monkeypatch, tmp_path):
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as spool_module

    class FailedSpool:
        def __init__(self, _path):
            pass

        def pending(self, _limit=128):
            return []

        def append(self, *_args):
            raise OSError("corrupt SQLite")

    monkeypatch.setattr(spool_module, "PublicationMappingSpool", FailedSpool)

    async def scenario():
        incidents = []
        recorded = []
        db_up = False
        class Repository:
            def record_telegram_publication_mappings(self, account, peer, pairs):
                if not db_up:
                    raise ConnectionError("PostgreSQL offline")
                recorded.extend(pairs)

            def record_publication_inbox_overflow(self, account, peer, ids):
                incidents.append((account, peer, list(ids)))

        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)

        async def get_me():
            return SimpleNamespace(id=7)

        client.get_me = get_me
        bind = telegram_publication.prepare_publication_inbox(client, Repository())
        peer = types.PeerChannel(99)
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=peer, messages=[28], sent_messages=[128],
        ))
        assert client.publication_write_blocked is True
        db_up = True
        with pytest.raises(RuntimeError, match="overflow"):
            await bind()
        # Startup may not reopen sends after disk evidence fails;
        # it must commit an incident that survives this process.
        assert recorded == [(28, 128)]
        assert len(incidents) == 1
        assert incidents[0][0] == 7
        assert incidents[0][2] == [28]

    asyncio.run(scenario())



def test_fallback_publication_journal_replays_after_restart_and_requires_review(monkeypatch, tmp_path):
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as module

    class FailedSpool:
        def __init__(self, _path):
            pass
        def append(self, *_args):
            raise OSError("SQLite damaged")
        def pending(self, _limit=128):
            return []

    async def scenario():
        session_file = str(tmp_path / "sender.session")
        peer = types.PeerChannel(99)

        def client():
            c = SimpleNamespace(session=SimpleNamespace(filename=session_file))
            c.add_event_handler = lambda callback, _event: setattr(c, "handler", callback)
            async def get_me():
                return SimpleNamespace(id=7)
            c.get_me = get_me
            return c

        # A first process loses its session SQLite file; its event has to
        # survive in the independent journal even without a DB connection.
        monkeypatch.setattr(module, "PublicationMappingSpool", FailedSpool)
        first = client()
        telegram_publication.prepare_publication_inbox(first, SimpleNamespace())
        await first.handler(types.UpdateDeleteScheduledMessages(
            peer=peer, messages=[41], sent_messages=[141],
        ))
        journal = module.PublicationFallbackJournal(
            tmp_path / "sender.publication-mappings.fallback.jsonl"
        )
        assert len(journal.pending()) == 1
        assert first.publication_write_blocked is True

        # New process, recovered SQLite/PostgreSQL: replay before opening
        # sends and leave a durable operator incident.
        class RecoveredSpool:
            def __init__(self, _path):
                pass
            def pending(self, _limit=128):
                return []
            def append(self, *_args):
                raise AssertionError("No new Telegram event required")

        monkeypatch.setattr(module, "PublicationMappingSpool", RecoveredSpool)
        stored, incidents = [], []
        repo = SimpleNamespace(
            record_telegram_publication_mappings=lambda account, peer, pairs: stored.extend(pairs),
            record_publication_inbox_overflow=lambda account, peer, ids: incidents.append((account, peer, ids)),
        )
        second = client()
        await telegram_publication.prepare_publication_inbox(second, repo)()
        assert stored == [(41, 141)]
        assert len(incidents) == 1
        assert incidents[0][0] == 7
        assert incidents[0][2] == [41]
        assert journal.pending() == []
        assert second.publication_write_blocked is True

    asyncio.run(scenario())


def test_fallback_journal_keeps_old_bytes_if_write_cannot_complete(tmp_path, monkeypatch):
    from app.publication_mapping_spool import PublicationFallbackJournal
    journal = PublicationFallbackJournal(tmp_path / "fallback.jsonl")
    journal.append(-10099, [12, 13], [112, 113])
    assert journal.pending() == [(-10099, [12, 13], [112, 113])]
    import os
    original = journal.path.read_bytes()
    def fail(*_args):
        raise OSError("replace failed")
    with monkeypatch.context() as patch:
        patch.setattr(os, "replace", fail)
        with pytest.raises(OSError):
            journal.clear()
    assert journal.path.read_bytes() == original
    journal.clear()
    assert journal.pending() == []



def test_irrecoverable_difference_records_durable_incident_without_advancing_cursor():
    from unittest.mock import AsyncMock
    from telethon import types
    from app.publication_difference_recovery import PublicationDifferenceRecovery

    async def scenario():
        date = datetime.now(timezone.utc)
        saved_incidents = []
        repo = SimpleNamespace(
            record_publication_inbox_overflow=lambda account, peer, ids:
                saved_incidents.append((account, peer, ids)),
        )
        client = AsyncMock(
            side_effect=[
                types.updates.DifferenceTooLong(pts=8888),
                types.updates.DifferenceTooLong(pts=8888),
            ],
        )
        inbox = SimpleNamespace(repository=repo, sender_account_id=7)
        recovery = PublicationDifferenceRecovery(client, inbox)
        recovery._advance = AsyncMock()
        cursor = {"pts": 42, "qts": 0, "state_date": date, "version": 1}
        assert await recovery._read_page(cursor) is False
        assert saved_incidents == [(7, 0, [42])]
        assert client.publication_write_blocked is True
        recovery._advance.assert_not_awaited()
    asyncio.run(scenario())



def test_corrupt_fallback_journal_cannot_strand_new_emergency_mapping(monkeypatch, tmp_path):
    from telethon import types
    from app import telegram_publication
    import app.publication_mapping_spool as module

    class FailedSpool:
        def __init__(self, _path):
            pass
        def pending(self, _limit=128):
            return []
        def append(self, *_args):
            raise OSError("SQLite unavailable")

    def partial_append(self, *_args):
        self.path.write_bytes(b'{"incomplete":')
        raise OSError("Journal write interrupted")

    monkeypatch.setattr(module, "PublicationMappingSpool", FailedSpool)
    monkeypatch.setattr(module.PublicationFallbackJournal, "append", partial_append)

    async def scenario():
        saved, incidents = [], []
        client = SimpleNamespace(session=SimpleNamespace(filename=str(tmp_path / "sender.session")))
        client.add_event_handler = lambda callback, _event: setattr(client, "handler", callback)
        async def get_me():
            return SimpleNamespace(id=7)
        client.get_me = get_me
        repo = SimpleNamespace(
            record_telegram_publication_mappings=lambda account, peer, pairs: saved.extend(pairs),
            record_publication_inbox_overflow=lambda account, peer, ids: incidents.append((account, peer, ids)),
        )
        await telegram_publication.prepare_publication_inbox(client, repo)()
        await client.handler(types.UpdateDeleteScheduledMessages(
            peer=types.PeerChannel(99), messages=[55], sent_messages=[155],
        ))
        assert saved == [(55, 155)]
        assert len(incidents) == 1 and incidents[0][2] == [55]
        assert client.publication_write_blocked is True
        assert client.publication_evidence_unpersisted is False
        journal = tmp_path / "sender.publication-mappings.fallback.jsonl"
        assert journal.read_bytes() == b'{"incomplete":'
        tasks = [task for task in asyncio.all_tasks()
                 if task.get_name() == "telegram-publication-persist-retry"]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_damaged_journal_preserves_valid_records_and_separates_new_append(tmp_path):
    from app.publication_mapping_spool import PublicationFallbackJournal
    journal = PublicationFallbackJournal(tmp_path / "fallback.jsonl")
    journal.append(-10099, [1], [101])
    with journal.path.open("ab") as stream:
        stream.write(b'{"incomplete":')
    journal.append(-10099, [2], [102])
    assert journal.read_recoverable() == ([(-10099, [1], [101]), (-10099, [2], [102])], True)
    assert b'{"incomplete":\n' in journal.path.read_bytes()
    with pytest.raises(RuntimeError):
        journal.pending()


def test_journal_io_waits_for_thread_completion_on_cancellation():
    import threading
    from app.telegram_publication import _journal_io
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    def write():
        started.set()
        try:
            assert release.wait(5)
        finally:
            finished.set()
    async def scenario():
        task = asyncio.create_task(_journal_io(write))
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
