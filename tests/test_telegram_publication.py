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
