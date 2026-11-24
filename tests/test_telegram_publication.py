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
