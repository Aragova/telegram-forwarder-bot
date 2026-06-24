import asyncio
from types import SimpleNamespace

import pytest

from app.sender import SenderService


class FakeAlbumRepo:
    def __init__(self):
        self.attempt = None
        self.marked = []
        self.rows = [
            {"delivery_id": 1, "message_id": 101},
            {"delivery_id": 2, "message_id": 102},
        ]
        self.fail_mark = False

    def get_delivery_attempt_by_idempotency_key(self, _key):
        return self.attempt

    def create_delivery_attempt(self, **_kwargs):
        return 1

    def mark_delivery_attempt_sending(self, *_args, **_kwargs):
        return None

    def mark_delivery_attempt_failed(self, *_args, **_kwargs):
        return None

    def get_rule(self, rule_id):
        return SimpleNamespace(id=rule_id)

    def get_processing_album_for_rule(self, *_args):
        return self.rows

    def mark_delivery_sent_with_target_message(self, delivery_id, **kwargs):
        if self.fail_mark:
            raise RuntimeError("mark failed")
        self.marked.append((delivery_id, kwargs))

    def mark_many_deliveries_sent(self, delivery_ids):
        if self.fail_mark:
            raise RuntimeError("mark failed")
        for delivery_id in delivery_ids:
            self.marked.append((delivery_id, {}))


async def _execute(repo, *, delivery_ids=None):
    service = SenderService.__new__(SenderService)
    service.db = repo

    async def fake_deliver_album(*_args, **_kwargs):
        return True

    service._deliver_album = fake_deliver_album
    service._touch_rule_after_send_sync = lambda *_args, **_kwargs: None
    return await service.execute_repost_album_from_job(
        rule_id=111,
        delivery_id=1,
        message_id=101,
        source_channel="src",
        source_thread_id=None,
        media_group_id="album-1",
        target_id="dst",
        target_thread_id=None,
        delivery_ids=delivery_ids or [1, 2],
    )


def test_repost_album_success_marks_all_deliveries_sent():
    repo = FakeAlbumRepo()

    ok = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert ok is True
    assert [delivery_id for delivery_id, _kwargs in repo.marked] == [1, 2]


def test_repost_album_cache_hit_marks_all_deliveries_sent():
    repo = FakeAlbumRepo()
    repo.attempt = {"status": "accepted", "sent_message_ids_json": [501, 502]}

    ok = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert ok is True
    assert [delivery_id for delivery_id, _kwargs in repo.marked] == [1, 2]
    assert [kwargs["delivery_method"] for _delivery_id, kwargs in repo.marked] == ["idempotency_cache", "idempotency_cache"]


def test_repost_album_no_false_success_if_mark_sent_fails():
    repo = FakeAlbumRepo()
    repo.fail_mark = True

    with pytest.raises(RuntimeError, match="mark failed"):
        asyncio.run(_execute(repo, delivery_ids=[1, 2]))
