import asyncio
from types import SimpleNamespace

import pytest

from app.sender import SenderService


class FakeAlbumRepo:
    def __init__(self):
        self.attempt = None
        self.attempt_by_key = {}
        self.marked = []
        self.rows = [
            {"delivery_id": 1, "message_id": 101},
            {"delivery_id": 2, "message_id": 102},
        ]
        self.fail_mark = False
        self.faulty = []
        self.promoted = []
        self.verification_result = {"ok": False, "sent_message_ids": []}

    def get_delivery_attempt_by_idempotency_key(self, key):
        if self.attempt_by_key:
            return self.attempt_by_key.get(key)
        return self.attempt

    def create_delivery_attempt(self, **_kwargs):
        return 1

    def mark_delivery_attempt_sending(self, *_args, **_kwargs):
        return True

    def mark_delivery_faulty(self, delivery_id, error_text):
        self.faulty.append((delivery_id, error_text))

    def mark_delivery_attempt_failed(self, *_args, **_kwargs):
        return None

    def mark_delivery_attempt_verified(self, key, *, sent_message_ids):
        self.promoted.append((key, list(sent_message_ids)))
        return True

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
        repo.attempt = {"status": "verified", "sent_message_ids_json": [501, 502], "source_message_ids_json": [1, 2]}
        return True

    service._deliver_album = fake_deliver_album

    async def fake_verify_album(**_kwargs):
        return repo.verification_result

    service._verify_album_delivery = fake_verify_album
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
    repo.attempt = {"status": "verified", "sent_message_ids_json": [501, 502], "source_message_ids_json": [1, 2]}

    ok = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert ok is True
    assert [delivery_id for delivery_id, _kwargs in repo.marked] == [1, 2]
    assert [kwargs["delivery_method"] for _delivery_id, kwargs in repo.marked] == ["idempotency_cache", "idempotency_cache"]


def test_repost_album_no_false_success_if_mark_sent_fails():
    repo = FakeAlbumRepo()
    repo.fail_mark = True

    with pytest.raises(RuntimeError, match="mark failed"):
        asyncio.run(_execute(repo, delivery_ids=[1, 2]))


def test_repost_album_accepted_cache_does_not_fake_success_or_resend():
    repo = FakeAlbumRepo()
    repo.attempt = {"status": "accepted", "sent_message_ids_json": [501, 502], "source_message_ids_json": [1, 2]}

    result = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert result["ok"] is False
    assert result["retryable"] is True
    assert result["accepted"] is False
    assert repo.marked == []
    assert repo.faulty == []


def test_repost_album_accepted_cache_is_read_only_reconciled_when_messages_exist():
    repo = FakeAlbumRepo()
    repo.attempt = {"status": "accepted", "sent_message_ids_json": [501, 502], "source_message_ids_json": [1, 2]}
    repo.verification_result = {"ok": True, "sent_message_ids": [501, 502]}

    ok = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert ok is True
    assert repo.promoted and repo.promoted[0][1] == [501, 502]
    assert [delivery_id for delivery_id, _ in repo.marked] == [1, 2]
    assert all(kwargs["delivery_method"] == "idempotency_album_cache_reconciled" for _, kwargs in repo.marked)


def test_repost_album_accepted_cache_does_not_recover_partial_copy():
    repo = FakeAlbumRepo()
    repo.attempt = {"status": "accepted", "sent_message_ids_json": [501], "source_message_ids_json": [1, 2]}
    repo.verification_result = {"ok": True, "sent_message_ids": [501]}

    result = asyncio.run(_execute(repo, delivery_ids=[1, 2]))
    assert result["ok"] is False
    assert result["retryable"] is True
    assert repo.marked == []
    assert repo.promoted == []


def test_verified_legacy_album_cache_preserves_member_target_id_association_when_reordered():
    repo = FakeAlbumRepo()
    repo.attempt = {
        "status": "verified",
        "sent_message_ids_json": [501, 502],
        "source_message_ids_json": [1, 2],
    }
    result = asyncio.run(_execute(repo, delivery_ids=[2, 1]))
    assert result is True
    assert repo.marked[0][0] == 2
    assert repo.marked[0][1]["sent_message_id"] == 502
    assert repo.marked[1][0] == 1
    assert repo.marked[1][1]["sent_message_id"] == 501


def test_modern_album_cache_is_looked_up_by_member_fingerprint_key_only():
    from app.delivery_idempotency import build_delivery_idempotency_key

    repo = FakeAlbumRepo()
    key = build_delivery_idempotency_key(
        operation_kind="album", rule_id=111, target_id="dst",
        media_group_id="album-1", source_message_ids=[1, 2],
    )
    repo.attempt_by_key[key] = {
        "status": "verified",
        "sent_message_ids_json": [501, 502],
        "source_message_ids_json": [1, 2],
    }
    result = asyncio.run(_execute(repo))
    assert result is True
    assert [row[0] for row in repo.marked] == [1, 2]


def test_stale_album_payload_returns_terminal_no_rpc_and_requests_safe_release():
    repo = FakeAlbumRepo()
    repo.rows.append({"delivery_id": 3, "message_id": 103})
    service = SenderService.__new__(SenderService)
    service.db = repo

    async def should_not_send(*_args, **_kwargs):
        raise AssertionError("Changed album must never reach Telegram")

    service._deliver_album = should_not_send
    result = asyncio.run(service.execute_repost_album_from_job(
        rule_id=111, delivery_id=1, message_id=101,
        source_channel="src", source_thread_id=None, media_group_id="album-1",
        target_id="dst", target_thread_id=None, delivery_ids=[1, 2],
    ))
    assert result["ok"] is False
    assert result["retryable"] is False
    assert result["album_membership_changed_pre_send"] is True
    assert result["manual_review_required"] is True
