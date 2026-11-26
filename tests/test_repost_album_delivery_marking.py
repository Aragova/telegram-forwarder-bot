import asyncio
from types import SimpleNamespace

import pytest

from app.sender import SenderService
from app.delivery_idempotency import build_delivery_idempotency_key


def _album_key(delivery_ids=None):
    return build_delivery_idempotency_key(
        operation_kind="album", rule_id=111, target_id="dst",
        media_group_id="album-1", source_message_ids=delivery_ids,
    )


class FakeAlbumRepo:
    def __init__(self):
        self.attempt_by_key = {}
        self.marked = []
        self.rows = [
            {"delivery_id": 1, "message_id": 101},
            {"delivery_id": 2, "message_id": 102},
        ]
        self.fail_mark = False
        self.faulty = []
        self.promoted = []
        self.rpc_members = []
        self.verification_result = {"ok": False, "sent_message_ids": []}

    def get_delivery_attempt_by_idempotency_key(self, key):
        return self.attempt_by_key.get(key)

    def create_delivery_attempt(self, **kwargs):
        self.attempt_by_key.setdefault(kwargs["idempotency_key"], {
            "status": kwargs["status"],
            "source_message_ids_json": list(kwargs["source_message_ids"]),
        })
        return 1

    def mark_delivery_attempt_sending(self, key, **_kwargs):
        attempt = self.attempt_by_key[key]
        if attempt["status"] not in {"created", "failed_before_send"}:
            return False
        attempt["status"] = "sending"
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


async def _execute(repo, *, delivery_ids=None, pre_send_echo_guard=None):
    service = SenderService.__new__(SenderService)
    service.db = repo

    async def fake_deliver_album(_rule, rows, *_args, **kwargs):
        repo.rpc_members.append([row["delivery_id"] for row in rows])
        repo.attempt_by_key[kwargs["idempotency_key"]].update(
            status="verified", sent_message_ids_json=[501, 502],
        )
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
        pre_send_echo_guard=pre_send_echo_guard,
    )


def test_repost_album_success_marks_all_deliveries_sent():
    repo = FakeAlbumRepo()

    ok = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert ok is True
    assert [delivery_id for delivery_id, _kwargs in repo.marked] == [1, 2]


def test_repost_album_cache_hit_marks_all_deliveries_sent():
    repo = FakeAlbumRepo()
    repo.attempt_by_key[_album_key([1, 2])] = {"status": "verified", "sent_message_ids_json": [501, 502], "source_message_ids_json": [1, 2]}

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
    repo.attempt_by_key[_album_key([1, 2])] = {"status": "accepted", "sent_message_ids_json": [501, 502], "source_message_ids_json": [1, 2]}

    result = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert result["ok"] is False
    assert result["retryable"] is True
    assert result["accepted"] is False
    assert repo.marked == []
    assert repo.faulty == []


def test_repost_album_accepted_cache_is_read_only_reconciled_when_messages_exist():
    repo = FakeAlbumRepo()
    repo.attempt_by_key[_album_key([1, 2])] = {"status": "accepted", "sent_message_ids_json": [501, 502], "source_message_ids_json": [1, 2]}
    repo.verification_result = {"ok": True, "sent_message_ids": [501, 502]}

    ok = asyncio.run(_execute(repo, delivery_ids=[1, 2]))

    assert ok is True
    assert repo.promoted and repo.promoted[0][1] == [501, 502]
    assert [delivery_id for delivery_id, _ in repo.marked] == [1, 2]
    assert all(kwargs["delivery_method"] == "idempotency_album_cache_reconciled" for _, kwargs in repo.marked)


def test_repost_album_accepted_cache_does_not_recover_partial_copy():
    repo = FakeAlbumRepo()
    repo.attempt_by_key[_album_key([1, 2])] = {"status": "accepted", "sent_message_ids_json": [501], "source_message_ids_json": [1, 2]}
    repo.verification_result = {"ok": True, "sent_message_ids": [501]}

    result = asyncio.run(_execute(repo, delivery_ids=[1, 2]))
    assert result["ok"] is False
    assert result["retryable"] is True
    assert repo.marked == []
    assert repo.promoted == []


def test_verified_legacy_album_cache_preserves_member_target_id_association_when_reordered():
    repo = FakeAlbumRepo()
    repo.attempt_by_key[_album_key()] = {
        "status": "verified",
        "sent_message_ids_json": [501, 502],
        "source_message_ids_json": [1, 2],
    }
    result = asyncio.run(_execute(repo, delivery_ids=[2, 1]))
    assert result is True
    assert set(repo.attempt_by_key) == {_album_key()}
    assert repo.marked[0][0] == 2
    assert repo.marked[0][1]["sent_message_id"] == 502
    assert repo.marked[1][0] == 1
    assert repo.marked[1][1]["sent_message_id"] == 501


def test_modern_album_cache_is_looked_up_by_member_fingerprint_key_only():
    repo = FakeAlbumRepo()
    key = _album_key([1, 2])
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


@pytest.mark.parametrize("legacy", [False, True])
def test_accepted_album_cache_keeps_original_member_mapping_after_sorted_verification(legacy):
    repo = FakeAlbumRepo()
    key = _album_key() if legacy else _album_key([1, 2])
    repo.attempt_by_key[key] = {
        "status": "accepted", "sent_message_ids_json": [502, 501],
        "source_message_ids_json": [1, 2],
    }
    # Telegram verification reports IDs sorted, not in current job order.
    repo.verification_result = {"ok": True, "sent_message_ids": [501, 502]}
    assert asyncio.run(_execute(repo, delivery_ids=[2, 1])) is True
    assert repo.promoted == [(key, [502, 501])]
    assert [(delivery_id, row["sent_message_id"]) for delivery_id, row in repo.marked] == [(2, 501), (1, 502)]


@pytest.mark.parametrize("verified_ids", [[501, 999], [501, 501], []])
def test_accepted_album_cache_rejects_wrong_verified_id_set(verified_ids):
    repo = FakeAlbumRepo()
    key = _album_key([1, 2])
    repo.attempt_by_key[key] = {
        "status": "accepted", "sent_message_ids_json": [501, 502],
        "source_message_ids_json": [1, 2],
    }
    repo.verification_result = {"ok": True, "sent_message_ids": verified_ids}
    result = asyncio.run(_execute(repo, delivery_ids=[2, 1]))
    assert result["ok"] is False
    assert result["manual_review_required"] is True
    assert repo.promoted == [] and repo.marked == []


@pytest.mark.parametrize("status", ["created", "failed_before_send"])
@pytest.mark.parametrize("ledger_order", [[1, 2], [2, 1]])
def test_album_retry_uses_persisted_order_for_send_and_verified_cache(status, ledger_order):
    repo = FakeAlbumRepo()
    key = _album_key([1, 2])
    repo.attempt_by_key[key] = {
        "status": status, "source_message_ids_json": ledger_order,
    }
    retry_order = list(reversed(ledger_order))
    assert asyncio.run(_execute(repo, delivery_ids=retry_order)) is True
    expected = dict(zip(ledger_order, [501, 502]))
    assert repo.rpc_members == [ledger_order]
    assert {member: row["sent_message_id"] for member, row in repo.marked} == expected
    repo.marked.clear()
    assert asyncio.run(_execute(repo, delivery_ids=retry_order)) is True
    assert repo.rpc_members == [ledger_order]
    assert {member: row["sent_message_id"] for member, row in repo.marked} == expected


@pytest.mark.parametrize("members", [None, [1, 1], [1, 3], [True, 2]])
def test_album_retry_rejects_invalid_persisted_vector_before_rpc(members):
    repo = FakeAlbumRepo()
    repo.attempt_by_key[_album_key([1, 2])] = {
        "status": "failed_before_send", "source_message_ids_json": members,
    }
    result = asyncio.run(_execute(repo, delivery_ids=[2, 1]))
    assert result["ok"] is False
    assert result["retryable"] is False
    assert repo.rpc_members == [] and repo.marked == []


@pytest.mark.parametrize("returned", [[501, 502], [501, 999], [501, 501]])
def test_album_verification_preserves_transport_order_and_requires_exact_ids(returned):
    from unittest.mock import AsyncMock
    from app.sender_telethon_helpers import SenderTelethonHelpers

    telethon = SimpleNamespace(get_messages=AsyncMock(return_value=[
        SimpleNamespace(id=message_id, grouped_id=7) for message_id in returned
    ]))
    result = asyncio.run(SenderTelethonHelpers(SimpleNamespace(telethon=telethon)).verify_album_delivery(
        target_id="-100123", expected_count=2, sent_message_ids=[502, 501],
    ))
    if returned == [501, 502]:
        assert result["ok"] is True
        assert result["sent_message_ids"] == [502, 501]
    else:
        assert result["ok"] is False


def test_copy_messages_sorts_request_but_preserves_ledger_mapping(monkeypatch):
    from unittest.mock import AsyncMock
    from types import SimpleNamespace
    from app import sender_botapi_copy_helpers as module
    monkeypatch.setattr(module, "DEBUG_FORCE_SKIP_COPY_ALBUM", False)
    bot = AsyncMock(return_value=[SimpleNamespace(message_id=501), SimpleNamespace(message_id=502)])
    helper = module.SenderBotApiCopyHelpers(SimpleNamespace(bot=bot))
    result = asyncio.run(helper.copy_album_via_bot("-1001", "-1002", [102, 101], None))
    assert list(bot.await_args.args[0].message_ids) == [101, 102]
    assert result["sent_message_ids"] == [502, 501]


@pytest.mark.parametrize("echoes", [[1, 2], None, []])
def test_late_album_echo_guard_blocks_rpc(echoes):
    repo = FakeAlbumRepo()

    async def guard(members):
        assert members == [1, 2]
        return echoes

    result = asyncio.run(_execute(repo, pre_send_echo_guard=guard))
    if echoes is None or echoes:
        assert result["self_loop_echo_fenced"] is True
        assert not repo.rpc_members and not repo.marked
    else:
        assert result is True
        assert repo.rpc_members == [[1, 2]]



def test_reopened_album_member_with_changed_vector_is_not_resent():
    reason = "repost_original_document_invalid_non_retryable"
    old_key = _album_key([1, 2])

    class RejectedAlbumRepo(FakeAlbumRepo):
        def find_conflicting_album_attempt(self, **kwargs):
            assert kwargs["current_delivery_ids"] == [2, 3]
            return {"idempotency_key": old_key, "status": "failed_before_send",
                    "error_text": reason, "source_message_ids_json": [1, 2]}

        def mark_delivery_faulty_if_attempt_unverified(self, delivery_id, key, error_text):
            assert key == old_key
            self.faulty.append((delivery_id, error_text))
            return True

    repo = RejectedAlbumRepo()
    repo.attempt_by_key[old_key] = {"status": "failed_before_send", "error_text": reason}
    result = asyncio.run(_execute(repo, delivery_ids=[2, 3]))
    assert result["retryable"] is False
    assert result["accepted"] is False
    assert repo.rpc_members == []
    assert repo.faulty == [(2, reason), (3, reason)]
    assert list(repo.attempt_by_key) == [old_key]
