import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.sender import SenderService
from app.worker_resource_policy import POLICY
from app.worker_runtime import _InFlightState, _process_job


TERMINAL_SINGLE = "telethon_send_accepted_target_id_unresolved_non_retryable"
TERMINAL_ONE_BY_ONE = "telethon_one_by_one_send_accepted_target_id_unresolved_non_retryable"


class _ExecutorRepo:
    def __init__(self, rows):
        self.rows = {int(k): dict(v) for k, v in rows.items()}
        self.failed_attempts = []
        self.attempts = {}

    def get_delivery_attempt_by_idempotency_key(self, key):
        return self.attempts.get(key)

    def create_delivery_attempt(self, **kwargs):
        self.attempts.setdefault(kwargs["idempotency_key"], {
            "status": kwargs["status"],
            "source_message_ids_json": list(kwargs.get("source_message_ids") or []),
        })
        return 1

    def mark_delivery_attempt_sending(self, key, **_kwargs):
        attempt = self.attempts[key]
        if attempt["status"] not in {"created", "failed_before_send"}:
            return False
        attempt["status"] = "sending"
        return True

    def mark_delivery_attempt_failed(self, key, *, status, error_text):
        self.failed_attempts.append((key, status, error_text))
        return True

    def get_rule(self, _rule_id):
        return SimpleNamespace(id=1)

    def get_delivery(self, delivery_id):
        return self.rows.get(int(delivery_id))

    def get_processing_album_for_rule(self, *_args):
        return [
            {"delivery_id": delivery_id, "message_id": delivery_id}
            for delivery_id in sorted(self.rows)
        ]


class _ExecutorSender(SenderService):
    def __init__(self, repo, *, delivery_ok=False):
        self.db = repo
        self.delivery_ok = delivery_ok
        self.single_calls = 0
        self.album_calls = 0

    async def _deliver_single(self, *_args, **_kwargs):
        self.single_calls += 1
        return self.delivery_ok

    async def _deliver_album(self, *_args, **_kwargs):
        self.album_calls += 1
        return self.delivery_ok

    def _touch_rule_after_send_sync(self, *_args, **_kwargs):
        return None


class _WorkerRepo:
    def __init__(self):
        self.retry_calls = []
        self.fail_calls = []
        self.complete_calls = []
        self.rows = {}

    def get_delivery(self, delivery_id):
        return self.rows.get(int(delivery_id), {"status": "processing"})

    def retry_job(self, job_id, error_text, delay_seconds):
        self.retry_calls.append((job_id, error_text, delay_seconds))
        return True

    def fail_job(self, job_id, error_text):
        self.fail_calls.append((job_id, error_text))
        return True

    def complete_job(self, job_id):
        self.complete_calls.append(job_id)
        return True

    def mark_job_processing(self, job_id, worker_id):
        return True


class _WorkerSender:
    def __init__(self, result):
        self.result = result
        self.calls = 0

    async def execute_repost_single_from_job(self, **_payload):
        self.calls += 1
        return self.result

    async def execute_repost_album_from_job(self, **_payload):
        self.calls += 1
        return self.result


def _non_retryable_result(reason=TERMINAL_SINGLE):
    return {
        "ok": False,
        "retryable": False,
        "accepted": True,
        "error_text": reason,
        "manual_review_required": True,
        "non_retryable": True,
        "transport_accepted": True,
        "authoritative_resolved": False,
    }


def test_single_executor_returns_non_retryable_for_terminal_faulty_delivery():
    repo = _ExecutorRepo({1: {"delivery_id": 1, "status": "faulty", "error_text": TERMINAL_SINGLE}})
    sender = _ExecutorSender(repo)

    result = asyncio.run(sender.execute_repost_single_from_job(
        rule_id=1, delivery_id=1, message_id=10, source_channel="src", target_id="dst",
    ))

    assert isinstance(result, dict)
    assert result["ok"] is False
    assert result["retryable"] is False
    assert result["accepted"] is True
    assert result["transport_accepted"] is True
    assert result["authoritative_resolved"] is False
    assert sender.single_calls == 1
    assert repo.failed_attempts
    assert repo.failed_attempts[-1][1] == "failed_after_send"
    assert all(status != "failed_before_send" for _key, status, _error in repo.failed_attempts)


def test_album_executor_returns_non_retryable_for_one_by_one_terminal_faulty_deliveries():
    repo = _ExecutorRepo({
        1: {"delivery_id": 1, "status": "faulty", "error_text": TERMINAL_ONE_BY_ONE},
        2: {"delivery_id": 2, "status": "faulty", "error_text": TERMINAL_ONE_BY_ONE},
    })
    sender = _ExecutorSender(repo)

    result = asyncio.run(sender.execute_repost_album_from_job(
        rule_id=1, delivery_id=1, message_id=1, source_channel="src", source_thread_id=None,
        media_group_id="g", target_id="dst", target_thread_id=None, delivery_ids=[1, 2],
    ))

    assert isinstance(result, dict)
    assert result["ok"] is False
    assert result["retryable"] is False
    assert result["accepted"] is True
    assert sender.album_calls == 1
    assert [repo.rows[1]["status"], repo.rows[2]["status"]] == ["faulty", "faulty"]
    assert repo.failed_attempts[-1][1] == "failed_after_send"
    assert all(status != "failed_before_send" for _key, status, _error in repo.failed_attempts)


def test_album_executor_returns_non_retryable_for_resolver_terminal_faulty_deliveries():
    repo = _ExecutorRepo({
        1: {"delivery_id": 1, "status": "faulty", "error_text": TERMINAL_SINGLE},
        2: {"delivery_id": 2, "status": "faulty", "error_text": TERMINAL_SINGLE},
    })
    sender = _ExecutorSender(repo)

    result = asyncio.run(sender.execute_repost_album_from_job(
        rule_id=1, delivery_id=1, message_id=1, source_channel="src", source_thread_id=None,
        media_group_id="g", target_id="dst", target_thread_id=None, delivery_ids=[1, 2],
    ))

    assert result["retryable"] is False
    assert result["accepted"] is True
    assert repo.failed_attempts[-1][1] == "failed_after_send"


def test_worker_fails_non_retryable_repost_single_without_retry():
    repo = _WorkerRepo()
    sender = _WorkerSender(_non_retryable_result())
    job = {"id": 10, "job_type": "repost_single", "payload": {"delivery_id": 1}, "attempts": 0, "max_attempts": 3}

    ok = asyncio.run(_process_job(repo, sender, "w", "light", job, policy=POLICY, state=_InFlightState()))

    assert ok is True
    assert sender.calls == 1
    assert repo.retry_calls == []
    assert len(repo.fail_calls) == 1


def test_worker_fails_non_retryable_repost_album_without_retry():
    repo = _WorkerRepo()
    sender = _WorkerSender(_non_retryable_result(TERMINAL_ONE_BY_ONE))
    job = {"id": 11, "job_type": "repost_album", "payload": {"delivery_ids": [1, 2]}, "attempts": 0, "max_attempts": 3}

    ok = asyncio.run(_process_job(repo, sender, "w", "light", job, policy=POLICY, state=_InFlightState()))

    assert ok is True
    assert sender.calls == 1
    assert repo.retry_calls == []
    assert len(repo.fail_calls) == 1


def test_worker_retries_retryable_presend_failure():
    repo = _WorkerRepo()
    sender = _WorkerSender({"ok": False, "retryable": True, "error_text": "pre_send_failure"})
    job = {"id": 12, "job_type": "repost_single", "payload": {"delivery_id": 1}, "attempts": 0, "max_attempts": 3}

    ok = asyncio.run(_process_job(repo, sender, "w", "light", job, policy=POLICY, state=_InFlightState()))

    assert ok is True
    assert sender.calls == 1
    assert repo.fail_calls == []
    assert len(repo.retry_calls) == 1


def test_single_executor_keeps_ambiguous_rpc_terminal_when_audit_fails():
    from app.send_intent import observe_publication

    repo = _ExecutorRepo({1: {"delivery_id": 1, "status": "processing"}})
    sender = _ExecutorSender(repo)

    async def send():
        raise TimeoutError("Telegram response lost")

    async def deliver(*_args, **_kwargs):
        try:
            await observe_publication(send)
        except TimeoutError:
            raise RuntimeError("audit database unavailable") from None

    sender._deliver_single = deliver
    result = asyncio.run(sender.execute_repost_single_from_job(
        rule_id=1, delivery_id=1, message_id=10, source_channel="src", target_id="dst",
    ))
    assert result["retryable"] is False
    assert repo.failed_attempts[-1][1] == "failed_after_send"


def test_album_executor_keeps_partial_rpc_terminal_when_audit_fails():
    from app.send_intent import observe_publication

    repo = _ExecutorRepo({1: {"delivery_id": 1, "status": "processing"},
                          2: {"delivery_id": 2, "status": "processing"}})
    sender = _ExecutorSender(repo)

    async def send():
        return [SimpleNamespace(message_id=20)]

    async def deliver(*_args, **_kwargs):
        await observe_publication(send, bot_result=True, expected_count=2)
        raise RuntimeError("audit database unavailable")

    sender._deliver_album = deliver
    result = asyncio.run(sender.execute_repost_album_from_job(
        rule_id=1, delivery_id=1, message_id=1, source_channel="src", source_thread_id=None,
        media_group_id="g", target_id="dst", target_thread_id=None, delivery_ids=[1, 2],
    ))
    assert result["retryable"] is False
    assert repo.failed_attempts[-1][1] == "failed_after_send"


@pytest.mark.parametrize("receipt_changes,allowed", [
    ({}, True),
    ({"delivery_ids": [2, 1]}, True),
    ({"delivery_ids": [1, 2, 3]}, False),
    ({"delivery_ids": [1, 1]}, False),
    ({"delivery_ids": [True, 2]}, False),
    ({"delivery_ids": None}, False),
    ({"tenant_id": 99}, False),
    ({"rule_id": 99}, False),
    ({"target_peer_id": -999}, False),
    ({"target_thread_id": 123}, False),
])
def test_worker_reconciles_exact_legacy_album_receipt_before_billing_gate(
    monkeypatch, receipt_changes, allowed,
):
    from app import worker_runtime
    from app.delivery_idempotency import build_delivery_idempotency_key

    repo = _WorkerRepo()
    sender = _WorkerSender({"ok": True})
    payload = {"rule_id": 1, "delivery_id": 1, "delivery_ids": [1, 2],
               "target_id": "-100123", "target_thread_id": None, "media_group_id": "g"}
    legacy_key = build_delivery_idempotency_key(
        operation_kind="album", rule_id=1, target_id="-100123", media_group_id="g",
    )
    receipt = {"rule_id": 1, "tenant_id": 1, "delivery_ids": [1, 2],
               "target_peer_id": -100123, "target_thread_id": None, **receipt_changes}
    repo.get_publication_receipt = Mock(side_effect=lambda key: receipt if key == legacy_key else None)
    billing = Mock(return_value=(False, "Подписка неактивна"))
    monkeypatch.setattr(worker_runtime.LimitService, "can_enqueue_job", billing)
    reconcile = AsyncMock(return_value={"ok": True, "accepted": True, "sent_message_ids": [501, 502]})
    monkeypatch.setattr(worker_runtime, "reconcile_pending_publication", reconcile)
    job = {"id": 15, "job_type": "repost_album", "payload_json": payload, "attempts": 0, "max_attempts": 3}

    assert asyncio.run(_process_job(repo, sender, "w", "light", job, policy=POLICY, state=_InFlightState()))
    assert sender.calls == 0
    if allowed:
        billing.assert_not_called()
        reconcile.assert_awaited_once_with(sender, legacy_key)
        assert repo.complete_calls == [15] and not repo.fail_calls
    else:
        billing.assert_called_once_with(1)
        reconcile.assert_not_awaited()
        assert len(repo.fail_calls) == 1 and not repo.complete_calls


def test_existing_current_receipt_has_priority_over_legacy_lookup():
    from app.single_publication_reconciliation import (
        existing_publication_key_for_job, publication_key_for_job,
    )
    payload = {"rule_id": 1, "delivery_id": 1, "delivery_ids": [1, 2],
               "target_id": "-100123", "media_group_id": "g"}
    modern = publication_key_for_job("repost_album", payload)
    lookup = Mock(return_value={"rule_id": 1, "tenant_id": 1})
    repo = SimpleNamespace(get_publication_receipt=lookup)
    assert asyncio.run(existing_publication_key_for_job(repo, "repost_album", payload, 1)) == modern
    lookup.assert_called_once_with(modern)


@pytest.mark.parametrize("job_type", ["repost_single", "repost_album"])
def test_worker_lost_echo_guard_ownership_never_sends(job_type):
    repo = _WorkerRepo()
    repo.quarantine_queued_self_target_echo_deliveries = Mock(return_value=None)
    sender = _WorkerSender({"ok": True})
    job = {"id": 16, "job_type": job_type, "payload_json": {"rule_id": 1,
           "delivery_id": 1, "delivery_ids": [1, 2]}, "attempts": 0, "max_attempts": 3}
    assert asyncio.run(_process_job(repo, sender, "w", "light", job, policy=POLICY, state=_InFlightState()))
    assert sender.calls == 0
    assert not repo.complete_calls and not repo.fail_calls and not repo.retry_calls



def test_album_definite_document_rejection_stays_terminal_on_replay():
    reason = "repost_original_document_invalid_non_retryable"

    class RejectedRepo(_ExecutorRepo):
        def mark_delivery_attempt_failed(self, key, *, status, error_text):
            self.attempts[key].update(status=status, error_text=error_text)
            return super().mark_delivery_attempt_failed(key, status=status, error_text=error_text)

    repo = RejectedRepo({
        1: {"delivery_id": 1, "status": "faulty", "error_text": reason},
        2: {"delivery_id": 2, "status": "faulty", "error_text": reason},
    })
    sender = _ExecutorSender(repo)
    for _ in range(2):
        result = asyncio.run(sender.execute_repost_album_from_job(
            rule_id=1, delivery_id=1, message_id=1, source_channel="src", source_thread_id=None,
            media_group_id="g", target_id="dst", target_thread_id=None, delivery_ids=[1, 2],
        ))
        assert result["retryable"] is False
        assert result["accepted"] is False
        assert result["error_text"] == reason
    assert sender.album_calls == 1
    assert repo.failed_attempts[-1][1:] == ("failed_before_send", reason)
