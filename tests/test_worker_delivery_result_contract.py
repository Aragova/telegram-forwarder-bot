import asyncio

from aiogram.exceptions import TelegramEntityTooLarge
from aiogram.methods import SendVideo

from app.worker_resource_policy import POLICY
from app.worker_runtime import _InFlightState, _process_job


class DummySub:
    def get_active_subscription(self, _tenant_id):
        return {}


class DummyUsage:
    def __init__(self):
        self.video_count = 0

    def get_today_usage(self, _tenant_id):
        return {}

    def increment_video(self, *_args, **_kwargs):
        self.video_count += 1


class FakeRepo:
    def __init__(self):
        self.completed = []
        self.retried = []
        self.failed = []

    def get_rule_tenant_id(self, _rule_id):
        return 1

    def mark_job_processing(self, *_args):
        return True

    def complete_job(self, job_id):
        self.completed.append(job_id)
        return True

    def retry_job(self, job_id, error, delay):
        self.retried.append((job_id, error, delay))
        return True

    def fail_job(self, job_id, error):
        self.failed.append((job_id, error))
        return True

    def get_delivery(self, _delivery_id):
        return {
            "id": 103,
            "rule_id": 1,
            "message_id": 1,
            "source_channel": "-100src",
            "source_thread_id": None,
            "target_id": "-100dst",
            "target_thread_id": None,
            "status": "processing",
        }

    def get_rule(self, _rule_id):
        class _Rule:
            mode = "video"
            interval = 0
            schedule_mode = "interval"

        return _Rule()

    def create_job(self, **_kwargs):
        return 999


class FakeSender:
    def __init__(self, *, video_delivery_result=True, video_send_result=None):
        self.video_delivery_result = video_delivery_result
        self.video_send_result = video_send_result if video_send_result is not None else {"ok": True}

    async def execute_video_delivery_from_job(self, **_payload):
        return self.video_delivery_result

    async def execute_video_send_from_job(self, **_payload):
        return self.video_send_result


def _run(repo, sender, job):
    return asyncio.run(_process_job(repo, sender, "w", "heavy", job, policy=POLICY, state=_InFlightState()))


def test_video_delivery_bool_true_contract_compatible():
    repo = FakeRepo()
    sender = FakeSender(video_delivery_result=True)
    ok = _run(repo, sender, {"id": 1, "job_type": "video_delivery", "payload_json": {"delivery_id": 101, "rule_id": 1}})
    assert ok is True
    assert repo.completed == [1]
    assert repo.retried == []
    assert repo.failed == []


def test_video_delivery_dict_contract_compatible():
    repo = FakeRepo()
    sender = FakeSender(video_delivery_result={"ok": True, "sent_message_ids": [123]})
    ok = _run(repo, sender, {"id": 2, "job_type": "video_delivery", "payload_json": {"delivery_id": 102, "rule_id": 1}})
    assert ok is True
    assert repo.completed == [2]


def test_video_send_fallback_to_legacy_contract_compatible():
    repo = FakeRepo()
    sender = FakeSender(video_send_result={"ok": False, "fallback_to_legacy": True})
    ok = _run(repo, sender, {"id": 3, "job_type": "video_send", "payload_json": {"delivery_id": 103, "rule_id": 1}})
    assert ok is True
    assert repo.completed == [3]


def test_explicit_telegram_flood_wait_honors_retry_after():
    repo = FakeRepo()
    sender = FakeSender(video_send_result={
        "ok": False, "retryable": True, "accepted": False,
        "retry_after_seconds": 120,
        "error_text": "FloodWaitError: retry_after=120",
    })
    ok = _run(repo, sender, {
        "id": 120, "job_type": "video_send",
        "payload_json": {"delivery_id": 103, "rule_id": 1},
        "attempts": 0, "max_attempts": 4,
    })
    assert ok is True
    assert repo.failed == []
    assert len(repo.retried) == 1
    assert repo.retried[0][0] == 120
    assert repo.retried[0][2] >= 121


def test_non_retryable_failure_preserved():
    repo = FakeRepo()
    sender = FakeSender(video_delivery_result={"ok": False, "retryable": False, "error_text": "manual review"})
    ok = _run(repo, sender, {"id": 4, "job_type": "video_delivery", "payload_json": {"delivery_id": 104, "rule_id": 1, "max_attempts": 3}})
    assert ok is True
    assert repo.failed == [(4, "manual review")]
    assert repo.retried == []



def test_legacy_video_413_fails_once_without_job_retry():
    class RejectingSender(FakeSender):
        async def execute_video_delivery_from_job(self, **_payload):
            raise TelegramEntityTooLarge(
                method=SendVideo(chat_id=123, video="id"),
                message="Request Entity Too Large",
            )

    repo = FakeRepo()
    ok = _run(
        repo, RejectingSender(),
        {"id": 55, "job_type": "video_delivery", "payload_json": {"delivery_id": 103, "rule_id": 1}},
    )
    assert ok is True
    assert len(repo.failed) == 1 and repo.failed[0][0] == 55
    assert repo.retried == []


def test_repost_orphan_finalizes_before_last_retry_job_failure():
    class OrphanRepo(FakeRepo):
        finalized = None

        def heartbeat_job_processing(self, job_id, worker_id):
            return True

        def finalize_orphaned_repost_attempt(self, **kwargs):
            self.finalized = kwargs
            return 1

    class RepostSender:
        async def execute_repost_single_from_job(self, **_kwargs):
            return {
                "ok": False, "retryable": True, "accepted": False,
                "manual_review_required": True,
                "idempotency_key": "delivery:103:target:-100dst:single",
                "error_text": "repost attempt orphaned after unclear Telegram response",
            }

    repo = OrphanRepo()
    ok = asyncio.run(_process_job(
        repo, RepostSender(), "w", "light",
        {"id": 113, "job_type": "repost_single", "attempts": 2, "max_attempts": 3,
         "payload_json": {"delivery_id": 103, "rule_id": 1, "message_id": 5,
                          "source_channel": "-100src", "target_id": "-100dst"}},
        policy=POLICY, state=_InFlightState(),
    ))
    assert ok is True
    assert repo.finalized["job_id"] == 113
    assert repo.finalized["delivery_ids"] == [103]
    assert len(repo.failed) == 1
    assert repo.retried == []


def test_repost_orphan_finalization_skips_lost_worker_lease():
    class OrphanRepo(FakeRepo):
        finalized = False

        def heartbeat_job_processing(self, job_id, worker_id):
            return False

        def finalize_orphaned_repost_attempt(self, **_kwargs):
            self.finalized = True
            return 1

    class RepostSender:
        async def execute_repost_single_from_job(self, **_kwargs):
            return {
                "ok": False, "retryable": True,
                "manual_review_required": True,
                "idempotency_key": "delivery:103:target:-100dst:single",
                "error_text": "uncertain",
            }

    repo = OrphanRepo()
    ok = asyncio.run(_process_job(
        repo, RepostSender(), "w", "light",
        {"id": 114, "job_type": "repost_single", "attempts": 2, "max_attempts": 3,
         "payload_json": {"delivery_id": 103, "rule_id": 1, "message_id": 5,
                          "source_channel": "-100src", "target_id": "-100dst"}},
        policy=POLICY, state=_InFlightState(),
    ))
    assert ok is True
    assert repo.finalized is False
    assert repo.failed == []


def test_repost_orphan_last_attempt_veto_defers_without_failing_or_resending():
    class ContendedRepo(FakeRepo):
        def __init__(self):
            super().__init__()
            self.deferred = []
            self.finalize_calls = 0

        def heartbeat_job_processing(self, job_id, worker_id):
            return True

        def finalize_orphaned_repost_attempt(self, **_kwargs):
            self.finalize_calls += 1
            return 0  # Another live worker owns the operation.

        def defer_job(self, job_id, error, delay):
            self.deferred.append((job_id, error, delay))
            return True

    class RepostSender:
        def __init__(self):
            self.calls = 0

        async def execute_repost_single_from_job(self, **_kwargs):
            self.calls += 1
            return {"ok": False, "retryable": True, "manual_review_required": True,
                    "idempotency_key": "delivery:103:target:-100dst:single", "error_text": "pending owner"}

    repo = ContendedRepo()
    sender = RepostSender()
    job = {"id": 115, "job_type": "repost_single", "attempts": 2, "max_attempts": 3,
           "payload_json": {"delivery_id": 103, "rule_id": 1, "message_id": 5,
                            "source_channel": "-100src", "target_id": "-100dst"}}
    assert asyncio.run(_process_job(repo, sender, "w", "light", job, policy=POLICY, state=_InFlightState()))
    assert repo.finalize_calls == 1
    assert len(repo.deferred) == 1
    assert repo.deferred[0][0] == 115
    assert repo.deferred[0][2] >= 30
    assert repo.failed == []
    assert sender.calls == 1


def test_repost_orphan_finalizer_db_exception_defers_recovery():
    class Repo(FakeRepo):
        def __init__(self):
            super().__init__()
            self.deferred = []

        def heartbeat_job_processing(self, _job_id, _worker_id):
            return True

        def finalize_orphaned_repost_attempt(self, **_kwargs):
            raise RuntimeError("temporary database failure")

        def defer_job(self, job_id, error, delay):
            self.deferred.append((job_id, error, delay))
            return True

    class RepostSender:
        async def execute_repost_single_from_job(self, **_kwargs):
            return {"ok": False, "retryable": True, "manual_review_required": True,
                    "idempotency_key": "delivery:103:target:-100dst:single", "error_text": "uncertain"}

    repo = Repo()
    assert asyncio.run(_process_job(
        repo, RepostSender(), "w", "light",
        {"id": 116, "job_type": "repost_single", "attempts": 2, "max_attempts": 3,
         "payload_json": {"delivery_id": 103, "rule_id": 1, "message_id": 5,
                          "source_channel": "-100src", "target_id": "-100dst"}},
        policy=POLICY, state=_InFlightState(),
    ))
    assert repo.failed == []
    assert len(repo.deferred) == 1
    assert repo.deferred[0][2] >= 30


def test_repost_orphan_zero_result_with_already_faulty_delivery_does_not_defer_forever():
    class Repo(FakeRepo):
        def __init__(self):
            super().__init__()
            self.deferred = []

        def heartbeat_job_processing(self, _job_id, _worker_id):
            return True

        def finalize_orphaned_repost_attempt(self, **_kwargs):
            return 0

        def get_delivery(self, _delivery_id):
            return {"status": "faulty"}

        def defer_job(self, job_id, error, delay):
            self.deferred.append(job_id)
            return True

    class Sender:
        async def execute_repost_single_from_job(self, **_kwargs):
            return {"ok": False, "retryable": True, "manual_review_required": True,
                    "idempotency_key": "delivery:103:target:-100dst:single"}

    repo = Repo()
    assert asyncio.run(_process_job(
        repo, Sender(), "w", "light",
        {"id": 117, "job_type": "repost_single", "attempts": 2, "max_attempts": 3,
         "payload_json": {"delivery_id": 103, "rule_id": 1, "message_id": 5,
                          "source_channel": "-100src", "target_id": "-100dst"}},
        policy=POLICY, state=_InFlightState(),
    ))
    assert repo.failed and repo.failed[0][0] == 117
    assert repo.deferred == []



def test_publication_reconciliation_db_failure_defers_at_last_attempt(monkeypatch):
    import app.worker_runtime as runtime

    class ReceiptRepo(FakeRepo):
        def __init__(self):
            super().__init__()
            self.deferred = []

        def get_publication_receipt(self, key):
            return {"rule_id": 1, "tenant_id": 1}

        def defer_job(self, job_id, error, delay):
            self.deferred.append((job_id, delay))
            return True

    async def broken_reconcile(sender, key):
        raise RuntimeError("temporary database failure")

    class NoSend:
        async def execute_video_send_from_job(self, **kwargs):
            raise AssertionError("Accepted publication must never be resent")

    monkeypatch.setattr(runtime, "reconcile_pending_publication", broken_reconcile)
    repo = ReceiptRepo()
    assert _run(repo, NoSend(), {
        "id": 901, "job_type": "video_send",
        "payload_json": {"rule_id": 1, "delivery_id": 103, "target_id": "-100dst"},
        "attempts": 2, "max_attempts": 3,
    })
    assert repo.deferred == [(901, 30)]
    assert not repo.failed and not repo.retried and not repo.completed
