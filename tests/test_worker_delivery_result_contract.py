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
