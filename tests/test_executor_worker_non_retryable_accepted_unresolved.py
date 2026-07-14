import asyncio
from types import SimpleNamespace

from app.sender import SenderService
from app.worker_resource_policy import POLICY
from app.worker_runtime import _InFlightState, _process_job


TERMINAL_SINGLE = "telethon_send_accepted_target_id_unresolved_non_retryable"
TERMINAL_ONE_BY_ONE = "telethon_one_by_one_send_accepted_target_id_unresolved_non_retryable"


class _ExecutorRepo:
    def __init__(self, rows):
        self.rows = {int(k): dict(v) for k, v in rows.items()}
        self.failed_attempts = []

    def get_delivery_attempt_by_idempotency_key(self, _key):
        return None

    def create_delivery_attempt(self, **_kwargs):
        return 1

    def mark_delivery_attempt_sending(self, *_args, **_kwargs):
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
