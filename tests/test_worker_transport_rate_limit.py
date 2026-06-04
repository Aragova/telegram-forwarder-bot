from __future__ import annotations

import asyncio

from app.transport_policy import TransportRateLimited
from app.worker_resource_policy import WorkerResourcePolicy
from app.worker_runtime import _InFlightState, _process_job


class _RateLimitRepo:
    def __init__(self) -> None:
        self.defer_calls: list[tuple[int, str, int]] = []
        self.retry_calls: list[tuple[int, str, int]] = []
        self.fail_calls: list[tuple[int, str]] = []
        self.complete_calls: list[int] = []
        self.video_events: list[dict] = []
        self.delivery_events: list[dict] = []
        self.faulty_calls: list[tuple[int, str]] = []
        self.post_ids: dict[int, int] = {123: 987}

    def get_rule_tenant_id(self, rule_id: int) -> int:
        return 1

    def get_delivery(self, delivery_id: int):
        return {"id": int(delivery_id), "status": "pending"}

    def mark_job_processing(self, job_id: int, worker_id: str) -> bool:
        return True

    def defer_job(self, job_id: int, error_text: str, delay_seconds: int) -> bool:
        self.defer_calls.append((int(job_id), str(error_text), int(delay_seconds)))
        return True

    def retry_job(self, job_id: int, error_text: str, delay_seconds: int) -> bool:
        self.retry_calls.append((int(job_id), str(error_text), int(delay_seconds)))
        raise AssertionError("retry_job must not be called for TransportRateLimited")

    def fail_job(self, job_id: int, error_text: str) -> bool:
        self.fail_calls.append((int(job_id), str(error_text)))
        raise AssertionError("fail_job must not be called for TransportRateLimited")

    def complete_job(self, job_id: int) -> bool:
        self.complete_calls.append(int(job_id))
        raise AssertionError("complete_job must not be called for TransportRateLimited")

    def mark_delivery_faulty(self, delivery_id: int, error_text: str) -> None:
        self.faulty_calls.append((int(delivery_id), str(error_text)))
        raise AssertionError("mark_delivery_faulty must not be called for TransportRateLimited")

    def get_post_id_by_delivery(self, delivery_id: int) -> int | None:
        return self.post_ids.get(int(delivery_id))

    def log_video_event(self, **kwargs) -> None:
        self.video_events.append(dict(kwargs))

    def log_delivery_event(self, **kwargs) -> None:
        self.delivery_events.append(dict(kwargs))


class _RepostRateLimitedSender:
    async def execute_repost_single_from_job(self, **payload):
        raise TransportRateLimited(
            retry_after_seconds=6402,
            backend="bot",
            op_name="send_message",
            key="sender_bot.send_message",
        )


class _VideoRateLimitedSender:
    async def execute_video_send_from_job(self, **payload):
        raise TransportRateLimited(
            retry_after_seconds=6402,
            backend="bot",
            op_name="send_message",
            key="sender_bot.send_message",
        )


def _policy() -> WorkerResourcePolicy:
    return WorkerResourcePolicy(
        light_max_concurrency=1,
        heavy_max_concurrency=1,
        heavy_download_max_concurrency=1,
        heavy_process_max_concurrency=1,
        heavy_send_max_concurrency=1,
    )


def _job(*, job_type: str = "repost_single", attempts: int = 0, max_attempts: int = 3, payload: dict | None = None) -> dict:
    return {
        "id": 777,
        "job_type": job_type,
        "payload_json": payload or {"delivery_id": 123, "rule_id": 45},
        "attempts": attempts,
        "max_attempts": max_attempts,
    }


def test_transport_rate_limited_defers_job_without_attempt_increment() -> None:
    repo = _RateLimitRepo()

    result = asyncio.run(_process_job(
        repo,
        _RepostRateLimitedSender(),
        "worker-1",
        "light",
        _job(),
        policy=_policy(),
        state=_InFlightState(),
    ))

    assert result is True
    assert len(repo.defer_calls) == 1
    job_id, error_text, delay = repo.defer_calls[0]
    assert job_id == 777
    assert delay >= 6412
    assert "retry_after=6402" in error_text
    assert repo.retry_calls == []
    assert repo.fail_calls == []
    assert repo.complete_calls == []


def test_transport_rate_limited_ignores_max_attempts() -> None:
    repo = _RateLimitRepo()

    result = asyncio.run(_process_job(
        repo,
        _RepostRateLimitedSender(),
        "worker-1",
        "light",
        _job(attempts=3, max_attempts=3),
        policy=_policy(),
        state=_InFlightState(),
    ))

    assert result is True
    assert len(repo.defer_calls) == 1
    assert repo.fail_calls == []


def test_transport_rate_limited_logs_video_event() -> None:
    repo = _RateLimitRepo()

    result = asyncio.run(_process_job(
        repo,
        _VideoRateLimitedSender(),
        "worker-1",
        "heavy",
        _job(job_type="video_send", payload={"delivery_id": 123, "rule_id": 45}),
        policy=_policy(),
        state=_InFlightState(),
    ))

    assert result is True
    assert len(repo.defer_calls) == 1
    assert repo.fail_calls == []
    assert repo.faulty_calls == []
    assert len(repo.video_events) == 1
    event = repo.video_events[0]
    assert event["event_type"] == "transport_rate_limited"
    assert event["status"] == "retry"
    assert event["delivery_id"] == 123
    assert event["rule_id"] == 45
    assert event["post_id"] == 987
    assert event["extra"]["retry_after_seconds"] == 6402
    assert event["extra"]["delay_seconds"] >= 6412
