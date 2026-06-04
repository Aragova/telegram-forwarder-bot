import asyncio

import pytest

from app.transport_policy import TransportPolicy, TransportRateLimited


class RetryAfterError(Exception):
    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after
        super().__init__(f"retry after {retry_after}")


def test_long_retry_after_raises_rate_limited_without_max_backoff_sleep(monkeypatch):
    sleep_calls = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    policy = TransportPolicy(
        max_attempts=2,
        max_backoff_sec=5.0,
        jitter_sec=0.0,
        long_retry_after_threshold_sec=30,
    )

    async def always_rate_limited():
        raise RetryAfterError(6402)

    with pytest.raises(TransportRateLimited) as exc_info:
        asyncio.run(
            policy.execute(
                backend="bot",
                key="tenant:1",
                op_name="send_message",
                func=always_rate_limited,
            )
        )

    exc = exc_info.value
    assert exc.retry_after_seconds == 6402
    assert exc.backend == "bot"
    assert exc.op_name == "send_message"
    assert exc.key == "tenant:1"
    assert sleep_calls == []


def test_short_retry_after_sleeps_retry_after_plus_safety(monkeypatch):
    sleep_calls = []
    calls = 0

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    policy = TransportPolicy(
        max_attempts=2,
        max_backoff_sec=5.0,
        jitter_sec=0.0,
        long_retry_after_threshold_sec=30,
    )

    async def succeeds_after_short_retry():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RetryAfterError(5)
        return "ok"

    result = asyncio.run(
        policy.execute(
            backend="bot",
            key="tenant:1",
            op_name="send_message",
            func=succeeds_after_short_retry,
        )
    )

    assert result == "ok"
    assert calls == 2
    assert sleep_calls == [5.5]


def test_short_retry_after_delay_is_not_clamped_by_max_backoff():
    policy = TransportPolicy(
        max_attempts=2,
        max_backoff_sec=5.0,
        jitter_sec=0.0,
        long_retry_after_threshold_sec=30,
    )

    decision = policy._classify_error(
        backend="bot",
        op_name="send_message",
        key="tenant:1",
        attempt=1,
        exc=RetryAfterError(8),
    )

    assert decision.should_retry is True
    assert decision.delay == 8.5
    assert decision.reason == "flood_wait_or_retry_after"
