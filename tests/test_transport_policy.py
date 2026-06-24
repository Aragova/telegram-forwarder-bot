from __future__ import annotations

import asyncio

import pytest

from app.transport_policy import (
    TransportPolicy,
    TransportRateLimited,
    build_reaction_policy,
    build_sender_bot_policy,
    build_sender_telethon_policy,
    build_video_bot_policy,
    build_video_telethon_policy,
)


async def _no_sleep(_delay):
    return None


def test_execute_successful_operation_returns_result_once():
    async def scenario():
        policy = TransportPolicy(max_attempts=2, base_backoff_sec=0.1, jitter_sec=0.0)
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            return {"ok": True}

        result = await policy.execute(backend="bot", key="test.ok", op_name="send_message", func=operation)

        assert result == {"ok": True}
        assert calls == 1

    asyncio.run(scenario())


def test_execute_retries_transient_error_then_returns_success(monkeypatch):
    async def scenario():
        monkeypatch.setattr(asyncio, "sleep", _no_sleep)
        policy = TransportPolicy(max_attempts=2, base_backoff_sec=0.1, jitter_sec=0.0)
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("timed out")
            return "success"

        result = await policy.execute(backend="bot", key="test.retry", op_name="send_message", func=operation)

        assert result == "success"
        assert calls == 2

    asyncio.run(scenario())


def test_execute_respects_max_attempts_for_retryable_errors(monkeypatch):
    async def scenario():
        monkeypatch.setattr(asyncio, "sleep", _no_sleep)
        policy = TransportPolicy(max_attempts=3, base_backoff_sec=0.1, jitter_sec=0.0)
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise TimeoutError("connection reset")

        with pytest.raises(TimeoutError):
            await policy.execute(backend="bot", key="test.max", op_name="send_message", func=operation)

        assert calls == 3

    asyncio.run(scenario())


def test_execute_does_not_retry_non_retryable_business_errors(monkeypatch):
    async def scenario():
        monkeypatch.setattr(asyncio, "sleep", _no_sleep)
        policy = TransportPolicy(max_attempts=3, base_backoff_sec=0.1, jitter_sec=0.0)
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise RuntimeError("chat not found")

        with pytest.raises(RuntimeError, match="chat not found"):
            await policy.execute(backend="bot", key="test.business", op_name="send_message", func=operation)

        assert calls == 1

    asyncio.run(scenario())


def test_long_retry_after_raises_transport_rate_limited():
    async def scenario():
        class RetryAfterError(Exception):
            def __init__(self, retry_after):
                self.retry_after = retry_after
                super().__init__(f"retry after {retry_after}")

        policy = TransportPolicy(max_attempts=2, long_retry_after_threshold_sec=30)

        async def operation():
            raise RetryAfterError(31)

        with pytest.raises(TransportRateLimited) as raised:
            await policy.execute(backend="telethon", key="test.flood", op_name="send_file", func=operation)

        exc = raised.value
        assert exc.retry_after_seconds == 31
        assert exc.backend == "telethon"
        assert exc.op_name == "send_file"
        assert exc.key == "test.flood"

    asyncio.run(scenario())


def test_short_retry_after_is_retried_without_real_sleep(monkeypatch):
    async def scenario():
        sleep_delays = []

        async def fake_sleep(delay):
            sleep_delays.append(delay)

        class RetryAfterError(Exception):
            def __init__(self, retry_after):
                self.retry_after = retry_after
                super().__init__(f"retry after {retry_after}")

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        policy = TransportPolicy(max_attempts=2, long_retry_after_threshold_sec=30)
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RetryAfterError(2)
            return "ok"

        result = await policy.execute(backend="bot", key="test.short-flood", op_name="send_message", func=operation)

        assert result == "ok"
        assert calls == 2
        assert sleep_delays == [2.5]

    asyncio.run(scenario())


def test_max_concurrency_limits_parallel_execution():
    async def scenario():
        policy = TransportPolicy(max_concurrency=1)
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        entered = []

        async def first_operation():
            entered.append("first")
            first_entered.set()
            await release_first.wait()
            return "first-result"

        async def second_operation():
            entered.append("second")
            return "second-result"

        first_task = asyncio.create_task(
            policy.execute(backend="bot", key="test.concurrent.first", op_name="send_message", func=first_operation)
        )
        await first_entered.wait()

        second_task = asyncio.create_task(
            policy.execute(backend="bot", key="test.concurrent.second", op_name="send_message", func=second_operation)
        )
        await asyncio.sleep(0)

        assert entered == ["first"]

        release_first.set()
        assert await first_task == "first-result"
        assert await second_task == "second-result"
        assert entered == ["first", "second"]

    asyncio.run(scenario())


def test_policy_builders_return_usable_transport_policies():
    builders = [
        build_sender_bot_policy,
        build_sender_telethon_policy,
        build_reaction_policy,
        build_video_bot_policy,
        build_video_telethon_policy,
    ]

    policies = [builder() for builder in builders]

    assert all(isinstance(policy, TransportPolicy) for policy in policies)
    assert build_sender_bot_policy().max_attempts == 2
    assert build_reaction_policy().max_attempts == 1
