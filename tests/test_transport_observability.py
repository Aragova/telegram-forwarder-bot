from __future__ import annotations

import asyncio
import logging

import pytest

from app.transport_policy import TransportPolicy, TransportRateLimited, build_sender_bot_policy


async def _no_sleep(_delay):
    return None


def _records_text(caplog) -> str:
    return "\n".join(record.getMessage() for record in caplog.records)


def test_successful_call_logs_call_ok(caplog):
    async def scenario():
        caplog.set_level(logging.DEBUG, logger="forwarder.transport")
        policy = TransportPolicy(max_attempts=1)

        async def operation():
            return {"ok": True}

        result = await policy.execute(backend="bot", key="sender.bot.get_chat", op_name="get_chat", func=operation)

        assert result == {"ok": True}
        text = _records_text(caplog)
        assert "TRANSPORT" in text
        assert "CALL_OK" in text
        assert "backend=bot" in text
        assert "label=sender.bot" in text
        assert "op=get_chat" in text
        assert "operation_kind=safe_read" in text
        assert "elapsed_ms=" in text

    asyncio.run(scenario())


def test_retry_logs_call_retry_and_success(monkeypatch, caplog):
    async def scenario():
        caplog.set_level(logging.DEBUG, logger="forwarder.transport")
        monkeypatch.setattr(asyncio, "sleep", _no_sleep)
        policy = TransportPolicy(max_attempts=2, base_backoff_sec=0.1, jitter_sec=0.0)
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("timed out")
            return "ok"

        result = await policy.execute(backend="telethon", key="sender.telethon.get_messages", op_name="get_messages", func=operation)

        assert result == "ok"
        text = _records_text(caplog)
        assert "CALL_RETRY" in text
        assert "CALL_OK" in text
        assert "attempt=1" in text
        assert "decision=retry" in text

    asyncio.run(scenario())


def test_non_idempotent_write_retry_skipped_log(caplog):
    async def scenario():
        caplog.set_level(logging.DEBUG, logger="forwarder.transport")
        policy = build_sender_bot_policy()
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise TimeoutError("timed out")

        with pytest.raises(TimeoutError):
            await policy.execute(backend="bot", key="sender.bot.copy_message", op_name="copy_message", func=operation)

        assert calls == 1
        text = _records_text(caplog)
        assert "RETRY_SKIPPED" in text
        assert "no_retry_non_idempotent_write" in text
        assert "operation_kind=non_idempotent_write" in text

    asyncio.run(scenario())


def test_unknown_operation_retry_skipped_log(caplog):
    async def scenario():
        caplog.set_level(logging.DEBUG, logger="forwarder.transport")
        policy = build_sender_bot_policy()
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise TimeoutError("connection reset")

        with pytest.raises(TimeoutError):
            await policy.execute(
                backend="bot",
                key="sender.bot.unknown_operation_name",
                op_name="unknown_operation_name",
                func=operation,
            )

        assert calls == 1
        text = _records_text(caplog)
        assert "RETRY_SKIPPED" in text
        assert "no_retry_unknown_operation" in text

    asyncio.run(scenario())


def test_long_retry_after_logs_rate_limited(caplog):
    async def scenario():
        class RetryAfterError(Exception):
            def __init__(self, retry_after):
                self.retry_after = retry_after
                super().__init__(f"retry after {retry_after}")

        caplog.set_level(logging.DEBUG, logger="forwarder.transport")
        policy = build_sender_bot_policy()

        async def operation():
            raise RetryAfterError(policy.long_retry_after_threshold_sec + 1)

        with pytest.raises(TransportRateLimited):
            await policy.execute(backend="bot", key="sender.bot.send_message", op_name="send_message", func=operation)

        text = _records_text(caplog)
        assert "RATE_LIMITED" in text
        assert "retry_after=" in text
        assert "decision=rate_limited" in text

    asyncio.run(scenario())


def test_final_non_retryable_failure_log(caplog):
    async def scenario():
        caplog.set_level(logging.DEBUG, logger="forwarder.transport")
        policy = TransportPolicy(max_attempts=3)
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise RuntimeError("chat not found")

        with pytest.raises(RuntimeError):
            await policy.execute(backend="bot", key="sender.bot.get_chat", op_name="get_chat", func=operation)

        assert calls == 1
        text = _records_text(caplog)
        assert "CALL_FAILED_NON_RETRYABLE" in text or "CALL_FAILED" in text
        assert "decision=non_retryable_error" in text

    asyncio.run(scenario())


def test_transport_logs_do_not_include_args_or_kwargs_payload(caplog):
    async def scenario():
        caplog.set_level(logging.DEBUG, logger="forwarder.transport")
        policy = TransportPolicy(max_attempts=1)
        token = "123:SECRET_TOKEN"
        kwargs = {"caption": "PRIVATE CAPTION"}

        async def operation():
            assert token == "123:SECRET_TOKEN"
            assert kwargs["caption"] == "PRIVATE CAPTION"
            return "ok"

        assert await policy.execute(backend="bot", key="sender.bot.send_message", op_name="send_message", func=operation) == "ok"

        text = _records_text(caplog)
        assert "SECRET_TOKEN" not in text
        assert "PRIVATE CAPTION" not in text

    asyncio.run(scenario())
