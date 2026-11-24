from __future__ import annotations

import asyncio

import pytest
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import CopyMessages, SendMessage
from telethon.errors import FloodWaitError

from app.transport import wrap_bot
from app.transport_policy import (
    TransportRateLimited,
    build_sender_bot_policy,
    build_sender_telethon_policy,
)


@pytest.mark.parametrize("backend", ["bot", "telethon"])
def test_short_explicit_publication_flood_wait_defers_without_resend(backend):
    async def scenario():
        policy = (
            build_sender_bot_policy()
            if backend == "bot"
            else build_sender_telethon_policy()
        )
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            if backend == "bot":
                raise TelegramRetryAfter(
                    method=SendMessage(chat_id=1, text="test"),
                    message="rate limited", retry_after=2,
                )
            raise FloodWaitError(request=None, capture=2)

        with pytest.raises(TransportRateLimited) as result:
            await policy.execute(
                backend=backend, key="sender.send_message",
                op_name="send_message", func=operation,
            )
        assert result.value.retry_after_seconds == 2
        assert calls == 1

    asyncio.run(scenario())


def test_network_timeout_attribute_is_not_a_flood_wait():
    class NetworkTimeout(TimeoutError):
        timeout = 60

    async def scenario():
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise NetworkTimeout("timed out")

        with pytest.raises(NetworkTimeout):
            await build_sender_bot_policy().execute(
                backend="bot", key="sender.send_message",
                op_name="send_message", func=operation,
            )
        assert calls == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("keyword", [False, True])
def test_direct_bot_api_copy_request_is_classified_as_write(keyword):
    class RawBot:
        async def __call__(self, *args, **kwargs):
            raise TelegramRetryAfter(
                method=CopyMessages(chat_id=1, from_chat_id=2, message_ids=[3]),
                message="rate limited", retry_after=2,
            )

    async def scenario():
        proxy = wrap_bot(
            RawBot(), label="sender.bot", policy=build_sender_bot_policy(),
        )
        method = CopyMessages(chat_id=1, from_chat_id=2, message_ids=[3])
        with pytest.raises(TransportRateLimited) as result:
            if keyword:
                await proxy(method=method)
            else:
                await proxy(method)
        assert result.value.op_name == "CopyMessages"

    asyncio.run(scenario())


def test_failed_telethon_connect_closes_client_and_preserves_error(monkeypatch):
    from app import telegram_client

    events = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def connect(self):
            events.append("connect")
            raise RuntimeError("connect failed")

        async def disconnect(self):
            events.append("disconnect")
            raise RuntimeError("disconnect failed")

    monkeypatch.setattr(telegram_client, "TelegramClient", FakeClient)
    with pytest.raises(RuntimeError, match="connect failed"):
        asyncio.run(telegram_client.create_telethon_client())
    assert events == ["connect", "disconnect"]


@pytest.mark.parametrize("seconds", [2, 60])
@pytest.mark.parametrize("op_name", ["send_message", "__call__"])
def test_unconfirmed_wait_hint_does_not_authorize_publication_retry(seconds, op_name):
    class AmbiguousTimeout(TimeoutError):
        pass

    async def scenario():
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            error = AmbiguousTimeout("network timeout")
            error.seconds = seconds
            raise error

        with pytest.raises(AmbiguousTimeout):
            await build_sender_bot_policy().execute(
                backend="bot", key=f"sender.{op_name}",
                op_name=op_name, func=operation,
            )
        assert calls == 1

    asyncio.run(scenario())


def test_explicit_flood_wait_at_attempt_limit_keeps_retry_after():
    from app.transport_policy import TransportPolicy

    async def scenario():
        async def operation():
            raise TelegramRetryAfter(
                method=SendMessage(chat_id=1, text="test"),
                message="rate limited", retry_after=2,
            )

        with pytest.raises(TransportRateLimited) as result:
            await TransportPolicy(max_attempts=1).execute(
                backend="bot", key="sender.get_chat",
                op_name="get_chat", func=operation,
            )
        assert result.value.retry_after_seconds == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["connect", "get_me"])
def test_reaction_startup_closes_all_opened_clients_on_failure(monkeypatch, failure):
    from types import SimpleNamespace
    from app import telegram_client

    closed = []

    class FakeClient:
        def __init__(self, session, *args, **kwargs):
            self.session = session

        async def connect(self):
            if self.session == "second" and failure == "connect":
                raise RuntimeError("startup failed")

        async def get_me(self):
            if self.session == "second" and failure == "get_me":
                raise RuntimeError("startup failed")
            return SimpleNamespace(premium=False)

        async def disconnect(self):
            closed.append(self.session)
            if self.session == "second":
                raise RuntimeError("cleanup failed")

    monkeypatch.setattr(telegram_client, "TelegramClient", FakeClient)
    monkeypatch.setattr(telegram_client, "settings", SimpleNamespace(
        reaction_sessions=["first", "second"], api_id=1, api_hash="test",
    ))
    with pytest.raises(RuntimeError, match="startup failed"):
        asyncio.run(telegram_client.create_reaction_clients())
    assert closed == ["second", "first"]


@pytest.mark.parametrize("seconds", [21, 120])
def test_telethon_slow_mode_is_explicit_rejection(seconds):
    from telethon.errors import SlowModeWaitError

    async def scenario():
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise SlowModeWaitError(request=None, capture=seconds)

        with pytest.raises(TransportRateLimited) as result:
            await build_sender_telethon_policy().execute(
                backend="telethon", key="sender.send_file",
                op_name="send_file", func=operation,
            )
        assert result.value.retry_after_seconds == seconds
        assert calls == 1

    asyncio.run(scenario())
