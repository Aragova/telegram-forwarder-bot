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
