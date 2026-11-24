import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import SendMessage
from telethon import functions, types

from app.send_intent import (PublicationWriteUnavailable, publication_client_class,
                             publication_intent, possible_write, observe_write)
from app.publication_transport_guard import PublicationContentError, validate_bot_publication, run_with_shared_cooldown
from app.transport_policy import TransportRateLimited


def test_mtproto_send_identity_is_persisted_before_wire_call_and_reused():
    rows = {}
    calls = []

    class Repo:
        def get_or_create_mtproto_random_ids(self, kind, key, ordinal, fingerprint, count):
            identity = (kind, key, ordinal)
            if identity not in rows:
                rows[identity] = (fingerprint, list(range(501 + ordinal * 10, 501 + ordinal * 10 + count)))
            saved_fp, ids = rows[identity]
            if fingerprint != saved_fp:
                raise RuntimeError("changed")
            return ids

    class Base:
        async def __call__(self, request, **kwargs):
            assert rows
            calls.append(request.random_id)
            return request.random_id

    async def scenario():
        client = publication_client_class(Base, Repo())()
        client.publication_inbox = SimpleNamespace(sender_account_id=9)
        def request(text="hello"):
            return functions.messages.SendMessageRequest(
                peer=types.InputPeerChannel(99, 1), message=text, random_id=1)
        with publication_intent("delivery", "key"):
            assert await client(request()) == 501
            assert possible_write()
            assert await client(request()) == 511
        with publication_intent("delivery", "key"):
            assert await client(request()) == 501
        with publication_intent("delivery", "key"):
            with pytest.raises(RuntimeError, match="changed"):
                await client(request("changed"))
        client.publication_write_blocked = True
        with publication_intent("delivery", "key"):
            with pytest.raises(PublicationWriteUnavailable):
                await client(request())
        assert calls == [501, 511, 501]
    asyncio.run(scenario())


def test_unknown_write_is_never_downgraded_by_later_rate_limit():
    async def scenario():
        exc = TelegramRetryAfter(method=SendMessage(chat_id=1, text="x"), message="wait", retry_after=3)
        with publication_intent("delivery", "k"):
            with pytest.raises(TelegramRetryAfter):
                await observe_write(AsyncMock(side_effect=exc))
            assert not possible_write()
            with pytest.raises(TimeoutError):
                await observe_write(AsyncMock(side_effect=TimeoutError()))
            assert possible_write()
            with pytest.raises(TelegramRetryAfter):
                await observe_write(AsyncMock(side_effect=exc))
            assert possible_write()
    asyncio.run(scenario())


def test_shared_cooldown_records_explicit_rate_limit_and_refuses_next_send():
    delays = {}
    repo = SimpleNamespace(
        get_transport_cooldown=lambda key: delays.get(key, 0),
        extend_transport_cooldown=lambda key, seconds: delays.update({key: seconds}),
    )
    async def scenario():
        exc = TelegramRetryAfter(method=SendMessage(chat_id=1,text="x"), message="wait", retry_after=3)
        call = AsyncMock(side_effect=exc)
        with pytest.raises(TelegramRetryAfter):
            await run_with_shared_cooldown(repo, "bot:1", "bot", "send_message", call)
        with pytest.raises(TransportRateLimited) as failure:
            await run_with_shared_cooldown(repo, "bot:1", "bot", "send_message", call)
        assert failure.value.retry_after_seconds == 4
        call.assert_awaited_once()
    asyncio.run(scenario())


@pytest.mark.parametrize("method,kwargs", [
    ("send_message", {"text": "x" * 4097, "parse_mode": None}),
    ("send_video", {"caption": "x" * 1025, "parse_mode": None}),
    ("send_media_group", {"media": [object()]}),
])
def test_invalid_bot_publication_rejected_before_rpc(method, kwargs):
    with pytest.raises(PublicationContentError):
        validate_bot_publication(method, kwargs)


def test_html_not_measured_as_plain_text():
    validate_bot_publication("send_message", {"text": "<b>" * 4097, "parse_mode": "HTML"})
