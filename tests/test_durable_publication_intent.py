from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import SendMessage
from telethon import functions, types
from telethon.errors import FloodWaitError, RandomIdDuplicateError

from app.send_intent import (
    PublicationAlreadyAttempted, PublicationIntent, PublicationWriteUnavailable,
    authorize_next_album_member, publication_client_class, publication_scope,
)
from app.transport import wrap_bot
from app.transport_policy import TransportRateLimited, build_sender_bot_policy


class MemoryRepository:
    def __init__(self):
        self.ids = {}
        self.waits = {}
        self.error = None

    def get_or_create_mtproto_random_ids(self, key, ordinal, fingerprint, count):
        if self.error:
            raise self.error
        entry = self.ids.setdefault((key, ordinal), (fingerprint, list(range(100, 100 + count))))
        if entry[0] != fingerprint or len(entry[1]) != count:
            raise ValueError("fingerprint changed")
        return entry[1]

    def get_transport_cooldown(self, scope):
        return self.waits.get(scope, 0)

    def extend_transport_cooldown(self, scope, delay):
        self.waits[scope] = max(self.waits.get(scope, 0), delay)


class RawTelethon:
    def __init__(self):
        self._request_retries = 8
        self.flood_sleep_threshold = 60
        self.calls = []
        self.error = None

    async def get_me(self, *, input_peer):
        assert input_peer
        return types.InputPeerUser(user_id=42, access_hash=123)

    async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
        if isinstance(request, functions.messages.SendMessageRequest):
            self.calls.append((request.random_id, self._request_retries, self.flood_sleep_threshold))
            if self.error:
                raise self.error
        else:
            self.calls.append(("read", self._request_retries, self.flood_sleep_threshold))
        return object()


def _request(text="hello"):
    return functions.messages.SendMessageRequest(peer=types.InputPeerChannel(9, 10), message=text)


def test_mtproto_identity_is_persisted_before_rpc_and_stable_across_safe_retry():
    async def scenario():
        repo = MemoryRepository()
        client = publication_client_class(RawTelethon)()
        client.error = FloodWaitError(request=None, capture=2)
        first = PublicationIntent(repo, "delivery1")
        with publication_scope(first), pytest.raises(FloodWaitError):
            await client(_request())
        assert not first.possible_write
        assert repo.ids
        assert client.calls == [(100, 0, 0)]
        assert repo.waits == {"mtproto:42": 3}
        repo.waits.clear()
        client.error = None
        with publication_scope(PublicationIntent(repo, "delivery1")):
            await client(_request())
        await client(functions.updates.GetStateRequest())
        assert client.calls == [(100, 0, 0), (100, 0, 0), ("read", 8, 60)]
    asyncio.run(scenario())


def test_mtproto_duplicate_id_and_identity_conflict_do_not_publish_again():
    async def scenario():
        repo = MemoryRepository()
        client = publication_client_class(RawTelethon)()
        client.error = RandomIdDuplicateError(request=None)
        intent = PublicationIntent(repo, "delivery2")
        with publication_scope(intent):
            with pytest.raises(RandomIdDuplicateError):
                await client(_request())
            assert intent.possible_write
            with pytest.raises(PublicationAlreadyAttempted):
                await client(_request())
        client.error = None
        with publication_scope(PublicationIntent(repo, "delivery2")), pytest.raises(PublicationWriteUnavailable):
            await client(_request("different"))
        assert len(client.calls) == 1
    asyncio.run(scenario())


def test_mtproto_database_failure_is_preflight_without_rpc():
    async def scenario():
        repo = MemoryRepository()
        repo.error = RuntimeError("database down")
        client = publication_client_class(RawTelethon)()
        intent = PublicationIntent(repo, "delivery3")
        with publication_scope(intent), pytest.raises(PublicationWriteUnavailable):
            await client(_request())
        assert not intent.possible_write
        assert intent.blocked
        assert client.calls == []
    asyncio.run(scenario())


class RawBot:
    id = 7

    def __init__(self):
        self.calls = 0
        self.error = None

    async def send_message(self, **_kwargs):
        self.calls += 1
        if self.error:
            raise self.error
        return SimpleNamespace(message_id=self.calls)

    async def send_media_group(self, **_kwargs):
        self.calls += 1
        return [SimpleNamespace(message_id=10)]


def _bot(raw):
    return wrap_bot(raw, label="sender.bot", policy=build_sender_bot_policy())


def test_partial_album_blocks_fallback_and_next_member():
    async def scenario():
        raw = RawBot()
        bot = _bot(raw)
        intent = PublicationIntent(MemoryRepository(), "album")
        with publication_scope(intent):
            await bot.send_media_group(chat_id=1, media=[object(), object()])
            assert intent.possible_write and intent.uncertain
            with pytest.raises(PublicationAlreadyAttempted):
                await bot.send_message(chat_id=1, text="fallback")
            with pytest.raises(PublicationAlreadyAttempted):
                authorize_next_album_member()
        assert raw.calls == 1
    asyncio.run(scenario())


def test_verified_distinct_album_members_can_continue_but_timeout_blocks_fallback():
    async def scenario():
        raw = RawBot()
        bot = _bot(raw)
        intent = PublicationIntent(MemoryRepository(), "album")
        with publication_scope(intent):
            await bot.send_message(chat_id=1, text="one")
            authorize_next_album_member()
            raw.error = TimeoutError("lost response")
            with pytest.raises(TimeoutError):
                await bot.send_message(chat_id=1, text="two")
            with pytest.raises(PublicationAlreadyAttempted):
                await bot.send_message(chat_id=1, text="two again")
        assert raw.calls == 2
    asyncio.run(scenario())


def test_bot_cooldown_shared_between_attempts_and_invalid_content_does_not_enter_rpc():
    async def scenario():
        raw = RawBot()
        bot = _bot(raw)
        repo = MemoryRepository()
        raw.error = TelegramRetryAfter(method=SendMessage(chat_id=1, text="x"),
                                       message="rate limited", retry_after=2)
        with publication_scope(PublicationIntent(repo, "first")), pytest.raises(TransportRateLimited):
            await bot.send_message(chat_id=1, text="first")
        raw.error = None
        with publication_scope(PublicationIntent(repo, "second")), pytest.raises(TransportRateLimited):
            await bot.send_message(chat_id=1, text="second")
        assert raw.calls == 1
        repo.waits.clear()
        intent = PublicationIntent(repo, "third")
        with publication_scope(intent), pytest.raises(ValueError, match="4096"):
            await bot.send_message(chat_id=1, text="x" * 4097, parse_mode=None)
        assert raw.calls == 1 and not intent.possible_write
    asyncio.run(scenario())
