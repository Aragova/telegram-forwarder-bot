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


def test_publication_client_can_read_properties_during_base_initialization():
    class InitializingBase:
        def __init__(self):
            assert self._request_retries == 0
            assert self.flood_sleep_threshold == 0
            self._request_retries = 3
            self.flood_sleep_threshold = 60

    client = publication_client_class(InitializingBase)()
    assert client._request_retries == 3
    assert client.flood_sleep_threshold == 60


@pytest.mark.parametrize("account_id", [42, None])
def test_self_peer_resolves_to_concrete_authorized_account(account_id):
    class SelfPeerClient(RawTelethon):
        async def get_me(self, *, input_peer):
            return types.InputPeerSelf() if input_peer else SimpleNamespace(id=account_id)

    async def scenario():
        repo = MemoryRepository()
        client = publication_client_class(SelfPeerClient)()
        with publication_scope(PublicationIntent(repo, "self-peer")):
            if account_id is None:
                with pytest.raises(PublicationWriteUnavailable):
                    await client(_request())
                assert not client.calls and not repo.ids
            else:
                await client(_request())
                assert client.calls == [(100, 0, 0)]
    asyncio.run(scenario())


def test_positional_album_payload_is_checked_for_partial_response():
    class PositionalBot(RawBot):
        async def send_media_group(self, chat_id, media):
            return await super().send_media_group(chat_id=chat_id, media=media)

    async def scenario():
        raw = PositionalBot()
        intent = PublicationIntent(MemoryRepository(), "positional")
        with publication_scope(intent):
            await _bot(raw).send_media_group(1, [object(), object()])
            assert intent.uncertain
            with pytest.raises(PublicationAlreadyAttempted):
                authorize_next_album_member()
        assert raw.calls == 1

    asyncio.run(scenario())



def test_mtproto_last_moment_inbox_failure_fences_alternate_send():
    async def scenario():
        repo = MemoryRepository()
        client = publication_client_class(RawTelethon)()
        client.publication_inbox_required = True
        client.publication_inbox = object()
        client.publication_write_blocked = False

        def block_after_identity(*_args, **_kwargs):
            client.publication_write_blocked = True
            return [100]

        repo.get_or_create_mtproto_random_ids = block_after_identity
        intent = PublicationIntent(repo, "fenced")
        with publication_scope(intent):
            with pytest.raises(PublicationWriteUnavailable):
                await client(_request("original"))
            assert intent.blocked and not intent.possible_write
            with pytest.raises(PublicationAlreadyAttempted):
                await client(_request("fallback"))
        assert client.calls == []

    asyncio.run(scenario())


def test_mtproto_document_invalid_restores_no_write_fence():
    from telethon.errors import DocumentInvalidError

    async def scenario():
        class MediaRejectingClient(RawTelethon):
            async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
                self.calls.append(request.random_id)
                raise DocumentInvalidError(request=request)

        client = publication_client_class(MediaRejectingClient)()
        intent = PublicationIntent(MemoryRepository(), "invalid-document")
        intent.video_diagnostics = {"phase": "telethon_send_file"}
        request = functions.messages.SendMediaRequest(
            peer=types.InputPeerChannel(9, 10),
            media=types.InputMediaUploadedDocument(
                file=types.InputFile(id=1, parts=1, name="video.mp4", md5_checksum=""),
                mime_type="video/mp4", attributes=[],
            ),
            message="",
        )
        with publication_scope(intent), pytest.raises(DocumentInvalidError):
            await client(request)
        assert client.calls == [100]
        assert not intent.possible_write
        assert not intent.uncertain
        assert intent.video_diagnostics["rpc_state"] == "rejected"
        assert intent.video_diagnostics["rpc_request"]["method"] == "SendMediaRequest"
        assert intent.video_diagnostics["rpc_request"]["mime_type"] == "video/mp4"
        assert intent.video_diagnostics["rpc_request"]["file_parts"] == 1
        assert intent.video_diagnostics["error_type"] == "DocumentInvalidError"
        assert "access_hash" not in str(intent.video_diagnostics)

    asyncio.run(scenario())


def test_diagnostic_rpc_scope_disables_retries_without_mutating_other_tasks():
    from app.send_intent import diagnostic_rpc_scope, current_publication_intent

    async def scenario():
        client = publication_client_class(RawTelethon)()
        entered = asyncio.Event()
        finished = asyncio.Event()

        async def diagnostic():
            with diagnostic_rpc_scope():
                assert current_publication_intent() is None
                entered.set()
                await finished.wait()
                await client(_request())
                await client(functions.updates.GetStateRequest())

        task = asyncio.create_task(diagnostic())
        await entered.wait()
        await client(functions.updates.GetStateRequest())
        finished.set()
        await task
        assert client.calls[0] == ("read", 8, 60)
        assert client.calls[1][1:] == (0, 0)
        assert client.calls[2] == ("read", 0, 0)
        assert client._request_retries == 8
        assert client.flood_sleep_threshold == 60

    asyncio.run(scenario())


def test_diagnostic_rpc_fence_tracks_only_publication_boundary():
    from app.send_intent import DiagnosticPublicationState, diagnostic_rpc_scope
    from telethon.errors import DocumentInvalidError

    class DiagnosticClient(RawTelethon):
        async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
            assert self._request_retries == 0
            if isinstance(request, functions.messages.SendMediaRequest):
                if self.error:
                    raise self.error
            elif isinstance(request, functions.upload.SaveFilePartRequest):
                raise TimeoutError("upload timed out before publication")
            return object()

    async def scenario():
        client = publication_client_class(DiagnosticClient)()
        state = DiagnosticPublicationState()
        request = functions.messages.SendMediaRequest(
            peer=types.InputPeerSelf(), media=types.InputMediaEmpty(), message="", random_id=1,
        )
        with diagnostic_rpc_scope(state):
            with pytest.raises(TimeoutError):
                await client(functions.upload.SaveFilePartRequest(file_id=1, file_part=0, bytes=b"x"))
            assert not state.uncertain
            client.error = DocumentInvalidError(request=request)
            with pytest.raises(DocumentInvalidError):
                await client(request)
            assert not state.uncertain
            client.error = TimeoutError("publication result unknown")
            with pytest.raises(TimeoutError):
                await client(request)
            assert state.uncertain
            with pytest.raises(PublicationAlreadyAttempted):
                await client(request)
        assert client._request_retries == 8

    asyncio.run(scenario())


def test_diagnostic_preflight_runs_at_publication_boundary_without_fencing_rejection():
    from app.send_intent import DiagnosticPublicationState, diagnostic_rpc_scope

    async def scenario():
        client = publication_client_class(RawTelethon)()
        checks = []

        async def preflight():
            checks.append(True)
            raise RuntimeError("cooldown active")

        state = DiagnosticPublicationState(before_publish=preflight)
        with diagnostic_rpc_scope(state):
            await client(functions.updates.GetStateRequest())
            assert not checks
            with pytest.raises(RuntimeError, match="cooldown active"):
                await client(_request())
            assert checks == [True]
            assert not state.uncertain
            assert len(client.calls) == 1

    asyncio.run(scenario())
