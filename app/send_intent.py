"""Per-delivery publication fence and durable MTProto request identity."""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge, TelegramRetryAfter,
)
from telethon import functions
from telethon.errors import BadRequestError, ForbiddenError, FloodWaitError, SlowModeWaitError, RandomIdDuplicateError

from .runtime_utils import run_db


class PublicationWriteUnavailable(RuntimeError):
    """Publication preflight could not establish durable identity."""


class PublicationAlreadyAttempted(RuntimeError):
    """A previous RPC may have published this delivery."""


@dataclass
class PublicationIntent:
    repository: Any
    key: str
    ordinal: int = 0
    possible_write: bool = False
    blocked: bool = False
    uncertain: bool = False


_intent: ContextVar[PublicationIntent | None] = ContextVar("publication_intent", default=None)
_single_rpc: ContextVar[bool] = ContextVar("publication_single_rpc", default=False)


@contextmanager
def publication_scope(intent: PublicationIntent):
    token = _intent.set(intent)
    try:
        yield
    finally:
        _intent.reset(token)


def current_publication_intent() -> PublicationIntent | None:
    return _intent.get()


def authorize_next_album_member() -> None:
    """Only the one-by-one loop may open a new, confirmed distinct member."""
    intent = _intent.get()
    if intent is None:
        return
    if intent.uncertain:
        raise PublicationAlreadyAttempted("Previous album member outcome is unknown")
    intent.blocked = False


def _definite_rejection(exc: Exception) -> bool:
    return not isinstance(exc, RandomIdDuplicateError) and isinstance(
        exc, (TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge,
              TelegramRetryAfter, BadRequestError, ForbiddenError, FloodWaitError, SlowModeWaitError),
    )


def _check_fence(intent: PublicationIntent) -> None:
    if intent.blocked:
        raise PublicationAlreadyAttempted("Previous publication must be reconciled; no alternate send")


async def observe_publication(call, *, bot_result=False, expected_count=None):
    intent = _intent.get()
    if intent is None:
        return await call()
    _check_fence(intent)
    previous = intent.possible_write
    intent.possible_write = intent.blocked = intent.uncertain = True
    try:
        result = await call()
    except Exception as exc:
        if _definite_rejection(exc):
            intent.possible_write = previous
            intent.blocked = intent.uncertain = False
        raise
    # Cancellation is deliberately not a definite rejection.
    if bot_result:
        members = result if isinstance(result, (list, tuple)) else [result]
        intent.uncertain = (
            not members
            or (expected_count is not None and len(members) != expected_count)
            or any(not isinstance(getattr(member, "message_id", None), int)
                   or member.message_id <= 0 for member in members)
        )
    else:
        intent.uncertain = result is None
    return result


def _publication_members(request):
    if isinstance(request, functions.messages.SendMultiMediaRequest):
        return list(request.multi_media)
    if isinstance(request, (functions.messages.SendMessageRequest, functions.messages.SendMediaRequest)):
        return [request]
    if isinstance(request, functions.messages.ForwardMessagesRequest):
        return list(request.id)
    return []


def _stable_descriptor(value):
    """Keep routing/text/shape; upload handles are freshly allocated on retry.

    The attempt row identifies the source delivery. This is not a content hash
    of uploaded bytes and is never used to authorize replay of an unknown send.
    """
    if isinstance(value, list):
        return [_stable_descriptor(item) for item in value]
    if isinstance(value, dict):
        return {
            key: ({"kind": item.get("_")} if key == "media" and isinstance(item, dict)
                  else _stable_descriptor(item))
            for key, item in value.items()
            if key not in {"random_id", "file_reference", "access_hash"}
        }
    return value


async def _bind_identity(client, intent: PublicationIntent, request, members) -> None:
    _check_fence(intent)
    try:
        auth_key_id = getattr(getattr(client.session, "auth_key", None), "key_id", None)
        if not auth_key_id:
            raise ValueError("MTProto session identity is unavailable")
        descriptor = {"account": auth_key_id, "request": _stable_descriptor(request.to_dict())}
        fingerprint = hashlib.sha256(
            json.dumps(descriptor, sort_keys=True, default=str).encode()
        ).hexdigest()
        ids = await run_db(
            intent.repository.get_or_create_mtproto_random_ids,
            intent.key, intent.ordinal, fingerprint, len(members),
        )
        if (len(ids) != len(members) or len(set(ids)) != len(ids)
                or any(type(value) is not int or not 0 < value < 2**63 for value in ids)):
            raise ValueError("Invalid durable random_id vector")
        if isinstance(request, functions.messages.ForwardMessagesRequest):
            request.random_id = ids
        else:
            for member, random_id in zip(members, ids):
                member.random_id = random_id
        intent.ordinal += 1
    except Exception as exc:
        intent.blocked = True
        raise PublicationWriteUnavailable("Could not persist MTProto publication identity") from exc


def publication_client_class(base):
    """Scope Telethon 1.x internal retry limits to publication RPCs only."""
    class PublicationClient(base):
        @property
        def _request_retries(self):
            return 0 if _single_rpc.get() else self._normal_request_retries

        @_request_retries.setter
        def _request_retries(self, value):
            self._normal_request_retries = value

        @property
        def flood_sleep_threshold(self):
            return 0 if _single_rpc.get() else self._flood_sleep_threshold

        @flood_sleep_threshold.setter
        def flood_sleep_threshold(self, value):
            self._flood_sleep_threshold = min(value or 0, 24 * 60 * 60)

        async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
            invoke = super().__call__
            intent = _intent.get()
            calls = request if isinstance(request, (list, tuple)) else [request]
            publications = [(item, _publication_members(item)) for item in calls]
            publications = [(item, members) for item, members in publications if members]
            scoped = intent is not None and bool(publications)
            token = _single_rpc.set(scoped)
            try:
                if not scoped:
                    return await invoke(request, ordered=ordered, flood_sleep_threshold=flood_sleep_threshold)
                if len(calls) != 1:
                    intent.blocked = True
                    raise PublicationWriteUnavailable("Batched publication RPC is not supported")
                item, members = publications[0]
                await _bind_identity(self, intent, item, members)
                return await observe_publication(
                    lambda: invoke(request, ordered=ordered, flood_sleep_threshold=0),
                )
            finally:
                _single_rpc.reset(token)

    return PublicationClient
