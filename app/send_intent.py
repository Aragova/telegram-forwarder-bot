"""Persist protocol write identities without replaying an unknown result.

A scope belongs to one existing delivery attempt or campaign placement.
No scope is installed for previews or unrelated Telegram UI operations.
"""
from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from telethon import functions, utils
from telethon.errors import BadRequestError, ForbiddenError, FloodWaitError, RandomIdDuplicateError
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge, TelegramRetryAfter

from .runtime_utils import run_db
from .publication_transport_guard import PublicationContentError, run_with_shared_cooldown


class PublicationWriteUnavailable(RuntimeError):
    """The application has not entered the Telegram publication RPC."""


@dataclass
class SendIntent:
    kind: str
    key: str
    ordinal: int = 0
    possible_write: bool = False


_current_intent: ContextVar[SendIntent | None] = ContextVar("send_intent", default=None)


@contextmanager
def publication_intent(kind: str, key: str):
    intent = SendIntent(kind, key)
    token = _current_intent.set(intent)
    try:
        yield intent
    finally:
        _current_intent.reset(token)


def current_intent() -> SendIntent | None:
    return _current_intent.get()


def definite_rejection(exc: BaseException) -> bool:
    return not isinstance(exc, RandomIdDuplicateError) and isinstance(
        exc, (TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge,
              TelegramRetryAfter, BadRequestError, ForbiddenError, FloodWaitError,
              PublicationWriteUnavailable, PublicationContentError),
    )


def possible_write() -> bool:
    intent = current_intent()
    return intent is not None and intent.possible_write


async def observe_write(call, *, atomic=True):
    intent = current_intent()
    if intent is None:
        return await call()
    previous = intent.possible_write
    intent.possible_write = True
    try:
        return await call()
    except Exception as exc:
        if atomic and definite_rejection(exc):
            intent.possible_write = previous
        raise


def _request_members(request):
    if isinstance(request, functions.messages.SendMultiMediaRequest):
        return list(request.multi_media)
    if isinstance(request, (functions.messages.SendMessageRequest, functions.messages.SendMediaRequest)):
        return [request]
    return []


async def bind_rpc_identity(client, repository, request) -> None:
    """Run at the public Telethon RPC boundary, before serializing the request."""
    requests = request if isinstance(request, (list, tuple)) else [request]
    intent = current_intent()
    for item in requests:
        members = _request_members(item)
        if not members:
            continue
        inbox = getattr(client, "publication_inbox", None)
        if getattr(client, "publication_write_blocked", False) or inbox is None:
            raise PublicationWriteUnavailable("MTProto publication evidence unavailable")
        if intent is None:
            continue
        peer = utils.get_peer_id(item.peer)
        reply = getattr(item, "reply_to", None)
        descriptor = {
            "account": inbox.sender_account_id, "peer": peer,
            "method": type(item).__name__,
            "reply": reply.to_dict() if hasattr(reply, "to_dict") else reply,
            "messages": [getattr(m, "message", "") for m in members],
            "entities": [[e.to_dict() for e in (getattr(m, "entities", None) or [])] for m in members],
        }
        fingerprint = hashlib.sha256(json.dumps(descriptor, sort_keys=True, default=str).encode()).hexdigest()
        ordinal = intent.ordinal
        ids = await run_db(repository.get_or_create_mtproto_random_ids,
                           intent.kind, intent.key, ordinal, fingerprint, len(members))
        if (len(ids) != len(members) or len(set(ids)) != len(ids)
                or any(type(i) is not int or not 0 < i < 2**63 for i in ids)):
            raise PublicationWriteUnavailable("Invalid durable MTProto random_id vector")
        for member, random_id in zip(members, ids):
            member.random_id = random_id
        intent.ordinal += 1


def publication_client_class(base, repository):
    """Use the public Telethon call hook; keep its uploads/entities machinery."""
    class PublicationClient(base):
        async def __call__(self, request, ordered=False, flood_sleep_threshold=None):
            calls = request if isinstance(request, (list, tuple)) else [request]
            invoke = super().__call__
            if any(_request_members(item) for item in calls):
                await bind_rpc_identity(self, repository, request)
                async def publish():
                    return await observe_write(
                        lambda: invoke(request, ordered=ordered, flood_sleep_threshold=flood_sleep_threshold),
                        atomic=len(calls) == 1,
                    )
                return await run_with_shared_cooldown(
                    repository, f"mtproto:{self.publication_inbox.sender_account_id}",
                    "telethon", "publish", publish,
                )
            return await invoke(request, ordered=ordered, flood_sleep_threshold=flood_sleep_threshold)

    return PublicationClient
