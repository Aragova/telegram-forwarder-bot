"""Shared server-imposed cooldown and conservative Bot API preflight."""
from __future__ import annotations

import logging

from aiogram.exceptions import TelegramRetryAfter
from telethon.errors import FloodWaitError

from .runtime_utils import run_db
from .transport_policy import TransportRateLimited

logger = logging.getLogger("forwarder.transport")


class PublicationContentError(ValueError):
    """Rejected locally before any publication RPC."""


def validate_bot_publication(method, kwargs, request=None):
    def value(name):
        return getattr(request, name, None) if request is not None else kwargs.get(name)

    name = method.lower().replace("_", "")
    if name == "sendmediagroup":
        items = value("media")
        if items is not None and not 2 <= len(items) <= 10:
            raise PublicationContentError("Медиагруппа должна содержать от 2 до 10 элементов")
        for item in items or []:
            _validate_length(getattr(item, "caption", None), getattr(item, "parse_mode", None), 1024)
    elif name == "sendmessage":
        _validate_length(value("text"), value("parse_mode"), 4096)
    elif name in {"sendphoto", "sendvideo", "senddocument", "sendanimation", "sendaudio", "sendvoice"}:
        _validate_length(value("caption"), value("parse_mode"), 1024)
    elif name == "copymessages":
        ids = value("message_ids")
        if ids is not None and not 1 <= len(ids) <= 100:
            raise PublicationContentError("Недопустимое количество сообщений CopyMessages")


def _validate_length(text, parse_mode, limit):
    # Markup/default parse mode must be evaluated by Telegram, not raw len().
    if isinstance(text, str) and parse_mode is None and len(text) > limit:
        raise PublicationContentError(f"Текст превышает лимит Telegram ({limit})")


async def run_with_shared_cooldown(repository, scope, backend, op, call):
    get_wait = getattr(repository, "get_transport_cooldown", None)
    extend = getattr(repository, "extend_transport_cooldown", None)
    if not callable(get_wait) or not callable(extend) or not scope:
        return await call()
    remaining = await run_db(get_wait, scope)
    if remaining > 0:
        raise TransportRateLimited(retry_after_seconds=remaining, backend=backend, op_name=op, key=scope)
    try:
        return await call()
    except (TelegramRetryAfter, FloodWaitError) as exc:
        delay = exc.retry_after if isinstance(exc, TelegramRetryAfter) else exc.seconds
        try:
            await run_db(extend, scope, int(delay) + 1)
        except Exception:
            logger.exception("TRANSPORT_SHARED_COOLDOWN_PERSIST_FAILED | scope=%s", scope)
        raise
