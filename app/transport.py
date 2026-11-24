from __future__ import annotations

import asyncio
import logging
import time

from app.transport_policy import (
    TransportPolicy,
    build_sender_telethon_read_policy,
    build_sender_telethon_media_policy,
    build_sender_telethon_upload_policy,
)

from app.send_intent import observe_publication, run_with_cooldown
from app.transport_operation import TransportOperationKind, classify_transport_operation, normalize_transport_operation_name

logger = logging.getLogger("forwarder.transport")


def _validate_publication_content(name, kwargs, method):
    """Reject only provably invalid raw content; Telegram parses markup itself."""
    def value(field):
        return getattr(method, field, None) if method is not None else kwargs.get(field)

    op = normalize_transport_operation_name(name)
    if op == "send_media_group":
        media = value("media")
        if isinstance(media, (list, tuple)) and not 2 <= len(media) <= 10:
            raise ValueError("Telegram media group requires 2 to 10 items")
        for item in media or []:
            if getattr(item, "parse_mode", None) is None:
                caption = getattr(item, "caption", None)
                if isinstance(caption, str) and len(caption) > 1024:
                    raise ValueError("Telegram media caption exceeds 1024 characters")
    elif op == "copy_messages":
        ids = value("message_ids")
        if isinstance(ids, (list, tuple)) and not 1 <= len(ids) <= 100:
            raise ValueError("CopyMessages requires 1 to 100 message IDs")
    elif op in {"send_message", "send_photo", "send_video", "send_document",
                "send_animation", "send_audio", "send_voice"}:
        # Missing parse_mode can inherit the bot's default, so only validate
        # when it was explicitly set to None (or the method object says None).
        explicit_plain = value("parse_mode") is None and (
            method is not None or "parse_mode" in kwargs
        )
        field, limit = ("text", 4096) if op == "send_message" else ("caption", 1024)
        content = value(field)
        if explicit_plain and isinstance(content, str) and len(content) > limit:
            raise ValueError(f"Telegram {field} exceeds {limit} characters")


# Short, read-only RPCs must not occupy worker slots indefinitely.
_TELETHON_READ_TIMEOUT_SEC = 15.0
_TELETHON_CONTROL_OPS = frozenset({"get_messages", "get_entity", "get_input_entity", "get_me"})
_TELETHON_MEDIA_OPS = frozenset({"download_media"})


class BotTransportProxy:
    def __init__(
        self,
        bot,
        *,
        label: str,
        policy: TransportPolicy,
    ) -> None:
        self._bot = bot
        self._label = label
        self._policy = policy
        self._is_transport_proxy = True
        self._transport_backend = "bot"

    @property
    def raw(self):
        return self._bot

    async def _invoke(self, name, call, kwargs, *, method=None):
        if classify_transport_operation("bot", name) != TransportOperationKind.NON_IDEMPOTENT_WRITE:
            return await call()
        _validate_publication_content(name, kwargs, method)
        if method is not None:
            items = getattr(method, "media", None) or getattr(method, "message_ids", None)
        else:
            items = kwargs.get("media") or kwargs.get("message_ids")
        count = len(items) if isinstance(items, (list, tuple)) else None
        async def publish():
            return await observe_publication(call, bot_result=True, expected_count=count)

        bot_id = getattr(self._bot, "id", None)
        return await run_with_cooldown(
            publish, account_scope=f"bot:{bot_id}" if bot_id else None,
            backend="bot", op_name=name,
        )

    async def __call__(self, *args, **kwargs):
        method = args[0] if args else kwargs.get("method")
        op_name = type(method).__name__ if method is not None else "__call__"
        return await self._policy.execute(
            backend="bot",
            key=f"{self._label}.{op_name}",
            op_name=op_name,
            func=lambda: self._invoke(op_name, lambda: self._bot(*args, **kwargs), kwargs, method=method),
        )

    def __getattr__(self, name: str):
        attr = getattr(self._bot, name)

        if not callable(attr):
            return attr

        async def _wrapped(*args, **kwargs):
            return await self._policy.execute(
                backend="bot",
                key=f"{self._label}.{name}",
                op_name=name,
                func=lambda: self._invoke(name, lambda: attr(*args, **kwargs), kwargs),
            )

        return _wrapped


class TelethonTransportProxy:
    def __init__(
        self,
        client,
        *,
        label: str,
        policy: TransportPolicy,
    ) -> None:
        self._client = client
        self._label = label
        self._policy = policy
        self._is_transport_proxy = True
        self._transport_backend = "telethon"
        # Only production TransportPolicy has separate lanes. Custom injected
        # test policies keep their existing delegation contract.
        self._read_policy = build_sender_telethon_read_policy() if isinstance(policy, TransportPolicy) else policy
        self._media_policy = build_sender_telethon_media_policy() if isinstance(policy, TransportPolicy) else policy
        self._upload_policy = build_sender_telethon_upload_policy() if isinstance(policy, TransportPolicy) else policy
        self._control_read_timeout_sec = _TELETHON_READ_TIMEOUT_SEC

    @property
    def raw(self):
        return self._client

    async def __call__(self, *args, **kwargs):
        return await self._policy.execute(
            backend="telethon",
            key=f"{self._label}.__call__",
            op_name="__call__",
            func=lambda: self._client(*args, **kwargs),
        )

    def __getattr__(self, name: str):
        attr = getattr(self._client, name)

        if not callable(attr):
            return attr

        async def _wrapped(*args, **kwargs):
            if name in _TELETHON_CONTROL_OPS:
                selected_policy = self._read_policy
                lane = "control"
            elif name in _TELETHON_MEDIA_OPS:
                selected_policy = self._media_policy
                lane = "bulk"
            elif name == "send_file":
                selected_policy = self._upload_policy
                lane = "upload"
            else:
                selected_policy = self._policy
                lane = "send"

            started = time.monotonic()
            operation = selected_policy.execute(
                backend="telethon",
                key=f"{self._label}.{name}",
                op_name=name,
                func=lambda: attr(*args, **kwargs),
            )
            try:
                if lane == "control":
                    # Safe to cancel READ operations. Never wrap sends or
                    # uploads in this timeout: acceptance may be ambiguous.
                    result = await asyncio.wait_for(operation, timeout=self._control_read_timeout_sec)
                else:
                    result = await operation
            except asyncio.TimeoutError:
                if lane == "control":
                    logger.warning(
                        "TELETHON_CONTROL_READ_TIMEOUT | op=%s | timeout_sec=%s | action=free_control_slot",
                        name, self._control_read_timeout_sec,
                    )
                raise
            finally:
                elapsed = time.monotonic() - started
                if elapsed >= 3.0:
                    logger.warning(
                        "TELETHON_TRANSPORT_SLOW | lane=%s | op=%s | elapsed_sec=%.1f",
                        lane, name, elapsed,
                    )
            return result

        return _wrapped


def wrap_bot(
    bot,
    *,
    label: str,
    policy: TransportPolicy,
):
    if bot is None:
        return None

    if getattr(bot, "_is_transport_proxy", False):
        return bot

    return BotTransportProxy(
        bot,
        label=label,
        policy=policy,
    )


def wrap_telethon_client(
    client,
    *,
    label: str,
    policy: TransportPolicy,
):
    if client is None:
        return None

    if getattr(client, "_is_transport_proxy", False):
        return client

    return TelethonTransportProxy(
        client,
        label=label,
        policy=policy,
    )
