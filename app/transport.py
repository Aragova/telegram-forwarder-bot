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

from app.send_intent import observe_publication
from app.transport_operation import TransportOperationKind, classify_transport_operation

logger = logging.getLogger("forwarder.transport")

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
        if method is not None:
            items = getattr(method, "media", None) or getattr(method, "message_ids", None)
        else:
            items = kwargs.get("media") or kwargs.get("message_ids")
        count = len(items) if isinstance(items, (list, tuple)) else None
        return await observe_publication(call, bot_result=True, expected_count=count)

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
