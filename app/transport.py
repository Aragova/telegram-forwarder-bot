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

from app.send_intent import observe_write, PublicationWriteUnavailable
from app.publication_transport_guard import run_with_shared_cooldown, validate_bot_publication
from app.transport_operation import classify_transport_operation, TransportOperationKind


async def _invoke_bot_write(op_name, invoke, *, repository, scope, kwargs, request=None):
    if classify_transport_operation("bot", op_name) == TransportOperationKind.NON_IDEMPOTENT_WRITE:
        validate_bot_publication(op_name, kwargs, request)
        return await run_with_shared_cooldown(
            repository, scope, "bot", op_name, lambda: observe_write(invoke),
        )
    return await invoke()


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
        repository=None,
    ) -> None:
        self._bot = bot
        self._repository = repository
        bot_id = getattr(bot, "id", None)
        self._cooldown_scope = f"bot:{bot_id}" if type(bot_id) is int else None
        self._label = label
        self._policy = policy
        self._is_transport_proxy = True
        self._transport_backend = "bot"

    @property
    def raw(self):
        return self._bot

    async def __call__(self, *args, **kwargs):
        method = args[0] if args else kwargs.get("method")
        op_name = type(method).__name__ if method is not None else "__call__"
        return await self._policy.execute(
            backend="bot",
            key=f"{self._label}.{op_name}",
            op_name=op_name,
            func=lambda: _invoke_bot_write(op_name, lambda: self._bot(*args, **kwargs),
                                           repository=self._repository, scope=self._cooldown_scope,
                                           kwargs=kwargs, request=method),
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
                func=lambda: _invoke_bot_write(name, lambda: attr(*args, **kwargs),
                                               repository=self._repository, scope=self._cooldown_scope,
                                               kwargs=kwargs),
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
            if (name in {"send_file", "send_message", "forward_messages"}
                    and getattr(self._client, "publication_write_blocked", False) is True):
                raise PublicationWriteUnavailable("MTProto publication evidence unavailable")
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
    repository=None,
):
    if bot is None:
        return None

    if getattr(bot, "_is_transport_proxy", False):
        return bot

    return BotTransportProxy(
        bot,
        label=label,
        policy=policy,
        repository=repository,
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
