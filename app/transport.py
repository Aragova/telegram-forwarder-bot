from __future__ import annotations

from app.transport_policy import TransportPolicy


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

    async def __call__(self, *args, **kwargs):
        return await self._policy.execute(
            backend="bot",
            key=f"{self._label}.__call__",
            op_name="__call__",
            func=lambda: self._bot(*args, **kwargs),
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
                func=lambda: attr(*args, **kwargs),
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
            return await self._policy.execute(
                backend="telethon",
                key=f"{self._label}.{name}",
                op_name=name,
                func=lambda: attr(*args, **kwargs),
            )

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
