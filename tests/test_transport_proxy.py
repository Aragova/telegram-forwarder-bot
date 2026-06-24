from __future__ import annotations

import asyncio

import pytest

from app.transport import BotTransportProxy, TelethonTransportProxy, wrap_bot, wrap_telethon_client


class FakePolicy:
    def __init__(self, *, result_override=None, exception=None):
        self.calls = []
        self.result_override = result_override
        self.exception = exception

    async def execute(self, *, backend, key, op_name, func):
        self.calls.append({"backend": backend, "key": key, "op_name": op_name})
        if self.exception is not None:
            raise self.exception
        result = await func()
        if self.result_override is not None:
            return self.result_override
        return result


class FakeRawClient:
    plain_attribute = "raw-value"

    def __init__(self):
        self.direct_calls = []

    async def send_message(self, *args, **kwargs):
        self.direct_calls.append({"args": args, "kwargs": kwargs})
        return {"ok": True, "message_id": 123}

    async def get_messages(self, *args, **kwargs):
        self.direct_calls.append({"args": args, "kwargs": kwargs})
        return ["message"]


class PolicyBoom(Exception):
    pass


def test_wrap_bot_returns_proxy_and_delegates_plain_attributes():
    raw_bot = FakeRawClient()
    policy = FakePolicy()

    proxy = wrap_bot(raw_bot, policy=policy, label="test.bot")

    assert isinstance(proxy, BotTransportProxy)
    assert proxy._is_transport_proxy is True
    assert proxy.raw is raw_bot
    assert proxy.plain_attribute == "raw-value"


def test_wrap_telethon_client_returns_proxy_and_delegates_plain_attributes():
    raw_client = FakeRawClient()
    policy = FakePolicy()

    proxy = wrap_telethon_client(raw_client, policy=policy, label="test.telethon")

    assert isinstance(proxy, TelethonTransportProxy)
    assert proxy._is_transport_proxy is True
    assert proxy.raw is raw_client
    assert proxy.plain_attribute == "raw-value"


def test_wrap_helpers_do_not_double_wrap_existing_proxy():
    raw_bot = FakeRawClient()
    raw_client = FakeRawClient()

    bot_proxy = wrap_bot(raw_bot, policy=FakePolicy(), label="test.bot")
    telethon_proxy = wrap_telethon_client(raw_client, policy=FakePolicy(), label="test.telethon")

    assert wrap_bot(bot_proxy, policy=FakePolicy(), label="test.bot.second") is bot_proxy
    assert wrap_telethon_client(telethon_proxy, policy=FakePolicy(), label="test.telethon.second") is telethon_proxy


def test_bot_proxy_routes_callable_methods_through_policy_and_returns_raw_result():
    async def scenario():
        raw_bot = FakeRawClient()
        policy = FakePolicy()
        proxy = wrap_bot(raw_bot, policy=policy, label="test.bot")

        result = await proxy.send_message(123, text="hello")

        assert result == {"ok": True, "message_id": 123}
        assert policy.calls == [
            {"backend": "bot", "key": "test.bot.send_message", "op_name": "send_message"}
        ]
        assert raw_bot.direct_calls == [{"args": (123,), "kwargs": {"text": "hello"}}]

    asyncio.run(scenario())


def test_telethon_proxy_routes_callable_methods_through_policy_and_returns_raw_result():
    async def scenario():
        raw_client = FakeRawClient()
        policy = FakePolicy()
        proxy = wrap_telethon_client(raw_client, policy=policy, label="test.telethon")

        result = await proxy.get_messages("source", ids=[1, 2])

        assert result == ["message"]
        assert policy.calls == [
            {"backend": "telethon", "key": "test.telethon.get_messages", "op_name": "get_messages"}
        ]
        assert raw_client.direct_calls == [{"args": ("source",), "kwargs": {"ids": [1, 2]}}]

    asyncio.run(scenario())


def test_proxy_propagates_policy_exception_without_wrapping():
    async def scenario():
        raw_bot = FakeRawClient()
        exc = PolicyBoom("policy failed")
        proxy = wrap_bot(raw_bot, policy=FakePolicy(exception=exc), label="test.bot")

        with pytest.raises(PolicyBoom) as raised:
            await proxy.send_message(123, text="hello")

        assert raised.value is exc
        assert raw_bot.direct_calls == []

    asyncio.run(scenario())
