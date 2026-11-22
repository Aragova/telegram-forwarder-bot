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


def test_telethon_control_reads_are_not_blocked_by_large_media_download():
    """Regression: one 180s download must not saturate control reads."""
    from app.transport_policy import build_sender_telethon_policy

    async def scenario():
        download_started = asyncio.Event()
        release_download = asyncio.Event()

        class Raw:
            async def download_media(self, *_args, **_kwargs):
                download_started.set()
                await release_download.wait()
                return "/tmp/media.mp4"

            async def get_messages(self, *_args, **_kwargs):
                return ["confirmed"]

        proxy = wrap_telethon_client(
            Raw(), label="sender.telethon", policy=build_sender_telethon_policy(),
        )
        bulk = asyncio.create_task(proxy.download_media("large-video"))
        await asyncio.wait_for(download_started.wait(), timeout=1.0)
        try:
            # With the old single semaphore (concurrency=1), this hung until
            # the large download finished. New control lane must be independent.
            assert await asyncio.wait_for(proxy.get_messages("target", ids=12), timeout=0.5) == ["confirmed"]
            assert not bulk.done()
        finally:
            release_download.set()
            assert await asyncio.wait_for(bulk, timeout=1.0) == "/tmp/media.mp4"

    asyncio.run(scenario())


def test_telethon_control_rpc_timeout_releases_its_lane():
    from app.transport_policy import build_sender_telethon_policy

    async def scenario():
        class Raw:
            async def get_messages(self, *_args, **_kwargs):
                await asyncio.Event().wait()

            async def get_entity(self, *_args, **_kwargs):
                return "target"

        proxy = wrap_telethon_client(Raw(), label="sender.telethon", policy=build_sender_telethon_policy())
        proxy._control_read_timeout_sec = 0.03
        with pytest.raises(asyncio.TimeoutError):
            await proxy.get_messages("target", ids=12)
        # A cancelled read cannot permanently hold the semaphore.
        assert await asyncio.wait_for(proxy.get_entity("target"), timeout=0.5) == "target"

    asyncio.run(scenario())


def test_telethon_write_operations_remain_serialized_after_lane_split():
    from app.transport_policy import build_sender_telethon_policy

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()
        active = 0
        maximum_active = 0

        class Raw:
            async def send_message(self, *_args, **_kwargs):
                nonlocal active, maximum_active
                active += 1
                maximum_active = max(maximum_active, active)
                started.set()
                try:
                    await release.wait()
                    return "sent"
                finally:
                    active -= 1

        proxy = wrap_telethon_client(Raw(), label="sender.telethon", policy=build_sender_telethon_policy())
        first = asyncio.create_task(proxy.send_message("target", "one"))
        await asyncio.wait_for(started.wait(), timeout=1.0)
        second = asyncio.create_task(proxy.send_message("target", "two"))
        await asyncio.sleep(0.02)
        try:
            assert active == 1
            assert not second.done()
        finally:
            release.set()
            assert await asyncio.wait_for(first, timeout=1.0) == "sent"
            assert await asyncio.wait_for(second, timeout=1.0) == "sent"
        assert maximum_active == 1

    asyncio.run(scenario())


def test_telethon_bulk_upload_does_not_block_short_message_write():
    from app.transport_policy import build_sender_telethon_policy

    async def scenario():
        upload_started = asyncio.Event()
        release_upload = asyncio.Event()

        class Raw:
            async def send_file(self, *_args, **_kwargs):
                upload_started.set()
                await release_upload.wait()
                return "uploaded"

            async def send_message(self, *_args, **_kwargs):
                return "short-message-sent"

        proxy = wrap_telethon_client(
            Raw(), label="sender.telethon", policy=build_sender_telethon_policy(),
        )
        upload = asyncio.create_task(proxy.send_file("target", file="large-video.mp4"))
        await asyncio.wait_for(upload_started.wait(), timeout=1.0)
        try:
            assert await asyncio.wait_for(proxy.send_message("target", "hello"), timeout=0.5) == "short-message-sent"
            assert not upload.done()
        finally:
            release_upload.set()
            assert await asyncio.wait_for(upload, timeout=1.0) == "uploaded"

    asyncio.run(scenario())
