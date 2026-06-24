from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from app.telegram_send_gateway import TelegramSendGateway
from app.telegram_send_result import TelegramSendResult


@dataclass(slots=True)
class FakeMessage:
    message_id: int | None = None
    id: int | None = None


class FakeBot:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.results: dict[str, Any] = {}
        self.errors: dict[str, BaseException] = {}

    async def copy_message(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("copy_message", *args, **kwargs)

    async def send_message(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("send_message", *args, **kwargs)

    async def send_video(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("send_video", *args, **kwargs)

    async def send_document(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("send_document", *args, **kwargs)

    async def send_media_group(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("send_media_group", *args, **kwargs)

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((method, args, dict(kwargs)))
        if method in self.errors:
            raise self.errors[method]
        return self.results[method]


class FakeTelethonClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.result: Any = FakeMessage(id=301)

    async def send_file(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("send_file", args, dict(kwargs)))
        return self.result


def test_gateway_stores_injected_clients() -> None:
    fake_bot = FakeBot()
    fake_telethon = FakeTelethonClient()

    gateway = TelegramSendGateway(bot=fake_bot, telethon_client=fake_telethon)

    assert gateway.bot is fake_bot
    assert gateway.telethon_client is fake_telethon


def test_copy_message_calls_bot_and_returns_result() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_bot.results["copy_message"] = {"message_id": 101}
        gateway = TelegramSendGateway(bot=fake_bot)
        kwargs = {"chat_id": -1001, "from_chat_id": -1002, "message_id": 55}

        result = await gateway.copy_message(**kwargs)

        assert fake_bot.calls == [("copy_message", (), kwargs)]
        assert isinstance(result, TelegramSendResult)
        assert result.ok is True
        assert result.method == "copy_message"
        assert result.sent_message_ids == [101]
        assert result.sent_message_id == 101

    asyncio.run(scenario())


def test_send_message_calls_bot() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_bot.results["send_message"] = FakeMessage(message_id=102)
        gateway = TelegramSendGateway(bot=fake_bot)

        result = await gateway.send_message(chat_id=-1001, text="hello")

        assert fake_bot.calls == [("send_message", (), {"chat_id": -1001, "text": "hello"})]
        assert result.method == "send_message"
        assert result.sent_message_ids == [102]

    asyncio.run(scenario())


def test_send_video_calls_bot() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_bot.results["send_video"] = FakeMessage(message_id=103)
        gateway = TelegramSendGateway(bot=fake_bot)

        result = await gateway.send_video(chat_id=-1001, video="file-id")

        assert fake_bot.calls == [("send_video", (), {"chat_id": -1001, "video": "file-id"})]
        assert result.method == "send_video"
        assert result.sent_message_ids == [103]

    asyncio.run(scenario())


def test_send_document_calls_bot() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_bot.results["send_document"] = FakeMessage(message_id=104)
        gateway = TelegramSendGateway(bot=fake_bot)

        result = await gateway.send_document(chat_id=-1001, document="file-id")

        assert fake_bot.calls == [("send_document", (), {"chat_id": -1001, "document": "file-id"})]
        assert result.method == "send_document"
        assert result.sent_message_ids == [104]

    asyncio.run(scenario())


def test_send_media_group_normalizes_multiple_ids() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_bot.results["send_media_group"] = [FakeMessage(201), FakeMessage(202)]
        gateway = TelegramSendGateway(bot=fake_bot)

        result = await gateway.send_media_group(chat_id=-1001, media=["a", "b"])

        assert fake_bot.calls == [("send_media_group", (), {"chat_id": -1001, "media": ["a", "b"]})]
        assert result.method == "send_media_group"
        assert result.sent_message_ids == [201, 202]
        assert result.sent_message_id == 201

    asyncio.run(scenario())


def test_telethon_send_file_calls_client() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_telethon = FakeTelethonClient()
        gateway = TelegramSendGateway(bot=fake_bot, telethon_client=fake_telethon)

        result = await gateway.telethon_send_file(-1001, "video.mp4", caption="caption")

        assert fake_telethon.calls == [("send_file", (-1001, "video.mp4"), {"caption": "caption"})]
        assert result.method == "telethon_send_file"
        assert result.sent_message_ids == [301]

    asyncio.run(scenario())


def test_telethon_send_file_fails_when_client_missing() -> None:
    async def scenario() -> None:
        gateway = TelegramSendGateway(bot=FakeBot(), telethon_client=None)

        with pytest.raises(RuntimeError, match="telethon_client"):
            await gateway.telethon_send_file(-1001, "video.mp4")

    asyncio.run(scenario())


def test_exceptions_propagate() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_bot.errors["send_message"] = RuntimeError("boom")
        gateway = TelegramSendGateway(bot=fake_bot)

        with pytest.raises(RuntimeError, match="boom"):
            await gateway.send_message(chat_id=-1001, text="hello")

    asyncio.run(scenario())


def test_gateway_does_not_access_raw() -> None:
    class FakeProxyBot:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

        @property
        def raw(self) -> object:
            raise AssertionError("raw must not be accessed")

        async def send_message(self, *args: Any, **kwargs: Any) -> Any:
            self.calls.append(("send_message", args, dict(kwargs)))
            return {"message_id": 401}

    async def scenario() -> None:
        fake_proxy_bot = FakeProxyBot()
        gateway = TelegramSendGateway(bot=fake_proxy_bot)

        result = await gateway.send_message(chat_id=-1001, text="hello")

        assert fake_proxy_bot.calls == [("send_message", (), {"chat_id": -1001, "text": "hello"})]
        assert result.sent_message_ids == [401]

    asyncio.run(scenario())


def test_gateway_does_not_leak_args_or_kwargs_into_result_extra() -> None:
    async def scenario() -> None:
        fake_bot = FakeBot()
        fake_bot.results["send_message"] = {"message_id": 501}
        gateway = TelegramSendGateway(bot=fake_bot)

        result = await gateway.send_message(
            chat_id=-1001,
            text="PRIVATE TEXT",
            caption="PRIVATE CAPTION",
            token="123:SECRET_TOKEN",
        )

        extra_text = str(result.to_dict().get("extra"))
        assert "PRIVATE TEXT" not in extra_text
        assert "PRIVATE CAPTION" not in extra_text
        assert "SECRET_TOKEN" not in extra_text

    asyncio.run(scenario())
