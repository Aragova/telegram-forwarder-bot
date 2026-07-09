from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import asyncio
import importlib


class FakeCopyMessageBot:
    def __init__(self, *, exc: Exception | None = None):
        self.copy_message_calls = []
        self.exc = exc

    async def copy_message(self, **kwargs):
        self.copy_message_calls.append(kwargs)
        if self.exc:
            raise self.exc
        return SimpleNamespace(message_id=123)


class FakeCopyMessagesBot:
    def __init__(self, result=None, *, exc: Exception | None = None):
        self.calls = []
        self.result = result
        self.exc = exc

    async def __call__(self, method):
        self.calls.append(method)
        if self.exc:
            raise self.exc
        return self.result


def test_sender_botapi_copy_helpers_extracted_from_sender():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_botapi_copy_helpers.py").read_text(encoding="utf-8")

    assert "def copy_single_via_bot" in helper_source
    assert "def copy_album_via_bot" in helper_source

    assert sender_source.count("def _copy_single_via_bot") == 1
    assert sender_source.count("def _copy_album_via_bot") == 1


def test_sender_botapi_copy_helpers_do_not_import_sender():
    source = Path("app/sender_botapi_copy_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


def test_sender_wrapper_delegates_copy_single_via_bot(monkeypatch):
    helper_module = importlib.import_module("app.sender_botapi_copy_helpers")
    SenderService = importlib.import_module("app." + "sender").SenderService

    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))
            self.owner = owner

        async def copy_single_via_bot(self, source_channel, target_id, message_id, target_thread_id):
            calls.append(("copy_single", source_channel, target_id, message_id, target_thread_id))
            return {"ok": "single"}

    monkeypatch.setattr(helper_module, "SenderBotApiCopyHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = asyncio.run(service._copy_single_via_bot("src", "dst", 42, 777))

    assert result == {"ok": "single"}
    assert calls == [("init", service), ("copy_single", "src", "dst", 42, 777)]


def test_sender_wrapper_delegates_copy_album_via_bot(monkeypatch):
    helper_module = importlib.import_module("app.sender_botapi_copy_helpers")
    SenderService = importlib.import_module("app." + "sender").SenderService

    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))
            self.owner = owner

        async def copy_album_via_bot(self, source_channel, target_id, message_ids, target_thread_id):
            calls.append(("copy_album", source_channel, target_id, message_ids, target_thread_id))
            return {"ok": "album"}

    monkeypatch.setattr(helper_module, "SenderBotApiCopyHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)
    message_ids = [1, 2]

    result = asyncio.run(service._copy_album_via_bot("src", "dst", message_ids, 777))

    assert result == {"ok": "album"}
    assert calls == [("init", service), ("copy_album", "src", "dst", message_ids, 777)]


def test_copy_single_via_bot_success_smoke():
    from app.sender_botapi_copy_helpers import SenderBotApiCopyHelpers

    fake_bot = FakeCopyMessageBot()
    owner = SimpleNamespace(
        bot=fake_bot,
        _extract_sent_message_ids=lambda sent: [int(sent.message_id)],
    )

    result = asyncio.run(SenderBotApiCopyHelpers(owner).copy_single_via_bot("src", "dst", 42, 777))

    assert result["attempted"] is True
    assert result["sent_ids"] == [123]
    assert result["fallback_allowed"] is False
    assert result["raw_result_type"] == "SimpleNamespace"
    assert result["raw_result"].message_id == 123
    assert fake_bot.copy_message_calls == [
        {
            "chat_id": "dst",
            "from_chat_id": "src",
            "message_id": 42,
            "message_thread_id": 777,
        }
    ]


def test_copy_single_via_bot_exception_smoke():
    from app.sender_botapi_copy_helpers import SenderBotApiCopyHelpers

    fake_bot = FakeCopyMessageBot(exc=RuntimeError("boom"))
    owner = SimpleNamespace(
        bot=fake_bot,
        _extract_sent_message_ids=lambda sent: [int(sent.message_id)],
    )

    result = asyncio.run(SenderBotApiCopyHelpers(owner).copy_single_via_bot("src", "dst", 42, 777))

    assert result["attempted"] is True
    assert result["sent_ids"] == []
    assert result["fallback_allowed"] is False
    assert result["raw_result_type"] == "exception"
    assert "boom" in result["error_text"]


def test_copy_album_via_bot_success_smoke():
    from app.sender_botapi_copy_helpers import SenderBotApiCopyHelpers

    fake_bot = FakeCopyMessagesBot(
        result=[SimpleNamespace(message_id=10), SimpleNamespace(message_id=11)]
    )
    owner = SimpleNamespace(bot=fake_bot)

    result = asyncio.run(SenderBotApiCopyHelpers(owner).copy_album_via_bot("src", "dst", [1, 2], 777))

    assert result["ok"] is True
    assert result["sent_message_id"] == 10
    assert result["sent_message_ids"] == [10, 11]
    assert result["sent_count"] == 2
    assert result["error_text"] is None
    assert len(fake_bot.calls) == 1
    method = fake_bot.calls[0]
    assert method.chat_id == "dst"
    assert method.from_chat_id == "src"
    assert method.message_ids == [1, 2]
    assert method.message_thread_id == 777


def test_copy_album_via_bot_empty_result_smoke():
    from app.sender_botapi_copy_helpers import SenderBotApiCopyHelpers

    fake_bot = FakeCopyMessagesBot(result=[])
    owner = SimpleNamespace(bot=fake_bot)

    result = asyncio.run(SenderBotApiCopyHelpers(owner).copy_album_via_bot("src", "dst", [1, 2], 777))

    assert result["ok"] is False
    assert result["sent_message_id"] is None
    assert result["sent_count"] == 0
    assert result["error_text"] == "CopyMessages вернул пустой результат"


def test_copy_album_via_bot_exception_smoke():
    from app.sender_botapi_copy_helpers import SenderBotApiCopyHelpers

    fake_bot = FakeCopyMessagesBot(exc=RuntimeError("boom"))
    owner = SimpleNamespace(bot=fake_bot)

    result = asyncio.run(SenderBotApiCopyHelpers(owner).copy_album_via_bot("src", "dst", [1, 2], 777))

    assert result["ok"] is False
    assert result["sent_message_id"] is None
    assert result["sent_count"] == 0
    assert "boom" in result["error_text"]


def test_copy_single_via_bot_debug_skip(monkeypatch):
    helper_module = importlib.import_module("app.sender_botapi_copy_helpers")

    monkeypatch.setattr(helper_module, "DEBUG_FORCE_SKIP_COPY_SINGLE", True)
    fake_bot = FakeCopyMessageBot()
    owner = SimpleNamespace(
        bot=fake_bot,
        _extract_sent_message_ids=lambda sent: [int(sent.message_id)],
    )

    result = asyncio.run(helper_module.SenderBotApiCopyHelpers(owner).copy_single_via_bot("src", "dst", 42, 777))

    assert result == {
        "attempted": False,
        "sent_ids": [],
        "fallback_allowed": True,
        "raw_result_type": "debug_skip",
    }
    assert fake_bot.copy_message_calls == []


def test_copy_album_via_bot_debug_skip(monkeypatch):
    helper_module = importlib.import_module("app.sender_botapi_copy_helpers")

    monkeypatch.setattr(helper_module, "DEBUG_FORCE_SKIP_COPY_ALBUM", True)
    fake_bot = FakeCopyMessagesBot(result=[SimpleNamespace(message_id=10)])
    owner = SimpleNamespace(bot=fake_bot)

    result = asyncio.run(helper_module.SenderBotApiCopyHelpers(owner).copy_album_via_bot("src", "dst", [1], 777))

    assert result == {
        "ok": False,
        "sent_message_id": None,
        "sent_count": 0,
        "error_text": "Bot API copy_album принудительно отключён",
    }
    assert fake_bot.calls == []
