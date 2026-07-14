from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
from types import SimpleNamespace

from app.sender import SenderService
from app.sender_telethon_helpers import SenderTelethonHelpers


def test_sender_telethon_helpers_extracted_from_sender():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_telethon_helpers.py").read_text(encoding="utf-8")

    moved_methods = [
        "def send_text_via_telethon",
        "def send_file_via_telethon",
        "def send_album_via_telethon",
        "def verify_album_delivery",
    ]

    for method in moved_methods:
        assert method in helper_source

    wrapper_methods = [
        "def _send_text_via_telethon",
        "def _send_file_via_telethon",
        "def _send_album_via_telethon",
        "def _verify_album_delivery",
    ]

    for method in wrapper_methods:
        assert sender_source.count(method) == 1


def test_sender_telethon_helpers_do_not_import_sender():
    source = Path("app/sender_telethon_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." "sender import",
        "import app." "sender",
        "import ." "sender",
    ]

    for item in forbidden:
        assert item not in source


class _FakeHelpers:
    def __init__(self, owner):
        self.owner = owner

    async def send_text_via_telethon(self, **kwargs):
        return {"method": "send_text_via_telethon", "owner": self.owner, "kwargs": kwargs}

    async def send_file_via_telethon(self, **kwargs):
        return {"method": "send_file_via_telethon", "owner": self.owner, "kwargs": kwargs}

    async def send_album_via_telethon(self, **kwargs):
        return {"method": "send_album_via_telethon", "owner": self.owner, "kwargs": kwargs}

    async def verify_album_delivery(self, **kwargs):
        return {"method": "verify_album_delivery", "owner": self.owner, "kwargs": kwargs}


def test_sender_wrapper_delegates_send_text_via_telethon(monkeypatch):
    helper_module = importlib.import_module("app.sender_telethon_helpers")
    monkeypatch.setattr(helper_module, "SenderTelethonHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = asyncio.run(service._send_text_via_telethon(target_id="-100", target_thread_id=7, text="text", entities=["e"]))

    assert result == {
        "method": "send_text_via_telethon",
        "owner": service,
        "kwargs": {"target_id": "-100", "target_thread_id": 7, "text": "text", "entities": ["e"]},
    }


def test_sender_wrapper_delegates_send_file_via_telethon(monkeypatch, tmp_path):
    helper_module = importlib.import_module("app.sender_telethon_helpers")
    monkeypatch.setattr(helper_module, "SenderTelethonHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)
    file_path = tmp_path / "a.mp4"

    result = asyncio.run(service._send_file_via_telethon(
        target_id="-100", target_thread_id=7, message="msg", file_path=file_path, force_document=True, post_row={"id": 1}
    ))

    assert result["method"] == "send_file_via_telethon"
    assert result["owner"] is service
    assert result["kwargs"] == {
        "target_id": "-100",
        "target_thread_id": 7,
        "message": "msg",
        "file_path": file_path,
        "force_document": True,
        "post_row": {"id": 1},
    }


def test_sender_wrapper_delegates_send_album_via_telethon(monkeypatch):
    helper_module = importlib.import_module("app.sender_telethon_helpers")
    monkeypatch.setattr(helper_module, "SenderTelethonHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = asyncio.run(service._send_album_via_telethon(messages=["m1"], target_id="-100", target_thread_id=7, post_rows=[{"id": 1}]))

    assert result == {
        "method": "send_album_via_telethon",
        "owner": service,
        "kwargs": {"messages": ["m1"], "target_id": "-100", "target_thread_id": 7, "post_rows": [{"id": 1}]},
    }


def test_sender_wrapper_delegates_verify_album_delivery(monkeypatch):
    helper_module = importlib.import_module("app.sender_telethon_helpers")
    monkeypatch.setattr(helper_module, "SenderTelethonHelpers", _FakeHelpers)
    service = SenderService.__new__(SenderService)

    result = asyncio.run(service._verify_album_delivery(
        target_id="-100", expected_count=2, sent_message_ids=[10, 11], target_thread_id=7, target_grouped_id=99
    ))

    assert result["method"] == "verify_album_delivery"
    assert result["owner"] is service
    assert result["kwargs"] == {
        "target_id": "-100",
        "expected_count": 2,
        "sent_message_ids": [10, 11],
        "target_thread_id": 7,
        "target_grouped_id": 99,
    }


class FakeTelethon:
    def __init__(self):
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(id=123, message=kwargs.get("message") or "")

    async def send_file(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(id=456, message=kwargs.get("caption") or kwargs.get("message") or "")

    async def get_messages(self, entity, ids=None, limit=None):
        if limit:
            return [SimpleNamespace(id=122)]
        return SimpleNamespace(id=ids, message="hello" if ids == 123 else "caption", reply_to=SimpleNamespace(reply_to_top_id=7))


def _owner(telethon):
    return SimpleNamespace(
        telethon=telethon,
        _clone_telethon_entities=lambda entities, text=None: list(entities or []),
        _content_from_message_or_post=lambda **kwargs: {"text": getattr(kwargs["message"], "raw_text", "")},
        _build_text_and_entities_from_content=lambda content: (content.get("text") or "", content.get("entities") or ["entity"]),
    )


def test_send_text_via_telethon_smoke():
    telethon = FakeTelethon()
    helper = SenderTelethonHelpers(_owner(telethon))

    result = asyncio.run(helper.send_text_via_telethon(target_id="-100", target_thread_id=7, text="hello", entities=["e1"]))

    assert result.authoritative_message_id == 123
    assert result.authoritative_resolved is True
    assert telethon.calls[0]["entity"] == -100
    assert telethon.calls[0]["message"] == "hello"
    assert telethon.calls[0]["formatting_entities"] == ["e1"]
    assert telethon.calls[0]["link_preview"] is False
    assert telethon.calls[0]["comment_to"] == 7


def test_send_file_via_telethon_original_media_smoke():
    telethon = FakeTelethon()
    message = SimpleNamespace(media="original-media", video=True, raw_text="caption")
    helper = SenderTelethonHelpers(_owner(telethon))

    result = asyncio.run(helper.send_file_via_telethon(target_id="-100", target_thread_id=None, message=message))

    assert result.authoritative_message_id == 456
    assert result.authoritative_resolved is True
    assert telethon.calls[0]["file"] == message.media
    assert telethon.calls[0]["caption"] == "caption"
    assert telethon.calls[0]["supports_streaming"] is True
    assert telethon.calls[0]["link_preview"] is False


def test_send_file_via_telethon_fallback_smoke(tmp_path):
    class FallbackTelethon(FakeTelethon):
        async def send_file(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                raise Exception("original failed")
            return SimpleNamespace(id=789, message=kwargs.get("caption") or "")

    telethon = FallbackTelethon()
    file_path = tmp_path / "fallback.mp4"
    file_path.write_text("x", encoding="utf-8")
    message = SimpleNamespace(media="original-media", video=True, raw_text="caption")
    helper = SenderTelethonHelpers(_owner(telethon))

    result = asyncio.run(helper.send_file_via_telethon(target_id="-100", target_thread_id=None, message=message, file_path=file_path))

    assert result.authoritative_message_id == 789
    assert result.authoritative_resolved is True
    assert len(telethon.calls) == 2
    assert telethon.calls[1]["file"] == str(file_path)


def test_send_album_via_telethon_original_media_smoke():
    class AlbumTelethon(FakeTelethon):
        async def send_file(self, **kwargs):
            self.calls.append(kwargs)
            return [SimpleNamespace(id=10, message=kwargs.get("caption") or ""), SimpleNamespace(id=11, message="")]

    telethon = AlbumTelethon()
    messages = [SimpleNamespace(media="m1", raw_text="caption"), SimpleNamespace(media="m2", raw_text="")]
    helper = SenderTelethonHelpers(_owner(telethon))

    result = asyncio.run(helper.send_album_via_telethon(messages=messages, target_id="-100", target_thread_id=None))

    assert result["ok"] is True
    assert result["sent_message_id"] == 10
    assert result["sent_message_ids"] == [10, 11]
    assert result["sent_count"] == 2
    assert result["error_text"] is None


def test_verify_album_delivery_smoke():
    class VerifyTelethon:
        async def get_messages(self, entity, ids):
            assert entity == -100
            assert ids == [10, 11]
            return [SimpleNamespace(id=10, grouped_id=99), SimpleNamespace(id=11, grouped_id=99)]

    helper = SenderTelethonHelpers(SimpleNamespace(telethon=VerifyTelethon()))

    result = asyncio.run(helper.verify_album_delivery(target_id="-100", expected_count=2, sent_message_ids=[10, 11]))

    assert result["ok"] is True
    assert result["count"] == 2
    assert result["first_message_id"] == 10
    assert result["sent_message_ids"] == [10, 11]
