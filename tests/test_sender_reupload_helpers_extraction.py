from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
from types import SimpleNamespace


class FakeBot:
    def __init__(self):
        self.send_message_calls = []
        self.send_photo_calls = []
        self.send_video_calls = []
        self.send_document_calls = []
        self.send_media_group_calls = []

    async def send_message(self, **kwargs):
        self.send_message_calls.append(kwargs)
        return SimpleNamespace(message_id=456)

    async def send_photo(self, **kwargs):
        self.send_photo_calls.append(kwargs)
        return SimpleNamespace(message_id=791)

    async def send_video(self, **kwargs):
        self.send_video_calls.append(kwargs)
        return SimpleNamespace(message_id=790)

    async def send_document(self, **kwargs):
        self.send_document_calls.append(kwargs)
        return SimpleNamespace(message_id=792)

    async def send_media_group(self, **kwargs):
        self.send_media_group_calls.append(kwargs)
        return [SimpleNamespace(message_id=20), SimpleNamespace(message_id=21)]


class FakeTelethon:
    def __init__(self, paths):
        self.paths = list(paths)
        self.download_media_calls = []

    async def download_media(self, message, file):
        self.download_media_calls.append((message, file))
        return str(self.paths.pop(0)) if self.paths else None


def test_sender_reupload_helpers_extracted_from_sender():
    sender_source = Path("app/sender.py").read_text(encoding="utf-8")
    helper_source = Path("app/sender_reupload_helpers.py").read_text(encoding="utf-8")

    assert "def send_album_one_by_one" in helper_source
    assert "def reupload_album" in helper_source
    assert "def reupload_message" in helper_source

    assert sender_source.count("def _send_album_one_by_one") == 1
    assert sender_source.count("def _reupload_album") == 1
    assert sender_source.count("def _reupload_message") == 1


def test_sender_reupload_helpers_do_not_import_sender():
    source = Path("app/sender_reupload_helpers.py").read_text(encoding="utf-8")

    forbidden = [
        "from ." + "sender import",
        "import app." + "sender",
        "import ." + "sender",
    ]

    for item in forbidden:
        assert item not in source


def test_sender_wrapper_delegates_send_album_one_by_one(monkeypatch):
    helper_module = importlib.import_module("app.sender_reupload_helpers")
    SenderService = importlib.import_module("app.sender").SenderService

    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

        async def send_album_one_by_one(self, messages, target_id, target_thread_id, post_rows=None):
            calls.append(("send_album_one_by_one", messages, target_id, target_thread_id, post_rows))
            return {"ok": "one_by_one"}

    monkeypatch.setattr(helper_module, "SenderReuploadHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)
    messages = ["m1", "m2"]
    post_rows = [{"id": 1}, {"id": 2}]

    result = asyncio.run(service._send_album_one_by_one(messages, "dst", 777, post_rows=post_rows))

    assert result == {"ok": "one_by_one"}
    assert calls == [("init", service), ("send_album_one_by_one", messages, "dst", 777, post_rows)]


def test_sender_wrapper_delegates_reupload_album(monkeypatch):
    helper_module = importlib.import_module("app.sender_reupload_helpers")
    SenderService = importlib.import_module("app.sender").SenderService

    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

        async def reupload_album(self, messages, target_id, target_thread_id, post_rows=None):
            calls.append(("reupload_album", messages, target_id, target_thread_id, post_rows))
            return {"ok": "album"}

    monkeypatch.setattr(helper_module, "SenderReuploadHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)
    messages = ["m1", "m2"]
    post_rows = [{"id": 1}, {"id": 2}]

    result = asyncio.run(service._reupload_album(messages, "dst", 777, post_rows=post_rows))

    assert result == {"ok": "album"}
    assert calls == [("init", service), ("reupload_album", messages, "dst", 777, post_rows)]


def test_sender_wrapper_delegates_reupload_message(monkeypatch):
    helper_module = importlib.import_module("app.sender_reupload_helpers")
    SenderService = importlib.import_module("app.sender").SenderService

    calls = []

    class FakeHelpers:
        def __init__(self, owner):
            calls.append(("init", owner))

        async def reupload_message(self, message, target_id, target_thread_id, post_row=None):
            calls.append(("reupload_message", message, target_id, target_thread_id, post_row))
            return 123

    monkeypatch.setattr(helper_module, "SenderReuploadHelpers", FakeHelpers)
    service = SenderService.__new__(SenderService)
    post_row = {"id": 1}

    result = asyncio.run(service._reupload_message("m1", "dst", 777, post_row=post_row))

    assert result == 123
    assert calls == [("init", service), ("reupload_message", "m1", "dst", 777, post_row)]


def test_send_album_one_by_one_success_smoke(monkeypatch):
    from app.sender_reupload_helpers import SenderReuploadHelpers

    calls = []

    async def fake_reupload_message(self, message, target_id, target_thread_id, post_row=None):
        calls.append((message, target_id, target_thread_id, post_row))
        return 101 if message == "m1" else 102

    monkeypatch.setattr(SenderReuploadHelpers, "reupload_message", fake_reupload_message)
    messages = ["m1", "m2"]
    post_rows = [{"id": 1}, {"id": 2}]

    result = asyncio.run(SenderReuploadHelpers(SimpleNamespace()).send_album_one_by_one(messages, "dst", 777, post_rows=post_rows))

    assert result == {
        "ok": True,
        "sent_message_id": 101,
        "sent_message_ids": [101, 102],
        "sent_count": 2,
        "error_text": None,
    }
    assert calls == [("m1", "dst", 777, post_rows[0]), ("m2", "dst", 777, post_rows[1])]


def test_send_album_one_by_one_partial_failure_smoke(monkeypatch):
    from app.sender_reupload_helpers import SenderReuploadHelpers

    async def fake_reupload_message(self, message, target_id, target_thread_id, post_row=None):
        return 101 if message == "m1" else None

    monkeypatch.setattr(SenderReuploadHelpers, "reupload_message", fake_reupload_message)

    result = asyncio.run(SenderReuploadHelpers(SimpleNamespace()).send_album_one_by_one(["m1", "m2"], "dst", 777))

    assert result["ok"] is False
    assert result["sent_message_id"] == 101
    assert result["sent_count"] == 1
    assert result["error_text"] == "Не удалось отправить один из элементов альбома в аварийном fallback"


def _owner_for_message(*, bot=None, telethon=None, text_id=None, file_id=None, album_result=None):
    async def send_text(**kwargs):
        return text_id

    async def send_file(**kwargs):
        owner.send_file_calls.append(kwargs)
        return file_id

    async def send_album(**kwargs):
        owner.send_album_calls.append(kwargs)
        return album_result

    owner = SimpleNamespace(
        bot=bot or FakeBot(),
        telethon=telethon or FakeTelethon([]),
        send_file_calls=[],
        send_album_calls=[],
        _content_from_message_or_post=lambda **kwargs: {"text": "hello", "entities": []},
        _build_text_and_entities_from_content=lambda content: (content["text"], content["entities"]),
        _send_text_via_telethon=send_text,
        _send_file_via_telethon=send_file,
        _send_album_via_telethon=send_album,
    )
    return owner


def test_reupload_message_text_telethon_success_smoke():
    from app.sender_reupload_helpers import SenderReuploadHelpers

    bot = FakeBot()
    owner = _owner_for_message(bot=bot, text_id=123)
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(SimpleNamespace(media=None), "dst", 777))

    assert result == 123
    assert bot.send_message_calls == []


def test_reupload_message_text_botapi_fallback_smoke():
    from app.sender_reupload_helpers import SenderReuploadHelpers

    bot = FakeBot()
    owner = _owner_for_message(bot=bot, text_id=None)
    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(SimpleNamespace(media=None), "dst", 777))

    assert result == 456
    assert bot.send_message_calls[0]["parse_mode"] == "HTML"
    assert bot.send_message_calls[0]["disable_web_page_preview"] is True


def test_reupload_message_media_telethon_success_smoke(tmp_path):
    from app.sender_reupload_helpers import SenderReuploadHelpers

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    bot = FakeBot()
    owner = _owner_for_message(bot=bot, telethon=FakeTelethon([video_path]), file_id=789)
    message = SimpleNamespace(media=object())

    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(message, "dst", 777))

    assert result == 789
    assert owner.send_file_calls[0]["force_document"] is False
    assert not video_path.exists()
    assert bot.send_video_calls == []
    assert bot.send_document_calls == []
    assert bot.send_photo_calls == []


def test_reupload_message_media_botapi_fallback_smoke(tmp_path):
    from app.sender_reupload_helpers import SenderReuploadHelpers

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    bot = FakeBot()
    owner = _owner_for_message(bot=bot, telethon=FakeTelethon([video_path]), file_id=None)
    message = SimpleNamespace(media=object())

    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(message, "dst", 777))

    assert result == 790
    assert bot.send_video_calls[0]["supports_streaming"] is True
    assert not video_path.exists()


def test_reupload_album_telethon_success_smoke():
    from app.sender_reupload_helpers import SenderReuploadHelpers

    bot = FakeBot()
    owner = _owner_for_message(
        bot=bot,
        album_result={
            "ok": True,
            "sent_message_id": 10,
            "sent_message_ids": [10, 11],
            "sent_count": 2,
            "error_text": None,
        },
    )

    result = asyncio.run(SenderReuploadHelpers(owner).reupload_album([SimpleNamespace()], "dst", 777))

    assert result["ok"] is True
    assert result["sent_message_id"] == 10
    assert result["sent_message_ids"] == [10, 11]
    assert bot.send_media_group_calls == []


def test_reupload_album_botapi_fallback_smoke(tmp_path):
    from app.sender_reupload_helpers import SenderReuploadHelpers

    image_path = tmp_path / "one.jpg"
    video_path = tmp_path / "two.mp4"
    image_path.write_bytes(b"image")
    video_path.write_bytes(b"video")
    bot = FakeBot()
    owner = _owner_for_message(
        bot=bot,
        telethon=FakeTelethon([image_path, video_path]),
        album_result={"ok": False, "sent_message_id": None, "sent_message_ids": [], "sent_count": 0, "error_text": "fail"},
    )

    result = asyncio.run(SenderReuploadHelpers(owner).reupload_album([SimpleNamespace(), SimpleNamespace()], "dst", 777))

    assert result["ok"] is True
    assert result["sent_message_id"] == 20
    assert result["sent_message_ids"] == [20, 21]
    assert not image_path.exists()
    assert not video_path.exists()


def test_reupload_message_media_accepted_unresolved_blocks_botapi_fallback(tmp_path):
    from app.sender_reupload_helpers import SenderReuploadHelpers
    from app.telethon_authoritative_resolver import TelethonSendOutcome

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")
    bot = FakeBot()
    unresolved = TelethonSendOutcome(
        True, True, False, None, [], returned_candidate_id=1157,
        returned_candidate_ids=[1157], resolution_method="unresolved", error_text="not_found_in_target",
    )
    owner = _owner_for_message(bot=bot, telethon=FakeTelethon([video_path]), file_id=unresolved)
    message = SimpleNamespace(media=object())

    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(message, "dst", 777))

    assert result.transport_accepted is True
    assert result.authoritative_resolved is False
    assert owner.send_file_calls and len(owner.send_file_calls) == 1
    assert bot.send_video_calls == []
    assert bot.send_document_calls == []
    assert bot.send_photo_calls == []
    assert bot.send_message_calls == []


def test_reupload_message_text_accepted_unresolved_blocks_outer_text_fallback():
    from app.sender_reupload_helpers import SenderReuploadHelpers
    from app.telethon_authoritative_resolver import TelethonSendOutcome

    bot = FakeBot()
    unresolved = TelethonSendOutcome(
        True, True, False, None, [], returned_candidate_id=1157,
        returned_candidate_ids=[1157], resolution_method="resolver_exception", error_text="boom",
    )
    owner = _owner_for_message(bot=bot, text_id=unresolved)

    result = asyncio.run(SenderReuploadHelpers(owner).reupload_message(SimpleNamespace(media=None), "dst", 777))

    assert result.transport_accepted is True
    assert result.authoritative_resolved is False
    assert bot.send_message_calls == []


def test_reupload_album_accepted_unresolved_blocks_botapi_album_fallback():
    from app.sender_reupload_helpers import SenderReuploadHelpers

    bot = FakeBot()
    owner = _owner_for_message(
        bot=bot,
        album_result={
            "ok": False,
            "transport_accepted": True,
            "authoritative_resolved": False,
            "sent_message_id": None,
            "sent_message_ids": [],
            "sent_count": 2,
            "error_text": "telethon_album_target_id_unresolved",
            "returned_candidate_ids": [1157, 1158],
        },
    )

    result = asyncio.run(SenderReuploadHelpers(owner).reupload_album([SimpleNamespace(), SimpleNamespace()], "dst", 777))

    assert result["transport_accepted"] is True
    assert result["authoritative_resolved"] is False
    assert len(owner.send_album_calls) == 1
    assert bot.send_media_group_calls == []
