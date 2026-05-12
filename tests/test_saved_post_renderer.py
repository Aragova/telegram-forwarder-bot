import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.saved_post_renderer import (
    SavedPostSentUnverifiedError,
    SavedPostRenderer,
    get_album_source_message_ids,
    normalize_telethon_target,
    send_saved_post_album_via_telethon_source,
    send_saved_post_content,
    send_saved_post_content_via_telethon,
)
import app.saved_post_renderer as spr


class _FakeSentMessage:
    def __init__(self, message_id: int):
        self.message_id = message_id


class _FakeBot:
    async def send_message(self, **kwargs):
        self.last_call = ("text", kwargs)
        return _FakeSentMessage(101)

    async def send_photo(self, **kwargs):
        self.last_call = ("photo", kwargs)
        return _FakeSentMessage(102)


def test_send_saved_post_content_text():
    bot = _FakeBot()
    result = asyncio.run(send_saved_post_content(bot=bot, chat_id=-100123, content={"kind": "text", "text": "Привет"}))
    assert result["ok"] is True
    assert result["kind"] == "text"
    assert result["message_id"] == 101
    assert bot.last_call[0] == "text"


def test_send_saved_post_content_photo():
    bot = _FakeBot()
    result = asyncio.run(send_saved_post_content(
        bot=bot,
        chat_id=-100123,
        content={"kind": "photo", "media": {"file_id": "abc"}, "caption": "cap"},
    ))
    assert result["ok"] is True
    assert result["kind"] == "photo"
    assert result["message_id"] == 102
    assert bot.last_call[0] == "photo"


def test_send_saved_post_content_unsupported_kind_raises():
    bot = _FakeBot()
    with pytest.raises(ValueError, match="Unsupported saved post kind"):
        asyncio.run(send_saved_post_content(bot=bot, chat_id=-100123, content={"kind": "sticker"}))


def test_saved_post_renderer_detect_render_method_plain():
    renderer = SavedPostRenderer(bot=_FakeBot(), telethon_client=None)
    assert renderer.detect_render_method({"kind": "text", "text": "hello", "entities": []}) == "bot_api"


def test_saved_post_renderer_detect_render_method_premium():
    renderer = SavedPostRenderer(bot=_FakeBot(), telethon_client=None)
    content = {
        "kind": "text",
        "text": "x",
        "entities": [
            {"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "123"}
        ],
    }
    assert renderer.detect_render_method(content) == "telethon_builder"


def test_renderer_send_returns_failed_on_unverified_error(monkeypatch):
    async def _send_via_telethon(**kwargs):
        raise SavedPostSentUnverifiedError(
            target_id=123,
            message_ids=[10, 11],
            verified_ids=[10],
            method="telethon_source_unverified",
            reason="Не удалось подтвердить ID отправленного альбома в целевом канале.",
        )

    monkeypatch.setattr(spr, "send_saved_post_content_via_telethon", _send_via_telethon)
    renderer = SavedPostRenderer(bot=_FakeBot(), telethon_client=object())
    result = asyncio.run(renderer.send(
        chat_id=123,
        content={"kind": "album", "media_items": [{}, {}], "caption_entities": [{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "1"}], "forward_origin": {"chat_id": 1, "message_ids": [1, 2]}},
    ))
    assert result.ok is False
    assert result.method == "telethon_source_unverified"
    assert result.message_ids is None


def test_saved_post_renderer_send_plain_returns_render_result():
    bot = _FakeBot()
    renderer = SavedPostRenderer(bot=bot, telethon_client=None)
    result = asyncio.run(renderer.send(chat_id=-100123, content={"kind": "text", "text": "Привет"}))
    assert result.ok is True
    assert result.method == "bot_api"
    assert result.message_id == 101


def test_saved_post_renderer_send_premium_without_telethon_returns_structured_error():
    renderer = SavedPostRenderer(bot=_FakeBot(), telethon_client=None)
    premium_content = {
        "kind": "text",
        "text": "x",
        "entities": [
            {"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "123"}
        ],
    }
    result = asyncio.run(renderer.send(chat_id=1, content=premium_content))
    assert result.ok is False
    assert result.method == "telethon_builder"
    assert result.premium_required is True
    assert "Telethon" in (result.error_text or "")


def test_normalize_telethon_target_numeric_string():
    assert normalize_telethon_target("-1002451047809") == -1002451047809


def test_normalize_telethon_target_int_passthrough():
    assert normalize_telethon_target(-1002451047809) == -1002451047809


def test_normalize_telethon_target_username_passthrough():
    assert normalize_telethon_target("@channel") == "@channel"

class _FakeMediaMsg:
    def __init__(self, message_id):
        self.message_id = message_id


def test_send_saved_post_content_album_bot_api():
    class Bot(_FakeBot):
        async def send_media_group(self, **kwargs):
            self.last_call = ("album", kwargs)
            return [_FakeMediaMsg(101), _FakeMediaMsg(102)]
        async def send_message(self, **kwargs):
            self.service = kwargs
            return _FakeSentMessage(999)
    bot = Bot()
    result = asyncio.run(send_saved_post_content(bot=bot, chat_id=1, content={"kind":"album","caption":"c","caption_entities":[],"media_items":[{"kind":"photo","file_id":"a"},{"kind":"video","file_id":"b"}]}))
    assert result["message_ids"] == [101, 102]
    assert result["message_id"] == 101


def test_send_saved_post_content_album_without_items_fails():
    with pytest.raises(ValueError):
        asyncio.run(send_saved_post_content(bot=_FakeBot(), chat_id=1, content={"kind":"album","media_items":[]}))


class _FailingBot:
    async def get_file(self, file_id):
        raise RuntimeError("file is too big")


class _FakeTelethon:
    pass


def test_renderer_returns_failed_result_when_telethon_album_download_fails():
    renderer = SavedPostRenderer(
        bot=_FailingBot(),
        telethon_client=_FakeTelethon(),
        temp_dir="media/temp",
    )
    content = {
        "kind": "album",
        "caption": "test",
        "caption_entities": [
            {
                "type": "custom_emoji",
                "offset": 0,
                "length": 2,
                "custom_emoji_id": "5470177992950946662",
            }
        ],
        "media_items": [
            {
                "kind": "photo",
                "file_id": "big-file-id",
                "file_unique_id": "u1",
            }
        ],
    }

    result = asyncio.run(renderer.send(chat_id=123, content=content))

    assert result.ok is False
    assert result.method == "telethon_builder"
    assert result.kind == "album"
    assert "слишком большой" in (result.error_text or "")
    assert result.message_ids is None


def test_get_album_source_message_ids_from_root():
    assert get_album_source_message_ids({"source_message_ids": ["101", 102]}) == [101, 102]


def test_get_album_source_message_ids_from_forward_origin():
    assert get_album_source_message_ids({"forward_origin": {"message_ids": [101, 102]}}) == [101, 102]


def test_send_saved_post_album_via_telethon_source_legacy_album_insufficient_ids():
    with pytest.raises(ValueError, match="Замените рекламный пост альбомом заново"):
        asyncio.run(send_saved_post_album_via_telethon_source(
            telethon_client=_FakeTelethon(),
            chat_id=1,
            content={"kind": "album", "media_items": [{}, {}], "forward_origin": {"chat_id": 1, "message_id": 101}},
        ))


class FakeSourceMessage:
    def __init__(self, id):
        self.id = id
        self.media = f"media-{id}"


class FakeSentMessage:
    def __init__(self, id):
        self.id = id


class FakeTelethonSource:
    def __init__(self):
        self.get_messages_calls = []
        self.send_file_calls = []

    async def get_messages(self, entity, ids):
        self.get_messages_calls.append((entity, ids))
        return [FakeSourceMessage(i) for i in ids]

    async def send_file(self, entity, file, caption="", formatting_entities=None):
        self.send_file_calls.append((entity, file, caption, formatting_entities))
        return [FakeSentMessage(900 + i) for i, _ in enumerate(file)]

    async def forward_messages(self, entity, messages, from_peer, drop_author=True):
        return [FakeSentMessage(900 + i) for i, _ in enumerate(messages)]


def test_send_saved_post_album_via_telethon_source_success():
    telethon = FakeTelethonSource()
    raw = asyncio.run(send_saved_post_album_via_telethon_source(
        telethon_client=telethon,
        chat_id=123,
        content={"kind": "album", "caption": "cap", "caption_entities": [], "media_items": [{}, {}], "forward_origin": {"chat_id": 1, "message_ids": [11, 12]}},
    ))
    assert raw["method"] == "telethon_source"
    assert raw["message_ids"] == [900, 901]


def test_renderer_premium_album_uses_source_and_skips_bot_download():
    class Bot:
        async def get_file(self, file_id):
            raise AssertionError("bot.get_file should not be called")

    renderer = SavedPostRenderer(bot=Bot(), telethon_client=FakeTelethonSource(), temp_dir="media/temp")
    result = asyncio.run(renderer.send(
        chat_id=123,
        content={"kind": "album", "caption_entities": [{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "1"}], "media_items": [{}, {}], "forward_origin": {"chat_id": 1, "message_ids": [11, 12]}},
    ))
    assert result.ok is True
    assert result.method == "telethon_source"


class _FakePeer:
    def __init__(self, channel_id):
        self.channel_id = channel_id


class _TelethonVerifyOk(FakeTelethonSource):
    async def get_messages(self, entity, ids):
        if entity == 1:
            return [FakeSourceMessage(i) for i in ids]
        return [SimpleNamespace(id=i, date=datetime.now(timezone.utc), peer_id=_FakePeer(2451047809)) for i in ids]


def test_source_send_ids_verify_failed_raises_unverified_error_without_forward(monkeypatch):
    telethon = FakeTelethonSource()

    async def _verify(**kwargs):
        from app.telethon_delivery_resolver import TelethonResolvedDelivery
        return TelethonResolvedDelivery(ok=False, method="telethon_source_unverified", message_id=None, message_ids=[], grouped_id=None, recovered=False, error_text="verify fail")

    async def _recover(**kwargs):
        from app.telethon_delivery_resolver import TelethonResolvedDelivery
        return TelethonResolvedDelivery(ok=False, method="telethon_source_unverified", message_id=None, message_ids=[], grouped_id=None, recovered=True, error_text="recover fail")

    monkeypatch.setattr(spr, "verify_raw_album_ids", _verify)
    monkeypatch.setattr(spr, "recover_album_ids_by_scan", _recover)
    with pytest.raises(SavedPostSentUnverifiedError) as exc:
        asyncio.run(send_saved_post_album_via_telethon_source(
            telethon_client=telethon,
            chat_id=123,
            content={"kind": "album", "caption": "cap", "media_items": [{}, {}], "forward_origin": {"chat_id": 1, "message_ids": [11, 12]}},
        ))
    assert exc.value.method == "telethon_source_unverified"
    assert exc.value.message_ids == [900, 901]


def test_premium_album_with_full_source_ids_never_downloads(monkeypatch):
    class _TelethonBroken:
        async def get_messages(self, entity, ids):
            return [FakeSourceMessage(i) for i in ids]
        async def send_file(self, **kwargs):
            raise RuntimeError("boom")
        async def forward_messages(self, **kwargs):
            raise RuntimeError("boom2")
    called = {"download": 0}
    async def _download(**kwargs):
        called["download"] += 1
        raise AssertionError("download should not be called")
    monkeypatch.setattr(spr, "download_saved_post_media_item_for_telethon", _download)
    with pytest.raises(RuntimeError):
        asyncio.run(send_saved_post_content_via_telethon(
            bot=_FakeBot(),
            telethon_client=_TelethonBroken(),
            chat_id=123,
            content={"kind": "album", "media_items": [{}, {}], "forward_origin": {"chat_id": 1, "message_ids": [11, 12]}},
            temp_dir="media/temp",
        ))
    assert called["download"] == 0


def test_telethon_album_send_verifies_fresh_target_message_ids():
    telethon = _TelethonVerifyOk()
    raw = asyncio.run(send_saved_post_album_via_telethon_source(
        telethon_client=telethon,
        chat_id="-1002451047809",
        content={"kind": "album", "media_items": [{}, {}, {}], "forward_origin": {"chat_id": 1, "message_ids": [11, 12, 13]}},
    ))
    assert raw["message_ids"] == [900, 901, 902]


class _TelethonOldIds(FakeTelethonSource):
    async def get_messages(self, entity, ids):
        if entity == 1:
            return [FakeSourceMessage(i) for i in ids]
        return [SimpleNamespace(id=i, date=datetime.now(timezone.utc) - timedelta(days=365), peer_id=_FakePeer(2451047809)) for i in ids]


def test_telethon_album_send_rejects_old_message_ids():
    with pytest.raises(SavedPostSentUnverifiedError) as exc:
        asyncio.run(send_saved_post_album_via_telethon_source(
            telethon_client=_TelethonOldIds(),
            chat_id="-1002451047809",
            content={"kind": "album", "media_items": [{}, {}], "forward_origin": {"chat_id": 1, "message_ids": [11, 12]}},
        ))
    assert exc.value.method == "telethon_source_unverified"


class _TelethonWrongPeer(FakeTelethonSource):
    async def get_messages(self, entity, ids):
        if entity == 1:
            return [FakeSourceMessage(i) for i in ids]
        return [SimpleNamespace(id=i, date=datetime.now(timezone.utc), peer_id=_FakePeer(999999)) for i in ids]


def test_telethon_album_send_rejects_wrong_peer():
    with pytest.raises(SavedPostSentUnverifiedError) as exc:
        asyncio.run(send_saved_post_album_via_telethon_source(
            telethon_client=_TelethonWrongPeer(),
            chat_id="-1002451047809",
            content={"kind": "album", "media_items": [{}, {}], "forward_origin": {"chat_id": 1, "message_ids": [11, 12]}},
        ))
    assert exc.value.method == "telethon_source_unverified"


class _FailSourceThenBuilder(_TelethonVerifyOk):
    async def get_messages(self, entity, ids):
        if entity == 1:
            raise RuntimeError("no source")
        return [SimpleNamespace(id=i, date=datetime.now(timezone.utc), peer_id=_FakePeer(2451047809)) for i in ids]

    async def send_file(self, entity, file, caption="", formatting_entities=None):
        return [FakeSentMessage(200 + i) for i, _ in enumerate(file)]


def test_telethon_album_builder_fallback_verifies_ids(tmp_path):
    class _Bot:
        async def get_file(self, file_id):
            return SimpleNamespace(file_path="x")
        async def download_file(self, file_path, destination):
            destination.write_bytes(b"x")
    raw = asyncio.run(send_saved_post_content_via_telethon(
        bot=_Bot(),
        telethon_client=_FailSourceThenBuilder(),
        chat_id="-1002451047809",
        content={"kind": "album", "media_items": [{"kind": "photo", "file_id": "1"}, {"kind": "photo", "file_id": "2"}]},
        temp_dir=tmp_path,
    ))
    assert raw["message_ids"] == [200, 201]
