import asyncio

import pytest

from app.saved_post_renderer import SavedPostRenderer, normalize_telethon_target, send_saved_post_content


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
    assert "file is too big" in (result.error_text or "")
    assert result.message_ids is None
