from types import SimpleNamespace

from app.saved_posts_service import (
    build_saved_post_content_from_aiogram_message,
    deserialize_message_entities,
    get_saved_post_short_description,
    serialize_message_entities,
    send_saved_post_content,
)


def test_serialize_entities_keeps_custom_emoji_id():
    entity = SimpleNamespace(type="custom_emoji", offset=0, length=2, custom_emoji_id="123456")
    result = serialize_message_entities([entity])
    assert result[0]["custom_emoji_id"] == "123456"


def test_build_content_text_contains_entities():
    message = SimpleNamespace(
        text="Привет",
        caption=None,
        entities=[SimpleNamespace(type="bold", offset=0, length=6)],
        caption_entities=None,
        photo=None,
        video=None,
        animation=None,
        document=None,
        chat=SimpleNamespace(id=-1001),
        message_id=10,
    )
    content = build_saved_post_content_from_aiogram_message(message)
    assert content["kind"] == "text"
    assert content["text"] == "Привет"
    assert content["entities"][0]["type"] == "bold"


def test_build_content_photo_contains_file_and_caption_entities():
    message = SimpleNamespace(
        text=None,
        caption="Подпись",
        entities=None,
        caption_entities=[SimpleNamespace(type="custom_emoji", offset=0, length=1, custom_emoji_id="777")],
        photo=[SimpleNamespace(file_id="small"), SimpleNamespace(file_id="big", file_unique_id="u1", width=100, height=50)],
        video=None,
        animation=None,
        document=None,
        chat=SimpleNamespace(id=-1002),
        message_id=11,
    )
    content = build_saved_post_content_from_aiogram_message(message)
    assert content["kind"] == "photo"
    assert content["media"]["file_id"] == "big"
    assert content["caption"] == "Подпись"
    assert content["caption_entities"][0]["custom_emoji_id"] == "777"


def test_short_description_readable():
    assert get_saved_post_short_description({"kind": "photo"}) == "фото"


def test_deserialize_message_entities_preserves_custom_emoji_id():
    raw_entities = [{"type": "custom_emoji", "offset": 0, "length": 2, "custom_emoji_id": "999"}]
    result = deserialize_message_entities(raw_entities)
    assert result is not None
    assert result[0].custom_emoji_id == "999"


def test_deserialize_message_entities_empty_returns_none():
    assert deserialize_message_entities([]) is None
    assert deserialize_message_entities(None) is None


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


import asyncio
import pytest


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
