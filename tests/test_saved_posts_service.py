from types import SimpleNamespace

from app.saved_posts_service import (
    build_saved_post_content_from_aiogram_message,
    deserialize_message_entities,
    get_saved_post_short_description,
    saved_post_entities_to_telethon,
    saved_post_requires_premium_send,
    serialize_message_entities,
    send_saved_post_content,
    summarize_aiogram_message_for_saved_post,
    summarize_saved_post_entities,
)
from telethon.tl import types as tl_types


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


def test_build_content_text_has_empty_caption_fields():
    message = SimpleNamespace(
        text="hello",
        caption=None,
        entities=[SimpleNamespace(type="italic", offset=0, length=5)],
        caption_entities=None,
        photo=None,
        video=None,
        animation=None,
        document=None,
        chat=SimpleNamespace(id=-1003),
        message_id=12,
    )
    content = build_saved_post_content_from_aiogram_message(message)
    assert content["kind"] == "text"
    assert content["text"] == "hello"
    assert content["entities"]
    assert content["caption"] == ""


def test_summarize_message_and_entities_for_media():
    message = SimpleNamespace(
        message_id=123,
        content_type="photo",
        text=None,
        entities=None,
        caption="hello",
        caption_entities=[SimpleNamespace(type="custom_emoji", offset=0, length=1, custom_emoji_id="ce1")],
        photo=[SimpleNamespace(file_id="a"), SimpleNamespace(file_id="b", file_unique_id="u2", width=20, height=20)],
        video=None,
        animation=None,
        document=None,
        media_group_id="grp1",
        forward_origin=SimpleNamespace(),
    )
    summary = summarize_aiogram_message_for_saved_post(message)
    assert summary["has_caption"] is True
    assert summary["caption_len"] == 5
    assert summary["caption_entities_count"] == 1

    content = build_saved_post_content_from_aiogram_message(message)
    entity_summary = summarize_saved_post_entities(content)
    assert entity_summary["caption_entities_count"] == 1
    assert entity_summary["caption_entities_custom_emoji_count"] == 1


def test_custom_emoji_entity_roundtrip_saved_post_content():
    message = SimpleNamespace(
        text=None,
        caption="emoji",
        entities=None,
        caption_entities=[SimpleNamespace(type="custom_emoji", offset=0, length=2, custom_emoji_id="555")],
        photo=[SimpleNamespace(file_id="small"), SimpleNamespace(file_id="big", file_unique_id="u5", width=100, height=100)],
        video=None,
        animation=None,
        document=None,
        chat=SimpleNamespace(id=-1004),
        message_id=13,
    )
    content = build_saved_post_content_from_aiogram_message(message)
    roundtrip = deserialize_message_entities(content["caption_entities"])
    assert roundtrip is not None
    assert roundtrip[0].custom_emoji_id == "555"


def test_saved_post_requires_premium_send_true_for_custom_emoji():
    assert saved_post_requires_premium_send({"entities": [{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "1"}]}) is True


def test_saved_post_requires_premium_send_false_for_plain_entities():
    assert saved_post_requires_premium_send({"entities": [{"type": "bold", "offset": 0, "length": 2}]}) is False


def test_saved_post_entities_to_telethon_custom_emoji():
    entities = saved_post_entities_to_telethon([{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "777"}])
    assert len(entities) == 1
    assert isinstance(entities[0], tl_types.MessageEntityCustomEmoji)
    assert entities[0].document_id == 777
