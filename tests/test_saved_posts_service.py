from types import SimpleNamespace

from app.saved_posts_service import (
    build_saved_post_content_from_aiogram_message,
    deserialize_message_entities,
    get_saved_post_short_description,
    serialize_message_entities,
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
