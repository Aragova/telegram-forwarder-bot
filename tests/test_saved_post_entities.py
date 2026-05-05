from types import SimpleNamespace

from app.saved_post_entities import (
    deserialize_message_entities,
    normalize_saved_post_entities,
    saved_post_entities_to_telethon,
    saved_post_requires_premium_send,
    serialize_message_entities,
)
from telethon.tl import types as tl_types


def test_serialize_entities_keeps_custom_emoji_id():
    entity = SimpleNamespace(type="custom_emoji", offset=0, length=2, custom_emoji_id="123456")
    result = serialize_message_entities([entity])
    assert result[0]["custom_emoji_id"] == "123456"


def test_deserialize_message_entities_preserves_custom_emoji_id():
    raw_entities = [{"type": "custom_emoji", "offset": 0, "length": 2, "custom_emoji_id": "999"}]
    result = deserialize_message_entities(raw_entities)
    assert result is not None
    assert result[0].custom_emoji_id == "999"


def test_deserialize_message_entities_empty_returns_none():
    assert deserialize_message_entities([]) is None
    assert deserialize_message_entities(None) is None


def test_saved_post_requires_premium_send_true_for_custom_emoji():
    assert saved_post_requires_premium_send({"entities": [{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "1"}]}) is True


def test_saved_post_requires_premium_send_false_for_plain_entities():
    assert saved_post_requires_premium_send({"entities": [{"type": "bold", "offset": 0, "length": 2}]}) is False


def test_saved_post_entities_to_telethon_custom_emoji():
    entities = saved_post_entities_to_telethon([{"type": "custom_emoji", "offset": 0, "length": 1, "custom_emoji_id": "777"}])
    assert len(entities) == 1
    assert isinstance(entities[0], tl_types.MessageEntityCustomEmoji)
    assert entities[0].document_id == 777


def test_normalize_saved_post_entities_keeps_custom_emoji_id():
    normalized = normalize_saved_post_entities([{"type": "custom_emoji", "offset": "1", "length": "2", "custom_emoji_id": 12345}])
    assert len(normalized) == 1
    assert normalized[0]["custom_emoji_id"] == "12345"
    assert normalized[0]["offset"] == 1
    assert normalized[0]["length"] == 2


def test_normalize_saved_post_entities_supports_json_string():
    normalized = normalize_saved_post_entities('[{"type":"bold","offset":"0","length":"5"}]')
    assert len(normalized) == 1
    assert normalized[0]["type"] == "bold"


def test_custom_emoji_roundtrip_via_serialize_deserialize():
    payload = serialize_message_entities([SimpleNamespace(type="custom_emoji", offset=1, length=2, custom_emoji_id="555")])
    roundtrip = deserialize_message_entities(payload)
    assert roundtrip is not None
    assert roundtrip[0].custom_emoji_id == "555"
