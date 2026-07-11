import logging
from types import SimpleNamespace

from telethon import types

from app.sender_content_helpers import SenderContentHelpers


def _message(text, entities):
    return SimpleNamespace(raw_text=text, text=text, message=text, entities=entities, media=None, date=None)


def test_content_merge_equal_text_merges_live_entities(caplog):
    caplog.set_level(logging.WARNING, logger="forwarder")
    helper = SenderContentHelpers(owner=SimpleNamespace())
    live_entity = types.MessageEntityBold(offset=0, length=5)

    result = helper.content_from_message_or_post(
        message=_message("hello caption", [live_entity]),
        post_row={"content_json": {"text": "hello caption", "entities": [], "has_media": True, "media_kind": "video"}},
    )

    assert result["text"] == "hello caption"
    assert result["entities"] == [{"offset": 0, "length": 5, "type": "bold"}]
    assert "merged_live_entities" in caplog.text


def test_content_merge_different_text_prefers_live_content(caplog):
    caplog.set_level(logging.WARNING, logger="forwarder")
    helper = SenderContentHelpers(owner=SimpleNamespace())
    live_text = "Normal Telegram caption with link and emoji"
    post_text = "**Markdown** caption with [link](https://example.com) " + ("x" * 180)
    live_entities = [types.MessageEntityBold(offset=0, length=6), types.MessageEntityUrl(offset=29, length=4)]

    result = helper.content_from_message_or_post(
        message=_message(live_text, live_entities),
        post_row={"content_json": {"text": post_text, "entities": [], "has_media": True, "media_kind": "video"}},
    )

    assert result["text"] == live_text
    assert result["text"] != post_text
    assert result["entities"] == [
        {"offset": 0, "length": 6, "type": "bold"},
        {"offset": 29, "length": 4, "type": "url"},
    ]
    assert "prefer_live_content_text_mismatch" in caplog.text
    assert "merged_live_entities" not in caplog.text


def test_custom_emoji_content_builds_telethon_entity_without_markdown_artifacts():
    helper = SenderContentHelpers(owner=SimpleNamespace())
    text = "A premium caption"
    result = helper.content_from_message_or_post(
        message=_message(text, [types.MessageEntityCustomEmoji(offset=0, length=1, document_id=123456)]),
        post_row={"content_json": {"text": text, "entities": [], "has_media": True, "media_kind": "video"}},
    )

    built_text, built_entities = helper.build_text_and_entities_from_content(result)

    assert built_text == text
    assert "**" not in built_text
    assert "[" not in built_text
    assert any(isinstance(entity, types.MessageEntityCustomEmoji) for entity in built_entities)
