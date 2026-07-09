from __future__ import annotations

import json

from app.delivery_content_helpers import (
    build_video_caption_delivery_payload,
    content_requires_builder,
    extract_text_from_content,
    normalize_caption_delivery_mode,
    normalize_caption_entities,
    video_caption_requires_premium,
)


def test_extract_text_from_content_returns_text_and_empty_default():
    assert extract_text_from_content({"text": "подпись"}) == "подпись"
    assert extract_text_from_content({}) == ""
    assert extract_text_from_content(None) == ""


def test_normalize_caption_entities_from_json_string():
    raw_entities = json.dumps([
        {"type": "BOLD", "offset": "0", "length": "7"},
        {"type": "text_link", "offset": 8, "length": 4, "url": "https://example.com"},
    ])

    assert normalize_caption_entities(raw_entities) == [
        {"type": "bold", "offset": 0, "length": 7},
        {"type": "text_link", "offset": 8, "length": 4, "url": "https://example.com"},
    ]


def test_normalize_caption_entities_defaults_and_malformed_payloads():
    assert normalize_caption_entities(None) == []
    assert normalize_caption_entities("") == []
    assert normalize_caption_entities({"type": "italic", "offset": 1, "length": 2}) == [
        {"type": "italic", "offset": 1, "length": 2}
    ]
    assert normalize_caption_entities({"type": "bold", "offset": -1, "length": 2}) == []
    assert normalize_caption_entities("not json") == []
    assert normalize_caption_entities({"unexpected": "shape"}) == []


def test_normalize_caption_entities_does_not_mutate_input():
    raw_entities = [
        {
            "type": "CUSTOM_EMOJI",
            "offset": "0",
            "length": "2",
            "custom_emoji_id": 12345,
        }
    ]
    original = [dict(raw_entities[0])]

    normalized = normalize_caption_entities(raw_entities)

    assert raw_entities == original
    assert normalized == [
        {
            "type": "custom_emoji",
            "offset": 0,
            "length": 2,
            "custom_emoji_id": "12345",
        }
    ]


def test_normalize_caption_delivery_mode():
    assert normalize_caption_delivery_mode("auto") == "auto"
    assert normalize_caption_delivery_mode("copy_first") == "copy_first"
    assert normalize_caption_delivery_mode("builder_first") == "builder_first"
    assert normalize_caption_delivery_mode(" BUILDER_FIRST ") == "builder_first"
    assert normalize_caption_delivery_mode(None) == "auto"
    assert normalize_caption_delivery_mode("unknown") == "auto"


def test_content_requires_builder_only_when_entity_has_type():
    assert content_requires_builder({"entities": [{"type": "bold", "offset": 0, "length": 4}]}) is True
    assert content_requires_builder({"entities": [{"type": "", "offset": 0, "length": 4}]}) is False
    assert content_requires_builder({"entities": ["bad-shape"]}) is False
    assert content_requires_builder({}) is False
    assert content_requires_builder(None) is False


def test_video_caption_requires_premium_for_custom_emoji_only():
    assert video_caption_requires_premium([{"type": "custom_emoji", "offset": 0, "length": 2}]) is True
    assert video_caption_requires_premium([{"type": "bold", "offset": 0, "length": 2}]) is False
    assert video_caption_requires_premium(None) is False


def test_build_video_caption_delivery_payload_modes_and_manual_caption_priority():
    raw_entities = [{"type": "bold", "offset": 0, "length": 4}]

    auto_payload = build_video_caption_delivery_payload(
        caption="ручная подпись",
        raw_caption_entities=raw_entities,
        caption_delivery_mode="auto",
    )
    assert auto_payload["caption"] == "ручная подпись"
    assert auto_payload["caption_entities"] == raw_entities
    assert auto_payload["caption_delivery_mode"] == "auto"
    assert auto_payload["selected_mode"] == "premium"
    assert auto_payload["has_any_entities"] is True
    assert auto_payload["requires_premium"] is False
    assert json.loads(auto_payload["caption_entities_json"]) == raw_entities

    copy_payload = build_video_caption_delivery_payload(
        caption="ручная подпись",
        raw_caption_entities=raw_entities,
        caption_delivery_mode="copy_first",
    )
    assert copy_payload["selected_mode"] == "plain"

    builder_payload = build_video_caption_delivery_payload(
        caption="ручная подпись",
        raw_caption_entities=[],
        caption_delivery_mode="builder_first",
    )
    assert builder_payload["selected_mode"] == "premium"
    assert builder_payload["caption_entities_json"] is None


def test_build_video_caption_delivery_payload_malformed_entities_keeps_existing_defaults():
    payload = build_video_caption_delivery_payload(
        caption=None,
        raw_caption_entities="not json",
        caption_delivery_mode="unknown",
    )

    assert payload == {
        "caption": "",
        "caption_entities": [],
        "caption_entities_json": None,
        "caption_delivery_mode": "auto",
        "requires_premium": False,
        "has_any_entities": False,
        "selected_mode": "plain",
    }


def test_content_requires_builder_custom_emoji_variants():
    from app.delivery_content_helpers import content_requires_builder

    variants = [
        {"type": "custom_emoji", "custom_emoji_id": "123"},
        {"type": "MessageEntityCustomEmoji", "custom_emoji_id": "123"},
        {"type": "messageentitycustomemoji", "document_id": "123"},
        {"_": "MessageEntityCustomEmoji", "document_id": "123"},
    ]

    for entity in variants:
        assert content_requires_builder({"entities": [entity]}) is True


def test_content_requires_builder_checks_alternate_entity_fields():
    from app.delivery_content_helpers import content_requires_builder

    assert content_requires_builder({
        "text": "x",
        "caption_entities": [
            {"type": "custom_emoji", "custom_emoji_id": "123"}
        ],
    }) is True

    assert content_requires_builder({
        "text": "x",
        "text_entities": [
            {"type": "MessageEntityCustomEmoji", "document_id": "123"}
        ],
    }) is True

    assert content_requires_builder({
        "text": "x",
        "raw_entities": [
            {"_": "MessageEntityCustomEmoji", "document_id": "123"}
        ],
    }) is True
