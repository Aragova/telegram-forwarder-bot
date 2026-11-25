from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.ai_repost_caption import (
    CaptionUnavailable, ai_mode, config_for, emoji_palette, period, render_caption,
    update_config, units, image_data, prepare_caption,
)


def test_time_boundaries():
    assert [period(datetime(2026, 11, 25, hour, tzinfo=timezone.utc), "UTC")
            for hour in (0, 4, 5, 11, 12, 16, 17, 22, 23)] == [
        "ночь", "ночь", "утро", "утро", "день", "день", "вечер", "вечер", "ночь",
    ]
    assert period(datetime(2026, 11, 25, 9, tzinfo=timezone.utc), "Europe/Moscow") == "день"


def test_required_elements_exact_and_utf16():
    rule = SimpleNamespace(repost_caption="🔥", repost_caption_entities_json=[
        {"type": "custom_emoji", "offset": 0, "length": 2, "custom_emoji_id": "123"}
    ])
    config = update_config(config_for(rule), "elements", "Telegram Stars\nБот | https://t.me/example_bot")
    caption = render_caption({"headline": "Привет 🌞", "body": "[[EMOJI_1]] Оплата: [[FACT_1]]. [[LINK_1]]"},
                             config, emoji_palette(rule))
    assert caption["ai_premium_only"]
    assert caption["entities"][0]["length"] == units("Привет 🌞")
    assert caption["entities"][1]["custom_emoji_id"] == "123"
    assert caption["entities"][2]["url"] == "https://t.me/example_bot"
    data = caption["text"].encode("utf-16-le")
    for entity, expected in zip(caption["entities"], ["Привет 🌞", "🔥", "Бот"]):
        start = entity["offset"] * 2
        assert data[start:start + entity["length"] * 2].decode("utf-16-le") == expected


@pytest.mark.parametrize("body", ["нет ссылки", "[[LINK_1]] [[LINK_1]]", "[[LINK_2]]", "https://t.me/bad [[LINK_1]]"])
def test_invalid_generation_is_rejected(body):
    config = update_config(config_for(SimpleNamespace()), "elements", "Бот | https://t.me/example_bot")
    with pytest.raises(CaptionUnavailable):
        render_caption({"headline": "Привет", "body": body}, config, {})


def test_config_validation():
    config = config_for(SimpleNamespace())
    with pytest.raises(ValueError):
        update_config(config, "timezone", "not-a-timezone")
    with pytest.raises(ValueError):
        update_config(config, "elements", "Бот | https://evil.example/")
    with pytest.raises(ValueError):
        update_config(config, "context", "https://t.me/private")
    assert update_config(config, "elements", "-")["links"] == []


def test_only_repost_ai():
    assert ai_mode(SimpleNamespace(mode="repost", repost_caption_delivery_mode="ai"))
    assert not ai_mode(SimpleNamespace(mode="video", repost_caption_delivery_mode="ai"))
    assert not ai_mode(SimpleNamespace(mode="repost", repost_caption_delivery_mode="builder_first"))


def test_invalid_image_and_length():
    with pytest.raises(CaptionUnavailable):
        image_data(b"not jpeg")
    with pytest.raises(CaptionUnavailable):
        render_caption({"headline": "Hi", "body": "a" * 1024}, config_for(SimpleNamespace()), {})


@pytest.mark.asyncio
async def test_persisted_caption_reused_without_download(monkeypatch):
    monkeypatch.setenv("VIMI_AI_CAPTION_ENABLED", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    expected = {"text": "Ready", "entities": [], "ai_premium_only": True}
    owner = SimpleNamespace(db=SimpleNamespace(
        get_delivery_attempt_by_idempotency_key=lambda key: {
            "status": "sending", "rule_id": 7, "ai_caption_json": expected
        }
    ))
    assert await prepare_caption(owner, SimpleNamespace(id=7), [], "key") == expected


@pytest.mark.asyncio
async def test_missing_attempt_blocks_generation(monkeypatch):
    monkeypatch.setenv("VIMI_AI_CAPTION_ENABLED", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    with pytest.raises(CaptionUnavailable):
        await prepare_caption(SimpleNamespace(), SimpleNamespace(id=1), [], None)
