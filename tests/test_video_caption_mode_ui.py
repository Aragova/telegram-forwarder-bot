"""Video caption UI offers two formatting choices, never a transport selector."""
from pathlib import Path


def test_video_mode_menu_has_two_buttons_and_repost_keeps_auto():
    source = Path("bot.py").read_text(encoding="utf-8")
    menu = source.split("def build_video_caption_mode_keyboard(", 1)[1].split("\ndef ", 1)[0]
    text = source.split("def build_video_caption_mode_menu_text(", 1)[1].split("\ndef ", 1)[0]
    assert "set_video_caption_mode_copy_first" in menu
    assert "set_video_caption_mode_builder_first" in menu
    assert "set_video_caption_mode_auto" not in menu
    assert "🤖 Авто" not in text
    assert "🎛 <b>Режим подписи</b>" in text
    assert "обычные смайлики" in text
    assert "premium emoji" in text
    assert "Telegram Bot API" not in text
    assert "Telethon" not in text
    assert 'text="🎛 Режим подписи"' in source
    assert "set_caption_mode_auto" in source
    assert "def handle_set_caption_mode_auto(" not in source


def test_video_rule_db_defaults_and_legacy_conversion():
    source = Path("app/postgres_repository.py").read_text(encoding="utf-8")
    assert "video_caption_delivery_mode TEXT NOT NULL DEFAULT 'copy_first'" in source
    assert "video_caption_delivery_mode SET DEFAULT 'copy_first'" in source
    assert "BTRIM(video_caption_entities_json)" in source
    assert '"copy_first", "builder_first", "auto"' not in source.split(
        "def update_rule_video_caption_delivery_mode(", 1
    )[1].split("\n    def ", 1)[0]
