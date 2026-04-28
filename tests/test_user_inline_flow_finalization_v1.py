from pathlib import Path


def test_user_channel_add_callbacks_present():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "user_channel_add_type:" in source
    assert "user_channel_add_entity:" in source


def test_user_channel_remove_callbacks_present():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "user_channel_remove_type" in source or "user_sources_remove" in source
    assert "user_channel_remove_pick:" in source
    assert "user_channel_remove_confirm:" in source


def test_user_cancel_callback_uses_reply_keyboard_remove():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "c.data == \"user_cancel\"" in source
    assert "ReplyKeyboardRemove()" in source


def test_rule_pick_callbacks_still_present():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "user_rule_pick_source:" in source
    assert "user_rule_pick_target:" in source


def test_user_rule_add_screen_is_inline_description_based():
    source = Path("app/user_handlers/rules.py").read_text(encoding="utf-8")
    assert "📤 Выберите источник" in source
    assert "callback_data=\"user_sources_add\"" in source
