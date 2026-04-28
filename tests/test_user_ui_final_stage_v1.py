from pathlib import Path


def test_non_admin_start_menu_remove_reply_keyboard():
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert "ReplyKeyboardRemove()" in bot_source


def test_timezone_button_exists_in_main_menu():
    source = Path("app/user_ui.py").read_text(encoding="utf-8")
    assert "🌍 TimeZone" in source


def test_no_user_mode_phrase_anywhere():
    joined = "\n".join(
        [
            Path("bot.py").read_text(encoding="utf-8"),
            Path("app/admin_handlers/menu.py").read_text(encoding="utf-8"),
            Path("app/user_ui.py").read_text(encoding="utf-8"),
        ]
    )
    assert "Пользовательский режим включён" not in joined
