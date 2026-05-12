from pathlib import Path


def test_admin_menu_uses_non_empty_reply_keyboard_remove_message() -> None:
    source = Path("app/admin_handlers/menu.py").read_text(encoding="utf-8")
    assert 'message.answer(" ", reply_markup=ReplyKeyboardRemove())' not in source
    assert source.count('message.answer("Меню открыто.", reply_markup=ReplyKeyboardRemove())') == 2
