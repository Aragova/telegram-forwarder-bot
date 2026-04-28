from __future__ import annotations

from pathlib import Path

from app import user_ui


def _callbacks(keyboard):
    return [button.callback_data or "" for row in keyboard.inline_keyboard for button in row]


def test_lava_callbacks_exist_in_handlers_source():
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "user_subscription" in source
    assert "user_tariff_basic" in source
    assert "user_pay_lava_basic" in source


def test_lava_invoice_keyboard_uses_url_button():
    keyboard = user_ui.build_lava_invoice_keyboard(payment_url="https://gate.lava.top/pay/abc")
    first_button = keyboard.inline_keyboard[0][0]
    assert first_button.url == "https://gate.lava.top/pay/abc"


def test_user_payment_flow_inline_only_no_reply_keyboard_markup():
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "ReplyKeyboardMarkup" not in source


def test_admin_menu_not_touched_and_critical_files_untouched():
    admin_source = Path("app/admin_handlers/menu.py").read_text(encoding="utf-8")
    assert "register_admin_menu_handlers" in admin_source

    assert not Path("scheduler_runtime.py").exists()
    assert not Path("worker_runtime.py").exists()
    assert not Path("sender.py").exists()

    assert Path("app/scheduler_runtime.py").exists()
    assert Path("app/worker_runtime.py").exists()
    assert Path("app/sender.py").exists()
