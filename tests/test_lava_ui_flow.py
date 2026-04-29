from __future__ import annotations

from pathlib import Path

from app import user_ui


def _callbacks(keyboard):
    return [button.callback_data or "" for row in keyboard.inline_keyboard for button in row]


def test_lava_callbacks_exist_in_handlers_source():
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "user_invoice_pay_lava:" in source


def test_lava_invoice_keyboard_uses_url_button():
    keyboard = user_ui.build_lava_invoice_keyboard(invoice_id=12, payment_url="https://gate.lava.top/pay/abc")
    first_button = keyboard.inline_keyboard[0][0]
    assert first_button.url == "https://gate.lava.top/pay/abc"
    assert keyboard.inline_keyboard[1][0].callback_data == "user_invoice_check_payment:12"
    assert keyboard.inline_keyboard[2][0].callback_data == "user_invoice_pay:12"
    assert keyboard.inline_keyboard[3][0].callback_data == "user_invoice_pay:12"


def test_tariff_screen_has_no_lava_button():
    keyboard = user_ui.build_user_plans_keyboard()
    button_texts = [button.text for row in keyboard.inline_keyboard for button in row]
    callbacks = _callbacks(keyboard)
    assert "💳 Оплатить BASIC — $9" not in button_texts
    assert "user_pay_lava_basic" not in callbacks


def test_invoice_payment_methods_show_lava_when_enabled(monkeypatch):
    monkeypatch.setattr(user_ui.settings, "lava_top_enabled", True)
    keyboard = user_ui.build_user_payment_methods_keyboard(invoice_id=7, methods=[{"provider": "manual_bank_card"}])
    callbacks = _callbacks(keyboard)
    assert "user_invoice_pay_lava:7" in callbacks


def test_invoice_payment_methods_hide_lava_when_disabled(monkeypatch):
    monkeypatch.setattr(user_ui.settings, "lava_top_enabled", False)
    keyboard = user_ui.build_user_payment_methods_keyboard(invoice_id=7, methods=[{"provider": "manual_bank_card"}])
    callbacks = _callbacks(keyboard)
    assert "user_invoice_pay_lava:7" not in callbacks


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


def test_invoice_screen_has_check_payment_button():
    keyboard = user_ui.build_user_invoice_keyboard(12)
    callbacks = _callbacks(keyboard)
    assert "user_invoice_check_payment:12" in callbacks


def test_check_payment_callback_exists_in_handlers_source():
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "user_invoice_check_payment:" in source
