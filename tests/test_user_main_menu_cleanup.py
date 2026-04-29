from pathlib import Path

from app import user_ui


def _flatten_callbacks(markup):
    return [btn.callback_data for row in markup.inline_keyboard for btn in row if getattr(btn, "callback_data", None)]


def test_main_menu_has_no_user_account_callback() -> None:
    callbacks = _flatten_callbacks(user_ui.build_user_main_keyboard())
    assert "user_account" not in callbacks


def test_user_account_callback_shows_kill_screen() -> None:
    source = Path("bot.py").read_text(encoding="utf-8")
    assert 'lambda c: c.data == "user_account"' in source
    assert "Этот раздел больше не используется." in source
    assert "callback_data=\"user_subscription\"" in source
    assert "callback_data=\"user_main\"" in source


def test_legacy_callbacks_are_disabled_and_do_not_open_old_payment_ui() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    bot_source = Path("bot.py").read_text(encoding="utf-8")
    assert 'lambda c: c.data == "user_plans"' in bot_source
    for cb in [
        "user_select_plan:",
        "user_confirm_plan:",
        "user_invoices",
        "user_invoice:",
        "user_invoice_pay:",
        "user_invoice_pay_lava:",
        "user_pay_provider:",
    ]:
        assert cb in source
    assert "Мои платежи" not in source
