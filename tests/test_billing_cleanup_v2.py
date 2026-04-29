from pathlib import Path


def test_legacy_callbacks_show_deprecated_message() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    for cb in [
        'user_select_plan:',
        'user_confirm_plan:',
        'user_invoice:',
        'user_invoice_pay:',
        'user_invoice_pay_lava:',
        'user_invoices',
        'user_pay_provider:',
    ]:
        assert cb in source
    assert "Этот раздел больше не используется." in source
    assert 'callback_data="user_subscription"' in source


def test_payments_flow_uses_subscription_entry_and_router() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "user_subscription_pay:" in source
    assert "await _start_user_billing_payment(callback, tariff=tariff, period=period, currency=currency, method_code=method_code)" in source
    assert "router.start_payment(" in source


def test_legacy_user_billing_pay_is_disabled() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "⚠️ Данные устарели.\\nОткройте оплату заново." in source
    assert 'text="💎 Подписка"' in source
    assert 'text="⬅️ Главное меню"' in source


def test_short_id_has_owner_and_ttl_checks() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert '"subscription_pay_actions"' in source
    assert "action.get(\"user_id\")" in source
    assert "PAY_ACTION_TTL_SECONDS" in source
    assert "⚠️ Данные устарели. Откройте оплату заново." in source


def test_router_creates_internal_invoice() -> None:
    source = Path("app/payments/payment_router.py").read_text(encoding="utf-8")
    assert "create_draft_invoice" in source
    assert "finalize_invoice" in source
    assert "create_lava_invoice_for_user_invoice" in source
