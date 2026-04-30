from app import user_ui
from pathlib import Path


def _collect_payment_ui_texts() -> list[str]:
    invoice = {"id": 7, "status": "open", "currency": "USD", "total": 25}
    items = [{"description": "BASIC", "amount": 25, "metadata_json": {"plan_name": "BASIC"}}]
    payment_intent = {"id": 11, "provider": "manual_bank_card", "status": "pending", "confirmation_payload_json": {}}
    payment_result = {"provider": "manual_bank_card", "status": "created", "message_ru": "Оплатите по реквизитам"}

    return [
        user_ui.build_user_invoice_text(invoice, items),
        user_ui.build_user_invoices_text([{"id": 7, "total": 25, "currency": "USD", "status": "open", "items": items}]),
        user_ui.build_user_payment_methods_text(invoice, [{"provider": "manual_bank_card"}]),
        user_ui.build_user_payment_result_text(invoice, payment_result),
        user_ui.build_user_payment_status_text(invoice, payment_intent),
        user_ui.build_user_manual_receipt_request_text(invoice, payment_intent),
        user_ui.build_user_manual_receipt_uploaded_text(invoice, payment_intent),
    ]


def test_user_payment_ui_has_no_legacy_invoice_terms() -> None:
    forbidden = [
        "Счёт #",
        "Счёт не найден",
        "Статус счёта",
        "open",
        "draft",
        "Вернуться к счёту",
    ]
    text_blob = "\n".join(_collect_payment_ui_texts())
    for token in forbidden:
        assert token not in text_blob


def test_crypto_flow_uses_created_result_and_no_last_invoice_lookup() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "handle_user_subscription_crypto_callback" in source
    assert "get_last_invoice" not in source
    assert "result = await router.start_payment(" in source
    assert "result.invoice_id" in source
    assert "result.payment_intent_id" in source
    crypto_block = source.split("handle_user_subscription_crypto_callback", 1)[1].split('c.data.startswith("user_upload_receipt:")', 1)[0]
    assert "user_upload_receipt" not in crypto_block
    assert "ПРОВЕРИТЬ ОПЛАТУ" in source
    assert "🆘 Поддержка" in source
    assert "👉 Назад" in source
    assert "🏠 Меню" in source


def test_legacy_user_upload_receipt_message() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "Этот способ больше не используется." in source
    assert "Просто отправьте скриншот оплаты сюда в чат." in source


def test_auto_receipt_handler_supports_crypto_manual_provider() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert '"crypto_manual"' in source
    assert "_find_active_manual_payment_for_user" in source
    assert "✅ Вы успешно отправили скриншот! Ожидайте ответа." in source
