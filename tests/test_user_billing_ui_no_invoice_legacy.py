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

    assert "result.provider or \"\") != \"crypto_manual\"" in source
    assert "result.status or \"\").lower() not in allowed_statuses" in source

    crypto_block = source.split("handle_user_subscription_crypto_callback", 1)[1].split('c.data.startswith("user_upload_receipt:")', 1)[0]
    assert "user_upload_receipt" not in crypto_block
    assert "ПРОВЕРИТЬ ОПЛАТУ" in source
    assert "🆘 Поддержка" in source
    assert "👉 Назад" in source
    assert "🏠 Меню" in source

    assert "⚠️ Не удалось создать ручную оплату" in source
    assert "🔁 Попробовать снова" in source
    assert "💳 Выбрать другой способ" in source



def test_legacy_user_upload_receipt_message() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "Этот способ больше не используется." in source
    assert "Просто отправьте скриншот оплаты сюда в чат." in source


def test_auto_receipt_handler_supports_crypto_manual_provider() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert '"crypto_manual"' in source
    assert "_find_active_manual_payment_for_user" in source
    assert "✅ Вы успешно отправили скриншот! Ожидайте ответа." in source


def test_uah_manual_bank_screen_texts_and_guard() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    details_source = Path("app/payments/manual_bank_details.py").read_text(encoding="utf-8")
    assert "Payment details:" in source
    assert "4323347388778133" in details_source
    assert "4483820043174381" in details_source
    assert "5355280059027787" in details_source
    assert "✏️ Отправьте скриншот оплаты сюда в чат." in source
    assert "ПРОВЕРИТЬ ОПЛАТУ" in source
    assert "🆘 Поддержка" in source
    assert "int(result.payment_intent_id or 0) > 0" in source


def test_admin_notification_contains_bank_title_and_card_number() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "provider_payload_json" in source
    assert "bank_title" in source
    assert "card_number" in source
    assert "Карта:" in source

def test_manual_bank_details_helper_is_imported_for_uah_flow() -> None:
    source = Path("app/user_handlers/payments.py").read_text(encoding="utf-8")
    assert "from app.payments.manual_bank_details import get_manual_bank_details" in source