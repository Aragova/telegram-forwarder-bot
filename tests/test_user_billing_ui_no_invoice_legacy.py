from app import user_ui


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
        "Статус счёта",
        "open",
        "draft",
        "Вернуться к счёту",
    ]
    text_blob = "\n".join(_collect_payment_ui_texts())
    for token in forbidden:
        assert token not in text_blob
