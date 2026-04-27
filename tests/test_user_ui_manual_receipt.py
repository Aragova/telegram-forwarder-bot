from app import user_ui


def _callbacks(keyboard):
    return [
        button.callback_data or ""
        for row in keyboard.inline_keyboard
        for button in row
    ]


def test_payment_result_keyboard_shows_attach_receipt_for_manual_waiting_confirmation():
    keyboard = user_ui.build_user_payment_result_keyboard(
        invoice_id=4,
        payment_result={"provider": "manual_bank_card", "status": "waiting_confirmation"},
    )
    callbacks = _callbacks(keyboard)
    assert "user_upload_receipt:4" in callbacks


def test_payment_result_keyboard_hides_attach_receipt_for_paid_status():
    keyboard = user_ui.build_user_payment_result_keyboard(
        invoice_id=4,
        payment_result={"provider": "manual_bank_card", "status": "paid"},
    )
    callbacks = _callbacks(keyboard)
    assert "user_upload_receipt:4" not in callbacks


def test_payment_status_keyboard_shows_attach_receipt_for_active_manual_intent():
    keyboard = user_ui.build_user_payment_status_keyboard(
        invoice_id=7,
        payment_intent={"provider": "sbp_provider", "status": "pending"},
    )
    callbacks = _callbacks(keyboard)
    assert "user_upload_receipt:7" in callbacks
