from __future__ import annotations

import importlib
from pathlib import Path


EXPECTED_CALLBACK_DATA = [
    "user_rules",
    "user_rules_page:",
    "user_rules_add",
    "user_rule_open:",
    "user_select_plan:",
    "user_invoices",
    "user_invoice:",
    "user_invoice_pay:",
    "user_pay_provider:",
    "user_upload_receipt:",
    "user_manual_paid:",
    "user_manual_paid_stub:",
    "user_payment_status:",
    "admin_confirm_manual_payment:",
    "admin_reject_manual_payment:",
    "user_recovery",
    "user_recovery_run",
]


def test_user_handlers_modules_importable() -> None:
    assert importlib.import_module("app.user_handlers.rules") is not None
    assert importlib.import_module("app.user_handlers.payments") is not None
    assert importlib.import_module("app.user_handlers.recovery") is not None


def test_register_functions_exist() -> None:
    rules_module = importlib.import_module("app.user_handlers.rules")
    payments_module = importlib.import_module("app.user_handlers.payments")
    recovery_module = importlib.import_module("app.user_handlers.recovery")

    assert callable(getattr(rules_module, "register_user_rule_handlers"))
    assert callable(getattr(payments_module, "register_user_payment_handlers"))
    assert callable(getattr(recovery_module, "register_user_recovery_handlers"))


def test_callback_data_strings_kept() -> None:
    source = "\n".join(
        [
            Path("app/user_handlers/rules.py").read_text(encoding="utf-8"),
            Path("app/user_handlers/payments.py").read_text(encoding="utf-8"),
            Path("app/user_handlers/recovery.py").read_text(encoding="utf-8"),
        ]
    )
    for callback_data in EXPECTED_CALLBACK_DATA:
        assert callback_data in source, f"callback_data not found: {callback_data}"
