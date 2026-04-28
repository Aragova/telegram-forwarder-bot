from __future__ import annotations

import importlib
from pathlib import Path


EXPECTED_ADMIN_TEXTS = [
    "📈 Живой статус",
    "🔄 Правила",
    "📡 Каналы",
    "📦 Очередь",
    "⚠️ Диагностика",
    "⚙️ Система",
]

EXPECTED_USER_CALLBACK_PREFIXES = [
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
    "user_recovery",
    "user_recovery_run",
]


def test_admin_handlers_modules_importable() -> None:
    assert importlib.import_module("app.admin_handlers.menu") is not None
    assert importlib.import_module("app.admin_handlers.channels") is not None
    assert importlib.import_module("app.admin_handlers.queue") is not None
    assert importlib.import_module("app.admin_handlers.diagnostics") is not None
    assert importlib.import_module("app.admin_handlers.system") is not None


def test_admin_register_functions_exist() -> None:
    assert callable(getattr(importlib.import_module("app.admin_handlers.menu"), "register_admin_menu_handlers"))
    assert callable(getattr(importlib.import_module("app.admin_handlers.channels"), "register_admin_channel_handlers"))
    assert callable(getattr(importlib.import_module("app.admin_handlers.queue"), "register_admin_queue_handlers"))
    assert callable(getattr(importlib.import_module("app.admin_handlers.diagnostics"), "register_admin_diagnostics_handlers"))
    assert callable(getattr(importlib.import_module("app.admin_handlers.system"), "register_admin_system_handlers"))


def test_admin_main_menu_texts_kept() -> None:
    source = Path("bot.py").read_text(encoding="utf-8")
    for text in EXPECTED_ADMIN_TEXTS:
        assert text in source, f"missing admin text: {text}"


def test_user_callback_prefixes_unchanged() -> None:
    source = "\n".join(
        [
            Path("app/user_handlers/rules.py").read_text(encoding="utf-8"),
            Path("app/user_handlers/payments.py").read_text(encoding="utf-8"),
            Path("app/user_handlers/recovery.py").read_text(encoding="utf-8"),
        ]
    )
    for callback_data in EXPECTED_USER_CALLBACK_PREFIXES:
        assert callback_data in source, f"callback_data not found: {callback_data}"
