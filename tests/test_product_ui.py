from app.i18n import get_user_language, set_user_language
from app import product_ui


def _subscription(plan: str = "PRO"):
    return {
        "plan_name": plan,
        "status": "active",
        "current_period_start": "2026-04-01",
        "current_period_end": "2026-04-30",
        "max_rules": 50,
        "max_jobs_per_day": 1000,
        "max_video_per_day": 100,
        "max_storage_mb": 1000,
    }


def test_ru_account_screen_formatting():
    text = product_ui.account_screen(
        lang="ru",
        subscription=_subscription("PRO"),
        usage_today={"jobs_count": 320, "video_count": 12, "storage_used_mb": 120},
        usage_period={"jobs_count": 8240},
        last_invoice={"id": 15, "status": "open", "total": 29.0, "currency": "USD"},
        rules_count=8,
    )
    assert "👤 Мой аккаунт" in text
    assert "💎 Тариф: PRO" in text
    assert "📋 Правила: 8 / 50" in text


def test_en_account_screen_formatting():
    text = product_ui.account_screen(
        lang="en",
        subscription=_subscription("PRO"),
        usage_today={"jobs_count": 320, "video_count": 12, "storage_used_mb": 120},
        usage_period={"jobs_count": 8240},
        last_invoice={"id": 15, "status": "open", "total": 29.0, "currency": "USD"},
        rules_count=8,
    )
    assert "👤 My account" in text
    assert "💎 Plan: PRO" in text
    assert "Rules: 8 / 50" in text


def test_plans_screen_hides_owner():
    text = product_ui.plans_screen(lang="ru", plans=[{"name": "OWNER", "description": "", "max_rules": 0, "max_video_per_day": 0, "max_jobs_per_day": 0, "price": 0}, {"name": "FREE", "description": "x", "max_rules": 3, "max_video_per_day": 5, "max_jobs_per_day": 100, "price": 0}])
    assert "OWNER" not in text
    assert "FREE" in text


def test_owner_account_shows_unlimited():
    text = product_ui.account_screen(
        lang="ru",
        subscription=_subscription("OWNER"),
        usage_today={},
        usage_period={},
        last_invoice=None,
        rules_count=999,
    )
    assert "без ограничений" in text


def test_usage_progress_formatting():
    text = product_ui.usage_screen(
        lang="en",
        today={"jobs_count": 320, "video_count": 12},
        period={"jobs_count": 8240, "video_count": 215, "storage_used_mb": 420},
        limits={"max_jobs_per_day": 1000, "max_video_per_day": 100},
    )
    assert "32%" in text
    assert "12%" in text


def test_invoice_screen_formatting():
    text = product_ui.invoice_screen(
        lang="en",
        invoice={"id": 15, "status": "open", "period_start": "2026-04-01", "period_end": "2026-04-30", "total": 29, "currency": "USD"},
        items=[{"description": "PRO plan", "amount": 29, "metadata": {"plan_name": "PRO"}}],
    )
    assert "Invoice #15" in text
    assert "Total: 29.00 USD" in text


def test_limit_error_messages_ru_en():
    ru = product_ui.rule_limit_error("ru", "FREE", 3, 3)
    en = product_ui.rule_limit_error("en", "FREE", 3, 3)
    assert "Лимит правил" in ru
    assert "Rule limit reached" in en


def test_language_selection_changes_language():
    assert set_user_language(123, "en") == "en"
    assert get_user_language(123) == "en"
    assert set_user_language(123, "ru") == "ru"
    assert get_user_language(123) == "ru"


def test_upgrade_confirm_flow_object():
    flow = product_ui.build_upgrade_invoice_flow(plan_name="PRO", price=29)
    assert flow["item_type"] == "base_plan"
    assert flow["unit_price"] == 29.0


def test_callbacks_do_not_expose_internal_terms():
    keyboards = [
        product_ui.plans_keyboard("ru"),
        product_ui.account_keyboard("ru"),
        product_ui.invoice_keyboard("ru"),
        product_ui.language_keyboard(),
    ]
    joined = " ".join(
        button.callback_data or ""
        for kb in keyboards
        for row in kb.inline_keyboard
        for button in row
    )
    for bad in ("tenant_id", "job_id", "dedup", "worker"):
        assert bad not in joined
