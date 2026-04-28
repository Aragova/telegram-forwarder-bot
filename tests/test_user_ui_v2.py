from app import user_ui
from pathlib import Path


def _flatten_texts(markup):
    return [btn.text for row in markup.inline_keyboard for btn in row]


def test_user_main_menu_contains_required_buttons():
    kb = user_ui.build_user_main_keyboard()
    texts = _flatten_texts(kb)
    assert "👤 Мой аккаунт" in texts
    assert "⚙️ Мои правила" in texts
    assert "📡 Мои каналы" in texts
    assert "🌐 Language" in texts
    assert "📊 Живой статус" in texts
    assert "🌍 TimeZone" in texts
    assert "🆘 Поддержка" in texts
    assert "📘 Инструкция" in texts


def test_user_main_menu_not_contains_removed_buttons():
    texts = _flatten_texts(user_ui.build_user_main_keyboard())
    assert "Моя очередь" not in texts
    assert "Использование" not in texts
    assert "Восстановить работу" not in texts
    assert "Оплата" not in texts


def test_language_button_exists_when_style_fallback(monkeypatch):
    real_button = user_ui.InlineKeyboardButton

    def fake_button(**kwargs):
        if "style" in kwargs:
            raise TypeError("style not supported")
        return real_button(**kwargs)

    monkeypatch.setattr(user_ui, "InlineKeyboardButton", fake_button)
    kb = user_ui.build_user_main_keyboard()
    texts = _flatten_texts(kb)
    assert "🌐 Language" in texts


def test_account_keyboard_layout_and_removed_buttons():
    kb = user_ui.build_user_account_keyboard()
    texts = _flatten_texts(kb)
    assert "💎 Сменить тариф" in texts
    assert "🧾 Мои счета" in texts
    assert "⬅️ Главное меню" in texts
    assert all("Использование" not in item for item in texts)
    assert all("Восстановить" not in item for item in texts)
    assert all("Оплата" not in item for item in texts)


def test_timezone_screen_contains_required_buttons():
    kb = user_ui.build_user_timezone_keyboard()
    texts = _flatten_texts(kb)
    assert "UTC+3 Москва" in texts
    assert "UTC+2 Европа" in texts
    assert "UTC+1 Центральная Европа" in texts
    assert "UTC+0 Лондон" in texts
    assert "✏️ Ввести вручную" in texts
    assert "⬅️ Главное меню" in texts


def test_single_button_rows_are_single_for_back_actions():
    kb = user_ui.build_user_account_keyboard()
    assert kb.inline_keyboard[-1][0].text == "⬅️ Главное меню"
    assert len(kb.inline_keyboard[-1]) == 1
    tz = user_ui.build_user_timezone_keyboard()
    assert len(tz.inline_keyboard[-1]) == 1


def test_main_text_uses_subscription_plan_and_not_hardcoded_free():
    text = user_ui.build_user_main_text(
        subscription={"plan_name": "BASIC", "status": "active", "max_rules": 15, "max_video_per_day": 30, "max_jobs_per_day": 1000, "expires_at": "2026-05-26T00:00:00+00:00"},
        usage_today={"video_count": 4, "jobs_count": 120},
        rules_count=3,
        timezone_label="Europe/Moscow · UTC+3",
    )
    assert "💎 Тариф: BASIC" in text
    assert "Тариф: FREE" not in text


def test_main_text_shows_expiry_date():
    text = user_ui.build_user_main_text(
        subscription={"plan_name": "PRO", "status": "active", "max_rules": 50, "max_video_per_day": 100, "max_jobs_per_day": 5000, "expires_at": "2026-05-26T12:00:00+00:00"},
        usage_today={"video_count": 0, "jobs_count": 0},
        rules_count=0,
    )
    assert "📅 Действует до: 26.05.2026" in text


def test_main_text_uses_current_period_end_when_expires_at_missing():
    text = user_ui.build_user_main_text(
        subscription={"plan_name": "BASIC", "status": "active", "max_rules": 15, "max_video_per_day": 30, "max_jobs_per_day": 1000, "current_period_end": "2026-05-26T12:00:00+00:00"},
        usage_today={"video_count": 0, "jobs_count": 0},
        rules_count=0,
    )
    assert "📅 Действует до: 26.05.2026" in text


def test_main_text_prefers_current_period_end_when_both_dates_present():
    text = user_ui.build_user_main_text(
        subscription={
            "plan_name": "BASIC",
            "status": "active",
            "max_rules": 15,
            "max_video_per_day": 30,
            "max_jobs_per_day": 1000,
            "expires_at": "2026-04-30T00:00:00+00:00",
            "current_period_end": "2026-05-26T12:00:00+00:00",
        },
        usage_today={"video_count": 0, "jobs_count": 0},
        rules_count=0,
    )
    assert "📅 Действует до: 26.05.2026" in text


def test_account_text_shows_expiry_date():
    text = user_ui.build_user_account_text(
        user_id=1,
        tenant_id=1,
        subscription={"plan_name": "BASIC", "status": "active", "max_rules": 15, "max_video_per_day": 30, "max_jobs_per_day": 1000, "expires_at": "2026-05-26T00:00:00+00:00"},
        usage_today={"video_count": 1, "jobs_count": 2},
        rules_count=1,
    )
    assert "📅 Действует до: 26.05.2026" in text


def test_plans_text_shows_current_plan():
    plans = [
        {"name": "FREE", "max_rules": 3, "max_video_per_day": 5, "max_jobs_per_day": 100, "price": 0},
        {"name": "BASIC", "max_rules": 15, "max_video_per_day": 30, "max_jobs_per_day": 1000, "price": 9},
        {"name": "PRO", "max_rules": 50, "max_video_per_day": 100, "max_jobs_per_day": 5000, "price": 29},
    ]
    text = user_ui.build_user_plans_text(plans, current_subscription={"plan_name": "BASIC", "status": "active"})
    assert "Ваш текущий тариф: BASIC" in text
    assert "Ваш текущий тариф: FREE" not in text


def test_plans_buttons_have_premium_icons_and_styles():
    kb = user_ui.build_user_plans_keyboard()
    assert kb.inline_keyboard[0][0].text.startswith("🚀")
    assert kb.inline_keyboard[1][0].text.startswith("💎")


def test_plans_buttons_style_fallback_still_keeps_text(monkeypatch):
    real_button = user_ui.InlineKeyboardButton

    def fake_button(**kwargs):
        if "style" in kwargs:
            raise TypeError("style not supported")
        return real_button(**kwargs)

    monkeypatch.setattr(user_ui, "InlineKeyboardButton", fake_button)
    kb = user_ui.build_user_plans_keyboard()
    assert "🚀 Выбрать BASIC" == kb.inline_keyboard[0][0].text
    assert "💎 Выбрать PRO" == kb.inline_keyboard[1][0].text


def test_user_help_sections_have_real_text():
    assert "Каналы" in user_ui.build_user_help_section_text("channels")
    assert "Правило" in user_ui.build_user_help_section_text("rules")
    assert "Оплата" in user_ui.build_user_help_section_text("payment")


def test_no_user_mode_enabled_phrase_in_user_texts():
    source = Path("app/user_ui.py").read_text(encoding="utf-8")
    assert "Пользовательский режим включён" not in source
