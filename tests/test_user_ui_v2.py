from app import user_ui


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
