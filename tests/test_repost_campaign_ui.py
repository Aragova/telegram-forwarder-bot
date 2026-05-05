from app.repost_campaign_ui import (
    build_repost_campaign_menu_view,
    build_repost_campaign_post_menu_view,
    build_repost_campaign_preview_view,
    build_repost_campaign_show_menu_view,
    format_repost_campaign_readiness_block,
)


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def test_post_menu_view_without_saved_post_has_add_button():
    _, keyboard = build_repost_campaign_post_menu_view(rule_id=3, saved_post_id=None, saved_post_description=None)
    texts = _texts_from_keyboard(keyboard)
    assert "➕ Добавить пост" in texts


def test_post_menu_view_with_saved_post_has_expected_buttons():
    _, keyboard = build_repost_campaign_post_menu_view(rule_id=3, saved_post_id=77, saved_post_description="текст")
    texts = _texts_from_keyboard(keyboard)
    assert "👁 Предпросмотр поста" in texts
    assert "🔁 Заменить пост" in texts
    assert "🗑 Убрать из кампании" in texts


def test_show_menu_view_has_presets_and_back():
    _, keyboard = build_repost_campaign_show_menu_view(rule_id=3, current_show_seconds_text="1 час")
    texts = _texts_from_keyboard(keyboard)
    assert "1 минута 🧪" in texts
    assert "48 часов" in texts
    assert "⬅️ Назад" in texts


def test_preview_view_contains_saved_post_id_and_warnings():
    text, _ = build_repost_campaign_preview_view(
        rule_id=3,
        saved_post_id=55,
        saved_post_description="текст",
        show_seconds_text="1 час",
        targets_active=2,
        targets_ready=1,
        targets_with_errors=1,
        targets_preview_text="1. 🟢 @a",
        warnings=["⚠️ Проверка 1", "⚠️ Проверка 2"],
    )
    assert "📝 Рекламный пост: #55" in text
    assert "⚠️ Проверка 1" in text


def test_format_readiness_block_ready():
    readiness = {
        "post_status_text": "✅ выбран",
        "show_seconds_status_text": "✅ 12 часов",
        "targets_status_text": "✅ 3 активных",
        "checks_status_text": "✅ ошибок нет",
        "summary_text": "✅ Кампания готова к тестовому запуску",
        "warnings": [],
    }
    block = format_repost_campaign_readiness_block(readiness)
    assert "🚦 Готовность кампании" in block
    assert "📝 Пост: ✅ выбран" in block
    assert "⏳ Срок: ✅ 12 часов" in block
    assert "📣 Каналы: ✅ 3 активных" in block
    assert "🔐 Проверка: ✅ ошибок нет" in block
    assert "✅ Кампания готова к тестовому запуску" in block


def test_format_readiness_block_warning():
    readiness = {
        "post_status_text": "❌ не выбран",
        "show_seconds_status_text": "❌ не задан",
        "targets_status_text": "❌ нет активных каналов",
        "checks_status_text": "⚠️ требуют проверки: 2",
        "summary_text": "⚠️ Кампания не готова: исправьте пункты выше",
        "warnings": ["a"],
    }
    block = format_repost_campaign_readiness_block(readiness)
    assert "📝 Пост: ❌ не выбран" in block
    assert "⏳ Срок: ❌ не задан" in block
    assert "📣 Каналы: ❌ нет активных каналов" in block
    assert "🔐 Проверка: ⚠️ требуют проверки: 2" in block
    assert "⚠️ Кампания не готова: исправьте пункты выше" in block


def test_menu_includes_readiness_block():
    text, _ = build_repost_campaign_menu_view(
        rule_id=3,
        summary={"show_seconds_text": "12 часов", "targets_active": 1, "saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        readiness={
            "ready": True,
            "post_status_text": "✅ выбран",
            "show_seconds_status_text": "✅ 12 часов",
            "targets_status_text": "✅ 1 активный",
            "checks_status_text": "✅ ошибок нет",
            "summary_text": "✅ Кампания готова к тестовому запуску",
        },
    )
    assert "🚦 Готовность кампании" in text
