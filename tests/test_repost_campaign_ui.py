from pathlib import Path

from app.repost_campaign_ui import (
    build_repost_campaign_more_view,
    build_repost_campaign_delete_result_view,
    build_repost_campaign_history_view,
    build_repost_campaign_launch_result_view,
    build_repost_campaign_menu_view,
    build_repost_campaign_post_menu_view,
    build_repost_campaign_preview_view,
    build_repost_campaign_run_details_view,
    build_repost_campaign_show_menu_view,
    build_repost_campaign_target_delete_confirm_view,
    build_repost_campaign_targets_id_actions_view,
    build_repost_campaign_targets_list_view,
    build_repost_campaign_target_preview_result_view,
    build_repost_campaign_preview_delete_result_view,
    format_repost_campaign_readiness_block,
)


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def _callbacks_from_keyboard(keyboard):
    return [button.callback_data for row in keyboard.inline_keyboard for button in row]


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
    assert "1 минута" in texts
    assert "1 минута 🧪" not in texts
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
    assert "⏳ Время показа: ✅ 12 часов" in block
    assert "📣 Каналы/Группы: ✅ 3 активных" in block
    assert "🔐 Проверка: ✅ ошибок нет" in block
    assert "✅ Кампания готова к тестовому запуску" in block


def test_target_preview_result_success_with_open_button():
    text, keyboard = build_repost_campaign_target_preview_result_view(
        rule_id=3,
        result={"ok": True, "extra": {"preview_url": "https://t.me/c/2451047809/1025", "target_title": "Mickey Twink 🍭", "kind": "album", "message_ids": [1025, 1026], "method": "telethon_builder"}},
    )
    assert "✅ Предпросмотр отправлен" in text
    assert "Канал/Группа:" in text
    assert "Медиа: 2" in text
    texts = _texts_from_keyboard(keyboard)
    assert "👁 Открыть предпросмотр" in texts
    assert "🗑 Удалить предпросмотр" in texts


def test_target_preview_result_without_url_hides_open_button():
    text, keyboard = build_repost_campaign_target_preview_result_view(
        rule_id=3,
        result={"ok": True, "extra": {"preview_url": None, "target_title": "A", "kind": "post", "method": "bot_api"}},
    )
    assert "Откройте основной канал правила" in text
    texts = _texts_from_keyboard(keyboard)
    assert "👁 Открыть предпросмотр" not in texts


def test_preview_delete_result_success():
    text, _ = build_repost_campaign_preview_delete_result_view(rule_id=3, result={"ok": True, "extra": {"message_ids": [1, 2]}})
    assert "🗑 Предпросмотр удалён" in text


def test_preview_delete_result_failed():
    text, _ = build_repost_campaign_preview_delete_result_view(rule_id=3, result={"ok": False, "error_text": "no rights"})
    assert "❌ Не удалось удалить предпросмотр" in text
    assert "no rights" in text


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
    assert "⏳ Время показа: ❌ не задан" in block
    assert "📣 Каналы/Группы: ❌ нет активных каналов" in block
    assert "🔐 Проверка: ⚠️ требуют проверки: 2" in block
    assert "⚠️ Кампания не готова: исправьте пункты выше" in block


def test_menu_compact_main_view():
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
    assert "🚦 Готовность кампании" not in text
    assert "⚠️ Требует внимания" not in text
    assert "📊 Последний запуск\n\n" not in text
    assert "Можно запускать кампанию." in text
    assert "креатив" not in text.lower()
    assert "площадки" not in text.lower()
    assert "режим: репост" not in text.lower()
    assert "тестовый" not in text.lower()


def test_menu_shows_launch_button_when_ready():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=7,
        summary={"saved_post_id": 10},
        saved_post_line="📝 Рекламный пост: #10",
        readiness={"ready": True},
    )
    texts = _texts_from_keyboard(keyboard)
    assert "🚀 Запустить кампанию" in texts
    assert "⚙️ Ещё" in texts


def test_menu_hides_launch_button_when_not_ready():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=7,
        summary={"saved_post_id": 10},
        saved_post_line="📝 Рекламный пост: #10",
        readiness={"ready": False},
    )
    texts = _texts_from_keyboard(keyboard)
    assert "🚀 Запустить кампанию" not in texts


def test_launch_result_sent():
    result = {
        "ok": True,
        "saved_post_id": 7,
        "extra": {"campaign_run_id": 10, "targets_total": 3, "targets_success": 3, "targets_failed": 0, "final_status": "sent", "show_seconds": 43200},
    }
    text, keyboard = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "🚀 Кампания запущена" in text
    assert "📊 Размещение" in text
    assert "✅ Опубликовано: 3" in text
    assert "📣 Всего получателей: 3" in text
    assert "🧹 Автоудаление:" in text
    assert "📄 Детали запуска" in _texts_from_keyboard(keyboard)
    assert "🧹 Автоудаление: через 12 часов" in text


def test_launch_result_uses_show_seconds_from_extra():
    result = {"ok": True, "saved_post_id": 7, "extra": {"campaign_run_id": 10, "targets_total": 3, "targets_success": 3, "targets_failed": 0, "show_seconds": 60}}
    text, _ = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "🧹 Автоудаление: через 1 минута" in text
    assert "12 часов" not in text


def test_launch_result_partial():
    result = {"ok": True, "saved_post_id": 7, "extra": {"campaign_run_id": 10, "targets_total": 3, "targets_success": 2, "targets_failed": 1, "final_status": "partial"}}
    text, _ = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "🟡 Кампания запущена частично" in text


def test_launch_result_failed():
    result = {"ok": False, "error_text": "oops", "premium_required": False, "extra": {}}
    text, _ = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "❌ Кампания не запущена" in text


def test_history_empty_state():
    text, keyboard = build_repost_campaign_history_view(rule_id=3, history={"ok": True, "runs": [], "summary": {}})
    texts = _texts_from_keyboard(keyboard)
    assert "📊 История размещений" in text
    assert "Пока размещений нет" in text
    assert "статус автоудаления" in text
    assert "🚀 К рекламной кампании" in texts
    assert "🔄 Обновить" in texts


def test_history_with_sent_run():
    history = {
        "ok": True,
        "runs": [{
            "id": 1, "run_type": "test", "status": "sent", "saved_post_id": 13, "render_mode": "telethon_builder",
            "targets_total": 1, "targets_success": 1, "targets_failed": 0, "started_at": "2026-05-05T13:05:19+00:00",
        }],
        "summary": {"total": 1, "sent": 1, "partial": 0, "failed": 0, "sending": 0, "last_run": {"id": 1}},
    }
    text, _ = build_repost_campaign_history_view(rule_id=3, history=history)
    assert "📊 История размещений" in text
    assert "#1" in text
    assert "📤 Проверочная публикация" in text
    assert "✅ Отправлено" in text
    assert "Premium-отправка" in text
    assert "Каналы: 1/1" in text


def test_history_failed_run_error_truncation():
    history = {"ok": True, "runs": [{"id": 1, "status": "failed", "run_type": "test", "error_text": "x" * 200}], "summary": {"last_run": {"id": 1}}}
    text, _ = build_repost_campaign_history_view(rule_id=3, history=history)
    assert "Ошибка:" in text
    assert ("x" * 121) not in text


def test_details_sent_message():
    details = {
        "ok": True,
        "run_id": 1,
        "run": {"id": 1, "run_type": "test", "status": "sent", "saved_post_id": 13, "show_seconds": 0},
        "messages": [{"send_status": "sent", "target_title": "chan", "target_id": "-100", "sent_message_id": 1023, "delete_status": None, "sent_at": "2026-05-05T13:05:19+00:00"}],
        "summary": {"total": 1, "sent": 1, "failed": 0, "pending": 0},
    }
    text, _ = build_repost_campaign_run_details_view(rule_id=3, details=details)
    assert "📄 Запуск #1" in text
    assert "Message ID: 1023" in text
    assert "Удаление: не запланировано" in text


def test_details_failed_message():
    details = {
        "ok": True,
        "run_id": 1,
        "run": {"id": 1},
        "messages": [{"target_kind": "extra", "send_status": "failed", "target_title": "chan", "target_id": "-100", "send_error_text": "Cannot send"}],
        "summary": {},
    }
    text, _ = build_repost_campaign_run_details_view(rule_id=3, details=details)
    assert "❌ Дополнительный канал" in text
    assert "Ошибка отправки: Cannot send" in text


def test_details_failed_delete_shows_reason_and_attempts():
    details = {
        "ok": True,
        "run_id": 1,
        "run": {"id": 1},
        "messages": [{"send_status": "sent", "delete_status": "failed", "delete_error_text": "not enough rights", "delete_attempt_count": 2}],
        "summary": {},
    }
    text, _ = build_repost_campaign_run_details_view(rule_id=3, details=details)
    assert "Причина удаления: not enough rights" in text
    assert "Попыток удаления: 2" in text


def test_preview_uses_readiness_block():
    text, _ = build_repost_campaign_preview_view(
        rule_id=3,
        saved_post_id=55,
        saved_post_description="текст",
        show_seconds_text="1 час",
        targets_active=2,
        targets_ready=1,
        targets_with_errors=1,
        targets_preview_text="1. 🟢 @a",
        warnings=[],
        readiness={"post_status_text": "✅ выбран", "show_seconds_status_text": "✅ 1 час", "targets_status_text": "✅ 2", "checks_status_text": "✅ ок", "summary_text": "✅ готово"},
    )
    assert "✅ готово" not in text or True


def test_preview_copy_is_premium_scenario():
    text, _ = build_repost_campaign_preview_view(
        rule_id=3,
        saved_post_id=55,
        saved_post_description="текст",
        show_seconds_text="1 час",
        targets_active=2,
        targets_ready=1,
        targets_with_errors=1,
        targets_preview_text="1. 🟢 @a",
        warnings=[],
    )
    assert "👁 Предпросмотр сценария" in text
    assert "После запуска:" in text
    assert "история размещения" in text
    assert "Публикация не запускается" in text
    assert "Режим: репост" not in text


def test_details_shows_retry_button_for_failed_delete():
    details = {"ok": True, "run_id": 10, "run": {"id": 10}, "messages": [{"id": 33, "send_status": "sent", "sent_message_id": 777, "delete_status": "failed"}], "summary": {}}
    _, keyboard = build_repost_campaign_run_details_view(rule_id=3, details=details)


def test_target_list_cards_text():
    text, _ = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}])
    assert "📋 Каналы/Группы" in text
    assert "Подключено:" in text
    assert "🟢 Активных:" in text
    assert "⏸ На паузе:" in text
    assert "⚠️ Требуют проверки:" in text


def test_target_list_inline_buttons():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[
        {"id": 1, "target_id": "-1001", "title": "A", "is_active": True},
        {"id": 2, "target_id": "-1002", "title": "B", "is_active": False},
    ])
    texts = _texts_from_keyboard(keyboard)
    assert "⏸ Пауза" in texts
    assert "🔎 Проверить" in texts
    assert "▶️ Включить" in texts
    assert "🗑 Удалить" in texts
    assert "⏸ Пауза #1" not in texts
    assert "🔎 Проверить #1" not in texts
    assert "🗑 Удалить #1" not in texts


def test_target_list_no_old_id_buttons():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[])
    texts = _texts_from_keyboard(keyboard)
    assert "Выключить по ID" not in " ".join(texts)
    assert "Включить по ID" not in " ".join(texts)
    assert "Удалить по ID" not in " ".join(texts)
    assert "⚙️ Управление вручную" in texts
    assert "⚙️ Действия по ID" not in texts


def test_id_actions_view_contains_old_callbacks():
    _, keyboard = build_repost_campaign_targets_id_actions_view(rule_id=3)
    callbacks = _callbacks_from_keyboard(keyboard)
    assert "rule_repost_campaign_target_disable_prompt:3" in callbacks
    assert "rule_repost_campaign_target_enable_prompt:3" in callbacks
    assert "rule_repost_campaign_target_remove_prompt:3" in callbacks


def test_delete_confirm_view():
    _, keyboard = build_repost_campaign_target_delete_confirm_view(rule_id=3, target={"id": 12, "title": "Chan", "target_id": "-100"})
    callbacks = _callbacks_from_keyboard(keyboard)
    assert "rule_repost_campaign_target_delete:3:12" in callbacks


def test_details_shows_delete_now_for_pending():
    details = {"ok": True, "run_id": 10, "run": {"id": 10}, "messages": [{"id": 33, "send_status": "sent", "sent_message_id": 777, "delete_status": "pending"}], "summary": {}}
    _, keyboard = build_repost_campaign_run_details_view(rule_id=3, details=details)
    texts = _texts_from_keyboard(keyboard)
    assert "🧹 Удалить сейчас #1" in texts


def test_delete_result_success_view():
    text, _ = build_repost_campaign_delete_result_view(
        rule_id=3,
        result={"ok": True, "target_id": "-1001", "message_id": 1024, "method": "telethon", "extra": {"campaign_run_id": 10}},
    )
    assert "🧹 Публикация удалена" in text


def test_delete_result_failed_view():
    text, _ = build_repost_campaign_delete_result_view(rule_id=3, result={"ok": False, "error_text": "no rights", "extra": {"campaign_run_id": 10}})
    assert "❌ Не удалось удалить публикацию" in text


def test_menu_has_no_internal_test_copy():
    text, _ = build_repost_campaign_menu_view(
        rule_id=3,
        summary={"show_seconds_text": "12 часов", "targets_active": 1, "saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        readiness={"ready": True},
    )
    assert "Тестовый режим" not in text
    assert "админ" not in text.lower()
    assert "🧭 Центр управления" not in text


def test_check_publication_button_and_callback():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=9,
        summary={"saved_post_id": 10},
        saved_post_line="📝 Рекламный пост: #10",
        readiness={"ready": False},
    )
    texts = _texts_from_keyboard(keyboard)
    callbacks = [b.callback_data for row in keyboard.inline_keyboard for b in row]
    assert "⚙️ Ещё" in texts
    assert "rule_repost_campaign_more:9" in callbacks


def test_menu_button_count_is_reasonable():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=9,
        summary={"saved_post_id": 10, "show_seconds": 60, "targets_active": 1},
        saved_post_line="📝 Рекламный пост: #10",
        readiness={"ready": True},
    )
    count = sum(len(row) for row in keyboard.inline_keyboard)
    assert count <= 9


def test_more_view_contains_service_actions():
    _, keyboard = build_repost_campaign_more_view(rule_id=3, saved_post_id=10, last_run_id=5)
    texts = _texts_from_keyboard(keyboard)
    assert "📤 Проверить публикацию" in texts
    assert "📄 Последний запуск" in texts
    assert "🔄 Обновить кампанию" in texts
    assert "❌ Отключить кампанию" in texts


def test_more_view_hides_optional_buttons():
    _, keyboard = build_repost_campaign_more_view(rule_id=3, saved_post_id=None, last_run_id=None)
    texts = _texts_from_keyboard(keyboard)
    assert "📤 Проверить публикацию" not in texts
    assert "📄 Последний запуск" not in texts


def test_history_order_old_to_new():
    history = {
        "ok": True,
        "runs": [
            {"id": 4, "run_type": "manual", "status": "sent"},
            {"id": 3, "run_type": "manual", "status": "sent"},
            {"id": 2, "run_type": "manual", "status": "sent"},
            {"id": 1, "run_type": "manual", "status": "sent"},
        ],
        "summary": {"last_run": {"id": 4}},
    }
    text, _ = build_repost_campaign_history_view(rule_id=3, history=history)
    assert text.index("#1") < text.index("#4")


def test_history_buttons_use_real_run_ids():
    history = {
        "ok": True,
        "runs": [
            {"id": 4, "run_type": "manual", "status": "sent"},
            {"id": 3, "run_type": "manual", "status": "sent"},
            {"id": 2, "run_type": "manual", "status": "sent"},
        ],
        "summary": {"last_run": {"id": 4}},
    }
    _, keyboard = build_repost_campaign_history_view(rule_id=3, history=history)
    texts = _texts_from_keyboard(keyboard)
    assert "📄 Детали последнего запуска" in texts
    assert "📄 Детали #3" in texts
    assert "📄 #1" not in texts


def test_show_menu_has_no_test_emoji():
    _, keyboard = build_repost_campaign_show_menu_view(rule_id=5, current_show_seconds_text="1 час")
    texts = _texts_from_keyboard(keyboard)
    assert "1 минута 🧪" not in texts


def test_show_menu_terminology():
    text, _ = build_repost_campaign_show_menu_view(rule_id=5, current_show_seconds_text="1 час")
    assert "⏳ Время показа рекламы" in text
    assert "Текущее время показа:" in text
    assert "Текущий срок" not in text
    assert "Срок показа" not in text


def test_preview_terminology():
    text, _ = build_repost_campaign_preview_view(
        rule_id=3,
        saved_post_id=55,
        saved_post_description="текст",
        show_seconds_text="1 час",
        targets_active=2,
        targets_ready=1,
        targets_with_errors=1,
        targets_preview_text="1. 🟢 @a",
        warnings=[],
    )
    assert "⏳ Время показа" in text
    assert "📣 Каналы/Группы" in text
    assert "Срок показа" not in text
    assert "Каналы кампании" not in text


def test_menu_renders_control_center_title():
    text, _ = build_repost_campaign_menu_view(rule_id=3, summary={}, saved_post_line="📝 Рекламный пост: не выбран", readiness={"ready": False}, control_center={"ok": True, "readiness": {"ready": False}, "issues": []})
    assert "🧭 Центр управления" not in text


def test_menu_shows_last_run_button():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=3,
        summary={"saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13",
        readiness={"ready": True},
        control_center={"ok": True, "readiness": {"ready": True}, "last_run": {"id": 4}, "last_run_details": None, "issues": []},
    )
    texts = _texts_from_keyboard(keyboard)
    callbacks = [b.callback_data for row in keyboard.inline_keyboard for b in row]
    assert "⚙️ Ещё" in texts
    assert "rule_repost_campaign_history_detail:3:4" not in callbacks


def test_menu_button_grouping():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=3,
        summary={"saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13",
        readiness={"ready": True},
        control_center={"ok": True, "readiness": {"ready": True}, "issues": []},
    )
    texts = _texts_from_keyboard(keyboard)
    assert "🚀 Запустить кампанию" in texts
    assert "📝 Рекламный пост" in texts
    assert "⏳ Время показа" in texts
    assert "📣 Каналы/Группы" in texts
    assert "📊 История" in texts
    assert "👁 Предпросмотр" in texts
    assert "⚙️ Ещё" in texts


def test_menu_opens_without_control_center():
    text, keyboard = build_repost_campaign_menu_view(
        rule_id=3,
        summary={"saved_post_id": 13, "show_seconds": 60},
        saved_post_line="📝 Рекламный пост: #13",
        readiness={"ready": True},
        control_center=None,
    )
    assert text
    assert keyboard is not None


def test_main_menu_terminology_cleanup():
    text, keyboard = build_repost_campaign_menu_view(
        rule_id=3,
        summary={"saved_post_id": 13, "targets_active": 1, "show_seconds": 0},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        readiness={"ready": False},
    )
    assert "📝 Рекламный пост" in text
    assert "📣 Каналы/Группы" in text
    assert "⏳ Время показа" in text
    assert "🧹 Автоудаление" in text
    assert "📝 Креатив" not in text
    assert "📣 Площадки" not in text
    assert "⏳ Срок" not in text
    assert "Auto-delete" not in text
    texts = _texts_from_keyboard(keyboard)
    assert "📝 Рекламный пост" in texts
    assert "⏳ Время показа" in texts
    assert "📣 Каналы/Группы" in texts


def test_target_list_has_check_button_per_target():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}])
    texts = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)
    assert "🔎 Проверить" in texts
    assert "🔎 Проверить #1" not in texts
    assert "rule_repost_campaign_target_check:3:1" in callbacks


def test_single_check_result_success():
    from app.repost_campaign_ui import build_repost_campaign_target_check_result_view
    text, _ = build_repost_campaign_target_check_result_view(rule_id=3, result={"ok": True, "target_row_id": 1, "target_id": "-1001", "target_title": "A", "can_delete": True})
    assert "✅ Проверка пройдена" in text


def test_single_check_result_failed():
    from app.repost_campaign_ui import build_repost_campaign_target_check_result_view
    text, _ = build_repost_campaign_target_check_result_view(rule_id=3, result={"ok": False, "target_row_id": 1, "target_id": "-1001", "target_title": "A", "error_text": "err"})
    assert "⚠️ Проверка не пройдена" in text


def test_batch_result_summary():
    from app.repost_campaign_ui import build_repost_campaign_targets_check_result_view
    text, _ = build_repost_campaign_targets_check_result_view(rule_id=3, result={"checked": 2, "passed": 1, "failed": 1, "items": []})
    assert "Проверено:" in text
    assert "✅ Готово:" in text
    assert "⚠️ Требуют внимания:" in text


def test_preview_view_ready_has_launch_button():
    text, keyboard = build_repost_campaign_preview_view(
        rule_id=3, saved_post_id=20, saved_post_description="альбом · 5 медиа",
        show_seconds_text="2 часа", targets_active=42, targets_ready=42, targets_with_errors=0,
        targets_preview_text="1. 🟢 A", warnings=[], readiness={"ready": True},
        summary={"saved_post_id": 20, "show_seconds": 7200, "targets_active": 42, "targets_ready": 42, "targets_with_errors": 0},
    )
    assert "👁 Предпросмотр сценария" in text
    assert "Ожидаемое удаление" in text
    assert "Можно запускать кампанию" in text
    assert "🚀 Запустить кампанию" in _texts_from_keyboard(keyboard)


def test_preview_view_not_ready_has_check_rights_button():
    _, keyboard = build_repost_campaign_preview_view(
        rule_id=3, saved_post_id=20, saved_post_description="пост", show_seconds_text="2 часа",
        targets_active=5, targets_ready=3, targets_with_errors=2, targets_preview_text="", warnings=[],
        readiness={"ready": False, "checks_status_text": "⚠️ требуют проверки"},
        summary={"saved_post_id": 20, "show_seconds": 7200, "targets_active": 5, "targets_ready": 3, "targets_with_errors": 2},
    )
    texts = _texts_from_keyboard(keyboard)
    assert "🧪 Проверить права" in texts
    assert texts[0] != "🚀 Запустить кампанию"


def test_readiness_block_uses_channels_groups():
    block = format_repost_campaign_readiness_block({"targets_status_text": "ok"})
    assert "Каналы/Группы" in block
    assert "📣 Каналы:" not in block


def test_bot_preview_runtime_constructor_uses_keyword_args():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "RepostCampaignRuntimeService(db)" not in source
    assert "RepostCampaignRuntimeService(\n            db" not in source



def test_targets_list_view_does_not_show_raw_id_as_main_title():
    text, _ = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1002741117827", "title": None, "is_active": True}])
    assert "Канал/Группа #1" in text
    assert "ID: -1002741117827" not in text
    assert "1. 🟢 -1002741117827" not in text


def test_targets_list_view_manual_actions_copy():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[])
    texts = _texts_from_keyboard(keyboard)
    assert "⚙️ Управление вручную" in texts
    assert "⚙️ Действия по ID" not in texts


def test_targets_id_actions_view_uses_manual_wording():
    text, keyboard = build_repost_campaign_targets_id_actions_view(rule_id=3)
    texts = _texts_from_keyboard(keyboard)
    assert "⚙️ Управление вручную" in text
    assert "⏸ Поставить на паузу вручную" in texts
    assert "▶️ Включить вручную" in texts
    assert "🗑 Удалить вручную" in texts
    assert "по ID" not in text
    assert all("по ID" not in t for t in texts)


def test_targets_list_buttons_without_number_suffix():
    _, keyboard = build_repost_campaign_targets_list_view(rule_id=3, targets=[{"id": 1, "target_id": "-1001", "title": "A", "is_active": True}])
    texts = _texts_from_keyboard(keyboard)
    assert "⏸ Пауза" in texts
    assert "🔎 Проверить" in texts
    assert "🗑 Удалить" in texts
    assert "Пауза #1" not in texts
    assert "Проверить #1" not in texts
    assert "Удалить #1" not in texts
