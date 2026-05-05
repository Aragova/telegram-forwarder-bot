from app.repost_campaign_ui import (
    build_repost_campaign_delete_result_view,
    build_repost_campaign_history_view,
    build_repost_campaign_launch_result_view,
    build_repost_campaign_menu_view,
    build_repost_campaign_post_menu_view,
    build_repost_campaign_preview_view,
    build_repost_campaign_run_details_view,
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


def test_menu_shows_launch_button_when_ready():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=7,
        summary={"saved_post_id": 10},
        saved_post_line="📝 Рекламный пост: #10",
        readiness={"ready": True},
    )
    texts = _texts_from_keyboard(keyboard)
    assert "🚀 Запустить кампанию" in texts


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
    assert "✅ Успешно: 3" in text
    assert "📣 Всего каналов: 3" in text
    assert "📄 Детали запуска" in _texts_from_keyboard(keyboard)
    assert "⏳ Срок показа: 12 часов" in text


def test_launch_result_uses_show_seconds_from_extra():
    result = {"ok": True, "saved_post_id": 7, "extra": {"campaign_run_id": 10, "targets_total": 3, "targets_success": 3, "targets_failed": 0, "show_seconds": 60}}
    text, _ = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "⏳ Срок показа: 1 минута" in text
    assert "12 часов" not in text


def test_launch_result_partial():
    result = {"ok": True, "saved_post_id": 7, "extra": {"campaign_run_id": 10, "targets_total": 3, "targets_success": 2, "targets_failed": 1, "final_status": "partial"}}
    text, _ = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "🟡 Кампания запущена частично" in text


def test_launch_result_failed():
    result = {"ok": False, "error_text": "oops", "premium_required": False, "extra": {}}
    text, _ = build_repost_campaign_launch_result_view(rule_id=3, result=result)
    assert "❌ Не удалось запустить кампанию" in text


def test_history_empty_state():
    text, keyboard = build_repost_campaign_history_view(rule_id=3, history={"ok": True, "runs": [], "summary": {}})
    texts = _texts_from_keyboard(keyboard)
    assert "Пока запусков нет" in text
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
    assert "📊 История кампаний" in text
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
    assert "🚦 Готовность кампании" in text


def test_details_shows_retry_button_for_failed_delete():
    details = {"ok": True, "run_id": 10, "run": {"id": 10}, "messages": [{"id": 33, "send_status": "sent", "sent_message_id": 777, "delete_status": "failed"}], "summary": {}}
    _, keyboard = build_repost_campaign_run_details_view(rule_id=3, details=details)
    texts = _texts_from_keyboard(keyboard)
    callbacks = [b.callback_data for row in keyboard.inline_keyboard for b in row]
    assert "🔁 Повторить удаление #1" in texts
    assert "rule_repost_campaign_delete_message:3:10:33" in callbacks


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
    assert "Рекламная кампания публикует выбранный пост" in text


def test_check_publication_button_and_callback():
    _, keyboard = build_repost_campaign_menu_view(
        rule_id=9,
        summary={"saved_post_id": 10},
        saved_post_line="📝 Рекламный пост: #10",
        readiness={"ready": False},
    )
    texts = _texts_from_keyboard(keyboard)
    callbacks = [b.callback_data for row in keyboard.inline_keyboard for b in row]
    assert "📤 Проверить публикацию" in texts
    assert "rule_repost_campaign_test_send:9" in callbacks


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
