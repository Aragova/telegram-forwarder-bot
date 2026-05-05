from app.repost_campaign_view_model import (
    build_campaign_run_item_view,
    build_campaign_run_message_view,
    format_campaign_delete_status_text,
    format_campaign_show_seconds_text,
    format_campaign_target_kind_text,
    format_campaign_run_type_text,
)


def test_show_seconds_formatting():
    assert format_campaign_show_seconds_text(60) == "1 минута"
    assert format_campaign_show_seconds_text(43200) == "12 часов"
    assert format_campaign_show_seconds_text(0) == "не задан"
    assert format_campaign_show_seconds_text(None) == "не задан"


def test_target_kind_formatting():
    assert format_campaign_target_kind_text("main") == "Основной канал"
    assert format_campaign_target_kind_text("extra") == "Дополнительный канал"
    assert format_campaign_target_kind_text("unknown") == "Канал кампании"


def test_build_campaign_run_message_view_uses_send_error_text():
    view = build_campaign_run_message_view({"send_status": "failed", "send_error_text": "Cannot send", "error_text": "ignored"}, index=1)
    assert view["send_error_text"] == "Ошибка отправки: Cannot send"


def test_delete_failed_text_contains_reason_and_attempts():
    text = format_campaign_delete_status_text({
        "delete_status": "failed",
        "delete_error_text": "not enough rights",
        "delete_attempt_count": 2,
    })
    assert "Удаление: ❌ ошибка удаления" in text
    assert "Причина удаления: not enough rights" in text
    assert "Попыток удаления: 2" in text


def test_run_item_uses_real_run_id_in_title():
    view = build_campaign_run_item_view({"id": 25, "run_type": "manual", "status": "sent"}, index=2)
    assert view["title"].startswith("#25 ·")


def test_can_delete_pending():
    view = build_campaign_run_message_view({"send_status": "sent", "sent_message_id": 777, "delete_status": "pending"})
    assert view["can_delete_now"] is True
    assert view["delete_action_text"] == "🧹 Удалить сейчас"


def test_can_retry_failed_delete():
    view = build_campaign_run_message_view({"send_status": "sent", "sent_message_id": 777, "delete_status": "failed"})
    assert view["can_delete_now"] is True
    assert view["delete_action_text"] == "🔁 Повторить удаление"


def test_cannot_delete_deleted():
    view = build_campaign_run_message_view({"send_status": "sent", "sent_message_id": 777, "delete_status": "deleted"})
    assert view["can_delete_now"] is False


def test_cannot_delete_none():
    view = build_campaign_run_message_view({"send_status": "sent", "sent_message_id": 777, "delete_status": "none"})
    assert view["can_delete_now"] is False


def test_run_type_test_uses_product_copy():
    assert format_campaign_run_type_text("test") == "📤 Проверочная публикация"


def test_run_type_manual_uses_campaign_copy():
    assert format_campaign_run_type_text("manual") == "🚀 Кампания"

from app.repost_campaign_view_model import build_campaign_control_center_view_model


def test_control_center_view_model_ready():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13, "targets_active": 2, "show_seconds": 60},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={"ok": True, "readiness": {"ready": True, "show_seconds": 60}, "issues": []},
    )
    assert vm["status_title"] == "✅ Кампания готова к запуску"
    assert "📝 Креатив:" in vm["post_line"]
    assert "📣 Площадки:" in vm["targets_line"]
    assert "🧹 Auto-delete:" in vm["delete_line"]
    assert vm["can_launch"] is True


def test_control_center_view_model_not_ready():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    assert vm["can_launch"] is False
    assert vm["status_title"] == "⚠️ Кампания требует настройки"


def test_control_center_view_model_last_run_block():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={
            "ok": True,
            "readiness": {"ready": True},
            "last_run": {"id": 4, "run_type": "manual", "status": "sent"},
            "last_run_details": {"ok": True, "summary": {"deleted": 2, "delete_failed": 0, "delete_pending": 0}},
            "issues": [],
        },
    )
    assert "📊 Последний запуск" in vm["last_run_block"]
    assert "#4" in vm["last_run_block"]


def test_control_center_view_model_issues_capped():
    vm = build_campaign_control_center_view_model(
        summary={},
        saved_post_line="📝 Рекламный пост: не выбран",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": [str(i) for i in range(7)]},
    )
    assert "• 0" in vm["issues_block"]
    assert "• 4" in vm["issues_block"]
    assert "...и ещё 2" in vm["issues_block"]
