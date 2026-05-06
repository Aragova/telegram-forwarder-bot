from datetime import datetime

from app.repost_campaign_view_model import (
    build_campaign_target_item_view,
    build_campaign_run_item_view,
    build_campaign_run_message_view,
    build_campaign_scenario_preview_view_model,
    format_campaign_delete_status_text,
    format_campaign_show_seconds_text,
    format_campaign_target_kind_text,
    format_campaign_run_type_text,
    normalize_campaign_target_error_text,
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


def test_build_campaign_target_item_view_active():
    view = build_campaign_target_item_view({"id": 1, "target_id": "-1001", "title": "Channel", "is_active": True}, index=1)
    assert "🟢" in view["title"]
    assert view["can_pause"] is True
    assert view["can_resume"] is False


def test_build_campaign_target_item_view_paused():
    view = build_campaign_target_item_view({"id": 1, "target_id": "-1001", "title": "Channel", "is_active": False}, index=1)
    assert "⏸" in view["title"]
    assert view["can_resume"] is True


def test_build_campaign_target_item_view_error():
    view = build_campaign_target_item_view({"id": 1, "target_id": "-1001", "title": "Channel", "is_active": True, "last_check_error": "not enough rights"}, index=1)
    assert "⚠️" in view["title"]
    assert view["error_line"]
    assert view["requires_attention"] is True
    assert view["can_pause"] is False
    assert view["can_resume"] is False
    assert view["can_check"] is True
    assert view["can_delete"] is True


def test_legacy_error_normalization():
    text = normalize_campaign_target_error_text("Аккаунт-парсер не имеет права публиковать в канал/группу")
    assert "ViMi пока не видит право публикации" in text

from app.repost_campaign_view_model import build_campaign_control_center_view_model


def test_control_center_view_model_ready():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13, "targets_active": 2, "show_seconds": 60},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={"ok": True, "readiness": {"ready": True, "show_seconds": 60}, "issues": []},
    )
    assert vm["title_status"] == "✅ Готова к запуску"
    assert "📝 Рекламный пост:" in vm["creative_line"]
    assert "📣 Каналы/Группы:" in vm["targets_line"]
    assert "⏳ Время показа:" in vm["show_seconds_line"]
    assert vm["can_launch"] is True


def test_control_center_view_model_not_ready_no_show_seconds():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    assert vm["can_launch"] is False
    assert vm["title_status"] == "⚠️ Нужно настроить время показа"


def test_control_center_view_model_last_run_line_compact():
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
    assert "📊 Последний запуск:" in vm["last_run_line"]
    assert "#4" in vm["last_run_line"]


def test_delete_failed_has_priority_over_ready():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13, "show_seconds": 60, "targets_active": 1},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={
            "ok": True,
            "readiness": {"ready": True},
            "last_run": {"id": 5, "status": "sent", "targets_success": 1, "targets_total": 1},
            "last_run_details": {"ok": True, "summary": {"delete_failed": 1}},
        },
    )
    assert vm["title_status"] == "⚠️ Есть проблемы удаления"


def test_delete_line_variants():
    base = {"ok": True, "readiness": {"ready": False}, "last_run": {"id": 5, "status": "sent", "targets_success": 1, "targets_total": 1}}
    pending = build_campaign_control_center_view_model(
        summary={},
        saved_post_line="📝 Рекламный пост: не выбран",
        control_center={**base, "last_run_details": {"ok": True, "summary": {"delete_pending": 2}}},
    )
    failed = build_campaign_control_center_view_model(
        summary={},
        saved_post_line="📝 Рекламный пост: не выбран",
        control_center={**base, "last_run_details": {"ok": True, "summary": {"delete_failed": 1}}},
    )
    deleted = build_campaign_control_center_view_model(
        summary={},
        saved_post_line="📝 Рекламный пост: не выбран",
        control_center={**base, "last_run_details": {"ok": True, "summary": {"deleted": 3}}},
    )
    assert pending["last_run_delete_line"] == "🧹 Удаление последнего запуска: ожидает 2"
    assert failed["last_run_delete_line"] == "🧹 Удаление последнего запуска: 1 ошибка"
    assert deleted["last_run_delete_line"] == "🧹 Удаление последнего запуска: всё удалено"


def test_auto_delete_lines():
    enabled = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13, "show_seconds": 60},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    not_set = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13, "show_seconds": 0},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    assert enabled["auto_delete_line"] == "🧹 Автоудаление: включено"
    assert not_set["auto_delete_line"] == "🧹 Автоудаление: не задано"


def test_control_center_copy_uses_ad_post_and_channels_groups():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": None, "targets_active": 0},
        saved_post_line="📝 Рекламный пост: не выбран",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    assert "креатив" not in vm["title_status"].lower()
    assert "площ" not in vm["next_step_line"].lower()


def test_targets_line_shows_ready_count():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 1, "targets_active": 2, "targets_ready": 2, "targets_with_errors": 0},
        saved_post_line="📝 Рекламный пост: #1",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    assert vm["targets_line"] == "📣 Каналы/Группы: 2 активных · 2 готовы"


def test_targets_line_shows_ready_and_errors_counts():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 1, "targets_active": 5, "targets_ready": 3, "targets_with_errors": 2},
        saved_post_line="📝 Рекламный пост: #1",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    assert vm["targets_line"] == "📣 Каналы/Группы: 5 активных · 3 готовы · 2 требуют проверки"


def test_scenario_preview_ready():
    vm = build_campaign_scenario_preview_view_model(
        rule_id=1,
        summary={"saved_post_id": 20, "show_seconds": 7200, "targets_active": 42, "targets_ready": 42, "targets_with_errors": 0},
        saved_post_id=20,
        saved_post_description="альбом · 5 медиа",
        readiness={"ready": True},
        now=datetime(2026, 5, 6, 20, 0),
    )
    assert vm["status_line"] == "✅ Готова к запуску"
    assert vm["can_launch"] is True
    assert "42 активных" in vm["targets_line"]
    assert "22:00" in vm["expected_delete_line"]
    assert any("автоматически удалит" in step for step in vm["scenario_steps"])
    assert any("история размещения" in step for step in vm["scenario_steps"])


def test_scenario_preview_with_target_errors():
    vm = build_campaign_scenario_preview_view_model(
        rule_id=1,
        summary={"saved_post_id": 20, "show_seconds": 7200, "targets_active": 5, "targets_ready": 3, "targets_with_errors": 2},
        saved_post_id=20,
        saved_post_description="пост",
        readiness={"ready": False, "checks_status_text": "⚠️ требуют проверки"},
    )
    assert vm["can_launch"] is False
    assert vm["can_check_rights"] is True
    assert "5 активных · 3 готовы · 2 требуют проверки" in vm["targets_line"]
    assert vm["next_step_line"] == "Следующий шаг: проверьте права каналов/групп."


def test_scenario_preview_missing_post():
    vm = build_campaign_scenario_preview_view_model(rule_id=1, summary={"show_seconds": 60, "targets_active": 1}, saved_post_id=None, saved_post_description=None, readiness={"ready": False})
    assert vm["can_launch"] is False
    assert vm["next_step_line"] == "Следующий шаг: выберите рекламный пост."


def test_scenario_preview_missing_show_seconds():
    vm = build_campaign_scenario_preview_view_model(rule_id=1, summary={"saved_post_id": 1, "show_seconds": 0, "targets_active": 1}, saved_post_id=1, saved_post_description="пост", readiness={"ready": False})
    assert vm["expected_delete_line"] == "🕒 Ожидаемое удаление: не рассчитано"
    assert vm["next_step_line"] == "Следующий шаг: задайте время показа."


def test_scenario_preview_no_banned_terms():
    vm = build_campaign_scenario_preview_view_model(rule_id=1, summary={}, saved_post_id=None, saved_post_description=None, readiness={"ready": False})
    joined = " ".join(str(v) for v in vm.values())
    assert "креатив" not in joined.lower()
    assert "площадк" not in joined.lower()
    assert "режим: репост" not in joined.lower()
    assert "тестовый" not in joined.lower()


def test_target_item_view_hides_raw_id_as_title():
    view = build_campaign_target_item_view({"id": 1, "target_id": "-1002741117827", "title": None, "is_active": True, "last_check_error": None}, index=1)
    assert "Канал/Группа #1" in view["title"]
    assert "-1002741117827" not in view["title"]
    assert view["target_line"] is None
    assert "-1002741117827" in view["technical_line"]


def test_target_item_view_uses_real_title():
    view = build_campaign_target_item_view({"id": 1, "target_id": "-1002741117827", "title": "Mickey Twink 🍭", "is_active": True}, index=1)
    assert "Mickey Twink 🍭" in view["title"]


def test_target_item_view_title_equal_to_id_is_treated_as_missing():
    view = build_campaign_target_item_view({"id": 1, "target_id": "-1002741117827", "title": "-1002741117827", "is_active": True}, index=1)
    assert "Канал/Группа #1" in view["title"]
    assert "-1002741117827" not in view["title"]
