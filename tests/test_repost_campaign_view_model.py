from datetime import datetime

from app.repost_campaign_view_model import (
    build_campaign_target_item_view,
    build_campaign_run_item_view,
    build_campaign_run_message_view,
    build_campaign_scenario_preview_view_model,
    build_campaign_launch_readiness_view_model,
    format_campaign_delete_status_text,
    format_campaign_show_seconds_text,
    format_campaign_target_kind_text,
    format_campaign_run_type_text,
    normalize_campaign_target_error_text,
    build_campaign_views_report_view_model,
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
    assert vm["screen_state"] == "ready_to_launch"
    assert vm["creative_line"] == "📝 Рекламный пост"
    assert "📣 Каналы/Группы:" in vm["targets_line"]
    assert vm["show_seconds_line"] == "⏳ Время показа"
    assert vm["can_launch"] is True


def test_control_center_view_model_not_ready_no_show_seconds():
    vm = build_campaign_control_center_view_model(
        summary={"saved_post_id": 13},
        saved_post_line="📝 Рекламный пост: #13 · фото",
        control_center={"ok": True, "readiness": {"ready": False}, "issues": []},
    )
    assert vm["can_launch"] is False
    assert vm["screen_state"] == "not_configured"


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
    assert vm["last_run_title_line"] == "📊 Последний запуск"
    assert "#4" not in vm["last_run_line"]


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
    assert vm["screen_state"] == "delete_problem"


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
    assert pending["screen_state"] == "active_placement"
    assert "ожида" in pending["last_run_delete_line"].lower()
    assert failed["screen_state"] == "delete_problem"
    assert deleted["screen_state"] in {"completed","not_configured"}


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
    assert "Автоудаление: включено" not in enabled["auto_delete_line"]
    assert "не настроено" in not_set["auto_delete_line"]


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


def test_views_report_vm_ready():
    vm = build_campaign_views_report_view_model(report={"status": "ready", "views_total": 12430, "views_available": 2, "sent_total": 2, "items": [{"views_status": "ok", "views": 1240, "target_title": "Канал A"}]})
    assert vm["status_line"] == "✅ Просмотры собраны"
    assert "12 430" in vm["total_views_line"]
    assert "2 / 2" in vm["coverage_line"]
    assert any("Канал A" in x for x in vm["channel_lines"])


def test_views_report_vm_partial():
    vm = build_campaign_views_report_view_model(report={"status": "partial", "items": [], "problem_items": [{"target_title": "Канал B", "error_text": "err"}]})
    assert "частично" in vm["status_line"]
    assert vm["problem_lines"]


def test_views_report_vm_album_line():
    vm = build_campaign_views_report_view_model(report={"status": "ready", "items": [{"views_status": "ok", "views": 10, "target_title": "Канал", "is_album": True, "album_items": 5}]})
    assert "альбом 5 медиа" in vm["channel_lines"][0]


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


def test_launch_readiness_vm_ready():
    vm = build_campaign_launch_readiness_view_model(readiness={"can_launch": True, "saved_post_exists": True, "show_seconds": 3600, "main_target_ready": True, "will_send_total": 3, "will_skip_total": 0, "extra_paused": 0, "extra_problem": 0, "extra_ready": 2}, now=datetime(2026,1,1,10,0,0))
    assert vm["status_line"] == "✅ Кампания готова к запуску"
    assert "Будет опубликовано" in vm["will_send_line"]
    assert "10:" in vm["expected_delete_line"] or "11:" in vm["expected_delete_line"]
    assert vm["can_launch"] is True

def test_launch_readiness_vm_problem_targets():
    vm = build_campaign_launch_readiness_view_model(readiness={"can_launch": False, "saved_post_exists": True, "show_seconds": 300, "main_target_ready": True, "extra_active_problem": 1, "extra_problem": 1, "will_send_total": 1, "will_skip_total": 1})
    assert vm["status_line"] == "⚠️ Нужно проверить каналы/группы"
    assert vm["can_check_rights"] is True
    assert "Проверьте права" in vm["next_step_line"]

def test_launch_readiness_vm_missing_post():
    vm = build_campaign_launch_readiness_view_model(readiness={"can_launch": False, "saved_post_exists": False, "show_seconds": 300, "main_target_ready": True})
    assert vm["status_line"] == "⚠️ Нужно выбрать рекламный пост"

def test_launch_readiness_vm_no_banned_terms():
    vm = build_campaign_launch_readiness_view_model(readiness={"can_launch": False, "saved_post_exists": False, "show_seconds": 0, "main_target_ready": True})
    dump = "\n".join(str(v) for v in vm.values())
    for bad in ["креатив", "площадк", "аккаунт-парсер", "тестовый", "Режим: репост"]:
        assert bad.lower() not in dump.lower()

from app.repost_campaign_view_model import build_campaign_posts_library_view_model, build_campaign_post_stats_view_model


def _library_item(saved_post_id, **kw):
    base = {
        "saved_post_id": saved_post_id,
        "kind": None,
        "is_album": False,
        "media_count": 0,
        "views_total": 8218,
        "runs_count": 1,
        "placements_sent": 43,
        "placements_failed": 0,
        "views_available": 4,
        "views_unavailable": 142,
        "top_channels": [{"target_title": "WikiBoy’s 😎", "views_total": 1111}],
        "last_started_at": "2026-05-07T12:04:00+00:00",
    }
    base.update(kw)
    return base


def test_posts_library_vm_premium_titles_no_ids():
    vm = build_campaign_posts_library_view_model(library={"items": [_library_item(24, is_current=True, kind="unknown")], "summary": {}})
    text = "\n".join([vm["items"][0]["title_line"], vm["items"][0]["kind_line"]])
    assert "#24" not in text
    assert "unknown" not in text.lower()
    assert "Текущий рекламный пост" in text or "Пост от" in text


def test_posts_library_vm_current_post_first():
    vm = build_campaign_posts_library_view_model(library={"items": [_library_item(1), _library_item(2, is_current=True)], "summary": {}})
    assert vm["items"][0]["saved_post_id"] == 2


def test_posts_library_vm_limits_to_five_items():
    items = [_library_item(i, is_current=(i == 7)) for i in range(1, 8)]
    vm = build_campaign_posts_library_view_model(library={"items": items, "summary": {"posts_total": 7}})
    assert len(vm["items"]) == 5
    assert "Показаны последние 5 постов" in (vm.get("limit_note") or "")


def test_posts_library_vm_summary_is_clean():
    vm = build_campaign_posts_library_view_model(library={"items": [_library_item(1)], "summary": {"posts_total": 1, "runs_total": 2, "placements_total": 43, "views_mode": "lazy"}})
    summary_text = "\n".join([vm["intro_line"], vm["placements_line"], vm["items"][0]["views_line"]])
    assert "Данные просмотров" not in summary_text
    assert "Просмотры открываются внутри карточки поста" in summary_text
    assert "Просмотры: открыть карточку" in summary_text


def test_post_stats_vm_no_internal_ids():
    vm = build_campaign_post_stats_view_model(stats={"saved_post_id": 24, "kind": "unknown"})
    assert "#24" not in vm["title"]
    assert "unknown" not in (vm["kind_line"]).lower()


def test_post_stats_vm_current_actions():
    vm = build_campaign_post_stats_view_model(stats={"is_current": True, "kind": "photo"})
    assert vm["current_line"] == "✅ Сейчас выбран"


def test_post_stats_vm_reuse_actions():
    vm = build_campaign_post_stats_view_model(stats={"is_current": False, "kind": "photo"})
    assert vm["current_line"] is None
