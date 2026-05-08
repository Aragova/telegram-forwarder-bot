from datetime import datetime

from app.repost_campaign_view_model import (
    build_campaign_target_item_view,
    build_campaign_run_item_view,
    build_campaign_run_message_view,
    build_campaign_launch_readiness_view_model,
    format_campaign_delete_status_text,
    format_campaign_show_seconds_text,
    format_campaign_target_kind_text,
    format_campaign_run_type_text,
    normalize_campaign_target_error_text,
    build_campaign_views_report_view_model,
)


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


def test_campaign_target_item_view_hides_empty_thread_line():
    item = build_campaign_target_item_view({
        "id": 1,
        "target_id": "-1001",
        "title": "Канал",
        "is_active": True,
        "target_thread_id": None,
    })

    assert item["thread_line"] is None


def test_campaign_target_item_view_keeps_thread_line_when_thread_id_exists():
    item = build_campaign_target_item_view({
        "id": 1,
        "target_id": "-1001",
        "title": "Группа",
        "is_active": True,
        "target_thread_id": 12345,
    })

    assert item["thread_line"] == "Тема: 12345"


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


def test_posts_library_vm_limits_to_ten_items():
    items = [_library_item(i, is_current=(i == 12)) for i in range(1, 13)]
    vm = build_campaign_posts_library_view_model(library={"items": items, "summary": {"posts_total": 12}})
    assert len(vm["items"]) == 10
    assert vm.get("limit_note") is None


def test_posts_library_vm_summary_is_clean():
    vm = build_campaign_posts_library_view_model(library={"items": [_library_item(1)], "summary": {"posts_total": 1, "runs_total": 2, "placements_total": 43, "views_mode": "lazy"}})
    summary_text = "\n".join([vm["intro_line"], vm["placements_line"], vm["items"][0]["views_line"]])
    assert "Данные просмотров" not in summary_text
    assert "Коллекция рекламных постов этой кампании." in summary_text
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


def test_post_stats_vm_builds_channels_items():
    vm = build_campaign_post_stats_view_model(stats={
        "kind": "photo",
        "runs_count": 1,
        "top_channels": [{"target_title": "WikiBoy's 😎", "views_total": 1240}],
        "problem_channels": [{"target_title": "Czech Hunter official"}],
    })
    items = vm["channels_items"]
    assert len(items) == 2
    assert items[0]["views_status"] == "ok"
    assert items[0]["views_total"] == 1240
    assert items[1]["views_status"] == "problem"


def test_post_stats_vm_keeps_total_views_and_placements():
    vm = build_campaign_post_stats_view_model(stats={"kind": "photo", "views_total": 10830, "placements_sent": 43})
    assert vm["views_line"] == "👁 Всего просмотров: 10 830"
    assert vm["placements_line"] == "📣 Размещений: 43"


def test_post_stats_vm_prefers_full_channels_stats_over_top_channels():
    vm = build_campaign_post_stats_view_model(stats={
        "kind": "photo",
        "channels_stats": [{"target_title": "Полный 1", "views_total": 10, "views_status": "ok"}],
        "top_channels": [{"target_title": "Только топ", "views_total": 99}],
        "problem_channels": [{"target_title": "Только проблема"}],
    })
    assert len(vm["channels_items"]) == 1
    assert vm["channels_items"][0]["title"] == "Полный 1"


def test_post_stats_vm_channel_line_for_unavailable():
    vm = build_campaign_post_stats_view_model(stats={
        "kind": "photo",
        "channels_stats": [{"target_title": "Channel", "views_total": 0, "views_status": "unavailable"}],
    })
    assert "⚠️ нет данных — Channel" in vm["channels_lines"]


def test_post_stats_vm_channel_line_for_ok():
    vm = build_campaign_post_stats_view_model(stats={
        "kind": "photo",
        "runs_count": 3,
        "channels_stats": [{"target_title": "Channel", "views_total": 520, "views_status": "ok"}],
    })
    assert "👁 520 — Channel" in vm["channels_lines"][0]


def test_views_report_vm_uses_final_snapshot_deleted_note():
    vm = build_campaign_views_report_view_model(report={
        "status": "ready",
        "views_total": 555,
        "views_available": 1,
        "sent_total": 1,
        "items": [{
            "target_title": "A",
            "views": 555,
            "views_status": "ok",
            "views_source": "final_snapshot",
            "delete_status": "deleted",
        }],
    })
    assert "зафиксированы перед удалением" in vm["delete_note_line"]


def test_views_report_vm_partial_final_snapshot_note():
    vm = build_campaign_views_report_view_model(report={
        "status": "partial",
        "views_total": 555,
        "views_available": 1,
        "sent_total": 2,
        "items": [
            {"target_title": "A", "views": 555, "views_status": "ok", "views_source": "final_snapshot", "delete_status": "deleted"},
            {"target_title": "B", "views": 0, "views_status": "unavailable", "views_source": "final_snapshot", "delete_status": "deleted"},
        ],
    })
    assert "Финальные просмотры собраны частично" in vm["delete_note_line"]
