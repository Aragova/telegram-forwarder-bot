from __future__ import annotations

from pathlib import Path

from app.repost_campaign_ui import (
    build_repost_campaign_active_placements_view,
    build_repost_campaign_clean_channel_settings_view,
    build_repost_campaign_vip_features_view,
)


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def _callbacks_from_keyboard(keyboard):
    return [button.callback_data for row in keyboard.inline_keyboard for button in row]


def _state(**extra):
    state = {
        "ok": True,
        "state": "active",
        "status_text": "🟢 Есть активные размещения",
        "active_total": 3,
        "delete_problem_total": 1,
        "placements_total": 3,
        "placements": [],
    }
    state.update(extra)
    return state


def _placement(run_id: int):
    return {
        "run_id": run_id,
        "summary_text": f"#{run_id} · Запуск сейчас",
        "details_callback_data": f"rule_repost_campaign_history_detail:10:{run_id}",
        "delete_callback_data": f"rule_repost_campaign_run_delete_confirm:10:{run_id}",
        "report_callback_data": f"rule_repost_campaign_views_report:10:{run_id}",
    }


def test_enabled_clean_channel_settings_screen_has_disable_toggle_and_active_placements_link():
    text, keyboard = build_repost_campaign_clean_channel_settings_view(
        rule_id=10,
        settings={"ok": True, "rule_id": 10, "enabled": True},
        state=_state(),
    )

    texts = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Статус: 🟢 Включён" in text
    assert "⚪ Выключить Чистый канал" in texts
    assert "rule_repost_campaign_clean_channel_toggle:10:off" in callbacks
    assert "🧹 Активные размещения" in texts
    assert "rule_repost_campaign_active_placements:10:0" in callbacks


def test_disabled_clean_channel_settings_screen_has_enable_toggle():
    text, keyboard = build_repost_campaign_clean_channel_settings_view(
        rule_id=10,
        settings={"ok": True, "rule_id": 10, "enabled": False},
        state=_state(state="clean", status_text="✅ Канал чист", active_total=0, delete_problem_total=0),
    )

    texts = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Статус: ⚪ Выключен" in text
    assert "🟢 Включить Чистый канал" in texts
    assert "rule_repost_campaign_clean_channel_toggle:10:on" in callbacks


def test_clean_channel_settings_screen_shows_active_state_counters():
    text, _ = build_repost_campaign_clean_channel_settings_view(
        rule_id=10,
        settings={"enabled": True},
        state=_state(active_total=7, delete_problem_total=2),
    )

    assert "Активных размещений: 7" in text
    assert "Проблем удаления: 2" in text


def test_clean_channel_settings_state_error_is_safe_for_ui():
    text, _ = build_repost_campaign_clean_channel_settings_view(
        rule_id=10,
        settings={"enabled": True},
        state={"ok": False, "error_text": "Traceback: DB error"},
    )

    assert "Не удалось проверить активные размещения" in text
    assert "Traceback" not in text
    assert "DB error" not in text


def test_vip_menu_clean_channel_opens_settings_screen_and_does_not_promise_launch_blocking():
    text, keyboard = build_repost_campaign_vip_features_view(rule_id=10)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "rule_repost_campaign_clean_channel:10" in callbacks
    assert "rule_repost_campaign_active_placements:10:0" not in callbacks
    forbidden = ["автоматически блокирует", "гарантированно", "не даст запустить", "перед новой рекламой"]
    assert all(value not in text for value in forbidden)


def test_active_placements_screen_keeps_existing_callbacks_and_links_back_to_settings():
    text, keyboard = build_repost_campaign_active_placements_view(
        rule_id=10,
        state={
            "ok": True,
            "state": "active",
            "placements": [_placement(123)],
            "placements_total": 1,
            "active_total": 1,
            "delete_problem_total": 0,
        },
    )
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "#123" in text
    assert "rule_repost_campaign_active_placements:10:0" in callbacks
    assert "rule_repost_campaign_clean_channel:10" in callbacks
    assert "rule_repost_campaign_history_detail:10:123" in callbacks
    assert "rule_repost_campaign_run_delete_confirm:10:123" in callbacks
    assert "rule_repost_campaign_views_report:10:123" in callbacks


def _runtime_method_source(source: str, method_name: str) -> str:
    marker = f"\n    async def {method_name}("
    start = source.find(marker)
    if start < 0:
        marker = f"\n    def {method_name}("
        start = source.find(marker)
    assert start >= 0
    next_def = source.find("\n    def ", start + 1)
    next_async_def = source.find("\n    async def ", start + 1)
    candidates = [pos for pos in (next_def, next_async_def) if pos >= 0]
    end = min(candidates) if candidates else len(source)
    return source[start:end]


def test_stage_four_source_guards_do_not_connect_setting_to_launch_runtime_or_schedule():
    scheduled_post_source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")
    runtime_source = Path("app/repost_campaign_runtime_service.py").read_text(encoding="utf-8")
    schedule_source = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")

    for forbidden in [
        "clean_channel_enabled",
        "repost_campaign_clean_channel_enabled",
        "RepostCampaignPlacementService",
        "build_launch_policy_preview",
        "waiting_clean_channel",
    ]:
        assert forbidden not in scheduled_post_source

    launch_source = _runtime_method_source(runtime_source, "launch_campaign_now")
    for forbidden in [
        "repost_campaign_clean_channel_enabled",
        "build_launch_policy_preview",
        "clean_channel_enabled",
    ]:
        assert forbidden not in launch_source

    schedule_campaign_launch_source = _runtime_method_source(schedule_source, "schedule_campaign_launch")
    process_due_scheduled_launches_source = _runtime_method_source(schedule_source, "process_due_scheduled_launches")
    for source in [schedule_campaign_launch_source, process_due_scheduled_launches_source]:
        for forbidden in ["clean_channel_enabled", "waiting_clean_channel"]:
            assert forbidden not in source
