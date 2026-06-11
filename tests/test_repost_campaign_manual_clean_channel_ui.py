from pathlib import Path

from app.repost_campaign_ui import (
    TG_TEXT_SAFE_LIMIT,
    build_repost_campaign_launch_clean_channel_blocked_view,
    build_repost_campaign_launch_clean_channel_warning_view,
)


def _callbacks_from_keyboard(keyboard):
    return [
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data
    ]


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def test_blocked_view_basic_text_is_safe_and_human_readable():
    text, _ = build_repost_campaign_launch_clean_channel_blocked_view(
        rule_id=10,
        policy_state={
            "action": "block",
            "clean_channel_enabled": True,
            "active_placements_total": 2,
            "delete_problem_total": 1,
            "blocking_text": "Сейчас уже есть активное рекламное размещение.",
        },
    )

    assert "Чистый канал включён" in text
    assert "Активных размещений: 2" in text
    assert "Проблем удаления: 1" in text
    assert "Новый запуск не будет опубликован поверх активной рекламы" in text
    assert "Traceback" not in text
    assert "runtime" not in text
    assert "DB" not in text


def test_blocked_view_buttons_do_not_include_force_launch():
    rule_id = 10
    _, keyboard = build_repost_campaign_launch_clean_channel_blocked_view(
        rule_id=rule_id,
        policy_state={"active_placements_total": 2, "delete_problem_total": 1},
    )
    callbacks = _callbacks_from_keyboard(keyboard)
    texts = _texts_from_keyboard(keyboard)

    assert f"rule_repost_campaign_active_placements:{rule_id}:0" in callbacks
    assert f"rule_repost_campaign_clean_channel:{rule_id}" in callbacks
    assert f"rule_repost_campaign_launch:{rule_id}" in callbacks
    assert f"rule_repost_campaign_menu:{rule_id}" in callbacks
    assert f"rule_repost_campaign_launch_confirm_force:{rule_id}" not in callbacks
    assert "🚀 Всё равно запустить" not in texts


def test_warning_view_basic_text_is_safe_and_human_readable():
    text, _ = build_repost_campaign_launch_clean_channel_warning_view(
        rule_id=10,
        policy_state={
            "action": "confirm_required",
            "clean_channel_enabled": False,
            "active_placements_total": 3,
            "delete_problem_total": 0,
            "warning_text": "Чистый канал выключен. ViMi разрешит запуск поверх активной рекламы.",
        },
    )

    assert "Чистый канал выключен" in text
    assert "новая реклама будет опубликована поверх старой" in text
    assert "Активных размещений: 3" in text
    assert "Проблем удаления: 0" in text
    assert "Продолжить запуск?" in text
    assert "Traceback" not in text
    assert "runtime" not in text
    assert "DB" not in text


def test_warning_view_buttons_include_force_launch_confirmation():
    rule_id = 10
    _, keyboard = build_repost_campaign_launch_clean_channel_warning_view(
        rule_id=rule_id,
        policy_state={"active_placements_total": 3, "delete_problem_total": 0},
    )
    callbacks = _callbacks_from_keyboard(keyboard)
    texts = _texts_from_keyboard(keyboard)

    assert f"rule_repost_campaign_launch_confirm_force:{rule_id}" in callbacks
    assert f"rule_repost_campaign_active_placements:{rule_id}:0" in callbacks
    assert f"rule_repost_campaign_clean_channel:{rule_id}" in callbacks
    assert f"rule_repost_campaign_launch:{rule_id}" in callbacks
    assert "🚀 Всё равно запустить" in texts


def test_missing_counters_fallback_to_zero():
    blocked_text, _ = build_repost_campaign_launch_clean_channel_blocked_view(rule_id=10, policy_state={})
    warning_text, _ = build_repost_campaign_launch_clean_channel_warning_view(rule_id=10, policy_state={})

    for text in (blocked_text, warning_text):
        assert "Активных размещений: 0" in text
        assert "Проблем удаления: 0" in text


def test_unsafe_long_policy_text_is_trimmed_and_does_not_crash():
    unsafe_text = "Traceback runtime DB sent_message_id target_id run_id " + ("подробности " * 2000)

    blocked_text, _ = build_repost_campaign_launch_clean_channel_blocked_view(
        rule_id=10,
        policy_state={"blocking_text": unsafe_text},
    )
    warning_text, _ = build_repost_campaign_launch_clean_channel_warning_view(
        rule_id=10,
        policy_state={"warning_text": unsafe_text},
    )

    assert len(blocked_text) <= TG_TEXT_SAFE_LIMIT
    assert len(warning_text) <= TG_TEXT_SAFE_LIMIT


def test_stage_five_six_source_guards_keep_ui_wiring_manual_only():
    handlers_source = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")
    runtime_source = Path("app/repost_campaign_runtime_service.py").read_text(encoding="utf-8")
    launch_job_source = Path("app/repost_campaign_launch_job_service.py").read_text(encoding="utf-8")
    schedule_source = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")
    scheduled_post_source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")

    for required in [
        "build_repost_campaign_launch_clean_channel_blocked_view",
        "build_repost_campaign_launch_clean_channel_warning_view",
        "rule_repost_campaign_launch_confirm_force",
    ]:
        assert required in handlers_source

    for forbidden in [
        "build_repost_campaign_launch_clean_channel_blocked_view",
        "build_repost_campaign_launch_clean_channel_warning_view",
    ]:
        assert forbidden not in runtime_source
        assert forbidden not in launch_job_source

    assert "rule_repost_campaign_launch_confirm_force" not in launch_job_source

    for forbidden in [
        "rule_repost_campaign_launch_confirm_force",
        "build_repost_campaign_launch_clean_channel_warning_view",
        "build_manual_launch_policy_state",
        "force_ignore_clean_channel",
    ]:
        assert forbidden not in schedule_source
        assert forbidden not in scheduled_post_source
