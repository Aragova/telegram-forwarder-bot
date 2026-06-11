from datetime import datetime, timezone
from pathlib import Path

from app.repost_campaign_ui import (
    TG_TEXT_SAFE_LIMIT,
    build_repost_campaign_schedule_clean_channel_error_view,
    build_repost_campaign_schedule_clean_channel_notice_view,
    build_repost_campaign_schedule_clean_channel_warning_view,
    build_repost_campaign_schedule_preview_view,
)

SCHEDULED_AT = "2026-06-10T09:00:00+00:00"
SCHEDULED_AT_TEXT = "10.06 12:00 UTC+3"
EXPECTED_EPOCH = 1781082000


def _callbacks_from_keyboard(keyboard):
    return [button.callback_data for row in keyboard.inline_keyboard for button in row]


def _texts_from_keyboard(keyboard):
    return [button.text for row in keyboard.inline_keyboard for button in row]


def _readiness():
    return {
        "can_launch": True,
        "saved_post_id": 2,
        "targets_total": 2,
        "will_send_total": 2,
        "will_skip_total": 0,
        "show_seconds": 3600,
    }


def _wait_policy(**extra):
    state = {
        "ok": True,
        "action": "schedule_with_clean_channel_wait",
        "scheduled_at": SCHEDULED_AT,
        "scheduled_at_text": SCHEDULED_AT_TEXT,
        "active_placements_total": 2,
        "delete_problem_total": 1,
        "warning_text": "Если к моменту запуска в канале будет активная реклама, ViMi подождёт.",
    }
    state.update(extra)
    return state


def _warning_policy(**extra):
    state = {
        "ok": True,
        "action": "schedule_with_overlap_warning",
        "scheduled_at": SCHEDULED_AT,
        "scheduled_at_text": SCHEDULED_AT_TEXT,
        "active_placements_total": 1,
        "delete_problem_total": 0,
        "warning_text": "Чистый канал выключен.",
    }
    state.update(extra)
    return state


def test_notice_view_for_schedule_with_clean_channel_wait():
    text, keyboard = build_repost_campaign_schedule_clean_channel_notice_view(
        rule_id=10,
        policy_state=_wait_policy(),
    )
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Чистый канал включён" in text
    assert "Запуск можно запланировать" in text
    assert "подождёт" in text
    assert "Активных размещений: 2" in text
    assert "Проблем удаления: 1" in text
    assert f"rule_repost_campaign_schedule_confirm:10:{EXPECTED_EPOCH}" in callbacks
    assert "rule_repost_campaign_active_placements:10:0" in callbacks
    assert "rule_repost_campaign_clean_channel:10" in callbacks
    assert "rule_repost_campaign_schedule_step4:10" in callbacks


def test_warning_view_for_schedule_with_overlap_warning():
    text, keyboard = build_repost_campaign_schedule_clean_channel_warning_view(
        rule_id=10,
        policy_state=_warning_policy(),
    )
    labels = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)
    payload = "\n".join([text, *labels, *callbacks])

    assert "Чистый канал выключен" in text
    assert "новая реклама может выйти поверх старой" in text
    assert "⚠️ Всё равно запланировать" in labels
    assert f"rule_repost_campaign_schedule_confirm:10:{EXPECTED_EPOCH}" in callbacks
    assert "force_ignore_clean_channel" not in payload
    assert "launch_confirm_force" not in payload


def test_error_view_uses_safe_text_and_has_no_confirm():
    text, keyboard = build_repost_campaign_schedule_clean_channel_error_view(
        rule_id=10,
        policy_state={"ok": False, "action": "policy_error", "blocking_text": "Traceback db is unavailable runtime json"},
    )
    labels = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Не удалось проверить Чистый канал" in text
    lowered = text.lower()
    assert "traceback" not in lowered
    assert "db" not in lowered
    assert "runtime" not in lowered
    assert "json" not in lowered
    assert not any("rule_repost_campaign_schedule_confirm:" in (callback or "") for callback in callbacks)
    assert "🔄 Проверить снова" in labels


def test_preview_backward_compatibility_without_scheduled_policy():
    text, keyboard = build_repost_campaign_schedule_preview_view(
        rule_id=10,
        readiness=_readiness(),
        scheduled_at_utc=datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc),
    )
    callbacks = _callbacks_from_keyboard(keyboard)

    assert f"rule_repost_campaign_schedule_confirm:10:{EXPECTED_EPOCH}" in callbacks
    assert "Чистый канал включён" not in text
    assert "Чистый канал выключен" not in text
    assert "Не удалось проверить Чистый канал" not in text


def test_preview_dispatches_wait_policy():
    text, keyboard = build_repost_campaign_schedule_preview_view(
        rule_id=10,
        readiness=_readiness(),
        scheduled_at_utc=datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc),
        scheduled_policy=_wait_policy(),
    )
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Чистый канал включён" in text
    assert f"rule_repost_campaign_schedule_confirm:10:{EXPECTED_EPOCH}" in callbacks


def test_preview_dispatches_overlap_warning_policy():
    text, keyboard = build_repost_campaign_schedule_preview_view(
        rule_id=10,
        readiness=_readiness(),
        scheduled_at_utc=datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc),
        scheduled_policy=_warning_policy(),
    )
    labels = _texts_from_keyboard(keyboard)
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Чистый канал выключен" in text
    assert "⚠️ Всё равно запланировать" in labels
    assert f"rule_repost_campaign_schedule_confirm:10:{EXPECTED_EPOCH}" in callbacks


def test_preview_dispatches_policy_error():
    text, keyboard = build_repost_campaign_schedule_preview_view(
        rule_id=10,
        readiness=_readiness(),
        scheduled_at_utc=datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc),
        scheduled_policy={"ok": False, "action": "policy_error", "blocking_text": "Traceback runtime json db"},
    )
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "Не удалось проверить Чистый канал" in text
    assert not any("rule_repost_campaign_schedule_confirm:" in (callback or "") for callback in callbacks)


def test_preview_allow_keeps_regular_confirm_callback():
    text, keyboard = build_repost_campaign_schedule_preview_view(
        rule_id=10,
        readiness=_readiness(),
        scheduled_at_utc=datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc),
        scheduled_policy={"ok": True, "action": "allow", "scheduled_at": SCHEDULED_AT, "scheduled_at_text": SCHEDULED_AT_TEXT},
    )
    callbacks = _callbacks_from_keyboard(keyboard)

    assert "👁 Предпросмотр запланированного запуска" in text
    assert f"rule_repost_campaign_schedule_confirm:10:{EXPECTED_EPOCH}" in callbacks


def test_long_policy_text_is_trimmed_to_telegram_safe_limit():
    text, _ = build_repost_campaign_schedule_clean_channel_notice_view(
        rule_id=10,
        policy_state=_wait_policy(warning_text="подсказка " * 10000),
    )

    assert len(text) <= TG_TEXT_SAFE_LIMIT


def test_stage_six_two_source_guards_keep_ui_unwired():
    schedule_handlers = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    schedule_service = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")
    scheduled_post_service = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")
    handlers = Path("app/repost_campaign_handlers.py").read_text(encoding="utf-8")

    for forbidden in [
        "build_repost_campaign_schedule_clean_channel_notice_view",
        "build_repost_campaign_schedule_clean_channel_warning_view",
        "build_repost_campaign_schedule_clean_channel_error_view",
    ]:
        assert forbidden not in schedule_handlers
    assert "scheduled_policy" in schedule_handlers

    for forbidden in [
        "build_repost_campaign_schedule_clean_channel_notice_view",
        "build_repost_campaign_schedule_clean_channel_warning_view",
        "build_repost_campaign_schedule_clean_channel_error_view",
    ]:
        assert forbidden not in schedule_service

    for forbidden in [
        "build_repost_campaign_schedule_clean_channel_notice_view",
        "build_repost_campaign_schedule_clean_channel_warning_view",
        "schedule_with_clean_channel_wait",
    ]:
        assert forbidden not in scheduled_post_service

    for forbidden in [
        "build_repost_campaign_schedule_clean_channel_notice_view",
        "build_repost_campaign_schedule_clean_channel_warning_view",
    ]:
        assert forbidden not in handlers
