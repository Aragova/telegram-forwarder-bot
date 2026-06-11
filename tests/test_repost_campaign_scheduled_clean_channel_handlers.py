from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app import repost_campaign_schedule_handlers as schedule_handlers


def _method_source(source: str, name: str) -> str:
    marker = f"    async def {name}"
    start = source.index(marker)
    rest = source[start:]
    next_marker = rest.find("\n    @dp.callback_query", 1)
    if next_marker == -1:
        return rest
    return rest[:next_marker]


class _User:
    id = 77


class _Callback:
    from_user = _User()


class _Ctx:
    def __init__(self):
        self.user_states = {}


def test_source_guards_handlers_wired_worker_and_vip_not_wired():
    schedule_source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    message_source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    service_source = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")
    process_due_source = service_source.split("    async def process_due_scheduled_launches", 1)[1]
    scheduled_post_service = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")

    for token in (
        "build_scheduled_launch_policy_state",
        "scheduled_policy",
        "schedule_with_overlap_warning",
        "schedule_policy_ack",
    ):
        assert token in schedule_source
    for token in (
        "RepostCampaignScheduleService",
        "build_scheduled_launch_policy_state",
        "scheduled_policy",
        "schedule_policy_ack",
    ):
        assert token in message_source
    for token in (
        "build_scheduled_launch_policy_state",
        "schedule_with_clean_channel_wait",
        "schedule_with_overlap_warning",
        "waiting_clean_channel",
    ):
        assert token not in process_due_source
    for token in (
        "build_scheduled_launch_policy_state",
        "schedule_with_clean_channel_wait",
        "schedule_with_overlap_warning",
        "force_ignore_clean_channel",
    ):
        assert token not in scheduled_post_service


def test_quick_preset_preview_passes_scheduled_policy_source_guard():
    source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    method = _method_source(source, "handle_rule_repost_campaign_schedule_quick")

    assert "_build_repost_campaign_schedule_policy_preview" in method
    assert "build_scheduled_launch_policy_state" in source
    assert "scheduled_policy=policy_state" in source
    assert "schedule_campaign_launch(" not in method


def test_custom_date_input_preview_passes_scheduled_policy_source_guard():
    source = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    branch = source.split('state.get("state") != "repost_campaign_schedule_input"', 1)[1]

    assert "RepostCampaignScheduleService" in branch
    assert "build_scheduled_launch_policy_state" in branch
    assert "scheduled_policy=policy_state" in branch
    assert "schedule_policy_ack" in branch


def test_confirm_rechecks_policy_and_blocks_errors_source_guard():
    source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    method = _method_source(source, "handle_rule_repost_campaign_schedule_confirm")

    assert "ctx.is_admin_callback" in method
    assert "repost_campaign_admin_test_enabled" in method
    assert "Ошибка данных" in method
    assert "build_scheduled_launch_policy_state" in method
    assert "REPOST_CAMPAIGN_SCHEDULE_CONFIRM_POLICY_FAILED" in method
    assert 'policy_state.get("ok") is False or not can_schedule' in method
    assert "scheduled_policy=policy_state" in method
    assert "schedule_campaign_launch(" in method


def test_confirm_wait_policy_has_no_force_or_worker_source_guard():
    source = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    method = _method_source(source, "handle_rule_repost_campaign_schedule_confirm")

    assert "schedule_with_clean_channel_wait" in method
    assert "force_ignore_clean_channel" not in method
    assert "process_due_scheduled_launches" not in method


def test_overlap_warning_ack_helpers_require_matching_rule_epoch_and_action():
    ctx = _Ctx()
    callback = _Callback()
    scheduled_at = datetime(2026, 5, 9, 15, 0, tzinfo=timezone.utc)

    assert not schedule_handlers._has_schedule_policy_ack(
        ctx,
        callback,
        rule_id=10,
        scheduled_at_utc=scheduled_at,
        action="schedule_with_overlap_warning",
    )

    schedule_handlers._set_schedule_policy_ack(
        ctx,
        callback,
        rule_id=10,
        scheduled_at_utc=scheduled_at,
        action="schedule_with_overlap_warning",
    )

    assert ctx.user_states[77]["schedule_policy_ack"] == {
        "key": f"repost_campaign_schedule_policy_ack:10:{int(scheduled_at.timestamp())}",
        "action": "schedule_with_overlap_warning",
    }
    assert schedule_handlers._has_schedule_policy_ack(
        ctx,
        callback,
        rule_id=10,
        scheduled_at_utc=scheduled_at,
        action="schedule_with_overlap_warning",
    )
    assert not schedule_handlers._has_schedule_policy_ack(
        ctx,
        callback,
        rule_id=11,
        scheduled_at_utc=scheduled_at,
        action="schedule_with_overlap_warning",
    )
    assert not schedule_handlers._has_schedule_policy_ack(
        ctx,
        callback,
        rule_id=10,
        scheduled_at_utc=scheduled_at,
        action="schedule_with_clean_channel_wait",
    )

    schedule_handlers._clear_schedule_policy_ack(ctx, callback)
    assert "schedule_policy_ack" not in ctx.user_states[77]
