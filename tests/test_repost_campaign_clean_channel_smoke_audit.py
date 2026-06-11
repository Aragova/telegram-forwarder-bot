from pathlib import Path
import inspect

from app.repost_campaign_ui import build_repost_campaign_scheduled_launch_detail_view


ROOT = Path(__file__).resolve().parents[1]


def _source(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _assert_contains_all(source: str, tokens: list[str]) -> None:
    missing = [token for token in tokens if token not in source]
    assert not missing


def _assert_contains_none(source: str, tokens: list[str]) -> None:
    present = [token for token in tokens if token in source]
    assert not present


def test_scheduled_clean_channel_chain_is_present():
    schedule_service = _source("app/repost_campaign_schedule_service.py")
    schedule_handlers = _source("app/repost_campaign_schedule_handlers.py")
    message_handlers = _source("app/repost_campaign_message_handlers.py")
    ui = _source("app/repost_campaign_ui.py")
    repository = _source("app/repository.py")
    postgres_repository = _source("app/postgres_repository.py")

    _assert_contains_all(
        schedule_service,
        [
            "build_scheduled_launch_policy_state",
            "build_launch_policy_preview",
            "schedule_with_clean_channel_wait",
            "schedule_with_overlap_warning",
            "mark_campaign_scheduled_launch_waiting_clean_channel",
            "waiting_clean_channel",
            "CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_RETRY_SECONDS",
            "CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_MAX_ATTEMPTS",
        ],
    )
    assert 'launch_mode="scheduled"' in schedule_service or 'launch_mode = "scheduled"' in schedule_service

    _assert_contains_all(
        schedule_handlers,
        [
            "build_scheduled_launch_policy_state",
            "scheduled_policy",
            "schedule_policy_ack",
            "schedule_with_overlap_warning",
        ],
    )
    _assert_contains_all(
        message_handlers,
        [
            "RepostCampaignScheduleService",
            "build_scheduled_launch_policy_state",
            "scheduled_policy",
            "schedule_policy_ack",
        ],
    )
    _assert_contains_all(
        ui,
        [
            "build_repost_campaign_schedule_clean_channel_notice_view",
            "build_repost_campaign_schedule_clean_channel_warning_view",
            "build_repost_campaign_schedule_clean_channel_error_view",
            "waiting_clean_channel",
            "Ждёт чистый канал",
            "Следующая проверка",
            "Попыток ожидания",
        ],
    )
    _assert_contains_all(
        repository,
        [
            "mark_campaign_scheduled_launch_waiting_clean_channel",
            "mark_campaign_scheduled_launch_scheduled_again",
        ],
    )
    _assert_contains_all(
        postgres_repository,
        [
            "clean_channel_next_retry_at",
            "clean_channel_wait_attempt_count",
            "clean_channel_last_wait_at",
            "clean_channel_last_reason",
            "clean_channel_policy_json",
            "idx_campaign_scheduled_launches_waiting_clean_channel_due",
        ],
    )


def test_manual_clean_channel_flow_is_still_manual_only():
    manual_handlers = _source("app/repost_campaign_handlers.py")
    schedule_handlers = _source("app/repost_campaign_schedule_handlers.py")
    message_handlers = _source("app/repost_campaign_message_handlers.py")
    schedule_service = _source("app/repost_campaign_schedule_service.py")

    _assert_contains_all(
        manual_handlers,
        [
            "rule_repost_campaign_launch_confirm_force",
            "build_manual_launch_policy_state",
            "force_ignore_clean_channel",
        ],
    )
    manual_only_tokens = [
        "rule_repost_campaign_launch_confirm_force",
        "force_ignore_clean_channel",
        "build_manual_launch_policy_state",
    ]
    _assert_contains_none(schedule_handlers, manual_only_tokens)
    _assert_contains_none(message_handlers, ["force_ignore_clean_channel", "build_manual_launch_policy_state"])
    assert "force_ignore_clean_channel" not in schedule_service


def test_vip_scheduled_posts_are_not_wired_to_ordinary_clean_channel_waiting():
    scheduled_post_service = _source("app/repost_campaign_scheduled_post_service.py")

    _assert_contains_none(
        scheduled_post_service,
        [
            "waiting_clean_channel",
            "clean_channel_next_retry_at",
            "clean_channel_wait_attempt_count",
            "clean_channel_policy_json",
            "mark_campaign_scheduled_launch_waiting_clean_channel",
            "build_scheduled_launch_policy_state",
            "schedule_with_clean_channel_wait",
            "schedule_with_overlap_warning",
            "force_ignore_clean_channel",
        ],
    )


def test_clean_channel_ui_does_not_render_raw_policy_snapshots():
    ui = _source("app/repost_campaign_ui.py")
    detail_builder_source = inspect.getsource(build_repost_campaign_scheduled_launch_detail_view)

    _assert_contains_all(ui, ["_safe_scheduled_launch_wait_reason", "_campaign_policy_user_note"])
    _assert_contains_none(
        ui,
        [
            "clean_channel_policy_json",
            "str(policy",
            "repr(policy",
            "json.dumps(policy",
        ],
    )
    assert "schedule_with_clean_channel_wait" not in detail_builder_source

    text, _ = build_repost_campaign_scheduled_launch_detail_view(
        rule_id=10,
        scheduled_launch={
            "id": 1,
            "status": "waiting_clean_channel",
            "scheduled_at": "2026-06-11T12:00:00+00:00",
            "clean_channel_policy_json": {"action": "schedule_with_clean_channel_wait"},
            "clean_channel_last_reason": "Traceback db runtime json clean_channel_policy",
        },
    )
    lowered = text.lower()
    for token in [
        "traceback",
        "db",
        "runtime",
        "json",
        "clean_channel_policy",
        "schedule_with_clean_channel_wait",
        "action",
    ]:
        assert token not in lowered
    assert "Чистый канал занят активной рекламой" in text


def test_repository_and_worker_boundaries_are_clear():
    postgres_repository = _source("app/postgres_repository.py")
    schedule_service = _source("app/repost_campaign_schedule_service.py")

    _assert_contains_all(
        postgres_repository,
        [
            "waiting_clean_channel",
            "mark_campaign_scheduled_launch_waiting_clean_channel",
            "mark_campaign_scheduled_launch_scheduled_again",
            "claim_due_campaign_scheduled_launches",
            "clean_channel_next_retry_at",
        ],
    )
    _assert_contains_none(
        postgres_repository,
        [
            "build_scheduled_launch_policy_state",
            "launch_campaign_now",
            "RepostCampaignRuntimeService",
        ],
    )
    _assert_contains_all(
        schedule_service,
        [
            "build_scheduled_launch_policy_state",
            "mark_campaign_scheduled_launch_waiting_clean_channel",
            "launch_campaign_now",
        ],
    )


def test_clean_channel_docs_exist():
    docs_path = ROOT / "docs/repost_campaign_clean_channel.md"
    assert docs_path.exists()
    docs = docs_path.read_text(encoding="utf-8")
    _assert_contains_all(
        docs,
        [
            "Чистый канал",
            "Запланировать запуск",
            "waiting_clean_channel",
            "schedule_with_clean_channel_wait",
            "schedule_with_overlap_warning",
            "manual",
            "VIP",
            "6.1",
            "6.6",
        ],
    )
