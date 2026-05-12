from types import SimpleNamespace
from pathlib import Path

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime


def _noop(*args, **kwargs):
    return None


def test_build_repost_campaign_runtime_factory_constructs_services():
    fake_bot = object()
    fake_telethon = object()
    ctx = RepostCampaignHandlersContext(
        db=SimpleNamespace(),
        settings=SimpleNamespace(temp_dir="media/temp"),
        logger=SimpleNamespace(info=_noop, warning=_noop, exception=_noop),
        user_states={},
        saved_post_album_buffer={},
        run_db=_noop,
        get_bot=lambda: fake_bot,
        get_telethon_client=lambda: fake_telethon,
        ensure_rule_callback_access=_noop,
        is_admin_callback=_noop,
        answer_callback_safe=_noop,
        answer_callback_safe_once=_noop,
        edit_message_text_safe=_noop,
        send_message_safe=_noop,
        invalidate_rule_card_cache=_noop,
        reset_user_state=_noop,
        should_answer_new_message_for_callback=_noop,
    )

    runtime = build_repost_campaign_runtime(ctx)

    assert runtime.repo is ctx.db
    assert runtime.renderer.bot is fake_bot
    assert runtime.renderer.telethon_client is fake_telethon
    assert runtime.deleter.bot is fake_bot
    assert runtime.target_checker.telethon_client is fake_telethon


def test_vip_scheduled_posts_handler_uses_service_active_placement_source():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "service = _build_repost_campaign_scheduled_post_service()" in source
    assert "active_placement = await run_db(service.build_active_scheduled_post_placement, rule_id=rule_id)" in source


def test_vip_delete_handler_checks_vip_active_before_delete():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "Активных VIP-запланированных размещений нет." in source
    assert "updated = await run_db(service.build_active_scheduled_post_placement, rule_id=rule_id)" in source


def test_manual_schedule_input_branch_builds_preview_and_returns():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert 'if state.get("state") == "repost_campaign_schedule_input":' in source
    assert "parsed = parse_campaign_schedule_input_to_utc(text)" in source
    assert "text_preview, kb_preview = build_repost_campaign_schedule_preview_view(rule_id=rule_id, readiness=readiness, scheduled_at_utc=parsed)" in source
    assert "reset_user_state(user_id)" in source
