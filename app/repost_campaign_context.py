from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.repost_campaign_delete_service import RepostCampaignDeleteService
from app.repost_campaign_runtime_service import RepostCampaignRuntimeService
from app.repost_campaign_scheduled_post_service import RepostCampaignScheduledPostService
from app.repost_campaign_target_check_service import RepostCampaignTargetCheckService
from app.saved_post_renderer import SavedPostRenderer


@dataclass(frozen=True)
class RepostCampaignHandlersContext:
    db: Any
    settings: Any
    logger: Any
    user_states: dict
    saved_post_album_buffer: dict
    run_db: Any
    get_bot: Callable[[], Any]
    get_telethon_client: Callable[[], Any]
    ensure_rule_callback_access: Callable[..., Any]
    is_admin_callback: Callable[..., Any]
    answer_callback_safe: Callable[..., Any]
    answer_callback_safe_once: Callable[..., Any]
    edit_message_text_safe: Callable[..., Any]
    send_message_safe: Callable[..., Any]
    invalidate_rule_card_cache: Callable[..., Any]
    reset_user_state: Callable[..., Any]
    should_answer_new_message_for_callback: Callable[..., Any]


def build_repost_campaign_runtime(ctx: RepostCampaignHandlersContext) -> RepostCampaignRuntimeService:
    bot = ctx.get_bot()
    telethon_client = ctx.get_telethon_client()

    renderer = SavedPostRenderer(
        bot=bot,
        telethon_client=telethon_client,
        temp_dir=getattr(ctx.settings, "temp_dir", "media/temp"),
        logger_=ctx.logger,
    )
    deleter = RepostCampaignDeleteService(
        bot=bot,
        telethon_client=telethon_client,
        logger_=ctx.logger,
    )
    target_checker = RepostCampaignTargetCheckService(
        telethon_client=telethon_client,
        bot=bot,
        logger_=ctx.logger,
    )
    return RepostCampaignRuntimeService(
        repo=ctx.db,
        renderer=renderer,
        deleter=deleter,
        target_checker=target_checker,
        telethon_client=telethon_client,
        logger_=ctx.logger,
    )


def build_repost_campaign_scheduled_post_service(ctx: RepostCampaignHandlersContext) -> RepostCampaignScheduledPostService:
    runtime = build_repost_campaign_runtime(ctx)
    return RepostCampaignScheduledPostService(repo=ctx.db, campaign_runtime=runtime, logger_=ctx.logger)
