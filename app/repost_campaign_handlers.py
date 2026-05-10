from __future__ import annotations

from aiogram import Dispatcher

from app.repost_campaign_context import RepostCampaignHandlersContext


def register_repost_campaign_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    _ = (dp, ctx)
