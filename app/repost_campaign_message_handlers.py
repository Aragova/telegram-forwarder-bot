from __future__ import annotations

from aiogram import Dispatcher

from app.repost_campaign_context import RepostCampaignHandlersContext


def is_waiting_vip_scheduled_post_material(ctx: RepostCampaignHandlersContext, user_id: int) -> bool:
    state = ctx.user_states.get(user_id, {})
    return state.get('state') == 'waiting_vip_scheduled_post_material'


async def handle_vip_scheduled_post_material_message(ctx: RepostCampaignHandlersContext, message) -> bool:
    _ = (ctx, message)
    return False


async def handle_repost_campaign_stateful_private_input(ctx: RepostCampaignHandlersContext, message, state: dict, text: str) -> bool:
    _ = (ctx, message, state, text)
    return False


def register_repost_campaign_message_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    _ = (dp, ctx)
