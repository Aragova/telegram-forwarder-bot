from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext
from app.repost_campaign_context import build_repost_campaign_runtime
from app.repost_campaign_schedule_service import parse_campaign_schedule_input_to_utc
from app.repost_campaign_ui import (
    build_repost_campaign_schedule_preview_view,
    build_repost_campaign_schedule_wizard_step3_view,
)


def is_waiting_vip_scheduled_post_material(ctx: RepostCampaignHandlersContext, user_id: int) -> bool:
    state = ctx.user_states.get(user_id, {})
    return state.get('state') == 'waiting_vip_scheduled_post_material'


async def handle_vip_scheduled_post_material_message(ctx: RepostCampaignHandlersContext, message) -> bool:
    _ = (ctx, message)
    return False


async def handle_repost_campaign_stateful_private_input(ctx: RepostCampaignHandlersContext, message, state: dict, text: str) -> bool:
    if state.get("state") != "repost_campaign_schedule_input":
        return False
    rule_id = int(state.get("rule_id") or 0)
    parsed = parse_campaign_schedule_input_to_utc(text)
    if parsed is None:
        await message.answer(
            "❌ Не удалось распознать дату и время.\n\nВведите в формате:\n09.05 18:00\n\nЧасовой пояс: UTC+3",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к выбору времени", callback_data=f"rule_repost_campaign_schedule_step4:{rule_id}")]]
            ),
        )
        return True
    runtime = build_repost_campaign_runtime(ctx)
    readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
    if int(readiness.get("show_seconds") or 0) <= 0:
        text_step3, kb_step3 = build_repost_campaign_schedule_wizard_step3_view(rule_id=rule_id, readiness=readiness)
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await message.answer(text_step3, reply_markup=kb_step3)
        return True
    text_preview, kb_preview = build_repost_campaign_schedule_preview_view(rule_id=rule_id, readiness=readiness, scheduled_at_utc=parsed)
    ctx.reset_user_state(message.from_user.id if message.from_user else None)
    await message.answer(text_preview, reply_markup=kb_preview)
    return True


def register_repost_campaign_message_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    _ = (dp, ctx)
