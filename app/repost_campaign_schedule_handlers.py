from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime
from app.repost_campaign_ui import build_repost_campaign_schedule_current_view, build_repost_campaign_schedule_wizard_step1_view


def register_repost_campaign_schedule_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_menu:"))
    async def handle_rule_repost_campaign_schedule_menu(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        runtime = build_repost_campaign_runtime(ctx)
        readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
        text, kb = build_repost_campaign_schedule_wizard_step1_view(rule_id=rule_id, readiness=readiness)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_current:"))
    async def handle_rule_repost_campaign_schedule_current(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        try:
            runtime = build_repost_campaign_runtime(ctx)
            readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
            text, keyboard = build_repost_campaign_schedule_current_view(rule_id=rule_id, readiness=readiness)
            if ctx.should_answer_new_message_for_callback(callback):
                await ctx.send_message_safe(chat_id=callback.from_user.id, text=text, reply_markup=keyboard)
            else:
                await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        except Exception as exc:
            ctx.logger.warning("REPOST_CAMPAIGN_SCHEDULE_CURRENT_UI_FAILED | rule_id=%s | error=%s", rule_id, exc)
            await ctx.answer_callback_safe(callback, "Не удалось открыть планирование запуска", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
