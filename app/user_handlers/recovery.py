from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery

from app import user_ui
from .context import UserHandlersContext


def register_user_recovery_handlers(dp: Dispatcher, ctx: UserHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data == "user_recovery")
    async def handle_user_recovery_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        ctx.logger.info("пользователь открыл recovery tenant_id=%s user_id=%s", tenant_id, user_id)
        summary = await ctx.run_db(ctx.recovery_service.build_recovery_summary, tenant_id)
        can_recover, reason = await ctx.run_db(ctx.recovery_service.can_recover, tenant_id)
        text = user_ui.build_user_recovery_summary_text(summary)
        if not can_recover and reason:
            text += f"\n\n⛔ {reason}"
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=text,
            reply_markup=user_ui.build_user_recovery_keyboard(can_recover=bool(can_recover)),
        )

    @dp.callback_query(lambda c: c.data == "user_recovery_run")
    async def handle_user_recovery_run_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        can_recover, reason = await ctx.run_db(ctx.recovery_service.can_recover, tenant_id)
        if not can_recover:
            ctx.logger.info("recovery запрещён из-за неактивной подписки tenant_id=%s", tenant_id)
            await ctx.answer_callback_safe(callback, reason or "Подписка ещё не активна", show_alert=True)
            return
        result = await ctx.run_db(ctx.recovery_service.recover_after_payment, tenant_id, user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_recovery_result_text(result),
            reply_markup=user_ui.build_user_recovery_keyboard(can_recover=True),
        )
