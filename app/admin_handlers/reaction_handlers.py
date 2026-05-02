from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery

from app.reaction_ui import (
    build_rule_reaction_accounts_keyboard,
    build_rule_reaction_accounts_text,
    build_rule_reaction_back_keyboard,
    build_rule_reaction_connect_text,
    build_rule_reaction_preset_text,
    build_rule_reaction_test_text,
    build_rule_reactions_keyboard,
    build_rule_reactions_text,
)

from .context import AdminHandlersContext


def _resolve_reaction_tenant_id(ctx: AdminHandlersContext, user_id: int, rule) -> int:
    if ctx.is_admin_user(user_id):
        return int(getattr(rule, "tenant_id", 0) or 1)
    return int(ctx.ensure_user_tenant(user_id))


def register_admin_reaction_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data.startswith("rule_reactions:") or c.data.startswith("rule_reactions_refresh:"))
    async def handle_rule_reactions(callback: CallbackQuery):
        prefix = "rule_reactions_refresh:"
        if callback.data.startswith("rule_reactions:"):
            prefix = "rule_reactions:"
        try:
            rule_id = int(callback.data.split(":", 1)[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        tenant_id = await ctx.run_db(_resolve_reaction_tenant_id, ctx, user_id, rule)
        settings = await ctx.run_db(ctx.db.get_rule_reaction_settings_for_tenant, tenant_id, rule_id)
        accounts = await ctx.run_db(ctx.db.list_reaction_accounts_for_tenant, tenant_id, False)
        ctx.logger.info("REACTION_UI_OPENED | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_rule_reactions_text(rule_id, settings, accounts),
            reply_markup=build_rule_reactions_keyboard(rule_id, bool(settings and settings.get("enabled"))),
        )

    @dp.callback_query(lambda c: c.data.startswith("rule_reactions_toggle:"))
    async def handle_rule_reactions_toggle(callback: CallbackQuery):
        rule_id = int(callback.data.split(":", 1)[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        tenant_id = await ctx.run_db(_resolve_reaction_tenant_id, ctx, user_id, rule)
        settings = await ctx.run_db(ctx.db.get_rule_reaction_settings_for_tenant, tenant_id, rule_id)
        next_enabled = False
        if not settings:
            next_enabled = True
            await ctx.run_db(
                ctx.db.upsert_rule_reaction_settings,
                tenant_id=tenant_id,
                rule_id=rule_id,
                enabled=True,
                mode="premium_then_normal",
                preset=None,
                max_accounts_per_post=3,
                delay_min_sec=3,
                delay_max_sec=30,
                premium_first=True,
                stop_after_premium_success=False,
            )
        else:
            next_enabled = not bool(settings.get("enabled"))
            await ctx.run_db(
                ctx.db.upsert_rule_reaction_settings,
                tenant_id=tenant_id,
                rule_id=rule_id,
                enabled=next_enabled,
                mode=str(settings.get("mode") or "premium_then_normal"),
                preset=settings.get("preset_json"),
                max_accounts_per_post=int(settings.get("max_accounts_per_post") or 3),
                delay_min_sec=int(settings.get("delay_min_sec") or 3),
                delay_max_sec=int(settings.get("delay_max_sec") or 30),
                premium_first=bool(settings.get("premium_first", True)),
                stop_after_premium_success=bool(settings.get("stop_after_premium_success", False)),
            )
        ctx.logger.info("REACTION_RULE_SETTINGS_TOGGLED | tenant_id=%s | rule_id=%s | enabled=%s", tenant_id, rule_id, next_enabled)
        callback.data = f"rule_reactions:{rule_id}"
        await handle_rule_reactions(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_reactions_accounts:"))
    async def handle_rule_reactions_accounts(callback: CallbackQuery):
        rule_id = int(callback.data.split(":", 1)[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        tenant_id = await ctx.run_db(_resolve_reaction_tenant_id, ctx, user_id, rule)
        accounts = await ctx.run_db(ctx.db.list_reaction_accounts_for_tenant, tenant_id, False)
        ctx.logger.info("REACTION_ACCOUNTS_UI_OPENED | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_rule_reaction_accounts_text(accounts),
            reply_markup=build_rule_reaction_accounts_keyboard(rule_id),
        )

    @dp.callback_query(lambda c: c.data.startswith("rule_reactions_add_account:"))
    async def handle_rule_reactions_add_account(callback: CallbackQuery):
        rule_id = int(callback.data.split(":", 1)[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        tenant_id = await ctx.run_db(_resolve_reaction_tenant_id, ctx, user_id, rule)
        ctx.user_states[user_id] = {"state": "reaction_account_connect_intro", "rule_id": rule_id, "tenant_id": tenant_id}
        ctx.logger.info("REACTION_ACCOUNT_CONNECT_UI_OPENED | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_rule_reaction_connect_text(),
            reply_markup=build_rule_reaction_back_keyboard(rule_id),
        )

    @dp.callback_query(lambda c: c.data.startswith("rule_reactions_preset:"))
    async def handle_rule_reactions_preset(callback: CallbackQuery):
        rule_id = int(callback.data.split(":", 1)[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_rule_reaction_preset_text(),
            reply_markup=build_rule_reaction_back_keyboard(rule_id),
        )

    @dp.callback_query(lambda c: c.data.startswith("rule_reactions_test:"))
    async def handle_rule_reactions_test(callback: CallbackQuery):
        rule_id = int(callback.data.split(":", 1)[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_rule_reaction_test_text(),
            reply_markup=build_rule_reaction_back_keyboard(rule_id),
        )
