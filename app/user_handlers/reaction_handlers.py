from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, Message

from app.config import settings
from app.reaction_auth_service import ReactionAuthService
from app.reaction_auth_state import REACTION_AUTH_STATES
from app.reaction_ui import (
    build_reaction_auth_cancel_keyboard,
    build_reaction_auth_code_text,
    build_reaction_auth_error_text,
    build_reaction_auth_password_text,
    build_reaction_auth_phone_text,
    build_reaction_auth_success_keyboard,
    build_reaction_auth_success_text,
    build_rule_reaction_accounts_keyboard,
    build_rule_reaction_accounts_text,
    build_rule_reaction_back_keyboard,
    build_rule_reaction_preset_text,
    build_rule_reaction_test_text,
    build_rule_reactions_keyboard,
    build_rule_reactions_text,
)

from .context import UserHandlersContext
from .rules import ensure_rule_callback_access


def register_user_reaction_handlers(dp: Dispatcher, ctx: UserHandlersContext) -> None:
    auth_service = ReactionAuthService(ctx.db, api_id=settings.api_id, api_hash=settings.api_hash)

    async def _render_reactions(callback: CallbackQuery, rule_id: int, tenant_id: int, user_id: int) -> None:
        settings_row = await ctx.run_db(ctx.db.get_rule_reaction_settings_for_tenant, tenant_id, rule_id)
        accounts = await ctx.run_db(ctx.db.list_reaction_accounts_for_tenant, tenant_id, False)
        ctx.logger.info("USER_REACTION_UI_OPENED | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_rule_reactions_text(rule_id, settings_row, accounts),
            reply_markup=build_rule_reactions_keyboard(rule_id, bool(settings_row and settings_row.get("enabled"))),
        )

    @dp.callback_query(lambda c: c.data and (c.data.startswith("user_rule_reactions:") or c.data.startswith("user_rule_reactions_refresh:")))
    async def handle_user_rule_reactions(callback: CallbackQuery):
        prefix = "user_rule_reactions_refresh:" if (callback.data or "").startswith("user_rule_reactions_refresh:") else "user_rule_reactions:"
        rule_id = int((callback.data or f"{prefix}0").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        await _render_reactions(callback, rule_id, tenant_id, user_id)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_toggle:"))
    async def handle_user_rule_reactions_toggle(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        settings_row = await ctx.run_db(ctx.db.get_rule_reaction_settings_for_tenant, tenant_id, rule_id)
        next_enabled = not bool(settings_row and settings_row.get("enabled"))
        await ctx.run_db(
            ctx.db.upsert_rule_reaction_settings,
            tenant_id=tenant_id,
            rule_id=rule_id,
            enabled=next_enabled,
            mode="premium_then_normal",
            preset=(settings_row or {}).get("preset_json"),
            max_accounts_per_post=int((settings_row or {}).get("max_accounts_per_post") or 3),
            delay_min_sec=int((settings_row or {}).get("delay_min_sec") or 3),
            delay_max_sec=int((settings_row or {}).get("delay_max_sec") or 30),
            premium_first=True,
            stop_after_premium_success=False,
        )
        ctx.logger.info("USER_REACTION_RULE_SETTINGS_TOGGLED | tenant_id=%s | rule_id=%s | enabled=%s", tenant_id, rule_id, next_enabled)
        await _render_reactions(callback, rule_id, tenant_id, user_id)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_accounts:"))
    async def handle_user_rule_reactions_accounts(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        accounts = await ctx.run_db(ctx.db.list_reaction_accounts_for_tenant, tenant_id, False)
        ctx.logger.info("USER_REACTION_ACCOUNTS_UI_OPENED | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_accounts_text(accounts), reply_markup=build_rule_reaction_accounts_keyboard(rule_id))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_add_account:"))
    async def handle_user_rule_reactions_add_account(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        ctx.user_states[user_id] = {"state": "reaction_auth_wait_phone", "flow": "user_rule_reactions", "rule_id": rule_id, "tenant_id": tenant_id}
        ctx.logger.info("USER_REACTION_AUTH_STARTED | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=build_reaction_auth_phone_text(rule_id), reply_markup=build_reaction_auth_cancel_keyboard(rule_id))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_auth_cancel:"))
    async def handle_user_rule_reactions_auth_cancel(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        auth_service.cleanup_tmp_session(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id)
        ctx.user_states.pop(user_id, None)
        ctx.logger.info("USER_REACTION_AUTH_CANCELLED | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        await _render_reactions(callback, rule_id, tenant_id, user_id)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_preset:"))
    async def handle_user_rule_reactions_preset(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_preset_text(), reply_markup=build_rule_reaction_back_keyboard(rule_id))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_test:"))
    async def handle_user_rule_reactions_test(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_test_text(), reply_markup=build_rule_reaction_back_keyboard(rule_id))

    @dp.message(lambda m: m.from_user is not None and (ctx.user_states.get(m.from_user.id) or {}).get("state") in REACTION_AUTH_STATES and (ctx.user_states.get(m.from_user.id) or {}).get("flow") == "user_rule_reactions")
    async def handle_user_reaction_auth_messages(message: Message):
        user_id = message.from_user.id if message.from_user else 0
        state = ctx.user_states.get(user_id) or {}
        phase = state.get("state")
        rule_id = int(state.get("rule_id") or 0)
        tenant_id = int(state.get("tenant_id") or 0)
        if not rule_id or not tenant_id:
            ctx.user_states.pop(user_id, None)
            await message.answer("Сессия подключения устарела. Начните заново.")
            return
        text_kind = "phone_candidate" if phase == "reaction_auth_wait_phone" else ("code_candidate" if phase == "reaction_auth_wait_code" else "password_candidate")
        ctx.logger.info("USER_REACTION_AUTH_MESSAGE_RECEIVED | tenant_id=%s | rule_id=%s | user_id=%s | phase=%s | text_kind=%s", tenant_id, rule_id, user_id, phase, text_kind)
        try:
            if phase == "reaction_auth_wait_phone":
                result = await auth_service.start_phone_login(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id, phone=message.text or "")
                ctx.user_states[user_id] = {"state": "reaction_auth_wait_code", "flow": "user_rule_reactions", "rule_id": rule_id, "tenant_id": tenant_id, "phone": result["phone"], "phone_hint": result["phone_hint"], "phone_code_hash": result["phone_code_hash"]}
                ctx.logger.info("USER_REACTION_AUTH_CODE_SENT | tenant_id=%s | rule_id=%s | user_id=%s | phone_hint=%s", tenant_id, rule_id, user_id, result["phone_hint"])
                await message.answer(build_reaction_auth_code_text(result["phone_hint"]), reply_markup=build_reaction_auth_cancel_keyboard(rule_id))
                return
            if phase == "reaction_auth_wait_code":
                code = (message.text or "").strip().replace(" ", "")
                result = await auth_service.complete_code_login(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id, phone=str(state.get("phone") or ""), phone_code_hash=str(state.get("phone_code_hash") or ""), code=code)
                if result.get("status") == "password_required":
                    ctx.user_states[user_id] = {"state": "reaction_auth_wait_password", "flow": "user_rule_reactions", "rule_id": rule_id, "tenant_id": tenant_id, "phone": state.get("phone"), "phone_hint": state.get("phone_hint")}
                    ctx.logger.info("USER_REACTION_AUTH_PASSWORD_REQUIRED | tenant_id=%s | rule_id=%s | user_id=%s | phone_hint=%s", tenant_id, rule_id, user_id, state.get("phone_hint"))
                    await message.answer(build_reaction_auth_password_text(str(state.get("phone_hint") or "")), reply_markup=build_reaction_auth_cancel_keyboard(rule_id))
                    return
                ctx.user_states.pop(user_id, None)
                ctx.logger.info("USER_REACTION_AUTH_SUCCESS | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | telegram_user_id=%s | is_premium=%s", tenant_id, rule_id, user_id, result.get("account_id"), result.get("telegram_user_id"), result.get("is_premium"))
                await message.answer(build_reaction_auth_success_text(result), reply_markup=build_reaction_auth_success_keyboard(rule_id))
                return
            if phase == "reaction_auth_wait_password":
                result = await auth_service.complete_password_login(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id, password=message.text or "", phone=str(state.get("phone") or ""))
                ctx.user_states.pop(user_id, None)
                ctx.logger.info("USER_REACTION_AUTH_SUCCESS | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | telegram_user_id=%s | is_premium=%s", tenant_id, rule_id, user_id, result.get("account_id"), result.get("telegram_user_id"), result.get("is_premium"))
                await message.answer(build_reaction_auth_success_text(result), reply_markup=build_reaction_auth_success_keyboard(rule_id))
                return
        except ValueError as exc:
            text = str(exc)
            if "истёк" in text.lower():
                ctx.user_states.pop(user_id, None)
            ctx.logger.info("USER_REACTION_AUTH_FAILED | tenant_id=%s | rule_id=%s | user_id=%s | error_type=%s", tenant_id, rule_id, user_id, "value_error")
            await message.answer(build_reaction_auth_error_text(text), reply_markup=build_reaction_auth_cancel_keyboard(rule_id))
        except Exception:
            auth_service.cleanup_tmp_session(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id)
            ctx.user_states.pop(user_id, None)
            ctx.logger.info("USER_REACTION_AUTH_FAILED | tenant_id=%s | rule_id=%s | user_id=%s | error_type=%s", tenant_id, rule_id, user_id, "unexpected")
            await message.answer(build_reaction_auth_error_text("Не удалось отправить код. Попробуйте позже или начните заново."), reply_markup=build_reaction_auth_cancel_keyboard(rule_id))
