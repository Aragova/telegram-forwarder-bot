from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, Message

from app.config import settings
from app.reaction_auth_service import ReactionAuthService
from app.reaction_onboarding_token import create_reaction_onboarding_token
from app.reaction_ui import (
    build_reaction_account_reactions_keyboard,
    build_reaction_account_reactions_text,
    build_rule_reaction_account_delete_confirm_keyboard,
    build_rule_reaction_account_delete_confirm_text,
    build_rule_reaction_account_detail_keyboard,
    build_rule_reaction_account_detail_text,
    build_rule_reaction_accounts_keyboard_with_items,
    build_reaction_web_onboarding_keyboard,
    build_reaction_web_onboarding_text,
    build_rule_reaction_accounts_text,
    build_rule_reaction_back_keyboard,
    build_rule_reaction_test_text,
    build_rule_reactions_keyboard,
    build_rule_reactions_text,
    normalize_fixed_reactions_input,
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
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_accounts_text(accounts), reply_markup=build_rule_reaction_accounts_keyboard_with_items(rule_id, accounts))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account:"))
    async def handle_user_rule_reactions_account(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        user_state = ctx.user_states.get(user_id) or {}
        if user_state.get("flow") == "user_rule_reactions":
            ctx.user_states.pop(user_id, None)
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        await ctx.answer_callback_safe_once(callback)
        if not account:
            await ctx.edit_message_text_safe(message=callback.message, text="Аккаунт не найден.")
            return
        ctx.logger.info("USER_REACTION_ACCOUNT_OPENED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s", tenant_id, rule_id, user_id, account_id)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_account_detail_text(account), reply_markup=build_rule_reaction_account_detail_keyboard(rule_id, account_id, str(account.get("status") or "")))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account_reactions:"))
    async def handle_user_rule_reactions_account_reactions(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        await ctx.answer_callback_safe_once(callback)
        if not account:
            await ctx.edit_message_text_safe(message=callback.message, text="Аккаунт не найден.")
            return
        ctx.user_states[user_id] = {
            "state": "reaction_account_reactions_wait_input",
            "flow": "user_rule_reactions",
            "rule_id": rule_id,
            "account_id": account_id,
            "tenant_id": tenant_id,
        }
        ctx.logger.info("USER_REACTION_ACCOUNT_REACTIONS_INPUT_OPENED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s", tenant_id, rule_id, user_id, account_id)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_reaction_account_reactions_text(account),
            reply_markup=build_reaction_account_reactions_keyboard(rule_id, account_id),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account_reactions_clear:"))
    async def handle_user_rule_reactions_account_reactions_clear(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        await ctx.run_db(ctx.db.update_reaction_account_fixed_reactions_for_tenant, tenant_id, account_id, [])
        ctx.user_states.pop(user_id, None)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        await ctx.answer_callback_safe_once(callback)
        if not account:
            await ctx.edit_message_text_safe(message=callback.message, text="Аккаунт не найден.")
            return
        ctx.logger.info("USER_REACTION_ACCOUNT_REACTIONS_CLEARED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s", tenant_id, rule_id, user_id, account_id)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_account_detail_text(account), reply_markup=build_rule_reaction_account_detail_keyboard(rule_id, account_id, str(account.get("status") or "")))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account_disable:"))
    async def handle_user_rule_reactions_account_disable(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        await ctx.run_db(ctx.db.update_reaction_account_status_for_tenant, tenant_id, account_id, "disabled", None)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        await ctx.answer_callback_safe_once(callback)
        if not account:
            await ctx.edit_message_text_safe(message=callback.message, text="Аккаунт не найден.")
            return
        ctx.logger.info("USER_REACTION_ACCOUNT_DISABLED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s", tenant_id, rule_id, user_id, account_id)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_account_detail_text(account), reply_markup=build_rule_reaction_account_detail_keyboard(rule_id, account_id, str(account.get("status") or "")))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account_enable:"))
    async def handle_user_rule_reactions_account_enable(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        await ctx.run_db(ctx.db.update_reaction_account_status_for_tenant, tenant_id, account_id, "active", None)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        await ctx.answer_callback_safe_once(callback)
        if not account:
            await ctx.edit_message_text_safe(message=callback.message, text="Аккаунт не найден.")
            return
        ctx.logger.info("USER_REACTION_ACCOUNT_ENABLED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s", tenant_id, rule_id, user_id, account_id)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_account_detail_text(account), reply_markup=build_rule_reaction_account_detail_keyboard(rule_id, account_id, str(account.get("status") or "")))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account_delete_confirm:"))
    async def handle_user_rule_reactions_account_delete_confirm(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        await ctx.answer_callback_safe_once(callback)
        if not account:
            await ctx.edit_message_text_safe(message=callback.message, text="Аккаунт не найден.")
            return
        ctx.logger.info("USER_REACTION_ACCOUNT_DELETE_CONFIRM_OPENED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s", tenant_id, rule_id, user_id, account_id)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_account_delete_confirm_text(account), reply_markup=build_rule_reaction_account_delete_confirm_keyboard(rule_id, account_id))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account_delete:"))
    async def handle_user_rule_reactions_account_delete(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        deleted = await ctx.run_db(ctx.db.delete_reaction_account_for_tenant, tenant_id, account_id)
        files_deleted = []
        if deleted and deleted.get("session_name"):
            files_deleted = auth_service.reaction_service.delete_reaction_account_session_files(tenant_id, str(deleted.get("session_name")))
        accounts = await ctx.run_db(ctx.db.list_reaction_accounts_for_tenant, tenant_id, False)
        ctx.logger.info("USER_REACTION_ACCOUNT_DELETED | tenant_id=%s | rule_id=%s | user_id=%s | account_id=%s | files_deleted=%s", tenant_id, rule_id, user_id, account_id, len(files_deleted))
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text="✅ Аккаунт-реактор удалён. Теперь его можно подключить заново.\n\n" + build_rule_reaction_accounts_text(accounts), reply_markup=build_rule_reaction_accounts_keyboard_with_items(rule_id, accounts))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_account_reconnect:"))
    async def handle_user_rule_reactions_account_reconnect(callback: CallbackQuery):
        _, rule_id_raw, account_id_raw = (callback.data or "0:0:0").split(":")
        rule_id = int(rule_id_raw)
        account_id = int(account_id_raw)
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        await ctx.answer_callback_safe_once(callback)
        if not account:
            await ctx.edit_message_text_safe(message=callback.message, text="Аккаунт не найден.")
            return
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="Для переподключения удалите аккаунт и добавьте его заново.",
            reply_markup=build_rule_reaction_account_delete_confirm_keyboard(rule_id, account_id),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_add_account:"))
    async def handle_user_rule_reactions_add_account(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        auth_service.cleanup_tmp_session(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id)
        ctx.user_states.pop(user_id, None)
        web_enabled = bool(settings.reaction_onboarding_enabled and settings.public_base_url and settings.reaction_onboarding_secret)
        onboarding_url = None
        if web_enabled:
            token = create_reaction_onboarding_token(
                tenant_id=tenant_id,
                user_id=user_id,
                rule_id=rule_id,
                secret=settings.reaction_onboarding_secret,
                ttl_sec=settings.reaction_onboarding_token_ttl_sec,
            )
            onboarding_url = f"{settings.public_base_url}{settings.reaction_onboarding_public_path}?token={token}"
        ctx.logger.info("USER_REACTION_ONBOARDING_OPENED | tenant_id=%s | rule_id=%s | user_id=%s | web_enabled=%s", tenant_id, rule_id, user_id, web_enabled)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=build_reaction_web_onboarding_text(rule_id, web_enabled=web_enabled),
            reply_markup=build_reaction_web_onboarding_keyboard(rule_id, onboarding_url=onboarding_url),
        )

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
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        accounts = await ctx.run_db(ctx.db.list_reaction_accounts_for_tenant, tenant_id, False)
        ctx.logger.info("USER_REACTION_PRESET_REDIRECT_TO_ACCOUNTS | tenant_id=%s | rule_id=%s | user_id=%s", tenant_id, rule_id, user_id)
        redirect_text = (
            "🎭 Набор реакций настраивается в карточке конкретного аккаунта-реактора.\n\n"
            "Выберите аккаунт ниже.\n\n"
            f"{build_rule_reaction_accounts_text(accounts)}"
        )
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=redirect_text,
            reply_markup=build_rule_reaction_accounts_keyboard_with_items(rule_id, accounts),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_reactions_test:"))
    async def handle_user_rule_reactions_test(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":", 1)[1])
        if not await ensure_rule_callback_access(ctx, callback, rule_id):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=build_rule_reaction_test_text(), reply_markup=build_rule_reaction_back_keyboard(rule_id))

    @dp.message(
        lambda m: m.from_user is not None
        and (ctx.user_states.get(m.from_user.id) or {}).get("state") == "reaction_account_reactions_wait_input"
        and (ctx.user_states.get(m.from_user.id) or {}).get("flow") == "user_rule_reactions"
    )
    async def handle_user_reaction_account_reactions_input(message: Message):
        user_id = message.from_user.id if message.from_user else 0
        state = ctx.user_states.get(user_id) or {}
        if str((message.text or "").strip()).lower() in {"/start", "/menu", "❌ отмена"}:
            ctx.user_states.pop(user_id, None)
            await message.answer("Действие отменено.")
            return
        text = (message.text or "").strip()
        if not text:
            await message.answer("Не удалось распознать реакции. Отправьте 1 emoji для обычного аккаунта или до 3 emoji для Premium.")
            return
        rule_id = int(state.get("rule_id") or 0)
        account_id = int(state.get("account_id") or 0)
        tenant_id = int(state.get("tenant_id") or 0)
        account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        if not account:
            ctx.user_states.pop(user_id, None)
            await message.answer("Аккаунт не найден.")
            return
        try:
            normalized = normalize_fixed_reactions_input(text, is_premium=bool(account.get("is_premium")))
        except ValueError:
            await message.answer("Не удалось распознать реакции. Отправьте 1 emoji для обычного аккаунта или до 3 emoji для Premium.")
            return
        await ctx.run_db(ctx.db.update_reaction_account_fixed_reactions_for_tenant, tenant_id, account_id, normalized)
        ctx.user_states.pop(user_id, None)
        updated_account = await ctx.run_db(ctx.db.get_reaction_account_for_tenant, tenant_id, account_id)
        if not updated_account:
            await message.answer("Аккаунт не найден.")
            return
        await message.answer(
            "✅ Набор реакций сохранён.\n\n" + build_rule_reaction_account_detail_text(updated_account),
            reply_markup=build_rule_reaction_account_detail_keyboard(rule_id, account_id, str(updated_account.get("status") or "")),
        )

    @dp.message(
        lambda m: m.from_user is not None
        and (ctx.user_states.get(m.from_user.id) or {}).get("state") in {"reaction_auth_wait_phone", "reaction_auth_wait_code", "reaction_auth_wait_password"}
        and (ctx.user_states.get(m.from_user.id) or {}).get("flow") == "user_rule_reactions"
    )
    async def handle_user_reaction_auth_messages(message: Message):
        user_id = message.from_user.id if message.from_user else 0
        state = ctx.user_states.get(user_id) or {}
        rule_id = int(state.get("rule_id") or 0)
        tenant_id = int(state.get("tenant_id") or 0)
        if rule_id and tenant_id:
            auth_service.cleanup_tmp_session(tenant_id=tenant_id, rule_id=rule_id, user_id=user_id)
        ctx.user_states.pop(user_id, None)
        await message.answer(
            "Подключение аккаунта через чат бота отключено из соображений безопасности. "
            "Откройте раздел «⚙️ Реакции» и используйте защищённую страницу подключения."
        )
