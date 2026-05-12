from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app import user_ui


@dataclass(frozen=True)
class UserMenuHandlersContext:
    db: Any
    logger: Any
    run_db: Callable[..., Awaitable[Any]]
    ensure_user_tenant: Callable[..., Awaitable[int]]
    _is_admin_user: Callable[[Any], bool]
    answer_callback_safe: Callable[..., Awaitable[Any]]
    answer_callback_safe_once: Callable[..., Awaitable[Any]]
    edit_message_text_safe: Callable[..., Awaitable[Any]]
    public_user_menu_text: Callable[[dict[str, Any]], str]
    public_user_menu_keyboard: Callable[[], InlineKeyboardMarkup]
    build_user_main_payload: Callable[[int], Awaitable[dict[str, Any]]]
    user_sources_keyboard: Callable[[], InlineKeyboardMarkup]
    user_targets_keyboard: Callable[[], InlineKeyboardMarkup]


def register_user_menu_handlers(dp: Dispatcher, ctx: UserMenuHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data == "user_main")
    async def handle_user_main_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        payload = await ctx.build_user_main_payload(callback.from_user.id if callback.from_user else 0)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=ctx.public_user_menu_text(payload),
            reply_markup=ctx.public_user_menu_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_channels")
    async def handle_user_channels_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        source_rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, "source") if hasattr(ctx.db, "get_channels_for_tenant") else []
        target_rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, "target") if hasattr(ctx.db, "get_channels_for_tenant") else []
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_channels_text(sources_count=len(source_rows), targets_count=len(target_rows)),
            reply_markup=user_ui.build_user_channels_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_sources")
    async def handle_user_sources_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        ctx.logger.info("пользователь открыл список источников user_id=%s tenant_id=%s", user_id, tenant_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="📡 Источники\n\nВыберите действие:",
            reply_markup=ctx.user_sources_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_targets")
    async def handle_user_targets_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        ctx.logger.info("пользователь открыл список получателей user_id=%s tenant_id=%s", user_id, tenant_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="🎯 Получатели\n\nВыберите действие:",
            reply_markup=ctx.user_targets_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_sources_list")
    async def handle_user_sources_list_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, "source") if hasattr(ctx.db, "get_channels_for_tenant") else []
        await ctx.answer_callback_safe_once(callback)
        if not rows:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="📡 Источники\n\nСписок пока пуст.",
                reply_markup=ctx.user_sources_keyboard(),
            )
            return
        lines = ["📡 Источники\n"]
        for idx, row in enumerate(rows, 1):
            title = row["title"] or row["channel_id"]
            suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
            lines.append(f"{idx}. {title}{suffix}")
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="\n".join(lines),
            reply_markup=ctx.user_sources_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_targets_list")
    async def handle_user_targets_list_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, "target") if hasattr(ctx.db, "get_channels_for_tenant") else []
        await ctx.answer_callback_safe_once(callback)
        if not rows:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="🎯 Получатели\n\nСписок пока пуст.",
                reply_markup=ctx.user_targets_keyboard(),
            )
            return
        lines = ["🎯 Получатели\n"]
        for idx, row in enumerate(rows, 1):
            title = row["title"] or row["channel_id"]
            suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
            lines.append(f"{idx}. {title}{suffix}")
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="\n".join(lines),
            reply_markup=ctx.user_targets_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_account")
    async def handle_user_account_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=(
                "Этот раздел больше не используется.\n\n"
                "Управление тарифом и оплатой теперь находится в разделе:\n\n"
                "💎 Подписка"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Подписка", callback_data="user_subscription")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="user_main")],
                ]
            ),
        )

    @dp.callback_query(lambda c: c.data == "user_plans")
    async def handle_user_plans_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=(
                "Этот раздел больше не используется.\n\n"
                "Управление тарифом и оплатой теперь находится в разделе:\n\n"
                "💎 Подписка"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Подписка", callback_data="user_subscription")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="user_main")],
                ]
            ),
        )
