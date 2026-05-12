from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app import user_ui

@dataclass(frozen=True)
class UserMenuHandlersContext:
    db: Any
    logger: Any
    user_tz: Any
    user_states: dict[int, dict[str, Any]]
    run_db: Callable[..., Awaitable[Any]]
    ensure_user_tenant: Callable[..., Awaitable[int]]
    subscription_service: Any
    usage_service: Any
    limit_service: Any
    _is_admin_user: Callable[[Any], bool]
    answer_callback_safe: Callable[..., Awaitable[Any]]
    answer_callback_safe_once: Callable[..., Awaitable[Any]]
    edit_message_text_safe: Callable[..., Awaitable[Any]]
    public_user_menu_text: Callable[[dict[str, Any]], str]
    public_user_menu_keyboard: Callable[[], InlineKeyboardMarkup]
    build_user_main_payload: Callable[[int], Awaitable[dict[str, Any]]]
    user_sources_keyboard: Callable[[], InlineKeyboardMarkup]
    user_targets_keyboard: Callable[[], InlineKeyboardMarkup]
    write_billing_event: Callable[..., Awaitable[Any]]
    is_subscription_blocked_status: Callable[[str], bool]


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

    @dp.callback_query(lambda c: c.data == "user_status")
    async def handle_user_status_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        sub = await ctx.run_db(ctx.subscription_service.get_active_subscription, tenant_id) or {}
        status = str(sub.get("status") or "active")
        usage_today = await ctx.run_db(ctx.usage_service.get_today_usage, tenant_id)
        rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id) if hasattr(ctx.db, "get_rules_for_tenant") else []
        active_rules = sum(1 for row in rules if bool(getattr(row, "is_active", False)))
        rule_limit = int(sub.get("max_rules") or 0)
        max_video = int(sub.get("max_video_per_day") or 0)
        max_jobs = int(sub.get("max_jobs_per_day") or 0)
        video_today = int((usage_today or {}).get("video_count") or 0)
        jobs_today = int((usage_today or {}).get("jobs_count") or 0)
        queue_total = 0
        errors_total = 0
        next_publication = "—"
        for row in rules:
            snapshot = await ctx.run_db(ctx.db.get_rule_card_snapshot, int(getattr(row, "id", 0)))
            if not snapshot:
                continue
            queue_total += int(snapshot.get("pending_count") or 0)
            errors_total += int(snapshot.get("faulty_count") or 0)
            next_run = snapshot.get("next_run_at")
            if next_run and next_publication == "—":
                next_publication = str(next_run)[11:16]
        state_line = "🟢 Доступ активен"
        if status == "grace":
            state_line = "⚠️ Льготный период"
            await ctx.run_db(ctx.write_billing_event, tenant_id, "subscription_grace_warning_shown", action="user_status", plan_name=str(sub.get("plan_name") or "FREE"), usage_today=usage_today)
        elif ctx.is_subscription_blocked_status(status):
            state_line = "🔒 Подписка неактивна"
        can_rule, _rule_reason = await ctx.run_db(ctx.limit_service.can_create_rule, tenant_id)
        can_job, _job_reason = await ctx.run_db(ctx.limit_service.can_enqueue_job, tenant_id)
        can_video, _video_reason = await ctx.run_db(ctx.limit_service.can_process_video, tenant_id)
        if not (can_rule and can_job and can_video):
            state_line = "🚫 Лимит достигнут"
    
        text = (
            "📊 Живой статус\n\n"
            f"{state_line.replace('Доступ активен', 'Автоматизация работает')}\n\n"
            "──────────────\n\n"
            f"📦 В очереди: {queue_total}\n"
            f"⏳ В обработке: {active_rules}\n"
            f"✅ Отправлено сегодня: {jobs_today}\n"
            f"⚠️ Ошибки: {errors_total}\n\n"
            "──────────────\n\n"
            f"🕒 Обновлено: {datetime.now(ctx.user_tz).strftime('%H:%M')} (UTC+3)\n"
            f"Следующая публикация: {next_publication}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="user_status")],
            [InlineKeyboardButton(text="⚙️ Мои правила", callback_data="user_rules"), InlineKeyboardButton(text="📡 Мои каналы", callback_data="user_channels")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="user_main")],
        ])
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

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

