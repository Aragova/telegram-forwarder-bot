from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup


@dataclass(frozen=True)
class UserStatusHandlersContext:
    db: Any
    USER_TZ: Any
    run_db: Callable[..., Awaitable[Any]]
    ensure_user_tenant: Callable[..., Awaitable[int]]
    subscription_service: Any
    usage_service: Any
    limit_service: Any
    _is_admin_user: Callable[[Any], bool]
    answer_callback_safe: Callable[..., Awaitable[Any]]
    answer_callback_safe_once: Callable[..., Awaitable[Any]]
    edit_message_text_safe: Callable[..., Awaitable[Any]]
    write_billing_event: Callable[..., Any]
    is_subscription_blocked_status: Callable[[str], bool]


def register_user_status_handlers(dp: Dispatcher, ctx: UserStatusHandlersContext) -> None:
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
        stats = await ctx.run_db(ctx.db.get_tenant_queue_stats, tenant_id)
        active_rules = int(stats["active_rules"])
        jobs_today = int((usage_today or {}).get("jobs_count") or 0)
        queue_total = int(stats["pending"])
        errors_total = int(stats["faulty"])
        next_run = stats["next_run_at"]
        next_publication = str(next_run)[11:16] if next_run else "—"
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
            f"🕒 Обновлено: {datetime.now(ctx.USER_TZ).strftime('%H:%M')} (UTC+3)\n"
            f"Следующая публикация: {next_publication}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="user_status")],
            [InlineKeyboardButton(text="⚙️ Мои правила", callback_data="user_rules"), InlineKeyboardButton(text="📡 Мои каналы", callback_data="user_channels")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="user_main")],
        ])
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
