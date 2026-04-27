from __future__ import annotations

from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def build_user_main_text() -> str:
    return (
        "👋 Добро пожаловать в ViMi\n\n"
        "ViMi помогает автоматизировать Telegram-каналы: источники, получатели, правила публикации, очередь и статус — внутри Telegram.\n\n"
        "Выберите действие:"
    )


def build_user_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📡 Источники", callback_data="user_sources")],
            [InlineKeyboardButton(text="🎯 Получатели", callback_data="user_targets")],
            [InlineKeyboardButton(text="⚙️ Мои правила", callback_data="user_rules")],
            [InlineKeyboardButton(text="📊 Статус", callback_data="user_status")],
            [InlineKeyboardButton(text="👤 Мой аккаунт", callback_data="user_account")],
            [InlineKeyboardButton(text="💎 Тарифы", callback_data="user_plans")],
            [InlineKeyboardButton(text="🧾 Мои счета", callback_data="user_invoices")],
            [InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],
        ]
    )


def build_user_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")]])


def build_user_account_text(
    *,
    user_id: int,
    tenant_id: int,
    subscription: dict[str, Any] | None,
    usage_today: dict[str, Any] | None,
    rules_count: int,
) -> str:
    sub = subscription or {}
    usage = usage_today or {}
    plan_name = str(sub.get("plan_name") or "FREE").upper()
    status = str(sub.get("status") or "active")
    max_rules = int(sub.get("max_rules") or 0)
    max_video = int(sub.get("max_video_per_day") or 0)
    max_jobs = int(sub.get("max_jobs_per_day") or 0)
    video_today = int(usage.get("video_count") or 0)
    jobs_today = int(usage.get("jobs_count") or 0)
    return (
        "👤 Мой аккаунт\n\n"
        f"Telegram ID: {int(user_id)}\n"
        f"Аккаунт: #{int(tenant_id)}\n\n"
        f"Тариф: {plan_name}\n"
        f"Статус: {status}\n\n"
        "Лимиты:\n"
        f"📌 Правила: {int(rules_count)} / {max_rules}\n"
        f"🎬 Видео сегодня: {video_today} / {max_video}\n"
        f"📦 Задачи сегодня: {jobs_today} / {max_jobs}"
    )


def build_user_account_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💎 Сменить тариф", callback_data="user_plans")],
            [InlineKeyboardButton(text="🧾 Мои счета", callback_data="user_invoices")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
        ]
    )


def build_user_plans_text(plans: list[dict[str, Any]]) -> str:
    lines = ["💎 Тарифы\n"]
    for plan in plans:
        lines.extend(
            [
                f"{plan['name']}",
                f"{plan['description']}",
                f"📌 Правила: {int(plan.get('max_rules') or 0)}",
                f"🎬 Видео/день: {int(plan.get('max_video_per_day') or 0)}",
                f"📦 Задачи/день: {int(plan.get('max_jobs_per_day') or 0)}",
                f"💰 Цена: ${float(plan.get('price') or 0):.0f}/мес",
                "",
            ]
        )
    return "\n".join(lines).strip()


def build_user_plans_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="BASIC — выбрать", callback_data="user_select_plan:BASIC")],
            [InlineKeyboardButton(text="PRO — выбрать", callback_data="user_select_plan:PRO")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
        ]
    )


def build_user_sources_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📜 Мои источники", callback_data="user_sources_list")],
            [InlineKeyboardButton(text="➕ Добавить источник", callback_data="user_sources_add")],
            [InlineKeyboardButton(text="➖ Удалить источник", callback_data="user_sources_remove")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
        ]
    )


def build_user_targets_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📜 Мои получатели", callback_data="user_targets_list")],
            [InlineKeyboardButton(text="➕ Добавить получатель", callback_data="user_targets_add")],
            [InlineKeyboardButton(text="➖ Удалить получатель", callback_data="user_targets_remove")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
        ]
    )
