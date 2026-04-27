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


def build_user_invoice_text(invoice: dict[str, Any], items: list[dict[str, Any]]) -> str:
    invoice_id = int(invoice.get("id") or 0)
    status = str(invoice.get("status") or "draft")
    currency = str(invoice.get("currency") or "USD").upper()
    total = float(invoice.get("total") or 0)
    plan_name = "UNKNOWN"
    lines = [f"🧾 Счёт #{invoice_id}", ""]
    for item in items:
        meta = item.get("metadata_json") or {}
        if isinstance(meta, dict) and meta.get("plan_name"):
            plan_name = str(meta.get("plan_name")).upper()
            break
    lines.extend(
        [
            f"Тариф: {plan_name}",
            f"Сумма: {total:.0f} {currency}",
            f"Статус: {status}",
            "",
            "Позиции:",
        ]
    )
    if not items:
        lines.append("• Позиции пока отсутствуют")
    for item in items:
        description = str(item.get("description") or item.get("item_type") or "Позиция")
        amount = float(item.get("amount") or 0)
        item_currency = str(invoice.get("currency") or "USD").upper()
        lines.append(f"• {description} — {amount:.0f} {item_currency}")
    return "\n".join(lines)


def build_user_invoice_keyboard(invoice_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Перейти к оплате", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
            [InlineKeyboardButton(text="🧾 Мои счета", callback_data="user_invoices")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_plans")],
        ]
    )


def build_user_invoices_text(invoices: list[dict[str, Any]]) -> str:
    lines = ["🧾 Мои счета", ""]
    if not invoices:
        lines.extend(
            [
                "У вас пока нет счетов.",
                "Выберите тариф, чтобы создать счёт.",
            ]
        )
        return "\n".join(lines)
    for invoice in invoices:
        plan_name = "UNKNOWN"
        for item in invoice.get("items") or []:
            meta = item.get("metadata_json") or {}
            if isinstance(meta, dict) and meta.get("plan_name"):
                plan_name = str(meta.get("plan_name")).upper()
                break
        lines.append(
            f"#{int(invoice.get('id') or 0)} — {plan_name} — {float(invoice.get('total') or 0):.0f} {str(invoice.get('currency') or 'USD').upper()} — {str(invoice.get('status') or 'draft')}"
        )
    return "\n".join(lines)


def build_user_invoices_keyboard(invoices: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if not invoices:
        rows.append([InlineKeyboardButton(text="💎 Тарифы", callback_data="user_plans")])
    else:
        for invoice in invoices:
            invoice_id = int(invoice.get("id") or 0)
            rows.append([InlineKeyboardButton(text=f"Открыть счёт #{invoice_id}", callback_data=f"user_invoice:{invoice_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
