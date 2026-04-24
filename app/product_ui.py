from __future__ import annotations

from datetime import datetime
from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.i18n import t

PLAN_ORDER = ("FREE", "BASIC", "PRO")
PLAN_ICONS = {"FREE": "🆓", "BASIC": "🚀", "PRO": "👑", "OWNER": "👑"}


def _fmt_period(date_from: str | None, date_to: str | None, lang: str) -> str:
    if not date_from or not date_to:
        return "—"
    try:
        d1 = datetime.fromisoformat(str(date_from)[:10])
        d2 = datetime.fromisoformat(str(date_to)[:10])
    except Exception:
        return f"{date_from} — {date_to}"
    if lang == "en":
        return f"{d1.strftime('%b %-d, %Y')} — {d2.strftime('%b %-d, %Y')}"
    return f"{d1.strftime('%d.%m.%Y')} — {d2.strftime('%d.%m.%Y')}"


def _progress(used: int, limit: int) -> str:
    if limit <= 0:
        return "██████████ 100%"
    pct = max(0, min(100, int(round((used / limit) * 100))))
    filled = max(0, min(10, pct // 10))
    return f"{'█' * filled}{'░' * (10 - filled)} {pct}%"


def product_menu_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("💎 Тарифы" if lang == "ru" else "💎 Plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=("👤 Мой аккаунт" if lang == "ru" else "👤 My account"), callback_data="product:account")],
        [InlineKeyboardButton(text=("🌐 Язык" if lang == "ru" else "🌐 Language"), callback_data="product:language")],
    ])


def account_screen(*, lang: str, subscription: dict[str, Any], usage_today: dict[str, Any], usage_period: dict[str, Any], last_invoice: dict[str, Any] | None, rules_count: int) -> str:
    plan = str(subscription.get("plan_name") or "FREE").upper()
    status = str(subscription.get("status") or "active")
    jobs_limit = int(subscription.get("max_jobs_per_day") or 0)
    video_limit = int(subscription.get("max_video_per_day") or 0)
    rules_limit = int(subscription.get("max_rules") or 0)
    storage_limit = int(subscription.get("max_storage_mb") or 0)
    unlimited = plan == "OWNER"
    period = _fmt_period(subscription.get("current_period_start"), subscription.get("current_period_end"), lang)
    if unlimited:
        limits_text = "без ограничений" if lang == "ru" else "unlimited"
        rule_line = f"📋 {'Правила' if lang == 'ru' else 'Rules'}: {limits_text}"
        jobs_line = f"📨 {'Задачи сегодня' if lang == 'ru' else 'Jobs today'}: {limits_text}"
        video_line = f"🎬 {'Видео сегодня' if lang == 'ru' else 'Videos today'}: {limits_text}"
        storage_line = f"💾 {'Хранилище' if lang == 'ru' else 'Storage'}: {limits_text}"
    else:
        rule_line = f"📋 {'Правила' if lang == 'ru' else 'Rules'}: {rules_count} / {rules_limit}"
        jobs_line = f"📨 {'Задачи сегодня' if lang == 'ru' else 'Jobs today'}: {int(usage_today.get('jobs_count') or 0)} / {jobs_limit}"
        video_line = f"🎬 {'Видео сегодня' if lang == 'ru' else 'Videos today'}: {int(usage_today.get('video_count') or 0)} / {video_limit}"
        storage_line = f"💾 {'Хранилище' if lang == 'ru' else 'Storage'}: {int(usage_today.get('storage_used_mb') or 0)} MB / {storage_limit} MB"
    invoice_line = "—"
    if last_invoice:
        invoice_line = f"#{last_invoice.get('id')} · {last_invoice.get('status')} · {float(last_invoice.get('total') or 0):.2f} {last_invoice.get('currency') or 'USD'}"
    if lang == "en":
        return "\n".join([
            "👤 My account",
            "",
            f"💎 Plan: {plan}",
            f"📌 Status: {status}",
            f"📅 Period: {period}",
            "",
            "📊 Usage:",
            rule_line,
            jobs_line,
            video_line,
            storage_line,
            "",
            f"🧾 Last invoice: {invoice_line}",
        ])
    return "\n".join([
        "👤 Мой аккаунт",
        "",
        f"💎 Тариф: {plan}",
        f"📌 Статус: {status}",
        f"📅 Период: {period}",
        "",
        "📊 Использование:",
        rule_line,
        jobs_line,
        video_line,
        storage_line,
        "",
        f"🧾 Последний счёт: {invoice_line}",
    ])


def account_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("💎 Тарифы" if lang == "ru" else "💎 Plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=("📈 Использование" if lang == "ru" else "📈 Usage"), callback_data="product:usage")],
        [InlineKeyboardButton(text=("🧾 Счета" if lang == "ru" else "🧾 Invoices"), callback_data="product:invoice")],
        [InlineKeyboardButton(text=("🌐 Язык" if lang == "ru" else "🌐 Language"), callback_data="product:language")],
        [InlineKeyboardButton(text=("⬅️ Назад" if lang == "ru" else "⬅️ Back"), callback_data="product:menu")],
    ])


def plans_screen(*, lang: str, plans: list[dict[str, Any]]) -> str:
    blocks: list[str] = ["💎 Тарифы" if lang == "ru" else "💎 Plans", ""]
    for row in plans:
        name = str(row.get("name") or "").upper()
        if name == "OWNER":
            continue
        icon = PLAN_ICONS.get(name, "💠")
        desc = str(row.get("description") or "")
        label_rules = "Правила" if lang == "ru" else "Rules"
        label_videos = "Видео/день" if lang == "ru" else "Videos/day"
        label_jobs = "Задачи/день" if lang == "ru" else "Jobs/day"
        label_price = "Цена" if lang == "ru" else "Price"
        blocks.extend([
            f"{icon} {name}",
            desc,
            f"• {label_rules}: {row.get('max_rules')}",
            f"• {label_videos}: {row.get('max_video_per_day')}",
            f"• {label_jobs}: {row.get('max_jobs_per_day')}",
            f"• {label_price}: {float(row.get('price') or 0):.0f} USD",
            "",
        ])
    return "\n".join(blocks).strip()


def plans_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("🚀 Выбрать BASIC" if lang == "ru" else "🚀 Choose BASIC"), callback_data="plan_select:BASIC")],
        [InlineKeyboardButton(text=("👑 Выбрать PRO" if lang == "ru" else "👑 Choose PRO"), callback_data="plan_select:PRO")],
        [InlineKeyboardButton(text=("📊 Мой тариф" if lang == "ru" else "📊 My plan"), callback_data="product:account")],
        [InlineKeyboardButton(text=("⬅️ Назад" if lang == "ru" else "⬅️ Back"), callback_data="product:menu")],
    ])


def upgrade_confirm_screen(lang: str, plan: dict[str, Any]) -> str:
    plan_name = str(plan.get("name") or "PRO").upper()
    price = float(plan.get("price") or 0)
    if lang == "en":
        return "\n".join([
            f"{PLAN_ICONS.get(plan_name, '💎')} Upgrade to {plan_name}",
            "",
            "You will get:",
            f"• Up to {plan.get('max_rules')} rules",
            f"• Up to {plan.get('max_video_per_day')} videos/day",
            f"• Up to {plan.get('max_jobs_per_day')} jobs/day",
            "• Higher processing priority",
            "",
            f"Price: {price:.0f} USD / month",
            "",
            "Create invoice?",
        ])
    return "\n".join([
        f"{PLAN_ICONS.get(plan_name, '💎')} Переход на {plan_name}",
        "",
        "Вы получите:",
        f"• До {plan.get('max_rules')} правил",
        f"• До {plan.get('max_video_per_day')} видео в день",
        f"• До {plan.get('max_jobs_per_day')} задач в день",
        "• Повышенный приоритет обработки",
        "",
        f"Стоимость: {price:.0f} USD / месяц",
        "",
        "Создать счёт?",
    ])


def upgrade_confirm_keyboard(lang: str, plan_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("🧾 Создать счёт" if lang == "ru" else "🧾 Create invoice"), callback_data=f"plan_confirm:{plan_name}")],
        [InlineKeyboardButton(text=("⬅️ Назад" if lang == "ru" else "⬅️ Back"), callback_data="product:plans")],
    ])


def invoice_screen(*, lang: str, invoice: dict[str, Any], items: list[dict[str, Any]]) -> str:
    period = _fmt_period(invoice.get("period_start"), invoice.get("period_end"), lang)
    plan_name = str((items[0].get("metadata") or {}).get("plan_name") if items else "") or "UNKNOWN"
    lines = [f"• {item.get('description')} — {float(item.get('amount') or 0):.2f} {invoice.get('currency') or 'USD'}" for item in items] or ["• —"]
    if lang == "en":
        return "\n".join([
            f"🧾 Invoice #{invoice.get('id')}",
            "",
            f"📌 Status: {invoice.get('status')}",
            f"💎 Plan: {plan_name}",
            f"📅 Period: {period}",
            "",
            "Items:",
            *lines,
            "",
            f"Total: {float(invoice.get('total') or 0):.2f} {invoice.get('currency') or 'USD'}",
            "",
            "Payment will be connected in the next step.",
        ])
    return "\n".join([
        f"🧾 Счёт #{invoice.get('id')}",
        "",
        f"📌 Статус: {invoice.get('status')}",
        f"💎 Тариф: {plan_name}",
        f"📅 Период: {period}",
        "",
        "Позиции:",
        *lines,
        "",
        f"Итого: {float(invoice.get('total') or 0):.2f} {invoice.get('currency') or 'USD'}",
        "",
        "Оплата будет подключена следующим этапом.",
    ])


def invoice_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("💳 Оплатить" if lang == "ru" else "💳 Pay"), callback_data="invoice:pay")],
        [InlineKeyboardButton(text=("💎 Тарифы" if lang == "ru" else "💎 Plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=("⬅️ Назад" if lang == "ru" else "⬅️ Back"), callback_data="product:menu")],
    ])


def payment_stub_screen(lang: str) -> str:
    if lang == "en":
        return "💳 Payments are not connected yet\n\nThe invoice has been created and is ready for payment.\nThe next step is payment provider integration."
    return "💳 Оплата ещё не подключена\n\nСчёт создан и готов к оплате.\nСледующий этап — подключение платёжного провайдера."


def usage_screen(*, lang: str, today: dict[str, Any], period: dict[str, Any], limits: dict[str, Any]) -> str:
    jobs = int(today.get("jobs_count") or 0)
    videos = int(today.get("video_count") or 0)
    jobs_limit = int(limits.get("max_jobs_per_day") or 0)
    videos_limit = int(limits.get("max_video_per_day") or 0)
    storage = int(period.get("storage_used_mb") or 0)
    status = "OK" if lang == "en" else "всё в порядке"
    if lang == "en":
        return "\n".join([
            "📈 Usage",
            "",
            "Today:",
            f"📨 Jobs: {jobs} / {jobs_limit} {_progress(jobs, jobs_limit)}",
            f"🎬 Videos: {videos} / {videos_limit} {_progress(videos, videos_limit)}",
            "",
            "Billing period:",
            f"📨 Jobs: {int(period.get('jobs_count') or 0):,}",
            f"🎬 Videos: {int(period.get('video_count') or 0):,}",
            f"💾 Storage: {storage:,} MB",
            "",
            f"Status: {status}",
        ])
    return "\n".join([
        "📈 Использование",
        "",
        "Сегодня:",
        f"📨 Задачи: {jobs} / {jobs_limit} {_progress(jobs, jobs_limit)}",
        f"🎬 Видео: {videos} / {videos_limit} {_progress(videos, videos_limit)}",
        "",
        "Период:",
        f"📨 Задачи: {int(period.get('jobs_count') or 0):,}",
        f"🎬 Видео: {int(period.get('video_count') or 0):,}",
        f"💾 Хранилище: {storage:,} МБ",
        "",
        f"Статус: {status}",
    ])


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru")],
        [InlineKeyboardButton(text="🇬🇧 English", callback_data="lang:en")],
        [InlineKeyboardButton(text="🇪🇸 Español", callback_data="lang:es")],
        [InlineKeyboardButton(text="🇩🇪 Deutsch", callback_data="lang:de")],
        [InlineKeyboardButton(text="🇵🇹 Português", callback_data="lang:pt")],
    ])


def help_screen(lang: str) -> str:
    if lang == "en":
        return "❓ Help\n\nMain sections:\n📋 Rules — forwarding settings\n📡 Channels — sources and targets\n💎 Plans — limits and subscription\n📈 Usage — consumed resources\n🧾 Invoices — plan and overage invoices\n\nIf something does not work, open “Live status”."
    return "❓ Помощь\n\nОсновные разделы:\n📋 Правила — управление пересылкой\n📡 Каналы — источники и получатели\n💎 Тарифы — лимиты и подписка\n📈 Использование — сколько ресурсов уже потрачено\n🧾 Счета — счета за тариф и превышения\n\nЕсли что-то не работает — откройте “Живой статус”."


def start_screen(lang: str, is_new: bool) -> str:
    if not is_new:
        return "👋 С возвращением! Откройте меню аккаунта." if lang == "ru" else "👋 Welcome back! Open your account menu."
    if lang == "en":
        return "👋 Welcome to TopPoster\n\nI help you automatically forward posts, process videos and manage publishing.\n\nYou started with the FREE plan.\n\nWhat you can do:\n1. Add a source\n2. Add a target\n3. Create a rule\n4. Check your plan and limits"
    return "👋 Добро пожаловать в TopPoster\n\nЯ помогу автоматически пересылать посты, обрабатывать видео и управлять публикациями.\n\nВы начали с тарифа FREE.\n\nЧто можно сделать:\n1. Добавить источник\n2. Добавить получателя\n3. Создать правило\n4. Проверить тариф и лимиты"


def start_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("📡 Добавить канал" if lang == "ru" else "📡 Add channel"), callback_data="start:add_channel")],
        [InlineKeyboardButton(text=("🔄 Создать правило" if lang == "ru" else "🔄 Create rule"), callback_data="start:create_rule")],
        [InlineKeyboardButton(text=("💎 Тарифы" if lang == "ru" else "💎 Plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=("🌐 Язык" if lang == "ru" else "🌐 Language"), callback_data="product:language")],
    ])


def rule_limit_error(lang: str, plan_name: str, allowed_rules: int, created_rules: int) -> str:
    if lang == "en":
        return f"⚠️ Rule limit reached\n\nYour plan: {plan_name}\nAllowed rules: {allowed_rules}\nCreated rules: {created_rules}\n\nUpgrade to BASIC or PRO to add more rules."
    return f"⚠️ Лимит правил достигнут\n\nВаш тариф: {plan_name}\nДоступно правил: {allowed_rules}\nУже создано: {created_rules}\n\nЧтобы добавить больше правил, перейдите на BASIC или PRO."


def video_limit_error(lang: str, plan_name: str, used: int, limit: int) -> str:
    if lang == "en":
        return f"🎬 Daily video limit reached\n\nYour plan: {plan_name}\nVideos today: {used} / {limit}\n\nNew videos will be available after daily reset or after upgrading to PRO."
    return f"🎬 Лимит видео на сегодня исчерпан\n\nВаш тариф: {plan_name}\nВидео сегодня: {used} / {limit}\n\nНовые видео будут доступны после обновления дневного лимита или после перехода на PRO."


def limit_error_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("💎 Посмотреть тарифы" if lang == "ru" else "💎 View plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=("⬅️ Назад" if lang == "ru" else "⬅️ Back"), callback_data="product:menu")],
    ])


def build_upgrade_invoice_flow(*, plan_name: str, price: float) -> dict[str, Any]:
    return {
        "item_type": "base_plan",
        "description": f"Тариф {plan_name}",
        "quantity": 1,
        "unit_price": round(float(price), 2),
    }
