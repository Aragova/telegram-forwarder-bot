from __future__ import annotations

from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

PAYMENT_PROVIDER_TITLES_RU: dict[str, str] = {
    "telegram_stars": "⭐ Telegram Stars",
    "telegram_payments": "💳 Telegram Payments",
    "paypal": "PayPal",
    "card_provider": "💳 Банковская карта",
    "manual_bank_card": "💳 Банковская карта",
    "sbp_provider": "⚡ СБП",
    "crypto_manual": "₿ Криптовалюта",
    "tribute": "Tribute",
    "lava_top": "Lava.top",
}

MANUAL_PAYMENT_PROVIDERS = {"manual_bank_card", "card_provider", "sbp_provider", "crypto_manual"}


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
            [InlineKeyboardButton(text="📊 Статус оплаты", callback_data=f"user_payment_status:{int(invoice_id)}")],
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


def payment_provider_title(provider: str) -> str:
    key = str(provider or "")
    return PAYMENT_PROVIDER_TITLES_RU.get(key, key)


def build_user_payment_methods_text(invoice: dict[str, Any], methods: list[dict[str, Any]]) -> str:
    invoice_id = int(invoice.get("id") or 0)
    if not methods:
        return (
            f"💳 Оплата счёта #{invoice_id}\n\n"
            "Сейчас нет доступных способов оплаты.\n"
            "Попробуйте позже или обратитесь в поддержку."
        )
    total = float(invoice.get("total") or 0)
    currency = str(invoice.get("currency") or "USD").upper()
    status = str(invoice.get("status") or "draft")
    return (
        f"💳 Оплата счёта #{invoice_id}\n\n"
        f"Сумма: {total:.0f} {currency}\n"
        f"Статус счёта: {status}\n\n"
        "Выберите способ оплаты:"
    )


def build_user_payment_methods_keyboard(invoice_id: int, methods: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for method in methods:
        provider = str(method.get("provider") or "")
        if not provider:
            continue
        rows.append([InlineKeyboardButton(text=payment_provider_title(provider), callback_data=f"user_pay_provider:{int(invoice_id)}:{provider}")])
    rows.extend(
        [
            [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_invoices")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_user_payment_result_text(invoice: dict[str, Any], payment_result: dict[str, Any]) -> str:
    invoice_id = int(invoice.get("id") or 0)
    provider_title = payment_provider_title(str(payment_result.get("provider") or ""))
    status = str(payment_result.get("status") or "created")
    lines = [
        "💳 Оплата создана",
        "",
        f"Счёт: #{invoice_id}",
        f"Способ: {provider_title}",
        f"Статус: {status}",
    ]
    checkout_url = str(payment_result.get("checkout_url") or "").strip()
    message_ru = str(payment_result.get("message_ru") or "").strip()
    if checkout_url:
        lines.extend(["", "Перейдите по ссылке для оплаты."])
    elif message_ru:
        lines.extend(["", message_ru])
    payload = payment_result.get("payload") or {}
    if isinstance(payload, dict):
        instruction = str(payload.get("instruction_ru") or payload.get("instructions_ru") or "").strip()
        if instruction:
            lines.extend(["", instruction])
    return "\n".join(lines)


def build_user_payment_result_keyboard(invoice_id: int, payment_result: dict[str, Any]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    checkout_url = str(payment_result.get("checkout_url") or "").strip()
    provider = str(payment_result.get("provider") or "")
    if checkout_url:
        rows.append([InlineKeyboardButton(text="Открыть оплату", url=checkout_url)])
    if provider in MANUAL_PAYMENT_PROVIDERS:
        rows.append([InlineKeyboardButton(text="📎 Прикрепить чек оплаты", callback_data=f"user_upload_receipt:{int(invoice_id)}")])
    rows.append([InlineKeyboardButton(text="📊 Статус оплаты", callback_data=f"user_payment_status:{int(invoice_id)}")])
    rows.extend(
        [
            [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_user_manual_receipt_keyboard(invoice_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📎 Прикрепить чек оплаты", callback_data=f"user_upload_receipt:{int(invoice_id)}")],
            [InlineKeyboardButton(text="📊 Статус оплаты", callback_data=f"user_payment_status:{int(invoice_id)}")],
            [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
        ]
    )


def build_user_payment_status_keyboard(invoice_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 К оплате", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
            [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_invoices")],
        ]
    )


def build_user_manual_receipt_request_text(invoice: dict[str, Any], payment_intent: dict[str, Any]) -> str:
    invoice_id = int(invoice.get("id") or 0)
    intent_id = int(payment_intent.get("id") or 0)
    provider_title = payment_provider_title(str(payment_intent.get("provider") or ""))
    return (
        "📎 Подтверждение ручной оплаты\n\n"
        f"Счёт: #{invoice_id}\n"
        f"Payment intent: #{intent_id}\n"
        f"Способ: {provider_title}\n\n"
        "Прикрепите чек оплаты файлом или фотографией.\n"
        "Поддерживаются: фото, PDF, JPG, PNG, WEBP.\n"
        "После загрузки чека появится кнопка ✅ Я оплатил."
    )


def build_user_manual_receipt_uploaded_text(invoice: dict[str, Any], payment_intent: dict[str, Any]) -> str:
    invoice_id = int(invoice.get("id") or 0)
    intent_id = int(payment_intent.get("id") or 0)
    return (
        "✅ Чек прикреплён\n\n"
        f"Счёт: #{invoice_id}\n"
        f"Payment intent: #{intent_id}\n\n"
        "Теперь нажмите «✅ Я оплатил», чтобы отправить заявку администратору."
    )


def build_user_payment_status_text(invoice: dict[str, Any], payment_intent: dict[str, Any] | None) -> str:
    invoice_id = int(invoice.get("id") or 0)
    invoice_status = str(invoice.get("status") or "draft")
    if not payment_intent:
        return (
            "📊 Статус оплаты\n\n"
            f"Счёт: #{invoice_id}\n"
            f"Статус счёта: {invoice_status}\n\n"
            "Оплата ещё не создавалась."
        )
    intent_id = int(payment_intent.get("id") or 0)
    provider_title = payment_provider_title(str(payment_intent.get("provider") or ""))
    payment_status = str(payment_intent.get("status") or "created")
    payload = payment_intent.get("confirmation_payload_json") if isinstance(payment_intent.get("confirmation_payload_json"), dict) else {}
    user_payload_status = str(payload.get("status") or "")
    description = "💳 Оплата создана"
    if payment_status == "waiting_confirmation":
        description = "⏳ Оплата ожидает проверки"
    if user_payload_status == "submitted_by_user":
        description = "📨 Чек отправлен администратору"
    if payment_status == "paid":
        description = "✅ Оплата подтверждена"
    if payment_status == "failed":
        description = "❌ Оплата отклонена"
    return (
        "📊 Статус оплаты\n\n"
        f"Счёт: #{invoice_id}\n"
        f"Payment intent: #{intent_id}\n"
        f"Способ: {provider_title}\n"
        f"Статус счёта: {invoice_status}\n"
        f"Статус оплаты: {payment_status}\n\n"
        f"{description}"
    )
