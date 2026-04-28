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


def build_button(
    text: str,
    *,
    callback_data: str | None = None,
    url: str | None = None,
    style: str | None = None,
) -> InlineKeyboardButton:
    payload: dict[str, Any] = {"text": text}
    if callback_data:
        payload["callback_data"] = callback_data
    if url:
        payload["url"] = url
    if style:
        try:
            return InlineKeyboardButton(**payload, style=style)
        except Exception:
            pass
    return InlineKeyboardButton(**payload)


def build_user_main_text() -> str:
    return (
        "✨ ViMi — автоматизация Telegram-каналов\n\n"
        "Ваш центр управления публикациями, видео, правилами и расписанием.\n\n"
        "Тариф: FREE\n"
        "Статус: 🟢 активен\n\n"
        "Выберите раздел:"
    )


def build_user_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                build_button(text="👤 Мой аккаунт", callback_data="user_account"),
                build_button(text="⚙️ Мои правила", callback_data="user_rules"),
            ],
            [
                build_button(text="📡 Мои каналы", callback_data="user_channels"),
                build_button(text="🌐 Language", callback_data="user_language", style="danger"),
            ],
            [
                build_button(text="📊 Живой статус", callback_data="user_status"),
                build_button(text="🌍 TimeZone", callback_data="user_timezone"),
            ],
            [
                build_button(text="🆘 Поддержка", callback_data="user_support", style="primary"),
                build_button(text="📘 Инструкция", callback_data="user_help"),
            ],
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
    status_label = "Активен" if status in {"active", "trial", "grace"} else "Неактивен"
    return (
        "👤 Мой аккаунт\n\n"
        f"💎 Тариф: {plan_name}\n"
        f"📊 Статус: {status_label}\n\n"
        "──────────────\n\n"
        f"📦 Правил: {int(rules_count)} / {max_rules if max_rules > 0 else '∞'}\n"
        f"🎬 Видео: {video_today} / {max_video if max_video > 0 else '∞'}\n"
        f"📤 Публикации: {jobs_today} / {max_jobs if max_jobs > 0 else '∞'}"
    )


def build_user_usage_text(
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
        "📊 Использование\n\n"
        f"Тариф: {plan_name}\n"
        f"Статус: {status}\n\n"
        "Лимиты:\n"
        f"📌 Правила: {int(rules_count)} / {max_rules}\n"
        f"🎬 Видео сегодня: {video_today} / {max_video}\n"
        f"📦 Задачи сегодня: {jobs_today} / {max_jobs}"
    )


def build_user_limit_block_text(
    subscription: dict[str, Any] | None,
    usage_today: dict[str, Any] | None,
    rules_count: int,
) -> str:
    return (
        "🚫 Лимит тарифа достигнут\n\n"
        + build_user_usage_text(subscription, usage_today, rules_count)
        + "\n\nЧтобы добавить больше правил, смените тариф."
    )


def build_user_limit_exceeded_text(
    reason: str | None,
    subscription: dict[str, Any] | None,
    usage_today: dict[str, Any] | None,
    rules_count: int,
) -> str:
    return (
        "🚫 Лимит тарифа достигнут\n\n"
        f"Причина: {str(reason or 'Лимит тарифа достигнут')}\n\n"
        + build_user_usage_text(subscription, usage_today, rules_count)
        + "\n\nЧтобы продолжить работу, смените тариф."
    )


def build_user_subscription_blocked_text(subscription: dict[str, Any] | None) -> str:
    plan_name = str((subscription or {}).get("plan_name") or "FREE").upper()
    status = str((subscription or {}).get("status") or "expired")
    return (
        "🔒 Подписка неактивна\n\n"
        "Чтобы продолжить пользоваться автоматизацией, выберите тариф и оплатите счёт.\n\n"
        f"Тариф: {plan_name}\n"
        f"Статус: {status}"
    )


def build_user_usage_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                build_button(text="💎 Сменить тариф", callback_data="user_plans", style="primary"),
                build_button(text="🧾 Мои счета", callback_data="user_invoices"),
            ],
            [build_button(text="⬅️ Главное меню", callback_data="user_main")],
        ]
    )


def build_user_account_keyboard() -> InlineKeyboardMarkup:
    return build_user_usage_keyboard()


def build_user_recovery_summary_text(summary: dict[str, Any]) -> str:
    active_rules_count = int(summary.get("active_rules_count") or 0)
    pending_deliveries_count = int(summary.get("pending_deliveries_count") or 0)
    failed_limit_video_jobs_count = int(summary.get("failed_limit_video_jobs_count") or 0)
    blocked_events_count = len(summary.get("last_blocked_events") or [])
    failed_limit_jobs_count = int(summary.get("failed_limit_jobs_count") or 0)
    if pending_deliveries_count <= 0 and failed_limit_video_jobs_count <= 0 and failed_limit_jobs_count <= 0:
        return (
            "✅ Всё в порядке\n\n"
            "Заблокированных задач не найдено.\n"
            "Ваши активные правила продолжат работу по расписанию."
        )
    return (
        "🔄 Восстановление работы\n\n"
        "После оплаты можно вернуть в работу публикации и видео, которые были остановлены из-за лимитов или неактивной подписки.\n\n"
        "Найдено:\n"
        f"📌 Активных правил: {active_rules_count}\n"
        f"📦 Ожидающих публикаций: {pending_deliveries_count}\n"
        f"🎬 Видео-задач после лимита: {failed_limit_video_jobs_count}\n"
        f"⚠️ Событий блокировки: {blocked_events_count}"
    )


def build_user_recovery_result_text(result: dict[str, Any]) -> str:
    if not bool(result.get("ok")):
        return f"⛔ Восстановление недоступно\n\n{str(result.get('reason') or 'Подписка ещё не активна')}"
    if bool(result.get("already_recovered")) and int(result.get("restored_jobs") or 0) == 0:
        return "✅ Всё уже восстановлено"
    return (
        "✅ Доступ восстановлен\n\n"
        f"📌 Правил проверено: {int(result.get('checked_rules') or 0)}\n"
        f"📦 Восстановлено задач: {int(result.get('restored_jobs') or 0)}\n"
        f"⏳ Ожидающих публикаций: {int(result.get('pending_deliveries') or 0)}\n"
        f"⚠️ Событий лимита найдено: {int(result.get('limit_events_found') or 0)}"
    )


def build_user_recovery_keyboard(can_recover: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_recover:
        rows.append([InlineKeyboardButton(text="🔄 Восстановить работу", callback_data="user_recovery_run")])
    rows.extend(
        [
            [InlineKeyboardButton(text="⚙️ Мои правила", callback_data="user_rules")],
            [InlineKeyboardButton(text="📊 Статус", callback_data="user_status")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_user_plans_text(plans: list[dict[str, Any]]) -> str:
    lines = ["💎 Тарифы ViMi", "", "Ваш текущий тариф: FREE", "", "──────────────", ""]
    for plan in plans:
        plan_name = str(plan.get("name") or "").upper()
        if plan_name == "OWNER":
            continue
        if plan_name == "FREE":
            lines.extend(
                [
                    "FREE",
                    "• до 3 правил",
                    "• до 5 видео в день",
                    "• до 100 публикаций в день",
                    "",
                ]
            )
            continue
        price = float(plan.get("price") or 0)
        title = f"{plan_name} — {price:.0f} USD / месяц"
        use_case = "• подходит для стабильной автопубликации" if plan_name == "BASIC" else "• подходит для больших каналов и видео-потоков"
        lines.extend(
            [
                title,
                f"📌 Правила: {int(plan.get('max_rules') or 0)}",
                f"🎬 Видео/день: {int(plan.get('max_video_per_day') or 0)}",
                f"📦 Публикации/день: {int(plan.get('max_jobs_per_day') or 0)}",
                use_case,
                "",
            ]
        )
    return "\n".join(lines).strip()


def build_user_plans_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [build_button(text="Выбрать BASIC", callback_data="user_select_plan:BASIC", style="primary")],
            [build_button(text="Выбрать PRO", callback_data="user_select_plan:PRO", style="primary")],
            [build_button(text="⬅️ Назад", callback_data="user_account")],
        ]
    )


def build_user_channels_text(*, sources_count: int = 0, targets_count: int = 0) -> str:
    return (
        "📡 Мои каналы\n\n"
        "Здесь находятся ваши источники и получатели.\n\n"
        "Источник — откуда ViMi берёт публикации.\n"
        "Получатель — куда ViMi отправляет публикации.\n\n"
        f"Источники: {int(sources_count)}\n"
        f"Получатели: {int(targets_count)}"
    )


def build_user_channels_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [build_button(text="📤 Источники", callback_data="user_sources"), build_button(text="📥 Получатели", callback_data="user_targets")],
            [build_button(text="➕ Добавить канал", callback_data="user_sources_add", style="primary"), build_button(text="➖ Удалить канал", callback_data="user_sources_remove")],
            [build_button(text="⬅️ Главное меню", callback_data="user_main")],
        ]
    )


def build_user_sources_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📜 Мои источники", callback_data="user_sources_list")],
            [InlineKeyboardButton(text="➕ Добавить источник", callback_data="user_sources_add")],
            [InlineKeyboardButton(text="➖ Удалить источник", callback_data="user_sources_remove")],
            [build_button(text="⬅️ Главное меню", callback_data="user_main")],
        ]
    )


def build_user_targets_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📜 Мои получатели", callback_data="user_targets_list")],
            [InlineKeyboardButton(text="➕ Добавить получатель", callback_data="user_targets_add")],
            [InlineKeyboardButton(text="➖ Удалить получатель", callback_data="user_targets_remove")],
            [build_button(text="⬅️ Главное меню", callback_data="user_main")],
        ]
    )


def build_user_timezone_text(current_tz: str = "Europe/Moscow", utc_label: str = "UTC+3") -> str:
    return (
        "🌍 TimeZone\n\n"
        f"Текущий часовой пояс:\n{current_tz} · {utc_label}\n\n"
        "Он используется для:\n"
        "• строки “Ждёт до”;\n"
        "• фиксированного времени публикаций;\n"
        "• живого статуса;\n"
        "• логов и истории."
    )


def build_user_timezone_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [build_button(text="UTC+3 Москва", callback_data="user_timezone:set:Europe/Moscow")],
        [build_button(text="UTC+2 Европа", callback_data="user_timezone:set:Europe/Kaliningrad")],
        [build_button(text="UTC+1 Центральная Европа", callback_data="user_timezone:set:Europe/Berlin")],
        [build_button(text="UTC+0 Лондон", callback_data="user_timezone:set:Europe/London")],
        [build_button(text="✏️ Ввести вручную", callback_data="user_timezone_manual")],
        [build_button(text="⬅️ Главное меню", callback_data="user_main")],
    ])


def build_user_language_text() -> str:
    return "🌐 Interface language\n\nChoose your language:"


def build_user_language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [build_button(text="🇷🇺 Русский", callback_data="user_set_lang:ru"), build_button(text="🇺🇸 English", callback_data="user_set_lang:en")],
        [build_button(text="🇪🇸 Español", callback_data="user_set_lang:es"), build_button(text="🇩🇪 Deutsch", callback_data="user_set_lang:de")],
        [build_button(text="⬅️ Main menu", callback_data="user_main")],
    ])


def build_user_support_text() -> str:
    return (
        "🆘 Поддержка ViMi\n\n"
        "Мы поможем с настройкой каналов, правилами, оплатой и ошибками публикации.\n\n"
        "Перед обращением подготовьте:\n"
        "• номер правила, если вопрос по публикации;\n"
        "• номер счёта, если вопрос по оплате;\n"
        "• скрин ошибки, если она есть.\n\n"
        "Нажмите кнопку ниже, чтобы открыть поддержку."
    )


def build_user_support_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [build_button(text="💬 Открыть поддержку", url="https://t.me/vimi_support_bot", style="primary")],
        [build_button(text="⬅️ Главное меню", callback_data="user_main")],
    ])


def build_user_help_text() -> str:
    return "📘 Инструкция ViMi\n\nВыберите раздел:"


def build_user_help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [build_button(text="📡 Каналы", callback_data="user_help:channels")],
        [build_button(text="⚙️ Правила", callback_data="user_help:rules")],
        [build_button(text="🔁 Режимы", callback_data="user_help:modes")],
        [build_button(text="🕒 Расписание", callback_data="user_help:schedule")],
        [build_button(text="🎬 Видеоредактор", callback_data="user_help:video")],
        [build_button(text="💳 Оплата", callback_data="user_help:payment")],
        [build_button(text="⬅️ Главное меню", callback_data="user_main")],
    ])


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
    if checkout_url:
        rows.append([InlineKeyboardButton(text="Открыть оплату", url=checkout_url)])
    provider = str(payment_result.get("provider") or "")
    payment_status = str(payment_result.get("status") or "")
    if provider in MANUAL_PAYMENT_PROVIDERS and payment_status in {"created", "pending", "waiting_confirmation"}:
        rows.append([InlineKeyboardButton(text="📤 Прикрепить чек", callback_data=f"user_upload_receipt:{int(invoice_id)}")])
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
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"user_manual_paid:{int(invoice_id)}")],
            [InlineKeyboardButton(text="📊 Статус оплаты", callback_data=f"user_payment_status:{int(invoice_id)}")],
            [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
        ]
    )


def build_user_payment_status_keyboard(invoice_id: int, payment_intent: dict[str, Any] | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    payment_status = ""
    if payment_intent:
        provider = str(payment_intent.get("provider") or "")
        payment_status = str(payment_intent.get("status") or "")
        if provider in MANUAL_PAYMENT_PROVIDERS and payment_status in {"created", "pending", "waiting_confirmation"}:
            rows.append([InlineKeyboardButton(text="📤 Прикрепить чек", callback_data=f"user_upload_receipt:{int(invoice_id)}")])
    rows.extend(
        [
            [InlineKeyboardButton(text="💳 К оплате", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
            [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_invoices")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_user_manual_receipt_request_text(invoice: dict[str, Any], payment_intent: dict[str, Any]) -> str:
    invoice_id = int(invoice.get("id") or 0)
    intent_id = int(payment_intent.get("id") or 0)
    provider_title = payment_provider_title(str(payment_intent.get("provider") or ""))
    return (
        "💎 <b>Премиум-подтверждение оплаты</b>\n\n"
        f"🧾 <b>Счёт:</b> #{invoice_id}\n"
        f"🔖 <b>Payment intent:</b> #{intent_id}\n"
        f"💳 <b>Способ оплаты:</b> {provider_title}\n\n"
        "📤 <b>Загрузите чек</b> файлом или фотографией прямо в этот чат.\n"
        "🖼️ <b>Поддерживаются форматы:</b> фото, PDF, JPG, PNG, WEBP.\n\n"
        "✅ После загрузки нажмите кнопку <b>«Я оплатил»</b> — и мы сразу отправим заявку администратору."
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
