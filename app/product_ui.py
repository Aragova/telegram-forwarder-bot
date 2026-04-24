from __future__ import annotations

from datetime import datetime
from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

PLAN_ORDER = ("FREE", "BASIC", "PRO")
PLAN_ICONS = {"FREE": "🆓", "BASIC": "🚀", "PRO": "👑", "OWNER": "👑"}
_SUPPORTED = {"ru", "en", "es", "de", "pt"}

_TEXT: dict[str, dict[str, str]] = {
    "plans": {"ru": "Тарифы", "en": "Plans", "es": "Planes", "de": "Tarife", "pt": "Planos"},
    "account": {"ru": "Мой аккаунт", "en": "My account", "es": "Mi cuenta", "de": "Mein Konto", "pt": "Minha conta"},
    "usage": {"ru": "Использование", "en": "Usage", "es": "Uso", "de": "Nutzung", "pt": "Uso"},
    "invoices": {"ru": "Счета", "en": "Invoices", "es": "Facturas", "de": "Rechnungen", "pt": "Faturas"},
    "language": {"ru": "Язык", "en": "Language", "es": "Idioma", "de": "Sprache", "pt": "Idioma"},
    "back": {"ru": "Назад", "en": "Back", "es": "Atrás", "de": "Zurück", "pt": "Voltar"},
    "my_plan": {"ru": "Мой тариф", "en": "My plan", "es": "Mi plan", "de": "Mein Tarif", "pt": "Meu plano"},
    "choose_basic": {"ru": "Выбрать BASIC", "en": "Choose BASIC", "es": "Elegir BASIC", "de": "BASIC wählen", "pt": "Escolher BASIC"},
    "choose_pro": {"ru": "Выбрать PRO", "en": "Choose PRO", "es": "Elegir PRO", "de": "PRO wählen", "pt": "Escolher PRO"},
    "create_invoice": {"ru": "Создать счёт", "en": "Create invoice", "es": "Crear factura", "de": "Rechnung erstellen", "pt": "Criar fatura"},
    "pay": {"ru": "Оплатить", "en": "Pay", "es": "Pagar", "de": "Bezahlen", "pt": "Pagar"},
    "rules": {"ru": "Правила", "en": "Rules", "es": "Reglas", "de": "Regeln", "pt": "Regras"},
    "jobs_day": {"ru": "Задачи/день", "en": "Jobs/day", "es": "Tareas/día", "de": "Jobs/Tag", "pt": "Tarefas/dia"},
    "videos_day": {"ru": "Видео/день", "en": "Videos/day", "es": "Videos/día", "de": "Videos/Tag", "pt": "Vídeos/dia"},
    "price": {"ru": "Цена", "en": "Price", "es": "Precio", "de": "Preis", "pt": "Preço"},
    "status": {"ru": "Статус", "en": "Status", "es": "Estado", "de": "Status", "pt": "Status"},
    "period": {"ru": "Период", "en": "Period", "es": "Período", "de": "Zeitraum", "pt": "Período"},
    "today": {"ru": "Сегодня", "en": "Today", "es": "Hoy", "de": "Heute", "pt": "Hoje"},
    "billing_period": {"ru": "Период", "en": "Billing period", "es": "Período de facturación", "de": "Abrechnungszeitraum", "pt": "Período de faturamento"},
    "storage": {"ru": "Хранилище", "en": "Storage", "es": "Almacenamiento", "de": "Speicher", "pt": "Armazenamento"},
    "items": {"ru": "Позиции", "en": "Items", "es": "Conceptos", "de": "Positionen", "pt": "Itens"},
    "total": {"ru": "Итого", "en": "Total", "es": "Total", "de": "Gesamt", "pt": "Total"},
    "payments_stub": {
        "ru": "💳 Оплата ещё не подключена\n\nСчёт создан и готов к оплате.\nСледующий этап — подключение платёжного провайдера.",
        "en": "💳 Payments are not connected yet\n\nThe invoice has been created and is ready for payment.\nThe next step is payment provider integration.",
        "es": "💳 Los pagos aún no están conectados\n\nLa factura ya está creada y lista para el pago.\nEl siguiente paso es integrar el proveedor de pago.",
        "de": "💳 Zahlungen sind noch nicht verbunden\n\nDie Rechnung wurde erstellt und ist zahlungsbereit.\nIm nächsten Schritt wird ein Zahlungsanbieter integriert.",
        "pt": "💳 Pagamentos ainda não estão conectados\n\nA fatura foi criada e está pronta para pagamento.\nO próximo passo é integrar o provedor de pagamento.",
    },
}


def _lang(lang: str) -> str:
    normalized = (lang or "ru").lower().strip()
    return normalized if normalized in _SUPPORTED else "ru"


def _t(lang: str, key: str) -> str:
    language = _lang(lang)
    pack = _TEXT.get(key, {})
    return pack.get(language) or pack.get("en") or key


def _fmt_period(date_from: str | None, date_to: str | None, lang: str) -> str:
    language = _lang(lang)
    if not date_from or not date_to:
        return "—"
    try:
        d1 = datetime.fromisoformat(str(date_from)[:10])
        d2 = datetime.fromisoformat(str(date_to)[:10])
    except Exception:
        return f"{date_from} — {date_to}"
    if language in {"en", "es", "de", "pt"}:
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
        [InlineKeyboardButton(text=f"💎 {_t(lang, 'plans')}", callback_data="product:plans")],
        [InlineKeyboardButton(text=f"👤 {_t(lang, 'account')}", callback_data="product:account")],
        [InlineKeyboardButton(text=f"🌐 {_t(lang, 'language')}", callback_data="product:language")],
    ])


def account_screen(*, lang: str, subscription: dict[str, Any], usage_today: dict[str, Any], usage_period: dict[str, Any], last_invoice: dict[str, Any] | None, rules_count: int) -> str:
    language = _lang(lang)
    plan = str(subscription.get("plan_name") or "FREE").upper()
    status = str(subscription.get("status") or "active")
    jobs_limit = int(subscription.get("max_jobs_per_day") or 0)
    video_limit = int(subscription.get("max_video_per_day") or 0)
    rules_limit = int(subscription.get("max_rules") or 0)
    storage_limit = int(subscription.get("max_storage_mb") or 0)
    unlimited_map = {"ru": "без ограничений", "en": "unlimited", "es": "ilimitado", "de": "unbegrenzt", "pt": "ilimitado"}
    period = _fmt_period(subscription.get("current_period_start"), subscription.get("current_period_end"), language)
    labels = {
        "ru": {"title": "👤 Мой аккаунт", "plan": "Тариф", "usage": "📊 Использование", "rules_today": "Правила", "jobs_today": "Задачи сегодня", "videos_today": "Видео сегодня", "last_invoice": "Последний счёт"},
        "en": {"title": "👤 My account", "plan": "Plan", "usage": "📊 Usage", "rules_today": "Rules", "jobs_today": "Jobs today", "videos_today": "Videos today", "last_invoice": "Last invoice"},
        "es": {"title": "👤 Mi cuenta", "plan": "Plan", "usage": "📊 Uso", "rules_today": "Reglas", "jobs_today": "Tareas hoy", "videos_today": "Videos hoy", "last_invoice": "Última factura"},
        "de": {"title": "👤 Mein Konto", "plan": "Tarif", "usage": "📊 Nutzung", "rules_today": "Regeln", "jobs_today": "Jobs heute", "videos_today": "Videos heute", "last_invoice": "Letzte Rechnung"},
        "pt": {"title": "👤 Minha conta", "plan": "Plano", "usage": "📊 Uso", "rules_today": "Regras", "jobs_today": "Tarefas hoje", "videos_today": "Vídeos hoje", "last_invoice": "Última fatura"},
    }
    l = labels[language]
    if plan == "OWNER":
        u = unlimited_map[language]
        rule_line = f"📋 {l['rules_today']}: {u}"
        jobs_line = f"📨 {l['jobs_today']}: {u}"
        video_line = f"🎬 {l['videos_today']}: {u}"
        storage_line = f"💾 {_t(language, 'storage')}: {u}"
    else:
        rule_line = f"📋 {l['rules_today']}: {rules_count} / {rules_limit}"
        jobs_line = f"📨 {l['jobs_today']}: {int(usage_today.get('jobs_count') or 0)} / {jobs_limit}"
        video_line = f"🎬 {l['videos_today']}: {int(usage_today.get('video_count') or 0)} / {video_limit}"
        storage_line = f"💾 {_t(language, 'storage')}: {int(usage_today.get('storage_used_mb') or 0)} MB / {storage_limit} MB"
    invoice_line = "—"
    if last_invoice:
        invoice_line = f"#{last_invoice.get('id')} · {last_invoice.get('status')} · {float(last_invoice.get('total') or 0):.2f} {last_invoice.get('currency') or 'USD'}"
    return "\n".join([
        l["title"],
        "",
        f"💎 {l['plan']}: {plan}",
        f"📌 {_t(language, 'status')}: {status}",
        f"📅 {_t(language, 'period')}: {period}",
        "",
        l["usage"],
        rule_line,
        jobs_line,
        video_line,
        storage_line,
        "",
        f"🧾 {l['last_invoice']}: {invoice_line}",
    ])


def account_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💎 {_t(lang, 'plans')}", callback_data="product:plans")],
        [InlineKeyboardButton(text=f"📈 {_t(lang, 'usage')}", callback_data="product:usage")],
        [InlineKeyboardButton(text=f"🧾 {_t(lang, 'invoices')}", callback_data="product:invoice")],
        [InlineKeyboardButton(text=f"🌐 {_t(lang, 'language')}", callback_data="product:language")],
        [InlineKeyboardButton(text=f"⬅️ {_t(lang, 'back')}", callback_data="product:menu")],
    ])


def plans_screen(*, lang: str, plans: list[dict[str, Any]]) -> str:
    language = _lang(lang)
    blocks: list[str] = [f"💎 {_t(language, 'plans')}", ""]
    for row in plans:
        name = str(row.get("name") or "").upper()
        if name == "OWNER":
            continue
        icon = PLAN_ICONS.get(name, "💠")
        blocks.extend([
            f"{icon} {name}",
            str(row.get("description") or ""),
            f"• {_t(language, 'rules')}: {row.get('max_rules')}",
            f"• {_t(language, 'videos_day')}: {row.get('max_video_per_day')}",
            f"• {_t(language, 'jobs_day')}: {row.get('max_jobs_per_day')}",
            f"• {_t(language, 'price')}: {float(row.get('price') or 0):.0f} USD",
            "",
        ])
    return "\n".join(blocks).strip()


def plans_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🚀 {_t(lang, 'choose_basic')}", callback_data="plan_select:BASIC")],
        [InlineKeyboardButton(text=f"👑 {_t(lang, 'choose_pro')}", callback_data="plan_select:PRO")],
        [InlineKeyboardButton(text=f"📊 {_t(lang, 'my_plan')}", callback_data="product:account")],
        [InlineKeyboardButton(text=f"⬅️ {_t(lang, 'back')}", callback_data="product:menu")],
    ])


def upgrade_confirm_screen(lang: str, plan: dict[str, Any]) -> str:
    language = _lang(lang)
    plan_name = str(plan.get("name") or "PRO").upper()
    price = float(plan.get("price") or 0)
    blocks = {
        "ru": ("Переход на", "Вы получите", "• До {rules} правил", "• До {videos} видео в день", "• До {jobs} задач в день", "• Повышенный приоритет обработки", "Стоимость", "Создать счёт?"),
        "en": ("Upgrade to", "You will get", "• Up to {rules} rules", "• Up to {videos} videos/day", "• Up to {jobs} jobs/day", "• Higher processing priority", "Price", "Create invoice?"),
        "es": ("Mejorar a", "Obtendrás", "• Hasta {rules} reglas", "• Hasta {videos} videos/día", "• Hasta {jobs} tareas/día", "• Mayor prioridad de procesamiento", "Precio", "¿Crear factura?"),
        "de": ("Upgrade auf", "Du erhältst", "• Bis zu {rules} Regeln", "• Bis zu {videos} Videos/Tag", "• Bis zu {jobs} Jobs/Tag", "• Höhere Verarbeitungspriorität", "Preis", "Rechnung erstellen?"),
        "pt": ("Upgrade para", "Você terá", "• Até {rules} regras", "• Até {videos} vídeos/dia", "• Até {jobs} tarefas/dia", "• Maior prioridade de processamento", "Preço", "Criar fatura?"),
    }
    b = blocks[language]
    return "\n".join([
        f"{PLAN_ICONS.get(plan_name, '💎')} {b[0]} {plan_name}",
        "",
        f"{b[1]}:",
        b[2].format(rules=plan.get("max_rules")),
        b[3].format(videos=plan.get("max_video_per_day")),
        b[4].format(jobs=plan.get("max_jobs_per_day")),
        b[5],
        "",
        f"{b[6]}: {price:.0f} USD / month",
        "",
        b[7],
    ])


def upgrade_confirm_keyboard(lang: str, plan_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🧾 {_t(lang, 'create_invoice')}", callback_data=f"plan_confirm:{plan_name}")],
        [InlineKeyboardButton(text=f"⬅️ {_t(lang, 'back')}", callback_data="product:plans")],
    ])


def invoice_screen(*, lang: str, invoice: dict[str, Any], items: list[dict[str, Any]]) -> str:
    language = _lang(lang)
    period = _fmt_period(invoice.get("period_start"), invoice.get("period_end"), language)
    plan_name = str((items[0].get("metadata") or {}).get("plan_name") if items else "") or "UNKNOWN"
    lines = [f"• {item.get('description')} — {float(item.get('amount') or 0):.2f} {invoice.get('currency') or 'USD'}" for item in items] or ["• —"]
    title = {"ru": "Счёт", "en": "Invoice", "es": "Factura", "de": "Rechnung", "pt": "Fatura"}[language]
    plan_label = {"ru": "Тариф", "en": "Plan", "es": "Plan", "de": "Tarif", "pt": "Plano"}[language]
    footer = {
        "ru": "Оплата будет подключена следующим этапом.",
        "en": "Payment will be connected in the next step.",
        "es": "El pago se conectará en la siguiente etapa.",
        "de": "Die Zahlung wird im nächsten Schritt verbunden.",
        "pt": "O pagamento será conectado na próxima etapa.",
    }[language]
    return "\n".join([
        f"🧾 {title} #{invoice.get('id')}",
        "",
        f"📌 {_t(language, 'status')}: {invoice.get('status')}",
        f"💎 {plan_label}: {plan_name}",
        f"📅 {_t(language, 'period')}: {period}",
        "",
        f"{_t(language, 'items')}:",
        *lines,
        "",
        f"{_t(language, 'total')}: {float(invoice.get('total') or 0):.2f} {invoice.get('currency') or 'USD'}",
        "",
        footer,
    ])


def invoice_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 {_t(lang, 'pay')}", callback_data="invoice:pay")],
        [InlineKeyboardButton(text=f"💎 {_t(lang, 'plans')}", callback_data="product:plans")],
        [InlineKeyboardButton(text=f"⬅️ {_t(lang, 'back')}", callback_data="product:menu")],
    ])


def payment_stub_screen(lang: str) -> str:
    return _t(lang, "payments_stub")


def usage_screen(*, lang: str, today: dict[str, Any], period: dict[str, Any], limits: dict[str, Any]) -> str:
    language = _lang(lang)
    jobs = int(today.get("jobs_count") or 0)
    videos = int(today.get("video_count") or 0)
    jobs_limit = int(limits.get("max_jobs_per_day") or 0)
    videos_limit = int(limits.get("max_video_per_day") or 0)
    storage = int(period.get("storage_used_mb") or 0)
    status_map = {"ru": "всё в порядке", "en": "OK", "es": "OK", "de": "OK", "pt": "OK"}
    jobs_label = {"ru": "Задачи", "en": "Jobs", "es": "Tareas", "de": "Jobs", "pt": "Tarefas"}[language]
    videos_label = {"ru": "Видео", "en": "Videos", "es": "Videos", "de": "Videos", "pt": "Vídeos"}[language]
    storage_unit = "МБ" if language == "ru" else "MB"
    return "\n".join([
        f"📈 {_t(language, 'usage')}",
        "",
        f"{_t(language, 'today')}:",
        f"📨 {jobs_label}: {jobs} / {jobs_limit} {_progress(jobs, jobs_limit)}",
        f"🎬 {videos_label}: {videos} / {videos_limit} {_progress(videos, videos_limit)}",
        "",
        f"{_t(language, 'billing_period')}:",
        f"📨 {jobs_label}: {int(period.get('jobs_count') or 0):,}",
        f"🎬 {videos_label}: {int(period.get('video_count') or 0):,}",
        f"💾 {_t(language, 'storage')}: {storage:,} {storage_unit}",
        "",
        f"{_t(language, 'status')}: {status_map[language]}",
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
    messages = {
        "ru": "❓ Помощь\n\nОсновные разделы:\n📋 Правила — управление пересылкой\n📡 Каналы — источники и получатели\n💎 Тарифы — лимиты и подписка\n📈 Использование — сколько ресурсов уже потрачено\n🧾 Счета — счета за тариф и превышения\n\nЕсли что-то не работает — откройте “Живой статус”.",
        "en": "❓ Help\n\nMain sections:\n📋 Rules — forwarding settings\n📡 Channels — sources and targets\n💎 Plans — limits and subscription\n📈 Usage — consumed resources\n🧾 Invoices — plan and overage invoices\n\nIf something does not work, open “Live status”.",
        "es": "❓ Ayuda\n\nSecciones principales:\n📋 Reglas — configuración de reenvío\n📡 Canales — fuentes y destinos\n💎 Planes — límites y suscripción\n📈 Uso — recursos consumidos\n🧾 Facturas — facturas del plan y excedentes\n\nSi algo no funciona, abre “Estado en vivo”.",
        "de": "❓ Hilfe\n\nHauptbereiche:\n📋 Regeln — Weiterleitungseinstellungen\n📡 Kanäle — Quellen und Ziele\n💎 Tarife — Limits und Abonnement\n📈 Nutzung — verbrauchte Ressourcen\n🧾 Rechnungen — Tarif- und Überziehungsrechnungen\n\nWenn etwas nicht funktioniert, öffne „Live-Status“.",
        "pt": "❓ Ajuda\n\nSeções principais:\n📋 Regras — configurações de encaminhamento\n📡 Canais — fontes e destinos\n💎 Planos — limites e assinatura\n📈 Uso — recursos consumidos\n🧾 Faturas — faturas do plano e excedentes\n\nSe algo não funcionar, abra “Status ao vivo”.",
    }
    return messages[_lang(lang)]


def start_screen(lang: str, is_new: bool) -> str:
    language = _lang(lang)
    if not is_new:
        return {
            "ru": "👋 С возвращением! Откройте меню аккаунта.",
            "en": "👋 Welcome back! Open your account menu.",
            "es": "👋 ¡Bienvenido de nuevo! Abre el menú de tu cuenta.",
            "de": "👋 Willkommen zurück! Öffne dein Kontomenü.",
            "pt": "👋 Bem-vindo de volta! Abra o menu da sua conta.",
        }[language]
    return {
        "ru": "👋 Добро пожаловать в TopPoster\n\nЯ помогу автоматически пересылать посты, обрабатывать видео и управлять публикациями.\n\nВы начали с тарифа FREE.\n\nЧто можно сделать:\n1. Добавить источник\n2. Добавить получателя\n3. Создать правило\n4. Проверить тариф и лимиты",
        "en": "👋 Welcome to TopPoster\n\nI help you automatically forward posts, process videos and manage publishing.\n\nYou started with the FREE plan.\n\nWhat you can do:\n1. Add a source\n2. Add a target\n3. Create a rule\n4. Check your plan and limits",
        "es": "👋 Bienvenido a TopPoster\n\nTe ayudo a reenviar publicaciones automáticamente, procesar videos y gestionar publicaciones.\n\nComenzaste con el plan FREE.\n\nQué puedes hacer:\n1. Añadir una fuente\n2. Añadir un destino\n3. Crear una regla\n4. Ver tu plan y límites",
        "de": "👋 Willkommen bei TopPoster\n\nIch helfe dir, Beiträge automatisch weiterzuleiten, Videos zu verarbeiten und Veröffentlichungen zu verwalten.\n\nDu startest mit dem FREE-Tarif.\n\nWas du tun kannst:\n1. Quelle hinzufügen\n2. Ziel hinzufügen\n3. Regel erstellen\n4. Tarif und Limits prüfen",
        "pt": "👋 Bem-vindo ao TopPoster\n\nEu ajudo você a encaminhar posts automaticamente, processar vídeos e gerenciar publicações.\n\nVocê começou no plano FREE.\n\nO que você pode fazer:\n1. Adicionar uma origem\n2. Adicionar um destino\n3. Criar uma regra\n4. Ver seu plano e limites",
    }[language]


def start_keyboard(lang: str) -> InlineKeyboardMarkup:
    labels = {
        "add_channel": {"ru": "Добавить канал", "en": "Add channel", "es": "Agregar canal", "de": "Kanal hinzufügen", "pt": "Adicionar canal"},
        "create_rule": {"ru": "Создать правило", "en": "Create rule", "es": "Crear regla", "de": "Regel erstellen", "pt": "Criar regra"},
    }
    language = _lang(lang)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📡 {labels['add_channel'][language]}", callback_data="start:add_channel")],
        [InlineKeyboardButton(text=f"🔄 {labels['create_rule'][language]}", callback_data="start:create_rule")],
        [InlineKeyboardButton(text=f"💎 {_t(language, 'plans')}", callback_data="product:plans")],
        [InlineKeyboardButton(text=f"🌐 {_t(language, 'language')}", callback_data="product:language")],
    ])


def rule_limit_error(lang: str, plan_name: str, allowed_rules: int, created_rules: int) -> str:
    messages = {
        "ru": f"⚠️ Лимит правил достигнут\n\nВаш тариф: {plan_name}\nДоступно правил: {allowed_rules}\nУже создано: {created_rules}\n\nЧтобы добавить больше правил, перейдите на BASIC или PRO.",
        "en": f"⚠️ Rule limit reached\n\nYour plan: {plan_name}\nAllowed rules: {allowed_rules}\nCreated rules: {created_rules}\n\nUpgrade to BASIC or PRO to add more rules.",
        "es": f"⚠️ Límite de reglas alcanzado\n\nTu plan: {plan_name}\nReglas permitidas: {allowed_rules}\nReglas creadas: {created_rules}\n\nActualiza a BASIC o PRO para agregar más reglas.",
        "de": f"⚠️ Regellimit erreicht\n\nDein Tarif: {plan_name}\nErlaubte Regeln: {allowed_rules}\nErstellte Regeln: {created_rules}\n\nUpgrade auf BASIC oder PRO, um mehr Regeln hinzuzufügen.",
        "pt": f"⚠️ Limite de regras atingido\n\nSeu plano: {plan_name}\nRegras permitidas: {allowed_rules}\nRegras criadas: {created_rules}\n\nFaça upgrade para BASIC ou PRO para adicionar mais regras.",
    }
    return messages[_lang(lang)]


def video_limit_error(lang: str, plan_name: str, used: int, limit: int) -> str:
    messages = {
        "ru": f"🎬 Лимит видео на сегодня исчерпан\n\nВаш тариф: {plan_name}\nВидео сегодня: {used} / {limit}\n\nНовые видео будут доступны после обновления дневного лимита или после перехода на PRO.",
        "en": f"🎬 Daily video limit reached\n\nYour plan: {plan_name}\nVideos today: {used} / {limit}\n\nNew videos will be available after daily reset or after upgrading to PRO.",
        "es": f"🎬 Límite diario de videos alcanzado\n\nTu plan: {plan_name}\nVideos hoy: {used} / {limit}\n\nNuevos videos estarán disponibles tras el reinicio diario o después de cambiar a PRO.",
        "de": f"🎬 Tägliches Videolimit erreicht\n\nDein Tarif: {plan_name}\nVideos heute: {used} / {limit}\n\nNeue Videos sind nach dem täglichen Reset oder nach Upgrade auf PRO verfügbar.",
        "pt": f"🎬 Limite diário de vídeos atingido\n\nSeu plano: {plan_name}\nVídeos hoje: {used} / {limit}\n\nNovos vídeos estarão disponíveis após o reset diário ou após upgrade para PRO.",
    }
    return messages[_lang(lang)]


def limit_error_keyboard(lang: str) -> InlineKeyboardMarkup:
    labels = {
        "ru": "Посмотреть тарифы",
        "en": "View plans",
        "es": "Ver planes",
        "de": "Tarife ansehen",
        "pt": "Ver planos",
    }
    language = _lang(lang)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💎 {labels[language]}", callback_data="product:plans")],
        [InlineKeyboardButton(text=f"⬅️ {_t(language, 'back')}", callback_data="product:menu")],
    ])


def build_upgrade_invoice_flow(*, plan_name: str, price: float) -> dict[str, Any]:
    return {
        "item_type": "base_plan",
        "description": f"Тариф {plan_name}",
        "quantity": 1,
        "unit_price": round(float(price), 2),
    }
