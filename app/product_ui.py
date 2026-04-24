from __future__ import annotations

from datetime import datetime
from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

PLAN_ORDER = ("FREE", "BASIC", "PRO")
PLAN_ICONS = {"FREE": "🆓", "BASIC": "🚀", "PRO": "👑", "OWNER": "👑"}

_LANG_DEFAULT = "en"

_TR = {
    "menu.plans": {"ru": "💎 Тарифы", "en": "💎 Plans", "es": "💎 Planes", "de": "💎 Tarife", "pt": "💎 Planos"},
    "menu.account": {"ru": "👤 Мой аккаунт", "en": "👤 My account", "es": "👤 Mi cuenta", "de": "👤 Mein Konto", "pt": "👤 Minha conta"},
    "menu.language": {"ru": "🌐 Язык", "en": "🌐 Language", "es": "🌐 Idioma", "de": "🌐 Sprache", "pt": "🌐 Idioma"},
    "menu.usage": {"ru": "📈 Использование", "en": "📈 Usage", "es": "📈 Uso", "de": "📈 Nutzung", "pt": "📈 Uso"},
    "menu.invoices": {"ru": "🧾 Счета", "en": "🧾 Invoices", "es": "🧾 Facturas", "de": "🧾 Rechnungen", "pt": "🧾 Faturas"},
    "menu.back": {"ru": "⬅️ Назад", "en": "⬅️ Back", "es": "⬅️ Atrás", "de": "⬅️ Zurück", "pt": "⬅️ Voltar"},
    "label.plan": {"ru": "Тариф", "en": "Plan", "es": "Plan", "de": "Tarif", "pt": "Plano"},
    "label.status": {"ru": "Статус", "en": "Status", "es": "Estado", "de": "Status", "pt": "Status"},
    "label.period": {"ru": "Период", "en": "Period", "es": "Período", "de": "Zeitraum", "pt": "Período"},
    "label.usage": {"ru": "📊 Использование:", "en": "📊 Usage:", "es": "📊 Uso:", "de": "📊 Nutzung:", "pt": "📊 Uso:"},
    "label.rules": {"ru": "Правила", "en": "Rules", "es": "Reglas", "de": "Regeln", "pt": "Regras"},
    "label.jobs_today": {"ru": "Задачи сегодня", "en": "Jobs today", "es": "Tareas hoy", "de": "Jobs heute", "pt": "Tarefas hoje"},
    "label.videos_today": {"ru": "Видео сегодня", "en": "Videos today", "es": "Videos hoy", "de": "Videos heute", "pt": "Vídeos hoje"},
    "label.storage": {"ru": "Хранилище", "en": "Storage", "es": "Almacenamiento", "de": "Speicher", "pt": "Armazenamento"},
    "label.last_invoice": {"ru": "Последний счёт", "en": "Last invoice", "es": "Última factura", "de": "Letzte Rechnung", "pt": "Última fatura"},
    "label.unlimited": {"ru": "без ограничений", "en": "unlimited", "es": "ilimitado", "de": "unbegrenzt", "pt": "ilimitado"},
    "label.videos_day": {"ru": "Видео/день", "en": "Videos/day", "es": "Videos/día", "de": "Videos/Tag", "pt": "Vídeos/dia"},
    "label.jobs_day": {"ru": "Задачи/день", "en": "Jobs/day", "es": "Tareas/día", "de": "Jobs/Tag", "pt": "Tarefas/dia"},
    "label.price": {"ru": "Цена", "en": "Price", "es": "Precio", "de": "Preis", "pt": "Preço"},
    "menu.choose_basic": {"ru": "🚀 Выбрать BASIC", "en": "🚀 Choose BASIC", "es": "🚀 Elegir BASIC", "de": "🚀 BASIC wählen", "pt": "🚀 Escolher BASIC"},
    "menu.choose_pro": {"ru": "👑 Выбрать PRO", "en": "👑 Choose PRO", "es": "👑 Elegir PRO", "de": "👑 PRO wählen", "pt": "👑 Escolher PRO"},
    "menu.my_plan": {"ru": "📊 Мой тариф", "en": "📊 My plan", "es": "📊 Mi plan", "de": "📊 Mein Tarif", "pt": "📊 Meu plano"},
    "upgrade.to": {"ru": "Переход на", "en": "Upgrade to", "es": "Cambiar a", "de": "Upgrade auf", "pt": "Migrar para"},
    "upgrade.will_get": {"ru": "Вы получите:", "en": "You will get:", "es": "Obtendrás:", "de": "Du bekommst:", "pt": "Você receberá:"},
    "upgrade.up_to_rules": {"ru": "• До {v} правил", "en": "• Up to {v} rules", "es": "• Hasta {v} reglas", "de": "• Bis zu {v} Regeln", "pt": "• Até {v} regras"},
    "upgrade.up_to_videos": {"ru": "• До {v} видео в день", "en": "• Up to {v} videos/day", "es": "• Hasta {v} videos/día", "de": "• Bis zu {v} Videos/Tag", "pt": "• Até {v} vídeos/dia"},
    "upgrade.up_to_jobs": {"ru": "• До {v} задач в день", "en": "• Up to {v} jobs/day", "es": "• Hasta {v} tareas/día", "de": "• Bis zu {v} Jobs/Tag", "pt": "• Até {v} tarefas/dia"},
    "upgrade.priority": {"ru": "• Повышенный приоритет обработки", "en": "• Higher processing priority", "es": "• Mayor prioridad de procesamiento", "de": "• Höhere Verarbeitungspriorität", "pt": "• Maior prioridade de processamento"},
    "upgrade.price": {"ru": "Стоимость: {v:.0f} USD / месяц", "en": "Price: {v:.0f} USD / month", "es": "Precio: {v:.0f} USD / mes", "de": "Preis: {v:.0f} USD / Monat", "pt": "Preço: {v:.0f} USD / mês"},
    "upgrade.create_invoice": {"ru": "Создать счёт?", "en": "Create invoice?", "es": "¿Crear factura?", "de": "Rechnung erstellen?", "pt": "Criar fatura?"},
    "menu.create_invoice": {"ru": "🧾 Создать счёт", "en": "🧾 Create invoice", "es": "🧾 Crear factura", "de": "🧾 Rechnung erstellen", "pt": "🧾 Criar fatura"},
    "invoice.title": {"ru": "🧾 Счёт #{id}", "en": "🧾 Invoice #{id}", "es": "🧾 Factura #{id}", "de": "🧾 Rechnung #{id}", "pt": "🧾 Fatura #{id}"},
    "invoice.items": {"ru": "Позиции:", "en": "Items:", "es": "Partidas:", "de": "Positionen:", "pt": "Itens:"},
    "invoice.total": {"ru": "Итого: {v:.2f} {ccy}", "en": "Total: {v:.2f} {ccy}", "es": "Total: {v:.2f} {ccy}", "de": "Gesamt: {v:.2f} {ccy}", "pt": "Total: {v:.2f} {ccy}"},
    "invoice.next_step": {"ru": "Оплата будет подключена следующим этапом.", "en": "Payment will be connected in the next step.", "es": "El pago se conectará en el siguiente paso.", "de": "Die Zahlung wird im nächsten Schritt angebunden.", "pt": "O pagamento será conectado na próxima etapa."},
    "menu.pay": {"ru": "💳 Оплатить", "en": "💳 Pay", "es": "💳 Pagar", "de": "💳 Bezahlen", "pt": "💳 Pagar"},
    "payment.stub": {
        "ru": "💳 Оплата ещё не подключена\n\nСчёт создан и готов к оплате.\nСледующий этап — подключение платёжного провайдера.",
        "en": "💳 Payments are not connected yet\n\nThe invoice has been created and is ready for payment.\nThe next step is payment provider integration.",
        "es": "💳 Los pagos aún no están conectados\n\nLa factura fue creada y está lista para el pago.\nEl siguiente paso es integrar el proveedor de pagos.",
        "de": "💳 Zahlungen sind noch nicht verbunden\n\nDie Rechnung wurde erstellt und ist zur Zahlung bereit.\nDer nächste Schritt ist die Integration des Zahlungsanbieters.",
        "pt": "💳 Os pagamentos ainda não estão conectados\n\nA fatura foi criada e está pronta para pagamento.\nA próxima etapa é integrar o provedor de pagamentos.",
    },
    "usage.title": {"ru": "📈 Использование", "en": "📈 Usage", "es": "📈 Uso", "de": "📈 Nutzung", "pt": "📈 Uso"},
    "usage.today": {"ru": "Сегодня:", "en": "Today:", "es": "Hoy:", "de": "Heute:", "pt": "Hoje:"},
    "usage.billing_period": {"ru": "Период:", "en": "Billing period:", "es": "Período de facturación:", "de": "Abrechnungszeitraum:", "pt": "Período de cobrança:"},
    "usage.jobs": {"ru": "Задачи", "en": "Jobs", "es": "Tareas", "de": "Jobs", "pt": "Tarefas"},
    "usage.videos": {"ru": "Видео", "en": "Videos", "es": "Videos", "de": "Videos", "pt": "Vídeos"},
    "usage.storage": {"ru": "Хранилище", "en": "Storage", "es": "Almacenamiento", "de": "Speicher", "pt": "Armazenamento"},
    "usage.status_ok": {"ru": "всё в порядке", "en": "OK", "es": "todo en orden", "de": "alles in Ordnung", "pt": "tudo em ordem"},
    "language.ru": {"ru": "🇷🇺 Русский", "en": "🇷🇺 Russian", "es": "🇷🇺 Ruso", "de": "🇷🇺 Russisch", "pt": "🇷🇺 Russo"},
    "language.en": {"ru": "🇬🇧 Английский", "en": "🇬🇧 English", "es": "🇬🇧 Inglés", "de": "🇬🇧 Englisch", "pt": "🇬🇧 Inglês"},
    "language.es": {"ru": "🇪🇸 Испанский", "en": "🇪🇸 Spanish", "es": "🇪🇸 Español", "de": "🇪🇸 Spanisch", "pt": "🇪🇸 Espanhol"},
    "language.de": {"ru": "🇩🇪 Немецкий", "en": "🇩🇪 German", "es": "🇩🇪 Alemán", "de": "🇩🇪 Deutsch", "pt": "🇩🇪 Alemão"},
    "language.pt": {"ru": "🇵🇹 Португальский", "en": "🇵🇹 Portuguese", "es": "🇵🇹 Portugués", "de": "🇵🇹 Portugiesisch", "pt": "🇵🇹 Português"},
    "help.text": {
        "ru": "❓ Помощь\n\nОсновные разделы:\n📋 Правила — управление пересылкой\n📡 Каналы — источники и получатели\n💎 Тарифы — лимиты и подписка\n📈 Использование — сколько ресурсов уже потрачено\n🧾 Счета — счета за тариф и превышения\n\nЕсли что-то не работает — откройте “Живой статус”.",
        "en": "❓ Help\n\nMain sections:\n📋 Rules — forwarding settings\n📡 Channels — sources and targets\n💎 Plans — limits and subscription\n📈 Usage — consumed resources\n🧾 Invoices — plan and overage invoices\n\nIf something does not work, open “Live status”.",
        "es": "❓ Ayuda\n\nSecciones principales:\n📋 Reglas — configuración de reenvío\n📡 Canales — orígenes y destinos\n💎 Planes — límites y suscripción\n📈 Uso — recursos consumidos\n🧾 Facturas — facturas del plan y de excedentes\n\nSi algo no funciona, abre “Estado en vivo”.",
        "de": "❓ Hilfe\n\nHauptbereiche:\n📋 Regeln — Weiterleitungseinstellungen\n📡 Kanäle — Quellen und Ziele\n💎 Tarife — Limits und Abonnement\n📈 Nutzung — verbrauchte Ressourcen\n🧾 Rechnungen — Tarif- und Überziehungsrechnungen\n\nWenn etwas nicht funktioniert, öffne „Live-Status“.",
        "pt": "❓ Ajuda\n\nSeções principais:\n📋 Regras — configurações de encaminhamento\n📡 Canais — origens e destinos\n💎 Planos — limites e assinatura\n📈 Uso — recursos consumidos\n🧾 Faturas — faturas do plano e de excedentes\n\nSe algo não funcionar, abra “Status ao vivo”.",
    },
    "start.back": {"ru": "👋 С возвращением! Откройте меню аккаунта.", "en": "👋 Welcome back! Open your account menu.", "es": "👋 ¡Bienvenido de nuevo! Abre el menú de tu cuenta.", "de": "👋 Willkommen zurück! Öffne dein Kontomenü.", "pt": "👋 Bem-vindo de volta! Abra o menu da sua conta."},
    "start.new": {
        "ru": "👋 Добро пожаловать в TopPoster\n\nЯ помогу автоматически пересылать посты, обрабатывать видео и управлять публикациями.\n\nВы начали с тарифа FREE.\n\nЧто можно сделать:\n1. Добавить источник\n2. Добавить получателя\n3. Создать правило\n4. Проверить тариф и лимиты",
        "en": "👋 Welcome to TopPoster\n\nI help you automatically forward posts, process videos and manage publishing.\n\nYou started with the FREE plan.\n\nWhat you can do:\n1. Add a source\n2. Add a target\n3. Create a rule\n4. Check your plan and limits",
        "es": "👋 Bienvenido a TopPoster\n\nTe ayudo a reenviar publicaciones automáticamente, procesar videos y gestionar publicaciones.\n\nComenzaste con el plan FREE.\n\nQué puedes hacer:\n1. Añadir un origen\n2. Añadir un destino\n3. Crear una regla\n4. Revisar tu plan y límites",
        "de": "👋 Willkommen bei TopPoster\n\nIch helfe dir, Beiträge automatisch weiterzuleiten, Videos zu verarbeiten und Veröffentlichungen zu verwalten.\n\nDu startest mit dem FREE-Tarif.\n\nWas du tun kannst:\n1. Quelle hinzufügen\n2. Ziel hinzufügen\n3. Regel erstellen\n4. Tarif und Limits prüfen",
        "pt": "👋 Bem-vindo ao TopPoster\n\nEu ajudo você a encaminhar posts automaticamente, processar vídeos e gerenciar publicações.\n\nVocê começou com o plano FREE.\n\nO que você pode fazer:\n1. Adicionar uma origem\n2. Adicionar um destino\n3. Criar uma regra\n4. Verificar seu plano e limites",
    },
    "menu.add_channel": {"ru": "📡 Добавить канал", "en": "📡 Add channel", "es": "📡 Añadir canal", "de": "📡 Kanal hinzufügen", "pt": "📡 Adicionar canal"},
    "menu.create_rule": {"ru": "🔄 Создать правило", "en": "🔄 Create rule", "es": "🔄 Crear regla", "de": "🔄 Regel erstellen", "pt": "🔄 Criar regra"},
    "limit.rule": {
        "ru": "⚠️ Лимит правил достигнут\n\nВаш тариф: {plan_name}\nДоступно правил: {allowed_rules}\nУже создано: {created_rules}\n\nЧтобы добавить больше правил, перейдите на BASIC или PRO.",
        "en": "⚠️ Rule limit reached\n\nYour plan: {plan_name}\nAllowed rules: {allowed_rules}\nCreated rules: {created_rules}\n\nUpgrade to BASIC or PRO to add more rules.",
        "es": "⚠️ Se alcanzó el límite de reglas\n\nTu plan: {plan_name}\nReglas permitidas: {allowed_rules}\nReglas creadas: {created_rules}\n\nActualiza a BASIC o PRO para añadir más reglas.",
        "de": "⚠️ Regellimit erreicht\n\nDein Tarif: {plan_name}\nErlaubte Regeln: {allowed_rules}\nErstellte Regeln: {created_rules}\n\nUpgrade auf BASIC oder PRO, um mehr Regeln hinzuzufügen.",
        "pt": "⚠️ Limite de regras atingido\n\nSeu plano: {plan_name}\nRegras permitidas: {allowed_rules}\nRegras criadas: {created_rules}\n\nAtualize para BASIC ou PRO para adicionar mais regras.",
    },
    "limit.video": {
        "ru": "🎬 Лимит видео на сегодня исчерпан\n\nВаш тариф: {plan_name}\nВидео сегодня: {used} / {limit}\n\nНовые видео будут доступны после обновления дневного лимита или после перехода на PRO.",
        "en": "🎬 Daily video limit reached\n\nYour plan: {plan_name}\nVideos today: {used} / {limit}\n\nNew videos will be available after daily reset or after upgrading to PRO.",
        "es": "🎬 Se alcanzó el límite diario de videos\n\nTu plan: {plan_name}\nVideos hoy: {used} / {limit}\n\nLos nuevos videos estarán disponibles tras el reinicio diario o al actualizar a PRO.",
        "de": "🎬 Tägliches Videolimit erreicht\n\nDein Tarif: {plan_name}\nVideos heute: {used} / {limit}\n\nNeue Videos sind nach dem täglichen Reset oder nach einem Upgrade auf PRO verfügbar.",
        "pt": "🎬 Limite diário de vídeos atingido\n\nSeu plano: {plan_name}\nVídeos hoje: {used} / {limit}\n\nNovos vídeos estarão disponíveis após o reset diário ou após upgrade para PRO.",
    },
    "menu.view_plans": {"ru": "💎 Посмотреть тарифы", "en": "💎 View plans", "es": "💎 Ver planes", "de": "💎 Tarife anzeigen", "pt": "💎 Ver planos"},
}

_STATUS_TR = {
    "active": {"ru": "активна", "en": "active", "es": "activa", "de": "aktiv", "pt": "ativa"},
    "trial": {"ru": "триал", "en": "trial", "es": "prueba", "de": "Testphase", "pt": "teste"},
    "past_due": {"ru": "просрочена", "en": "past due", "es": "vencida", "de": "überfällig", "pt": "vencida"},
    "canceled": {"ru": "отменена", "en": "canceled", "es": "cancelada", "de": "gekündigt", "pt": "cancelada"},
    "draft": {"ru": "черновик", "en": "draft", "es": "borrador", "de": "Entwurf", "pt": "rascunho"},
    "open": {"ru": "открыт", "en": "open", "es": "abierta", "de": "offen", "pt": "aberta"},
    "paid": {"ru": "оплачен", "en": "paid", "es": "pagada", "de": "bezahlt", "pt": "paga"},
    "void": {"ru": "аннулирован", "en": "void", "es": "anulada", "de": "storniert", "pt": "anulada"},
}


def _msg(lang: str, key: str, **kwargs: Any) -> str:
    table = _TR.get(key) or {}
    template = table.get(lang) or table.get(_LANG_DEFAULT) or key
    return template.format(**kwargs)


def _status(lang: str, value: Any) -> str:
    raw = str(value or "").strip().lower()
    table = _STATUS_TR.get(raw)
    if not table:
        return str(value or "—")
    return table.get(lang) or table.get(_LANG_DEFAULT) or raw


def _fmt_period(date_from: str | None, date_to: str | None, lang: str) -> str:
    if not date_from or not date_to:
        return "—"
    try:
        d1 = datetime.fromisoformat(str(date_from)[:10])
        d2 = datetime.fromisoformat(str(date_to)[:10])
    except Exception:
        return f"{date_from} — {date_to}"
    if lang in {"en", "es", "de", "pt"}:
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
        [InlineKeyboardButton(text=_msg(lang, "menu.plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=_msg(lang, "menu.account"), callback_data="product:account")],
        [InlineKeyboardButton(text=_msg(lang, "menu.language"), callback_data="product:language")],
    ])


def account_screen(*, lang: str, subscription: dict[str, Any], usage_today: dict[str, Any], usage_period: dict[str, Any], last_invoice: dict[str, Any] | None, rules_count: int) -> str:
    plan = str(subscription.get("plan_name") or "FREE").upper()
    status = _status(lang, subscription.get("status") or "active")
    jobs_limit = int(subscription.get("max_jobs_per_day") or 0)
    video_limit = int(subscription.get("max_video_per_day") or 0)
    rules_limit = int(subscription.get("max_rules") or 0)
    storage_limit = int(subscription.get("max_storage_mb") or 0)
    unlimited = plan == "OWNER"
    period = _fmt_period(subscription.get("current_period_start"), subscription.get("current_period_end"), lang)
    if unlimited:
        limits_text = _msg(lang, "label.unlimited")
        rule_line = f"📋 {_msg(lang, 'label.rules')}: {limits_text}"
        jobs_line = f"📨 {_msg(lang, 'label.jobs_today')}: {limits_text}"
        video_line = f"🎬 {_msg(lang, 'label.videos_today')}: {limits_text}"
        storage_line = f"💾 {_msg(lang, 'label.storage')}: {limits_text}"
    else:
        rule_line = f"📋 {_msg(lang, 'label.rules')}: {rules_count} / {rules_limit}"
        jobs_line = f"📨 {_msg(lang, 'label.jobs_today')}: {int(usage_today.get('jobs_count') or 0)} / {jobs_limit}"
        video_line = f"🎬 {_msg(lang, 'label.videos_today')}: {int(usage_today.get('video_count') or 0)} / {video_limit}"
        storage_line = f"💾 {_msg(lang, 'label.storage')}: {int(usage_today.get('storage_used_mb') or 0)} MB / {storage_limit} MB"
    invoice_line = "—"
    if last_invoice:
        invoice_status = _status(lang, last_invoice.get("status"))
        invoice_line = f"#{last_invoice.get('id')} · {invoice_status} · {float(last_invoice.get('total') or 0):.2f} {last_invoice.get('currency') or 'USD'}"
    return "\n".join([
        _msg(lang, "menu.account"),
        "",
        f"💎 {_msg(lang, 'label.plan')}: {plan}",
        f"📌 {_msg(lang, 'label.status')}: {status}",
        f"📅 {_msg(lang, 'label.period')}: {period}",
        "",
        _msg(lang, "label.usage"),
        rule_line,
        jobs_line,
        video_line,
        storage_line,
        "",
        f"🧾 {_msg(lang, 'label.last_invoice')}: {invoice_line}",
    ])


def account_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=_msg(lang, "menu.plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=_msg(lang, "menu.usage"), callback_data="product:usage")],
        [InlineKeyboardButton(text=_msg(lang, "menu.invoices"), callback_data="product:invoice")],
        [InlineKeyboardButton(text=_msg(lang, "menu.language"), callback_data="product:language")],
        [InlineKeyboardButton(text=_msg(lang, "menu.back"), callback_data="product:menu")],
    ])


def plans_screen(*, lang: str, plans: list[dict[str, Any]]) -> str:
    blocks: list[str] = [_msg(lang, "menu.plans"), ""]
    for row in plans:
        name = str(row.get("name") or "").upper()
        if name == "OWNER":
            continue
        icon = PLAN_ICONS.get(name, "💠")
        desc = str(row.get("description") or "")
        blocks.extend([
            f"{icon} {name}",
            desc,
            f"• {_msg(lang, 'label.rules')}: {row.get('max_rules')}",
            f"• {_msg(lang, 'label.videos_day')}: {row.get('max_video_per_day')}",
            f"• {_msg(lang, 'label.jobs_day')}: {row.get('max_jobs_per_day')}",
            f"• {_msg(lang, 'label.price')}: {float(row.get('price') or 0):.0f} USD",
            "",
        ])
    return "\n".join(blocks).strip()


def plans_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=_msg(lang, "menu.choose_basic"), callback_data="plan_select:BASIC")],
        [InlineKeyboardButton(text=_msg(lang, "menu.choose_pro"), callback_data="plan_select:PRO")],
        [InlineKeyboardButton(text=_msg(lang, "menu.my_plan"), callback_data="product:account")],
        [InlineKeyboardButton(text=_msg(lang, "menu.back"), callback_data="product:menu")],
    ])


def upgrade_confirm_screen(lang: str, plan: dict[str, Any]) -> str:
    plan_name = str(plan.get("name") or "PRO").upper()
    price = float(plan.get("price") or 0)
    return "\n".join([
        f"{PLAN_ICONS.get(plan_name, '💎')} {_msg(lang, 'upgrade.to')} {plan_name}",
        "",
        _msg(lang, "upgrade.will_get"),
        _msg(lang, "upgrade.up_to_rules", v=plan.get("max_rules")),
        _msg(lang, "upgrade.up_to_videos", v=plan.get("max_video_per_day")),
        _msg(lang, "upgrade.up_to_jobs", v=plan.get("max_jobs_per_day")),
        _msg(lang, "upgrade.priority"),
        "",
        _msg(lang, "upgrade.price", v=price),
        "",
        _msg(lang, "upgrade.create_invoice"),
    ])


def upgrade_confirm_keyboard(lang: str, plan_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=_msg(lang, "menu.create_invoice"), callback_data=f"plan_confirm:{plan_name}")],
        [InlineKeyboardButton(text=_msg(lang, "menu.back"), callback_data="product:plans")],
    ])


def invoice_screen(*, lang: str, invoice: dict[str, Any], items: list[dict[str, Any]]) -> str:
    period = _fmt_period(invoice.get("period_start"), invoice.get("period_end"), lang)
    plan_name = str((items[0].get("metadata") or {}).get("plan_name") if items else "") or "UNKNOWN"
    lines = [f"• {item.get('description')} — {float(item.get('amount') or 0):.2f} {invoice.get('currency') or 'USD'}" for item in items] or ["• —"]
    return "\n".join([
        _msg(lang, "invoice.title", id=invoice.get("id")),
        "",
        f"📌 {_msg(lang, 'label.status')}: {_status(lang, invoice.get('status'))}",
        f"💎 {_msg(lang, 'label.plan')}: {plan_name}",
        f"📅 {_msg(lang, 'label.period')}: {period}",
        "",
        _msg(lang, "invoice.items"),
        *lines,
        "",
        _msg(lang, "invoice.total", v=float(invoice.get("total") or 0), ccy=(invoice.get("currency") or "USD")),
        "",
        _msg(lang, "invoice.next_step"),
    ])


def invoice_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=_msg(lang, "menu.pay"), callback_data="invoice:pay")],
        [InlineKeyboardButton(text=_msg(lang, "menu.plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=_msg(lang, "menu.back"), callback_data="product:menu")],
    ])


def payment_stub_screen(lang: str) -> str:
    return _msg(lang, "payment.stub")


def usage_screen(*, lang: str, today: dict[str, Any], period: dict[str, Any], limits: dict[str, Any]) -> str:
    jobs = int(today.get("jobs_count") or 0)
    videos = int(today.get("video_count") or 0)
    jobs_limit = int(limits.get("max_jobs_per_day") or 0)
    videos_limit = int(limits.get("max_video_per_day") or 0)
    storage = int(period.get("storage_used_mb") or 0)
    return "\n".join([
        _msg(lang, "usage.title"),
        "",
        _msg(lang, "usage.today"),
        f"📨 {_msg(lang, 'usage.jobs')}: {jobs} / {jobs_limit} {_progress(jobs, jobs_limit)}",
        f"🎬 {_msg(lang, 'usage.videos')}: {videos} / {videos_limit} {_progress(videos, videos_limit)}",
        "",
        _msg(lang, "usage.billing_period"),
        f"📨 {_msg(lang, 'usage.jobs')}: {int(period.get('jobs_count') or 0):,}",
        f"🎬 {_msg(lang, 'usage.videos')}: {int(period.get('video_count') or 0):,}",
        f"💾 {_msg(lang, 'usage.storage')}: {storage:,} MB",
        "",
        f"{_msg(lang, 'label.status')}: {_msg(lang, 'usage.status_ok')}",
    ])


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=_msg("ru", "language.ru"), callback_data="lang:ru")],
        [InlineKeyboardButton(text=_msg("en", "language.en"), callback_data="lang:en")],
        [InlineKeyboardButton(text=_msg("es", "language.es"), callback_data="lang:es")],
        [InlineKeyboardButton(text=_msg("de", "language.de"), callback_data="lang:de")],
        [InlineKeyboardButton(text=_msg("pt", "language.pt"), callback_data="lang:pt")],
    ])


def help_screen(lang: str) -> str:
    return _msg(lang, "help.text")


def start_screen(lang: str, is_new: bool) -> str:
    if not is_new:
        return _msg(lang, "start.back")
    return _msg(lang, "start.new")


def start_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=_msg(lang, "menu.add_channel"), callback_data="start:add_channel")],
        [InlineKeyboardButton(text=_msg(lang, "menu.create_rule"), callback_data="start:create_rule")],
        [InlineKeyboardButton(text=_msg(lang, "menu.plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=_msg(lang, "menu.language"), callback_data="product:language")],
    ])


def rule_limit_error(lang: str, plan_name: str, allowed_rules: int, created_rules: int) -> str:
    return _msg(lang, "limit.rule", plan_name=plan_name, allowed_rules=allowed_rules, created_rules=created_rules)


def video_limit_error(lang: str, plan_name: str, used: int, limit: int) -> str:
    return _msg(lang, "limit.video", plan_name=plan_name, used=used, limit=limit)


def limit_error_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=_msg(lang, "menu.view_plans"), callback_data="product:plans")],
        [InlineKeyboardButton(text=_msg(lang, "menu.back"), callback_data="product:menu")],
    ])


def build_upgrade_invoice_flow(*, plan_name: str, price: float) -> dict[str, Any]:
    return {
        "item_type": "base_plan",
        "description": f"Тариф {plan_name}",
        "quantity": 1,
        "unit_price": round(float(price), 2),
    }
