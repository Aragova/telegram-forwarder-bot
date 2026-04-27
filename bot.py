#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import argparse
import json
import time
from html import escape
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BotCommandScopeDefault,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    FSInputFile,
)

from app.config import settings
from app.repository_models import utc_now_iso
from app.repository import RepositoryProtocol
from app.repository_factory import create_repository
from app.transport import wrap_bot, wrap_telethon_client
from app.transport_policy import (
    build_reaction_policy,
    build_sender_bot_policy,
    build_sender_telethon_policy,
)
from app.keyboards import (
    get_cancel_keyboard,
    get_channel_type_keyboard,
    get_channels_menu,
    get_diagnostics_menu,
    get_entity_kind_keyboard,
    get_main_menu,
    get_queue_menu,
    get_reset_queue_menu,
    get_rules_menu,
    get_start_keyboard,
    get_system_menu,
)
from app.logging_setup import setup_logging
from app.parser import parse_channel_history, parse_group_history
from app.sender import SenderService
telethon_client = None
from app.telegram_client import create_telethon_client, create_reaction_clients
from app.ui_error_policy import UIErrorPolicy
from app.scheduler_service import SchedulerService
from app.runtime_roles import normalize_runtime_role, run_role as run_runtime_role
from app.health_service import get_system_health, update_heartbeat
from app.ops_health_service import build_operational_snapshot
from app.preflight_checks import PreflightError, run_preflight_checks
from app.runtime_context import RuntimeContext
from app.worker_runtime import run_heavy_worker, run_light_worker
from app.scheduler_runtime import run_scheduler_loop
from app.job_watchdog import run_watchdog_loop
from app.tenant_service import TenantService
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService
from app.limit_service import LimitService
from app.invoice_service import InvoiceService
from app.billing_service import BillingService
from app.payment_service import PaymentService
from app.saas_bootstrap import ensure_owner_and_default_tenant_bootstrap
from app.i18n import get_user_language, set_user_language, t as tr
from app import product_ui
from app import access_control, user_ui

logger = setup_logging(settings.log_level)

dp = Dispatcher()
bot: Bot | None = None
db: RepositoryProtocol = create_repository()
scheduler_service = SchedulerService(db)
tenant_service = TenantService(db)
subscription_service = SubscriptionService(db)
usage_service = UsageService(db)
limit_service = LimitService(db, subscription_service, usage_service)
invoice_service = InvoiceService(db)
billing_service = BillingService(db)
payment_service = PaymentService(db)
telethon_client = None
reaction_clients = []
sender_service = None
runtime_context: RuntimeContext | None = None
posting_active = False
job_worker_tasks: list[asyncio.Task] = []
job_workers_stop_event: asyncio.Event | None = None
scheduler_runtime_task: asyncio.Task | None = None
job_watchdog_task: asyncio.Task | None = None
workers_runtime_enabled = True
user_states: dict[int, dict[str, Any]] = {}
dashboard_tasks: dict[int, asyncio.Task] = {}
ui_policy: UIErrorPolicy | None = None
last_notifications: dict[str, datetime] = {}
rule_ui_tasks: dict[str, asyncio.Task] = {}

preview_queue_cache: dict[int, dict[str, Any]] = {}
preview_busy_users: set[int] = set()
PREVIEW_CACHE_TTL_SECONDS = 300

USER_TZ = timezone(timedelta(hours=3))
RULES_PAGE_SIZE = 8
TG_TEXT_SOFT_LIMIT = 3900
DEFAULT_TELEGRAM_COMMANDS = [
    BotCommand(command="start", description="Старт"),
    BotCommand(command="menu", description="Главное меню"),
    BotCommand(command="help", description="Помощь"),
]

MENU_NAVIGATION_TEXTS = {
    "📋 Меню",
    "🔙 Главное меню",
    "⬅️ Назад в меню",
    "❌ Отмена",
    "📈 Живой статус",
    "🔄 Правила",
    "📡 Каналы",
    "📦 Очередь",
    "⚠️ Диагностика",
    "⚙️ Система",
    "💎 Тарифы",
    "👤 Мой аккаунт",
    "📡 Источники",
    "🎯 Получатели",
    "⚙️ Мои правила",
    "📊 Статус",
    "🌐 Язык",
    "📈 Использование",
    "🧾 Счета",
    "🧾 Мои счета",
    "💳 Оплата",
    "🆘 Поддержка",
    "📜 Список",
    "📜 Список правил",
    "📜 Список каналов",
    "📜 Мои источники",
    "📜 Мои получатели",
    "➕ Добавить канал",
    "➕ Добавить источник",
    "➕ Добавить получатель",
    "➖ Канал",
    "➖ Удалить канал",
    "➖ Удалить источник",
    "➖ Удалить получатель",
    "➕ Добавить правило",
    "⚠️ Проблемные доставки",
    "📊 Журнал системы",
    "▶️ Запуск",
    "▶️ Запустить пересылку",
    "⏸ Стоп",
    "⏸ Остановить пересылку",
    "🔄 Сброс",
    "🔄 Сбросить всё",
    "📊 Сброс по источнику",
}

MENU_NAVIGATION_PREFIXES = (
    "📤 ",
    "📥 ",
)


def is_menu_navigation_text(text: str | None) -> bool:
    value = (text or "").strip()
    if not value:
        return False
    if value in MENU_NAVIGATION_TEXTS:
        return True
    return any(value.startswith(prefix) for prefix in MENU_NAVIGATION_PREFIXES)


def reset_user_state(user_id: int | None) -> None:
    if user_id is None:
        return
    user_states.pop(user_id, None)

def _rule_ui_task_key(kind: str, rule_id: int) -> str:
    return f"{kind}:{rule_id}"


def _schedule_rule_ui_task(task_key: str, coro) -> bool:
    existing = rule_ui_tasks.get(task_key)
    if existing and not existing.done():
        return False

    task = asyncio.create_task(coro)
    rule_ui_tasks[task_key] = task

    def _cleanup(done_task: asyncio.Task) -> None:
        current = rule_ui_tasks.get(task_key)
        if current is done_task:
            rule_ui_tasks.pop(task_key, None)

        try:
            done_task.result()
        except Exception as exc:
            logger.exception("Фоновая UI-задача %s завершилась с ошибкой: %s", task_key, exc)

    task.add_done_callback(_cleanup)
    return True

@dataclass(slots=True)
class RuleCardCacheEntry:
    text: str
    keyboard: InlineKeyboardMarkup
    created_at_monotonic: float


RULE_CARD_CACHE_TTL_SEC = 15.0
rule_card_cache: dict[int, RuleCardCacheEntry] = {}
rule_card_build_locks: dict[int, asyncio.Lock] = {}

RULE_REFRESH_DEBOUNCE_SEC = 1.5
rule_refresh_inflight: set[int] = set()
rule_refresh_last_ts: dict[int, float] = {}

rule_card_open_inflight: set[int] = set()
rule_card_open_last_ts: dict[int, float] = {}

rule_to_list_last_ts: dict[str, float] = {}


def invalidate_rule_card_cache(rule_id: int) -> None:
    rule_card_cache.pop(int(rule_id), None)


def invalidate_rule_card_cache_many(rule_ids: list[int] | tuple[int, ...] | set[int]) -> None:
    for rule_id in rule_ids:
        invalidate_rule_card_cache(int(rule_id))


def _get_rule_card_build_lock(rule_id: int) -> asyncio.Lock:
    rule_id = int(rule_id)
    lock = rule_card_build_locks.get(rule_id)
    if lock is None:
        lock = asyncio.Lock()
        rule_card_build_locks[rule_id] = lock
    return lock


def _get_cached_rule_card(rule_id: int) -> RuleCardCacheEntry | None:
    entry = rule_card_cache.get(int(rule_id))
    if not entry:
        return None

    age = time.monotonic() - entry.created_at_monotonic
    if age > RULE_CARD_CACHE_TTL_SEC:
        rule_card_cache.pop(int(rule_id), None)
        return None

    return entry

def _is_debounce_active(storage: dict[Any, float], key: Any, cooldown_sec: float) -> bool:
    now = time.monotonic()
    last_ts = storage.get(key, 0.0)
    return (now - last_ts) < cooldown_sec


def _mark_debounce(storage: dict[Any, float], key: Any) -> None:
    storage[key] = time.monotonic()

async def run_db(callable_obj, *args, **kwargs):
    """
    Уводит sync DB/CPU работу из event loop в thread pool.
    """
    return await asyncio.to_thread(callable_obj, *args, **kwargs)


async def heartbeat_loop(role: str, repo: RepositoryProtocol):
    while True:
        try:
            await run_db(update_heartbeat, repo, role)
        except Exception as exc:
            logger.warning("HEARTBEAT | ошибка обновления роли %s: %s", role, exc)
        await asyncio.sleep(5)


def _fmt_health_status(status: str) -> str:
    return "🟢 работает" if status == "ok" else "🔴 не отвечает"


async def watchdog_loop(
    repo: RepositoryProtocol,
    *,
    startup_grace_seconds: float = 20.0,
):
    last_state: dict[str, str] = {}
    started_at = time.monotonic()

    while True:
        try:
            health = await run_db(get_system_health, repo)
            roles = health.get("roles") or {}
            in_startup_grace = (time.monotonic() - started_at) < startup_grace_seconds

            for role, state in roles.items():
                if state == "down" and last_state.get(role) != "down" and not in_startup_grace:
                    await notify_admin_once(
                        f"role_{role}_down",
                        f"❌ Роль {role} не отвечает",
                        problem_type="runtime_role_down",
                    )

            last_state = dict(roles)
        except Exception as exc:
            logger.warning("WATCHDOG | ошибка проверки health: %s", exc)

        await asyncio.sleep(10)

async def build_rule_card_payload_cached(rule_id: int) -> tuple[str | None, InlineKeyboardMarkup | None, str]:
    cached = _get_cached_rule_card(rule_id)
    if cached:
        return cached.text, cached.keyboard, "cache_hit"

    lock = _get_rule_card_build_lock(rule_id)

    async with lock:
        cached = _get_cached_rule_card(rule_id)
        if cached:
            return cached.text, cached.keyboard, "cache_hit_after_wait"

        render = await _build_rule_card_render_async(rule_id)
        if not render:
            return None, None, "rule_not_found"

        entry = RuleCardCacheEntry(
            text=render["text"],
            keyboard=render["reply_markup"],
            created_at_monotonic=time.monotonic(),
        )
        rule_card_cache[int(rule_id)] = entry

        return entry.text, entry.keyboard, "rebuilt"

async def _show_rule_processing_screen(
    *,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup=None,
    parse_mode: str | None = None,
) -> None:
    try:
        if ui_policy:
            await ui_policy.edit_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
            return
    except Exception as exc:
        logger.exception("Не удалось показать экран обработки chat_id=%s message_id=%s: %s", chat_id, message_id, exc)


async def _run_rescan_rule_fresh_job(
    *,
    rule_id: int,
    chat_id: int,
    message_id: int,
    admin_id: int,
) -> None:
    try:
        rule = await run_db(db.get_rule, rule_id)
        if not rule:
            await _show_rule_processing_screen(
                chat_id=chat_id,
                message_id=message_id,
                text=f"❌ Правило #{rule_id} не найдено",
            )
            return

        await run_db(db.clear_rule_deliveries, rule_id)

        if rule.source_thread_id is None:
            parsed_count = await parse_channel_history(
                telethon_client,
                db,
                str(rule.source_id),
                clean_start=True,
            )
        else:
            parsed_count = await parse_group_history(
                telethon_client,
                db,
                str(rule.source_id),
                int(rule.source_thread_id),
                clean_start=True,
            )

        rebuilt_count = await run_db(db.backfill_rule, rule_id)
        invalidate_preview_cache(rule_id)
        await run_db(scheduler_service.set_next_run, rule_id, utc_now_iso())

        await run_db(
            db.log_rule_change,
            event_type="rule_rescanned_fresh",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value=None,
            new_value={
                "next_run_at": utc_now_iso(),
            },
            extra={
                "mode": "fresh",
                "parsed_count": parsed_count,
                "rebuilt_count": rebuilt_count,
                "source_id": str(rule.source_id),
                "source_thread_id": rule.source_thread_id,
            },
        )

        await ensure_rule_workers()

        invalidate_rule_card_cache(rule_id)
        refreshed = await refresh_rule_card_by_ids(
            chat_id=chat_id,
            message_id=message_id,
            rule_id=rule_id,
            prefix_text=(
                f"✅ Правило #{rule_id} пересканировано заново.\n"
                f"Новых/обновлённых постов источника: {parsed_count}\n"
                f"Очередь правила собрана заново: {rebuilt_count}"
            ),
        )

        if not refreshed:
            row = get_rule_stats_row(rule_id)
            if row:
                await send_message_safe(
                    chat_id=chat_id,
                    text=build_rule_card_text(row),
                    parse_mode="HTML",
                    reply_markup=build_rule_card_keyboard(
                        rule_id,
                        bool(row["is_active"]),
                        row["schedule_mode"] or "interval",
                        row["mode"] or "repost",
                    ),
                )
            else:
                await send_message_safe(
                    chat_id=chat_id,
                    text=(
                        f"✅ Правило #{rule_id} пересканировано заново.\n"
                        f"Новых/обновлённых постов источника: {parsed_count}\n"
                        f"Очередь правила собрана заново: {rebuilt_count}"
                    ),
                )

    except Exception as exc:
        logger.exception("Ошибка полного перескана правила %s: %s", rule_id, exc)

        await _show_rule_processing_screen(
            chat_id=chat_id,
            message_id=message_id,
            text=(
                f"❌ Не удалось пересканировать правило #{rule_id} заново.\n\n"
                f"Причина: {str(exc)[:300]}"
            ),
            reply_markup=build_rescan_rule_keyboard(rule_id),
        )


async def _run_rescan_rule_keep_job(
    *,
    rule_id: int,
    chat_id: int,
    message_id: int,
    admin_id: int,
) -> None:
    try:
        rule = await run_db(db.get_rule, rule_id)
        if not rule:
            await _show_rule_processing_screen(
                chat_id=chat_id,
                message_id=message_id,
                text=f"❌ Правило #{rule_id} не найдено",
            )
            return

        saved_sent_message_ids = await run_db(db.get_rule_sent_message_ids, rule_id)
        saved_first_pending_message_id = await run_db(db.get_rule_first_pending_message_id, rule_id)
        old_next_run_at = await run_db(db.get_rule_next_run_at, rule_id)

        await run_db(db.clear_rule_deliveries, rule_id)

        if rule.source_thread_id is None:
            parsed_count = await parse_channel_history(
                telethon_client,
                db,
                str(rule.source_id),
                clean_start=True,
            )
        else:
            parsed_count = await parse_group_history(
                telethon_client,
                db,
                str(rule.source_id),
                int(rule.source_thread_id),
                clean_start=True,
            )

        rebuilt_count = await run_db(db.backfill_rule, rule_id)
        invalidate_preview_cache(rule_id)

        restored_sent_count = await run_db(
            db.mark_rule_messages_sent,
            rule_id=rule_id,
            source_channel=str(rule.source_id),
            source_thread_id=rule.source_thread_id,
            message_ids=saved_sent_message_ids,
        )

        removed_before = 0
        if saved_first_pending_message_id is not None:
            removed_before = await run_db(
                db.drop_rule_pending_before_message,
                rule_id=rule_id,
                source_channel=str(rule.source_id),
                source_thread_id=rule.source_thread_id,
                message_id=saved_first_pending_message_id,
            )

        await run_db(scheduler_service.set_next_run, rule_id, old_next_run_at)

        await run_db(
            db.log_rule_change,
            event_type="rule_rescanned_keep_position",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "saved_sent_message_ids": saved_sent_message_ids,
                "saved_first_pending_message_id": saved_first_pending_message_id,
                "old_next_run_at": old_next_run_at,
            },
            new_value={
                "next_run_at": old_next_run_at,
            },
            extra={
                "mode": "keep_position",
                "parsed_count": parsed_count,
                "rebuilt_count": rebuilt_count,
                "restored_sent_count": restored_sent_count,
                "removed_before": removed_before,
                "source_id": str(rule.source_id),
                "source_thread_id": rule.source_thread_id,
            },
        )

        await ensure_rule_workers()

        invalidate_rule_card_cache(rule_id)
        refreshed = await refresh_rule_card_by_ids(
            chat_id=chat_id,
            message_id=message_id,
            rule_id=rule_id,
            prefix_text=(
                f"✅ Правило #{rule_id} пересканировано с сохранением позиции.\n"
                f"Найдено постов источника: {parsed_count}\n"
                f"Очередь пересобрана: {rebuilt_count}\n"
                f"Восстановлено отправленных: {restored_sent_count}\n"
                f"Убрано pending до нужной точки: {removed_before}"
            ),
        )

        if not refreshed:
            row = get_rule_stats_row(rule_id)
            if row:
                await send_message_safe(
                    chat_id=chat_id,
                    text=build_rule_card_text(row),
                    parse_mode="HTML",
                    reply_markup=build_rule_card_keyboard(
                        rule_id,
                        bool(row["is_active"]),
                        row["schedule_mode"] or "interval",
                        row["mode"] or "repost",
                    ),
                )
            else:
                await send_message_safe(
                    chat_id=chat_id,
                    text=(
                        f"✅ Правило #{rule_id} пересканировано с сохранением позиции.\n"
                        f"Найдено постов источника: {parsed_count}\n"
                        f"Очередь пересобрана: {rebuilt_count}\n"
                        f"Восстановлено отправленных: {restored_sent_count}\n"
                        f"Убрано pending до нужной точки: {removed_before}"
                    ),
                )

    except Exception as exc:
        logger.exception("Ошибка перескана правила %s с сохранением позиции: %s", rule_id, exc)

        await _show_rule_processing_screen(
            chat_id=chat_id,
            message_id=message_id,
            text=(
                f"❌ Не удалось пересканировать правило #{rule_id} с сохранением позиции.\n\n"
                f"Причина: {str(exc)[:300]}"
            ),
            reply_markup=build_rescan_rule_keyboard(rule_id),
        )

@dataclass(slots=True)
class ChannelChoice:
    channel_id: str
    thread_id: int | None
    title: str


async def is_admin(message: Message) -> bool:
    if message.from_user and message.from_user.id == settings.admin_id:
        return True

    await message.reply("⛔ Нет прав")
    return False


async def is_admin_callback(callback: CallbackQuery) -> bool:
    if callback.from_user and callback.from_user.id == settings.admin_id:
        return True

    await answer_callback_safe(callback, "⛔ Нет прав", show_alert=True)
    return False


def _resolve_language(user_id: int | None) -> str:
    if user_id is None:
        return "ru"
    return get_user_language(int(user_id), db)


def _is_admin_user(user_id: int | None) -> bool:
    return access_control.is_admin_user(user_id)

def is_admin_user(user_id: int | None) -> bool:
    return access_control.is_admin_user(user_id)


def get_current_tenant_for_user(user_id: int) -> int:
    return access_control.get_current_tenant_for_user(user_id, tenant_service)


def ensure_user_tenant(user_id: int) -> int:
    return access_control.ensure_user_tenant(user_id, tenant_service)


def is_rule_owned_by_user(rule_id: int, user_id: int) -> bool:
    return access_control.is_rule_owned_by_user(rule_id, user_id, db, tenant_service)


def is_channel_owned_by_user(
    channel_id: str,
    thread_id: int | None,
    channel_type: str,
    user_id: int,
) -> bool:
    return access_control.is_channel_owned_by_user(
        channel_id,
        thread_id,
        channel_type,
        user_id,
        db,
        tenant_service,
    )


def _public_user_menu_text() -> str:
    return user_ui.build_user_main_text()


def _public_user_menu_keyboard() -> InlineKeyboardMarkup:
    return user_ui.build_user_main_keyboard()


def _public_user_back_keyboard() -> InlineKeyboardMarkup:
    return user_ui.build_user_back_keyboard()


async def _show_public_user_menu_message(message: Message) -> None:
    await message.reply(_public_user_menu_text(), reply_markup=_public_user_menu_keyboard())


def _build_public_account_text(
    *,
    user_id: int,
    tenant_id: int,
    subscription: dict[str, Any] | None,
    usage_today: dict[str, Any] | None,
    rules_count: int,
) -> str:
    return user_ui.build_user_account_text(
        user_id=user_id,
        tenant_id=tenant_id,
        subscription=subscription,
        usage_today=usage_today,
        rules_count=rules_count,
    )


def _public_account_keyboard() -> InlineKeyboardMarkup:
    return user_ui.build_user_account_keyboard()


def _user_sources_keyboard() -> InlineKeyboardMarkup:
    return user_ui.build_user_sources_keyboard()


def _user_targets_keyboard() -> InlineKeyboardMarkup:
    return user_ui.build_user_targets_keyboard()


def _build_public_plans_text() -> str:
    plans = [item for item in _default_plan_catalog("ru") if str(item.get("name")).upper() != "OWNER"]
    return user_ui.build_user_plans_text(plans)


def _public_plans_keyboard() -> InlineKeyboardMarkup:
    return user_ui.build_user_plans_keyboard()


def _public_invoice_keyboard(invoice_id: int) -> InlineKeyboardMarkup:
    return user_ui.build_user_invoice_keyboard(invoice_id)


def _invoice_plan_name(invoice: dict[str, Any], items: list[dict[str, Any]]) -> str:
    for item in items:
        meta = item.get("metadata_json") or {}
        if isinstance(meta, dict) and meta.get("plan_name"):
            return str(meta.get("plan_name")).upper()
    for key in ("plan_name", "selected_plan"):
        if invoice.get(key):
            return str(invoice.get(key)).upper()
    return "UNKNOWN"


def _build_user_invoice_payload(invoice: dict[str, Any]) -> dict[str, Any]:
    invoice_id = int(invoice.get("id") or 0)
    items = db.list_invoice_items(invoice_id) if hasattr(db, "list_invoice_items") else []
    return {"invoice": invoice, "items": items}


def _get_user_invoices_payload(tenant_id: int, limit: int = 10) -> list[dict[str, Any]]:
    if not hasattr(db, "list_invoices_for_tenant"):
        last_invoice = db.get_last_invoice(int(tenant_id)) if hasattr(db, "get_last_invoice") else None
        invoices = [last_invoice] if last_invoice else []
    else:
        invoices = db.list_invoices_for_tenant(int(tenant_id), limit=int(limit))
    payload: list[dict[str, Any]] = []
    for invoice in invoices:
        item_rows = db.list_invoice_items(int(invoice.get("id") or 0)) if hasattr(db, "list_invoice_items") else []
        payload.append({**invoice, "items": item_rows})
    return payload


def _default_plan_catalog(lang: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "FREE",
            "description": "Для теста и маленьких каналов" if lang == "ru" else "For testing and small channels",
            "max_rules": 3,
            "max_video_per_day": 5,
            "max_jobs_per_day": 100,
            "price": 0,
        },
        {
            "name": "BASIC",
            "description": "Для стабильной автопубликации" if lang == "ru" else "For stable autopublishing",
            "max_rules": 15,
            "max_video_per_day": 30,
            "max_jobs_per_day": 1000,
            "price": 9,
        },
        {
            "name": "PRO",
            "description": "Для больших каналов и видео" if lang == "ru" else "For large channels and video workflows",
            "max_rules": 50,
            "max_video_per_day": 100,
            "max_jobs_per_day": 5000,
            "price": 29,
        },
    ]


def _get_plan_info(plan_name: str, lang: str) -> dict[str, Any]:
    normalized = str(plan_name or "FREE").upper()
    for item in _default_plan_catalog(lang):
        if item["name"] == normalized:
            return item
    return _default_plan_catalog(lang)[0]


def _build_invoice_for_plan_sync(tenant_id: int, plan_name: str) -> dict[str, Any] | None:
    sub = subscription_service.get_active_subscription(int(tenant_id))
    if not sub:
        return None
    sub = billing_service.ensure_billing_period(sub)
    plan = _get_plan_info(plan_name, "ru")
    invoice_id = invoice_service.create_draft_invoice(
        int(tenant_id),
        int(sub.get("id") or 0),
        str(sub.get("current_period_start")),
        str(sub.get("current_period_end")),
        currency="USD",
    )
    if not invoice_id:
        return None
    flow_item = product_ui.build_upgrade_invoice_flow(plan_name=plan_name, price=float(plan.get("price") or 0))
    invoice_service.add_invoice_item(
        int(invoice_id),
        item_type=str(flow_item["item_type"]),
        description=str(flow_item["description"]),
        quantity=int(flow_item["quantity"]),
        unit_price=float(flow_item["unit_price"]),
        metadata={"plan_name": str(plan_name).upper()},
    )
    invoice_service.finalize_invoice(int(invoice_id))
    invoice = db.get_invoice(int(invoice_id)) if hasattr(db, "get_invoice") else None
    items = db.list_invoice_items(int(invoice_id)) if hasattr(db, "list_invoice_items") else []
    return {"invoice": invoice, "items": items}


def interval_to_text(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}с"
    if seconds < 3600:
        return f"{seconds // 60}м"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}ч"
    return f"{seconds}s"


def build_user_rules_keyboard(rules, page: int = 0) -> InlineKeyboardMarkup:
    total = len(rules)
    if total == 0:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")]])
    max_page = (total - 1) // RULES_PAGE_SIZE
    page = max(0, min(page, max_page))
    start = page * RULES_PAGE_SIZE
    current = rules[start:start + RULES_PAGE_SIZE]
    rows: list[list[InlineKeyboardButton]] = []
    for row in current:
        rid = int(getattr(row, "id", 0))
        label = compact_rule_text(row)
        state = "▶️" if bool(getattr(row, "is_active", False)) else "⏸"
        rows.append([InlineKeyboardButton(text=f"{state} #{rid} {label}", callback_data=f"user_rule_open:{rid}")])
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"user_rules_page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{max_page + 1}", callback_data="user_rules_noop"))
    if page < max_page:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"user_rules_page:{page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="➕ Добавить правило", callback_data="user_rules_add")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_user_rule_card(rule) -> tuple[str, InlineKeyboardMarkup]:
    rid = int(getattr(rule, "id", 0))
    text = (
        f"⚙️ Правило #{rid}\n\n"
        f"Источник: {getattr(rule, 'source_title', None) or getattr(rule, 'source_id', '')}\n"
        f"Получатель: {getattr(rule, 'target_title', None) or getattr(rule, 'target_id', '')}\n"
        f"Интервал: {interval_to_text(int(getattr(rule, 'interval', 0) or 0))}\n"
        f"Статус: {'активно' if bool(getattr(rule, 'is_active', False)) else 'остановлено'}"
    )
    toggle = "⏸ Остановить" if bool(getattr(rule, "is_active", False)) else "▶️ Запустить"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle, callback_data=f"user_rule_toggle:{rid}")],
        [InlineKeyboardButton(text="⏱ Интервал", callback_data=f"user_rule_interval:{rid}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"user_rule_delete:{rid}")],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data="user_rules")],
    ])
    return text, kb


def rule_label(row) -> str:
    src = row["source_title"] or row["source_id"]
    tgt = row["target_title"] or row["target_id"]
    if row["source_thread_id"]:
        src = f"{src} (тема {row['source_thread_id']})"
    if row["target_thread_id"]:
        tgt = f"{tgt} (тема {row['target_thread_id']})"
    state = "▶️" if row["is_active"] else "⏸"
    return f"{state} #{row['id']} {src} → {tgt} [{interval_to_text(int(row['interval']))}]"

def video_caption_delivery_mode_to_text(mode: str | None) -> str:
    normalized = (mode or "auto").strip().lower()

    if normalized == "copy_first":
        return "⚡ Обычный"
    if normalized == "builder_first":
        return "💎 Премиум"
    return "🤖 Авто"

def build_video_caption_menu_text(rule_id: int) -> str:
    rule = db.get_rule(rule_id)
    if not rule:
        return "❌ Правило не найдено"

    current_caption = (getattr(rule, "video_caption", None) or "").strip()
    current_mode = video_caption_delivery_mode_to_text(
        getattr(rule, "video_caption_delivery_mode", "auto")
    )

    if current_caption:
        caption_state = "— задана"
    else:
        caption_state = "— не задана"

    return (
        "📝 <b>Подпись видео</b>\n\n"
        f"Текущая подпись:\n"
        f"{caption_state}\n\n"
        f"Текущий режим:\n"
        f"{current_mode}\n\n"
        "Выберите действие:"
    )

def build_video_caption_menu_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Изменить",
                    callback_data=f"video_caption_edit:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎛 Режим подписи",
                    callback_data=f"video_caption_mode_menu:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Очистить",
                    callback_data=f"video_caption_clear:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад к правилу",
                    callback_data=f"rule_card:{rule_id}",
                )
            ],
        ]
    )

def compact_rule_text(rule) -> str:
    src = rule.source_title or rule.source_id
    tgt = rule.target_title or rule.target_id
    if rule.source_thread_id:
        src = f"{src} ({rule.source_thread_id})"
    if rule.target_thread_id:
        tgt = f"{tgt} ({rule.target_thread_id})"
    base = f"{src} → {tgt}"
    return (base[:57] + "...") if len(base) > 60 else base


def source_label(choice: ChannelChoice) -> str:
    return f"{choice.title}{f' (тема {choice.thread_id})' if choice.thread_id else ''}"
def parse_next_run_user_time(text: str) -> str | None:
    text = text.strip()

    try:
        hour, minute = map(int, text.split(":"))
    except Exception:
        return None

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    now_local = datetime.now(USER_TZ)
    target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if target_local <= now_local:
        target_local = target_local + timedelta(days=1)

    target_utc = target_local.astimezone(timezone.utc)
    return target_utc.isoformat()

def detect_message_media_kind_for_storage(message: Message) -> str:
    if message.video:
        return "video"

    if message.photo:
        return "image"

    if message.animation:
        return "video"

    if message.document:
        mime = (message.document.mime_type or "").lower()
        if mime.startswith("video/"):
            return "video"
        if mime.startswith("image/"):
            return "image"
        return "document"

    if message.audio:
        return "audio"

    return "text"

def normalize_fixed_times(times: list[str]) -> list[str]:
    normalized = []

    for raw in times:
        value = raw.strip()
        if not value:
            continue

        parts = value.split(":")
        if len(parts) != 2:
            continue

        try:
            hour = int(parts[0])
            minute = int(parts[1])
        except Exception:
            continue

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            continue

        normalized.append(f"{hour:02d}:{minute:02d}")

    return sorted(set(normalized))

def build_video_caption_mode_menu_text(rule_id: int) -> str:
    rule = db.get_rule(rule_id)
    if not rule:
        return "❌ Правило не найдено"

    current_mode = video_caption_delivery_mode_to_text(
        getattr(rule, "video_caption_delivery_mode", "auto")
    )

    return (
        "🎛 <b>Режим подписи</b>\n\n"
        "Выберите, как бот должен отправлять подпись видео.\n\n"
        "⚡ <b>Обычный</b>\n"
        "Самый быстрый режим. Подходит для простой подписи без premium-оформления.\n\n"
        "💎 <b>Премиум</b>\n"
        "Безопасный режим для custom emoji, жирного текста, ссылок и сложного оформления.\n\n"
        "🤖 <b>Авто</b>\n"
        "Бот сам определяет, нужна ли безопасная premium-отправка.\n\n"
        f"Текущий режим: {current_mode}"
    )

def build_dashboard_text() -> str:
    stats = db.get_queue_stats()
    health = get_system_health(db)
    next_rule = db.get_next_scheduled_rule()
    now_text = datetime.now(USER_TZ).strftime("%d.%m.%Y %H:%M:%S")
    live_workers = sum(1 for task in job_worker_tasks if not task.done())

    next_block = "⏭ **Ближайший пост:**\nне запланирован"

    if next_rule:
        src = next_rule["source_title"] or next_rule["source_id"]
        tgt = next_rule["target_title"] or next_rule["target_id"]

        if next_rule["source_thread_id"]:
            src = f"{src} (тема {next_rule['source_thread_id']})"
        if next_rule["target_thread_id"]:
            tgt = f"{tgt} (тема {next_rule['target_thread_id']})"

        next_run_text = format_next_run_user_time(next_rule["next_run_at"])
        state = "▶️" if next_rule["is_active"] else "⏸"

        next_block = (
            "⏭ **Ближайший пост:**\n"
            f"{state} Правило #{next_rule['id']}\n"
            f"{src} → {tgt}\n"
            f"{next_run_text}"
        )

    return (
        "📊 **ОБЗОР СИСТЕМЫ**\n\n"
        f"📦 Всего постов: {stats['posts']}\n"
        f"⏳ В очереди: {stats['pending']}\n"
        f"✅ Отправлено: {stats['sent']}\n"
        f"⚠️ С ошибками: {stats['faulty']}\n"
        f"🔄 Правил: {stats['rules']}\n"
        f"🤖 Воркеров: {live_workers}\n\n"
        "📊 **СОСТОЯНИЕ СИСТЕМЫ**\n"
        f"🤖 Бот: {_fmt_health_status(health['roles']['bot'])}\n"
        f"🧠 Планировщик: {_fmt_health_status(health['roles']['scheduler'])}\n"
        f"⚙️ Воркер: {_fmt_health_status(health['roles']['worker'])}\n\n"
        "📦 **ОЧЕРЕДЬ**\n"
        f"В ожидании: {health['pending']}\n"
        f"В обработке: {health['processing']}\n\n"
        f"⚠️ Ошибки за 5 минут: {health['errors']}\n\n"
        f"{next_block}\n\n"
        f"Статус: {'✅ РАБОТАЕТ' if posting_active else '⏸ ОСТАНОВЛЕН'}\n"
        f"🕒 Обновлено: {now_text}"
    )

def build_dashboard_keyboard(running: bool = True) -> InlineKeyboardMarkup:
    control_button = (
        InlineKeyboardButton(text="⏸ Пауза обновления", callback_data="dashboard_stop")
        if running
        else InlineKeyboardButton(text="▶️ Возобновить обновление", callback_data="dashboard_resume")
    )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Обновить", callback_data="dashboard_refresh"),
                control_button,
            ],
            [
                InlineKeyboardButton(text="⬅️ В меню", callback_data="dashboard_back"),
            ],
        ]
    )

async def dashboard_worker(user_id: int, message: Message):
    try:
        while True:
            await asyncio.sleep(30)

            text = await run_db(build_dashboard_text)

            try:
                await edit_message_text_safe(
                    message=message,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=build_dashboard_keyboard(running=True),
                )
            except Exception as exc:
                if "message is not modified" not in str(exc):
                    logger.exception("Ошибка обновления живого статуса: %s", exc)

    except asyncio.CancelledError:
        pass
    finally:
        dashboard_tasks.pop(user_id, None)

def format_next_run_user_time(next_run_at: str | None) -> str:
    if not next_run_at:
        return "не запланирован"

    try:
        dt_utc = datetime.fromisoformat(next_run_at)
        dt_local = dt_utc.astimezone(USER_TZ)
        return dt_local.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "ошибка времени"

def schedule_mode_to_text(row) -> str:
    mode = row["schedule_mode"] or "interval"

    if mode == "fixed":
        raw = row["fixed_times_json"]
        try:
            times = json.loads(raw) if raw else []
        except Exception:
            times = []

        if times:
            return f"Фиксированное ({', '.join(times)})"
        return "Фиксированное"

    return "Плавающее"

def rule_mode_label(row) -> str:
    mode = row["mode"] if "mode" in row.keys() else "repost"
    return "Видеоредактор" if mode == "video" else "Репост"

def rule_mode_to_text(row) -> str:
    mode = row["mode"] if "mode" in row.keys() else "repost"
    return "🎬 Видеоредактор" if mode == "video" else "🔁 Репост"

def caption_delivery_mode_to_text(mode: str | None) -> str:
    normalized = (mode or "auto").strip().lower()

    if normalized == "copy_first":
        return "Обычный"
    if normalized == "builder_first":
        return "Премиум"
    return "Автоматический"


def get_rule_caption_mode_value(rule_id: int, row=None) -> str:
    """
    Единая точка чтения режима подписи.

    ВАЖНО:
    - сначала читаем живое правило через db.get_rule(rule_id)
    - только если по какой-то причине не получилось, пробуем row
    - fallback всегда auto
    """
    try:
        rule = db.get_rule(rule_id)
        mode = getattr(rule, "caption_delivery_mode", None) if rule else None
        if mode:
            return str(mode).strip().lower()
    except Exception:
        logger.exception("Не удалось получить caption_delivery_mode из db.get_rule(rule_id=%s)", rule_id)

    try:
        if row is not None:
            if hasattr(row, "keys") and "caption_delivery_mode" in row.keys():
                mode = row["caption_delivery_mode"]
                if mode:
                    return str(mode).strip().lower()
    except Exception:
        logger.exception("Не удалось получить caption_delivery_mode из row для rule_id=%s", rule_id)

    return "auto"

def build_caption_mode_text(current_mode: str | None) -> str:
    current = caption_delivery_mode_to_text(current_mode)

    return (
        "✍️ <b>Режим подписи</b>\n\n"
        f"Текущий режим: <b>{safe_html(current)}</b>\n\n"
        "Обычный — сначала быстрый copy, потом fallback.\n"
        "Премиум — сразу builder / reupload для сохранения premium formatting.\n"
        "Автоматический — бот сам выбирает путь по содержимому поста."
    )

def build_rule_status_line(row) -> str:
    mode_label = rule_mode_label(row)
    is_active = bool(row["is_active"])
    processing = int(row["processing"] or 0)

    if not is_active:
        return f"⏸ Остановлен · {mode_label}"

    if processing > 0:
        return f"🟡 Обрабатывает · {mode_label}"

    return f"🟢 Работает · {mode_label}"

def build_rule_wait_line(row) -> str:
    is_active = bool(row["is_active"])
    pending = int(row["pending"] or 0)
    next_run_at = row["next_run_at"]

    if not is_active:
        return "⏸ Правило выключено"

    if pending <= 0:
        return "📭 Очередь закончена"

    if not next_run_at:
        return "⚡ Готов к отправке"

    try:
        dt_utc = datetime.fromisoformat(next_run_at)
        now_utc = datetime.now(timezone.utc)

        if dt_utc <= now_utc:
            return "⚡ Готов к отправке"

        dt_local = dt_utc.astimezone(USER_TZ)
        return f"🕒 Ждёт до {dt_local.strftime('%H:%M')}"
    except Exception:
        return "⚠️ Ошибка времени"

def video_intro_status_text(row) -> tuple[str, str]:
    horizontal_name = "—"
    vertical_name = "—"

    horizontal_id = row["video_intro_horizontal_id"] if "video_intro_horizontal_id" in row.keys() else None
    vertical_id = row["video_intro_vertical_id"] if "video_intro_vertical_id" in row.keys() else None

    if horizontal_id:
        intro = db.get_intro(int(horizontal_id))
        if intro:
            horizontal_name = intro.display_name

    if vertical_id:
        intro = db.get_intro(int(vertical_id))
        if intro:
            vertical_name = intro.display_name

    return horizontal_name, vertical_name

def video_caption_status_text(row) -> str:
    caption = row["video_caption"] if "video_caption" in row.keys() else None
    if caption and str(caption).strip():
        return "задана"
    return "не задана"

def safe_html(value: Any) -> str:
    return escape("" if value is None else str(value))

async def notify_admin_once(
    key: str,
    text: str,
    cooldown_sec: int = 300,
    problem_type: str = "generic",
    rule_id: int | None = None,
    delivery_id: int | None = None,
    extra: dict[str, Any] | None = None,
):
    state = await run_db(
        db.register_problem,
        problem_key=key,
        problem_type=problem_type,
        rule_id=rule_id,
        delivery_id=delivery_id,
        extra=extra,
    )

    if int(state.get("is_muted") or 0) == 1:
        return

    last_notified_at = state.get("last_notified_at")
    if last_notified_at:
        try:
            last_dt = datetime.fromisoformat(last_notified_at)
            now_dt = datetime.now(timezone.utc)
            if (now_dt - last_dt).total_seconds() < cooldown_sec:
                return
        except Exception:
            pass

    try:
        await send_message_safe(chat_id=settings.admin_id, text=text)
        await run_db(db.mark_problem_notified, key)
    except Exception as e:
        logger.exception("Ошибка отправки уведомления админу: %s", e)

def sanitize_intro_name(name: str | None) -> str | None:
    import re

    if not name:
        return None

    name = name.strip().lower()

    # Разрешаем буквы, цифры, пробел, _, -, кириллицу и латиницу
    name = re.sub(r"[^a-zA-Zа-яА-Я0-9 _-]", "", name)

    # Пробелы -> подчеркивания
    name = re.sub(r"\s+", "_", name)

    # Сжимаем повторяющиеся _
    name = re.sub(r"_+", "_", name)

    name = name.strip("_- ")

    return name[:60] if name else None


def make_unique_intro_filename(base_name: str, extension: str, intros_dir: str) -> str:
    import os

    candidate = f"{base_name}.{extension}"
    full_path = os.path.join(intros_dir, candidate)

    if not os.path.exists(full_path):
        return candidate

    counter = 2
    while True:
        candidate = f"{base_name}_{counter}.{extension}"
        full_path = os.path.join(intros_dir, candidate)
        if not os.path.exists(full_path):
            return candidate
        counter += 1

def serialize_message_entities(entities) -> str | None:
    if not entities:
        return None

    payload = []
    for entity in entities:
        item = {
            "type": entity.type,
            "offset": entity.offset,
            "length": entity.length,
        }

        if getattr(entity, "url", None):
            item["url"] = entity.url
        if getattr(entity, "user", None):
            item["user_id"] = entity.user.id
        if getattr(entity, "language", None):
            item["language"] = entity.language
        if getattr(entity, "custom_emoji_id", None):
            item["custom_emoji_id"] = entity.custom_emoji_id

        payload.append(item)

    return json.dumps(payload, ensure_ascii=False)

def get_rule_wait_reason(row) -> str:
    if not row["is_active"]:
        return "⏸ Правило выключено"

    pending = int(row["pending"] or 0)
    faulty = int(row["faulty"] or 0)
    next_run_at = row["next_run_at"]

    if pending <= 0:
        if faulty > 0:
            return "⚠️ Нет pending, есть ошибки"
        return "✅ Очередь по правилу закончилась"

    if not next_run_at:
        return "⚡ Готов к отправке"

    try:
        next_run_dt = datetime.fromisoformat(next_run_at)
        now_utc = datetime.now(timezone.utc)

        if next_run_dt <= now_utc:
            return "⚡ Готов к отправке"

        next_run_text = format_next_run_user_time(next_run_at)
        return f"🕒 Ждёт до {next_run_text}"
    except Exception:
        return "⚠️ Ошибка расчёта времени"

def paginate_items(items, page: int, page_size: int = RULES_PAGE_SIZE):
    total = len(items)
    if total == 0:
        return [], 0, 0

    total_pages = (total + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))

    start = page * page_size
    end = start + page_size
    return items[start:end], page, total_pages

def rules_inline_keyboard(rules, action: str, page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    prefix = {
        "disable": "disable_rule",
        "enable": "enable_rule",
        "delete": "delete_rule",
    }[action]

    page_rules, page, total_pages = paginate_items(rules, page)

    for rule in page_rules:
        rows.append([
            InlineKeyboardButton(
                text=compact_rule_text(rule),
                callback_data=f"{prefix}:{rule.id}",
            )
        ])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"rules_page:{action}:{page-1}",
                )
            )
        nav_row.append(
            InlineKeyboardButton(
                text=f"{page+1}/{total_pages}",
                callback_data="rules_page_info",
            )
        )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"rules_page:{action}:{page+1}",
                )
            )
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="rules_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def rules_next_run_keyboard(rules, page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    page_rules, page, total_pages = paginate_items(rules, page)

    for rule in page_rules:
        rows.append([
            InlineKeyboardButton(
                text=compact_rule_text(rule),
                callback_data=f"change_next_run:{rule.id}",
            )
        ])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"rules_page:next_run:{page-1}",
                )
            )
        nav_row.append(
            InlineKeyboardButton(
                text=f"{page+1}/{total_pages}",
                callback_data="rules_page_info",
            )
        )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"rules_page:next_run:{page+1}",
                )
            )
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="rules_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def rules_interval_keyboard(rules, page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    page_rules, page, total_pages = paginate_items(rules, page)

    for rule in page_rules:
        rows.append([
            InlineKeyboardButton(
                text=compact_rule_text(rule),
                callback_data=f"change_interval:{rule.id}",
            )
        ])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"rules_page:interval:{page-1}",
                )
            )
        nav_row.append(
            InlineKeyboardButton(
                text=f"{page+1}/{total_pages}",
                callback_data="rules_page_info",
            )
        )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"rules_page:interval:{page+1}",
                )
            )
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="rules_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def rules_trigger_now_keyboard(rules, page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    page_rules, page, total_pages = paginate_items(rules, page)

    for rule in page_rules:
        rows.append([
            InlineKeyboardButton(
                text=compact_rule_text(rule),
                callback_data=f"trigger_now:{rule.id}",
            )
        ])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"rules_page:trigger:{page-1}",
                )
            )
        nav_row.append(
            InlineKeyboardButton(
                text=f"{page+1}/{total_pages}",
                callback_data="rules_page_info",
            )
        )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"rules_page:trigger:{page+1}",
                )
            )
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="rules_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def rules_list_keyboard(rules, page: int = 0) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    page_rules, page, total_pages = paginate_items(rules, page)

    for rule in page_rules:
        rows.append([
            InlineKeyboardButton(
                text=rule_label({
                    "id": rule.id,
                    "source_id": rule.source_id,
                    "source_thread_id": rule.source_thread_id,
                    "target_id": rule.target_id,
                    "target_thread_id": rule.target_thread_id,
                    "interval": rule.interval,
                    "is_active": rule.is_active,
                    "source_title": rule.source_title,
                    "target_title": rule.target_title,
                })[:60],
                callback_data=f"rule_card:{rule.id}",
            )
        ])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"rules_page:list:{page-1}",
                )
            )
        nav_row.append(
            InlineKeyboardButton(
                text=f"{page+1}/{total_pages}",
                callback_data="rules_page_info",
            )
        )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"rules_page:list:{page+1}",
                )
            )
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="rules_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_rescan_rule_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="♻️ Сохранить позицию",
                    callback_data=f"rescan_rule_keep:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🆕 Начать заново",
                    callback_data=f"rescan_rule_fresh:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад к правилу",
                    callback_data=f"rule_card:{rule_id}",
                )
            ],
        ]
    )

def sources_inline_keyboard(sources: list[ChannelChoice]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, src in enumerate(sources):
        label = source_label(src)
        rows.append([
            InlineKeyboardButton(
                text=(label[:57] + "...") if len(label) > 60 else label,
                callback_data=f"reset_source:{idx}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="reset_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def ensure_rule_workers() -> None:
    # Историческое имя функции: теперь в проекте используются только job workers.
    await start_job_workers_runtime()

async def stop_all_workers() -> None:
    # Историческое имя функции: останавливаем текущий runtime job workers.
    await stop_job_workers_runtime()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id if message.from_user else settings.admin_id
    if _is_admin_user(user_id):
        reset_user_state(user_id)
        await message.reply("📋 Главное меню", reply_markup=get_main_menu())
        return

    tenant_before = await run_db(tenant_service.get_tenant_by_admin, user_id)
    tenant = await run_db(tenant_service.ensure_tenant_exists, user_id)
    tenant_id = int(tenant.get("id") or 0)
    if tenant_before is None:
        logger.info("Создан tenant для user_id=%s tenant_id=%s", user_id, tenant_id)
    else:
        logger.info("Получен tenant для user_id=%s tenant_id=%s", user_id, tenant_id)
    await message.answer("✅ Пользовательский режим включён", reply_markup=ReplyKeyboardRemove())
    await _show_public_user_menu_message(message)


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    await handle_start(message)


@dp.message(Command("language"))
async def cmd_language(message: Message):
    if not await is_admin(message):
        return
    lang = _resolve_language(message.from_user.id if message.from_user else None)
    await message.answer(tr("language.select", lang), reply_markup=product_ui.language_keyboard())


@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not await is_admin(message):
        return
    lang = _resolve_language(message.from_user.id if message.from_user else None)
    await message.answer(product_ui.help_screen(lang))


@dp.message(Command("plans"))
async def cmd_plans(message: Message):
    if not await is_admin(message):
        return
    lang = _resolve_language(message.from_user.id if message.from_user else None)
    await message.answer(
        product_ui.plans_screen(lang=lang, plans=_default_plan_catalog(lang)),
        reply_markup=product_ui.plans_keyboard(lang),
    )


@dp.message(Command("account"))
async def cmd_account(message: Message):
    if not await is_admin(message):
        return
    admin_id = message.from_user.id if message.from_user else settings.admin_id
    lang = _resolve_language(admin_id)
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    subscription = await run_db(subscription_service.get_active_subscription, tenant_id) or _get_plan_info("FREE", lang)
    usage_today = await run_db(usage_service.get_today_usage, tenant_id)
    billing = await run_db(billing_service.build_billing_summary, tenant_id)
    usage_period = billing.get("usage") or {}
    last_invoice = billing.get("last_invoice_summary")
    rules_count = await run_db(db.count_rules_for_tenant, tenant_id) if hasattr(db, "count_rules_for_tenant") else 0
    await message.answer(
        product_ui.account_screen(
            lang=lang,
            subscription=subscription,
            usage_today=usage_today,
            usage_period=usage_period,
            last_invoice=last_invoice,
            rules_count=int(rules_count or 0),
        ),
        reply_markup=product_ui.account_keyboard(lang),
    )


@dp.message(Command("plan"))
async def cmd_plan(message: Message):
    if not await is_admin(message):
        return
    admin_id = message.from_user.id if message.from_user else settings.admin_id
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    subscription = await run_db(subscription_service.get_active_subscription, int(tenant.get("id") or 1))
    if not subscription:
        await message.answer("❌ Подписка не назначена.")
        return
    await message.answer(
        "\n".join(
            [
                f"🏷 Тариф: {subscription.get('plan_name', 'UNKNOWN')}",
                f"📌 Статус подписки: {subscription.get('status', 'unknown')}",
                f"🧱 Лимит правил: {subscription.get('max_rules', '∞')}",
                f"⚙️ Лимит задач/день: {subscription.get('max_jobs_per_day', '∞')}",
                f"🎬 Лимит видео/день: {subscription.get('max_video_per_day', '∞')}",
            ]
        )
    )


@dp.message(Command("usage"))
async def cmd_usage(message: Message):
    if not await is_admin(message):
        return
    admin_id = message.from_user.id if message.from_user else settings.admin_id
    lang = _resolve_language(admin_id)
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    today = await run_db(usage_service.get_today_usage, tenant_id)
    billing = await run_db(billing_service.build_billing_summary, tenant_id)
    limits = await run_db(subscription_service.get_active_subscription, tenant_id) or {}
    await message.answer(product_ui.usage_screen(lang=lang, today=today, period=billing.get("usage") or {}, limits=limits))


@dp.message(Command("limits"))
async def cmd_limits(message: Message):
    if not await is_admin(message):
        return
    admin_id = message.from_user.id if message.from_user else settings.admin_id
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    can_rule, reason_rule = await run_db(limit_service.can_create_rule, tenant_id)
    can_job, reason_job = await run_db(limit_service.can_enqueue_job, tenant_id)
    can_video, reason_video = await run_db(limit_service.can_process_video, tenant_id)
    await message.answer(
        "\n".join(
            [
                "🚦 Статус лимитов:",
                f"• rules: {'ok' if can_rule else 'blocked'} {'' if can_rule else '- ' + str(reason_rule)}",
                f"• jobs/day: {'ok' if can_job else 'blocked'} {'' if can_job else '- ' + str(reason_job)}",
                f"• video/day: {'ok' if can_video else 'blocked'} {'' if can_video else '- ' + str(reason_video)}",
            ]
        )
    )


@dp.message(Command("subscription"))
async def cmd_subscription(message: Message):
    if not await is_admin(message):
        return
    admin_id = message.from_user.id if message.from_user else settings.admin_id
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    subscription = await run_db(subscription_service.get_active_subscription, tenant_id)
    if not subscription and hasattr(db, "get_latest_subscription"):
        subscription = await run_db(db.get_latest_subscription, tenant_id)
    if not subscription:
        await message.answer("❌ Подписка не назначена.")
        return
    await message.answer(
        "\n".join(
            [
                "🧾 Подписка tenant:",
                f"• План: {subscription.get('plan_name', 'UNKNOWN')}",
                f"• Статус: {subscription.get('status', 'unknown')}",
                f"• Старт: {subscription.get('started_at', '—')}",
                f"• Окончание: {subscription.get('expires_at', '—')}",
                f"• Grace до: {subscription.get('grace_ends_at', '—')}",
            ]
        )
    )


@dp.message(Command("billing"))
async def cmd_billing(message: Message):
    if not await is_admin(message):
        return
    admin_id = message.from_user.id if message.from_user else settings.admin_id
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    summary = await run_db(billing_service.build_billing_summary, tenant_id)
    events = await run_db(billing_service.get_recent_billing_events, tenant_id, 5)
    event_lines = [f"• {event.get('event_type')} ({event.get('created_at')})" for event in events] or ["• событий пока нет"]
    usage_snapshot = summary.get("usage") or {}
    overage_items = summary.get("overage_items") or []
    overage_lines = [f"• {item.get('description')}: {item.get('amount')} USD" for item in overage_items] or ["• нет"]
    await message.answer(
        "\n".join(
            [
                "💳 Billing summary:",
                f"• Период: {summary.get('period_start')} — {summary.get('period_end')}",
                f"• Тариф: {summary.get('plan_name')}",
                f"• Базовая цена: {summary.get('base_price')} USD",
                f"• jobs: {usage_snapshot.get('jobs_count', 0)}",
                f"• video: {usage_snapshot.get('video_count', 0)}",
                f"• storage_mb: {usage_snapshot.get('storage_used_mb', 0)}",
                "• Overage:",
                *overage_lines,
                f"• Прогноз суммы: {summary.get('forecast_total', 0)} USD",
                "🕘 Последние события:",
                *event_lines,
            ]
        )
    )


@dp.message(Command("invoice"))
async def cmd_invoice(message: Message):
    if not await is_admin(message):
        return
    admin_id = message.from_user.id if message.from_user else settings.admin_id
    lang = _resolve_language(admin_id)
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    summary = await run_db(billing_service.get_last_invoice_summary, tenant_id)
    if not summary:
        await message.answer("🧾 Счёт за текущий период ещё не создан." if lang == "ru" else "🧾 There is no invoice yet.")
        return
    invoice = summary.get("invoice") or {}
    items = summary.get("items") or []
    await message.answer(product_ui.invoice_screen(lang=lang, invoice=invoice, items=items), reply_markup=product_ui.invoice_keyboard(lang))

@dp.message(lambda m: m.text == "📋 Меню")
async def handle_start(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if _is_admin_user(message.from_user.id if message.from_user else None):
        await message.reply(
            "📋 Главное меню",
            reply_markup=get_main_menu(),
        )
        return
    await message.answer("✅ Пользовательский режим включён", reply_markup=ReplyKeyboardRemove())
    await _show_public_user_menu_message(message)


@dp.message(lambda m: m.text == "👤 Мой аккаунт")
async def handle_account_button(message: Message):
    if not _is_admin_user(message.from_user.id if message.from_user else None):
        await _show_public_user_menu_message(message)
        return
    await cmd_account(message)


@dp.message(lambda m: m.text == "💎 Тарифы")
async def handle_plans_button(message: Message):
    if not _is_admin_user(message.from_user.id if message.from_user else None):
        await _show_public_user_menu_message(message)
        return
    await cmd_plans(message)


@dp.message(lambda m: m.text == "🌐 Язык")
async def handle_language_button(message: Message):
    await cmd_language(message)


@dp.message(lambda m: m.text == "📈 Использование")
async def handle_usage_button(message: Message):
    await cmd_usage(message)


@dp.message(lambda m: m.text == "🧾 Счета")
async def handle_invoices_button(message: Message):
    await cmd_invoice(message)

@dp.message(lambda m: m.text in ("🔙 Главное меню", "⬅️ Назад в меню"))
async def handle_main_menu(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if _is_admin_user(message.from_user.id if message.from_user else None):
        await message.reply("📋 Главное меню", reply_markup=get_main_menu())
        return
    await message.answer("✅ Пользовательский режим включён", reply_markup=ReplyKeyboardRemove())
    await _show_public_user_menu_message(message)

@dp.message(lambda m: m.text == "❌ Отмена")
async def handle_cancel(message: Message):
    user_id = message.from_user.id if message.from_user else None
    state = user_states.get(user_id) if user_id else None

    if not state:
        reset_user_state(user_id)
        await message.answer("❌ Отменено", reply_markup=get_main_menu())
        return

    rule_id = state.get("rule_id")
    card_chat_id = state.get("card_chat_id")
    card_message_id = state.get("card_message_id")
    prompt_chat_id = state.get("prompt_chat_id")
    prompt_message_id = state.get("prompt_message_id")

    await try_delete_message_safe(message.chat.id, message.message_id)

    if prompt_chat_id and prompt_message_id:
        await try_delete_message_safe(prompt_chat_id, prompt_message_id)

    refreshed = False
    if rule_id and card_chat_id and card_message_id:
        refreshed = await refresh_rule_card_by_ids(
            chat_id=card_chat_id,
            message_id=card_message_id,
            rule_id=rule_id,
        )

    reset_user_state(user_id)

    if not refreshed:
        await message.answer("❌ Отменено", reply_markup=get_main_menu())


@dp.callback_query(lambda c: c.data and c.data.startswith("lang:"))
async def handle_language_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    code = (callback.data or "").split(":", 1)[1]
    lang = set_user_language(callback.from_user.id, code, db)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=tr(f"language.changed.{lang}", lang),
        reply_markup=product_ui.product_menu_keyboard(lang),
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("product:"))
async def handle_product_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    action = (callback.data or "").split(":", 1)[1]
    lang = _resolve_language(callback.from_user.id if callback.from_user else None)
    if action == "menu":
        text = "💼 Аккаунт" if lang == "ru" else "💼 Account"
        kb = product_ui.product_menu_keyboard(lang)
    elif action == "plans":
        text = product_ui.plans_screen(lang=lang, plans=_default_plan_catalog(lang))
        kb = product_ui.plans_keyboard(lang)
    elif action == "account":
        admin_id = callback.from_user.id if callback.from_user else settings.admin_id
        tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
        tenant_id = int(tenant.get("id") or 1)
        subscription = await run_db(subscription_service.get_active_subscription, tenant_id) or _get_plan_info("FREE", lang)
        today = await run_db(usage_service.get_today_usage, tenant_id)
        summary = await run_db(billing_service.build_billing_summary, tenant_id)
        rules_count = await run_db(db.count_rules_for_tenant, tenant_id) if hasattr(db, "count_rules_for_tenant") else 0
        text = product_ui.account_screen(
            lang=lang,
            subscription=subscription,
            usage_today=today,
            usage_period=summary.get("usage") or {},
            last_invoice=summary.get("last_invoice_summary"),
            rules_count=int(rules_count or 0),
        )
        kb = product_ui.account_keyboard(lang)
    elif action == "usage":
        admin_id = callback.from_user.id if callback.from_user else settings.admin_id
        tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
        tenant_id = int(tenant.get("id") or 1)
        today = await run_db(usage_service.get_today_usage, tenant_id)
        summary = await run_db(billing_service.build_billing_summary, tenant_id)
        limits = await run_db(subscription_service.get_active_subscription, tenant_id) or {}
        text = product_ui.usage_screen(lang=lang, today=today, period=summary.get("usage") or {}, limits=limits)
        kb = product_ui.account_keyboard(lang)
    elif action == "invoice":
        admin_id = callback.from_user.id if callback.from_user else settings.admin_id
        tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
        tenant_id = int(tenant.get("id") or 1)
        summary = await run_db(billing_service.get_last_invoice_summary, tenant_id)
        if not summary:
            text = "🧾 Счетов пока нет." if lang == "ru" else "🧾 No invoices yet."
            kb = product_ui.product_menu_keyboard(lang)
        else:
            text = product_ui.invoice_screen(lang=lang, invoice=summary.get("invoice") or {}, items=summary.get("items") or [])
            kb = product_ui.invoice_keyboard(lang)
    else:
        text = tr("language.select", lang)
        kb = product_ui.language_keyboard()
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)


@dp.message(lambda m: m.text == "🧾 Мои счета")
async def handle_user_invoices_text(message: Message):
    if _is_admin_user(message.from_user.id if message.from_user else None):
        await cmd_invoice(message)
        return
    user_id = message.from_user.id if message.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    invoices = await run_db(_get_user_invoices_payload, tenant_id, 10)
    await message.answer(
        user_ui.build_user_invoices_text(invoices),
        reply_markup=user_ui.build_user_invoices_keyboard(invoices),
    )


@dp.message(lambda m: m.text == "💳 Оплата")
async def handle_user_payments_text(message: Message):
    if _is_admin_user(message.from_user.id if message.from_user else None):
        await message.answer("Раздел оплаты доступен через админские команды.")
        return
    await _show_public_user_menu_message(message)


@dp.message(lambda m: m.text == "🆘 Поддержка")
async def handle_user_support_text(message: Message):
    if _is_admin_user(message.from_user.id if message.from_user else None):
        await message.answer("Поддержка: используйте внутренние админские инструменты.")
        return
    await _show_public_user_menu_message(message)


@dp.callback_query(lambda c: c.data == "user_main")
async def handle_user_main_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=_public_user_menu_text(),
        reply_markup=_public_user_menu_keyboard(),
    )

@dp.callback_query(lambda c: c.data == "user_sources")
async def handle_user_sources_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    logger.info("пользователь открыл список источников user_id=%s tenant_id=%s", user_id, tenant_id)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text="📡 Источники\n\nВыберите действие:",
        reply_markup=_user_sources_keyboard(),
    )


@dp.callback_query(lambda c: c.data == "user_targets")
async def handle_user_targets_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    logger.info("пользователь открыл список получателей user_id=%s tenant_id=%s", user_id, tenant_id)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text="🎯 Получатели\n\nВыберите действие:",
        reply_markup=_user_targets_keyboard(),
    )


@dp.callback_query(lambda c: c.data == "user_sources_list")
async def handle_user_sources_list_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    rows = await run_db(db.get_channels_for_tenant, tenant_id, "source") if hasattr(db, "get_channels_for_tenant") else []
    await answer_callback_safe_once(callback)
    if not rows:
        await edit_message_text_safe(
            message=callback.message,
            text="📡 Источники\n\nСписок пока пуст.",
            reply_markup=_user_sources_keyboard(),
        )
        return
    lines = ["📡 Источники\n"]
    for idx, row in enumerate(rows, 1):
        title = row["title"] or row["channel_id"]
        suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
        lines.append(f"{idx}. {title}{suffix}")
    await edit_message_text_safe(
        message=callback.message,
        text="\n".join(lines),
        reply_markup=_user_sources_keyboard(),
    )


@dp.callback_query(lambda c: c.data == "user_targets_list")
async def handle_user_targets_list_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    rows = await run_db(db.get_channels_for_tenant, tenant_id, "target") if hasattr(db, "get_channels_for_tenant") else []
    await answer_callback_safe_once(callback)
    if not rows:
        await edit_message_text_safe(
            message=callback.message,
            text="🎯 Получатели\n\nСписок пока пуст.",
            reply_markup=_user_targets_keyboard(),
        )
        return
    lines = ["🎯 Получатели\n"]
    for idx, row in enumerate(rows, 1):
        title = row["title"] or row["channel_id"]
        suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
        lines.append(f"{idx}. {title}{suffix}")
    await edit_message_text_safe(
        message=callback.message,
        text="\n".join(lines),
        reply_markup=_user_targets_keyboard(),
    )


@dp.callback_query(lambda c: c.data == "user_sources_add")
async def handle_user_sources_add_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    await answer_callback_safe_once(callback)
    if callback.from_user:
        user_states[callback.from_user.id] = {"action": "choose_source_kind"}
    await callback.message.answer("Выберите: канал или группа с темой", reply_markup=get_entity_kind_keyboard())


@dp.callback_query(lambda c: c.data == "user_targets_add")
async def handle_user_targets_add_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    await answer_callback_safe_once(callback)
    if callback.from_user:
        user_states[callback.from_user.id] = {"action": "choose_target_kind"}
    await callback.message.answer("Выберите: канал или группа с темой", reply_markup=get_entity_kind_keyboard())


@dp.callback_query(lambda c: c.data in ("user_sources_remove", "user_targets_remove"))
async def handle_user_channel_remove_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    channel_type = "source" if callback.data == "user_sources_remove" else "target"
    rows = await run_db(db.get_channels_for_tenant, tenant_id, channel_type) if hasattr(db, "get_channels_for_tenant") else []
    await answer_callback_safe_once(callback)
    if not rows:
        title = "📡 Источники" if channel_type == "source" else "🎯 Получатели"
        kb = _user_sources_keyboard() if channel_type == "source" else _user_targets_keyboard()
        await edit_message_text_safe(
            message=callback.message,
            text=f"{title}\n\nСписок пока пуст.",
            reply_markup=kb,
        )
        return

    keyboard = []
    mapping = []
    text = "Выберите запись для удаления\n\n"
    for idx, row in enumerate(rows, 1):
        title = row["title"] or row["channel_id"]
        suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
        keyboard.append([KeyboardButton(text=f"Удалить {idx}")])
        mapping.append((row["channel_id"], row["thread_id"], row["channel_type"]))
        text += f"{idx}. [{row['channel_type']}] {title}{suffix}\n"
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    if callback.from_user:
        user_states[callback.from_user.id] = {
            "action": "remove_channel",
            "mapping": mapping,
            "tenant_id": tenant_id,
        }
    await callback.message.answer(
        text[:4000],
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
    )


@dp.callback_query(lambda c: c.data == "user_rules")
async def handle_user_rules_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    logger.info("пользователь открыл свои правила user_id=%s tenant_id=%s", user_id, tenant_id)
    rules = await run_db(db.get_rules_for_tenant, tenant_id) if hasattr(db, "get_rules_for_tenant") else []
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(message=callback.message, text="📜 Список правил:", reply_markup=build_user_rules_keyboard(rules, page=0))


@dp.callback_query(lambda c: c.data == "user_status")
async def handle_user_status_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    sub = await run_db(subscription_service.get_active_subscription, tenant_id) or {}
    rules = await run_db(db.get_rules_for_tenant, tenant_id) if hasattr(db, "get_rules_for_tenant") else []
    active_rules = sum(1 for row in rules if bool(getattr(row, "is_active", False)))
    rule_limit = int(sub.get("max_rules") or 0)
    queue_total = 0
    errors_total = 0
    next_publication = "—"
    for row in rules:
        snapshot = await run_db(db.get_rule_card_snapshot, int(getattr(row, "id", 0)))
        if not snapshot:
            continue
        queue_total += int(snapshot.get("pending_count") or 0)
        errors_total += int(snapshot.get("faulty_count") or 0)
        next_run = snapshot.get("next_run_at")
        if next_run and next_publication == "—":
            next_publication = str(next_run)[11:16]
    text = (
        "📊 Статус\n\n"
        f"Правил: {len(rules)} / {rule_limit}\n"
        f"Активных правил: {active_rules}\n"
        f"Публикаций в очереди: {queue_total}\n"
        f"Ошибок: {errors_total}\n"
        f"Следующая публикация: {next_publication}\n\n"
        f"Тариф: {str(sub.get('plan_name') or 'FREE').upper()}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Мои правила", callback_data="user_rules")],
        [InlineKeyboardButton(text="💎 Сменить тариф", callback_data="user_plans")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
    ])
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)


@dp.callback_query(lambda c: c.data == "user_account")
async def handle_user_account_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant = await run_db(tenant_service.ensure_tenant_exists, user_id)
    tenant_id = int(tenant.get("id") or 1)
    subscription = await run_db(subscription_service.get_active_subscription, tenant_id) or _get_plan_info("FREE", "ru")
    usage_today = await run_db(usage_service.get_today_usage, tenant_id)
    rules_count = await run_db(db.count_rules_for_tenant, tenant_id) if hasattr(db, "count_rules_for_tenant") else 0
    logger.info("Пользователь открыл user_account user_id=%s tenant_id=%s", user_id, tenant_id)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=_build_public_account_text(
            user_id=user_id,
            tenant_id=tenant_id,
            subscription=subscription,
            usage_today=usage_today,
            rules_count=int(rules_count or 0),
        ),
        reply_markup=_public_account_keyboard(),
    )


@dp.callback_query(lambda c: c.data == "user_plans")
async def handle_user_plans_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    logger.info("Пользователь открыл user_plans user_id=%s", callback.from_user.id if callback.from_user else 0)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=_build_public_plans_text(),
        reply_markup=_public_plans_keyboard(),
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("user_select_plan:"))
async def handle_user_select_plan_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    plan_name = str((callback.data or "").split(":", 1)[1] if ":" in (callback.data or "") else "").upper()
    logger.info("пользователь выбрал тариф plan_name=%s user_id=%s tenant_id=%s", plan_name, user_id, tenant_id)
    if plan_name == "OWNER":
        await answer_callback_safe(callback, "Тариф OWNER недоступен", show_alert=True)
        return
    if plan_name == "FREE":
        await answer_callback_safe_once(callback)
        await edit_message_text_safe(
            message=callback.message,
            text="Тариф FREE не требует создания счёта.",
            reply_markup=_public_plans_keyboard(),
        )
        return
    if plan_name not in {"BASIC", "PRO"}:
        await answer_callback_safe(callback, "Разрешены только тарифы BASIC и PRO", show_alert=True)
        return

    invoices = await run_db(_get_user_invoices_payload, tenant_id, 10)
    for invoice in invoices:
        status = str(invoice.get("status") or "")
        if status not in {"open", "draft"}:
            continue
        if _invoice_plan_name(invoice, invoice.get("items") or []) == plan_name:
            await answer_callback_safe_once(callback)
            await edit_message_text_safe(
                message=callback.message,
                text=user_ui.build_user_invoice_text(invoice, invoice.get("items") or []),
                reply_markup=_public_invoice_keyboard(int(invoice.get("id") or 0)),
            )
            return

    sub = await run_db(subscription_service.get_active_subscription, tenant_id)
    if not sub:
        await answer_callback_safe(callback, "Не удалось найти активную подписку", show_alert=True)
        return
    sub = await run_db(billing_service.ensure_billing_period, sub)
    plan = _get_plan_info(plan_name, "ru")
    invoice_id = await run_db(
        invoice_service.create_draft_invoice,
        int(tenant_id),
        int(sub.get("id") or 0),
        str(sub.get("current_period_start")),
        str(sub.get("current_period_end")),
        currency="USD",
    )
    if not invoice_id:
        await answer_callback_safe(callback, "Не удалось создать счёт", show_alert=True)
        return
    await run_db(
        invoice_service.add_invoice_item,
        int(invoice_id),
        item_type="base_plan",
        description=plan_name,
        quantity=1,
        unit_price=float(plan.get("price") or 0),
        metadata={"plan_name": plan_name},
    )
    await run_db(invoice_service.finalize_invoice, int(invoice_id))
    invoice = await run_db(db.get_invoice, int(invoice_id)) if hasattr(db, "get_invoice") else {"id": int(invoice_id), "status": "open", "total": float(plan.get("price") or 0), "currency": "USD"}
    items = await run_db(db.list_invoice_items, int(invoice_id)) if hasattr(db, "list_invoice_items") else []
    logger.info("создан счёт invoice_id=%s tenant_id=%s plan=%s", invoice_id, tenant_id, plan_name)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=user_ui.build_user_invoice_text(invoice, items),
        reply_markup=_public_invoice_keyboard(int(invoice_id)),
    )


@dp.callback_query(lambda c: c.data == "user_invoices")
async def handle_user_invoices_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    invoices = await run_db(_get_user_invoices_payload, tenant_id, 10)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=user_ui.build_user_invoices_text(invoices),
        reply_markup=user_ui.build_user_invoices_keyboard(invoices),
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice:"))
async def handle_user_invoice_callback(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    invoice_id = int((callback.data or "0").split(":", 1)[1] or 0)
    invoice = await run_db(db.get_invoice, invoice_id) if hasattr(db, "get_invoice") else None
    if not invoice:
        await answer_callback_safe(callback, "Счёт не найден", show_alert=True)
        return
    is_admin = _is_admin_user(user_id)
    if not is_admin:
        tenant_id = await run_db(ensure_user_tenant, user_id)
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            logger.warning("попытка открыть чужой счёт user_id=%s invoice_id=%s", user_id, invoice_id)
            await answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
    items = await run_db(db.list_invoice_items, invoice_id) if hasattr(db, "list_invoice_items") else []
    logger.info("пользователь открыл счёт invoice_id=%s", invoice_id)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=user_ui.build_user_invoice_text(invoice, items),
        reply_markup=_public_invoice_keyboard(invoice_id),
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice_pay:"))
async def handle_user_invoice_pay_callback(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    invoice_id = int((callback.data or "0").split(":", 1)[1] or 0)
    invoice = await run_db(db.get_invoice, invoice_id) if hasattr(db, "get_invoice") else None
    if not invoice:
        await answer_callback_safe(callback, "Счёт не найден", show_alert=True)
        return
    is_admin = _is_admin_user(user_id)
    if not is_admin:
        tenant_id = await run_db(ensure_user_tenant, user_id)
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            logger.warning("попытка открыть чужой счёт user_id=%s invoice_id=%s", user_id, invoice_id)
            await answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=(
            f"💳 Оплата счёта #{invoice_id}\n\n"
            "Выбор способа оплаты будет подключён следующим этапом."
        ),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{invoice_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_invoices")],
            ]
        ),
    )


@dp.callback_query(lambda c: c.data == "user_payments")
async def handle_user_payments_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text="Оплата будет подключена следующим этапом.",
        reply_markup=_public_user_back_keyboard(),
    )


@dp.callback_query(lambda c: c.data == "user_support")
async def handle_user_support_callback(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    support_placeholder = getattr(settings, "support", "") or getattr(settings, "support_contact", "")
    text = (
        "🆘 Поддержка\n\n"
        "Если нужна помощь с тарифом, оплатой или настройкой — напишите администратору."
    )
    if support_placeholder:
        text += f"\n\nКонтакт: {support_placeholder}"
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=text,
        reply_markup=_public_user_back_keyboard(),
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("plan_select:"))
async def handle_plan_select_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    lang = _resolve_language(callback.from_user.id if callback.from_user else None)
    plan_name = (callback.data or "").split(":", 1)[1].upper()
    plan = _get_plan_info(plan_name, lang)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=product_ui.upgrade_confirm_screen(lang, plan),
        reply_markup=product_ui.upgrade_confirm_keyboard(lang, plan_name),
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("plan_confirm:"))
async def handle_plan_confirm_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    admin_id = callback.from_user.id if callback.from_user else settings.admin_id
    lang = _resolve_language(admin_id)
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    result = await run_db(_build_invoice_for_plan_sync, int(tenant.get("id") or 1), (callback.data or "").split(":", 1)[1].upper())
    await answer_callback_safe_once(callback)
    if not result or not result.get("invoice"):
        await edit_message_text_safe(message=callback.message, text=("❌ Не удалось создать счёт." if lang == "ru" else "❌ Failed to create invoice."))
        return
    await edit_message_text_safe(
        message=callback.message,
        text=product_ui.invoice_screen(lang=lang, invoice=result["invoice"], items=result.get("items") or []),
        reply_markup=product_ui.invoice_keyboard(lang),
    )


@dp.callback_query(lambda c: c.data == "invoice:pay")
async def handle_invoice_pay_stub(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    lang = _resolve_language(callback.from_user.id if callback.from_user else None)
    await answer_callback_safe_once(callback)
    admin_id = callback.from_user.id if callback.from_user else settings.admin_id
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    summary = await run_db(billing_service.get_last_invoice_summary, tenant_id)
    invoice = (summary or {}).get("invoice") or {}
    providers = await run_db(
        lambda: [m["provider"] for m in payment_service.get_available_payment_methods(tenant_id, int(invoice.get("id") or 0))]
    )
    if not providers:
        await edit_message_text_safe(
            message=callback.message,
            text=product_ui.payment_stub_screen(lang),
            reply_markup=product_ui.invoice_keyboard(lang),
        )
        return
    await edit_message_text_safe(
        message=callback.message,
        text=product_ui.payment_methods_screen(lang),
        reply_markup=product_ui.payment_methods_keyboard(lang, providers),
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("invoice:provider:"))
async def handle_invoice_provider_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    admin_id = callback.from_user.id if callback.from_user else settings.admin_id
    lang = _resolve_language(admin_id)
    provider = (callback.data or "").split(":", 2)[2]
    tenant = await run_db(tenant_service.ensure_tenant_exists, admin_id)
    tenant_id = int(tenant.get("id") or 1)
    summary = await run_db(billing_service.get_last_invoice_summary, tenant_id)
    invoice = (summary or {}).get("invoice") or {}
    result = await run_db(payment_service.create_payment_for_invoice, int(invoice.get("id") or 0), provider)
    await answer_callback_safe_once(callback)
    if not result.get("ok"):
        await edit_message_text_safe(
            message=callback.message,
            text=("❌ Не удалось создать оплату." if lang == "ru" else "❌ Failed to create payment."),
            reply_markup=product_ui.invoice_keyboard(lang),
        )
        return
    message_text = (result.get("message_ru") if lang == "ru" else result.get("message_en")) or "OK"
    if result.get("checkout_url"):
        message_text += "\n\n" + str(result["checkout_url"])
    kb = product_ui.invoice_keyboard(lang)
    if str(result.get("status")) == "waiting_confirmation":
        kb = product_ui.payment_manual_confirm_keyboard(lang, int(result.get("payment_intent_id") or 0))
    await edit_message_text_safe(message=callback.message, text=message_text, reply_markup=kb)


@dp.callback_query(lambda c: c.data and c.data.startswith("payment:manual_sent:"))
async def handle_manual_payment_sent(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    admin_id = callback.from_user.id if callback.from_user else settings.admin_id
    lang = _resolve_language(admin_id)
    payment_intent_id = int((callback.data or "0").split(":")[-1] or 0)
    await run_db(payment_service.save_manual_confirmation_payload, payment_intent_id, {"requested_by": admin_id, "requested_at": utc_now_iso()})
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(
        message=callback.message,
        text=product_ui._msg(lang, "payment.manual.sent"),
        reply_markup=product_ui.invoice_keyboard(lang),
    )
    if admin_id != settings.admin_id and bot:
        await bot.send_message(
            settings.admin_id,
            (
                "💳 Новая ручная оплата\n\n"
                f"Пользователь: {admin_id}\n"
                f"PaymentIntent: #{payment_intent_id}\n\n"
                "Подтвердить?\n"
                f"/payment_confirm {payment_intent_id}\n"
                f"/payment_reject {payment_intent_id}"
            ),
        )


@dp.pre_checkout_query()
async def handle_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.answer(ok=True)


@dp.message(lambda m: bool(getattr(m, "successful_payment", None)))
async def handle_successful_payment(message: Message):
    successful = getattr(message, "successful_payment", None)
    if not successful:
        return
    external_id = str(getattr(successful, "invoice_payload", "") or getattr(successful, "provider_payment_charge_id", ""))
    if not external_id:
        return
    intent = await run_db(db.get_payment_intent_by_external_id, external_id) if hasattr(db, "get_payment_intent_by_external_id") else None
    if not intent:
        return
    if str(intent.get("status") or "") != "paid":
        await run_db(db.mark_payment_paid, int(intent.get("id") or 0), confirmation_payload={"source": "telegram_successful_payment"})
        await run_db(payment_service.activate_subscription_after_payment, int(intent.get("id") or 0))


@dp.message(Command("payment_confirm"))
async def handle_payment_confirm(message: Message):
    if not await is_admin(message):
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: /payment_confirm <payment_intent_id>")
        return
    payment_intent_id = int(parts[1])
    note = parts[2] if len(parts) > 2 else "manual_admin_confirmation"
    ok = await run_db(payment_service.confirm_manual_payment, payment_intent_id, message.from_user.id, note)
    await message.answer("✅ Оплата подтверждена" if ok else "❌ Не удалось подтвердить оплату")


@dp.message(Command("payment_reject"))
async def handle_payment_reject(message: Message):
    if not await is_admin(message):
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: /payment_reject <payment_intent_id>")
        return
    payment_intent_id = int(parts[1])
    reason = parts[2] if len(parts) > 2 else "manual_rejected_by_admin"
    ok = await run_db(db.mark_payment_failed, payment_intent_id, reason, payload={"rejected_by": message.from_user.id})
    await message.answer("❌ Оплата отклонена" if ok else "❌ Не удалось отклонить оплату")


@dp.callback_query(lambda c: c.data and c.data.startswith("start:"))
async def handle_start_shortcuts(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return
    action = (callback.data or "").split(":", 1)[1]
    await answer_callback_safe_once(callback)
    if action == "add_channel":
        await edit_message_text_safe(
            message=callback.message,
            text="Выберите тип записи",
            reply_markup=None,
        )
        await callback.message.answer("Выберите тип записи", reply_markup=get_channel_type_keyboard())
        return
    if action == "create_rule":
        await callback.message.answer("Раздел правил", reply_markup=get_rules_menu())
        return

@dp.message(lambda m: m.text == "📈 Живой статус")
async def handle_live_status(message: Message):
    if not await is_admin(message):
        return

    text = await run_db(build_dashboard_text)

    msg = await message.reply(
        text,
        parse_mode="Markdown",
        reply_markup=build_dashboard_keyboard(running=True),
    )

    old_task = dashboard_tasks.get(message.from_user.id)
    if old_task:
        old_task.cancel()

    task = asyncio.create_task(dashboard_worker(message.from_user.id, msg))
    dashboard_tasks[message.from_user.id] = task

@dp.callback_query(lambda c: c.data == "dashboard_refresh")
async def handle_dashboard_refresh(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    text = await run_db(build_dashboard_text)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=text,
            parse_mode="Markdown",
            reply_markup=build_dashboard_keyboard(running=True),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка dashboard_refresh: %s", exc)

@dp.callback_query(lambda c: c.data == "dashboard_stop")
async def handle_dashboard_stop(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback, "⏸ Автообновление остановлено")

    task = dashboard_tasks.get(callback.from_user.id)
    if task:
        task.cancel()
        dashboard_tasks.pop(callback.from_user.id, None)

    text = await run_db(build_dashboard_text)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=text,
            parse_mode="Markdown",
            reply_markup=build_dashboard_keyboard(running=False),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка dashboard_stop: %s", exc)

@dp.callback_query(lambda c: c.data == "dashboard_resume")
async def handle_dashboard_resume(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback, "▶️ Автообновление возобновлено")

    text = await run_db(build_dashboard_text)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=text,
            parse_mode="Markdown",
            reply_markup=build_dashboard_keyboard(running=True),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка dashboard_resume: %s", exc)

    old_task = dashboard_tasks.get(callback.from_user.id)
    if old_task:
        old_task.cancel()

    task = asyncio.create_task(dashboard_worker(callback.from_user.id, callback.message))
    dashboard_tasks[callback.from_user.id] = task

@dp.callback_query(lambda c: c.data == "dashboard_back")
async def handle_dashboard_back(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    task = dashboard_tasks.get(callback.from_user.id)
    if task:
        task.cancel()
        dashboard_tasks.pop(callback.from_user.id, None)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text="📋 Главное меню",
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка dashboard_back edit_text: %s", exc)

    await callback.message.answer("📋 Главное меню", reply_markup=get_main_menu())

@dp.message(lambda m: m.text in ("📋 Очередь", "📋 Общая очередь"))
async def handle_queue(message: Message):
    if not await is_admin(message):
        return
    stats = await run_db(db.get_queue_stats)
    await message.reply(
        f"📋 Очередь\n\n⏳ Pending: {stats['pending']}\n✅ Sent: {stats['sent']}\n⚠️ Faulty: {stats['faulty']}",
        reply_markup=get_main_menu(),
    )
@dp.message(lambda m: m.text in ("▶️ Запуск", "▶️ Запустить пересылку"))
async def handle_global_start(message: Message):
    global posting_active
    if not await is_admin(message):
        return
    posting_active = True
    await ensure_rule_workers()
    await message.reply("✅ Пересылка запущена", reply_markup=get_main_menu())


@dp.message(lambda m: m.text in ("⏸ Стоп", "⏸ Остановить пересылку"))
async def handle_global_stop(message: Message):
    global posting_active
    if not await is_admin(message):
        return
    posting_active = False
    await stop_all_workers()
    await message.reply("⏸ Пересылка остановлена", reply_markup=get_main_menu())


@dp.message(lambda m: m.text == "🔄 Правила")
async def handle_rules_menu_open(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if not await is_admin(message):
        return
    await message.reply("🔄 Раздел: Правила", reply_markup=get_rules_menu())

@dp.message(lambda m: m.text == "📡 Каналы")
async def handle_channels_menu(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if not await is_admin(message):
        return
    await message.reply("📡 Раздел: Каналы", reply_markup=get_channels_menu())

@dp.message(lambda m: m.text == "📦 Очередь")
async def handle_queue_menu(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if not await is_admin(message):
        return
    await message.reply("📦 Раздел: Очередь", reply_markup=get_queue_menu())

@dp.message(lambda m: m.text == "⚠️ Диагностика")
async def handle_diagnostics_menu(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if not await is_admin(message):
        return
    await message.reply("⚠️ Раздел: Диагностика", reply_markup=get_diagnostics_menu())

@dp.message(lambda m: (m.text or "").strip() == "⚠️ Проблемные доставки")
async def handle_faulty(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    logger.warning("DEBUG: handle_faulty ENTER text=%r", message.text)

    if not await is_admin(message):
        logger.warning("DEBUG: handle_faulty rejected by is_admin text=%r", message.text)
        return

    pages = await run_db(build_faulty_pages, 200)
    page = 0
    total_pages = len(pages)

    logger.warning("DEBUG: faulty pages count=%s first_page=%r", total_pages, pages[0] if pages else None)

    current = pages[page]

    await message.reply(
        current["text"],
        parse_mode="HTML",
        reply_markup=build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]),
    )

    logger.warning("DEBUG: handle_faulty DONE")

@dp.message(lambda m: m.text == "📊 Журнал системы")
async def handle_system_journal(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if not await is_admin(message):
        return

    pages = await run_db(build_system_journal_pages, 300)
    page = 0
    total_pages = len(pages)

    await message.reply(
        pages[page],
        parse_mode="HTML",
        reply_markup=build_system_journal_inline_keyboard(page, total_pages),
    )

@dp.callback_query(lambda c: c.data == "syslog_page_info")
async def handle_syslog_page_info(callback: CallbackQuery):
    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("syslog_page:"))
async def handle_syslog_page(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, page_raw = parse_callback_parts(callback.data, "syslog_page", 2)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    pages = await run_db(build_system_journal_pages, 300)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=pages[page],
            parse_mode="HTML",
            reply_markup=build_system_journal_inline_keyboard(page, total_pages),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_syslog_page: %s", exc)

@dp.callback_query(lambda c: c.data.startswith("syslog_refresh:"))
async def handle_syslog_refresh(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, page_raw = parse_callback_parts(callback.data, "syslog_refresh", 2)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    pages = await run_db(build_system_journal_pages, 300)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)

    await edit_message_text_safe(
        message=callback.message,
        text=pages[page],
        parse_mode="HTML",
        reply_markup=build_system_journal_inline_keyboard(page, total_pages),
    )

@dp.callback_query(lambda c: c.data == "syslog_back")
async def handle_syslog_back(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    await edit_message_text_safe(
        message=callback.message,
        text="⚠️ Раздел: Диагностика",
    )

    await send_message_safe(
        chat_id=callback.message.chat.id,
        text="⚠️ Раздел: Диагностика",
        reply_markup=get_diagnostics_menu(),
    )

@dp.message(lambda m: m.text == "⚙️ Система")
async def handle_system_menu(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    if not await is_admin(message):
        return
    await message.reply("⚙️ Раздел: Система", reply_markup=get_system_menu())

@dp.message(lambda m: m.text == "📜 Список правил")
async def handle_list_rules(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    user_id = message.from_user.id if message.from_user else settings.admin_id
    is_admin_mode = is_admin_user(user_id)
    if is_admin_mode:
        rules = await run_db(db.get_all_rules)
    else:
        tenant_id = await run_db(ensure_user_tenant, user_id)
        rules = await run_db(db.get_rules_for_tenant, tenant_id) if hasattr(db, "get_rules_for_tenant") else []

    if not rules:
        await message.reply("Правил пока нет", reply_markup=get_rules_menu())
        return

    if is_admin_mode:
        await message.reply(
            "📜 Список правил:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await message.answer(
            "📜 Список правил:",
            reply_markup=rules_list_keyboard(rules, page=0),
        )
        return

    await message.answer("📜 Список правил:", reply_markup=build_user_rules_keyboard(rules, page=0))

@dp.callback_query(lambda c: c.data == "rules_back")
async def handle_rules_back(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    rules = await run_db(db.get_all_rules)
    if not rules:
        try:
            await callback.message.edit_text("Правил пока нет")
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                logger.exception("Ошибка rules_back empty: %s", exc)
        return

    await edit_message_text_safe(
        message=callback.message,
        text="🔄 Раздел: Правила",
    )

    await send_message_safe(
        chat_id=callback.message.chat.id,
        text="🔄 Раздел: Правила",
        reply_markup=get_rules_menu(),
    )


@dp.callback_query(lambda c: c.data == "user_rules_noop")
async def handle_user_rules_noop(callback: CallbackQuery):
    await answer_callback_safe_once(callback)


@dp.callback_query(lambda c: c.data and c.data.startswith("user_rules_page:"))
async def handle_user_rules_page(callback: CallbackQuery):
    if _is_admin_user(callback.from_user.id if callback.from_user else None):
        await answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
        return
    page = int((callback.data or "user_rules_page:0").split(":", 1)[1])
    tenant_id = await run_db(ensure_user_tenant, callback.from_user.id if callback.from_user else 0)
    rules = await run_db(db.get_rules_for_tenant, tenant_id) if hasattr(db, "get_rules_for_tenant") else []
    await answer_callback_safe_once(callback)
    await edit_message_reply_markup_safe(message=callback.message, reply_markup=build_user_rules_keyboard(rules, page=page))


@dp.callback_query(lambda c: c.data == "user_rules_add")
async def handle_user_rules_add(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    tenant_id = await run_db(ensure_user_tenant, user_id)
    source_rows = await run_db(db.get_channels_for_tenant, tenant_id, "source") if hasattr(db, "get_channels_for_tenant") else []
    sources = [ChannelChoice(r["channel_id"], r["thread_id"], r["title"] or r["channel_id"]) for r in source_rows]
    if not sources:
        await answer_callback_safe(callback, "Сначала добавьте источник", show_alert=True)
        return
    keyboard = [[KeyboardButton(text=f"📤 {i}. {s.title}{f' (тема {s.thread_id})' if s.thread_id else ''}")] for i, s in enumerate(sources, 1)]
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    user_states[user_id] = {"action": "pick_rule_source", "sources": sources}
    await answer_callback_safe_once(callback)
    await callback.message.answer("Выберите источник", reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True))


@dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_open:"))
async def handle_user_rule_open(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    rule_id = int((callback.data or "").split(":", 1)[1])
    if not await run_db(is_rule_owned_by_user, rule_id, user_id):
        logger.warning("пользователь попытался открыть чужой объект user_id=%s object=rule:%s", user_id, rule_id)
        await answer_callback_safe(callback, "⛔ Нет доступа к этому объекту", show_alert=True)
        return
    rule = await run_db(db.get_rule, rule_id)
    if not rule:
        await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return
    text, kb = build_user_rule_card(rule)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)


@dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_toggle:"))
async def handle_user_rule_toggle(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    rule_id = int((callback.data or "").split(":", 1)[1])
    if not await run_db(is_rule_owned_by_user, rule_id, user_id):
        logger.warning("пользователь попытался открыть чужой объект user_id=%s object=rule:%s", user_id, rule_id)
        await answer_callback_safe(callback, "⛔ Нет доступа к этому объекту", show_alert=True)
        return
    rule = await run_db(db.get_rule, rule_id)
    if not rule:
        await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return
    if bool(getattr(rule, "is_active", False)):
        await run_db(scheduler_service.deactivate_rule, rule_id)
    else:
        await run_db(scheduler_service.activate_with_backfill, rule_id)
    updated = await run_db(db.get_rule, rule_id)
    text, kb = build_user_rule_card(updated)
    await answer_callback_safe_once(callback)
    await edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)


@dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_delete:"))
async def handle_user_rule_delete(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    rule_id = int((callback.data or "").split(":", 1)[1])
    if not await run_db(is_rule_owned_by_user, rule_id, user_id):
        logger.warning("пользователь попытался открыть чужой объект user_id=%s object=rule:%s", user_id, rule_id)
        await answer_callback_safe(callback, "⛔ Нет доступа к этому объекту", show_alert=True)
        return
    await run_db(db.remove_rule, rule_id)
    tenant_id = await run_db(ensure_user_tenant, user_id)
    rules = await run_db(db.get_rules_for_tenant, tenant_id) if hasattr(db, "get_rules_for_tenant") else []
    await answer_callback_safe_once(callback, "Удалено")
    await edit_message_text_safe(message=callback.message, text="📜 Список правил:", reply_markup=build_user_rules_keyboard(rules, page=0))


@dp.callback_query(lambda c: c.data and c.data.startswith("user_rule_interval:"))
async def handle_user_rule_interval(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    rule_id = int((callback.data or "").split(":", 1)[1])
    if not await run_db(is_rule_owned_by_user, rule_id, user_id):
        logger.warning("пользователь попытался открыть чужой объект user_id=%s object=rule:%s", user_id, rule_id)
        await answer_callback_safe(callback, "⛔ Нет доступа к этому объекту", show_alert=True)
        return
    user_states[user_id] = {"action": "user_set_rule_interval", "rule_id": rule_id}
    await answer_callback_safe_once(callback)
    await callback.message.answer("Отправьте новый интервал в секундах", reply_markup=get_cancel_keyboard())

@dp.callback_query(lambda c: c.data == "rules_page_info")
async def handle_rules_page_info(callback: CallbackQuery):
    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("rules_page:"))
async def handle_rules_page(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, action, page_raw = parse_callback_parts(callback.data, "rules_page", 3)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    if action in {"disable", "enable", "delete", "interval", "next_run", "trigger", "list"}:
        all_rules = await run_db(db.get_all_rules)
    else:
        await answer_callback_safe(callback, "Неизвестная страница", show_alert=True)
        return

    if action == "disable":
        rules = [r for r in all_rules if r.is_active]
        text = "Список активных правил:"
        markup = rules_inline_keyboard(rules, "disable", page=page)

    elif action == "enable":
        rules = [r for r in all_rules if not r.is_active]
        text = "Список выключенных правил:"
        markup = rules_inline_keyboard(rules, "enable", page=page)

    elif action == "delete":
        rules = all_rules
        text = "📜 Список правил:"
        markup = rules_inline_keyboard(rules, "delete", page=page)

    elif action == "interval":
        rules = all_rules
        text = "Выберите правило для изменения интервала:"
        markup = rules_interval_keyboard(rules, page=page)

    elif action == "next_run":
        rules = all_rules
        text = "Выберите правило для переноса следующего поста:"
        markup = rules_next_run_keyboard(rules, page=page)

    elif action == "trigger":
        rules = [r for r in all_rules if r.is_active]
        text = "Выберите правило, для которого нужно отправить следующий пост сейчас:"
        markup = rules_trigger_now_keyboard(rules, page=page)

    elif action == "list":
        rules = all_rules
        text = "📜 Список правил:"
        markup = rules_list_keyboard(rules, page=page)

    await edit_message_text_safe(
        message=callback.message,
        text=text,
        reply_markup=markup,
    )

def build_rule_card_text(row) -> str:
    """
    Универсальный рендер карточки.

    Если в row уже есть snapshot-поля:
    - logical_pending
    - logical_completed
    - logical_total
    - logical_current_position

    то НИКАКИХ дополнительных тяжёлых DB-вызовов не делаем.

    Если этих полей нет — используем fallback на старую логику.
    """
    rule_id = int(row["id"])
    target_title = row["target_title"] or row["target_id"]

    status_line = build_rule_status_line(row)
    wait_line = build_rule_wait_line(row)

    processing = int(row["processing"] or 0)
    faulty = int(row["faulty"] or 0)

    has_snapshot = (
        hasattr(row, "keys")
        and "logical_pending" in row.keys()
        and "logical_completed" in row.keys()
        and "logical_total" in row.keys()
        and "logical_current_position" in row.keys()
    )

    if has_snapshot:
        logical_pending = int(row["logical_pending"] or 0)
        completed = int(row["logical_completed"] or 0)
        total = int(row["logical_total"] or 0)
        current_position = row["logical_current_position"]
    else:
        logger.warning(
            "build_rule_card_text: snapshot-поля отсутствуют, используем аварийный fallback, rule_id=%s",
            rule_id,
        )
        try:
            pos_info = db.get_rule_position_info(rule_id)
        except Exception:
            logger.exception("Не удалось получить позицию правила #%s", rule_id)
            pos_info = {"total": 0, "current_position": None, "completed": 0}

        try:
            queue_items = db.get_rule_queue_logical_items(rule_id)
        except Exception:
            logger.exception("Не удалось получить логическую очередь правила #%s", rule_id)
            queue_items = []

        total = int(pos_info.get("total") or 0)
        current_position = pos_info.get("current_position")
        completed = int(pos_info.get("completed") or 0)
        logical_pending = len(queue_items)

    if total <= 0 or current_position is None:
        position_text = "—"
    else:
        position_text = f"{current_position} / {total}"

    mode = (row["mode"] or "repost") if "mode" in row.keys() else "repost"
    caption_mode_line = ""

    if mode == "repost":
        caption_mode_value = get_rule_caption_mode_value(rule_id, row=row)
        caption_mode_text = caption_delivery_mode_to_text(caption_mode_value)
        caption_mode_line = f"\n✍️ Режим подписи: {safe_html(caption_mode_text)}"

    return (
        f"<b>Правило #{row['id']}</b>\n"
        f"👉 {safe_html(target_title)}\n\n"
        f"{safe_html(status_line)}\n"
        f"{safe_html(wait_line)}\n"
        f"📦 В очереди: {logical_pending}\n"
        f"⏳ В обработке: {processing}\n"
        f"✅ Отправлено: {completed}\n"
        f"⚠️ Ошибки: {faulty}\n"
        f"📍 Позиция: {safe_html(position_text)}"
        f"{caption_mode_line}"
    )

def build_start_position_text(
    item: dict[str, Any],
    rule_mode: str = "repost",
    preview_method: str | None = None,
) -> str:
    lines = [
        "📍 <b>Выбор точки старта</b>",
        "",
        f"Позиция: {item['position']}",
        "",
        "Листайте кнопками ниже и нажмите <b>«Начать с этого»</b>, когда найдёте нужный пост.",
    ]

    if rule_mode != "video" and preview_method:
        lines.extend([
            "",
            f"Метод предпросмотра: {safe_html(preview_method)}",
        ])

    return "\n".join(lines)

async def send_preview_post(
    bot: Bot,
    chat_id: int,
    item: dict[str, Any],
    rule_mode: str = "repost",
) -> tuple[str, list[int]]:
    """
    Предпросмотр поста без лишнего служебного мусора в video-режиме.
    Возвращает:
    - текст метода предпросмотра
    - список message_id сообщений предпросмотра
    """

    source_chat = item["source_channel"]
    message_ids = item["message_ids"]
    preview_ids: list[int] = []

    # 1. Самый быстрый способ: copy
    try:
        if item["kind"] == "single":
            sent = await bot.copy_message(
                chat_id=chat_id,
                from_chat_id=source_chat,
                message_id=message_ids[0],
            )
            preview_ids.append(sent.message_id)
            return "Быстрый copy", preview_ids

        # Для альбома:
        # - в video режиме показываем только первый элемент и без служебной подписи
        # - в repost режиме сохраняем старое поведение с инфо о размере альбома
        sent = await bot.copy_message(
            chat_id=chat_id,
            from_chat_id=source_chat,
            message_id=message_ids[0],
        )
        preview_ids.append(sent.message_id)

        if rule_mode == "video":
            return "Быстрый copy", preview_ids

        info_msg = await bot.send_message(
            chat_id,
            f"📦 Альбом: {len(message_ids)} элементов",
        )
        preview_ids.append(info_msg.message_id)
        return f"Быстрый preview (альбом: {len(message_ids)} эл.)", preview_ids

    except Exception as exc:
        logger.warning("Предпросмотр copy не сработал: %s", exc)

    preview_ids = []

    # 2. Запасной способ: forward
    try:
        if item["kind"] == "single":
            sent = await bot.forward_message(
                chat_id=chat_id,
                from_chat_id=source_chat,
                message_id=message_ids[0],
            )
            preview_ids.append(sent.message_id)
            return "Пересылка forward", preview_ids

        if rule_mode == "video":
            sent = await bot.forward_message(
                chat_id=chat_id,
                from_chat_id=source_chat,
                message_id=message_ids[0],
            )
            preview_ids.append(sent.message_id)
            return "Пересылка forward", preview_ids

        for mid in message_ids:
            sent = await bot.forward_message(
                chat_id=chat_id,
                from_chat_id=source_chat,
                message_id=mid,
            )
            preview_ids.append(sent.message_id)

        return f"Пересылка forward (альбом: {len(preview_ids)} эл.)", preview_ids

    except Exception as exc:
        logger.warning("Предпросмотр forward не сработал: %s", exc)

    preview_ids = []

    # 3. Текстовый fallback
    try:
        raw = item.get("content_json") or "{}"
        payload = json.loads(raw)
        text = payload.get("text") or "(без текста)"

        sent = await bot.send_message(
            chat_id,
            f"⚠️ Не удалось показать медиа.\n\n{text}",
        )
        preview_ids.append(sent.message_id)
        return "Только текст", preview_ids

    except Exception as exc:
        logger.warning("Текстовый предпросмотр не сработал: %s", exc)

    # 4. Аварийный вариант
    sent = await bot.send_message(
        chat_id,
        "❌ Не удалось показать предпросмотр поста",
    )
    preview_ids.append(sent.message_id)
    return "Ошибка предпросмотра", preview_ids

async def cleanup_preview_messages(bot: Bot, chat_id: int, preview_message_ids: list[int] | None):
    if not preview_message_ids:
        return

    for msg_id in preview_message_ids:
        try:
            await delete_message_safe(chat_id=chat_id, message_id=msg_id)
        except Exception as exc:
            logger.warning("Не удалось удалить сообщение предпросмотра %s: %s", msg_id, exc)

def build_rule_card_keyboard(
    rule_id: int,
    is_active: bool,
    schedule_mode: str = "interval",
    mode: str = "repost",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    # 1 строка
    if schedule_mode == "fixed":
        rows.append([
            InlineKeyboardButton(
                text="🟢 Сделать плавающим",
                callback_data=f"set_interval_mode:{rule_id}",
            )
        ])
    else:
        rows.append([
            InlineKeyboardButton(
                text="📌 Сделать фиксированным",
                callback_data=f"change_fixed_times:{rule_id}",
            )
        ])

    # 2 строка
    if schedule_mode == "fixed":
        rows.append([
            InlineKeyboardButton(
                text="⏰ Фикс. время",
                callback_data=f"change_fixed_times:{rule_id}",
            ),
            InlineKeyboardButton(
                text="🕒 Время",
                callback_data=f"change_next_run:{rule_id}",
            ),
        ])
    else:
        rows.append([
            InlineKeyboardButton(
                text="⏱ Интервал",
                callback_data=f"change_interval:{rule_id}",
            ),
            InlineKeyboardButton(
                text="🕒 Время",
                callback_data=f"change_next_run:{rule_id}",
            ),
        ])

    # 3 / 4 / 5 строки
    if mode == "video":
        rows.append([
            InlineKeyboardButton(
                text="🎬 Заставки",
                callback_data=f"video_intro_menu:{rule_id}",
            ),
            InlineKeyboardButton(
                text="📝 Подпись",
                callback_data=f"video_caption_menu:{rule_id}",
            ),
        ])
        rows.append([
            InlineKeyboardButton(
                text="⚙️ Дополнительные функции",
                callback_data=f"rule_extra_menu:{rule_id}",
            )
        ])
    else:
        rows.append([
            InlineKeyboardButton(
                text="⚙️ Дополнительные функции",
                callback_data=f"rule_extra_menu:{rule_id}",
            )
        ])

    control_text = "⏸ Выключить" if is_active else "▶️ Включить"
    control_callback = f"disable_rule:{rule_id}" if is_active else f"enable_rule:{rule_id}"

    rows.append([
        InlineKeyboardButton(
            text="🔄 Обновить",
            callback_data=f"rule_refresh:{rule_id}",
        ),
        InlineKeyboardButton(
            text=control_text,
            callback_data=control_callback,
        ),
    ])

    rows.append([
        InlineKeyboardButton(
            text="🔄 К правилам",
            callback_data="rule_to_list",
        ),
        InlineKeyboardButton(
            text="📋 Главное меню",
            callback_data="rule_to_main_menu",
        ),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_rule_extra_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    rule = db.get_rule(rule_id)
    mode = (getattr(rule, "mode", "repost") or "repost").strip().lower()

    rows: list[list[InlineKeyboardButton]] = []

    rows.append([
        InlineKeyboardButton(
            text="🎛 Режим",
            callback_data=f"toggle_rule_mode:{rule_id}",
        )
    ])

    if mode == "repost":
        rows.append([
            InlineKeyboardButton(
                text="✍️ Режим подписи",
                callback_data=f"caption_mode_menu:{rule_id}",
            )
        ])

    rows.extend([
        [
            InlineKeyboardButton(
                text="⚡ Отправить сейчас",
                callback_data=f"trigger_now:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="↪ Начать с номера",
                callback_data=f"start_from_number:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="🔄 Пересканировать",
                callback_data=f"rescan_rule_menu:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="⏪ Откатить",
                callback_data=f"rollback:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="🧾 Логи правила",
                callback_data=f"rule_logs:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="🗑 Удалить",
                callback_data=f"delete_rule:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="⬅️ Назад к правилу",
                callback_data=f"rule_card:{rule_id}",
            )
        ],
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_caption_mode_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⚡ Обычный",
                    callback_data=f"set_caption_mode_copy_first:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="💎 Премиум",
                    callback_data=f"set_caption_mode_builder_first:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🤖 Автоматический",
                    callback_data=f"set_caption_mode_auto:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data=f"rule_extra_menu:{rule_id}",
                )
            ],
        ]
    )

def build_video_caption_mode_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⚡ Обычный",
                    callback_data=f"set_video_caption_mode_copy_first:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="💎 Премиум",
                    callback_data=f"set_video_caption_mode_builder_first:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🤖 Авто",
                    callback_data=f"set_video_caption_mode_auto:{rule_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data=f"video_caption_menu:{rule_id}",
                )
            ],
        ]
    )

def build_rule_input_inline_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data=f"rule_card:{rule_id}",
                )
            ]
        ]
    )

def get_rule_stats_row(rule_id: int):
    """
    Быстрый snapshot только для одной карточки правила.

    Раньше здесь был очень дорогой путь:
    - db.get_rule_stats() по всем правилам
    - потом build_rule_card_text() ещё отдельно дёргал
      get_rule_position_info() и get_rule_queue_logical_items()

    Теперь карточка берётся сразу готовым snapshot'ом из репозитория.
    """
    try:
        return db.get_rule_card_snapshot(rule_id)
    except Exception:
        logger.exception("Не удалось получить snapshot карточки правила #%s", rule_id)
        return None

async def get_rule_stats_row_async(rule_id: int):
    return await run_db(get_rule_stats_row, rule_id)

async def refresh_rule_card_message(
    callback: CallbackQuery,
    rule_id: int,
    *,
    prefix_text: str | None = None,
) -> str:
    """
    Быстрый путь карточки:
    - snapshot cache
    - без лишнего edit на cache_hit
    - без шторма message_not_modified
    """
    try:
        text, reply_markup, cache_status = await build_rule_card_payload_cached(rule_id)
        if not text or not reply_markup:
            return "error"

        if prefix_text:
            text = f"{prefix_text}\n\n{text}"

        logger.info(
            "RULE_CARD_MESSAGE | rule_id=%s | cache_status=%s",
            rule_id,
            cache_status,
        )

        # КЛЮЧЕВОЕ:
        # если карточка взята из кеша и сверху нет нового prefix_text,
        # значит текст и клавиатура те же самые — edit не нужен
        if cache_status in {"cache_hit", "cache_hit_after_wait"} and not prefix_text:
            return "not_modified"

        try:
            await edit_message_text_safe(
                message=callback.message,
                text=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
            return "updated"

        except Exception as exc:
            if "message is not modified" in str(exc).lower():
                return "not_modified"

            logger.warning("RULE_CARD_MESSAGE | edit failed, fallback send | %s", exc)

            await send_message_safe(
                chat_id=callback.message.chat.id,
                text=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
            return "resent"

    except Exception:
        logger.exception("RULE_CARD_MESSAGE | FAILED | rule_id=%s", rule_id)
        return "error"

def _save_video_caption_sync(
    rule_id: int,
    caption: str | None,
    entities_json: str | None,
    admin_id: int,
) -> dict[str, Any]:
    before_rule = db.get_rule(rule_id)
    ok = db.update_rule_video_caption(rule_id, caption, entities_json)
    after_rule = db.get_rule(rule_id)

    if ok:
        db.log_rule_change(
            event_type="rule_video_caption_changed",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "video_caption": before_rule.video_caption if before_rule else None,
                "video_caption_entities_json": before_rule.video_caption_entities_json if before_rule else None,
            },
            new_value={
                "video_caption": after_rule.video_caption if after_rule else None,
                "video_caption_entities_json": after_rule.video_caption_entities_json if after_rule else None,
            },
        )

    return {
        "ok": bool(ok),
    }

def _change_next_run_sync(
    rule_id: int,
    next_run_iso: str,
    input_text: str,
    admin_id: int,
) -> dict[str, Any]:
    before_rule = db.get_rule(rule_id)
    ok = scheduler_service.set_next_run(rule_id, next_run_iso)
    after_rule = db.get_rule(rule_id)

    if ok:
        db.log_rule_change(
            event_type="rule_next_run_changed",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "next_run_at": before_rule.next_run_at if before_rule else None,
            },
            new_value={
                "next_run_at": after_rule.next_run_at if after_rule else None,
                "input_local_time": input_text,
            },
        )

    return {
        "ok": bool(ok),
    }

def _change_fixed_times_sync(
    rule_id: int,
    normalized_times: list[str],
    admin_id: int,
) -> dict[str, Any]:
    before_rule = db.get_rule(rule_id)
    ok = scheduler_service.update_fixed_times(rule_id, normalized_times)
    after_rule = db.get_rule(rule_id)

    if ok:
        db.log_rule_change(
            event_type="rule_fixed_times_changed",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "fixed_times": before_rule.fixed_times() if before_rule else [],
            },
            new_value={
                "fixed_times": after_rule.fixed_times() if after_rule else [],
            },
        )

    return {
        "ok": bool(ok),
    }

def _change_interval_sync(
    rule_id: int,
    action: str,
    interval: int,
    admin_id: int,
) -> dict[str, Any]:
    before_rule = db.get_rule(rule_id)

    if action == "set_interval_mode":
        ok = scheduler_service.update_interval(rule_id, interval, set_interval_mode=True)
        event_type = "rule_set_interval_mode"
        success_text = "✅ Правило переведено в плавающий режим."
    else:
        ok = scheduler_service.update_interval(rule_id, interval, set_interval_mode=False)
        event_type = "rule_interval_changed"
        success_text = "✅ Интервал обновлён."

    after_rule = db.get_rule(rule_id)

    if ok:
        db.log_rule_change(
            event_type=event_type,
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "interval": before_rule.interval if before_rule else None,
                "schedule_mode": before_rule.schedule_mode if before_rule else None,
            },
            new_value={
                "interval": after_rule.interval if after_rule else None,
                "schedule_mode": after_rule.schedule_mode if after_rule else None,
            },
        )

    return {
        "ok": bool(ok),
        "success_text": success_text,
    }

def _create_rule_sync(
    choice: dict[str, Any],
    interval: int,
    admin_id: int,
) -> int | None:
    tenant = tenant_service.ensure_tenant_exists(admin_id)
    can_create, reason = limit_service.can_create_rule(int(tenant.get("id") or 1))
    if not can_create:
        logger.warning("Лимит создания правил достигнут | admin_id=%s | tenant_id=%s | reason=%s", admin_id, tenant.get("id"), reason)
        return None

    rule_id = db.add_rule(
        choice["source_id"],
        choice["source_thread_id"],
        choice["target_id"],
        choice["target_thread_id"],
        interval,
        admin_id,
    )

    if rule_id:
        db.log_rule_change(
            event_type="rule_created",
            rule_id=rule_id,
            admin_id=admin_id,
            new_value={
                "source_id": choice["source_id"],
                "source_thread_id": choice["source_thread_id"],
                "target_id": choice["target_id"],
                "target_thread_id": choice["target_thread_id"],
                "interval": interval,
                "schedule_mode": "interval",
            },
        )

    return rule_id

def _enable_rule_sync(
    rule_id: int,
    admin_id: int,
) -> dict[str, Any]:
    before_rule = db.get_rule(rule_id)
    ok = scheduler_service.activate_with_backfill(rule_id)
    after_rule = db.get_rule(rule_id)

    if ok:
        db.log_rule_change(
            event_type="rule_enabled",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "is_active": before_rule.is_active if before_rule else None,
            },
            new_value={
                "is_active": after_rule.is_active if after_rule else True,
            },
        )

    return {
        "ok": bool(ok),
    }

def _toggle_rule_mode_sync(rule_id: int, admin_id: int):
    rule = db.get_rule(rule_id)
    if not rule:
        return {"ok": False, "reason": "rule_not_found"}

    new_mode = "video" if rule.mode == "repost" else "repost"
    ok = db.update_rule_mode(rule_id, new_mode)

    if ok:
        db.log_rule_change(
            event_type="rule_mode_changed",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={"mode": rule.mode},
            new_value={"mode": new_mode},
        )

    return {"ok": ok, "new_mode": new_mode}

def _disable_rule_sync(
    rule_id: int,
    admin_id: int,
) -> dict[str, Any]:
    before_rule = db.get_rule(rule_id)
    ok = db.set_rule_active(rule_id, False)
    after_rule = db.get_rule(rule_id)

    if ok:
        db.log_rule_change(
            event_type="rule_disabled",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "is_active": before_rule.is_active if before_rule else None,
            },
            new_value={
                "is_active": after_rule.is_active if after_rule else False,
            },
        )

    return {
        "ok": bool(ok),
        "before_rule": before_rule,
        "after_rule": after_rule,
    }

def _trigger_rule_now_sync(rule_id: int, admin_id: int):
    before_rule = db.get_rule(rule_id)
    ok = scheduler_service.trigger_now(rule_id)
    after_rule = db.get_rule(rule_id)

    if ok:
        db.log_rule_change(
            event_type="rule_triggered_now",
            rule_id=rule_id,
            admin_id=admin_id,
            old_value={
                "next_run_at": before_rule.next_run_at if before_rule else None,
                "is_active": before_rule.is_active if before_rule else None,
            },
            new_value={
                "next_run_at": after_rule.next_run_at if after_rule else None,
                "is_active": after_rule.is_active if after_rule else None,
            },
        )

    return {
        "ok": ok,
    }

def _apply_caption_delivery_mode_sync(rule_id: int, new_mode: str, admin_id: int):
    rule = db.get_rule(rule_id)
    if not rule:
        return {"ok": False, "reason": "rule_not_found"}

    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "repost":
        return {"ok": False, "reason": "wrong_mode"}

    current_mode = get_rule_caption_mode_value(rule_id)
    new_mode = (new_mode or "auto").strip().lower()

    if new_mode not in {"copy_first", "builder_first", "auto"}:
        return {"ok": False, "reason": "bad_mode"}

    if current_mode == new_mode:
        return {"ok": False, "reason": "same_mode", "current_mode": current_mode}

    ok = db.update_rule_caption_delivery_mode(rule_id, new_mode)
    if not ok:
        return {"ok": False, "reason": "save_failed"}

    db.log_rule_change(
        event_type="rule_caption_delivery_mode_changed",
        rule_id=rule_id,
        admin_id=admin_id,
        old_value={
            "caption_delivery_mode": current_mode,
        },
        new_value={
            "caption_delivery_mode": new_mode,
        },
    )

    return {
        "ok": True,
        "current_mode": current_mode,
        "new_mode": new_mode,
    }

def _apply_video_caption_delivery_mode_sync(
    rule_id: int,
    new_mode: str,
    admin_id: int,
) -> dict[str, Any]:
    rule = db.get_rule(rule_id)
    if not rule:
        return {
            "ok": False,
            "reason": "rule_not_found",
        }

    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "video":
        return {
            "ok": False,
            "reason": "wrong_mode",
        }

    current_mode = (
        getattr(rule, "video_caption_delivery_mode", "auto") or "auto"
    ).strip().lower()
    normalized_new_mode = (new_mode or "auto").strip().lower()

    if normalized_new_mode not in {"copy_first", "builder_first", "auto"}:
        return {
            "ok": False,
            "reason": "bad_mode",
        }

    if current_mode == normalized_new_mode:
        return {
            "ok": False,
            "reason": "same_mode",
            "current_mode": current_mode,
            "new_mode": normalized_new_mode,
        }

    ok = db.update_rule_video_caption_delivery_mode(rule_id, normalized_new_mode)
    if not ok:
        return {
            "ok": False,
            "reason": "save_failed",
            "current_mode": current_mode,
            "new_mode": normalized_new_mode,
        }

    db.log_rule_change(
        event_type="rule_video_caption_delivery_mode_changed",
        rule_id=rule_id,
        admin_id=admin_id,
        old_value={
            "video_caption_delivery_mode": current_mode,
        },
        new_value={
            "video_caption_delivery_mode": normalized_new_mode,
        },
    )

    return {
        "ok": True,
        "reason": "saved",
        "current_mode": current_mode,
        "new_mode": normalized_new_mode,
    }

def _apply_intro_sync(rule_id: int, mode: str, intro_id_val: int | None):
    if mode == "horizontal":
        db.set_rule_intro_horizontal(rule_id, intro_id_val)
    else:
        db.set_rule_intro_vertical(rule_id, intro_id_val)

    row = get_rule_stats_row(rule_id)
    if not row:
        return None

    horizontal_id = row["video_intro_horizontal_id"] if "video_intro_horizontal_id" in row.keys() else None
    vertical_id = row["video_intro_vertical_id"] if "video_intro_vertical_id" in row.keys() else None

    enable_intro = bool(horizontal_id or vertical_id)
    db.set_rule_add_intro(rule_id, enable_intro)

    return get_rule_stats_row(rule_id)

def _build_rule_card_render_sync(rule_id: int, prefix_text: str | None = None) -> dict | None:
    row = get_rule_stats_row(rule_id)
    if not row:
        return None

    text = build_rule_card_text(row)

    if prefix_text:
        text = f"{prefix_text}\n\n{text}"

    reply_markup = build_rule_card_keyboard(
        rule_id,
        bool(row["is_active"]),
        row["schedule_mode"] or "interval",
        row["mode"] or "repost",
    )

    return {
        "text": text,
        "reply_markup": reply_markup,
    }

async def _build_rule_card_render_async(rule_id: int, prefix_text: str | None = None) -> dict | None:
    return await asyncio.to_thread(_build_rule_card_render_sync, rule_id, prefix_text)

async def refresh_rule_card_by_ids(
    *,
    chat_id: int | str,
    message_id: int,
    rule_id: int,
    prefix_text: str | None = None,
) -> str:
    """
    Возвращает:
    - "updated"
    - "not_modified"
    - "resent"
    - "failed"
    """
    text, reply_markup, cache_status = await build_rule_card_payload_cached(rule_id)
    if not text or not reply_markup:
        return "failed"

    if prefix_text:
        text = f"{prefix_text}\n\n{text}"

    logger.info(
        "RULE_CARD_REFRESH | rule_id=%s | cache_status=%s",
        rule_id,
        cache_status,
    )

    # КЛЮЧЕВОЕ:
    # если cache_hit и нет prefix_text — не дёргаем edit вообще
    if cache_status in {"cache_hit", "cache_hit_after_wait"} and not prefix_text:
        return "not_modified"

    edit_result = await try_edit_message_text_by_ids_safe(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode="HTML",
        reply_markup=reply_markup,
    )

    if edit_result == "updated":
        return "updated"

    if edit_result == "not_modified":
        return "not_modified"

    if edit_result == "failed":
        return "failed"

    if edit_result == "gone":
        try:
            sent = await send_message_safe(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
            return "resent" if sent else "failed"
        except Exception as exc:
            logger.exception(
                "Не удалось ни обновить, ни отправить карточку правила #%s по ids chat_id=%s message_id=%s: %s",
                rule_id,
                chat_id,
                message_id,
                exc,
            )
            return "failed"

    return "failed"

async def refresh_input_prompt_by_ids(
    *,
    chat_id: int | str,
    message_id: int,
    text: str,
    rule_id: int | None = None,
) -> str:
    reply_markup = build_rule_input_inline_keyboard(rule_id) if rule_id else None

    edit_result = await try_edit_message_text_by_ids_safe(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        reply_markup=reply_markup,
    )

    if edit_result == "updated":
        return "updated"

    if edit_result == "not_modified":
        return "not_modified"

    if edit_result == "failed":
        return "failed"

    if edit_result == "gone":
        try:
            sent = await send_message_safe(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
            )
            return "resent" if sent else "failed"
        except Exception as exc:
            logger.exception(
                "Не удалось ни обновить, ни отправить input prompt | chat_id=%s | message_id=%s | rule_id=%s | error=%s",
                chat_id,
                message_id,
                rule_id,
                exc,
            )
            return "failed"

    return "failed"

async def answer_callback_safe(
    callback: CallbackQuery,
    text: str | None = None,
    show_alert: bool = False,
) -> bool:
    global ui_policy

    if ui_policy is not None:
        try:
            result = await ui_policy.answer_callback(
                callback,
                text=text,
                show_alert=show_alert,
            )
            return bool(result.ok)
        except Exception as exc:
            logger.exception("UI policy answer_callback failed: %s", exc)
            return False

    try:
        await callback.answer(text=text, show_alert=show_alert)
        return True
    except Exception as exc:
        error_text = str(exc).lower()
        if (
            "query is too old" in error_text
            or "response timeout expired" in error_text
            or "query id is invalid" in error_text
        ):
            logger.warning(
                "callback.answer пропущен: callback устарел. data=%r error=%s",
                callback.data,
                exc,
            )
            return False

        logger.exception(
            "Ошибка callback.answer data=%r: %s",
            callback.data,
            exc,
        )
        return False

async def edit_message_text_safe(
    *,
    message,
    text: str,
    reply_markup=None,
    parse_mode: str | None = None,
    disable_web_page_preview: bool | None = None,
) -> bool:
    global ui_policy

    if ui_policy is None:
        try:
            await message.edit_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
            return True
        except Exception:
            logger.exception("edit_message_text_safe fallback failed")
            return False

    result = await ui_policy.edit_text_from_message(
        message=message,
        text=text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        disable_web_page_preview=disable_web_page_preview,
    )
    return bool(result.ok)


async def send_message_safe(
    *,
    chat_id: int | str,
    text: str,
    reply_markup=None,
    parse_mode: str | None = None,
    disable_web_page_preview: bool | None = None,
    message_thread_id: int | None = None,
):
    global ui_policy, bot

    if ui_policy is None:
        try:
            return await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                message_thread_id=message_thread_id,
            )
        except Exception:
            logger.exception("send_message_safe fallback failed")
            return None

    result = await ui_policy.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        disable_web_page_preview=disable_web_page_preview,
        message_thread_id=message_thread_id,
    )
    return result.result


async def delete_message_safe(
    *,
    chat_id: int | str,
    message_id: int,
) -> bool:
    global ui_policy, bot

    if ui_policy is None:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            return True
        except Exception:
            logger.exception("delete_message_safe fallback failed")
            return False

    result = await ui_policy.delete_message(
        chat_id=chat_id,
        message_id=message_id,
    )
    return bool(result.ok)


async def delete_from_message_safe(message) -> bool:
    global ui_policy

    if ui_policy is None:
        try:
            chat_id = message.chat.id
            message_id = message.message_id
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            return True
        except Exception:
            logger.exception("delete_from_message_safe fallback failed")
            return False

    result = await ui_policy.delete_from_message(message=message)
    return bool(result.ok)

def _telegram_error_text(exc: Exception) -> str:
    try:
        return str(exc or "").lower().strip()
    except Exception:
        return ""


def is_message_not_modified_error(exc: Exception) -> bool:
    text = _telegram_error_text(exc)
    return "message is not modified" in text


def is_message_id_invalid_error(exc: Exception) -> bool:
    text = _telegram_error_text(exc)
    return (
        "message_id_invalid" in text
        or "message to edit not found" in text
        or "message can't be edited" in text
        or "message to delete not found" in text
        or "message identifier is not specified" in text
    )


def is_message_not_found_error(exc: Exception) -> bool:
    text = _telegram_error_text(exc)
    return (
        "message to delete not found" in text
        or "message to edit not found" in text
        or "message_id_invalid" in text
    )


async def try_edit_message_text_safe(
    *,
    message,
    text: str,
    parse_mode: str | None = None,
    reply_markup=None,
    disable_web_page_preview: bool | None = None,
) -> str:
    """
    Возвращает:
    - "updated"      -> сообщение реально обновлено
    - "not_modified" -> сообщение живо, но текст/markup не изменились
    - "gone"         -> сообщение уже недоступно / нельзя редактировать
    - "failed"       -> прочая ошибка
    """
    if not message:
        return "gone"

    try:
        await message.edit_text(
            text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
        )
        return "updated"

    except Exception as exc:
        if is_message_not_modified_error(exc):
            return "not_modified"

        if is_message_id_invalid_error(exc) or is_message_not_found_error(exc):
            logger.warning(
                "UI edit skipped: сообщение уже недоступно | chat_id=%s | message_id=%s | error=%s",
                getattr(getattr(message, "chat", None), "id", None),
                getattr(message, "message_id", None),
                exc,
            )
            return "gone"

        logger.exception(
            "UI edit failed | chat_id=%s | message_id=%s | error=%s",
            getattr(getattr(message, "chat", None), "id", None),
            getattr(message, "message_id", None),
            exc,
        )
        return "failed"


async def try_edit_message_text_by_ids_safe(
    *,
    chat_id: int | str,
    message_id: int,
    text: str,
    parse_mode: str | None = None,
    reply_markup=None,
    disable_web_page_preview: bool | None = None,
) -> str:
    """
    Возвращает:
    - "updated"
    - "not_modified"
    - "gone"
    - "failed"
    """
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
        )
        return "updated"

    except Exception as exc:
        if is_message_not_modified_error(exc):
            return "not_modified"

        if is_message_id_invalid_error(exc) or is_message_not_found_error(exc):
            logger.warning(
                "UI edit by ids skipped: сообщение уже недоступно | chat_id=%s | message_id=%s | error=%s",
                chat_id,
                message_id,
                exc,
            )
            return "gone"

        logger.exception(
            "UI edit by ids failed | chat_id=%s | message_id=%s | error=%s",
            chat_id,
            message_id,
            exc,
        )
        return "failed"


async def try_delete_message_safe(chat_id: int | str, message_id: int | None) -> bool:
    if not chat_id or not message_id:
        return False

    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True

    except Exception as exc:
        if is_message_id_invalid_error(exc) or is_message_not_found_error(exc):
            logger.warning(
                "UI delete skipped: сообщение уже недоступно | chat_id=%s | message_id=%s | error=%s",
                chat_id,
                message_id,
                exc,
            )
            return False

        logger.exception(
            "UI delete failed | chat_id=%s | message_id=%s | error=%s",
            chat_id,
            message_id,
            exc,
        )
        return False

async def try_copy_message_safe(
    *,
    from_chat_id: int | str,
    chat_id: int | str,
    message_id: int,
    message_thread_id: int | None = None,
):
    try:
        return await bot.copy_message(
            chat_id=chat_id,
            from_chat_id=from_chat_id,
            message_id=message_id,
            message_thread_id=message_thread_id,
        )
    except Exception as exc:
        logger.warning(
            "UI copy failed | from_chat_id=%s | chat_id=%s | message_id=%s | thread_id=%s | error=%s",
            from_chat_id,
            chat_id,
            message_id,
            message_thread_id,
            exc,
        )
        return None


async def try_forward_message_safe(
    *,
    from_chat_id: int | str,
    chat_id: int | str,
    message_id: int,
    message_thread_id: int | None = None,
):
    try:
        return await bot.forward_message(
            chat_id=chat_id,
            from_chat_id=from_chat_id,
            message_id=message_id,
            message_thread_id=message_thread_id,
        )
    except Exception as exc:
        logger.warning(
            "UI forward failed | from_chat_id=%s | chat_id=%s | message_id=%s | thread_id=%s | error=%s",
            from_chat_id,
            chat_id,
            message_id,
            message_thread_id,
            exc,
        )
        return None

async def answer_callback_safe_once(
    callback: CallbackQuery,
    text: str | None = None,
    show_alert: bool = False,
) -> bool:
    if getattr(callback, "_safe_answer_sent", False):
        return False

    ok = await answer_callback_safe(
        callback=callback,
        text=text,
        show_alert=show_alert,
    )

    if ok:
        setattr(callback, "_safe_answer_sent", True)

    return ok

def _state_prompt_ids(state: dict[str, Any]) -> tuple[int | str | None, int | None]:
    prompt_chat_id = state.get("prompt_chat_id")
    prompt_message_id = state.get("prompt_message_id")

    if prompt_chat_id and prompt_message_id:
        return prompt_chat_id, prompt_message_id

    card_chat_id = state.get("card_chat_id")
    card_message_id = state.get("card_message_id")

    if card_chat_id and card_message_id:
        return card_chat_id, card_message_id

    return None, None


async def _finalize_rule_state_input(
    message: Message,
    state: dict[str, Any],
    rule_id: int,
    *,
    prefix_text: str | None = None,
    success_fallback_text: str = "✅ Сохранено",
) -> None:
    """
    Единый SaaS-финализатор для stateful-ввода по карточке правила.

    Что делает:
    - удаляет пользовательский ввод
    - удаляет отдельный prompt, если он не совпадает с карточкой
    - пытается вернуть/обновить карточку правила по её message_id
    - если карточка недоступна, отправляет новую
    """

    await try_delete_message_safe(message.chat.id, message.message_id)

    card_chat_id = state.get("card_chat_id")
    card_message_id = state.get("card_message_id")

    prompt_chat_id = state.get("prompt_chat_id")
    prompt_message_id = state.get("prompt_message_id")

    same_prompt_as_card = (
        prompt_chat_id == card_chat_id
        and prompt_message_id == card_message_id
        and prompt_chat_id is not None
        and prompt_message_id is not None
    )

    if prompt_chat_id and prompt_message_id and not same_prompt_as_card:
        await try_delete_message_safe(prompt_chat_id, prompt_message_id)

    refresh_result = None
    if card_chat_id and card_message_id:
        refresh_result = await refresh_rule_card_by_ids(
            chat_id=card_chat_id,
            message_id=card_message_id,
            rule_id=rule_id,
            prefix_text=prefix_text,
        )

    if refresh_result in ("updated", "not_modified", "resent"):
        return

    row = await get_rule_stats_row_async(rule_id)
    if row:
        await message.answer(
            build_rule_card_text(row),
            parse_mode="HTML",
            reply_markup=build_rule_card_keyboard(
                rule_id,
                bool(row["is_active"]),
                row["schedule_mode"] or "interval",
                row["mode"] or "repost",
            ),
        )
        return

    await message.answer(
        success_fallback_text,
        reply_markup=get_main_menu(),
    )

def _safe_json_loads(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def audit_event_title(event_type: str) -> str:
    mapping = {
        "delivery_started": "🚚 Доставка взята в работу",
        "delivery_sent": "✅ Доставка выполнена",
        "delivery_failed": "❌ Ошибка доставки",
        "delivery_process_exception": "💥 Исключение в обработке доставки",
        "delivery_rolled_back": "⏪ Пост возвращён в очередь",
        "faulty_log_cleared": "🧹 Лог проблемной доставки очищен",

        "rule_created": "➕ Правило создано",
        "rule_deleted": "🗑 Правило удалено",
        "rule_enabled": "▶️ Правило включено",
        "rule_disabled": "⏸ Правило выключено",
        "rule_interval_changed": "⏱ Интервал изменён",
        "rule_next_run_changed": "🕒 Время следующего поста изменено",
        "rule_fixed_times_changed": "📌 Фиксированные времена изменены",
        "rule_set_interval_mode": "🟢 Перевод в плавающий режим",
        "rule_triggered_now": "⚡ Немедленная отправка",
        "rule_mode_changed": "🎛 Режим правила изменён",
        "rule_video_caption_changed": "📝 Подпись видеорежима изменена",
        "rule_caption_delivery_mode_changed": "✍️ Режим подписи изменён",
    }
    return mapping.get(event_type, event_type)

def _short_time_from_iso(iso_value: str | None) -> str | None:
    if not iso_value:
        return None
    try:
        dt = datetime.fromisoformat(iso_value).astimezone(USER_TZ)
        return dt.strftime("%H:%M")
    except Exception:
        return None

def audit_row_time_local(created_at: Any | None) -> str:
    if not created_at:
        return "??.??.???? ??:??:??"
    try:
        if isinstance(created_at, datetime):
            dt = created_at
        elif isinstance(created_at, (int, float)):
            dt = datetime.fromtimestamp(float(created_at), tz=timezone.utc)
        else:
            dt = datetime.fromisoformat(str(created_at))

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        dt = dt.astimezone(USER_TZ)
        return dt.strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return "??.??.???? ??:??:??"

def faulty_delivery_time_from_audit(delivery_id: int) -> str:
    rows = db.get_audit_for_delivery(delivery_id, limit=50)

    for row in rows:
        event_type = row["event_type"] or ""
        if event_type in ("delivery_failed", "delivery_process_exception"):
            return audit_row_time_local(row["created_at"])

    return "время не зафиксировано"

def format_audit_details(row) -> list[str]:
    event_type = row["event_type"] or ""
    old_data = _safe_json_loads(row["old_value_json"])
    new_data = _safe_json_loads(row["new_value_json"])
    extra_data = _safe_json_loads(row["extra_json"])
    error_text = (row["error_text"] or "").strip()

    details: list[str] = []

    if event_type == "delivery_started":
        method = extra_data.get("method")
        msg_id = extra_data.get("message_id")
        source = extra_data.get("source_channel")
        target = extra_data.get("target_id")
        schedule_mode = extra_data.get("schedule_mode")

        parts = []
        if method:
            parts.append(str(method))
        if msg_id is not None:
            parts.append(f"msg {msg_id}")
        if source and target:
            parts.append(f"{source} → {target}")
        if schedule_mode:
            parts.append(f"режим: {schedule_mode}")

        if parts:
            details.append("  " + " | ".join(parts))

    elif event_type == "delivery_sent":
        method = extra_data.get("method")
        sent_message_id = extra_data.get("sent_message_id")
        source_message_id = extra_data.get("source_message_id")

        parts = []
        if method:
            parts.append(str(method))
        if source_message_id is not None:
            parts.append(f"src msg {source_message_id}")
        if sent_message_id is not None:
            parts.append(f"dst msg {sent_message_id}")

        if parts:
            details.append("  " + " | ".join(parts))

    elif event_type == "rule_caption_delivery_mode_changed":
        old_mode = old_data.get("caption_delivery_mode")
        new_mode = new_data.get("caption_delivery_mode")

        details.append(
            f"  {caption_delivery_mode_to_text(old_mode)} → {caption_delivery_mode_to_text(new_mode)}"
        )

    elif event_type in {"delivery_failed", "delivery_process_exception"}:
        method = extra_data.get("method")
        msg_id = extra_data.get("source_message_id") or extra_data.get("message_id")

        parts = []
        if method:
            parts.append(str(method))
        if msg_id is not None:
            parts.append(f"msg {msg_id}")

        if parts:
            details.append("  " + " | ".join(parts))
        if error_text:
            details.append(f"  ❌ {error_text[:160]}")

    elif event_type == "delivery_rolled_back":
        delivery_id = row["delivery_id"]
        post_id = row["post_id"]

        if delivery_id is not None:
            details.append(f"  Возвращена доставка #{delivery_id}")
        if post_id is not None:
            details.append(f"  Исходный пост #{post_id}")

        details.append("  Будет отправлен повторно")

    elif event_type == "rule_interval_changed":
        old_interval = old_data.get("interval")
        new_interval = new_data.get("interval")
        if old_interval is not None or new_interval is not None:
            details.append(
                f"  {interval_to_text(int(old_interval or 0))} → {interval_to_text(int(new_interval or 0))}"
            )

    elif event_type == "rule_next_run_changed":
        old_run = _short_time_from_iso(old_data.get("next_run_at"))
        new_run = _short_time_from_iso(new_data.get("next_run_at"))
        input_local = new_data.get("input_local_time")

        if old_run or new_run:
            details.append(f"  {old_run or '—'} → {new_run or '—'}")
        if input_local:
            details.append(f"  Введено: {input_local} (UTC+3)")

    elif event_type == "rule_fixed_times_changed":
        old_times = old_data.get("fixed_times") or []
        new_times = new_data.get("fixed_times") or []

        details.append(
            f"  {', '.join(old_times) if old_times else '—'} → {', '.join(new_times) if new_times else '—'}"
        )

    elif event_type == "rule_set_interval_mode":
        old_mode = old_data.get("schedule_mode")
        new_mode = new_data.get("schedule_mode")
        new_interval = new_data.get("interval")

        parts = []
        if old_mode or new_mode:
            parts.append(f"{old_mode or '—'} → {new_mode or '—'}")
        if new_interval is not None:
            parts.append(f"интервал {interval_to_text(int(new_interval))}")

        if parts:
            details.append("  " + " | ".join(parts))

    elif event_type == "rule_triggered_now":
        old_run = _short_time_from_iso(old_data.get("next_run_at"))
        new_run = _short_time_from_iso(new_data.get("next_run_at"))
        if old_run or new_run:
            details.append(f"  {old_run or '—'} → {new_run or '—'}")

    elif event_type == "rule_enabled":
        details.append("  Правило активировано")

    elif event_type == "rule_disabled":
        details.append("  Правило остановлено")

    elif event_type == "rule_created":
        source_id = new_data.get("source_id")
        target_id = new_data.get("target_id")
        interval = new_data.get("interval")

        parts = []
        if source_id and target_id:
            parts.append(f"{source_id} → {target_id}")
        if interval is not None:
            parts.append(f"интервал {interval_to_text(int(interval))}")

        if parts:
            details.append("  " + " | ".join(parts))

    elif event_type == "rule_deleted":
        interval = old_data.get("interval")
        schedule_mode = old_data.get("schedule_mode")
        next_run = _short_time_from_iso(old_data.get("next_run_at"))

        parts = []
        if interval is not None:
            parts.append(f"интервал {interval_to_text(int(interval))}")
        if schedule_mode:
            parts.append(f"режим {schedule_mode}")
        if next_run:
            parts.append(f"следующий пост {next_run}")

        if parts:
            details.append("  " + " | ".join(parts))

    elif error_text:
        details.append(f"  ❌ {error_text[:160]}")

    return details

def format_audit_details_html(row) -> list[str]:
    lines = []
    for detail in format_audit_details(row):
        clean = detail.strip()
        if not clean:
            continue

        if clean.startswith("❌ "):
            lines.append(f"  ❌ {escape(clean[2:].strip())}")
        else:
            lines.append(f"  <code>{escape(clean)}</code>")

    return lines

def build_audit_event_block_html(row, include_rule: bool = False) -> str:
    time_part = audit_row_time_local(row["created_at"])
    title = escape(audit_event_title(row["event_type"] or "unknown"))
    status = escape(row["status"] or "")

    rule_part = ""
    if include_rule and row["rule_id"] is not None:
        rule_part = f" · правило #{row['rule_id']}"

    line = f"• <b>{escape(time_part)}</b> — {title}{escape(rule_part)}"
    if status:
        line += f" [{status}]"

    details = format_audit_details_html(row)
    if details:
        return "\n".join([line] + details)

    return line

def paginate_html_blocks(
    header_html: str,
    blocks: list[str],
    soft_limit: int = TG_TEXT_SOFT_LIMIT,
) -> list[str]:
    pages: list[str] = []
    current = header_html.strip()

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        candidate = f"{current}\n\n{block}" if current else block

        if len(candidate) <= soft_limit:
            current = candidate
            continue

        if current:
            pages.append(current)

        if len(block) <= soft_limit:
            current = f"{header_html.strip()}\n\n{block}" if header_html.strip() else block
        else:
            # Теоретический защитный случай:
            # если один блок сам больше лимита, режем только его.
            chunk_start = 0
            while chunk_start < len(block):
                chunk = block[chunk_start:chunk_start + soft_limit]
                if header_html.strip() and chunk_start == 0:
                    pages.append(f"{header_html.strip()}\n\n{chunk}")
                else:
                    pages.append(chunk)
                chunk_start += soft_limit
            current = header_html.strip()

    if current:
        pages.append(current)

    return pages

def build_system_journal_pages(limit: int = 300) -> list[str]:
    rows = db.get_recent_audit(limit=limit)
    if not rows:
        return ["<b>📊 Журнал системы</b>\n\nПока пуст."]

    rows = list(reversed(rows))
    blocks = [build_audit_event_block_html(row, include_rule=True) for row in rows]

    pages = paginate_html_blocks(
        header_html="<b>📊 ЖУРНАЛ СИСТЕМЫ</b>",
        blocks=blocks,
    )

    return list(reversed(pages))


def build_rule_log_pages(rule_id: int, limit: int = 200) -> list[str]:
    rows = db.get_audit_for_rule(rule_id, limit=limit)
    if not rows:
        return [f"<b>🧾 Логи правила #{rule_id}</b>\n\nПока пусто."]

    rows = list(reversed(rows))
    blocks = [build_audit_event_block_html(row, include_rule=False) for row in rows]

    pages = paginate_html_blocks(
        header_html=f"<b>🧾 ЛОГИ ПРАВИЛА #{rule_id}</b>",
        blocks=blocks,
    )

    return list(reversed(pages))


def clamp_page(page: int, total_pages: int) -> int:
    if total_pages <= 0:
        return 0
    return max(0, min(page, total_pages - 1))

def parse_callback_parts(data: str, expected_prefix: str, expected_len: int) -> list[str]:
    parts = data.split(":")

    if len(parts) != expected_len:
        raise ValueError(f"Invalid callback format: {data}")

    if parts[0] != expected_prefix:
        raise ValueError(f"Unexpected callback prefix: {data}")

    return parts

def build_system_journal_inline_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup:
    nav_row: list[InlineKeyboardButton] = []

    if total_pages > 1:
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(text="⬅️", callback_data=f"syslog_page:{page-1}")
            )

        nav_row.append(
            InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="syslog_page_info")
        )

        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(text="➡️", callback_data=f"syslog_page:{page+1}")
            )

    rows = []
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"syslog_refresh:{page}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="syslog_back")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_rule_logs_inline_keyboard(rule_id: int, page: int, total_pages: int) -> InlineKeyboardMarkup:
    nav_row: list[InlineKeyboardButton] = []

    if total_pages > 1:
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(text="⬅️", callback_data=f"rule_logs_page:{rule_id}:{page-1}")
            )

        nav_row.append(
            InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="rule_logs_page_info")
        )

        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(text="➡️", callback_data=f"rule_logs_page:{rule_id}:{page+1}")
            )

    rows = []
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"rule_logs_refresh:{rule_id}:{page}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад к правилу", callback_data=f"rule_card:{rule_id}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_faulty_inline_keyboard(page: int, total_pages: int, delivery_id: int | None = None) -> InlineKeyboardMarkup:
    nav_row: list[InlineKeyboardButton] = []

    if total_pages > 1:
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(text="⬅️", callback_data=f"faulty_page:{page-1}")
            )

        nav_row.append(
            InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="faulty_page_info")
        )

        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(text="➡️", callback_data=f"faulty_page:{page+1}")
            )

    rows = []

    if delivery_id is not None:
        rows.append([
            InlineKeyboardButton(text="✅ Взята в работу", callback_data=f"faulty_ack:{delivery_id}:{page}"),
        ])
        rows.append([
            InlineKeyboardButton(text="🧹 Очистить лог", callback_data=f"faulty_clear:{delivery_id}:{page}"),
        ])

    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"faulty_refresh:{page}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="faulty_back")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_start_from_number_input_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ Назад к правилу",
                    callback_data=f"rule_card:{rule_id}",
                )
            ]
        ]
    )

def build_start_position_keyboard(rule_id: int, position: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ Предыдущий", callback_data=f"startpos_prev:{rule_id}:{position}"),
                InlineKeyboardButton(text="➡️ Следующий", callback_data=f"startpos_next:{rule_id}:{position}"),
            ],
            [
                InlineKeyboardButton(text="✅ Начать с этого", callback_data=f"startpos_apply:{rule_id}:{position}"),
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"startpos_cancel:{rule_id}"),
            ],
        ]
    )

def invalidate_preview_cache(rule_id: int | None = None) -> None:
    if rule_id is None:
        preview_queue_cache.clear()
        return
    preview_queue_cache.pop(rule_id, None)


def get_preview_queue_items(rule_id: int) -> list[dict[str, Any]]:
    now_ts = datetime.now(timezone.utc).timestamp()
    cached = preview_queue_cache.get(rule_id)

    if cached:
        created_ts = cached.get("created_ts", 0)
        if now_ts - created_ts <= PREVIEW_CACHE_TTL_SECONDS:
            return cached["items"]

    items = db.get_rule_queue_logical_items(rule_id)
    preview_queue_cache[rule_id] = {
        "created_ts": now_ts,
        "items": items,
    }
    return items

async def get_preview_queue_items_async(rule_id: int) -> list[dict[str, Any]]:
    return await run_db(get_preview_queue_items, rule_id)

async def get_preview_item_by_position_async(rule_id: int, position: int) -> dict[str, Any] | None:
    return await run_db(get_preview_item_by_position, rule_id, position)

async def get_preview_item_shifted_async(rule_id: int, current_position: int, shift: int) -> dict[str, Any] | None:
    return await run_db(get_preview_item_shifted, rule_id, current_position, shift)

def get_preview_item_by_position(rule_id: int, position: int) -> dict[str, Any] | None:
    items = get_preview_queue_items(rule_id)
    if not items:
        return None

    position = max(1, min(position, len(items)))
    return items[position - 1]


def get_preview_item_shifted(rule_id: int, current_position: int, shift: int) -> dict[str, Any] | None:
    items = get_preview_queue_items(rule_id)
    if not items:
        return None

    new_position = current_position + shift
    new_position = max(1, min(new_position, len(items)))
    return items[new_position - 1]

def build_faulty_pages(limit: int = 200) -> list[dict[str, Any]]:
    rows = db.get_faulty_deliveries(limit=limit)
    if not rows:
        return [{
            "text": "✅ Проблемных доставок нет",
            "delivery_id": None,
        }]

    rows = list(reversed(rows))
    pages: list[dict[str, Any]] = []

    for row in rows:
        src = row["source_title"] or row["source_channel"]
        tgt = row["target_title"] or row["target_id"]

        if row["source_thread_id"]:
            src = f"{src} (тема {row['source_thread_id']})"
        if row["target_thread_id"]:
            tgt = f"{tgt} (тема {row['target_thread_id']})"

        status_label, error_type = classify_faulty_row(row)
        error_text = escape((row["error_text"] or "Причина не указана").strip())
        created_at_text = faulty_delivery_time_from_created_at(row["fault_created_at"] or row["created_at"])

        problem_state = db.get_problem_state(f"rule_faulty_{row['rule_id']}")
        in_work_text = ""
        if problem_state and int(problem_state.get("is_active") or 0) == 0:
            in_work_text = "\n  <b>Статус:</b> ✅ Взята в работу"

        text = (
            f"<b>⚠️ ПРОБЛЕМНАЯ ДОСТАВКА</b>\n\n"
            f"<b>{status_label}</b>\n"
            f"<b>Правило #{row['rule_id']}</b>\n"
            f"<code>{escape(created_at_text)}</code>\n"
            f"<code>{escape(str(src))} → {escape(str(tgt))}</code>\n"
            f"<code>Тип: {escape(error_type)}</code>\n"
            f"<code>Сообщение: {row['message_id']} | Попыток: {row['attempt_count']}</code>\n"
            f"❌ {error_text}"
            f"{in_work_text}"
        )

        pages.append({
            "text": text,
            "delivery_id": int(row["id"]),
        })

    return list(reversed(pages))

def classify_faulty_row(row) -> tuple[str, str]:
    try:
        created_dt = datetime.fromisoformat(row["created_at"])
        now_dt = datetime.now(timezone.utc)
        age_minutes = (now_dt - created_dt).total_seconds() / 60
    except Exception:
        age_minutes = 999999

    same_error_count = int(row["same_error_count"] or 1)

    if same_error_count >= 3:
        status_label = f"🔁 Повторяющаяся ({same_error_count})"
    elif age_minutes <= 120:
        status_label = "🆕 Новая"
    else:
        status_label = "💤 Старая"

    text = (row["error_text"] or "").lower()

    if "entity too large" in text or "request entity too large" in text:
        error_type = "📦 Слишком большой файл"
    elif "timeout" in text:
        error_type = "⏱ Таймаут"
    elif "forbidden" in text or "not found" in text:
        error_type = "🚫 Получатель недоступен"
    elif "copy_" in text or "copy " in text:
        error_type = "📤 Ошибка копирования"
    elif "self-loop" in text:
        error_type = "🔁 Self-loop"
    else:
        error_type = "⚠️ Общая ошибка"

    return status_label, error_type

def faulty_delivery_time_from_created_at(created_at: str | None) -> str:
    if not created_at:
        return "время неизвестно"

    try:
        dt = datetime.fromisoformat(created_at)
        dt_local = dt.astimezone(USER_TZ)
        return dt_local.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "ошибка времени"

def build_intro_list_keyboard(
    intros,
    rule_id: int | None = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    if rule_id is not None:
        rows.append([
            InlineKeyboardButton(
                text="🖥 Горизонтальная",
                callback_data=f"video_intro_horizontal:{rule_id}",
            ),
            InlineKeyboardButton(
                text="📱 Вертикальная",
                callback_data=f"video_intro_vertical:{rule_id}",
            ),
        ])

    for intro in intros:
        rows.append([
            InlineKeyboardButton(
                text=f"👁 {intro.display_name}",
                callback_data=f"intro_view:{intro.id}",
            ),
            InlineKeyboardButton(
                text="🗑",
                callback_data=f"intro_delete:{intro.id}",
            ),
        ])

    rows.append([
        InlineKeyboardButton(
            text="➕ Загрузить заставку",
            callback_data="intro_upload",
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад к правилу",
            callback_data="rule_back_from_intro",
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(lambda c: c.data.startswith("video_intro_menu:"))
async def handle_video_intro_menu(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    intros = await run_db(db.get_intros)

    text = (
        f"🎬 Управление заставками\n\n"
        f"Всего заставок: {len(intros)}\n\n"
        f"Выберите заставку для просмотра или удаления.\n"
        f"Или загрузите новую."
    )

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=text,
            reply_markup=build_intro_list_keyboard(intros, rule_id=rule_id),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_video_intro_menu: %s", exc)

    user_states[callback.from_user.id] = {
        "action": "intro_menu",
        "rule_id": rule_id,
    }

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data == "rule_back_from_intro")
async def handle_intro_back(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    state = user_states.get(callback.from_user.id)
    if not state:
        await answer_callback_safe(callback, "Ошибка состояния", show_alert=True)
        return

    rule_id = state.get("rule_id")

    row = await get_rule_stats_row_async(rule_id)
    if not row:
        await answer_callback_safe(callback, "Ошибка", show_alert=True)
        return

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=build_rule_card_text(row),
            parse_mode="HTML",
            reply_markup=build_rule_card_keyboard(
                rule_id,
                bool(row["is_active"]),
                row["schedule_mode"] or "interval",
                row["mode"] or "repost",
            ),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_intro_back: %s", exc)

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("caption_mode_menu:"))
async def handle_caption_mode_menu(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    rule = await run_db(db.get_rule, rule_id)
    if not rule:
        await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return

    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "repost":
        await answer_callback_safe(
            callback,
            "Это меню доступно только для режима репоста",
            show_alert=True,
        )
        return

    current_mode = await run_db(get_rule_caption_mode_value, rule_id)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=build_caption_mode_text(current_mode),
            parse_mode="HTML",
            reply_markup=build_caption_mode_keyboard(rule_id),
        )
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            await answer_callback_safe_once(callback, "Уже открыто")
            return
        logger.exception("Ошибка открытия меню режима подписи rule_id=%s: %s", rule_id, exc)
        await answer_callback_safe(callback, "Не удалось открыть меню", show_alert=True)
        return

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data == "intro_upload")
async def handle_intro_upload(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    prev_state = user_states.get(callback.from_user.id, {})

    user_states[callback.from_user.id] = {
        "action": "intro_upload_wait_file",
        "rule_id": prev_state.get("rule_id"),
    }

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=(
                "Отправьте видео или изображение заставки.\n\n"
                "Название укажите сразу в подписи к файлу.\n\n"
                "Пример подписи:\n"
                "grom_vert"
            ),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_intro_upload: %s", exc)

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("intro_view:"))
async def handle_intro_view(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        intro_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    intro = await run_db(db.get_intro, intro_id)

    if not intro:
        await answer_callback_safe(callback, "❌ Заставка не найдена", show_alert=True)
        return

    import os

    if not intro.file_path or not os.path.exists(intro.file_path):
        await answer_callback_safe(callback, "❌ Файл заставки не найден на диске", show_alert=True)
        return

    input_file = FSInputFile(intro.file_path)

    if intro.duration and intro.duration > 0:
        await callback.message.answer_video(
            input_file,
            caption=(
                f"🎬 {intro.display_name}\n"
                f"⏱ Длительность: {intro.duration} сек"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="intro_back_to_list")]
                ]
            ),
        )
    else:
        await callback.message.answer_photo(
            input_file,
            caption=f"🖼 {intro.display_name}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="intro_back_to_list")]
                ]
            ),
        )

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data == "intro_back_to_list")
async def handle_intro_back_to_list(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        await callback.message.delete()
    except Exception:
        pass

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("intro_delete:"))
async def handle_intro_delete(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        intro_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    ok = await run_db(db.delete_intro, intro_id)
    intros = await run_db(db.get_intros)

    if not ok:
        await answer_callback_safe(callback, "❌ Заставка уже удалена или не найдена", show_alert=True)
        return

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=(
                f"🗑 Заставка удалена\n\n"
                f"🎬 Управление заставками\n\n"
                f"Всего заставок: {len(intros)}\n\n"
                f"Выберите заставку для просмотра или удаления.\n"
                f"Или загрузите новую."
            ),
            reply_markup=build_intro_list_keyboard(intros),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_intro_delete: %s", exc)

    await answer_callback_safe_once(callback, "Удалено")

@dp.callback_query(lambda c: c.data.startswith("video_caption_menu:"))
async def handle_video_caption_menu(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    rule = await run_db(db.get_rule, rule_id)
    if not rule:
        await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return

    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "video":
        await answer_callback_safe(
            callback,
            "Раздел подписи доступен только в видеорежиме",
            show_alert=True,
        )
        return

    try:
        await edit_message_text_safe(
            message=callback.message,
            text = await run_db(build_video_caption_menu_text, rule_id),
            reply_markup=build_video_caption_menu_keyboard(rule_id),
            parse_mode="HTML",
        )
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            await answer_callback_safe_once(callback, "Уже открыто")
            return
        logger.exception("Ошибка handle_video_caption_menu: %s", exc)
        await answer_callback_safe(callback, "Не удалось открыть меню", show_alert=True)
        return

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("video_caption_edit:"))
async def handle_video_caption_edit(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    edited = await edit_message_text_safe(
        message=callback.message,
        text=(
            "📝 <b>Изменение подписи видео</b>\n\n"
            f"Правило #{rule_id}\n\n"
            "Отправьте новый текст подписи одним сообщением.\n\n"
            "Если хотите убрать подпись полностью — нажмите кнопку очистки в предыдущем меню."
        ),
        parse_mode="HTML",
        reply_markup=build_rule_input_inline_keyboard(rule_id),
    )

    if not edited:
        await answer_callback_safe_once(callback, "Не удалось открыть ввод подписи", show_alert=True)
        return

    user_states[callback.from_user.id] = {
        "action": "video_caption",
        "rule_id": rule_id,
        "card_chat_id": callback.message.chat.id,
        "card_message_id": callback.message.message_id,
        "prompt_chat_id": callback.message.chat.id,
        "prompt_message_id": callback.message.message_id,
    }

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("video_caption_clear:"))
async def handle_video_caption_clear(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    before_rule = await run_db(db.get_rule, rule_id)
    ok = await run_db(db.update_rule_video_caption, rule_id, None, None)
    after_rule = await run_db(db.get_rule, rule_id)

    if ok:
        await run_db(
            db.log_rule_change,
            event_type="rule_video_caption_cleared",
            rule_id=rule_id,
            admin_id=callback.from_user.id,
            old_value={
                "video_caption": before_rule.video_caption if before_rule else None,
            },
            new_value={
                "video_caption": None,
            },
        )
        await edit_message_text_safe(
            message=callback.message,
            text = await run_db(build_video_caption_menu_text, rule_id),
            parse_mode="HTML",
            reply_markup=build_video_caption_menu_keyboard(rule_id),
        )
        await answer_callback_safe_once(callback, "Очищено")
    else:
        await answer_callback_safe(callback, "Ошибка при очистке", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("video_intro_horizontal:"))
async def handle_video_intro_horizontal(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    intros = await run_db(db.get_intros)

    if not intros:
        await answer_callback_safe(callback, "Нет заставок", show_alert=True)
        return

    rows = []

    for intro in intros:
        rows.append([
            InlineKeyboardButton(
                text=intro.display_name,
                callback_data=f"apply_intro:horizontal:{rule_id}:{intro.id}",
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="❌ Убрать",
            callback_data=f"apply_intro:horizontal:{rule_id}:none",
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"rule_card:{rule_id}",
        )
    ])

    try:
        await edit_message_text_safe(
            message=callback.message,
            text="🎬 Выбор горизонтальной заставки",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_video_intro_horizontal: %s", exc)

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("video_intro_vertical:"))
async def handle_video_intro_vertical(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    intros = await run_db(db.get_intros)

    if not intros:
        await answer_callback_safe(callback, "Нет заставок", show_alert=True)
        return

    rows = []

    for intro in intros:
        rows.append([
            InlineKeyboardButton(
                text=intro.display_name,
                callback_data=f"apply_intro:vertical:{rule_id}:{intro.id}",
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="❌ Убрать",
            callback_data=f"apply_intro:vertical:{rule_id}:none",
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"rule_card:{rule_id}",
        )
    ])

    try:
        await edit_message_text_safe(
            message=callback.message,
            text="🎬 Выбор вертикальной заставки",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_video_intro_vertical: %s", exc)

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("apply_intro:"))
async def handle_apply_intro(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, mode, rule_id_raw, intro_id_raw = callback.data.split(":")
        rule_id = int(rule_id_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    try:
        intro_id_val = None if intro_id_raw == "none" else int(intro_id_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    row = await run_db(_apply_intro_sync, rule_id, mode, intro_id_val)
    if not row:
        await answer_callback_safe(callback, "Ошибка", show_alert=True)
        return

    invalidate_rule_card_cache(rule_id)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=build_rule_card_text(row),
            reply_markup=build_rule_card_keyboard(
                rule_id,
                bool(row["is_active"]),
                row["schedule_mode"] or "interval",
                row["mode"] or "repost",
            ),
            parse_mode="HTML",
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_apply_intro: %s", exc)

    await answer_callback_safe_once(callback, "Сохранено")

@dp.callback_query(lambda c: c.data.startswith("rule_card:"))
async def handle_rule_card(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    # отвечаем мгновенно
    await answer_callback_safe_once(callback)

    if rule_id in rule_card_open_inflight:
        return

    if _is_debounce_active(rule_card_open_last_ts, rule_id, RULE_REFRESH_DEBOUNCE_SEC):
        return

    _mark_debounce(rule_card_open_last_ts, rule_id)

    rule_card_open_inflight.add(rule_id)
    try:
        result = await refresh_rule_card_message(callback, rule_id)

        if result in {"updated", "not_modified", "resent"}:
            return

        await send_message_safe(
            chat_id=callback.message.chat.id,
            text="❌ Не удалось открыть карточку",
        )
    finally:
        rule_card_open_inflight.discard(rule_id)

@dp.callback_query(lambda c: c.data == "rule_to_main_menu")
async def handle_rule_to_main_menu(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    await try_delete_message_safe(callback.message.chat.id, callback.message.message_id)

    await send_message_safe(
        chat_id=callback.message.chat.id,
        text="📋 Главное меню",
        reply_markup=get_main_menu(),
    )

@dp.callback_query(lambda c: c.data.startswith("rule_extra_menu:"))
async def handle_rule_extra_menu(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=f"⚙️ Дополнительные функции правила #{rule_id}",
            reply_markup=await run_db(build_rule_extra_keyboard, rule_id),
        )
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            return
        logger.exception("Ошибка открытия доп. функций rule_id=%s: %s", rule_id, exc)

@dp.callback_query(lambda c: c.data.startswith("video_caption_mode_menu:"))
async def handle_video_caption_mode_menu(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    rule = await run_db(db.get_rule, rule_id)
    if not rule:
        await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return

    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "video":
        await answer_callback_safe(callback, "Для репоста это меню недоступно", show_alert=True)
        return

    try:
        edited = await edit_message_text_safe(
            message=callback.message,
            text = await run_db(build_video_caption_mode_menu_text, rule_id),
            parse_mode="HTML",
            reply_markup=build_video_caption_mode_keyboard(rule_id),
        )

        if not edited:
            await send_message_safe(
                chat_id=callback.message.chat.id,
                text=build_video_caption_mode_menu_text(rule_id),
                parse_mode="HTML",
                reply_markup=build_video_caption_mode_keyboard(rule_id),
            )
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            await answer_callback_safe_once(callback, "Уже открыто")
            return
        logger.exception("Ошибка handle_video_caption_mode_menu: %s", exc)
        await answer_callback_safe(callback, "Не удалось открыть меню", show_alert=True)
        return

    await answer_callback_safe_once(callback)

async def _apply_caption_delivery_mode(
    callback: CallbackQuery,
    rule_id: int,
    new_mode: str,
) -> None:
    result = await run_db(
        _apply_caption_delivery_mode_sync,
        rule_id,
        new_mode,
        callback.from_user.id if callback.from_user else settings.admin_id,
    )

    reason = result.get("reason")
    current_mode = result.get("current_mode")
    saved_mode = result.get("new_mode") or (new_mode or "auto").strip().lower()

    if not result.get("ok"):
        if reason == "rule_not_found":
            await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return

        if reason == "wrong_mode":
            await answer_callback_safe(callback, "Это доступно только для режима репоста", show_alert=True)
            return

        if reason == "bad_mode":
            await answer_callback_safe(callback, "Некорректный режим", show_alert=True)
            return

        if reason == "same_mode":
            await answer_callback_safe(
                callback,
                f"Уже выбран режим: {caption_delivery_mode_to_text(current_mode)}",
            )
            return

        if reason == "save_failed":
            await answer_callback_safe(callback, "Не удалось сохранить режим", show_alert=True)
            return

        await answer_callback_safe(callback, "Не удалось сохранить режим", show_alert=True)
        return

    menu_text = await run_db(build_caption_mode_text, saved_mode)

    try:
        edited = await edit_message_text_safe(
            message=callback.message,
            text=menu_text,
            reply_markup=build_caption_mode_keyboard(rule_id),
            parse_mode="HTML",
        )

        if not edited:
            await send_message_safe(
                chat_id=callback.message.chat.id,
                text=menu_text,
                reply_markup=build_caption_mode_keyboard(rule_id),
                parse_mode="HTML",
            )
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            await answer_callback_safe(
                callback,
                f"Режим уже: {caption_delivery_mode_to_text(saved_mode)}",
            )
            return

        logger.exception("Ошибка применения режима подписи rule_id=%s: %s", rule_id, exc)
        await answer_callback_safe(callback, "Не удалось обновить меню", show_alert=True)
        return

    await answer_callback_safe_once(
        callback,
        f"Сохранено: {caption_delivery_mode_to_text(saved_mode)}",
    )

async def _apply_video_caption_delivery_mode(
    callback: CallbackQuery,
    rule_id: int,
    new_mode: str,
) -> None:
    result = await run_db(
        _apply_video_caption_delivery_mode_sync,
        rule_id,
        new_mode,
        callback.from_user.id if callback.from_user else settings.admin_id,
    )

    reason = result.get("reason")
    current_mode = result.get("current_mode")
    saved_mode = result.get("new_mode") or (new_mode or "auto").strip().lower()

    if not result.get("ok"):
        if reason == "rule_not_found":
            await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return

        if reason == "wrong_mode":
            await answer_callback_safe(callback, "Для репоста это недоступно", show_alert=True)
            return

        if reason == "bad_mode":
            await answer_callback_safe(callback, "Некорректный режим", show_alert=True)
            return

        if reason == "same_mode":
            await answer_callback_safe(
                callback,
                f"Уже выбран режим: {video_caption_delivery_mode_to_text(current_mode)}",
            )
            return

        if reason == "save_failed":
            await answer_callback_safe(callback, "Не удалось сохранить режим", show_alert=True)
            return

        await answer_callback_safe(callback, "Не удалось сохранить режим", show_alert=True)
        return

    menu_text = await run_db(build_video_caption_mode_menu_text, rule_id)

    try:
        edited = await edit_message_text_safe(
            message=callback.message,
            text=menu_text,
            reply_markup=build_video_caption_mode_keyboard(rule_id),
            parse_mode="HTML",
        )

        if not edited:
            await send_message_safe(
                chat_id=callback.message.chat.id,
                text=menu_text,
                reply_markup=build_video_caption_mode_keyboard(rule_id),
                parse_mode="HTML",
            )
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            await answer_callback_safe(
                callback,
                f"Режим уже: {video_caption_delivery_mode_to_text(saved_mode)}",
            )
            return

        logger.exception("Ошибка применения video caption mode rule_id=%s: %s", rule_id, exc)
        await answer_callback_safe(callback, "Не удалось обновить меню", show_alert=True)
        return

    await answer_callback_safe_once(
        callback,
        f"Сохранено: {video_caption_delivery_mode_to_text(saved_mode)}",
    )

@dp.callback_query(lambda c: c.data.startswith("set_caption_mode_copy_first:"))
async def handle_set_caption_mode_copy_first_repost(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await _apply_caption_delivery_mode(callback, rule_id, "copy_first")


@dp.callback_query(lambda c: c.data.startswith("set_caption_mode_builder_first:"))
async def handle_set_caption_mode_builder_first_repost(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await _apply_caption_delivery_mode(callback, rule_id, "builder_first")


@dp.callback_query(lambda c: c.data.startswith("set_caption_mode_auto:"))
async def handle_set_caption_mode_auto_repost(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await _apply_caption_delivery_mode(callback, rule_id, "auto")

@dp.callback_query(lambda c: c.data.startswith("set_video_caption_mode_copy_first:"))
async def handle_set_caption_mode_copy_first(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await _apply_video_caption_delivery_mode(callback, rule_id, "copy_first")


@dp.callback_query(lambda c: c.data.startswith("set_video_caption_mode_builder_first:"))
async def handle_set_caption_mode_builder_first(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await _apply_video_caption_delivery_mode(callback, rule_id, "builder_first")


@dp.callback_query(lambda c: c.data.startswith("set_video_caption_mode_auto:"))
async def handle_set_caption_mode_auto(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await _apply_video_caption_delivery_mode(callback, rule_id, "auto")

@dp.callback_query(lambda c: c.data.startswith("rule_refresh:"))
async def handle_rule_refresh(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    # отвечаем сразу, пока callback живой
    await answer_callback_safe_once(callback)

    # антишторм: если уже идёт refresh этого правила — игнорируем
    if rule_id in rule_refresh_inflight:
        return

    # антидребезг: если кликнули повторно слишком быстро — игнорируем
    if _is_debounce_active(rule_refresh_last_ts, rule_id, RULE_REFRESH_DEBOUNCE_SEC):
        return

    _mark_debounce(rule_refresh_last_ts, rule_id)

    rule_refresh_inflight.add(rule_id)
    try:
        result = await refresh_rule_card_message(callback, rule_id)

        if result in {"updated", "not_modified", "resent"}:
            return

        await send_message_safe(
            chat_id=callback.message.chat.id,
            text="❌ Не удалось обновить карточку",
        )
    finally:
        rule_refresh_inflight.discard(rule_id)

@dp.callback_query(lambda c: c.data == "rule_to_list")
async def handle_rule_to_list(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    debounce_key = f"{callback.message.chat.id}:{callback.message.message_id}:rule_to_list"
    if _is_debounce_active(rule_to_list_last_ts, debounce_key, 1.0):
        return
    _mark_debounce(rule_to_list_last_ts, debounce_key)

    rules = await run_db(db.get_all_rules)

    if not rules:
        await edit_message_text_safe(
            message=callback.message,
            text="Правил пока нет",
        )
        return

    await edit_message_text_safe(
        message=callback.message,
        text="📜 Список правил:",
        reply_markup=rules_list_keyboard(rules, page=0),
    )

@dp.callback_query(lambda c: c.data.startswith("start_from_number:"))
async def handle_start_from_number(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, rule_id_raw = parse_callback_parts(callback.data, "start_from_number", 2)
        rule_id = int(rule_id_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    user_states[callback.from_user.id] = {
        "action": "start_from_number_wait_value",
        "rule_id": rule_id,
        "card_chat_id": callback.message.chat.id,
        "card_message_id": callback.message.message_id,
    }

    await edit_message_text_safe(
        message=callback.message,
        text=(
            f"↪ <b>Выбор точки старта</b>\n\n"
            f"Правило #{rule_id}\n\n"
            f"Введите примерный номер позиции, с которой хотите начать.\n\n"
            f"Например: <b>50</b>"
        ),
        parse_mode="HTML",
        reply_markup=build_start_from_number_input_keyboard(rule_id),
    )
    await answer_callback_safe(callback)

@dp.callback_query(lambda c: c.data.startswith("rollback:"))
async def rollback_handler(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    result = await run_db(
        db.rollback_last_delivery,
        rule_id=rule_id,
        admin_id=callback.from_user.id if callback.from_user else settings.admin_id,
    )

    if not result:
        await answer_callback_safe(
            callback,
            "❌ Нет отправленного логического поста для отката",
            show_alert=True,
        )
        return

    invalidate_preview_cache(rule_id)
    await ensure_rule_workers()

    kind = result.get("kind") or "single"
    rolled_back_count = int(result.get("rolled_back_count") or 0)
    position = result.get("position")

    if kind == "album":
        human_kind = f"альбом ({rolled_back_count} эл.)"
    elif kind == "video_single":
        human_kind = "видео"
    else:
        human_kind = "пост"

    prefix_text = (
        f"⏪ Откат выполнен.\n"
        f"Возвращён в очередь: {human_kind}"
        + (f"\nПредыдущая логическая позиция: {position}" if position is not None else "")
    )

    await answer_callback_safe_once(callback, "Откат выполнен")

    invalidate_rule_card_cache(rule_id)
    await refresh_rule_card_message(
        callback,
        rule_id,
        prefix_text=prefix_text,
    )

@dp.callback_query(lambda c: c.data.startswith("rule_logs:"))
async def handle_rule_logs(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    pages = await run_db(build_rule_log_pages, rule_id, 200)
    page = 0
    total_pages = len(pages)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=pages[page],
            reply_markup=build_rule_logs_inline_keyboard(rule_id, page, total_pages),
            parse_mode="HTML",
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_rule_logs: %s", exc)

@dp.callback_query(lambda c: c.data == "rule_logs_page_info")
async def handle_rule_logs_page_info(callback: CallbackQuery):
    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("rule_logs_page:"))
async def handle_rule_logs_page(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, rule_id_raw, page_raw = parse_callback_parts(callback.data, "rule_logs_page", 3)
        rule_id = int(rule_id_raw)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    pages = await run_db(build_rule_log_pages, rule_id, 200)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=pages[page],
            reply_markup=build_rule_logs_inline_keyboard(rule_id, page, total_pages),
            parse_mode="HTML",
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_rule_logs_page: %s", exc)

@dp.callback_query(lambda c: c.data.startswith("rule_logs_refresh:"))
async def handle_rule_logs_refresh(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, rule_id_raw, page_raw = parse_callback_parts(callback.data, "rule_logs_refresh", 3)
        rule_id = int(rule_id_raw)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    pages = await run_db(build_rule_log_pages, rule_id, 200)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=pages[page],
            reply_markup=build_rule_logs_inline_keyboard(rule_id, page, total_pages),
            parse_mode="HTML",
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_rule_logs_refresh: %s", exc)

@dp.callback_query(lambda c: c.data.startswith("rescan_rule_menu:"))
async def handle_rescan_rule_menu(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    rule = await run_db(db.get_rule, rule_id)
    if not rule:
        await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return

    source_name = rule.source_title or rule.source_id
    if rule.source_thread_id:
        source_name = f"{source_name} (тема {rule.source_thread_id})"

    try:
        await edit_message_text_safe(
            message=callback.message,
            text=(
                f"🔄 Пересканировать правило #{rule_id}\n\n"
                f"Источник: {source_name}\n\n"
                f"Выбери режим:\n"
                f"• ♻️ Сохранить позицию — пересканировать источник и попытаться сохранить текущую точку старта\n"
                f"• 🆕 Начать заново — полностью обновить источник и очередь правила с начала"
            ),
            reply_markup=build_rescan_rule_keyboard(rule_id),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            logger.exception("Ошибка handle_rescan_rule_menu: %s", exc)

    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("rescan_rule_fresh:"))
async def handle_rescan_rule_fresh(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    task_key = _rule_ui_task_key("rescan_fresh", rule_id)
    started = _schedule_rule_ui_task(
        task_key,
        _run_rescan_rule_fresh_job(
            rule_id=rule_id,
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            admin_id=callback.from_user.id if callback.from_user else settings.admin_id,
        ),
    )

    if not started:
        await send_message_safe(
            chat_id=callback.message.chat.id,
            text=f"⏳ Пересканирование правила #{rule_id} уже выполняется",
        )
        return

    await edit_message_text_safe(
        message=callback.message,
        text=(
            f"🔄 Пересканирование правила #{rule_id}\n\n"
            f"Режим: начать заново\n"
            f"Идёт парсинг источника и пересборка очереди.\n"
            f"Это может занять время."
        ),
        reply_markup=None,
    )

@dp.callback_query(lambda c: c.data.startswith("rescan_rule_keep:"))
async def handle_rescan_rule_keep(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    task_key = _rule_ui_task_key("rescan_keep", rule_id)
    started = _schedule_rule_ui_task(
        task_key,
        _run_rescan_rule_keep_job(
            rule_id=rule_id,
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            admin_id=callback.from_user.id if callback.from_user else settings.admin_id,
        ),
    )

    if not started:
        await send_message_safe(
            chat_id=callback.message.chat.id,
            text=f"⏳ Пересканирование правила #{rule_id} уже выполняется",
        )
        return

    await edit_message_text_safe(
        message=callback.message,
        text=(
            f"🔄 Пересканирование правила #{rule_id}\n\n"
            f"Режим: сохранить позицию\n"
            f"Идёт парсинг источника и пересборка очереди.\n"
            f"Это может занять время."
        ),
        reply_markup=None,
    )

@dp.callback_query(lambda c: c.data.startswith("change_interval:"))
async def handle_change_interval_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    rule_id = int(callback.data.split(":")[1])

    prompt = await callback.message.answer(
        f"Введите новый интервал (в секундах) для правила #{rule_id}.",
        reply_markup=build_rule_input_inline_keyboard(rule_id),
    )

    user_states[callback.from_user.id] = {
        "action": "change_interval",
        "rule_id": rule_id,
        "card_chat_id": callback.message.chat.id,
        "card_message_id": callback.message.message_id,
        "prompt_chat_id": prompt.chat.id,
        "prompt_message_id": prompt.message_id,
    }

@dp.callback_query(lambda c: c.data.startswith("change_next_run:"))
async def handle_change_next_run_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    prompt = await callback.message.answer(
        f"Введите новое время следующего поста для правила #{rule_id}.\n\n"
        f"Формат: HH:MM\n"
        f"Время указывается по UTC+3.",
        reply_markup=build_rule_input_inline_keyboard(rule_id),
    )

    user_states[callback.from_user.id] = {
        "action": "change_next_run",
        "rule_id": rule_id,
        "card_chat_id": callback.message.chat.id,
        "card_message_id": callback.message.message_id,
        "prompt_chat_id": prompt.chat.id,
        "prompt_message_id": prompt.message_id,
    }

    await answer_callback_safe(callback)

@dp.callback_query(lambda c: c.data.startswith("change_fixed_times:"))
async def handle_change_fixed_times_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    rule_id = int(callback.data.split(":")[1])

    prompt = await callback.message.answer(
        f"Введите фиксированные времена для правила #{rule_id}.\n\n"
        f"Формат: 11:20, 23:20\n"
        f"Можно указать одно или несколько времён через запятую.\n"
        f"Время указывается по UTC+3.",
        reply_markup=build_rule_input_inline_keyboard(rule_id),
    )

    user_states[callback.from_user.id] = {
        "action": "change_fixed_times",
        "rule_id": rule_id,
        "card_chat_id": callback.message.chat.id,
        "card_message_id": callback.message.message_id,
        "prompt_chat_id": prompt.chat.id,
        "prompt_message_id": prompt.message_id,
    }

@dp.callback_query(lambda c: c.data.startswith("set_interval_mode:"))
async def handle_set_interval_mode_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    rule_id = int(callback.data.split(":")[1])

    prompt = await callback.message.answer(
        f"Введите новый интервал в секундах для правила #{rule_id}.\n\n"
        f"Например: 3600",
        reply_markup=build_rule_input_inline_keyboard(rule_id),
    )

    user_states[callback.from_user.id] = {
        "action": "set_interval_mode",
        "rule_id": rule_id,
        "card_chat_id": callback.message.chat.id,
        "card_message_id": callback.message.message_id,
        "prompt_chat_id": prompt.chat.id,
        "prompt_message_id": prompt.message_id,
    }

@dp.callback_query(lambda c: c.data.startswith("trigger_now:"))
async def handle_trigger_now_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    result = await run_db(
        _trigger_rule_now_sync,
        rule_id,
        callback.from_user.id if callback.from_user else settings.admin_id,
    )
    ok = result["ok"]

    if ok:
        await answer_callback_safe_once(callback, "⚡ Поставлено на немедленную отправку")

        await ensure_rule_workers()
        invalidate_rule_card_cache(rule_id)
        await refresh_rule_card_message(
            callback,
            rule_id,
            prefix_text="⚡ Следующий пост поставлен на немедленную отправку.",
        )
    else:
        await answer_callback_safe_once(
            callback,
            "❌ Не удалось запустить правило сейчас",
            show_alert=True,
        )
        await edit_message_text_safe(
            message=callback.message,
            text="❌ Не удалось запустить правило сейчас. Проверь, что правило активно.",
        )

@dp.callback_query(lambda c: c.data.startswith("disable_rule:"))
async def handle_disable_rule_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    rule_id = int(callback.data.split(":")[1])

    result = await run_db(
        _disable_rule_sync,
        rule_id,
        callback.from_user.id if callback.from_user else settings.admin_id,
    )
    ok = result["ok"]

    if ok:
        await ensure_rule_workers()
        invalidate_rule_card_cache(rule_id)
        await refresh_rule_card_message(
            callback,
            rule_id,
            prefix_text=(
                "✅ Правило выключено.\n"
                "Текущая обработка, если уже началась, будет завершена до конца.\n"
                "Новые посты браться не будут до повторного включения."
            ),
        )
    else:
        await edit_message_text_safe(
            message=callback.message,
            text="❌ Не удалось отключить правило",
        )

@dp.callback_query(lambda c: c.data.startswith("toggle_rule_mode:"))
async def handle_toggle_rule_mode(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    result = await run_db(
        _toggle_rule_mode_sync,
        rule_id,
        callback.from_user.id if callback.from_user else settings.admin_id,
    )

    ok = result.get("ok", False)
    reason = result.get("reason")
    new_mode = result.get("new_mode")

    if reason == "rule_not_found":
        await answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return

    if not ok:
        await answer_callback_safe(callback, "Ошибка смены режима", show_alert=True)
        return

    await answer_callback_safe_once(
        callback,
        "🎬 Видеоредактор включён" if new_mode == "video" else "🔁 Репост включён",
    )

    invalidate_rule_card_cache(rule_id)
    await refresh_rule_card_message(
        callback,
        rule_id,
        prefix_text=(
            "🎬 Видеоредактор включён"
            if new_mode == "video"
            else "🔁 Репост включён"
        ),
    )

@dp.callback_query(lambda c: c.data.startswith("enable_rule:"))
async def handle_enable_rule_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    result = await run_db(
        _enable_rule_sync,
        rule_id,
        callback.from_user.id if callback.from_user else settings.admin_id,
    )
    ok = result["ok"]

    if ok:
        await answer_callback_safe_once(callback, "✅ Правило включено")

        await ensure_rule_workers()

        invalidate_rule_card_cache(rule_id)
        await refresh_rule_card_message(
            callback,
            rule_id,
            prefix_text="✅ Правило включено.",
        )
    else:
        await answer_callback_safe_once(callback, "❌ Не удалось включить правило", show_alert=True)
        await edit_message_text_safe(
            message=callback.message,
            text="❌ Не удалось включить правило",
        )

@dp.callback_query(lambda c: c.data.startswith("delete_rule:"))
async def handle_delete_rule_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        rule_id = int(callback.data.split(":")[1])
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    ok = await run_db(
        db.delete_rule_with_audit,
        rule_id=rule_id,
        admin_id=callback.from_user.id if callback.from_user else settings.admin_id,
    )

    if ok:
        await answer_callback_safe_once(callback, "✅ Правило удалено")
        await ensure_rule_workers()
        await edit_message_text_safe(
            message=callback.message,
            text="✅ Правило удалено",
        )
    else:
        await answer_callback_safe_once(callback, "❌ Не удалось удалить правило", show_alert=True)
        await edit_message_text_safe(
            message=callback.message,
            text="❌ Не удалось удалить правило",
        )

@dp.message(
    lambda m: (
        m.chat.type == "private"
        and m.from_user is not None
        and user_states.get(m.from_user.id) is not None
        and m.text is not None
        and not is_menu_navigation_text(m.text)
        and (m.text or "").strip() not in {"📺 Канал", "👥 Группа с темой", "📤 Источник", "📥 Получатель"}
        and not (m.text or "").startswith("Удалить ")
        and not (m.text or "").startswith("📤 ")
        and not (m.text or "").startswith("📥 ")
        and not (m.text or "").strip().startswith("-100")
    )
)
async def handle_stateful_private_inputs(message: Message):
    if not await is_admin(message):
        return

    user_id = message.from_user.id if message.from_user else None
    state = user_states.get(user_id) if user_id is not None else None
    if not state:
        return

    action = state.get("action")
    text = (message.text or "").strip()

    # =========================================================
    # 1. Подпись видео
    # =========================================================
    if action == "video_caption":
        rule_id = state["rule_id"]

        caption = None if text == "-" else text
        entities_json = serialize_message_entities(message.entities) if caption else None

        result = await run_db(
            _save_video_caption_sync,
            rule_id,
            caption,
            entities_json,
            message.from_user.id if message.from_user else settings.admin_id,
        )
        ok = result["ok"]

        if ok:
            invalidate_rule_card_cache(rule_id)
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id = state.get("prompt_chat_id")
            prompt_message_id = state.get("prompt_message_id")
            if prompt_chat_id and prompt_message_id:
                await try_delete_message_safe(prompt_chat_id, prompt_message_id)

            card_chat_id = state.get("card_chat_id")
            card_message_id = state.get("card_message_id")
            if card_chat_id and card_message_id:
                await try_delete_message_safe(card_chat_id, card_message_id)

            await message.answer(
                build_video_caption_menu_text(rule_id),
                parse_mode="HTML",
                reply_markup=build_video_caption_menu_keyboard(rule_id),
            )
        else:
            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=(
                        "📝 <b>Изменение подписи видео</b>\n\n"
                        f"Правило #{rule_id}\n\n"
                        "Не удалось сохранить подпись.\n"
                        "Отправьте новый текст подписи одним сообщением."
                    ),
                    rule_id=rule_id,
                )
            else:
                await message.answer(
                    "❌ Не удалось сохранить подпись",
                    reply_markup=get_cancel_keyboard(),
                )

        reset_user_state(user_id)
        return

    # =========================================================
    # 2. Перенос следующего поста
    # =========================================================
    if action == "change_next_run":
        rule_id = state["rule_id"]

        next_run_iso = parse_next_run_user_time(text)
        if not next_run_iso:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"Введите новое время следующего поста для правила #{rule_id}.\n\n"
                f"Формат: HH:MM\n"
                f"Время указывается по UTC+3.\n\n"
                f"❌ Введите время в формате HH:MM\n"
                f"Пример: 18:30"
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            return

        result = await run_db(
            _change_next_run_sync,
            rule_id,
            next_run_iso,
            text,
            message.from_user.id if message.from_user else settings.admin_id,
        )
        ok = result["ok"]

        if not ok:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"Введите новое время следующего поста для правила #{rule_id}.\n\n"
                f"Формат: HH:MM\n"
                f"Время указывается по UTC+3.\n\n"
                f"❌ Не удалось обновить время. Попробуйте ещё раз."
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            else:
                await message.answer("❌ Не удалось обновить время", reply_markup=get_cancel_keyboard())
            return

        await ensure_rule_workers()

        invalidate_rule_card_cache(rule_id)
        await _finalize_rule_state_input(
            message,
            state,
            rule_id,
            prefix_text=f"✅ Время следующего поста изменено на {text} (UTC+3).",
            success_fallback_text="✅ Время следующего поста обновлено",
        )

        reset_user_state(user_id)
        return

    # =========================================================
    # 3. Фиксированные времена
    # =========================================================
    if action == "change_fixed_times":
        rule_id = state["rule_id"]

        raw_times = [part.strip() for part in text.split(",")]
        normalized_times = normalize_fixed_times(raw_times)

        if not normalized_times:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"Введите фиксированные времена для правила #{rule_id}.\n\n"
                f"Формат: 11:20, 23:20\n"
                f"Можно указать одно или несколько времён через запятую.\n"
                f"Время указывается по UTC+3.\n\n"
                f"❌ Введите хотя бы одно корректное время в формате HH:MM\n"
                f"Пример: 09:00, 14:30, 21:45"
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            return

        result = await run_db(
            _change_fixed_times_sync,
            rule_id,
            normalized_times,
            message.from_user.id if message.from_user else settings.admin_id,
        )
        ok = result["ok"]

        if not ok:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"Введите фиксированные времена для правила #{rule_id}.\n\n"
                f"Формат: 11:20, 23:20\n"
                f"Можно указать одно или несколько времён через запятую.\n"
                f"Время указывается по UTC+3.\n\n"
                f"❌ Не удалось сохранить фиксированные времена. Попробуйте ещё раз."
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            else:
                await message.answer(
                    "❌ Не удалось сохранить фиксированные времена",
                    reply_markup=get_cancel_keyboard(),
                )
            return

        await ensure_rule_workers()

        invalidate_rule_card_cache(rule_id)
        await _finalize_rule_state_input(
            message,
            state,
            rule_id,
            prefix_text="✅ Фиксированные времена обновлены.",
            success_fallback_text="✅ Фиксированные времена сохранены",
        )

        reset_user_state(user_id)
        return

    # =========================================================
    # 4 и 5  Перевод в плавающий режим и смена интервала
    # =========================================================
    if action in {"set_interval_mode", "change_interval"}:
        rule_id = state["rule_id"]

        try:
            interval = int(text)
        except Exception:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"Введите интервал в секундах для правила #{rule_id}.\n\n"
                f"Пример: 1800\n\n"
                f"❌ Нужно ввести число"
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            return

        if interval <= 0:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"Введите интервал в секундах для правила #{rule_id}.\n\n"
                f"❌ Интервал должен быть больше 0"
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            return

        result = await run_db(
            _change_interval_sync,
            rule_id,
            action,
            interval,
            message.from_user.id if message.from_user else settings.admin_id,
        )
        ok = result["ok"]
        success_text = result["success_text"]

        if not ok:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"Введите интервал в секундах для правила #{rule_id}.\n\n"
                f"❌ Не удалось сохранить интервал"
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            else:
                await message.answer(
                    "❌ Не удалось сохранить интервал",
                    reply_markup=get_cancel_keyboard(),
                )
            return

        await ensure_rule_workers()
        invalidate_rule_card_cache(rule_id)
        await _finalize_rule_state_input(
            message,
            state,
            rule_id,
            prefix_text=success_text,
            success_fallback_text=success_text,
        )

        reset_user_state(user_id)
        return


    # =========================================================
    # 6. Выбор точки старта по номеру
    # =========================================================
    if action == "start_from_number_wait_value":
        rule_id = state["rule_id"]
        rule = await run_db(db.get_rule, rule_id)
        rule_mode = (getattr(rule, "mode", "repost") or "repost") if rule else "repost"

        try:
            position = int(text)
        except Exception:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"↪ <b>Выбор точки старта</b>\n\n"
                f"Правило #{rule_id}\n\n"
                f"Введите примерный номер позиции, с которой хотите начать.\n\n"
                f"Например: <b>50</b>\n\n"
                f"❌ Нужно ввести целое число."
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            return

        if position <= 0:
            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            prompt_text = (
                f"↪ <b>Выбор точки старта</b>\n\n"
                f"Правило #{rule_id}\n\n"
                f"Введите примерный номер позиции, с которой хотите начать.\n\n"
                f"Например: <b>50</b>\n\n"
                f"❌ Номер должен быть больше нуля."
            )

            if prompt_chat_id and prompt_message_id:
                await refresh_input_prompt_by_ids(
                    chat_id=prompt_chat_id,
                    message_id=prompt_message_id,
                    text=prompt_text,
                    rule_id=rule_id,
                )
            return

        if message.from_user.id in preview_busy_users:
            await try_delete_message_safe(message.chat.id, message.message_id)
            await message.answer("⏳ Подожди, ещё обрабатываю предыдущий предпросмотр.")
            return

        preview_busy_users.add(message.from_user.id)
        try:
            item = await get_preview_item_by_position_async(rule_id, position)
            if not item:
                await try_delete_message_safe(message.chat.id, message.message_id)

                prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
                prompt_text = (
                    f"↪ <b>Выбор точки старта</b>\n\n"
                    f"Правило #{rule_id}\n\n"
                    f"Очередь пуста или позиция не найдена.\n"
                    f"Введите другой номер."
                )

                if prompt_chat_id and prompt_message_id:
                    await refresh_input_prompt_by_ids(
                        chat_id=prompt_chat_id,
                        message_id=prompt_message_id,
                        text=prompt_text,
                        rule_id=rule_id,
                    )
                return

            await try_delete_message_safe(message.chat.id, message.message_id)

            prompt_chat_id, prompt_message_id = _state_prompt_ids(state)
            if prompt_chat_id and prompt_message_id:
                await try_delete_message_safe(prompt_chat_id, prompt_message_id)

            progress_msg = await message.answer("⏳ Готовлю предпросмотр поста.")

            preview_method, preview_message_ids = await send_preview_post(
                bot,
                message.chat.id,
                item,
                rule_mode=rule_mode,
            )

            try:
                await progress_msg.delete()
            except Exception:
                pass

            control_msg = await message.answer(
                build_start_position_text(
                    item,
                    rule_mode=rule_mode,
                    preview_method=preview_method,
                ),
                parse_mode="HTML",
                reply_markup=build_start_position_keyboard(rule_id, item["position"]),
            )

            user_states[message.from_user.id] = {
                "action": "start_from_number_preview",
                "rule_id": rule_id,
                "position": item["position"],
                "preview_message_ids": preview_message_ids,
                "control_message_id": control_msg.message_id,
            }
        finally:
            preview_busy_users.discard(message.from_user.id)
        return

    # =========================================================
    # 7. Добавление thread_id для группы
    # =========================================================
    if action in {"add_source_group_thread", "add_target_group_thread"}:
        try:
            thread_id = int(text)
        except Exception:
            await message.answer(
                "❌ ID темы должен быть числом",
                reply_markup=get_cancel_keyboard(),
            )
            return

        chat_id = state["chat_id"]
        title = state["title"]
        channel_type = "source" if action == "add_source_group_thread" else "target"

        try:
            exists = await run_db(db.channel_exists, chat_id, thread_id, channel_type)

            if exists:
                await message.answer("Такая тема уже добавлена", reply_markup=get_main_menu())
            else:
                actor_id = message.from_user.id if message.from_user else settings.admin_id
                if is_admin_user(actor_id):
                    await run_db(db.add_channel, chat_id, thread_id, channel_type, title, actor_id)
                else:
                    tenant_id = await run_db(ensure_user_tenant, actor_id)
                    await run_db(db.add_channel_for_tenant, tenant_id, chat_id, thread_id, channel_type, title, actor_id)
                await message.answer(
                    f"✅ Добавлена запись: {title} / тема {thread_id}",
                    reply_markup=get_main_menu(),
                )
                if channel_type == "source":
                    asyncio.create_task(
                        parse_group_history(telethon_client, db, chat_id, thread_id, clean_start=False)
                    )
        finally:
            reset_user_state(user_id)
        return

    # =========================================================
    # 8. Создание правила: ввод интервала
    # =========================================================
    if action == "set_rule_interval":
        try:
            interval = int(text)
        except Exception:
            await message.answer(
                "❌ Введите интервал в секундах, например 3600",
                reply_markup=get_cancel_keyboard(),
            )
            return

        if interval < 1:
            await message.answer(
                "❌ Интервал должен быть не меньше 1 секунды",
                reply_markup=get_cancel_keyboard(),
            )
            return

        choice = state["choice"]

        rule_id = await run_db(
            _create_rule_sync,
            choice,
            interval,
            message.from_user.id if message.from_user else settings.admin_id,
        )

        if rule_id:
            await ensure_rule_workers()
            if not is_admin_user(message.from_user.id if message.from_user else None):
                tenant_id = await run_db(ensure_user_tenant, message.from_user.id if message.from_user else 0)
                logger.info("пользователь создал правило rule_id=%s tenant_id=%s", rule_id, tenant_id)
            await message.answer(
                f"✅ Правило создано #{rule_id}. Первый пост выйдет сразу, дальше — каждые {interval_to_text(interval)}",
                reply_markup=get_main_menu(),
            )
        else:
            tenant = await run_db(tenant_service.ensure_tenant_exists, message.from_user.id if message.from_user else settings.admin_id)
            tenant_id = int(tenant.get("id") or 1)
            can_create, _reason = await run_db(limit_service.can_create_rule, tenant_id)
            lang = _resolve_language(message.from_user.id if message.from_user else None)
            if not can_create:
                sub = await run_db(subscription_service.get_active_subscription, tenant_id) or {}
                created_rules = await run_db(db.count_rules_for_tenant, tenant_id) if hasattr(db, "count_rules_for_tenant") else 0
                await message.answer(
                    (
                        "🚫 Лимит тарифа достигнут\n\n"
                        f"На вашем тарифе доступно: {int(sub.get('max_rules') or 0)} правил.\n"
                        f"Сейчас создано: {int(created_rules or 0)}.\n\n"
                        "Чтобы добавить больше правил, смените тариф."
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="💎 Сменить тариф", callback_data="user_plans")],
                            [InlineKeyboardButton(text="⬅️ Назад", callback_data="user_main")],
                        ]
                    ),
                )
                reset_user_state(user_id)
                return
            await message.answer(
                "⚠️ Не удалось создать правило: возможно, оно уже существует или достигнут лимит тарифа.",
                reply_markup=get_main_menu(),
            )

        reset_user_state(user_id)
        return

    if action == "user_set_rule_interval":
        try:
            interval = int(text)
        except Exception:
            await message.answer("❌ Введите число секунд", reply_markup=get_cancel_keyboard())
            return
        if interval < 1:
            await message.answer("❌ Интервал должен быть не меньше 1 секунды", reply_markup=get_cancel_keyboard())
            return
        rule_id = int(state.get("rule_id") or 0)
        if not await run_db(is_rule_owned_by_user, rule_id, message.from_user.id if message.from_user else 0):
            await message.answer("⛔ Нет доступа к этому объекту", reply_markup=get_main_menu())
            reset_user_state(user_id)
            return
        updated = await run_db(_change_interval_sync, rule_id, "change_interval", interval, message.from_user.id if message.from_user else settings.admin_id)
        if updated.get("ok"):
            await message.answer("✅ Интервал обновлён", reply_markup=get_main_menu())
        else:
            await message.answer("❌ Не удалось обновить интервал", reply_markup=get_main_menu())
        reset_user_state(user_id)
        return

@dp.message(lambda m: m.text in ("➕ Канал", "➕ Добавить канал", "➕ Добавить источник", "➕ Добавить получатель"))
async def handle_add_channel(message: Message):
    if message.text == "➕ Добавить источник":
        user_states[message.from_user.id] = {"action": "choose_source_kind"}
        await message.reply("Выберите: канал или группа с темой", reply_markup=get_entity_kind_keyboard())
        return
    if message.text == "➕ Добавить получатель":
        user_states[message.from_user.id] = {"action": "choose_target_kind"}
        await message.reply("Выберите: канал или группа с темой", reply_markup=get_entity_kind_keyboard())
        return
    if not await is_admin(message):
        return
    await message.reply("Выберите тип записи", reply_markup=get_channel_type_keyboard())


@dp.message(lambda m: m.text == "📤 Источник")
async def handle_source_type(message: Message):
    if not is_admin_user(message.from_user.id if message.from_user else None):
        return
    user_states[message.from_user.id] = {"action": "choose_source_kind"}
    await message.reply("Выберите: канал или группа с темой", reply_markup=get_entity_kind_keyboard())


@dp.message(lambda m: m.text == "📥 Получатель")
async def handle_target_type(message: Message):
    if not is_admin_user(message.from_user.id if message.from_user else None):
        return
    user_states[message.from_user.id] = {"action": "choose_target_kind"}
    await message.reply("Выберите: канал или группа с темой", reply_markup=get_entity_kind_keyboard())


@dp.message(lambda m: m.text in ("📺 Канал", "👥 Группа с темой"))
async def handle_entity_kind(message: Message):
    state = user_states.get(message.from_user.id)
    if not state:
        return
    if state["action"] == "choose_source_kind":
        state["action"] = "add_source_channel" if message.text == "📺 Канал" else "add_source_group"
        await message.reply(
            "Отправьте ID канала" if message.text == "📺 Канал" else "Отправьте ID группы",
            reply_markup=get_cancel_keyboard(),
        )
    elif state["action"] == "choose_target_kind":
        state["action"] = "add_target_channel" if message.text == "📺 Канал" else "add_target_group"
        await message.reply(
            "Отправьте ID канала" if message.text == "📺 Канал" else "Отправьте ID группы",
            reply_markup=get_cancel_keyboard(),
        )


async def resolve_chat_title(chat_id: str) -> str:
    chat = await bot.get_chat(chat_id)
    return chat.title or str(chat_id)


@dp.message(lambda m: m.text and m.text.startswith("-100"))
async def handle_chat_id_inputs(message: Message):
    state = user_states.get(message.from_user.id)
    if not state:
        return

    chat_id = (message.text or "").strip()
    action = state.get("action")

    # =========================================================
    # 1. Добавление обычного канала / получателя
    # =========================================================
    if action in {"add_source_channel", "add_target_channel"}:
        channel_type = "source" if action == "add_source_channel" else "target"

        try:
            title = await resolve_chat_title(chat_id)

            exists = await run_db(db.channel_exists, chat_id, None, channel_type)

            if exists:
                await message.reply(
                    "Такая запись уже есть",
                    reply_markup=get_main_menu(),
                )
            else:
                actor_id = message.from_user.id if message.from_user else settings.admin_id
                if is_admin_user(actor_id):
                    created = await run_db(db.add_channel, chat_id, None, channel_type, title, actor_id)
                else:
                    tenant_id = await run_db(ensure_user_tenant, actor_id)
                    created = await run_db(db.add_channel_for_tenant, tenant_id, chat_id, None, channel_type, title, actor_id)

                if not created:
                    await message.reply(
                        "⚠️ Не удалось добавить запись в базу",
                        reply_markup=get_main_menu(),
                    )
                else:
                    await message.reply(
                        f"✅ Добавлен {'источник' if channel_type == 'source' else 'получатель'}: {title}",
                        reply_markup=get_main_menu(),
                    )

                    if channel_type == "source":
                        asyncio.create_task(
                            parse_channel_history(
                                telethon_client,
                                db,
                                chat_id,
                                clean_start=False,
                            )
                        )

        except Exception as exc:
            logger.exception(
                "Ошибка добавления канала | action=%s | chat_id=%s | error=%s",
                action,
                chat_id,
                exc,
            )
            await message.reply(
                f"❌ Ошибка доступа к каналу/чату: {exc}",
                reply_markup=get_main_menu(),
            )
        finally:
            reset_user_state(message.from_user.id)
        return

    # =========================================================
    # 2. Добавление группы с темой / получателя с темой
    # =========================================================
    if action in {"add_source_group", "add_target_group"}:
        try:
            title = await resolve_chat_title(chat_id)

            state["chat_id"] = chat_id
            state["title"] = title
            state["action"] = (
                "add_source_group_thread"
                if action == "add_source_group"
                else "add_target_group_thread"
            )

            await message.reply(
                "Теперь отправьте ID темы",
                reply_markup=get_cancel_keyboard(),
            )

        except Exception as exc:
            logger.exception(
                "Ошибка доступа к группе перед вводом thread_id | action=%s | chat_id=%s | error=%s",
                action,
                chat_id,
                exc,
            )
            await message.reply(
                f"❌ Не удалось получить доступ к группе: {exc}",
                reply_markup=get_main_menu(),
            )
            reset_user_state(message.from_user.id)

        return

@dp.message(lambda m: m.photo or m.video or m.document)
async def handle_intro_file(message: Message):
    state = user_states.get(message.from_user.id)
    if not state or state.get("action") != "intro_upload_wait_file":
        return

    if not await is_admin(message):
        return

    caption = (message.caption or "").strip()
    if not caption:
        await message.answer(
            "❌ Укажи название заставки в подписи к файлу.\n\n"
            "Пример:\n"
            "grom_vert",
            reply_markup=get_cancel_keyboard(),
        )
        return

    safe_name = sanitize_intro_name(caption)
    if not safe_name:
        await message.answer(
            "❌ Некорректное название заставки.\n"
            "Разрешены буквы, цифры, пробел, _, -",
            reply_markup=get_cancel_keyboard(),
        )
        return

    if message.photo:
        tg_file = message.photo[-1]
        extension = "jpg"
        duration = 0
    else:
        tg_file = message.video or message.document
        mime_type = (getattr(tg_file, "mime_type", None) or "").lower()

        if message.video:
            extension = "mp4"
        elif mime_type.startswith("image/"):
            extension = "jpg"
        elif mime_type.startswith("video/"):
            extension = "mp4"
        else:
            await message.answer(
                "❌ Поддерживаются только видео или изображения для заставки",
                reply_markup=get_cancel_keyboard(),
            )
            return

        duration = int(getattr(tg_file, "duration", 0) or 0)

    if duration > 30:
        await message.answer(
            "❌ Видео-заставка не должна быть длиннее 30 секунд",
            reply_markup=get_cancel_keyboard(),
        )
        return

    file_name = make_unique_intro_filename(
        safe_name,
        extension,
        str(settings.intros_dir),
    )
    file_path = str(settings.intros_dir / file_name)

    try:
        telegram_file = await bot.get_file(tg_file.file_id)
        await bot.download_file(telegram_file.file_path, destination=file_path)
    except Exception as exc:
        await message.answer(
            f"❌ Не удалось скачать заставку: {exc}",
            reply_markup=get_cancel_keyboard(),
        )
        return

    intro_id = await run_db(
        db.add_intro,
        display_name=caption,
        file_name=file_name,
        file_path=file_path,
        duration=duration,
    )

    if not intro_id:
        try:
            import os
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass

        await message.answer(
            "❌ Такая заставка уже существует",
            reply_markup=get_cancel_keyboard(),
        )
        return

    intros = await run_db(db.get_intros)

    await message.answer(
        f"✅ Заставка '{caption}' добавлена",
        reply_markup=build_intro_list_keyboard(intros),
    )

    user_states[message.from_user.id] = {
        "action": "intro_menu",
        "rule_id": state.get("rule_id"),
    }

@dp.message(
    lambda m: (
        m.text is not None
        and not is_menu_navigation_text(m.text)
        and not (m.text or "").startswith("Удалить ")
        and not (m.text or "").isdigit()
    )
)

@dp.message(lambda m: m.text in ("📜 Список", "📜 Список каналов", "📜 Мои источники", "📜 Мои получатели"))
async def handle_list_channels(message: Message):
    reset_user_state(message.from_user.id if message.from_user else None)
    user_id = message.from_user.id if message.from_user else settings.admin_id
    is_admin_mode = is_admin_user(user_id)
    if is_admin_mode:
        rows = await run_db(db.get_channels)
    else:
        tenant_id = await run_db(ensure_user_tenant, user_id)
        channel_type = None
        if message.text == "📜 Мои источники":
            channel_type = "source"
        if message.text == "📜 Мои получатели":
            channel_type = "target"
        rows = await run_db(db.get_channels_for_tenant, tenant_id, channel_type) if hasattr(db, "get_channels_for_tenant") else []
        logger.info("пользователь открыл список каналов user_id=%s tenant_id=%s type=%s", user_id, tenant_id, channel_type or "all")
    if not rows:
        await message.reply("Нет каналов", reply_markup=get_main_menu())
        return
    text = "📜 **СПИСОК КАНАЛОВ**\n\n"
    for idx, row in enumerate(rows, 1):
        title = row["title"] or row["channel_id"]
        suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
        text += f"{idx}. [{row['channel_type']}] {title}{suffix}\n"
    await message.reply(text[:4000], parse_mode="Markdown", reply_markup=get_main_menu())


@dp.message(lambda m: m.text in ("➖ Канал", "➖ Удалить канал", "➖ Удалить источник", "➖ Удалить получатель"))
async def handle_remove_channel(message: Message):
    user_id = message.from_user.id if message.from_user else settings.admin_id
    is_admin_mode = is_admin_user(user_id)
    if is_admin_mode:
        rows = await run_db(db.get_channels)
    else:
        tenant_id = await run_db(ensure_user_tenant, user_id)
        channel_type = "source" if message.text == "➖ Удалить источник" else ("target" if message.text == "➖ Удалить получатель" else None)
        rows = await run_db(db.get_channels_for_tenant, tenant_id, channel_type) if hasattr(db, "get_channels_for_tenant") else []
    if not rows:
        await message.reply("Нет каналов", reply_markup=get_main_menu())
        return

    keyboard = []
    mapping = []
    text = "Выберите запись для удаления\n\n"

    for idx, row in enumerate(rows, 1):
        title = row["title"] or row["channel_id"]
        suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
        keyboard.append([KeyboardButton(text=f"Удалить {idx}")])
        mapping.append((row["channel_id"], row["thread_id"], row["channel_type"]))
        text += f"{idx}. [{row['channel_type']}] {title}{suffix}\n"

    keyboard.append([KeyboardButton(text="❌ Отмена")])
    user_states[message.from_user.id] = {
        "action": "remove_channel",
        "mapping": mapping,
        "tenant_id": None if is_admin_mode else tenant_id,
    }

    await message.reply(
        text[:4000],
        reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True),
    )


@dp.message(lambda m: m.text and m.text.startswith("Удалить "))
async def handle_remove_selected(message: Message):
    user_id = message.from_user.id if message.from_user else settings.admin_id
    state = user_states.get(message.from_user.id)
    if not state or state.get("action") != "remove_channel":
        return

    try:
        idx = int(message.text.split()[-1]) - 1
        channel_id, thread_id, channel_type = state["mapping"][idx]
        tenant_id = state.get("tenant_id")
        if tenant_id is None:
            await run_db(db.remove_channel, channel_id, thread_id, channel_type)
        else:
            await run_db(db.remove_channel_for_tenant, tenant_id, channel_id, thread_id, channel_type)
        await ensure_rule_workers()
        await message.reply("✅ Канал удалён", reply_markup=get_main_menu())
    except Exception as exc:
        await message.reply(f"❌ Ошибка удаления: {exc}", reply_markup=get_main_menu())
    finally:
        user_states.pop(message.from_user.id, None)


@dp.message(lambda m: m.text == "➕ Добавить правило")
async def handle_add_rule(message: Message):
    user_id = message.from_user.id if message.from_user else settings.admin_id
    if is_admin_user(user_id):
        source_rows = await run_db(db.get_channels, "source")
    else:
        tenant_id = await run_db(ensure_user_tenant, user_id)
        source_rows = await run_db(db.get_channels_for_tenant, tenant_id, "source") if hasattr(db, "get_channels_for_tenant") else []
    sources = [ChannelChoice(r["channel_id"], r["thread_id"], r["title"] or r["channel_id"]) for r in source_rows]
    if not sources:
        await message.reply("Нет источников", reply_markup=get_main_menu())
        return
    keyboard = [[KeyboardButton(text=f"📤 {i}. {s.title}{f' (тема {s.thread_id})' if s.thread_id else ''}")] for i, s in enumerate(sources, 1)]
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    user_states[message.from_user.id] = {"action": "pick_rule_source", "sources": sources}
    await message.reply("Выберите источник", reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True))


@dp.message(lambda m: m.text and m.text.startswith("📤 "))
async def handle_pick_rule_source(message: Message):
    state = user_states.get(message.from_user.id)
    if not state or state.get("action") != "pick_rule_source":
        return
    idx = int(message.text.split(".")[0].split()[1]) - 1
    choice = state["sources"][idx]
    user_id = message.from_user.id if message.from_user else settings.admin_id
    if is_admin_user(user_id):
        target_rows = await run_db(db.get_channels, "target")
    else:
        tenant_id = await run_db(ensure_user_tenant, user_id)
        target_rows = await run_db(db.get_channels_for_tenant, tenant_id, "target") if hasattr(db, "get_channels_for_tenant") else []
    targets = [ChannelChoice(r["channel_id"], r["thread_id"], r["title"] or r["channel_id"]) for r in target_rows]
    if not targets:
        await message.reply("Нет получателей", reply_markup=get_main_menu())
        reset_user_state(message.from_user.id)
        return
    keyboard = [[KeyboardButton(text=f"📥 {j}. {t.title}{f' (тема {t.thread_id})' if t.thread_id else ''}")] for j, t in enumerate(targets, 1)]
    keyboard.append([KeyboardButton(text="❌ Отмена")])
    state["action"] = "pick_rule_target"
    state["choice"] = {"source_id": choice.channel_id, "source_thread_id": choice.thread_id}
    state["targets"] = targets
    await message.reply("Выберите получателя", reply_markup=ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True))

@dp.message(lambda m: m.text and m.text.startswith("📥 "))
async def handle_pick_rule_target(message: Message):
    state = user_states.get(message.from_user.id)
    if not state or state.get("action") != "pick_rule_target":
        return
    idx = int(message.text.split(".")[0].split()[1]) - 1
    choice = state["targets"][idx]
    state["choice"]["target_id"] = choice.channel_id
    state["choice"]["target_thread_id"] = choice.thread_id
    state["action"] = "set_rule_interval"
    await message.reply("Отправьте интервал в секундах, например 3600", reply_markup=get_cancel_keyboard())

@dp.callback_query(lambda c: c.data == "faulty_page_info")
async def handle_faulty_page_info(callback: CallbackQuery):
    await answer_callback_safe_once(callback)

@dp.callback_query(lambda c: c.data.startswith("startpos_prev:"))
async def handle_startpos_prev(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, rule_id_raw, position_raw = parse_callback_parts(callback.data, "startpos_prev", 3)
        rule_id = int(rule_id_raw)
        position = int(position_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    rule = await run_db(db.get_rule, rule_id)
    rule_mode = (getattr(rule, "mode", "repost") or "repost") if rule else "repost"

    user_id = callback.from_user.id
    if user_id in preview_busy_users:
        await answer_callback_safe(callback, "⏳ Подожди, ещё обрабатываю предыдущий предпросмотр.")
        return

    preview_busy_users.add(user_id)
    try:
        item = await get_preview_item_shifted_async(rule_id, position, -1)
        if not item:
            await answer_callback_safe(callback, "Позиция не найдена", show_alert=True)
            return

        if item["position"] == position:
            await answer_callback_safe(callback, "Это уже первый пост")
            return

        old_state = user_states.get(callback.from_user.id, {})
        await answer_callback_safe_once(callback, "⏳ Готовлю предпросмотр.")
        await cleanup_preview_messages(
            callback.bot,
            callback.message.chat.id,
            old_state.get("preview_message_ids"),
        )

        preview_method, preview_message_ids = await send_preview_post(
            callback.bot,
            callback.message.chat.id,
            item,
            rule_mode=rule_mode,
        )

        try:
            await callback.message.delete()
        except Exception:
            pass

        control_msg = await callback.message.answer(
            build_start_position_text(
                item,
                rule_mode=rule_mode,
                preview_method=preview_method,
            ),
            parse_mode="HTML",
            reply_markup=build_start_position_keyboard(rule_id, item["position"]),
        )

        user_states[callback.from_user.id] = {
            "action": "start_from_number_preview",
            "rule_id": rule_id,
            "position": item["position"],
            "preview_message_ids": preview_message_ids,
            "control_message_id": control_msg.message_id,
        }
    finally:
        preview_busy_users.discard(user_id)

@dp.callback_query(lambda c: c.data.startswith("startpos_next:"))
async def handle_startpos_next(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, rule_id_raw, position_raw = parse_callback_parts(callback.data, "startpos_next", 3)
        rule_id = int(rule_id_raw)
        position = int(position_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    rule = await run_db(db.get_rule, rule_id)
    rule_mode = (getattr(rule, "mode", "repost") or "repost") if rule else "repost"

    user_id = callback.from_user.id
    if user_id in preview_busy_users:
        await answer_callback_safe(callback, "⏳ Подожди, ещё обрабатываю предыдущий предпросмотр.")
        return

    preview_busy_users.add(user_id)
    try:
        item = await get_preview_item_shifted_async(rule_id, position, 1)
        if not item:
            await answer_callback_safe(callback, "Позиция не найдена", show_alert=True)
            return

        if item["position"] == position:
            await answer_callback_safe(callback, "Это уже последний пост")
            return

        old_state = user_states.get(callback.from_user.id, {})
        await answer_callback_safe_once(callback, "⏳ Готовлю предпросмотр.")
        await cleanup_preview_messages(
            callback.bot,
            callback.message.chat.id,
            old_state.get("preview_message_ids"),
        )

        preview_method, preview_message_ids = await send_preview_post(
            callback.bot,
            callback.message.chat.id,
            item,
            rule_mode=rule_mode,
        )

        try:
            await callback.message.delete()
        except Exception:
            pass

        control_msg = await callback.message.answer(
            build_start_position_text(
                item,
                rule_mode=rule_mode,
                preview_method=preview_method,
            ),
            parse_mode="HTML",
            reply_markup=build_start_position_keyboard(rule_id, item["position"]),
        )

        user_states[callback.from_user.id] = {
            "action": "start_from_number_preview",
            "rule_id": rule_id,
            "position": item["position"],
            "preview_message_ids": preview_message_ids,
            "control_message_id": control_msg.message_id,
        }
    finally:
        preview_busy_users.discard(user_id)

@dp.callback_query(lambda c: c.data.startswith("startpos_apply:"))
async def handle_startpos_apply(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, rule_id_raw, position_raw = parse_callback_parts(callback.data, "startpos_apply", 3)
        rule_id = int(rule_id_raw)
        position = int(position_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    selected = await run_db(db.set_rule_start_from_position, rule_id, position)
    if not selected:
        await answer_callback_safe(callback, "Не удалось изменить точку старта", show_alert=True)
        return

    invalidate_preview_cache(rule_id)

    old_state = user_states.get(callback.from_user.id, {})
    await cleanup_preview_messages(
        callback.bot,
        callback.message.chat.id,
        old_state.get("preview_message_ids"),
    )

    user_states.pop(callback.from_user.id, None)
    invalidate_rule_card_cache(rule_id)

    row = await get_rule_stats_row_async(rule_id)
    if not row:
        await answer_callback_safe_once(callback, f"Старт с {selected['position']}")
        await edit_message_text_safe(
            message=callback.message,
            text="Точка старта изменена",
        )
        return

    await edit_message_text_safe(
        message=callback.message,
        text=build_rule_card_text(row),
        parse_mode="HTML",
        reply_markup=build_rule_card_keyboard(
            rule_id,
            bool(row["is_active"]),
            row["schedule_mode"] or "interval",
            row["mode"] or "repost",
        ),
    )

    await answer_callback_safe_once(
        callback,
        f"Старт с {selected['position']}",
        show_alert=False,
    )

@dp.callback_query(lambda c: c.data.startswith("startpos_cancel:"))
async def handle_startpos_cancel(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, rule_id_raw = parse_callback_parts(callback.data, "startpos_cancel", 2)
        rule_id = int(rule_id_raw)
        invalidate_preview_cache(rule_id)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    old_state = user_states.get(callback.from_user.id, {})
    await cleanup_preview_messages(
        callback.bot,
        callback.message.chat.id,
        old_state.get("preview_message_ids"),
    )

    row = await get_rule_stats_row_async(rule_id)
    user_states.pop(callback.from_user.id, None)

    if not row:
        await answer_callback_safe_once(callback, "Отменено")
        await edit_message_text_safe(
            message=callback.message,
            text="Отменено",
        )
        return

    await edit_message_text_safe(
        message=callback.message,
        text=build_rule_card_text(row),
        parse_mode="HTML",
        reply_markup=build_rule_card_keyboard(
            rule_id,
            bool(row["is_active"]),
            row["schedule_mode"] or "interval",
            row["mode"] or "repost",
        ),
    )

    await answer_callback_safe_once(callback, "Отменено")

@dp.callback_query(lambda c: c.data.startswith("faulty_page:"))
async def handle_faulty_page(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, page_raw = parse_callback_parts(callback.data, "faulty_page", 2)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    pages = await run_db(build_faulty_pages, 200)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)

    current = pages[page]

    await edit_message_text_safe(
        message=callback.message,
        text=current["text"],
        parse_mode="HTML",
        reply_markup=build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]),
    )

@dp.callback_query(lambda c: c.data.startswith("faulty_refresh:"))
async def handle_faulty_refresh(callback: CallbackQuery):
    logger.warning("DEBUG: faulty_refresh called data=%r", callback.data)

    if not await is_admin_callback(callback):
        return

    try:
        _, page_raw = parse_callback_parts(callback.data, "faulty_refresh", 2)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    await answer_callback_safe_once(callback)

    pages = await run_db(build_faulty_pages, 200)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)

    current = pages[page]

    await edit_message_text_safe(
        message=callback.message,
        text=current["text"],
        parse_mode="HTML",
        reply_markup=build_faulty_inline_keyboard(
            page,
            total_pages,
            current["delivery_id"],
        ),
    )

@dp.callback_query(lambda c: c.data.startswith("faulty_ack:"))
async def handle_faulty_ack(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, delivery_id_raw, page_raw = parse_callback_parts(callback.data, "faulty_ack", 3)
        delivery_id = int(delivery_id_raw)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    row = await run_db(db.get_delivery, delivery_id)
    if not row:
        await answer_callback_safe(callback, "Этой проблемы уже нет", show_alert=True)
        return

    rule_id = int(row["rule_id"])

    await run_db(db.resolve_problem, f"rule_faulty_{rule_id}")
    await run_db(db.resolve_problem, f"target_dead_{rule_id}")
    await run_db(db.resolve_problem, f"rule_worker_error_{rule_id}")

    await answer_callback_safe_once(callback, "✅ Помечено как «взята в работу»")

    pages = await run_db(build_faulty_pages, 200)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)
    current = pages[page]

    await edit_message_text_safe(
        message=callback.message,
        text=current["text"],
        parse_mode="HTML",
        reply_markup=build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]),
    )

@dp.callback_query(lambda c: c.data.startswith("faulty_clear:"))
async def handle_faulty_clear(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    try:
        _, delivery_id_raw, page_raw = parse_callback_parts(callback.data, "faulty_clear", 3)
        delivery_id = int(delivery_id_raw)
        page = int(page_raw)
    except Exception:
        await answer_callback_safe(callback, "Ошибка данных", show_alert=True)
        return

    ok = await run_db(
        db.clear_faulty_delivery_log,
        delivery_id=delivery_id,
        admin_id=callback.from_user.id if callback.from_user else settings.admin_id,
    )

    if not ok:
        await answer_callback_safe(callback, "Нечего очищать", show_alert=True)
        return

    await answer_callback_safe_once(callback, "🧹 Лог очищен")

    pages = await run_db(build_faulty_pages, 200)
    total_pages = len(pages)
    page = clamp_page(page, total_pages)
    current = pages[page]

    await edit_message_text_safe(
        message=callback.message,
        text=current["text"],
        parse_mode="HTML",
        reply_markup=build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]),
    )

@dp.callback_query(lambda c: c.data == "faulty_back")
async def handle_faulty_back(callback: CallbackQuery):
    logger.warning("DEBUG: faulty_back called data=%r", callback.data)

    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    await edit_message_text_safe(
        message=callback.message,
        text="⚠️ Раздел: Диагностика",
    )

    await send_message_safe(
        chat_id=callback.message.chat.id,
        text="⚠️ Раздел: Диагностика",
        reply_markup=get_diagnostics_menu(),
    )

@dp.message(lambda m: m.text == "🔄 Сброс")
async def handle_reset_menu(message: Message):
    if not await is_admin(message):
        return
    await message.reply("Меню сброса", reply_markup=get_reset_queue_menu())

@dp.message(lambda m: m.text == "🔄 Сбросить всё")
async def handle_reset_all(message: Message):
    if not await is_admin(message):
        return

    count, faulty = await run_db(db.reset_all_deliveries)

    await message.reply(
        f"✅ Сброшено доставок: {count}\n⚠️ Faulty раньше было: {faulty}",
        reply_markup=get_main_menu(),
    )

@dp.message(lambda m: m.text == "📊 Сброс по источнику")
async def handle_reset_source_pick(message: Message):
    if not await is_admin(message):
        return

    source_rows = await run_db(db.get_channels, "source")
    sources = [
        ChannelChoice(r["channel_id"], r["thread_id"], r["title"] or r["channel_id"])
        for r in source_rows
    ]

    if not sources:
        await message.reply("Нет источников", reply_markup=get_main_menu())
        return

    user_states[message.from_user.id] = {"action": "reset_source_inline", "sources": sources}
    await message.reply("Выберите источник для сброса:", reply_markup=sources_inline_keyboard(sources))

@dp.callback_query(lambda c: c.data == "reset_back")
async def handle_reset_back(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    await answer_callback_safe_once(callback)

    await edit_message_text_safe(
        message=callback.message,
        text="Меню сброса:\n\n• 🔄 Сбросить всё\n• 📊 Сброс по источнику",
    )

@dp.callback_query(lambda c: c.data.startswith("reset_source:"))
async def handle_reset_source_callback(callback: CallbackQuery):
    if not await is_admin_callback(callback):
        return

    state = user_states.get(callback.from_user.id)
    if not state or state.get("action") != "reset_source_inline":
        await answer_callback_safe(callback, "Список устарел", show_alert=True)
        return

    try:
        idx = int(callback.data.split(":")[1])
        choice = state["sources"][idx]
        count = await run_db(db.reset_source_deliveries, choice.channel_id, choice.thread_id)

        await answer_callback_safe_once(callback)

        await edit_message_text_safe(
            message=callback.message,
            text=f"✅ Сброшено доставок: {count}",
        )
    except Exception as exc:
        await answer_callback_safe_once(callback)
        await edit_message_text_safe(
            message=callback.message,
            text=f"❌ Ошибка сброса: {exc}",
        )
    finally:
        user_states.pop(callback.from_user.id, None)

@dp.message()
async def handle_new_message(message: Message):
    if message.chat.type == "private":
        return

    try:
        source_rows = await run_db(db.get_channels, "source")

        for row in source_rows:
            if str(message.chat.id) != str(row["channel_id"]):
                continue
            if row["thread_id"] is not None and message.message_thread_id != row["thread_id"]:
                continue

            await run_db(
                db.save_post,
                message_id=message.message_id,
                source_channel=str(row["channel_id"]),
                source_thread_id=row["thread_id"],
                content={
                    "text": message.text or message.caption or "",
                    "has_media": bool(
                        message.photo or message.video or message.document
                        or message.animation or message.audio
                    ),
                    "media_kind": detect_message_media_kind_for_storage(message),
                    "date": message.date.isoformat() if message.date else None,
                },
                media_group_id=str(message.media_group_id) if message.media_group_id else None,
            )

            await ensure_rule_workers()
            break
    except Exception as exc:
        logger.exception("Ошибка обработки нового сообщения: %s", exc)

@dp.channel_post()
async def handle_channel_post(message: Message):
    await handle_new_message(message)

async def _init_db_runtime() -> None:
    await run_db(db.init)
    await run_db(ensure_owner_and_default_tenant_bootstrap, db, settings.admin_id)
    ok, msg = await run_db(db.integrity_check)
    if not ok:
        raise RuntimeError(f"PostgreSQL недоступен: {msg}")

    reset_count = await run_db(db.reset_stuck_processing)
    logger.warning(f"♻️ Сброшено зависших processing задач: {reset_count}")


async def _init_sender_runtime(*, create_ui_policy: bool) -> None:
    global bot, telethon_client, reaction_clients, sender_service, ui_policy, runtime_context

    bot = Bot(
        token=settings.bot_token,
        base_url=f"{settings.bot_api_base}/bot",
    )

    if create_ui_policy:
        ui_policy = UIErrorPolicy(bot)
    else:
        ui_policy = None

    try:
        await bot.delete_webhook(drop_pending_updates=True, request_timeout=90)
    except Exception as e:
        logger.warning("Webhook skip (network issue): %s", e)
    try:
        await bot.set_my_commands(
            DEFAULT_TELEGRAM_COMMANDS,
            scope=BotCommandScopeDefault(),
        )
    except Exception as exc:
        logger.warning("Не удалось установить команды бота: %s", exc)

    telethon_client = await create_telethon_client()
    reaction_clients = await create_reaction_clients()

    sender_service = SenderService(
        bot=bot,
        db=db,
        telethon_client=telethon_client,
        reaction_clients=reaction_clients,
    )
    runtime_context = RuntimeContext(
        repo=db,
        sender_service=sender_service,
        scheduler_service=scheduler_service,
        bot=bot,
        telethon_client=telethon_client,
        reaction_clients=reaction_clients,
    )


async def _shutdown_runtime(
    *,
    stop_workers_runtime: bool,
    close_dashboard_tasks: bool,
    close_telegram_clients: bool,
    close_bot_session: bool,
) -> None:
    global telethon_client, reaction_clients, sender_service, bot, runtime_context, scheduler_runtime_task, job_watchdog_task

    if stop_workers_runtime:
        await stop_job_workers_runtime()
        await stop_all_workers()

    if close_dashboard_tasks:
        for task in dashboard_tasks.values():
            task.cancel()
        dashboard_tasks.clear()

    if scheduler_runtime_task and not scheduler_runtime_task.done():
        scheduler_runtime_task.cancel()
        await asyncio.gather(scheduler_runtime_task, return_exceptions=True)
    scheduler_runtime_task = None

    if job_watchdog_task and not job_watchdog_task.done():
        job_watchdog_task.cancel()
        await asyncio.gather(job_watchdog_task, return_exceptions=True)
    job_watchdog_task = None

    if close_telegram_clients and telethon_client:
        try:
            raw_telethon = getattr(telethon_client, "raw", telethon_client)
            await raw_telethon.disconnect()
        except Exception as exc:
            logger.warning("Ошибка при закрытии Telethon: %s", exc)
        telethon_client = None

    if close_telegram_clients:
        for reactor in reaction_clients:
            try:
                raw_client = getattr(reactor.client, "raw", reactor.client)
                await raw_client.disconnect()
            except Exception as exc:
                logger.warning("Ошибка при закрытии реактора %s: %s", reactor.session_name, exc)
        reaction_clients = []
        sender_service = None
        runtime_context = None

    if close_bot_session and bot:
        try:
            await bot.session.close()
        except Exception as exc:
            logger.warning("Ошибка при закрытии Bot session: %s", exc)
        bot = None


async def _run_scheduler_role_loop() -> None:
    logger.info("🚀 Режим scheduler запущен")
    while True:
        try:
            await run_db(db.get_rule_stats)
        except Exception as exc:
            logger.warning("SCHEDULER_LOOP | ошибка тика: %s", exc)
        await asyncio.sleep(30)


async def _run_worker_role_loop() -> None:
    global posting_active
    await start_job_workers_runtime()
    logger.info("🚀 Режим worker запущен")
    while True:
        await asyncio.sleep(10)


async def _start_bot_role() -> None:
    global workers_runtime_enabled, posting_active
    workers_runtime_enabled = False
    posting_active = False
    await _init_db_runtime()
    await _init_sender_runtime(create_ui_policy=True)
    asyncio.create_task(heartbeat_loop("bot", db))
    asyncio.create_task(watchdog_loop(db))
    logger.info("STARTUP | Роль UI (bot) запущена")
    await dp.start_polling(
        bot,
        polling_timeout=30,
        handle_as_tasks=True,
    )


async def _start_scheduler_role() -> None:
    global workers_runtime_enabled, posting_active
    workers_runtime_enabled = False
    posting_active = False
    await _init_db_runtime()
    logger.info("STARTUP | Запуск роли scheduler")
    asyncio.create_task(heartbeat_loop("scheduler", db))
    watchdog_task = asyncio.create_task(run_watchdog_loop(db, interval_seconds=10.0))
    try:
        await run_scheduler_loop(
            db,
            interval_seconds=1.0,
            is_enabled=lambda: True,
        )
    finally:
        watchdog_task.cancel()
        await asyncio.gather(watchdog_task, return_exceptions=True)


async def _start_worker_role() -> None:
    global workers_runtime_enabled
    workers_runtime_enabled = True
    await _init_db_runtime()
    await _init_sender_runtime(create_ui_policy=False)
    logger.info("STARTUP | Запуск роли worker")
    asyncio.create_task(heartbeat_loop("worker", db))
    await _run_worker_role_loop()


async def _start_all_role() -> None:
    global workers_runtime_enabled, posting_active, scheduler_runtime_task, job_watchdog_task
    workers_runtime_enabled = True
    posting_active = False
    await _init_db_runtime()
    await _init_sender_runtime(create_ui_policy=True)
    asyncio.create_task(heartbeat_loop("bot", db))
    asyncio.create_task(heartbeat_loop("scheduler", db))
    asyncio.create_task(heartbeat_loop("worker", db))
    asyncio.create_task(watchdog_loop(db))
    if scheduler_runtime_task is None or scheduler_runtime_task.done():
        scheduler_runtime_task = asyncio.create_task(
            run_scheduler_loop(
                db,
                interval_seconds=1.0,
                is_enabled=lambda: posting_active,
            )
        )
    if job_watchdog_task is None or job_watchdog_task.done():
        job_watchdog_task = asyncio.create_task(run_watchdog_loop(db, interval_seconds=10.0))
    await start_job_workers_runtime()
    logger.info("STARTUP | Legacy режим all запущен")
    await dp.start_polling(
        bot,
        polling_timeout=30,
        handle_as_tasks=True,
    )


async def start_job_workers_runtime() -> None:
    global job_worker_tasks, job_workers_stop_event
    if not sender_service:
        return
    if any(not task.done() for task in job_worker_tasks):
        return

    job_workers_stop_event = asyncio.Event()
    job_worker_tasks = [
        asyncio.create_task(run_light_worker(db, sender_service, "light-worker-1", stop_event=job_workers_stop_event)),
        asyncio.create_task(run_heavy_worker(db, sender_service, "heavy-worker-1", stop_event=job_workers_stop_event)),
    ]
    logger.info("JOB WORKERS | запущены light/heavy воркеры")


async def stop_job_workers_runtime() -> None:
    global job_worker_tasks, job_workers_stop_event
    if not job_worker_tasks:
        return
    tasks = list(job_worker_tasks)
    job_worker_tasks = []
    if job_workers_stop_event is not None:
        job_workers_stop_event.set()
    job_workers_stop_event = None
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("JOB WORKERS | остановлены")


async def main(role: str = "all"):
    normalized_role = normalize_runtime_role(role)
    logger.info("STARTUP | Инициализация роли %s", normalized_role)
    try:
        preflight_result = run_preflight_checks(normalized_role, settings_obj=settings)
        for message in preflight_result.messages:
            logger.info("PREFLIGHT | %s", message)
    except PreflightError as exc:
        logger.error("PREFLIGHT | Критическая ошибка: %s", exc)
        raise SystemExit(2) from exc

    try:
        await run_runtime_role(
            normalized_role,
            run_bot=_start_bot_role,
            run_scheduler=_start_scheduler_role,
            run_worker=_start_worker_role,
            run_all=_start_all_role,
        )
    finally:
        logger.info("SHUTDOWN | Остановка роли %s", normalized_role)
        await _shutdown_runtime(
            stop_workers_runtime=(normalized_role in {"worker", "all"}),
            close_dashboard_tasks=(normalized_role in {"bot", "all"}),
            close_telegram_clients=(normalized_role in {"bot", "worker", "all"}),
            close_bot_session=(normalized_role in {"bot", "worker", "all"}),
        )
        logger.info("SHUTDOWN | Роль %s остановлена", normalized_role)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram forwarder bot runtime")
    parser.add_argument(
        "--role",
        choices=["bot", "ui", "scheduler", "worker", "all"],
        default="all",
        help="Роль процесса: bot|ui|scheduler|worker|all",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Проверить preflight и завершить процесс без запуска роли",
    )
    parser.add_argument(
        "--ops-status",
        action="store_true",
        help="Показать operational статус (для smoke-check/runbook) и завершить процесс",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Вывести результат в JSON (используется для operational-скриптов)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    role = normalize_runtime_role(args.role)
    if args.ops_status:
        snapshot = build_operational_snapshot(db).as_dict()
        if args.json:
            print(json.dumps(snapshot, ensure_ascii=False))
        else:
            print(
                "OPS STATUS\n"
                f"- overall_status: {snapshot['overall_status']}\n"
                f"- system_mode: {snapshot['system_mode']}\n"
                f"- roles: {snapshot['roles']}\n"
                f"- role_problems: {snapshot['role_problems']}\n"
                f"- backlog: {snapshot['backlog']}\n"
                f"- restart_loop_symptoms: {snapshot['restart_loop_symptoms']}"
            )
        raise SystemExit(0)

    if args.preflight_only:
        try:
            result = run_preflight_checks(role, settings_obj=settings)
            if args.json:
                print(json.dumps({"ok": result.ok, "role": result.role, "messages": result.messages}, ensure_ascii=False))
            else:
                print(f"PREFLIGHT OK ({result.role})")
                for item in result.messages:
                    print(f"- {item}")
            raise SystemExit(0)
        except PreflightError as exc:
            print(f"PREFLIGHT ERROR: {exc}")
            raise SystemExit(2) from exc

    try:
        asyncio.run(main(role=role))
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
