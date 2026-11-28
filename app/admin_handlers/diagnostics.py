from __future__ import annotations

from pathlib import Path
from html import escape
import re
from time import monotonic
from traceback import extract_tb
from uuid import uuid4

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.delivery_observability import DeliveryObservabilityConfig

from .context import AdminHandlersContext


def register_admin_diagnostics_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    confirmations = _OrphanConfirmations()
    @dp.message(lambda m: m.text == "⚠️ Диагностика")
    async def handle_diagnostics_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        await ctx.send_message_safe(chat_id=message.chat.id, text="⚠️ Раздел: Диагностика", reply_markup=ctx.get_diagnostics_menu())

    @dp.message(lambda m: (m.text or "").strip() == "⚠️ Проблемные доставки")
    async def handle_faulty(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        pages = await ctx.run_db(ctx.build_faulty_pages, 200)
        page = 0
        total_pages = len(pages)
        current = pages[page]
        await ctx.send_message_safe(chat_id=message.chat.id, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"], rule_id=current["rule_id"]))


    @dp.message(lambda m: (m.text or "").strip() in {"⚠️ Центр ошибок", "📊 Диагностика доставки"})
    async def handle_delivery_diagnostics(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        text, markup = await center_payload()
        await ctx.send_message_safe(chat_id=message.chat.id, text=text, parse_mode="HTML", reply_markup=markup)

    async def center_payload():
        try:
            counts = await ctx.run_db(
                ctx.db.get_diagnostics_center_snapshot,
                older_than_seconds=DeliveryObservabilityConfig().stale_processing_after_seconds,
            )
        except Exception:
            ctx.logger.warning("Ошибка центра диагностики", exc_info=True)
            counts = None
        return _center_payload(counts)

    @dp.callback_query(lambda c: c.data == "diag_center")
    async def handle_center_refresh(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback)
        text, markup = await center_payload()
        await ctx.edit_message_text_safe(message=callback.message, text=text, parse_mode="HTML", reply_markup=markup)

    async def stale_payload():
        try:
            rows = await ctx.run_db(
                ctx.db.get_stuck_processing_deliveries,
                older_than_seconds=DeliveryObservabilityConfig().stale_processing_after_seconds, limit=50,
            )
        except Exception as exc:
            ctx.logger.warning("Диагностика зависших заданий: %s | %s",
                               type(exc).__name__, _safe_quarantine_traceback(exc))
            return _diagnostic_unavailable_payload("diag_stale")
        return _format_stuck_processing_deliveries_text(rows), _build_stuck_processing_keyboard(rows)

    @dp.callback_query(lambda c: c.data == "diag_stale")
    async def handle_center_stale(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback)
        text, markup = await stale_payload()
        await ctx.edit_message_text_safe(message=callback.message, text=text, parse_mode="HTML", reply_markup=markup)

    @dp.callback_query(lambda c: c.data and c.data.startswith("diag_review:"))
    async def handle_center_review(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, category, raw_offset = callback.data.split(":")
            offset = int(raw_offset)
            if category not in {"receipts", "cooldowns"} or not 0 <= offset < 2**31:
                raise ValueError
        except (ValueError, TypeError):
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        try:
            rows = await ctx.run_db(ctx.db.get_diagnostics_review_rows, category, limit=10, offset=offset)
        except Exception as exc:
            ctx.logger.warning("Диагностика %s: %s | %s", category,
                               type(exc).__name__, _safe_quarantine_traceback(exc))
            text, markup = _diagnostic_unavailable_payload(f"diag_review:{category}:{offset}")
        else:
            text, markup = _review_payload(category, rows, offset)
        await ctx.edit_message_text_safe(message=callback.message, text=text, parse_mode="HTML", reply_markup=markup)


    @dp.message(lambda m: (m.text or "").strip() == "🧯 Зависшие задачи")
    async def handle_stuck_deliveries(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        text, markup = await stale_payload()
        await ctx.send_message_safe(chat_id=message.chat.id, text=text, parse_mode="HTML", reply_markup=markup)
        orphans = await ctx.run_db(ctx.db.get_orphaned_processing_rules, limit=10)
        if orphans:
            await ctx.send_message_safe(
                chat_id=message.chat.id, text=_format_orphan_recovery_instruction(),
                parse_mode="HTML", reply_markup=_orphan_page_keyboard(orphans, 0),
            )

    @dp.callback_query(lambda c: c.data and c.data.startswith("stuck_orphan_page:"))
    async def handle_stuck_orphan_page(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        cursor = _parse_orphan_rule_id(callback.data, allow_zero=True)
        if cursor is None or not isinstance(callback.message, Message):
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        rows = await ctx.run_db(ctx.db.get_orphaned_processing_rules, limit=10, after_rule_id=cursor)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=_format_orphan_recovery_instruction() if rows else "<b>🧯 Больше правил нет</b>",
            parse_mode="HTML", reply_markup=_orphan_page_keyboard(rows, cursor),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("stuck_orphan_request:"))
    async def handle_stuck_orphan_request(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        rule_id = _parse_orphan_rule_id(callback.data)
        if rule_id is None or callback.from_user is None or not isinstance(callback.message, Message):
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        ids = await ctx.run_db(ctx.db.get_orphan_processing_ids, rule_id)
        if not ids or len(ids) > 100:
            await ctx.edit_message_text_safe(
                message=callback.message, parse_mode="HTML",
                text="<b>🧯 Проверка</b>\n\nНет доставок или их больше 100. Обновите диагностику; для большой очереди нужна ручная сверка.",
            )
            return
        token = confirmations.issue(callback, rule_id, ids)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=_format_orphan_recovery_instruction(rule_id) + "\n\n<b>ID доставок</b>\n" + ", ".join(map(str, ids)),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Завершить без повтора", callback_data=f"stuck_orphan_confirm:{token}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"stuck_orphan_cancel:{token}")],
            ]),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("stuck_orphan_cancel:"))
    async def handle_stuck_orphan_cancel(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if callback.from_user is None or not isinstance(callback.message, Message):
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if confirmations.consume(callback) is None:
            await ctx.answer_callback_safe(callback, "Подтверждение устарело. Откройте диагностику заново.", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback, "Отменено")
        await ctx.edit_message_text_safe(message=callback.message, text="<b>🧯 Действие отменено</b>", parse_mode="HTML", reply_markup=_back_to_center())

    @dp.callback_query(lambda c: c.data and c.data.startswith("stuck_orphan_confirm:"))
    async def handle_stuck_orphan_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if callback.from_user is None or not isinstance(callback.message, Message):
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        snapshot = confirmations.consume(callback)
        if snapshot is None:
            await ctx.answer_callback_safe(callback, "Подтверждение устарело. Откройте диагностику заново.", show_alert=True)
            return
        rule_id, ids = snapshot
        await ctx.answer_callback_safe_once(callback)
        try:
            result = await ctx.run_db(
                ctx.db.quarantine_orphaned_processing_rule, rule_id,
                admin_id=callback.from_user.id, expected_ids=ids,
            )
        except Exception as exc:
            ctx.logger.warning(
                "ORPHAN_QUARANTINE_UNKNOWN | rule_id=%s | error_type=%s | traceback=%s",
                rule_id, type(exc).__name__, _safe_quarantine_traceback(exc),
            )
            message = "Результат неизвестен. Обновите диагностику и проверьте состояние правила."
        else:
            status = result.get("status")
            if status == "quarantined":
                message = f"Завершено проблемных доставок: {int(result.get('updated_count') or 0)}. История сохранена."
            else:
                message = _ORPHAN_STATUS_TEXT.get(status, "Действие не выполнено. Обновите диагностику.")
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=f"<b>🧯 Правило #{rule_id}</b>\n\n<b>Результат</b>\n{message}",
            parse_mode="HTML", reply_markup=_back_to_center(),
        )

    @dp.callback_query(lambda c: c.data == "stuck_reset_request")
    async def handle_stuck_reset_request(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        stale_seconds = DeliveryObservabilityConfig().stale_processing_after_seconds
        rows = await ctx.run_db(ctx.db.get_stuck_processing_deliveries, older_than_seconds=stale_seconds, limit=50)
        if not rows:
            await ctx.answer_callback_safe(callback, "Зависшие задачи не найдены", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        text = (
            "<b>🧯 Безопасное восстановление</b>\n\n"
            "<b>Причина</b>\n"
            f"Найдено доставок с устаревшими заданиями: {len(rows)}.\n\n"
            "<b>Действие</b>\n"
            "Подтвердите восстановление не более 50 заданий. "
            "Устаревший heartbeat не доказывает остановку воркера: его задание может быть отозвано. "
            "Статусы публикаций не сбрасываются. "
            "Бот проверит сохранённые данные отправки."
        )
        await ctx.edit_message_text_safe(
            message=callback.message, text=text, parse_mode="HTML",
            reply_markup=_build_stuck_reset_confirm_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "stuck_reset_confirm")
    async def handle_stuck_reset_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        stale_seconds = DeliveryObservabilityConfig().stale_processing_after_seconds
        result = await ctx.run_db(
            ctx.db.recover_stale_delivery_jobs,
            older_than_seconds=stale_seconds,
            limit=50,
            admin_id=callback.from_user.id if callback.from_user else None,
        )
        await ctx.answer_callback_safe_once(callback, "Готово")
        updated = int(result.get("updated_count") or 0)
        remaining = await ctx.run_db(ctx.db.get_stuck_processing_deliveries, older_than_seconds=stale_seconds, limit=1)
        tail = "\nОстались устаревшие задания. Проверьте диагностику повторно." if remaining else "\nУстаревших заданий больше не найдено."
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=f"<b>🧯 Зависшие задачи</b>\n\n<b>Результат</b>\nВосстановлено заданий: {updated}.{tail}",
            parse_mode="HTML", reply_markup=_back_to_center(),
        )

    @dp.callback_query(lambda c: c.data == "stuck_reset_cancel")
    async def handle_stuck_reset_cancel(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback, "Отменено")
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="<b>🧯 Зависшие задачи</b>\n\nДействие отменено. Возврат в очередь не выполнялся.",
            parse_mode="HTML", reply_markup=_back_to_center(),
        )

    async def journal_payload(category, page):
        pages = await ctx.run_db(ctx.build_system_journal_pages, 300, category=category)
        total = len(pages)
        page = ctx.clamp_page(page, total)
        current = pages[page]
        return current["text"], ctx.build_system_journal_inline_keyboard(
            page, total, category=category, rule_ids=current["rule_ids"],
        )

    @dp.message(lambda m: (m.text or "").strip() in {"📊 Журнал событий", "📊 Журнал системы"})
    async def handle_system_journal(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        text, markup = await journal_payload("all", 0)
        await ctx.send_message_safe(chat_id=message.chat.id, text=text, parse_mode="HTML", reply_markup=markup)

    # Old reply keyboards can survive a deployment; route them away from the
    # retired test screen without retaining any test publication functionality.
    @dp.message(lambda m: m.text == "🎨 Тест styled-кнопок")
    async def handle_retired_test(message: Message):
        if not await ctx.is_admin(message):
            return
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await ctx.send_message_safe(chat_id=message.chat.id, text="Раздел обновлён.", reply_markup=ctx.get_diagnostics_menu())

    @dp.callback_query(lambda c: c.data and c.data.startswith("styled_test_"))
    async def handle_retired_test_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe(callback, "Кнопка устарела.", show_alert=True)

    @dp.callback_query(lambda c: c.data in {"syslog_page_info", "faulty_page_info"})
    async def handle_page_info(callback: CallbackQuery):
        if await ctx.is_admin_callback(callback):
            await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith(("syslog_page:", "syslog_refresh:", "syslog_filter:")))
    async def handle_system_journal_navigation(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            category, page = _parse_journal_callback(callback.data)
        except (ValueError, TypeError):
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        text, markup = await journal_payload(category, page)
        await ctx.edit_message_text_safe(message=callback.message, text=text, parse_mode="HTML", reply_markup=markup)

    @dp.callback_query(lambda c: c.data == "syslog_back")
    async def handle_syslog_back(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text="⚠️ Раздел: Диагностика")
        await ctx.send_message_safe(chat_id=callback.message.chat.id, text="⚠️ Раздел: Диагностика", reply_markup=ctx.get_diagnostics_menu())

    @dp.callback_query(lambda c: c.data and c.data.startswith("faulty_page:"))
    async def handle_faulty_page(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, page_raw = ctx.parse_callback_parts(callback.data, "faulty_page", 2)
            page = int(page_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        pages = await ctx.run_db(ctx.build_faulty_pages, 200)
        total_pages = len(pages)
        page = ctx.clamp_page(page, total_pages)
        current = pages[page]
        await ctx.edit_message_text_safe(message=callback.message, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"], rule_id=current["rule_id"]))

    @dp.callback_query(lambda c: c.data and c.data.startswith("faulty_refresh:"))
    async def handle_faulty_refresh(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, page_raw = ctx.parse_callback_parts(callback.data, "faulty_refresh", 2)
            page = int(page_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        pages = await ctx.run_db(ctx.build_faulty_pages, 200)
        total_pages = len(pages)
        page = ctx.clamp_page(page, total_pages)
        current = pages[page]
        await ctx.edit_message_text_safe(message=callback.message, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"], rule_id=current["rule_id"]))

    @dp.callback_query(lambda c: c.data and c.data.startswith("faulty_ack:"))
    async def handle_faulty_ack(callback: CallbackQuery):
        # Legacy keyboards must not acknowledge or resolve incidents.
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe(callback, "Кнопка устарела. Нажмите «Обновить».", show_alert=True)

    @dp.callback_query(lambda c: c.data and c.data.startswith("faulty_clear:"))
    async def handle_faulty_clear(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, delivery_id_raw, page_raw = ctx.parse_callback_parts(callback.data, "faulty_clear", 3)
            delivery_id = int(delivery_id_raw)
            page = int(page_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        ok = await ctx.run_db(ctx.db.clear_faulty_delivery_log, delivery_id=delivery_id, admin_id=callback.from_user.id if callback.from_user else ctx.settings.admin_id)
        if not ok:
            await ctx.answer_callback_safe(callback, "Нечего очищать", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback, "🧹 Ошибка очищена; история сохранена")
        pages = await ctx.run_db(ctx.build_faulty_pages, 200)
        total_pages = len(pages)
        page = ctx.clamp_page(page, total_pages)
        current = pages[page]
        await ctx.edit_message_text_safe(message=callback.message, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"], rule_id=current["rule_id"]))

    @dp.callback_query(lambda c: c.data == "faulty_back")
    async def handle_faulty_back(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback)
        text, markup = await center_payload()
        await ctx.edit_message_text_safe(message=callback.message, text=text, parse_mode="HTML", reply_markup=markup)


def _duration_ru(seconds: object) -> str:
    try:
        value = max(0, int(float(seconds or 0)))
    except (TypeError, ValueError):
        return "нет данных"
    minutes = value // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} ч {minutes} мин"


def _back_to_center():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Центр ошибок", callback_data="diag_center")],
    ])


def _diagnostic_unavailable_payload(refresh_callback):
    buttons = [[InlineKeyboardButton(text="🔄 Обновить", callback_data=refresh_callback)]]
    buttons.extend(_back_to_center().inline_keyboard)
    return "⚪ <b>Данные недоступны</b>\n\nНе удалось получить данные. Попробуйте обновить.", InlineKeyboardMarkup(inline_keyboard=buttons)


def _center_payload(counts):
    labels = (
        ("⚠️ Проблемные доставки", "faulty", "faulty_page:0"),
        ("🧯 Зависшие задания", "stale_jobs", "diag_stale"),
        ("🔧 Правила без задания", "orphan_rules", "stuck_orphan_page:0"),
        ("🔎 Неподтверждённые публикации", "receipts", "diag_review:receipts:0"),
        ("⏳ Активные ограничения", "cooldowns", "diag_review:cooldowns:0"),
    )
    lines = ["<b>⚠️ ЦЕНТР ОШИБОК</b>", ""]
    buttons = []
    if counts is None:
        lines.extend(["⚪ <b>Данные недоступны</b>", "Обновите диагностику."])
    else:
        lines.append("🟡 Требуется внимание" if any(counts.values()) else "🟢 Проблем не найдено")
        lines.append("")
    for title, key, callback in labels:
        value = "—" if counts is None else str(int(counts[key]))
        lines.append(f"{title}: <b>{value}</b>")
        buttons.append([InlineKeyboardButton(text=f"{title} ({value})", callback_data=callback)])
    lines.extend(["", "Доставки считаются по записям (список последних 200), задания — по ID.",
                  "Доставки без задания сгруппированы по правилам.",
                  "Ограничения — временные лимиты Telegram и квоты проверок эмодзи; не число задержанных постов."])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="diag_center")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


def _parse_journal_callback(data):
    parts = data.split(":")
    if len(parts) == 2 and parts[0] == "syslog_filter":
        category, page = parts[1], 0
    elif len(parts) == 2 and parts[0] in {"syslog_page", "syslog_refresh"}:
        category, page = "all", int(parts[1])
    elif len(parts) == 3 and parts[0] in {"syslog_page", "syslog_refresh"}:
        category, page = parts[1], int(parts[2])
    else:
        raise ValueError("Invalid journal callback")
    if category not in {"all", "errors", "publications", "admin"} or not 0 <= page < 2**31:
        raise ValueError("Invalid journal page")
    return category, page


def _review_payload(category, rows, offset):
    receipt = category == "receipts"
    lines = ["<b>🔎 Неподтверждённые публикации</b>" if receipt else "<b>⏳ Активные ограничения</b>", ""]
    buttons = []
    if not rows:
        lines.append("Записей на этой странице нет.")
    for row in rows:
        if receipt:
            rule_id = int(row["rule_id"])
            source = ", ".join(str(int(mid)) for mid in row["source_message_ids"])
            status = "Срок истёк — требуется сверка" if row["expired"] else "Ожидает подтверждения"
            lines.extend([f"<b>Правило #{rule_id}</b> · {status}", f"Исходные сообщения: {source}", ""])
            buttons.append([InlineKeyboardButton(text=f"📂 Правило #{rule_id}", callback_data=f"rule_card:{rule_id}")])
        else:
            # Scope keys must not expose arbitrary secret-bearing strings.
            scope = str(row["scope_key"])
            if re.fullmatch(r"(?:mtproto|bot):\d+(?::-?\d+)?", scope):
                label = f"Telegram: {scope}"
            elif re.fullmatch(r"video-emoji:\d+(?::user:\d+)?:quota", scope):
                label = "Квота проверки эмодзи (не лимит Telegram)"
            else:
                label = "Другая область"
            lines.append(f"• {escape(label)} — <b>{_duration_ru(row['seconds'])}</b>")
    lines.extend(["", "Не отправляйте повторно без сверки с получателем." if receipt else "Ограничение снимается автоматически по истечении срока."])
    nav = []
    if offset:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"diag_review:{category}:{max(0, offset-10)}"))
    if len(rows) == 10:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"diag_review:{category}:{offset+10}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"diag_review:{category}:{offset}")])
    buttons.extend(_back_to_center().inline_keyboard)
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


def _format_stuck_processing_deliveries_text(rows) -> str:
    rows = list(rows or [])
    lines = ["<b>🧯 Зависшие задания</b>", "", f"Записей на экране: <b>{len(rows)}</b> (до 50)"]
    if not rows:
        lines.extend(["", "Устаревшие задания не найдены."])
    tail = [
        "",
        "Можно восстановить существующие задания с истёкшей арендой или устаревшим heartbeat.",
        "Это не сбрасывает статусы публикаций и не разрешает повторную отправку без проверки.",
    ]
    for row in rows:
        line = f"• Правило #{int(row['rule_id'])} · доставка {int(row['delivery_id'])} · {_duration_ru(row['age_seconds'])}"
        if len("\n".join([*lines, line, "… Остальные доступны по кнопкам правил.", *tail]).encode("utf-16-le")) // 2 > 4000:
            lines.append("… Остальные доступны по кнопкам правил.")
            break
        lines.append(line)
    return "\n".join([*lines, *tail])


def _build_stuck_processing_keyboard(rows) -> InlineKeyboardMarkup:
    buttons = []
    if rows:
        buttons.append([InlineKeyboardButton(text="🔁 Восстановить задания", callback_data="stuck_reset_request")])
        for rule_id in sorted({int(row["rule_id"]) for row in rows}):
            buttons.append([InlineKeyboardButton(text=f"📂 Правило #{rule_id}", callback_data=f"rule_card:{rule_id}")])
    buttons.extend(_back_to_center().inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_stuck_reset_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Восстановить", callback_data="stuck_reset_confirm")], [InlineKeyboardButton(text="❌ Отмена", callback_data="stuck_reset_cancel")]])



_ORPHAN_STATUS_TEXT = {
    "rule_not_paused": "Выключите правило и дождитесь завершения пересканирования.",
    "job_can_resume": "Есть задание, которое ещё может выполняться или восстановиться после оплаты.",
    "nothing_to_do": "Незавершённых доставок больше нет.",
    "snapshot_changed": "Список доставок изменился. Откройте диагностику заново и подтвердите новый список.",
    "publication_evidence": "Есть сохранённые данные о публикации. Нужна сверка получателя, а не сброс доставки.",
    "partial_album": "Есть незавершённая часть альбома. Сверьте получатель и используйте пересканирование.",
}


def _parse_orphan_rule_id(data: str | None, *, allow_zero: bool = False) -> int | None:
    try:
        rule_id = int((data or "").split(":")[1])
    except (IndexError, ValueError):
        return None
    return rule_id if int(not allow_zero) <= rule_id < 2**63 else None



def _format_orphan_recovery_instruction(rule_id: int | None = None) -> str:
    heading = "Доставки без активного задания" if rule_id is None else f"Правило #{int(rule_id)}"
    return (
        f"<b>🧯 {heading}</b>\n\n"
        "<b>Причина</b>\n"
        "Результат прежней отправки неизвестен, активное задание отсутствует.\n\n"
        "<b>Что делать</b>\n"
        "1. Выключите правило.\n"
        "2. Дождитесь завершения прежней отправки и проверьте получатель.\n"
        "3. Если результат неизвестен, подтвердите завершение без повтора.\n\n"
        "История сохранится. Доставки без подтверждения будут отмечены как проблемные. "
        "Бот не отправит их повторно."
    )



def _orphan_page_keyboard(rows, cursor: int) -> InlineKeyboardMarkup:
    buttons = []
    for row in rows:
        rule_id = int(row["rule_id"])
        active = row["is_active"]
        buttons.append([InlineKeyboardButton(
            text=f"Правило #{rule_id} — {'выключить' if active else 'проверить'}",
            callback_data=f"rule_card:{rule_id}" if active else f"stuck_orphan_request:{rule_id}",
        )])
    navigation = []
    if cursor:
        navigation.append(InlineKeyboardButton(text="В начало", callback_data="stuck_orphan_page:0"))
    if len(rows) == 10:
        navigation.append(InlineKeyboardButton(text="Далее", callback_data=f"stuck_orphan_page:{int(rows[-1]['rule_id'])}"))
    if navigation:
        buttons.append(navigation)
    buttons.extend(_back_to_center().inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


class _OrphanConfirmations:
    """Process-local, one-use, bounded confirmation for the displayed IDs."""

    def __init__(self):
        self._items = {}

    @staticmethod
    def _scope(callback):
        user = callback.from_user
        message = callback.message
        if user is None or message is None:
            return None
        return (user.id, message.chat.id, message.message_id)

    def issue(self, callback, rule_id: int, ids: list[int]) -> str:
        now = monotonic()
        scope = self._scope(callback)
        if scope is None:
            raise ValueError("Confirmation requires operator and message")
        self._items = {key: item for key, item in self._items.items()
                       if item[0] > now and item[1] != scope}
        if len(self._items) >= 128:
            self._items.pop(next(iter(self._items)))
        token = uuid4().hex
        self._items[token] = (now + 300, scope, rule_id, tuple(ids))
        return token

    def consume(self, callback):
        token = (callback.data or "").split(":", 1)[-1]
        item = self._items.get(token)
        if item is None or item[1] != self._scope(callback):
            return None
        self._items.pop(token)
        if item[0] <= monotonic():
            return None
        return item[2], list(item[3])


def _safe_quarantine_traceback(exc: Exception) -> str:
    """Identify the failed operation without logging exception text or locals."""
    frames = extract_tb(exc.__traceback__, limit=-16)
    return " -> ".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}" for frame in frames
    ) or "unavailable"
