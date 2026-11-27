from __future__ import annotations

from time import monotonic
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


    @dp.message(lambda m: m.text == "📊 Диагностика доставки")
    async def handle_delivery_diagnostics(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        try:
            text = await ctx.build_delivery_diagnostics_admin_text()
        except Exception:
            ctx.logger.warning("Ошибка подготовки диагностики доставки", exc_info=True)
            text = (
                "<b>📊 Диагностика доставки</b>\n\n"
                "⚪ <b>Диагностика недоступна</b>\n\n"
                "Не удалось получить данные.\n\n"
                "🔄 Попробуйте обновить диагностику позже."
            )
        await ctx.send_message_safe(chat_id=message.chat.id, text=text, parse_mode="HTML")


    @dp.message(lambda m: (m.text or "").strip() == "🧯 Зависшие задачи")
    async def handle_stuck_deliveries(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        stale_seconds = DeliveryObservabilityConfig().stale_processing_after_seconds
        rows = await ctx.run_db(ctx.db.get_stuck_processing_deliveries, older_than_seconds=stale_seconds, limit=50)
        text = ctx.format_stuck_processing_deliveries_text(rows) if ctx.format_stuck_processing_deliveries_text else _format_stuck_processing_deliveries_text(rows)
        await ctx.send_message_safe(chat_id=message.chat.id, text=text, reply_markup=_build_stuck_processing_keyboard(bool(rows)))
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
        if not isinstance(callback.message, Message):
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        confirmations.consume(callback)
        await ctx.answer_callback_safe_once(callback, "Отменено")
        await ctx.edit_message_text_safe(message=callback.message, text="<b>🧯 Действие отменено</b>", parse_mode="HTML")

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
            ctx.logger.warning("ORPHAN_QUARANTINE_UNKNOWN | rule_id=%s | error_type=%s", rule_id, type(exc).__name__)
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
            parse_mode="HTML",
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
            parse_mode="HTML",
        )

    @dp.callback_query(lambda c: c.data == "stuck_reset_cancel")
    async def handle_stuck_reset_cancel(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback, "Отменено")
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="<b>🧯 Зависшие задачи</b>\n\nДействие отменено. Возврат в очередь не выполнялся.",
            parse_mode="HTML",
        )

    @dp.message(lambda m: m.text == "📊 Журнал системы")
    async def handle_system_journal(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        pages = await ctx.run_db(ctx.build_system_journal_pages, 300)
        page = 0
        total_pages = len(pages)
        await ctx.send_message_safe(chat_id=message.chat.id, text=pages[page], parse_mode="HTML", reply_markup=ctx.build_system_journal_inline_keyboard(page, total_pages))

    @dp.message(lambda m: m.text == "🎨 Тест styled-кнопок")
    async def handle_styled_buttons_test(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Primary", style="primary", callback_data="styled_test_primary"),
                    InlineKeyboardButton(text="Success", style="success", callback_data="styled_test_success"),
                ],
                [
                    InlineKeyboardButton(text="Danger", style="danger", callback_data="styled_test_danger"),
                ],
            ]
        )
        await ctx.send_message_safe(chat_id=message.chat.id, text=
            "🎨 Тест styled-кнопок (только для ADMIN_ID).\n"
            "Если цвета не отображаются, остаёмся на emoji-style.",
            reply_markup=markup,
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("styled_test_"))
    async def handle_styled_buttons_test_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback, "✅ Нажатие получено")

    @dp.callback_query(lambda c: c.data == "syslog_page_info")
    async def handle_syslog_page_info(callback: CallbackQuery):
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("syslog_page:"))
    async def handle_syslog_page(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, page_raw = ctx.parse_callback_parts(callback.data, "syslog_page", 2)
            page = int(page_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        pages = await ctx.run_db(ctx.build_system_journal_pages, 300)
        total_pages = len(pages)
        page = ctx.clamp_page(page, total_pages)
        await ctx.edit_message_text_safe(message=callback.message, text=pages[page], parse_mode="HTML", reply_markup=ctx.build_system_journal_inline_keyboard(page, total_pages))

    @dp.callback_query(lambda c: c.data and c.data.startswith("syslog_refresh:"))
    async def handle_syslog_refresh(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, page_raw = ctx.parse_callback_parts(callback.data, "syslog_refresh", 2)
            page = int(page_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        pages = await ctx.run_db(ctx.build_system_journal_pages, 300)
        total_pages = len(pages)
        page = ctx.clamp_page(page, total_pages)
        await ctx.edit_message_text_safe(message=callback.message, text=pages[page], parse_mode="HTML", reply_markup=ctx.build_system_journal_inline_keyboard(page, total_pages))

    @dp.callback_query(lambda c: c.data == "syslog_back")
    async def handle_syslog_back(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text="⚠️ Раздел: Диагностика")
        await ctx.send_message_safe(chat_id=callback.message.chat.id, text="⚠️ Раздел: Диагностика", reply_markup=ctx.get_diagnostics_menu())

    @dp.callback_query(lambda c: c.data == "faulty_page_info")
    async def handle_faulty_page_info(callback: CallbackQuery):
        await ctx.answer_callback_safe_once(callback)

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
        await ctx.edit_message_text_safe(message=callback.message, text="⚠️ Раздел: Диагностика")
        await ctx.send_message_safe(chat_id=callback.message.chat.id, text="⚠️ Раздел: Диагностика", reply_markup=ctx.get_diagnostics_menu())


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


def _format_stuck_processing_deliveries_text(rows) -> str:
    rows = list(rows or [])
    lines = ["🧯 Зависшие задачи", "", f"Найдено: {len(rows)}"]
    if not rows:
        lines.extend(["", "Зависшие задачи не найдены."])
        return "\n".join(lines)
    for index, row in enumerate(rows, start=1):
        lines.extend([
            "",
            f"{index}) Доставка №{row.get('delivery_id')}",
            f"Правило #{row.get('rule_id')}",
            f"Висит: {_duration_ru(row.get('age_seconds'))}",
            f"Пост: {row.get('post_id')}",
        ])
    lines.extend([
        "",
        "Можно восстановить существующие задания с истёкшей арендой или устаревшим heartbeat.",
        "Это не сбрасывает статусы публикаций и не разрешает повторную отправку без проверки.",
    ])
    return "\n".join(lines)[:4000]


def _build_stuck_processing_keyboard(has_rows: bool) -> InlineKeyboardMarkup | None:
    if not has_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔁 Восстановить задания", callback_data="stuck_reset_request")]])


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
    return InlineKeyboardMarkup(inline_keyboard=buttons)


class _OrphanConfirmations:
    """Process-local, one-use, bounded confirmation for the displayed IDs."""

    def __init__(self):
        self._items = {}

    @staticmethod
    def _scope(callback):
        return (callback.from_user.id, callback.message.chat.id, callback.message.message_id)

    def issue(self, callback, rule_id: int, ids: list[int]) -> str:
        now = monotonic()
        scope = self._scope(callback)
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
