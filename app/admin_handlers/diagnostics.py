from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.delivery_observability import DeliveryObservabilityConfig

from .context import AdminHandlersContext


def register_admin_diagnostics_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
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
        await ctx.send_message_safe(chat_id=message.chat.id, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]))


    @dp.message(lambda m: m.text == "📊 Диагностика доставки")
    async def handle_delivery_diagnostics(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        try:
            text = await ctx.build_delivery_diagnostics_admin_text()
        except Exception:
            ctx.logger.warning("Ошибка подготовки диагностики доставки", exc_info=True)
            text = "📊 Диагностика доставки\n\nСтатус: UNKNOWN\nПричина: временная ошибка диагностики. Попробуйте позже."
        await ctx.send_message_safe(chat_id=message.chat.id, text=text)


    @dp.message(lambda m: (m.text or "").strip() == "🧯 Зависшие задачи")
    async def handle_stuck_deliveries(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        stale_seconds = DeliveryObservabilityConfig().stale_processing_after_seconds
        rows = await ctx.run_db(ctx.db.get_stuck_processing_deliveries, older_than_seconds=stale_seconds, limit=50)
        text = ctx.format_stuck_processing_deliveries_text(rows) if ctx.format_stuck_processing_deliveries_text else _format_stuck_processing_deliveries_text(rows)
        await ctx.send_message_safe(chat_id=message.chat.id, text=text, reply_markup=_build_stuck_processing_keyboard(bool(rows)))

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
            "🧯 Возврат зависших задач в очередь\n\n"
            f"Будет обработано не больше 50 задач. Сейчас найдено к возврату: {len(rows)}.\n\n"
            "⚠️ Важно:\n"
            "если задача зависла после фактической отправки в Telegram, возврат в очередь может привести к дублю.\n"
            "Используйте только когда уверены, что задача действительно зависла."
        )
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=_build_stuck_reset_confirm_keyboard())

    @dp.callback_query(lambda c: c.data == "stuck_reset_confirm")
    async def handle_stuck_reset_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        stale_seconds = DeliveryObservabilityConfig().stale_processing_after_seconds
        result = await ctx.run_db(
            ctx.db.reset_stuck_processing_deliveries_to_pending,
            older_than_seconds=stale_seconds,
            limit=50,
            admin_id=callback.from_user.id if callback.from_user else None,
        )
        await ctx.answer_callback_safe_once(callback, "Готово")
        updated = int(result.get("updated_count") or 0)
        remaining = await ctx.run_db(ctx.db.get_stuck_processing_deliveries, older_than_seconds=stale_seconds, limit=1)
        tail = "\nОстались зависшие задачи — повторите действие при необходимости." if remaining else "\nЗависших задач больше не найдено."
        await ctx.edit_message_text_safe(message=callback.message, text=f"🧯 Зависшие задачи\n\nВозвращено в очередь: {updated}.{tail}")

    @dp.callback_query(lambda c: c.data == "stuck_reset_cancel")
    async def handle_stuck_reset_cancel(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback, "Отменено")
        await ctx.edit_message_text_safe(message=callback.message, text="🧯 Зависшие задачи\n\nДействие отменено. Reset не выполнялся.")

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
        await ctx.edit_message_text_safe(message=callback.message, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]))

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
        await ctx.edit_message_text_safe(message=callback.message, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]))

    @dp.callback_query(lambda c: c.data and c.data.startswith("faulty_ack:"))
    async def handle_faulty_ack(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, delivery_id_raw, page_raw = ctx.parse_callback_parts(callback.data, "faulty_ack", 3)
            delivery_id = int(delivery_id_raw)
            page = int(page_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        row = await ctx.run_db(ctx.db.get_delivery, delivery_id)
        if not row:
            await ctx.answer_callback_safe(callback, "Этой проблемы уже нет", show_alert=True)
            return
        rule_id = int(row["rule_id"])
        await ctx.run_db(ctx.db.resolve_problem, f"rule_faulty_{rule_id}")
        await ctx.run_db(ctx.db.resolve_problem, f"target_dead_{rule_id}")
        await ctx.run_db(ctx.db.resolve_problem, f"rule_worker_error_{rule_id}")
        await ctx.answer_callback_safe_once(callback, "✅ Помечено как «взята в работу»")
        pages = await ctx.run_db(ctx.build_faulty_pages, 200)
        total_pages = len(pages)
        page = ctx.clamp_page(page, total_pages)
        current = pages[page]
        await ctx.edit_message_text_safe(message=callback.message, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]))

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
        await ctx.answer_callback_safe_once(callback, "🧹 Лог очищен")
        pages = await ctx.run_db(ctx.build_faulty_pages, 200)
        total_pages = len(pages)
        page = ctx.clamp_page(page, total_pages)
        current = pages[page]
        await ctx.edit_message_text_safe(message=callback.message, text=current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]))

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
            f"{index}) delivery_id={row.get('delivery_id')}",
            f"Правило #{row.get('rule_id')}",
            f"Висит: {_duration_ru(row.get('age_seconds'))}",
            f"Пост: {row.get('post_id')}",
        ])
    lines.extend([
        "",
        "Вы можете вернуть зависшие задачи в очередь.",
        "Это безопаснее, чем править базу руками, но если пост уже был фактически отправлен Telegram, возможен дубль.",
    ])
    return "\n".join(lines)[:4000]


def _build_stuck_processing_keyboard(has_rows: bool) -> InlineKeyboardMarkup | None:
    if not has_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔁 Вернуть зависшие в очередь", callback_data="stuck_reset_request")]])


def _build_stuck_reset_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Да, вернуть в очередь", callback_data="stuck_reset_confirm")], [InlineKeyboardButton(text="❌ Отмена", callback_data="stuck_reset_cancel")]])
