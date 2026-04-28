from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .context import AdminHandlersContext


def register_admin_diagnostics_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.message(lambda m: m.text == "⚠️ Диагностика")
    async def handle_diagnostics_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        await message.reply("⚠️ Раздел: Диагностика", reply_markup=ctx.get_diagnostics_menu())

    @dp.message(lambda m: (m.text or "").strip() == "⚠️ Проблемные доставки")
    async def handle_faulty(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        pages = await ctx.run_db(ctx.build_faulty_pages, 200)
        page = 0
        total_pages = len(pages)
        current = pages[page]
        await message.reply(current["text"], parse_mode="HTML", reply_markup=ctx.build_faulty_inline_keyboard(page, total_pages, current["delivery_id"]))

    @dp.message(lambda m: m.text == "📊 Журнал системы")
    async def handle_system_journal(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        pages = await ctx.run_db(ctx.build_system_journal_pages, 300)
        page = 0
        total_pages = len(pages)
        await message.reply(pages[page], parse_mode="HTML", reply_markup=ctx.build_system_journal_inline_keyboard(page, total_pages))

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
        await message.reply(
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
