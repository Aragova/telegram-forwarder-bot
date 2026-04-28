from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .context import AdminHandlersContext


def _sources_inline_keyboard(ctx: AdminHandlersContext, sources) -> InlineKeyboardMarkup:
    rows = []
    for idx, src in enumerate(sources):
        label = f"{src.title} [{src.channel_id}]" + (f" • тема {src.thread_id}" if src.thread_id else "")
        rows.append([InlineKeyboardButton(text=(label[:57] + "...") if len(label) > 60 else label, callback_data=f"reset_source:{idx}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="reset_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def register_admin_queue_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.message(lambda m: m.text == "📦 Очередь")
    async def handle_queue_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        await message.reply("📦 Раздел: Очередь", reply_markup=ctx.get_queue_menu())

    @dp.message(lambda m: m.text in ("📋 Очередь", "📋 Общая очередь"))
    async def handle_queue(message: Message):
        if not await ctx.is_admin(message):
            return
        stats = await ctx.run_db(ctx.db.get_queue_stats)
        await message.reply(f"📋 Очередь\n\n⏳ Pending: {stats['pending']}\n✅ Sent: {stats['sent']}\n⚠️ Faulty: {stats['faulty']}", reply_markup=ctx.get_main_menu())

    @dp.message(lambda m: m.text == "🔄 Сброс")
    async def handle_reset_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        await message.reply("Меню сброса", reply_markup=ctx.get_reset_queue_menu())

    @dp.message(lambda m: m.text == "🔄 Сбросить всё")
    async def handle_reset_all(message: Message):
        if not await ctx.is_admin(message):
            return
        count, faulty = await ctx.run_db(ctx.db.reset_all_deliveries)
        await message.reply(f"✅ Сброшено доставок: {count}\n⚠️ Faulty раньше было: {faulty}", reply_markup=ctx.get_main_menu())

    @dp.message(lambda m: m.text == "📊 Сброс по источнику")
    async def handle_reset_source_pick(message: Message):
        if not await ctx.is_admin(message):
            return
        source_rows = await ctx.run_db(ctx.db.get_channels, "source")
        sources = [ctx.channel_choice_cls(r["channel_id"], r["thread_id"], r["title"] or r["channel_id"]) for r in source_rows]
        if not sources:
            await message.reply("Нет источников", reply_markup=ctx.get_main_menu())
            return
        ctx.user_states[message.from_user.id] = {"action": "reset_source_inline", "sources": sources}
        await message.reply("Выберите источник для сброса:", reply_markup=_sources_inline_keyboard(ctx, sources))

    @dp.callback_query(lambda c: c.data == "reset_back")
    async def handle_reset_back(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text="Меню сброса:\n\n• 🔄 Сбросить всё\n• 📊 Сброс по источнику")

    @dp.callback_query(lambda c: c.data and c.data.startswith("reset_source:"))
    async def handle_reset_source_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        state = ctx.user_states.get(callback.from_user.id)
        if not state or state.get("action") != "reset_source_inline":
            await ctx.answer_callback_safe(callback, "Список устарел", show_alert=True)
            return
        try:
            idx = int((callback.data or "").split(":")[1])
            choice = state["sources"][idx]
            count = await ctx.run_db(ctx.db.reset_source_deliveries, choice.channel_id, choice.thread_id)
            await ctx.answer_callback_safe_once(callback)
            await ctx.edit_message_text_safe(message=callback.message, text=f"✅ Сброшено доставок: {count}")
        except Exception as exc:
            await ctx.answer_callback_safe_once(callback)
            await ctx.edit_message_text_safe(message=callback.message, text=f"❌ Ошибка сброса: {exc}")
        finally:
            ctx.user_states.pop(callback.from_user.id, None)
