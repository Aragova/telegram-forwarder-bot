from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message


@dataclass(frozen=True)
class UserChannelHandlersContext:
    db: Any
    user_states: dict[int, dict[str, Any]]
    run_db: Callable[..., Awaitable[Any]]
    ensure_user_tenant: Callable[[int], Awaitable[int]]
    ensure_rule_workers: Callable[[], Awaitable[None]]
    reset_user_state: Callable[[int | None], None]
    _is_admin_user: Callable[[int | None], bool]
    answer_callback_safe: Callable[..., Awaitable[Any]]
    answer_callback_safe_once: Callable[..., Awaitable[Any]]
    edit_message_text_safe: Callable[..., Awaitable[Any]]
    answer_user_inline_message: Callable[..., Awaitable[Any]]
    user_sources_keyboard: Callable[[], InlineKeyboardMarkup]
    user_targets_keyboard: Callable[[], InlineKeyboardMarkup]
    user_channels_keyboard: Callable[[], InlineKeyboardMarkup]
    user_channel_add_type_keyboard: Callable[[], InlineKeyboardMarkup]
    user_channel_add_entity_keyboard: Callable[[str], InlineKeyboardMarkup]
    user_channel_add_text_input_keyboard: Callable[[], InlineKeyboardMarkup]
    get_chat: Callable[[str], Awaitable[Any]]


async def handle_user_channel_state_message(
    message: Message,
    ctx: UserChannelHandlersContext,
) -> bool:
    user_id_safe = message.from_user.id if message.from_user else 0
    state = ctx.user_states.get(user_id_safe) or {}
    if state.get("action") != "awaiting_user_channel_id":
        return False

    tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id_safe)
    channel_type = str(state.get("channel_type") or "source")
    entity_kind = str(state.get("entity_kind") or "channel")
    chat_id = (message.text or "").strip()
    try:
        chat = await ctx.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception as exc:
        await ctx.answer_user_inline_message(
            message,
            f"❌ Не удалось получить доступ к каналу/чату: {exc}",
            reply_markup=ctx.user_channel_add_text_input_keyboard(),
        )
        return True
    if entity_kind == "forum_topic":
        state["action"] = "awaiting_user_channel_thread_id"
        state["chat_id"] = chat_id
        state["title"] = title
        await ctx.answer_user_inline_message(
            message,
            "Отправьте ID темы.",
            reply_markup=ctx.user_channel_add_text_input_keyboard(),
        )
        return True
    exists = await ctx.run_db(ctx.db.channel_exists, chat_id, None, channel_type)
    if exists:
        await ctx.answer_user_inline_message(message, "Такая запись уже есть", reply_markup=ctx.user_channels_keyboard())
        ctx.reset_user_state(user_id_safe)
        return True
    await ctx.run_db(ctx.db.add_channel_for_tenant, tenant_id, chat_id, None, channel_type, title, user_id_safe)
    ctx.reset_user_state(user_id_safe)
    await ctx.answer_user_inline_message(
        message,
        "✅ Канал добавлен",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📡 Мои каналы", callback_data="user_channels")],
                [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="user_sources_add")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="user_main")],
            ]
        ),
    )
    return True


def register_user_channel_handlers(dp: Dispatcher, ctx: UserChannelHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data == "user_sources_add")
    async def handle_user_sources_add_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        if callback.from_user:
            ctx.user_states[callback.from_user.id] = {"action": "user_channel_add_type"}
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="➕ Добавить канал\n\nЧто вы хотите добавить?",
            reply_markup=ctx.user_channel_add_type_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_channels_add")
    async def handle_user_channels_add_callback(callback: CallbackQuery):
        await handle_user_sources_add_callback(callback)

    @dp.callback_query(lambda c: c.data == "user_targets_add")
    async def handle_user_targets_add_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        if callback.from_user:
            ctx.user_states[callback.from_user.id] = {
                "action": "user_channel_add_entity_kind",
                "channel_type": "target",
            }
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="Выберите тип:",
            reply_markup=ctx.user_channel_add_entity_keyboard("target"),
        )

    @dp.callback_query(lambda c: c.data in ("user_sources_remove", "user_targets_remove"))
    async def handle_user_channel_remove_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        channel_type = "source" if callback.data == "user_sources_remove" else "target"
        rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, channel_type) if hasattr(ctx.db, "get_channels_for_tenant") else []
        await ctx.answer_callback_safe_once(callback)
        if not rows:
            title = "📡 Источники" if channel_type == "source" else "🎯 Получатели"
            kb = ctx.user_sources_keyboard() if channel_type == "source" else ctx.user_targets_keyboard()
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=f"{title}\n\nСписок пока пуст.",
                reply_markup=kb,
            )
            return

        rows_kb = []
        mapping = []
        for idx, row in enumerate(rows):
            title = row["title"] or row["channel_id"]
            suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
            rows_kb.append([InlineKeyboardButton(text=f"🗑 {title}{suffix}", callback_data=f"user_channel_remove_pick:{idx}")])
            mapping.append((row["channel_id"], row["thread_id"], row["channel_type"], title, suffix))
        rows_kb.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="user_channels")])
        if callback.from_user:
            ctx.user_states[callback.from_user.id] = {"action": "user_channel_remove_pick", "mapping": mapping, "tenant_id": tenant_id}
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="🗑 Выберите канал для удаления",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows_kb),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_channel_add_type:"))
    async def handle_user_channel_add_type_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        channel_type = (callback.data or "").split(":", 1)[1]
        user_id = callback.from_user.id if callback.from_user else 0
        ctx.user_states[user_id] = {"action": "user_channel_add_entity_kind", "channel_type": channel_type}
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="Выберите тип:",
            reply_markup=ctx.user_channel_add_entity_keyboard(channel_type),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_channel_add_entity:"))
    async def handle_user_channel_add_entity_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        parts = (callback.data or "").split(":")
        if len(parts) != 3:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        _, channel_type, entity_kind = parts
        user_id = callback.from_user.id if callback.from_user else 0
        ctx.user_states[user_id] = {
            "action": "awaiting_user_channel_id",
            "channel_type": channel_type,
            "entity_kind": entity_kind,
        }
        await ctx.answer_user_inline_message(
            callback.message,
            "Отправьте ID канала или username.\n\nПример:\n@channel_name\nили\n-1001234567890",
            reply_markup=ctx.user_channel_add_text_input_keyboard(),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_channel_remove_pick:"))
    async def handle_user_channel_remove_pick_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        state = ctx.user_states.get(user_id) or {}
        if state.get("action") != "user_channel_remove_pick":
            await ctx.answer_callback_safe(callback, "Сессия устарела", show_alert=True)
            return
        try:
            idx = int((callback.data or "").split(":", 1)[1])
            channel_id, thread_id, channel_type, title, suffix = state.get("mapping", [])[idx]
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        state["action"] = "user_channel_remove_confirm"
        state["remove_selection"] = (channel_id, thread_id, channel_type, title, suffix)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=f"🗑 Удалить канал?\n\n{title}{suffix}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🗑 Да, удалить", callback_data="user_channel_remove_confirm:1")],
                    [InlineKeyboardButton(text="⬅️ Отмена", callback_data="user_channel_remove_cancel")],
                ]
            ),
        )

    @dp.callback_query(lambda c: c.data == "user_channel_remove_cancel")
    async def handle_user_channel_remove_cancel_callback(callback: CallbackQuery):
        await ctx.answer_callback_safe_once(callback)
        ctx.reset_user_state(callback.from_user.id if callback.from_user else None)
        await ctx.edit_message_text_safe(message=callback.message, text="📡 Мои каналы", reply_markup=ctx.user_channels_keyboard())

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_channel_remove_confirm:"))
    async def handle_user_channel_remove_confirm_callback(callback: CallbackQuery):
        if ctx._is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        state = ctx.user_states.get(user_id) or {}
        if state.get("action") != "user_channel_remove_confirm":
            await ctx.answer_callback_safe(callback, "Сессия устарела", show_alert=True)
            return
        channel_id, thread_id, channel_type, _, _ = state.get("remove_selection")
        tenant_id = state.get("tenant_id")
        await ctx.run_db(ctx.db.remove_channel_for_tenant, tenant_id, channel_id, thread_id, channel_type)
        await ctx.ensure_rule_workers()
        ctx.reset_user_state(user_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="✅ Канал удалён",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📡 Мои каналы", callback_data="user_channels")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="user_main")],
                ]
            ),
        )
