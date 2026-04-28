from __future__ import annotations

import asyncio

from aiogram import Dispatcher
from aiogram.types import Message

from .context import AdminHandlersContext


def register_admin_channel_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.message(lambda m: m.text == "📡 Каналы")
    async def handle_channels_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        await message.reply("📡 Раздел: Каналы", reply_markup=ctx.get_channels_menu())

    @dp.message(lambda m: m.text in ("➕ Канал", "➕ Добавить канал", "➕ Добавить источник", "➕ Добавить получатель"))
    async def handle_add_channel(message: Message):
        if message.text == "➕ Добавить источник":
            ctx.user_states[message.from_user.id] = {"action": "choose_source_kind"}
            await message.reply("Выберите: канал или группа с темой", reply_markup=ctx.get_entity_kind_keyboard())
            return
        if message.text == "➕ Добавить получатель":
            ctx.user_states[message.from_user.id] = {"action": "choose_target_kind"}
            await message.reply("Выберите: канал или группа с темой", reply_markup=ctx.get_entity_kind_keyboard())
            return
        if not await ctx.is_admin(message):
            return
        await message.reply("Выберите тип записи", reply_markup=ctx.get_channel_type_keyboard())

    @dp.message(lambda m: m.text == "📤 Источник")
    async def handle_source_type(message: Message):
        if not ctx.is_admin_user(message.from_user.id if message.from_user else None):
            return
        ctx.user_states[message.from_user.id] = {"action": "choose_source_kind"}
        await message.reply("Выберите: канал или группа с темой", reply_markup=ctx.get_entity_kind_keyboard())

    @dp.message(lambda m: m.text == "📥 Получатель")
    async def handle_target_type(message: Message):
        if not ctx.is_admin_user(message.from_user.id if message.from_user else None):
            return
        ctx.user_states[message.from_user.id] = {"action": "choose_target_kind"}
        await message.reply("Выберите: канал или группа с темой", reply_markup=ctx.get_entity_kind_keyboard())

    @dp.message(lambda m: m.text in ("📺 Канал", "👥 Группа с темой"))
    async def handle_entity_kind(message: Message):
        state = ctx.user_states.get(message.from_user.id)
        if not state:
            return
        if state["action"] == "choose_source_kind":
            state["action"] = "add_source_channel" if message.text == "📺 Канал" else "add_source_group"
            await message.reply("Отправьте ID канала" if message.text == "📺 Канал" else "Отправьте ID группы", reply_markup=ctx.get_cancel_keyboard())
        elif state["action"] == "choose_target_kind":
            state["action"] = "add_target_channel" if message.text == "📺 Канал" else "add_target_group"
            await message.reply("Отправьте ID канала" if message.text == "📺 Канал" else "Отправьте ID группы", reply_markup=ctx.get_cancel_keyboard())

    async def resolve_chat_title(chat_id: str) -> str:
        chat = await ctx.bot.get_chat(chat_id)
        return chat.title or str(chat_id)

    @dp.message(lambda m: m.text and m.text.startswith("-100"))
    async def handle_chat_id_inputs(message: Message):
        state = ctx.user_states.get(message.from_user.id)
        if not state:
            return
        chat_id = (message.text or "").strip()
        action = state.get("action")

        if action in {"add_source_channel", "add_target_channel"}:
            channel_type = "source" if action == "add_source_channel" else "target"
            try:
                title = await resolve_chat_title(chat_id)
                exists = await ctx.run_db(ctx.db.channel_exists, chat_id, None, channel_type)
                if exists:
                    await message.reply("Такая запись уже есть", reply_markup=ctx.get_main_menu())
                else:
                    actor_id = message.from_user.id if message.from_user else ctx.settings.admin_id
                    if ctx.is_admin_user(actor_id):
                        created = await ctx.run_db(ctx.db.add_channel, chat_id, None, channel_type, title, actor_id)
                    else:
                        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, actor_id)
                        created = await ctx.run_db(ctx.db.add_channel_for_tenant, tenant_id, chat_id, None, channel_type, title, actor_id)
                    if not created:
                        await message.reply("⚠️ Не удалось добавить запись в базу", reply_markup=ctx.get_main_menu())
                    else:
                        await message.reply(f"✅ Добавлен {'источник' if channel_type == 'source' else 'получатель'}: {title}", reply_markup=ctx.get_main_menu())
                        if channel_type == "source":
                            asyncio.create_task(ctx.parse_channel_history(ctx.telethon_client, ctx.db, chat_id, clean_start=False))
            except Exception as exc:
                ctx.logger.exception("Ошибка добавления канала | action=%s | chat_id=%s | error=%s", action, chat_id, exc)
                await message.reply(f"❌ Ошибка доступа к каналу/чату: {exc}", reply_markup=ctx.get_main_menu())
            finally:
                ctx.reset_user_state(message.from_user.id)
            return

        if action in {"add_source_group", "add_target_group"}:
            try:
                title = await resolve_chat_title(chat_id)
                state["chat_id"] = chat_id
                state["title"] = title
                state["action"] = "add_source_group_thread" if action == "add_source_group" else "add_target_group_thread"
                await message.reply("Теперь отправьте ID темы", reply_markup=ctx.get_cancel_keyboard())
            except Exception as exc:
                ctx.logger.exception("Ошибка доступа к группе перед вводом thread_id | action=%s | chat_id=%s | error=%s", action, chat_id, exc)
                await message.reply(f"❌ Не удалось получить доступ к группе: {exc}", reply_markup=ctx.get_main_menu())
                ctx.reset_user_state(message.from_user.id)
            return

    @dp.message(lambda m: m.text in ("📜 Список", "📜 Список каналов", "📜 Мои источники", "📜 Мои получатели"))
    async def handle_list_channels(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        user_id = message.from_user.id if message.from_user else ctx.settings.admin_id
        is_admin_mode = ctx.is_admin_user(user_id)
        if is_admin_mode:
            rows = await ctx.run_db(ctx.db.get_channels)
        else:
            tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
            channel_type = None
            if message.text == "📜 Мои источники":
                channel_type = "source"
            if message.text == "📜 Мои получатели":
                channel_type = "target"
            rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, channel_type) if hasattr(ctx.db, "get_channels_for_tenant") else []
            ctx.logger.info("пользователь открыл список каналов user_id=%s tenant_id=%s type=%s", user_id, tenant_id, channel_type or "all")
        if not rows:
            await message.reply("Нет каналов", reply_markup=ctx.get_main_menu())
            return
        text = "📜 **СПИСОК КАНАЛОВ**\n\n"
        for idx, row in enumerate(rows, 1):
            title = row["title"] or row["channel_id"]
            suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
            text += f"{idx}. [{row['channel_type']}] {title}{suffix}\n"
        await message.reply(text[:4000], parse_mode="Markdown", reply_markup=ctx.get_main_menu())

    @dp.message(lambda m: m.text in ("➖ Канал", "➖ Удалить канал", "➖ Удалить источник", "➖ Удалить получатель"))
    async def handle_remove_channel(message: Message):
        user_id = message.from_user.id if message.from_user else ctx.settings.admin_id
        is_admin_mode = ctx.is_admin_user(user_id)
        if is_admin_mode:
            rows = await ctx.run_db(ctx.db.get_channels)
            tenant_id = None
        else:
            tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
            channel_type = "source" if message.text == "➖ Удалить источник" else ("target" if message.text == "➖ Удалить получатель" else None)
            rows = await ctx.run_db(ctx.db.get_channels_for_tenant, tenant_id, channel_type) if hasattr(ctx.db, "get_channels_for_tenant") else []
        if not rows:
            await message.reply("Нет каналов", reply_markup=ctx.get_main_menu())
            return

        keyboard = []
        mapping = []
        text = "Выберите запись для удаления\n\n"
        for idx, row in enumerate(rows, 1):
            title = row["title"] or row["channel_id"]
            suffix = f" (тема {row['thread_id']})" if row["thread_id"] else ""
            keyboard.append([ctx.keyboard_button_cls(text=f"Удалить {idx}")])
            mapping.append((row["channel_id"], row["thread_id"], row["channel_type"]))
            text += f"{idx}. [{row['channel_type']}] {title}{suffix}\n"

        keyboard.append([ctx.keyboard_button_cls(text="❌ Отмена")])
        ctx.user_states[message.from_user.id] = {"action": "remove_channel", "mapping": mapping, "tenant_id": tenant_id}
        await message.reply(text[:4000], reply_markup=ctx.reply_keyboard_markup_cls(keyboard=keyboard, resize_keyboard=True))

    @dp.message(lambda m: m.text and m.text.startswith("Удалить "))
    async def handle_remove_selected(message: Message):
        state = ctx.user_states.get(message.from_user.id)
        if not state or state.get("action") != "remove_channel":
            return
        try:
            idx = int(message.text.split()[-1]) - 1
            channel_id, thread_id, channel_type = state["mapping"][idx]
            tenant_id = state.get("tenant_id")
            if tenant_id is None:
                await ctx.run_db(ctx.db.remove_channel, channel_id, thread_id, channel_type)
            else:
                await ctx.run_db(ctx.db.remove_channel_for_tenant, tenant_id, channel_id, thread_id, channel_type)
            await ctx.ensure_rule_workers()
            await message.reply("✅ Канал удалён", reply_markup=ctx.get_main_menu())
        except Exception as exc:
            await message.reply(f"❌ Ошибка удаления: {exc}", reply_markup=ctx.get_main_menu())
        finally:
            ctx.user_states.pop(message.from_user.id, None)
