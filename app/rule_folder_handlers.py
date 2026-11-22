"""Inline folder manager for both owner and tenant users.

Only UI and folder assignments are changed. Delivery, parser, scheduler and worker
interfaces remain untouched.
"""
from __future__ import annotations

from dataclasses import dataclass
from html import escape
from time import monotonic
from typing import Any

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, KeyboardButton, Message, ReplyKeyboardMarkup

from app.rule_folders_repository import normalize_folder_name
from app.rule_folders_ui import (
    _button,
    additional_functions_view,
    folder_delete_confirmation_view,
    folder_index_view,
    folder_settings_view,
    move_picker_view,
    root_view,
    rule_catalog_view,
)

# These are navigation/input caches only. Persistent assignments stay in PostgreSQL.
_searches: dict[int, str] = {}
_origins: dict[tuple[int, int], tuple[int, int, bool]] = {}
_selected: dict[int, tuple[int, set[int]]] = {}
_inputs: dict[int, dict[str, Any]] = {}
_pending_new_rule: dict[int, tuple[int, float]] = {}


def remember_new_rule_folder(user_id: int, folder_id: int) -> None:
    _pending_new_rule[int(user_id)] = (int(folder_id), monotonic())


def clear_new_rule_folder(user_id: int) -> None:
    _pending_new_rule.pop(int(user_id), None)


def consume_new_rule_folder(user_id: int) -> int | None:
    pending = _pending_new_rule.pop(int(user_id), None)
    if not pending or monotonic() - pending[1] > 900:
        return None
    return pending[0]


async def _actor(ctx: Any, user_id: int) -> tuple[int, bool]:
    tenant_id = await ctx.run_db(ctx.ensure_user_tenant, int(user_id))
    return int(tenant_id), bool(ctx.is_admin_user(user_id))


async def _render(ctx: Any, callback: CallbackQuery, text: str, kb: InlineKeyboardMarkup) -> None:
    await ctx.answer_callback_safe_once(callback)
    await ctx.edit_message_text_safe(
        message=callback.message, text=text, reply_markup=kb, parse_mode="HTML",
    )


async def _error(ctx: Any, callback: CallbackQuery, message: str) -> None:
    await ctx.answer_callback_safe_once(callback, message, show_alert=True)


async def _root(ctx: Any, callback: CallbackQuery, tenant_id: int, admin: bool) -> None:
    rules = await ctx.run_db(ctx.db.get_all_rules if admin else ctx.db.get_rules_for_tenant, *([] if admin else [tenant_id]))
    folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
    await _render(ctx, callback, *root_view(
        total_rules=len(rules), folders_count=len(folders), admin=admin,
    ))


async def _catalog(ctx: Any, callback: CallbackQuery, *, tenant_id: int, admin: bool,
                   folder_id: int, page: int, search: str | None = None, batch: bool = False):
    # Admin's legacy All rules continues to include every rule; folders are tenant-specific.
    if folder_id == -1 and admin and not batch:
        rules = await ctx.run_db(ctx.db.get_all_rules)
    else:
        rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
    folder_name = "Без папки"
    if folder_id >= 0:
        mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
        if folder_id > 0:
            folder = await ctx.run_db(ctx.db.get_rule_folder, tenant_id, folder_id)
            if not folder:
                await _error(ctx, callback, "Папка не найдена")
                return
            folder_name = str(folder["name"])
        rules = [
            r for r in rules
            if (int(mapping[int(r.id)]["id"]) if int(r.id) in mapping else 0) == folder_id
        ]
    if search is not None:
        needle = search.casefold()
        rules = [
            r for r in rules
            if needle in str(r.id).casefold()
            or needle in str(r.source_title or r.source_id).casefold()
            or needle in str(r.target_title or r.target_id).casefold()
            or needle in str(r.source_id).casefold()
            or needle in str(r.target_id).casefold()
        ]
    chosen = None
    if batch:
        selection = _selected.get(callback.from_user.id)
        if not selection or selection[0] != folder_id:
            selection = (folder_id, set())
            _selected[callback.from_user.id] = selection
        chosen = selection[1]
        # If a rule was moved by another action, prevent stale bulk selections.
        chosen.intersection_update({int(r.id) for r in rules})
    await _render(ctx, callback, *rule_catalog_view(
        rules, folder_id=folder_id, folder_name=folder_name, page=page,
        search_term=search, selected=chosen,
    ))


async def _extra(ctx: Any, callback: CallbackQuery, tenant_id: int, rule_id: int):
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return
    if int(await ctx.run_db(ctx.db.get_rule_tenant_id, rule_id)) != tenant_id:
        await _error(ctx, callback, "Папки доступны для правил вашего аккаунта")
        return
    rule = await ctx.run_db(ctx.db.get_rule, rule_id)
    if not rule:
        await _error(ctx, callback, "Правило не найдено")
        return
    mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
    folder = mapping.get(rule_id)
    await _render(ctx, callback, *additional_functions_view(
        rule_id=rule_id,
        target_title=str(rule.target_title or rule.target_id),
        mode=str(rule.mode or "repost"),
        folder_name=folder["name"] if folder else None,
        admin=bool(ctx.is_admin_user(callback.from_user.id)),
    ))


async def _move(ctx: Any, callback: CallbackQuery, *, tenant_id: int, origin: str, page: int = 0):
    folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
    if origin.startswith("s"):
        rule_id = int(origin[1:])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        if int(await ctx.run_db(ctx.db.get_rule_tenant_id, rule_id)) != tenant_id:
            await _error(ctx, callback, "Нельзя переместить правило другого аккаунта")
            return
        mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
        folder_name = mapping.get(rule_id, {}).get("name")
        count = 1
    elif origin.startswith("b"):
        selection = _selected.get(callback.from_user.id)
        if not selection or selection[0] != int(origin[1:]) or not selection[1]:
            await _error(ctx, callback, "Сначала выберите правила")
            return
        folder_id = int(origin[1:])
        current = next((f for f in folders if int(f["id"]) == folder_id), None)
        folder_name = current["name"] if current else None
        count = len(selection[1])
    else:
        await _error(ctx, callback, "Некорректная операция")
        return
    await _render(ctx, callback, *move_picker_view(
        folders=folders, active_name=folder_name, origin=origin,
        rule_count=count, page=page,
    ))


async def _apply_move(ctx: Any, callback: CallbackQuery, *, tenant_id: int,
                      origin: str, folder_id: int):
    if origin.startswith("s"):
        rule_id = int(origin[1:])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        rule_ids = [rule_id]
    elif origin.startswith("b"):
        choice = _selected.get(callback.from_user.id)
        if not choice or choice[0] != int(origin[1:]) or not choice[1]:
            await _error(ctx, callback, "Выбор правил устарел")
            return
        rule_ids = sorted(choice[1])
    else:
        await _error(ctx, callback, "Некорректная операция")
        return
    try:
        await ctx.run_db(ctx.db.assign_rules_to_folder, tenant_id, rule_ids,
                         folder_id if folder_id else None)
    except ValueError as exc:
        await _error(ctx, callback, str(exc))
        return
    if origin.startswith("s"):
        await _extra(ctx, callback, tenant_id, rule_ids[0])
    else:
        _selected.pop(callback.from_user.id, None)
        await _catalog(ctx, callback, tenant_id=tenant_id,
                       admin=ctx.is_admin_user(callback.from_user.id),
                       folder_id=folder_id if folder_id else 0, page=0)


def _input_prompt(kind: str, *, folder_id: int = 0, origin: str = ""):
    if kind == "search":
        return (
            "🔎 <b>ПОИСК ПРАВИЛА</b>\n\n"
            "Введите номер (#214), название источника или получателя "
            "либо часть названия.\n\n"
            "🔻 Отправьте поисковый запрос одним сообщением.",
            "rf:root",
        )
    if kind == "rename":
        return (
            "✏️ <b>ПЕРЕИМЕНОВАНИЕ ПАПКИ</b>\n\n"
            "Введите новое название (до 64 символов).\n"
            "Правила и их настройки останутся прежними.",
            f"rf:folder_settings:{folder_id}",
        )
    return (
        "📁 <b>НОВАЯ ПАПКА</b>\n\n"
        "Придумайте название для организации правил.\n\n"
        "✍️ Отправьте название длиной от 1 до 64 символов.",
        f"rf:move_back:{origin}" if origin else "rf:root",
    )


def register_rule_folder_handlers(dp: Dispatcher, ctx: Any) -> None:
    @dp.callback_query(lambda c: bool(c.data) and c.data.startswith("rf:"))
    async def rule_folder_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if not user_id:
            return
        tenant_id, admin = await _actor(ctx, user_id)
        data = (callback.data or "").split(":")
        action = data[1]
        # A back/cancel/navigation press must invalidate pending text input.
        if action not in {"create", "create_for", "rename", "search"}:
            _inputs.pop(user_id, None)
        # Only an active folder creation wizard may reserve a target folder.
        if action not in {"add", "add_in"}:
            clear_new_rule_folder(user_id)
        if action == "noop":
            await ctx.answer_callback_safe_once(callback)
            return
        try:
            if action == "root":
                _inputs.pop(user_id, None)
                await _root(ctx, callback, tenant_id, admin)
            elif action == "main":
                await ctx.answer_callback_safe_once(callback)
                if not admin:
                    await _root(ctx, callback, tenant_id, admin)
                else:
                    _inputs.pop(user_id, None)
                    await ctx.edit_message_text_safe(message=callback.message, text="📋 Главное меню")
                    await ctx.send_message_safe(chat_id=callback.message.chat.id,
                                                text="📋 Главное меню",
                                                reply_markup=ctx.get_main_menu())
            elif action == "folders":
                folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                await _render(ctx, callback, *folder_index_view(folders, int(data[2])))
            elif action == "list":
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin,
                               folder_id=int(data[2]), page=int(data[3]))
            elif action in {"open", "open_search"}:
                is_search = action == "open_search"
                rule_id = int(data[2])
                fid = -1 if is_search else int(data[3])
                page = int(data[3]) if is_search else int(data[4])
                if not await ctx.ensure_rule_callback_access(callback, rule_id):
                    return
                if fid >= 0:
                    mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
                    if (int(mapping.get(rule_id, {}).get("id") or 0)) != fid:
                        await _error(ctx, callback, "Правило уже перемещено")
                        return
                _origins[(user_id, rule_id)] = (fid, page, is_search)
                await ctx.answer_callback_safe_once(callback)
                await ctx.refresh_rule_card_for_actor(callback, rule_id)
            elif action == "back":
                rid = int(data[2])
                fid, page, was_search = _origins.get((user_id, rid), (-1, 0, False))
                term = _searches.get(user_id) if was_search else None
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin,
                               folder_id=fid, page=page, search=term)
            elif action == "search":
                text, back = _input_prompt("search")
                _inputs[user_id] = {"kind": "search", "chat_id": callback.message.chat.id,
                                    "message_id": callback.message.message_id}
                await _render(ctx, callback, text, InlineKeyboardMarkup(inline_keyboard=[
                    [_button("⬅️ Назад", back)],
                ]))
            elif action == "results":
                term = _searches.get(user_id)
                if not term:
                    await _error(ctx, callback, "Введите поисковый запрос ещё раз")
                    return
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin,
                               folder_id=-1, page=int(data[2]), search=term)
            elif action in {"create", "create_for", "rename"}:
                origin = data[2] if action == "create_for" else ""
                fid = int(data[2]) if action == "rename" else 0
                kind = "rename" if action == "rename" else "create"
                text, back = _input_prompt(kind, folder_id=fid, origin=origin)
                _inputs[user_id] = {"kind": kind, "folder_id": fid, "origin": origin,
                                    "chat_id": callback.message.chat.id,
                                    "message_id": callback.message.message_id}
                await _render(ctx, callback, text, InlineKeyboardMarkup(inline_keyboard=[
                    [_button("⬅️ Отмена", back)],
                ]))
            elif action == "folder_settings":
                fid = int(data[2])
                folder = next((f for f in await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                               if int(f["id"]) == fid), None)
                if not folder:
                    await _error(ctx, callback, "Папка не найдена")
                else:
                    await _render(ctx, callback, *folder_settings_view(folder))
            elif action == "delete_confirm":
                folder = await ctx.run_db(ctx.db.get_rule_folder, tenant_id, int(data[2]))
                if folder:
                    await _render(ctx, callback, *folder_delete_confirmation_view(folder))
                else:
                    await _error(ctx, callback, "Папка не найдена")
            elif action == "delete":
                if not await ctx.run_db(ctx.db.delete_rule_folder, tenant_id, int(data[2])):
                    await _error(ctx, callback, "Папка не найдена")
                    return
                folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                await _render(ctx, callback, *folder_index_view(folders, 0))
            elif action == "move":
                await _move(ctx, callback, tenant_id=tenant_id, origin="s" + data[2])
            elif action == "move_page":
                await _move(ctx, callback, tenant_id=tenant_id,
                            origin=data[2], page=int(data[3]))
            elif action == "move_back":
                origin = data[2]
                if origin.startswith("s"):
                    await _extra(ctx, callback, tenant_id, int(origin[1:]))
                else:
                    await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin,
                                   folder_id=int(origin[1:]), page=0, batch=True)
            elif action == "apply":
                await _apply_move(ctx, callback, tenant_id=tenant_id,
                                  origin=data[2], folder_id=int(data[3]))
            elif action == "batch":
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin,
                               folder_id=int(data[2]), page=int(data[3]), batch=True)
            elif action == "pick":
                fid, page, rid = int(data[2]), int(data[3]), int(data[4])
                selection = _selected.get(user_id)
                if not selection or selection[0] != fid:
                    await _error(ctx, callback, "Выбор устарел")
                    return
                # Selection is constrained to the current tenant and folder.
                own_rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
                mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
                match = any(int(r.id) == rid for r in own_rules)
                if fid >= 0:
                    match = match and int(mapping.get(rid, {}).get("id") or 0) == fid
                if not match:
                    await _error(ctx, callback, "Правило недоступно")
                    return
                if rid in selection[1]:
                    selection[1].remove(rid)
                else:
                    selection[1].add(rid)
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin,
                               folder_id=fid, page=page, batch=True)
            elif action == "batch_reset":
                _selected.pop(user_id, None)
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin,
                               folder_id=int(data[2]), page=int(data[3]), batch=True)
            elif action == "bulk_to":
                await _move(ctx, callback, tenant_id=tenant_id,
                            origin="b" + data[2])
            elif action in {"add", "add_in"}:
                await ctx.answer_callback_safe_once(callback)
                fid = int(data[2]) if action == "add_in" else 0
                if fid:
                    if not await ctx.run_db(ctx.db.get_rule_folder, tenant_id, fid):
                        await _error(ctx, callback, "Папка не найдена")
                        return
                    remember_new_rule_folder(user_id, fid)
                else:
                    clear_new_rule_folder(user_id)
                if admin:
                    source_rows = await ctx.run_db(ctx.db.get_channels, "source")
                    if not source_rows:
                        await _error(ctx, callback, "Сначала добавьте источник")
                        return
                    choices = [
                        ctx.channel_choice_cls(row["channel_id"], row["thread_id"],
                                               row["title"] or row["channel_id"])
                        for row in source_rows
                    ]
                    ctx.user_states[user_id] = {"action": "pick_rule_source", "sources": choices}
                    keyboard = ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text=f"📤 {i}. {s.title}")]
                            for i, s in enumerate(choices, 1)
                        ] + [[KeyboardButton(text="❌ Отмена")]],
                        resize_keyboard=True,
                    )
                    await ctx.edit_message_text_safe(message=callback.message,
                                                     text="➕ Выберите источник нового правила")
                    await ctx.send_message_safe(chat_id=callback.message.chat.id,
                                                text="📤 Выберите источник", reply_markup=keyboard)
                else:
                    # Reuse the existing user_rules_add wizard rather than cloning it.
                    await _render(ctx, callback,
                                  "➕ <b>НОВОЕ ПРАВИЛО</b>\n\n"
                                  "Выберите источник и получателя в существующем мастере.\n"
                                  "Новая связка будет добавлена в выбранную папку.",
                                  InlineKeyboardMarkup(inline_keyboard=[
                                      [_button("➕ Продолжить", "user_rules_add", style="success")],
                                      [_button("⬅️ Назад", f"rf:list:{fid}:0" if fid else "rf:root")],
                                  ]))
            else:
                await _error(ctx, callback, "Неизвестное действие")
        except (IndexError, ValueError, TypeError) as exc:
            ctx.logger.warning("RULE_FOLDER_UI_INVALID | action=%s user_id=%s error=%s", action, user_id, exc)
            await _error(ctx, callback, "Некорректные данные действия")

    @dp.message(lambda m: bool(getattr(m, "text", None))
                and bool(getattr(m, "from_user", None))
                and m.from_user.id in _inputs)
    async def folder_input(message: Message):
        user_id = int(message.from_user.id)
        state = _inputs.get(user_id)
        if not state:
            return
        tenant_id, admin = await _actor(ctx, user_id)
        raw = (message.text or "").strip()
        kind = state["kind"]
        if raw.startswith("/"):
            _inputs.pop(user_id, None)
            return
        if kind == "search":
            if not raw or len(raw) > 100:
                await ctx.send_message_safe(chat_id=message.chat.id,
                                            text="⚠️ Введите запрос длиной от 1 до 100 символов.")
                return
            _searches[user_id] = raw
            _inputs.pop(user_id, None)
            rules = await ctx.run_db(ctx.db.get_all_rules if admin else ctx.db.get_rules_for_tenant,
                                     *([] if admin else [tenant_id]))
            needle = raw.casefold()
            filtered = [r for r in rules if needle in str(r.id).casefold()
                        or needle in str(r.source_title or r.source_id).casefold()
                        or needle in str(r.target_title or r.target_id).casefold()
                        or needle in str(r.source_id).casefold()
                        or needle in str(r.target_id).casefold()]
            text, kb = rule_catalog_view(filtered, folder_id=-1, folder_name="",
                                         page=0, search_term=raw)
        else:
            try:
                name = normalize_folder_name(raw)
            except ValueError as exc:
                await ctx.send_message_safe(chat_id=message.chat.id, text=f"⚠️ {exc}. Повторите ввод.")
                return
            if kind == "rename":
                ok = await ctx.run_db(ctx.db.rename_rule_folder, tenant_id,
                                      int(state["folder_id"]), name)
                if not ok:
                    await ctx.send_message_safe(chat_id=message.chat.id,
                                                text="⚠️ Папка не найдена или такое название уже занято.")
                    return
                folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                folder = next(f for f in folders if int(f["id"]) == int(state["folder_id"]))
                text, kb = folder_settings_view(folder)
            else:
                fid = await ctx.run_db(ctx.db.create_rule_folder, tenant_id, name)
                if not fid:
                    await ctx.send_message_safe(chat_id=message.chat.id,
                                                text="⚠️ Такая папка уже существует. Выберите другое название.")
                    return
                origin = str(state.get("origin") or "")
                if origin:
                    if origin.startswith("s"):
                        ids = [int(origin[1:])]
                    else:
                        selection = _selected.get(user_id)
                        ids = sorted(selection[1]) if selection and selection[0] == int(origin[1:]) else []
                    if ids:
                        try:
                            await ctx.run_db(ctx.db.assign_rules_to_folder, tenant_id, ids, fid)
                        except ValueError as exc:
                            await ctx.send_message_safe(chat_id=message.chat.id,
                                                        text=f"⚠️ Папка создана, но правила не перемещены: {exc}")
                        _selected.pop(user_id, None)
                folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                text, kb = folder_index_view(folders, 0)
            _inputs.pop(user_id, None)
        # Remove the message with user input; preserve a single navigable UI screen.
        try:
            await ctx.get_bot().delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        try:
            await ctx.get_bot().edit_message_text(
                chat_id=state["chat_id"], message_id=state["message_id"],
                text=text, parse_mode="HTML", reply_markup=kb,
            )
        except Exception as exc:
            ctx.logger.warning("RULE_FOLDER_UI_EDIT_FAILED | user_id=%s error=%s", user_id, exc)
            await ctx.send_message_safe(chat_id=message.chat.id, text=text,
                                        parse_mode="HTML", reply_markup=kb)
