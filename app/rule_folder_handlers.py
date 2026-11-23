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
    folder_fill_candidates,
    folder_fill_view,
    folder_index_view,
    folder_settings_view,
    move_picker_view,
    root_view,
    rule_catalog_view,
    rule_matches_query,
    CATALOG_PAGE_SIZE,
)

# These are navigation/input caches only. Persistent assignments stay in PostgreSQL.
_searches: dict[int, str] = {}
_origins: dict[tuple[int, int], tuple[int, int, bool]] = {}
_selected: dict[int, tuple[int, set[int]]] = {}
_fill_selected: dict[int, tuple[int, set[int]]] = {}
_folder_index_page: dict[int, int] = {}
_inputs: dict[int, dict[str, Any]] = {}
_pending_new_rule: dict[int, tuple[int, float]] = {}


def remember_new_rule_folder(user_id: int, folder_id: int) -> None:
    _pending_new_rule[int(user_id)] = (int(folder_id), monotonic())


def clear_new_rule_folder(user_id: int) -> None:
    _pending_new_rule.pop(int(user_id), None)


def clear_folder_input(user_id: int | None) -> None:
    """Cancel in-progress search/name prompts when leaving their screen."""
    if user_id is not None:
        _inputs.pop(int(user_id), None)


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


async def _workspace_rules(ctx: Any, tenant_id: int, admin: bool, user_id: int):
    """Accessible rules for folder organization, including the owner's legacy rules."""
    if not admin:
        return await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
    ids = await ctx.run_db(
        ctx.db.list_folder_manageable_rule_ids, tenant_id, int(user_id),
    )
    all_rules = await ctx.run_db(ctx.db.get_all_rules)
    return [rule for rule in all_rules if int(rule.id) in ids]


async def _can_manage_folder_rule(
    ctx: Any, tenant_id: int, admin: bool, user_id: int, rule_id: int,
) -> bool:
    if admin:
        ids = await ctx.run_db(
            ctx.db.list_folder_manageable_rule_ids, tenant_id, int(user_id),
        )
        return int(rule_id) in ids
    return int(await ctx.run_db(ctx.db.get_rule_tenant_id, rule_id)) == tenant_id


async def _assign_for_actor(
    ctx: Any, tenant_id: int, admin: bool, user_id: int,
    ids: list[int], folder_id: int | None,
) -> int:
    """Only the hard-coded system administrator may organize legacy owner-created rules."""
    if admin:
        return await ctx.run_db(
            ctx.db.assign_rules_to_folder, tenant_id, ids, folder_id,
            owner_admin_id=int(user_id),
        )
    return await ctx.run_db(
        ctx.db.assign_rules_to_folder, tenant_id, ids, folder_id,
    )


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
        rules = [r for r in rules if rule_matches_query(r, search)]
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
        folders_page=_folder_index_page.get(callback.from_user.id, 0),
    ))


async def _fill_catalog(
    ctx: Any, callback: CallbackQuery, *, tenant_id: int, folder_id: int, page: int,
) -> None:
    """Select from all rules of this tenant, not from the destination folder."""
    folder = await ctx.run_db(ctx.db.get_rule_folder, tenant_id, folder_id)
    if not folder:
        await _error(ctx, callback, "Папка не найдена")
        return
    rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
    assignments = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
    candidates = folder_fill_candidates(rules, assignments, folder_id)
    choice = _fill_selected.get(callback.from_user.id)
    if choice is None or choice[0] != folder_id:
        choice = (folder_id, set())
        _fill_selected[callback.from_user.id] = choice
    choice[1].intersection_update({int(r.id) for r in candidates})
    await _render(ctx, callback, *folder_fill_view(
        candidates, folder_id=folder_id, folder_name=str(folder["name"]),
        page=page, selected=choice[1], assignments=assignments,
    ))


async def _selected_folder_rule_ids(
    ctx: Any, tenant_id: int, folder_id: int, selected_ids: set[int],
) -> list[int]:
    """Require every selected ID to be in this folder and owned by this tenant."""
    if not selected_ids:
        raise ValueError("Сначала выберите правила")
    rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
    mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
    allowed = {
        int(r.id) for r in rules
        if int(mapping.get(int(r.id), {}).get("id") or 0) == folder_id
    }
    if not selected_ids.issubset(allowed):
        raise ValueError("Список изменился. Повторите выбор правил")
    return sorted(selected_ids)


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
        try:
            rule_ids = await _selected_folder_rule_ids(
                ctx, tenant_id, int(origin[1:]), choice[1],
            ) if int(origin[1:]) > 0 else sorted(choice[1])
        except ValueError as exc:
            await _error(ctx, callback, str(exc))
            return
    else:
        await _error(ctx, callback, "Некорректная операция")
        return
    try:
        await ctx.run_db(ctx.db.assign_rules_to_folder, tenant_id, rule_ids,
                         folder_id if folder_id else None)
    except ValueError as exc:
        await _error(ctx, callback, str(exc))
        return
    except Exception:
        ctx.logger.exception("RULE_FOLDER_MOVE_FAILED | tenant_id=%s count=%s", tenant_id, len(rule_ids))
        await _error(ctx, callback, "Не удалось переместить правила. Повторите действие.")
        return
    if origin.startswith("s"):
        _origins[(callback.from_user.id, rule_ids[0])] = (
            folder_id if folder_id else 0, 0, False,
        )
        await _extra(ctx, callback, tenant_id, rule_ids[0])
    else:
        _selected.pop(callback.from_user.id, None)
        await _catalog(ctx, callback, tenant_id=tenant_id,
                       admin=ctx.is_admin_user(callback.from_user.id),
                       folder_id=folder_id if folder_id else 0, page=0)


def _input_prompt(kind: str, *, folder_id: int = 0, origin: str = "", page: int = 0):
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
            f"rf:folder_settings:{folder_id}:{page}",
        )
    return (
        "📁 <b>НОВАЯ ПАПКА</b>\n\n"
        "Придумайте название для организации правил.\n\n"
        "✍️ Отправьте название длиной от 1 до 64 символов.",
        f"rf:move_page:{origin}:{page}" if origin else "rf:root",
    )


def register_rule_folder_handlers(dp: Dispatcher, ctx: Any) -> None:
    @dp.callback_query(lambda c: bool(c.data) and c.data.startswith("rf:"))
    async def rule_folder_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if not user_id:
            return
        if getattr(callback.message.chat, "type", None) != "private":
            await ctx.answer_callback_safe_once(
                callback, "Управление правилами доступно только в личном чате", show_alert=True,
            )
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
                    clear_folder_input(user_id)
                    await ctx.try_delete_message_safe(
                        callback.message.chat.id, callback.message.message_id,
                    )
                    await ctx.send_message_safe(
                        chat_id=callback.message.chat.id,
                        text="📋 Главное меню",
                        reply_markup=ctx.get_main_menu(),
                    )
            elif action == "folders":
                requested_page = int(data[2])
                _folder_index_page[user_id] = requested_page
                folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                await _render(ctx, callback, *folder_index_view(folders, requested_page))
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
                ctx.user_states.pop(user_id, None)
                text, back = _input_prompt("search")
                _inputs[user_id] = {"kind": "search", "chat_id": callback.message.chat.id,
                                    "message_id": callback.message.message_id,
                                    "origin_message": callback.message}
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
                folder_page = int(data[3]) if action == "create_for" and len(data) > 3 else 0
                fid = int(data[2]) if action == "rename" else 0
                list_page = int(data[3]) if action == "rename" and len(data) > 3 else 0
                kind = "rename" if action == "rename" else "create"
                ctx.user_states.pop(user_id, None)
                text, back = _input_prompt(
                    kind, folder_id=fid, origin=origin,
                    page=list_page if kind == "rename" else folder_page,
                )
                _inputs[user_id] = {"kind": kind, "folder_id": fid, "origin": origin,
                                    "list_page": list_page,
                                    "chat_id": callback.message.chat.id,
                                    "message_id": callback.message.message_id,
                                    "origin_message": callback.message}
                await _render(ctx, callback, text, InlineKeyboardMarkup(inline_keyboard=[
                    [_button("⬅️ Отмена", back)],
                ]))
            elif action == "folder_settings":
                fid = int(data[2])
                page = int(data[3]) if len(data) > 3 else 0
                folder = next((f for f in await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                               if int(f["id"]) == fid), None)
                if not folder:
                    await _error(ctx, callback, "Папка не найдена")
                else:
                    await _render(ctx, callback, *folder_settings_view(folder, page=page))
            elif action == "delete_confirm":
                folder = await ctx.run_db(ctx.db.get_rule_folder, tenant_id, int(data[2]))
                if folder:
                    page = int(data[3]) if len(data) > 3 else 0
                    await _render(ctx, callback, *folder_delete_confirmation_view(folder, page=page))
                else:
                    await _error(ctx, callback, "Папка не найдена")
            elif action == "delete":
                if not await ctx.run_db(ctx.db.delete_rule_folder, tenant_id, int(data[2])):
                    await _error(ctx, callback, "Папка не найдена")
                    return
                folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                await _render(ctx, callback, *folder_index_view(
                    folders, _folder_index_page.get(user_id, 0),
                ))
            elif action == "move":
                await _move(ctx, callback, tenant_id=tenant_id, origin="s" + data[2])
            elif action == "unfile_one":
                rid = int(data[2])
                if not await ctx.ensure_rule_callback_access(callback, rid):
                    return
                if int(await ctx.run_db(ctx.db.get_rule_tenant_id, rid)) != tenant_id:
                    await _error(ctx, callback, "Нет доступа к правилу")
                    return
                assignments = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
                if rid not in assignments:
                    await _error(ctx, callback, "Это правило уже находится вне папок")
                    return
                await ctx.run_db(ctx.db.assign_rules_to_folder, tenant_id, [rid], None)
                _origins[(user_id, rid)] = (0, 0, False)
                await _extra(ctx, callback, tenant_id, rid)
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
            elif action == "fill":
                await _fill_catalog(
                    ctx, callback, tenant_id=tenant_id,
                    folder_id=int(data[2]), page=int(data[3]),
                )
            elif action == "fill_pick":
                fid, page, rid = int(data[2]), int(data[3]), int(data[4])
                choice = _fill_selected.get(user_id)
                if not choice or choice[0] != fid:
                    await _error(ctx, callback, "Список выбора устарел")
                    return
                rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
                mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
                eligible = {int(r.id) for r in folder_fill_candidates(rules, mapping, fid)}
                if rid not in eligible:
                    await _error(ctx, callback, "Это правило уже в папке или недоступно")
                    return
                if rid in choice[1]:
                    choice[1].remove(rid)
                else:
                    choice[1].add(rid)
                await _fill_catalog(ctx, callback, tenant_id=tenant_id, folder_id=fid, page=page)
            elif action == "fill_page_all":
                fid, page = int(data[2]), int(data[3])
                choice = _fill_selected.get(user_id)
                if not choice or choice[0] != fid:
                    await _error(ctx, callback, "Список выбора устарел")
                    return
                rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
                mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
                candidates = folder_fill_candidates(rules, mapping, fid)
                current = candidates[max(0, page) * CATALOG_PAGE_SIZE:(max(0, page) + 1) * CATALOG_PAGE_SIZE]
                if not current:
                    await _error(ctx, callback, "На странице нет правил")
                    return
                page_ids = {int(r.id) for r in current}
                if page_ids.issubset(choice[1]):
                    choice[1].difference_update(page_ids)
                else:
                    choice[1].update(page_ids)
                await _fill_catalog(ctx, callback, tenant_id=tenant_id, folder_id=fid, page=page)
            elif action == "fill_reset":
                fid, page = int(data[2]), int(data[3])
                _fill_selected.pop(user_id, None)
                await _fill_catalog(ctx, callback, tenant_id=tenant_id, folder_id=fid, page=page)
            elif action == "fill_apply":
                fid = int(data[2])
                choice = _fill_selected.get(user_id)
                if not choice or choice[0] != fid or not choice[1]:
                    await _error(ctx, callback, "Сначала выберите правила")
                    return
                if not await ctx.run_db(ctx.db.get_rule_folder, tenant_id, fid):
                    await _error(ctx, callback, "Папка не найдена")
                    return
                rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
                mapping = await ctx.run_db(ctx.db.get_rule_folder_map, tenant_id)
                eligible = {int(r.id) for r in folder_fill_candidates(rules, mapping, fid)}
                if not choice[1].issubset(eligible):
                    await _error(ctx, callback, "Часть правил изменила папку. Повторите выбор")
                    return
                ids = sorted(choice[1])
                await ctx.run_db(ctx.db.assign_rules_to_folder, tenant_id, ids, fid)
                _fill_selected.pop(user_id, None)
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin, folder_id=fid, page=0)
            elif action == "unfile":
                fid, page = int(data[2]), int(data[3])
                if fid <= 0:
                    await _error(ctx, callback, "Выберите обычную папку")
                    return
                choice = _selected.get(user_id)
                if not choice or choice[0] != fid:
                    await _error(ctx, callback, "Список выбора устарел")
                    return
                try:
                    ids = await _selected_folder_rule_ids(ctx, tenant_id, fid, choice[1])
                except ValueError as exc:
                    await _error(ctx, callback, str(exc))
                    return
                await ctx.run_db(ctx.db.assign_rules_to_folder, tenant_id, ids, None)
                _selected.pop(user_id, None)
                await _catalog(ctx, callback, tenant_id=tenant_id, admin=admin, folder_id=fid, page=page)
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
                    await ctx.answer_callback_safe_once(callback)
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
        except Exception:
            ctx.logger.exception("RULE_FOLDER_UI_FAILED | action=%s user_id=%s", action, user_id)
            await _error(ctx, callback, "Не удалось выполнить действие. Повторите попытку.")

    @dp.message(lambda m: bool(getattr(m, "text", None))
                and getattr(m.chat, "type", None) == "private"
                and bool(getattr(m, "from_user", None))
                and m.from_user.id in _inputs
                and _inputs[m.from_user.id].get("chat_id") == m.chat.id)
    async def folder_input(message: Message):
        user_id = int(message.from_user.id)
        state = _inputs.get(user_id)
        if not state or state.get("chat_id") != message.chat.id:
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
            filtered = [r for r in rules if rule_matches_query(r, raw)]
            text, kb = rule_catalog_view(filtered, folder_id=-1, folder_name="",
                                         page=0, search_term=raw)
        else:
            try:
                name = normalize_folder_name(raw)
            except ValueError as exc:
                await ctx.send_message_safe(chat_id=message.chat.id, text=f"⚠️ {exc}. Повторите ввод.")
                return
            if kind == "rename":
                try:
                    ok = await ctx.run_db(
                        ctx.db.rename_rule_folder, tenant_id, int(state["folder_id"]), name,
                    )
                except Exception:
                    ctx.logger.exception("RULE_FOLDER_RENAME_FAILED | tenant_id=%s", tenant_id)
                    await ctx.send_message_safe(
                        chat_id=message.chat.id, text="❌ Не удалось переименовать папку. Повторите попытку.",
                    )
                    return
                if not ok:
                    await ctx.send_message_safe(chat_id=message.chat.id,
                                                text="⚠️ Папка не найдена или такое название уже занято.")
                    return
                folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                folder = next(f for f in folders if int(f["id"]) == int(state["folder_id"]))
                text, kb = folder_settings_view(
                    folder, page=int(state.get("list_page") or 0),
                )
            else:
                try:
                    fid = await ctx.run_db(ctx.db.create_rule_folder, tenant_id, name)
                except Exception:
                    ctx.logger.exception("RULE_FOLDER_CREATE_FAILED | tenant_id=%s", tenant_id)
                    await ctx.send_message_safe(
                        chat_id=message.chat.id, text="❌ Не удалось создать папку. Повторите попытку.",
                    )
                    return
                if not fid:
                    await ctx.send_message_safe(chat_id=message.chat.id,
                                                text="⚠️ Такая папка уже существует. Выберите другое название.")
                    return
                origin = str(state.get("origin") or "")
                moved = False
                ids = []
                if origin:
                    if origin.startswith("s"):
                        ids = [int(origin[1:])]
                    elif origin.startswith("b"):
                        selection = _selected.get(user_id)
                        if selection and selection[0] == int(origin[1:]):
                            ids = sorted(selection[1])
                    if ids:
                        try:
                            await ctx.run_db(ctx.db.assign_rules_to_folder, tenant_id, ids, fid)
                            moved = True
                        except ValueError as exc:
                            await ctx.send_message_safe(
                                chat_id=message.chat.id,
                                text=f"⚠️ Папка создана, но правила не перемещены: {exc}",
                            )
                        except Exception:
                            ctx.logger.exception("RULE_FOLDER_NEW_MOVE_FAILED | tenant_id=%s", tenant_id)
                            await ctx.send_message_safe(
                                chat_id=message.chat.id,
                                text="⚠️ Папка создана, но перенос не удался. "
                                     "Откройте папку и повторите перемещение.",
                            )
                if moved and origin.startswith("s"):
                    _origins[(user_id, ids[0])] = (fid, 0, False)
                    rule = await ctx.run_db(ctx.db.get_rule, ids[0])
                    if rule:
                        text, kb = additional_functions_view(
                            rule_id=ids[0],
                            target_title=str(rule.target_title or rule.target_id),
                            mode=str(rule.mode or "repost"),
                            folder_name=name, admin=admin,
                        )
                    else:
                        folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                        _folder_index_page[user_id] = 0
                        text, kb = folder_index_view(folders, 0)
                elif moved and origin.startswith("b"):
                    _selected.pop(user_id, None)
                    own_rules = await ctx.run_db(ctx.db.get_rules_for_tenant, tenant_id)
                    text, kb = rule_catalog_view(
                        [r for r in own_rules if int(r.id) in set(ids)],
                        folder_id=fid, folder_name=name, page=0,
                    )
                else:
                    folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
                    _folder_index_page[user_id] = 0
                    text, kb = folder_index_view(folders, 0)
            _inputs.pop(user_id, None)
        # Remove the message with user input; preserve a single navigable UI screen.
        await ctx.try_delete_message_safe(message.chat.id, message.message_id)
        edited = await ctx.edit_message_text_safe(
            message=state["origin_message"], text=text,
            parse_mode="HTML", reply_markup=kb,
        )
        if not edited:
            ctx.logger.warning("RULE_FOLDER_UI_EDIT_FAILED | user_id=%s", user_id)
            await ctx.send_message_safe(
                chat_id=message.chat.id, text=text, parse_mode="HTML", reply_markup=kb,
            )
