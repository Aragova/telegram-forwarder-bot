"""Rich inline UI for rule folders and compact, fully readable rule lists."""
from __future__ import annotations

from html import escape
from math import ceil
import re

from aiogram.types import InlineKeyboardMarkup

from app.user_ui import build_button

CATALOG_PAGE_SIZE = 5
FOLDER_PAGE_SIZE = 7

def rule_matches_query(rule, query: str) -> bool:
    """A #123 lookup matches that rule exactly; text matches channel details."""
    needle = str(query or "").strip().casefold()
    if not needle:
        return False
    id_match = re.fullmatch(r"#\s*(\d+)", needle)
    if id_match:
        return int(rule.id) == int(id_match.group(1))
    fields = (
        rule.id,
        rule.source_title or rule.source_id,
        rule.target_title or rule.target_id,
        rule.source_id,
        rule.target_id,
    )
    return any(needle in str(value).casefold() for value in fields)



def _button(text: str, callback: str, *, style: str = "primary"):
    return build_button(text=text, callback_data=callback, style=style)


def _page_buttons(prefix: str, page: int, pages: int) -> list:
    if pages <= 1:
        return []
    return [
        _button("◀️", f"{prefix}:{page - 1}" if page > 0 else "rf:noop"),
        _button(f"{page + 1}/{pages}", "rf:noop"),
        _button("▶️", f"{prefix}:{page + 1}" if page + 1 < pages else "rf:noop"),
    ]


def _paginate(items: list, page: int, page_size: int) -> tuple[list, int, int]:
    pages = max(1, ceil(len(items) / page_size))
    page = max(0, min(int(page), pages - 1))
    return items[page * page_size:(page + 1) * page_size], page, pages


def root_view(*, total_rules: int, folders_count: int, admin: bool):
    text = (
        "🔄 <b>УПРАВЛЕНИЕ ПРАВИЛАМИ</b>\n\n"
        f"📊 <b>Всего правил:</b> {total_rules}\n"
        f"📁 <b>Создано папок:</b> {folders_count}\n\n"
        "🔻 Находите правила по номеру и названию канала, "
        "распределяйте их по папкам и управляйте публикациями."
    )
    rows = []
    if folders_count:
        rows.append([_button("📁 Мои папки", "rf:folders:0")])
    rows.extend([
        [_button("📜 Список правил", "rf:list:-1:0")],
        [_button("🔎 Найти правило", "rf:search")],
        [_button("➕ Добавить правило", "rf:add" if admin else "user_rules_add", style="success")],
        [_button("📁 Создать папку", "rf:create")],
        [_button("⬅️ Назад в меню", "rf:main" if admin else "user_main")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def folder_index_view(folders: list[dict], page: int):
    current, page, pages = _paginate(folders, page, FOLDER_PAGE_SIZE)
    lines = [
        "📁 <b>МОИ ПАПКИ</b>",
        "",
        f"🗂 <b>Всего папок:</b> {len(folders)}",
        "",
        "🔻 Выберите папку для просмотра правил.",
        "",
    ]
    rows = []
    for folder in current:
        fid = int(folder["id"])
        name = str(folder["name"])
        lines.append(f"📁 <b>{escape(name)}</b> · {int(folder['rules_count'])} правил")
        short = name if len(name) <= 28 else name[:27] + "…"
        rows.append([_button(f"📂 {short}", f"rf:list:{fid}:0")])
    nav = _page_buttons("rf:folders", page, pages)
    if nav:
        rows.append(nav)
    rows += [
        [_button("📂 Без папки", "rf:list:0:0")],
        [_button("➕ Создать папку", "rf:create", style="success")],
        [_button("⬅️ Назад к правилам", "rf:root")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


def rule_catalog_view(
    rules: list, *, folder_id: int, folder_name: str, page: int,
    search_term: str | None = None, selected: set[int] | None = None,
    folders_page: int = 0,
):
    current, page, pages = _paginate(rules, page, CATALOG_PAGE_SIZE)
    enabled = sum(bool(getattr(r, "is_active", False)) for r in rules)
    lines = [
        ("🔎 <b>РЕЗУЛЬТАТЫ ПОИСКА</b>" if search_term is not None
         else ("📜 <b>ВСЕ ПРАВИЛА</b>" if folder_id == -1
               else f"📁 <b>{escape(folder_name)}</b>")),
        "",
        f"📊 <b>Всего:</b> {len(rules)} · 🟢 {enabled} · ⏸ {len(rules) - enabled}",
    ]
    if search_term is not None:
        lines.append(f"🔎 <b>Запрос:</b> {escape(search_term)}")
    if selected is None:
        lines += ["", "🔻 Названия показаны полностью. Откройте правило кнопкой с его номером.", ""]
    else:
        lines += ["", "☑️ Отмечайте номера правил. Выбор сохраняется при смене страницы.", ""]
    rows = []
    number_buttons = []
    for rule in current:
        rid = int(rule.id)
        state = "🟢" if bool(rule.is_active) else "⏸"
        source = str(rule.source_title or rule.source_id)
        target = str(rule.target_title or rule.target_id)
        if rule.source_thread_id:
            source += f" (тема {rule.source_thread_id})"
        if rule.target_thread_id:
            target += f" (тема {rule.target_thread_id})"
        lines.extend([
            f"{state} <b>Правило #{rid}</b>",
            f"📤 <b>Источник:</b> {escape(source)}",
            f"📥 <b>Получатель:</b> {escape(target)}",
            "",
        ])
        if selected is not None:
            label = f"{'✅' if rid in selected else '☐'} #{rid}"
            action = f"rf:pick:{folder_id}:{page}:{rid}"
        else:
            label = f"{state} #{rid}"
            action = (f"rf:open_search:{rid}:{page}" if search_term is not None
                      else f"rf:open:{rid}:{folder_id}:{page}")
        number_buttons.append(_button(label, action))
        if len(number_buttons) == 2:
            rows.append(number_buttons)
            number_buttons = []
    if number_buttons:
        rows.append(number_buttons)
    if not current:
        lines.append("Правил пока нет. Вы можете добавить новые или переместить существующие.")
    nav_prefix = ("rf:batch" if selected is not None else
                  "rf:results" if search_term is not None else "rf:list")
    if nav_prefix in {"rf:list", "rf:batch"}:
        nav_prefix += f":{folder_id}"
    nav = _page_buttons(nav_prefix, page, pages)
    if nav:
        rows.append(nav)
    if selected is not None:
        if folder_id > 0:
            rows.append([_button(f"📂 Убрать из папки ({len(selected)})",
                                 f"rf:unfile:{folder_id}:{page}")])
            rows.append([_button(f"📁 Переместить в другую папку ({len(selected)})",
                                 f"rf:bulk_to:{folder_id}", style="success")])
        else:
            rows.append([_button(f"📁 Переместить выбранные ({len(selected)})",
                                 f"rf:bulk_to:{folder_id}", style="success")])
        rows.append([_button("❌ Сбросить выбор", f"rf:batch_reset:{folder_id}:{page}")])
        rows.append([_button("⬅️ К списку", f"rf:list:{folder_id}:{page}")])
    else:
        if search_term is None:
            if folder_id > 0:
                rows.append([_button("📥 Добавить существующие правила",
                                     f"rf:fill:{folder_id}:0", style="success")])
                if rules:
                    rows.append([_button("☑️ Выбрать несколько", f"rf:batch:{folder_id}:{page}")])
                rows.append([_button("➕ Создать новое правило", f"rf:add_in:{folder_id}")])
                rows.append([_button("⚙️ Управление папкой", f"rf:folder_settings:{folder_id}:{page}")])
            else:
                if rules:
                    rows.append([_button("☑️ Выбрать несколько", f"rf:batch:{folder_id}:{page}")])
        rows.append([_button("🔎 Поиск", "rf:search")])
        rows.append([_button("⬅️ Назад", "rf:search" if search_term is not None
                             else f"rf:folders:{folders_page}" if folder_id != -1
                             else "rf:root")])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(inline_keyboard=rows)



def folder_fill_candidates(rules: list, assignments: dict[int, dict], folder_id: int) -> list:
    """All accessible rules except those already assigned to the destination folder."""
    return [
        rule for rule in rules
        if int(assignments.get(int(rule.id), {}).get("id") or 0) != int(folder_id)
    ]


def folder_fill_view(
    candidates: list, *, folder_id: int, folder_name: str, page: int,
    selected: set[int], assignments: dict[int, dict],
):
    """Pick rules from the tenant's whole catalog, not from the empty destination."""
    current, page, pages = _paginate(candidates, page, CATALOG_PAGE_SIZE)
    lines = [
        "📥 <b>ДОБАВЛЕНИЕ СУЩЕСТВУЮЩИХ ПРАВИЛ</b>",
        "",
        f"📂 <b>Папка:</b> {escape(folder_name)}",
        f"📊 <b>Доступно для добавления:</b> {len(candidates)}",
        f"✅ <b>Выбрано:</b> {len(selected)}",
        "",
        "🔻 Выберите правила из общего списка. Можно отмечать на разных страницах.",
        "Если правило уже в другой папке, оно будет перенесено сюда.",
        "",
    ]
    rows = []
    pair = []
    for rule in current:
        rid = int(rule.id)
        source = str(rule.source_title or rule.source_id)
        target = str(rule.target_title or rule.target_id)
        owner_folder = assignments.get(rid, {}).get("name") or "Без папки"
        state = "🟢" if bool(rule.is_active) else "⏸"
        lines.extend([
            f"{state} <b>Правило #{rid}</b>",
            f"📤 <b>Источник:</b> {escape(source)}",
            f"📥 <b>Получатель:</b> {escape(target)}",
            f"📂 <b>Сейчас:</b> {escape(str(owner_folder))}",
            "",
        ])
        pair.append(_button(f"{'✅' if rid in selected else '☐'} #{rid}",
                            f"rf:fill_pick:{folder_id}:{page}:{rid}"))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    if not current:
        lines.append("📭 Все доступные правила уже находятся в этой папке.")
    if current:
        page_ids = {int(r.id) for r in current}
        all_checked = page_ids.issubset(selected)
        rows.append([_button(
            "☑️ Снять выбор на странице" if all_checked else "✅ Выбрать всю страницу",
            f"rf:fill_page_all:{folder_id}:{page}",
        )])
    nav = _page_buttons(f"rf:fill:{folder_id}", page, pages)
    if nav:
        rows.append(nav)
    if selected:
        rows.append([_button(f"✅ Добавить выбранные ({len(selected)})",
                             f"rf:fill_apply:{folder_id}", style="success")])
        rows.append([_button("❌ Сбросить выбор", f"rf:fill_reset:{folder_id}:{page}")])
    rows.append([_button("⬅️ Назад в папку", f"rf:list:{folder_id}:0")])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(inline_keyboard=rows)


def folder_settings_view(folder: dict, *, page: int = 0):
    name = escape(str(folder["name"]))
    fid = int(folder["id"])
    text = (
        "⚙️ <b>УПРАВЛЕНИЕ ПАПКОЙ</b>\n\n"
        f"📁 <b>{name}</b>\n"
        f"📊 <b>Правил:</b> {int(folder.get('rules_count') or 0)}\n\n"
        "✏️ <b>Переименовать</b> — изменить название без затрагивания правил.\n\n"
        "🗑 <b>Удалить папку</b> — переместить все правила в «Без папки». "
        "Сами правила, настройки и очереди останутся."
    )
    rows = [
        [_button("✏️ Переименовать", f"rf:rename:{fid}:{page}")],
        [_button("🗑 Удалить папку", f"rf:delete_confirm:{fid}:{page}", style="danger")],
        [_button("⬅️ Назад к папке", f"rf:list:{fid}:{page}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def folder_delete_confirmation_view(folder: dict, *, page: int = 0):
    fid = int(folder["id"])
    text = (
        "⚠️ <b>УДАЛИТЬ ПАПКУ?</b>\n\n"
        f"📁 <b>{escape(str(folder['name']))}</b>\n\n"
        "❗ Удалится только папка. Сами правила останутся рабочими "
        "и будут видны в разделе «Без папки»."
    )
    return text, InlineKeyboardMarkup(inline_keyboard=[
        [_button("🗑 Да, удалить папку", f"rf:delete:{fid}:{page}", style="danger")],
        [_button("⬅️ Отмена", f"rf:folder_settings:{fid}:{page}")],
    ])


def move_picker_view(
    *, folders: list[dict], active_name: str | None,
    origin: str, rule_count: int, page: int = 0,
):
    current, page, pages = _paginate(folders, page, FOLDER_PAGE_SIZE)
    lines = [
        "📁 <b>ПЕРЕМЕЩЕНИЕ ПРАВИЛ</b>",
        "",
        f"🔢 <b>Выбрано:</b> {rule_count}",
        f"📂 <b>Текущая папка:</b> {escape(active_name or 'Без папки')}",
        "",
        "🔻 Выберите папку назначения. Публикации и очередь не изменятся.",
        "",
    ]
    rows = []
    for folder in current:
        fid = int(folder["id"])
        name = str(folder["name"])
        lines.append(f"📁 {escape(name)}")
        short = name if len(name) <= 27 else name[:26] + "…"
        rows.append([_button(f"📁 {short}", f"rf:apply:{origin}:{fid}", style="success")])
    nav = _page_buttons(f"rf:move_page:{origin}", page, pages)
    if nav:
        rows.append(nav)
    rows.append([_button("📂 Без папки", f"rf:apply:{origin}:0")])
    rows.append([_button("➕ Создать новую папку", f"rf:create_for:{origin}:{page}")])
    rows.append([_button("⬅️ Назад", f"rf:move_back:{origin}")])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(inline_keyboard=rows)


def additional_functions_view(
    *, rule_id: int, target_title: str, mode: str,
    folder_name: str | None, admin: bool,
):
    mode = str(mode or "repost").strip().lower()
    is_video = mode == "video"
    text = "\n".join([
        "⚙️ <b>ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ</b>",
        "",
        f"🔖 <b>Правило #{rule_id}</b>",
        f"📥 <b>Получатель:</b> {escape(target_title)}",
        f"📁 <b>Папка:</b> {escape(folder_name or 'Без папки')}",
        "",
        "━━━━━━━━━━━━━━",
        "🎛 <b>НАСТРОЙКИ</b>",
        "",
        "🔁 <b>Режим</b> — переключение между репостом и видеоредактором.",
        ("🎞 <b>Длина видео</b> — продолжительность обрабатываемого фрагмента."
         if is_video and admin else "✍️ <b>Подпись</b> — собственный текст или оригинал публикации."),
        "",
        "━━━━━━━━━━━━━━",
        "⚡ <b>ПУБЛИКАЦИИ</b>",
        "",
        "🚀 <b>Отправить сейчас</b> — внеочередная публикация.",
        "🔢 <b>Начать с №</b> — выбрать точку продолжения очереди.",
        "",
        "━━━━━━━━━━━━━━",
        "🛠 <b>ОБСЛУЖИВАНИЕ</b>",
        "",
        "🔄 <b>Пересканировать</b> — проверить историю источника повторно.",
        "📜 <b>Логи</b> — история действий и ошибок.",
        "📁 <b>Папка</b> — перенести правило без сброса очереди.",
    ])
    if not admin:
        text += "\n⚙️ <b>Реакции</b> — параметры автоматических реакций."
    text += (
        "\n\n━━━━━━━━━━━━━━\n"
        "⚠️ <b>ОПАСНЫЕ ДЕЙСТВИЯ</b>\n\n"
        "⏪ <b>Откатить</b> — вернуть последнюю доставку в очередь.\n"
        "🗑 <b>Удалить</b> — удалить правило и связанные данные."
    )
    def cb(admin_name: str, user_name: str) -> str:
        return (admin_name if admin else user_name) + f":{int(rule_id)}"
    mode_cb = (
        f"toggle_rule_mode:{rule_id}" if admin else
        f"user_rule_switch_mode:{rule_id}:{'repost' if is_video else 'video'}"
    )
    rows = [[
        _button("🎛 Режим", mode_cb),
        _button("🎞 Длина видео" if is_video and admin else "✍️ Подпись",
                cb("rule_video_clip_duration", "user_rule_caption") if is_video
                else cb("repost_caption_menu", "user_rule_repost_caption")),
    ], [
        _button("⚡ Отправить", cb("trigger_now", "user_rule_send_now"), style="success"),
        _button("🔢 Начать с №", cb("start_from_number", "user_rule_start_from")),
    ], [
        _button("🔄 Пересканировать", cb("rescan_rule_menu", "user_rule_rescan")),
        _button("📜 Логи", cb("rule_logs", "user_rule_logs")),
    ], [
        _button("📁 Переместить в папку", f"rf:move:{rule_id}"),
    ]]
    if folder_name:
        rows.append([_button("📂 Убрать из папки", f"rf:unfile_one:{rule_id}")])
    if not admin:
        rows.append([_button("⚙️ Реакции", f"user_rule_reactions:{rule_id}")])
    rows += [[
        _button("⏪ Откатить", cb("rollback", "user_rule_rollback"), style="danger"),
        _button("🗑 Удалить", cb("delete_rule", "user_rule_delete"), style="danger"),
    ], [
        _button("⬅️ Назад к правилу", cb("rule_card", "user_rule_open")),
    ]]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)
