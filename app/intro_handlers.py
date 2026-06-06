from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from aiogram import Dispatcher
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from app.repository import RepositoryProtocol


@dataclass(slots=True)
class IntroHandlersContext:
    db: RepositoryProtocol
    settings: Any
    logger: Any
    user_states: dict[int, dict[str, Any]]

    run_db: Callable[..., Awaitable[Any]]

    is_admin: Callable[[Message], Awaitable[bool]]
    is_admin_callback: Callable[[CallbackQuery], Awaitable[bool]]
    is_admin_user: Callable[[int | None], bool]
    ensure_rule_callback_access: Callable[[CallbackQuery, int], Awaitable[bool]]

    answer_callback_safe: Callable[..., Awaitable[bool]]
    answer_callback_safe_once: Callable[..., Awaitable[bool]]

    edit_message_text_safe: Callable[..., Awaitable[Any]]
    send_message_safe: Callable[..., Awaitable[Any]]
    send_photo_safe: Callable[..., Awaitable[Any]]
    send_video_safe: Callable[..., Awaitable[Any]]
    try_delete_message_safe: Callable[..., Awaitable[bool]]

    get_rule_stats_row_async: Callable[[int], Awaitable[Any]]
    build_rule_card_text: Callable[[Any], str]
    build_rule_card_keyboard: Callable[..., InlineKeyboardMarkup]
    filter_user_rule_card_keyboard: Callable[[InlineKeyboardMarkup, int], InlineKeyboardMarkup]

    invalidate_rule_card_cache: Callable[[int], None]
    cancel_reply_markup_for_user: Callable[[int | None], Any]


def _sanitize_intro_name(name: str | None) -> str | None:
    import re

    if not name:
        return None

    name = name.strip().lower()

    # Разрешаем буквы, цифры, пробел, _, -, кириллицу и латиницу
    name = re.sub(r"[^a-zA-Zа-яА-Я0-9 _-]", "", name)

    # Пробелы -> подчеркивания
    name = re.sub(r"\s+", "_", name)

    # Сжимаем повторяющиеся _
    name = re.sub(r"_+", "_", name)

    name = name.strip("_- ")

    return name[:60] if name else None


def _make_unique_intro_filename(base_name: str, extension: str, intros_dir: str) -> str:
    import os

    candidate = f"{base_name}.{extension}"
    full_path = os.path.join(intros_dir, candidate)

    if not os.path.exists(full_path):
        return candidate

    counter = 2
    while True:
        candidate = f"{base_name}_{counter}.{extension}"
        full_path = os.path.join(intros_dir, candidate)
        if not os.path.exists(full_path):
            return candidate
        counter += 1


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if not row:
        return default
    try:
        if hasattr(row, "keys") and key not in row.keys():
            return default
        return row[key]
    except Exception:
        return getattr(row, key, default)


def build_intro_list_keyboard(
    intros,
    rule_id: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="🖥 Выбрать горизонтальную",
                callback_data=f"video_intro_horizontal:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="📱 Выбрать вертикальную",
                callback_data=f"video_intro_vertical:{rule_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text="➕ Загрузить заставку",
                callback_data=f"intro_upload:{rule_id}",
            )
        ],
    ]

    if intros:
        rows.append([
            InlineKeyboardButton(
                text="📦 Мои заставки",
                callback_data=f"intro_back_to_list:{rule_id}",
            )
        ])
        for intro in intros:
            rows.append([
                InlineKeyboardButton(
                    text=f"👁 {intro.display_name}",
                    callback_data=f"intro_view:{rule_id}:{intro.id}",
                )
            ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад к правилу",
            callback_data=f"rule_card:{rule_id}",
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_intro_selection_keyboard(intros, rule_id: int, mode: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for intro in intros:
        rows.append([
            InlineKeyboardButton(
                text=f"✅ {intro.display_name}",
                callback_data=f"apply_intro:{mode}:{rule_id}:{intro.id}",
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="❌ Убрать",
            callback_data=f"apply_intro:{mode}:{rule_id}:none",
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="🎬 К заставкам правила",
            callback_data=f"video_intro_menu:{rule_id}",
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_empty_selection_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Загрузить заставку", callback_data=f"intro_upload:{rule_id}")],
            [InlineKeyboardButton(text="🎬 К заставкам правила", callback_data=f"video_intro_menu:{rule_id}")],
        ]
    )


def register_intro_handlers(dp: Dispatcher, ctx: IntroHandlersContext) -> None:
    def _apply_intro_sync(rule_id: int, mode: str, intro_id_val: int | None):
        if mode == "horizontal":
            ctx.db.set_rule_intro_horizontal(rule_id, intro_id_val)
        else:
            ctx.db.set_rule_intro_vertical(rule_id, intro_id_val)

        row = ctx.db.get_rule_card_snapshot(rule_id)
        if not row:
            return None

        horizontal_id = _row_get(row, "video_intro_horizontal_id")
        vertical_id = _row_get(row, "video_intro_vertical_id")

        enable_intro = bool(horizontal_id or vertical_id)
        ctx.db.set_rule_video_intro_enabled(rule_id, enable_intro)

        return ctx.db.get_rule_card_snapshot(rule_id)

    def _legacy_intro_name_sync(intro_id: int | None) -> str | None:
        if not intro_id:
            return None
        intro = ctx.db.get_intro_by_id(intro_id)
        return intro.display_name if intro else None

    def _clear_intro_assignment_sync(rule_id: int, intro_id: int) -> tuple[Any, bool]:
        row = ctx.db.get_rule_card_snapshot(rule_id)
        if not row:
            return None, False

        horizontal_id = _row_get(row, "video_intro_horizontal_id")
        vertical_id = _row_get(row, "video_intro_vertical_id")
        removed_assignment = False
        next_horizontal_id = horizontal_id
        next_vertical_id = vertical_id

        if horizontal_id == intro_id:
            ctx.db.set_rule_intro_horizontal(rule_id, None)
            next_horizontal_id = None
            removed_assignment = True
        if vertical_id == intro_id:
            ctx.db.set_rule_intro_vertical(rule_id, None)
            next_vertical_id = None
            removed_assignment = True

        if removed_assignment and not (next_horizontal_id or next_vertical_id):
            ctx.db.set_rule_video_intro_enabled(rule_id, False)

        return ctx.db.get_rule_card_snapshot(rule_id), removed_assignment

    async def _build_rule_intro_menu_response(rule_id: int) -> tuple[str, InlineKeyboardMarkup]:
        intros = await ctx.run_db(ctx.db.list_rule_intros, rule_id)
        row = await ctx.get_rule_stats_row_async(rule_id)

        horizontal_id = _row_get(row, "video_intro_horizontal_id")
        vertical_id = _row_get(row, "video_intro_vertical_id")
        intro_by_id = {intro.id: intro for intro in intros}
        legacy_notes: list[str] = []

        async def selected_label(intro_id: int | None, label: str) -> str:
            if not intro_id:
                return "не выбрана"
            rule_intro = intro_by_id.get(intro_id)
            if rule_intro:
                return rule_intro.display_name
            legacy_name = await ctx.run_db(_legacy_intro_name_sync, intro_id)
            if legacy_name:
                legacy_notes.append(
                    f"ℹ️ {label}: это старая общая заставка. Новые загрузки будут доступны только этому правилу."
                )
                return legacy_name
            return "не выбрана"

        horizontal_label = await selected_label(horizontal_id, "Горизонтальная")
        vertical_label = await selected_label(vertical_id, "Вертикальная")

        text_parts = [
            f"🎬 Заставки правила #{rule_id}",
            "",
            "Эти заставки доступны только этому правилу.",
            "",
            f"🖥 Горизонтальная: {horizontal_label}",
            f"📱 Вертикальная: {vertical_label}",
            f"📦 Загружено в правило: {len(intros)}",
        ]
        if legacy_notes:
            text_parts.extend(["", *legacy_notes])
        text_parts.extend([
            "",
            "Загрузите новую заставку или выберите, какую использовать для горизонтальных и вертикальных видео.",
        ])
        if not intros:
            text_parts.extend([
                "",
                "📦 В этом правиле пока нет заставок.",
                "",
                "Загрузите первую заставку, затем назначьте её горизонтальной или вертикальной.",
            ])

        return "\n".join(text_parts), build_intro_list_keyboard(intros, rule_id=rule_id)

    async def _show_rule_intro_menu(callback: CallbackQuery, rule_id: int) -> None:
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        text, reply_markup = await _build_rule_intro_menu_response(rule_id)

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=text,
                reply_markup=reply_markup,
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_video_intro_menu: %s", exc)

        ctx.user_states[callback.from_user.id] = {
            "action": "intro_menu",
            "rule_id": rule_id,
        }

        await ctx.answer_callback_safe_once(callback)

    async def _send_rule_intro_menu(callback: CallbackQuery, rule_id: int) -> None:
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        text, reply_markup = await _build_rule_intro_menu_response(rule_id)
        await ctx.send_message_safe(
            chat_id=callback.message.chat.id,
            text=text,
            reply_markup=reply_markup,
        )
        ctx.user_states[callback.from_user.id] = {
            "action": "intro_menu",
            "rule_id": rule_id,
        }
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("video_intro_menu:") or c.data.startswith("user_rule_intros:"))
    async def handle_video_intro_menu(callback: CallbackQuery):
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await _show_rule_intro_menu(callback, rule_id)

    @dp.callback_query(lambda c: c.data == "rule_back_from_intro")
    async def handle_intro_back(callback: CallbackQuery):
        state = ctx.user_states.get(callback.from_user.id)
        if not state:
            await ctx.answer_callback_safe(callback, "Ошибка состояния", show_alert=True)
            return

        rule_id = state.get("rule_id")
        if not await ctx.ensure_rule_callback_access(callback, int(rule_id or 0)):
            return

        row = await ctx.get_rule_stats_row_async(rule_id)
        if not row:
            await ctx.answer_callback_safe(callback, "Ошибка", show_alert=True)
            return

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=ctx.build_rule_card_text(row),
                parse_mode="HTML",
                reply_markup=(
                    ctx.build_rule_card_keyboard(
                        rule_id,
                        bool(row["is_active"]),
                        row["schedule_mode"] or "interval",
                        row["mode"] or "repost",
                    )
                    if ctx.is_admin_user(callback.from_user.id if callback.from_user else None)
                    else ctx.filter_user_rule_card_keyboard(
                        ctx.build_rule_card_keyboard(
                            rule_id,
                            bool(row["is_active"]),
                            row["schedule_mode"] or "interval",
                            row["mode"] or "repost",
                        ),
                        rule_id,
                    )
                ),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_intro_back: %s", exc)

        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data == "intro_upload" or c.data.startswith("intro_upload:"))
    async def handle_intro_upload(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        rule_id = None
        if callback.data.startswith("intro_upload:"):
            try:
                rule_id = int(callback.data.split(":")[1])
            except Exception:
                await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
                return
        else:
            prev_state = ctx.user_states.get(callback.from_user.id, {})
            rule_id = prev_state.get("rule_id")
            if not rule_id:
                await ctx.answer_callback_safe(
                    callback,
                    "Сессия устарела. Откройте заставки из карточки правила заново.",
                    show_alert=True,
                )
                return

        if not await ctx.ensure_rule_callback_access(callback, int(rule_id)):
            return

        ctx.user_states[callback.from_user.id] = {
            "action": "intro_upload_wait_file",
            "rule_id": int(rule_id),
            "flow": "rule_intro_upload",
        }

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=(
                    f"➕ Загрузка заставки для правила #{rule_id}\n\n"
                    "Заставка будет доступна только этому правилу.\n\n"
                    "Отправьте видео или изображение.\n"
                    "Название укажите в подписи к файлу.\n\n"
                    "Ограничения:\n"
                    "• видео до 30 секунд;\n"
                    "• изображение JPG/PNG;\n"
                    "• название: буквы, цифры, пробел, _ и -.\n\n"
                    "Пример подписи:\n"
                    "intro_horizontal_main"
                ),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_intro_upload: %s", exc)

        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("intro_view:"))
    async def handle_intro_view(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        try:
            _, rule_id_raw, intro_id_raw = callback.data.split(":")
            rule_id = int(rule_id_raw)
            intro_id = int(intro_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        intro = await ctx.run_db(ctx.db.get_rule_intro, rule_id, intro_id)

        if not intro:
            await ctx.answer_callback_safe(
                callback,
                "⚠️ Заставка не найдена в этом правиле.\n\nВозможно, она была удалена или принадлежит другому правилу.",
                show_alert=True,
            )
            return

        import os

        if not intro.file_path or not os.path.exists(intro.file_path):
            await ctx.answer_callback_safe(callback, "❌ Файл заставки не найден на диске", show_alert=True)
            return

        input_file = FSInputFile(intro.file_path)

        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🖥 Назначить горизонтальной", callback_data=f"apply_intro:horizontal:{rule_id}:{intro.id}"),
                ],
                [
                    InlineKeyboardButton(text="📱 Назначить вертикальной", callback_data=f"apply_intro:vertical:{rule_id}:{intro.id}"),
                ],
                [
                    InlineKeyboardButton(text="🗑 Удалить", callback_data=f"intro_delete_confirm:{rule_id}:{intro.id}"),
                ],
                [
                    InlineKeyboardButton(text="⬅️ К заставкам правила", callback_data=f"intro_back_to_list:{rule_id}"),
                ],
            ]
        )
        if intro.duration and intro.duration > 0:
            await ctx.send_video_safe(
                chat_id=callback.message.chat.id,
                video=input_file,
                caption=(
                    f"🎬 {intro.display_name}\n"
                    f"⏱ Длительность: {intro.duration} сек"
                ),
                reply_markup=reply_markup,
            )
        else:
            await ctx.send_photo_safe(
                chat_id=callback.message.chat.id,
                photo=input_file,
                caption=f"🖼 {intro.display_name}",
                reply_markup=reply_markup,
            )

        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data == "intro_back_to_list" or c.data.startswith("intro_back_to_list:"))
    async def handle_intro_back_to_list(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        if callback.data == "intro_back_to_list":
            state = ctx.user_states.get(callback.from_user.id, {})
            rule_id = state.get("rule_id")
        else:
            try:
                rule_id = int(callback.data.split(":")[1])
            except Exception:
                await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
                return

        if not rule_id:
            await ctx.answer_callback_safe(
                callback,
                "Сессия устарела. Откройте заставки из карточки правила заново.",
                show_alert=True,
            )
            return

        if not await ctx.ensure_rule_callback_access(callback, int(rule_id)):
            return

        try:
            await ctx.try_delete_message_safe(callback.message.chat.id, callback.message.message_id)
        except Exception:
            pass

        if callback.data.startswith("intro_back_to_list:"):
            await _send_rule_intro_menu(callback, int(rule_id))
        else:
            await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("intro_delete_confirm:"))
    async def handle_intro_delete_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        try:
            _, rule_id_raw, intro_id_raw = callback.data.split(":")
            rule_id = int(rule_id_raw)
            intro_id = int(intro_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        intro = await ctx.run_db(ctx.db.get_rule_intro, rule_id, intro_id)
        if not intro:
            await ctx.answer_callback_safe(callback, "⚠️ Заставка не найдена в этом правиле.", show_alert=True)
            return

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=(
                    "🗑 Удалить заставку?\n\n"
                    f"Правило #{rule_id}\n"
                    f"Заставка: {intro.display_name}\n\n"
                    "Заставка будет удалена только из этого правила.\n"
                    "Если она назначена как горизонтальная или вертикальная, назначение будет снято."
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Удалить", callback_data=f"intro_delete_apply:{rule_id}:{intro_id}")],
                        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"intro_delete_cancel:{rule_id}:{intro_id}")],
                    ]
                ),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_intro_delete_confirm: %s", exc)

        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("intro_delete_cancel:"))
    async def handle_intro_delete_cancel(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, rule_id_raw, intro_id_raw = callback.data.split(":")
            rule_id = int(rule_id_raw)
            int(intro_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await _show_rule_intro_menu(callback, rule_id)

    @dp.callback_query(lambda c: c.data.startswith("intro_delete_apply:"))
    async def handle_intro_delete_apply(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        try:
            _, rule_id_raw, intro_id_raw = callback.data.split(":")
            rule_id = int(rule_id_raw)
            intro_id = int(intro_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        intro = await ctx.run_db(ctx.db.get_rule_intro, rule_id, intro_id)
        if not intro:
            await ctx.answer_callback_safe(callback, "⚠️ Заставка не найдена в этом правиле.", show_alert=True)
            return

        _, removed_assignment = await ctx.run_db(_clear_intro_assignment_sync, rule_id, intro_id)
        ok = await ctx.run_db(ctx.db.soft_delete_rule_intro, rule_id, intro_id)

        if not ok:
            await ctx.answer_callback_safe(callback, "❌ Заставка уже удалена или не найдена", show_alert=True)
            return

        ctx.invalidate_rule_card_cache(rule_id)

        text = (
            f"✅ Заставка удалена из правила #{rule_id}\n\n"
            f"Название: {intro.display_name}"
        )
        if removed_assignment:
            text += "\n\nℹ️ Назначение этой заставки в правиле также снято."

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=text,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🎬 К заставкам правила", callback_data=f"video_intro_menu:{rule_id}")],
                        [InlineKeyboardButton(text="💰 К правилу", callback_data=f"rule_card:{rule_id}")],
                    ]
                ),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_intro_delete_apply: %s", exc)

        await ctx.answer_callback_safe_once(callback, "Удалено")

    @dp.callback_query(lambda c: c.data.startswith("video_intro_horizontal:"))
    async def handle_video_intro_horizontal(callback: CallbackQuery):
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        intros = await ctx.run_db(ctx.db.list_rule_intros, rule_id)

        if not intros:
            text = (
                "🖥 Горизонтальная заставка\n\n"
                "В этом правиле пока нет загруженных заставок.\n\n"
                "Сначала загрузите заставку, затем назначьте её горизонтальной."
            )
            markup = _build_empty_selection_keyboard(rule_id)
        else:
            text = (
                "🖥 Выберите горизонтальную заставку\n\n"
                "Доступны только заставки этого правила."
            )
            markup = _build_intro_selection_keyboard(intros, rule_id, "horizontal")

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=text,
                reply_markup=markup,
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_video_intro_horizontal: %s", exc)

        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("video_intro_vertical:"))
    async def handle_video_intro_vertical(callback: CallbackQuery):
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        intros = await ctx.run_db(ctx.db.list_rule_intros, rule_id)

        if not intros:
            text = (
                "📱 Вертикальная заставка\n\n"
                "В этом правиле пока нет загруженных заставок.\n\n"
                "Сначала загрузите заставку, затем назначьте её вертикальной."
            )
            markup = _build_empty_selection_keyboard(rule_id)
        else:
            text = (
                "📱 Выберите вертикальную заставку\n\n"
                "Доступны только заставки этого правила."
            )
            markup = _build_intro_selection_keyboard(intros, rule_id, "vertical")

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=text,
                reply_markup=markup,
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_video_intro_vertical: %s", exc)

        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("apply_intro:"))
    async def handle_apply_intro(callback: CallbackQuery):
        try:
            _, mode, rule_id_raw, intro_id_raw = callback.data.split(":")
            rule_id = int(rule_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        if mode not in {"horizontal", "vertical"}:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        try:
            intro_id_val = None if intro_id_raw == "none" else int(intro_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        intro = None
        if intro_id_val is not None:
            intro = await ctx.run_db(ctx.db.get_rule_intro, rule_id, intro_id_val)
            if intro is None:
                await ctx.answer_callback_safe(
                    callback,
                    "⚠️ Эта заставка не принадлежит выбранному правилу или была удалена.",
                    show_alert=True,
                )
                return

        row = await ctx.run_db(_apply_intro_sync, rule_id, mode, intro_id_val)
        if not row:
            await ctx.answer_callback_safe(callback, "Ошибка", show_alert=True)
            return

        ctx.invalidate_rule_card_cache(rule_id)

        if intro_id_val is None:
            success_text = (
                f"✅ {'Горизонтальная' if mode == 'horizontal' else 'Вертикальная'} заставка снята\n\n"
                f"Правило #{rule_id}"
            )
        else:
            success_text = (
                f"✅ {'Горизонтальная' if mode == 'horizontal' else 'Вертикальная'} заставка назначена\n\n"
                f"Правило #{rule_id}\n"
                f"Заставка: {intro.display_name}"
            )

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=success_text,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🎬 К заставкам правила", callback_data=f"video_intro_menu:{rule_id}")],
                        [InlineKeyboardButton(text="💰 К правилу", callback_data=f"rule_card:{rule_id}")],
                    ]
                ),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_apply_intro: %s", exc)

        await ctx.answer_callback_safe_once(callback, "Сохранено")

    @dp.message(
        lambda m:
            m.chat.type == "private"
            and m.from_user is not None
            and ctx.is_admin_user(m.from_user.id)
            and ctx.user_states.get(m.from_user.id, {}).get("action") == "intro_upload_wait_file"
            and (m.photo or m.video or m.document)
    )
    async def handle_intro_file(message: Message):
        state = ctx.user_states.get(message.from_user.id)
        if not state or state.get("action") != "intro_upload_wait_file":
            return

        if not await ctx.is_admin(message):
            return

        rule_id = state.get("rule_id")
        if not rule_id:
            await ctx.send_message_safe(
                chat_id=message.chat.id,
                text="Сессия устарела. Откройте заставки из карточки правила заново.",
                reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None),
            )
            return
        rule_id = int(rule_id)

        caption = (message.caption or "").strip()
        if not caption:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Укажи название заставки в подписи к файлу.\n\n"
                "Пример:\n"
                "intro_horizontal_main", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        safe_name = _sanitize_intro_name(caption)
        if not safe_name:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Некорректное название заставки.\n"
                "Разрешены буквы, цифры, пробел, _, -", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        media_kind = "document"
        if message.photo:
            tg_file = message.photo[-1]
            extension = "jpg"
            duration = 0
            media_kind = "photo"
        else:
            tg_file = message.video or message.document
            mime_type = (getattr(tg_file, "mime_type", None) or "").lower()

            if message.video:
                extension = "mp4"
                media_kind = "video"
            elif mime_type in {"image/jpeg", "image/jpg", "image/png"}:
                extension = "png" if mime_type == "image/png" else "jpg"
                media_kind = "document"
            elif mime_type.startswith("video/"):
                extension = "mp4"
                media_kind = "document"
            else:
                await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Поддерживаются только видео или изображения JPG/PNG для заставки", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
                return

            duration = int(getattr(tg_file, "duration", 0) or 0)

        if duration > 30:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Видео-заставка не должна быть длиннее 30 секунд", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        file_name = _make_unique_intro_filename(
            safe_name,
            extension,
            str(ctx.settings.intros_dir),
        )
        file_path = str(ctx.settings.intros_dir / file_name)

        try:
            telegram_file = await message.bot.get_file(tg_file.file_id)
            await message.bot.download_file(telegram_file.file_path, destination=file_path)
        except Exception as exc:
            await ctx.send_message_safe(chat_id=message.chat.id, text=f"❌ Не удалось скачать заставку: {exc}", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        try:
            intro_id = await ctx.run_db(
                ctx.db.add_rule_intro,
                rule_id=rule_id,
                display_name=caption,
                file_name=file_name,
                file_path=file_path,
                duration=duration,
                created_by=message.from_user.id,
                media_kind=media_kind,
            )
        except Exception as exc:
            try:
                import os
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass

            ctx.logger.exception("Ошибка добавления заставки правила #%s: %s", rule_id, exc)
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Такая заставка уже существует", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        if not intro_id:
            try:
                import os
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass

            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Такая заставка уже существует", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        await ctx.send_message_safe(
            chat_id=message.chat.id,
            text=(
                f"✅ Заставка добавлена в правило #{rule_id}\n\n"
                f"Название: {caption}\n"
                f"Тип: {media_kind}\n"
                f"Длительность: {duration} сек\n\n"
                "Теперь её можно назначить как горизонтальную или вертикальную."
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🖥 Назначить горизонтальной", callback_data=f"apply_intro:horizontal:{rule_id}:{intro_id}")],
                    [InlineKeyboardButton(text="📱 Назначить вертикальной", callback_data=f"apply_intro:vertical:{rule_id}:{intro_id}")],
                    [InlineKeyboardButton(text="🎬 К заставкам правила", callback_data=f"video_intro_menu:{rule_id}")],
                ]
            ),
        )

        ctx.user_states[message.from_user.id] = {
            "action": "intro_menu",
            "rule_id": rule_id,
        }
