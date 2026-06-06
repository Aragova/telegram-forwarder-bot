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


def build_intro_list_keyboard(
    intros,
    rule_id: int | None = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    if rule_id is not None:
        rows.append([
            InlineKeyboardButton(
                text="🖥 Горизонтальная",
                callback_data=f"video_intro_horizontal:{rule_id}",
            ),
            InlineKeyboardButton(
                text="📱 Вертикальная",
                callback_data=f"video_intro_vertical:{rule_id}",
            ),
        ])

    for intro in intros:
        rows.append([
            InlineKeyboardButton(
                text=f"👁 {intro.display_name}",
                callback_data=f"intro_view:{intro.id}",
            ),
            InlineKeyboardButton(
                text="🗑",
                callback_data=f"intro_delete:{intro.id}",
            ),
        ])

    rows.append([
        InlineKeyboardButton(
            text="➕ Загрузить заставку",
            callback_data="intro_upload",
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад к правилу",
            callback_data="rule_back_from_intro",
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def register_intro_handlers(dp: Dispatcher, ctx: IntroHandlersContext) -> None:
    def _apply_intro_sync(rule_id: int, mode: str, intro_id_val: int | None):
        if mode == "horizontal":
            ctx.db.set_rule_intro_horizontal(rule_id, intro_id_val)
        else:
            ctx.db.set_rule_intro_vertical(rule_id, intro_id_val)

        row = ctx.db.get_rule_card_snapshot(rule_id)
        if not row:
            return None

        horizontal_id = row["video_intro_horizontal_id"] if "video_intro_horizontal_id" in row.keys() else None
        vertical_id = row["video_intro_vertical_id"] if "video_intro_vertical_id" in row.keys() else None

        enable_intro = bool(horizontal_id or vertical_id)
        ctx.db.set_rule_add_intro(rule_id, enable_intro)

        return ctx.db.get_rule_card_snapshot(rule_id)

    @dp.callback_query(lambda c: c.data.startswith("video_intro_menu:") or c.data.startswith("user_rule_intros:"))
    async def handle_video_intro_menu(callback: CallbackQuery):
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        intros = await ctx.run_db(ctx.db.get_intros)

        text = (
            f"🎬 Управление заставками\n\n"
            f"Всего заставок: {len(intros)}\n\n"
            f"Выберите заставку для просмотра или удаления.\n"
            f"Или загрузите новую."
        )

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=text,
                reply_markup=build_intro_list_keyboard(intros, rule_id=rule_id),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_video_intro_menu: %s", exc)

        ctx.user_states[callback.from_user.id] = {
            "action": "intro_menu",
            "rule_id": rule_id,
        }

        await ctx.answer_callback_safe_once(callback)

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

    @dp.callback_query(lambda c: c.data == "intro_upload")
    async def handle_intro_upload(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        prev_state = ctx.user_states.get(callback.from_user.id, {})

        ctx.user_states[callback.from_user.id] = {
            "action": "intro_upload_wait_file",
            "rule_id": prev_state.get("rule_id"),
        }

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=(
                    "Отправьте видео или изображение заставки.\n\n"
                    "Название укажите сразу в подписи к файлу.\n\n"
                    "Пример подписи:\n"
                    "grom_vert"
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
            intro_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        intro = await ctx.run_db(ctx.db.get_intro, intro_id)

        if not intro:
            await ctx.answer_callback_safe(callback, "❌ Заставка не найдена", show_alert=True)
            return

        import os

        if not intro.file_path or not os.path.exists(intro.file_path):
            await ctx.answer_callback_safe(callback, "❌ Файл заставки не найден на диске", show_alert=True)
            return

        input_file = FSInputFile(intro.file_path)

        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="intro_back_to_list")]
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

    @dp.callback_query(lambda c: c.data == "intro_back_to_list")
    async def handle_intro_back_to_list(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        try:
            await ctx.try_delete_message_safe(callback.message.chat.id, callback.message.message_id)
        except Exception:
            pass

        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("intro_delete:"))
    async def handle_intro_delete(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return

        try:
            intro_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        ok = await ctx.run_db(ctx.db.delete_intro, intro_id)
        intros = await ctx.run_db(ctx.db.get_intros)

        if not ok:
            await ctx.answer_callback_safe(callback, "❌ Заставка уже удалена или не найдена", show_alert=True)
            return

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=(
                    f"🗑 Заставка удалена\n\n"
                    f"🎬 Управление заставками\n\n"
                    f"Всего заставок: {len(intros)}\n\n"
                    f"Выберите заставку для просмотра или удаления.\n"
                    f"Или загрузите новую."
                ),
                reply_markup=build_intro_list_keyboard(intros),
            )
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                ctx.logger.exception("Ошибка handle_intro_delete: %s", exc)

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

        intros = await ctx.run_db(ctx.db.get_intros)

        if not intros:
            await ctx.answer_callback_safe(callback, "Нет заставок", show_alert=True)
            return

        rows = []

        for intro in intros:
            rows.append([
                InlineKeyboardButton(
                    text=intro.display_name,
                    callback_data=f"apply_intro:horizontal:{rule_id}:{intro.id}",
                )
            ])

        rows.append([
            InlineKeyboardButton(
                text="❌ Убрать",
                callback_data=f"apply_intro:horizontal:{rule_id}:none",
            )
        ])

        rows.append([
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"rule_card:{rule_id}",
            )
        ])

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="🎬 Выбор горизонтальной заставки",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
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

        intros = await ctx.run_db(ctx.db.get_intros)

        if not intros:
            await ctx.answer_callback_safe(callback, "Нет заставок", show_alert=True)
            return

        rows = []

        for intro in intros:
            rows.append([
                InlineKeyboardButton(
                    text=intro.display_name,
                    callback_data=f"apply_intro:vertical:{rule_id}:{intro.id}",
                )
            ])

        rows.append([
            InlineKeyboardButton(
                text="❌ Убрать",
                callback_data=f"apply_intro:vertical:{rule_id}:none",
            )
        ])

        rows.append([
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"rule_card:{rule_id}",
            )
        ])

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="🎬 Выбор вертикальной заставки",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
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

        try:
            intro_id_val = None if intro_id_raw == "none" else int(intro_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return

        row = await ctx.run_db(_apply_intro_sync, rule_id, mode, intro_id_val)
        if not row:
            await ctx.answer_callback_safe(callback, "Ошибка", show_alert=True)
            return

        ctx.invalidate_rule_card_cache(rule_id)

        try:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=ctx.build_rule_card_text(row),
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
                parse_mode="HTML",
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

        caption = (message.caption or "").strip()
        if not caption:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Укажи название заставки в подписи к файлу.\n\n"
                "Пример:\n"
                "grom_vert", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        safe_name = _sanitize_intro_name(caption)
        if not safe_name:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Некорректное название заставки.\n"
                "Разрешены буквы, цифры, пробел, _, -", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        if message.photo:
            tg_file = message.photo[-1]
            extension = "jpg"
            duration = 0
        else:
            tg_file = message.video or message.document
            mime_type = (getattr(tg_file, "mime_type", None) or "").lower()

            if message.video:
                extension = "mp4"
            elif mime_type.startswith("image/"):
                extension = "jpg"
            elif mime_type.startswith("video/"):
                extension = "mp4"
            else:
                await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Поддерживаются только видео или изображения для заставки", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
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

        intro_id = await ctx.run_db(
            ctx.db.add_intro,
            display_name=caption,
            file_name=file_name,
            file_path=file_path,
            duration=duration,
        )

        if not intro_id:
            try:
                import os
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass

            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Такая заставка уже существует", reply_markup=ctx.cancel_reply_markup_for_user(message.from_user.id if message.from_user else None))
            return

        intros = await ctx.run_db(ctx.db.get_intros)

        await ctx.send_message_safe(chat_id=message.chat.id, text=f"✅ Заставка '{caption}' добавлена", reply_markup=build_intro_list_keyboard(intros))

        ctx.user_states[message.from_user.id] = {
            "action": "intro_menu",
            "rule_id": state.get("rule_id"),
        }
