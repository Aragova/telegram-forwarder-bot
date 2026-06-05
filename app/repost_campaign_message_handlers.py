from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext
from app.repost_campaign_context import build_repost_campaign_runtime
from app.repost_campaign_context import build_repost_campaign_scheduled_post_service
from app.repost_campaign_schedule_service import parse_campaign_schedule_input_to_utc
from app.repost_campaign_ui import (
    build_repost_campaign_targets_check_result_view,
    build_repost_campaign_schedule_preview_view,
    build_repost_campaign_schedule_wizard_step3_view,
    build_vip_scheduled_post_preview_view,
    build_vip_scheduled_post_wizard_targets_view,
)
from app.saved_posts_service import (
    build_saved_post_album_content_from_aiogram_messages,
    build_saved_post_content_from_aiogram_message,
    get_saved_post_short_description,
    summarize_aiogram_message_for_saved_post,
    summarize_saved_post_entities,
)
import re


def is_waiting_vip_scheduled_post_material(ctx: RepostCampaignHandlersContext, user_id: int) -> bool:
    state = ctx.user_states.get(user_id, {})
    return state.get('state') == 'waiting_vip_scheduled_post_material'


async def _save_vip_scheduled_post_material(
    ctx: RepostCampaignHandlersContext,
    *,
    admin_id: int,
    rule_id: int,
    scheduled_post_id: int,
    content_json: dict,
    source_message,
) -> int | None:
    saved_post_id = await ctx.run_db(
        ctx.db.create_saved_post,
        rule_id=rule_id,
        title=None,
        content=content_json,
        source_chat_id=str(source_message.chat.id) if source_message.chat else None,
        source_message_id=source_message.message_id,
        source_media_group_id=str(getattr(source_message, "media_group_id", "") or "") or None,
        created_by=admin_id,
    )
    if not saved_post_id:
        return None
    service = build_repost_campaign_scheduled_post_service(ctx)
    result = await ctx.run_db(
        service.update_draft_saved_post,
        scheduled_post_id=scheduled_post_id,
        saved_post_id=int(saved_post_id),
        actor_id=admin_id,
    )
    if not result.ok:
        return None
    return int(saved_post_id)


async def _open_vip_scheduled_post_step_targets_message(
    ctx: RepostCampaignHandlersContext,
    *,
    message,
    rule_id: int,
    scheduled_post_id: int,
    prefix_text: str,
) -> None:
    service = build_repost_campaign_scheduled_post_service(ctx)
    row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, scheduled_post_id)
    targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, scheduled_post_id)
    readiness = await ctx.run_db(service.build_readiness, scheduled_post_id=scheduled_post_id)
    text_step2, kb_step2 = build_vip_scheduled_post_wizard_targets_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=readiness or {})
    await ctx.send_message_safe(chat_id=message.chat.id, text=f"{prefix_text}\n\n{text_step2}", reply_markup=kb_step2)


async def handle_vip_scheduled_post_material_message(ctx: RepostCampaignHandlersContext, message) -> bool:
    user_id = message.from_user.id if message.from_user else None
    state = ctx.user_states.get(user_id, {})
    if state.get("state") != "waiting_vip_scheduled_post_material":
        return False
    rule_id = int(state.get("rule_id") or 0)
    scheduled_post_id = int(state.get("scheduled_post_id") or 0)
    admin_id = message.from_user.id if message.from_user else None
    if rule_id <= 0 or scheduled_post_id <= 0 or admin_id is None:
        ctx.reset_user_state(user_id)
        await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось определить черновик запланированного поста")
        return True

    if getattr(message, "media_group_id", None):
        async def _on_album_ready(*, admin_id: int, messages: list):
            state_now = ctx.user_states.get(admin_id, {})
            if state_now.get("state") != "waiting_vip_scheduled_post_material":
                return
            rule_id_now = int(state_now.get("rule_id") or 0)
            scheduled_post_id_now = int(state_now.get("scheduled_post_id") or 0)
            if rule_id_now <= 0 or scheduled_post_id_now <= 0:
                ctx.reset_user_state(admin_id)
                await messages[-1].answer("❌ Не удалось определить черновик запланированного поста")
                return
            try:
                content_json = build_saved_post_album_content_from_aiogram_messages(messages)
            except Exception:
                await messages[-1].answer("❌ Не удалось сохранить альбом. Попробуйте отправить его ещё раз.")
                return
            saved_post_id = await _save_vip_scheduled_post_material(
                ctx,
                admin_id=admin_id,
                rule_id=rule_id_now,
                scheduled_post_id=scheduled_post_id_now,
                content_json=content_json,
                source_message=messages[-1],
            )
            if not saved_post_id:
                await messages[-1].answer("❌ Не удалось сохранить рекламный альбом")
                return
            ctx.reset_user_state(admin_id)
            await _open_vip_scheduled_post_step_targets_message(
                ctx,
                message=messages[-1],
                rule_id=rule_id_now,
                scheduled_post_id=scheduled_post_id_now,
                prefix_text="✅ Рекламный альбом сохранён.",
            )

        is_new_album = await ctx.saved_post_album_buffer.add_message(admin_id=admin_id, message=message, on_album_ready=_on_album_ready)
        if is_new_album:
            await ctx.send_message_safe(chat_id=message.chat.id, text="📎 Получаю альбом… Подождите пару секунд.")
        return True

    content_json = build_saved_post_content_from_aiogram_message(message)
    saved_post_id = await _save_vip_scheduled_post_material(
        ctx,
        admin_id=admin_id,
        rule_id=rule_id,
        scheduled_post_id=scheduled_post_id,
        content_json=content_json,
        source_message=message,
    )
    if not saved_post_id:
        await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось сохранить рекламный пост")
        return True
    ctx.reset_user_state(user_id)
    await _open_vip_scheduled_post_step_targets_message(
        ctx,
        message=message,
        rule_id=rule_id,
        scheduled_post_id=scheduled_post_id,
        prefix_text="✅ Рекламный пост сохранён.",
    )
    return True



async def _finalize_repost_campaign_saved_post_album(ctx: RepostCampaignHandlersContext, *, admin_id: int, messages: list) -> None:
    state = ctx.user_states.get(admin_id) or {}
    if state.get("state") != "awaiting_repost_campaign_saved_post":
        ctx.logger.info("SAVED_POST_ALBUM_FINALIZE_SKIPPED | admin_id=%s | reason=state_changed", admin_id)
        return
    rule_id = int(state.get("rule_id") or 0)
    if rule_id <= 0 or not messages:
        return
    content_json = build_saved_post_album_content_from_aiogram_messages(messages)
    first = sorted(messages, key=lambda m: int(getattr(m, "message_id", 0) or 0))[0]
    saved_post_id = await ctx.run_db(
        ctx.db.create_saved_post,
        rule_id=rule_id,
        title=None,
        content=content_json,
        source_chat_id=str(first.chat.id) if first.chat else None,
        source_message_id=first.message_id,
        source_media_group_id=getattr(first, "media_group_id", None),
        created_by=admin_id,
    )
    if not saved_post_id:
        await ctx.send_message_safe(chat_id=admin_id, text="❌ Не удалось сохранить рекламный альбом")
        return
    await ctx.run_db(ctx.db.set_rule_repost_campaign_saved_post, rule_id, int(saved_post_id))
    ctx.reset_user_state(admin_id)
    ctx.invalidate_rule_card_cache(rule_id)
    await ctx.send_message_safe(chat_id=admin_id, text="✅ Альбом сохранён как рекламный пост\n\n"
            f"Медиа: {len(content_json.get('media_items') or [])}\n"
            "Форматирование подписи сохранено.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👁 Предпросмотр поста", callback_data=f"rule_repost_campaign_post_preview:{rule_id}")],
            [InlineKeyboardButton(text="💰 К рекламной кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ К рекламному посту", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        ]))


async def _handle_repost_campaign_saved_post_input(ctx: RepostCampaignHandlersContext, message, state: dict) -> bool:
    rule_id = int(state.get("rule_id") or 0)
    user_id = message.from_user.id if message.from_user else None
    message_summary = summarize_aiogram_message_for_saved_post(message)
    ctx.logger.info("SAVED_POST_INCOMING_MESSAGE | rule_id=%s | summary=%s", rule_id, message_summary)
    if getattr(message, "media_group_id", None):
        await ctx.saved_post_album_buffer.add_message(
            admin_id=message.from_user.id,
            message=message,
            on_album_ready=lambda **kwargs: _finalize_repost_campaign_saved_post_album(ctx, **kwargs),
        )
        return True

    content_json = build_saved_post_content_from_aiogram_message(message)
    entity_summary = summarize_saved_post_entities(content_json)
    ctx.logger.info(
        "SAVED_POST_CAPTURED | rule_id=%s | kind=%s | text_len=%s | caption_len=%s | entity_summary=%s",
        rule_id,
        content_json.get("kind"),
        len(content_json.get("text") or ""),
        len(content_json.get("caption") or ""),
        entity_summary,
    )
    saved_post_id = await ctx.run_db(
        ctx.db.create_saved_post,
        rule_id=rule_id,
        title=None,
        content=content_json,
        source_chat_id=str(message.chat.id) if message.chat else None,
        source_message_id=message.message_id,
        source_media_group_id=getattr(message, "media_group_id", None),
        created_by=message.from_user.id if message.from_user else ctx.settings.admin_id,
    )
    if not saved_post_id:
        await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось сохранить рекламный пост")
        return True
    await ctx.run_db(ctx.db.set_rule_repost_campaign_saved_post, rule_id, int(saved_post_id))
    ctx.reset_user_state(user_id)
    ctx.invalidate_rule_card_cache(rule_id)
    ctx.logger.info(
        "REPOST_CAMPAIGN_SAVED_POST_ADDED | rule_id=%s | saved_post_id=%s | kind=%s",
        rule_id,
        saved_post_id,
        str(content_json.get("kind") or "text"),
    )
    kind_label = get_saved_post_short_description(content_json)
    kind = str(content_json.get("kind") or "text")
    caption_len = len(content_json.get("caption") or "")
    caption_entities_count = len(content_json.get("caption_entities") or [])
    is_media_kind = kind in {"photo", "video", "animation", "document"}

    footer = "Форматирование и premium emoji сохранены."
    if is_media_kind and caption_len == 0:
        footer = (
            "⚠️ Подпись у сохранённого медиа пустая.\n"
            "Если в оригинале была подпись, Telegram не передал её боту. "
            "Попробуйте отправить пост боту напрямую, а не пересылкой из канала."
        )
    elif is_media_kind and caption_entities_count == 0:
        footer = (
            "⚠️ Текст сохранён, но форматирование/premium emoji не были переданы Telegram.\n"
            "Для 1 в 1 сохранения попробуйте отправить пост боту напрямую."
        )

    await ctx.send_message_safe(chat_id=message.chat.id, text=f"✅ Рекламный пост сохранён\n\nID: #{saved_post_id}\nТип: {kind_label}\n\n{footer}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👁 Предпросмотр поста", callback_data=f"rule_repost_campaign_post_preview:{rule_id}")],
            [InlineKeyboardButton(text="💰 К рекламной кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ К рекламному посту", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        ]))
    return True


async def _handle_repost_campaign_targets_list_input(ctx: RepostCampaignHandlersContext, message, state: dict, text: str) -> bool:
    rule_id = int(state.get("rule_id") or 0)
    raw_lines = [line.strip() for line in text.splitlines()]
    total = len(raw_lines)
    unique_values: list[str] = []
    seen: set[str] = set()
    invalid = 0
    for value in raw_lines:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        is_valid = value.startswith("@") or value.startswith("-100") or bool(re.fullmatch(r"-\d+", value))
        if not is_valid:
            invalid += 1
            continue
        unique_values.append(value)
    added = 0
    skipped = 0
    for value in unique_values:
        row_id = await ctx.run_db(
            ctx.db.add_rule_repost_campaign_target,
            rule_id=rule_id,
            target_id=value,
            target_thread_id=None,
            title=value,
            created_by=message.from_user.id if message.from_user else ctx.settings.admin_id,
        )
        if row_id:
            added += 1
        else:
            skipped += 1
    try:
        await ctx.run_db(
            ctx.db.log_rule_change,
            event_type="repost_campaign_targets_list_added",
            rule_id=rule_id,
            admin_id=message.from_user.id if message.from_user else ctx.settings.admin_id,
            extra={"added": added, "skipped": skipped, "invalid": invalid, "total": total},
        )
    except Exception:
        ctx.logger.warning("Не удалось записать аудит добавления каналов кампании rule_id=%s", rule_id)
    ctx.reset_user_state(message.from_user.id if message.from_user else None)
    ctx.invalidate_rule_card_cache(rule_id)
    result_text = (
        "📥 Каналы обработаны\n\n"
        f"✅ Добавлено: {added}\n"
        f"⚠️ Уже были или пропущены: {skipped}\n"
        f"❌ Ошибки формата: {invalid}\n\n"
        f"Всего строк: {total}"
    )
    if added > 0:
        try:
            runtime = build_repost_campaign_runtime(ctx)
            check_result = await runtime.check_campaign_targets(
                rule_id=rule_id,
                active_only=False,
                admin_id=message.from_user.id if message.from_user else None,
                limit=50,
            )
            check_text, check_keyboard = build_repost_campaign_targets_check_result_view(rule_id=rule_id, result=check_result)
            await ctx.send_message_safe(chat_id=message.chat.id, text=f"{result_text}\n\n{check_text}", reply_markup=check_keyboard)
            return True
        except Exception as exc:
            ctx.logger.warning("REPOST_CAMPAIGN_TARGETS_AUTO_CHECK_FAILED | rule_id=%s | error=%s", rule_id, exc)
            result_text += "\n\nℹ️ Нажмите 🔎 Проверить права, чтобы подтянуть названия и готовность."
    await ctx.send_message_safe(chat_id=message.chat.id, text=result_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📋 К списку каналов", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")]]))
    return True

def _normalize_vip_scheduled_target_input(value: str) -> dict | None:
    raw = (value or "").strip()
    if not raw:
        return None
    normalized = raw
    if normalized.startswith("https://t.me/"):
        normalized = normalized[len("https://t.me/"):]
    elif normalized.startswith("http://t.me/"):
        normalized = normalized[len("http://t.me/"):]
    elif normalized.startswith("t.me/"):
        normalized = normalized[len("t.me/"):]
    normalized = normalized.strip().strip("/")
    target_id = None
    target_title = None
    if re.fullmatch(r"-100\d{6,}", normalized):
        target_id = normalized
        target_title = normalized
    else:
        username = normalized[1:] if normalized.startswith("@") else normalized
        if re.fullmatch(r"[A-Za-z0-9_]{4,}", username):
            target_id = f"@{username}"
            target_title = f"@{username}"
    if not target_id:
        return None
    return {"target_id": target_id, "target_thread_id": None, "target_title": target_title}


async def handle_repost_campaign_stateful_private_input(ctx: RepostCampaignHandlersContext, message, state: dict, text: str) -> bool:
    if state.get("state") == "awaiting_repost_campaign_saved_post":
        return await _handle_repost_campaign_saved_post_input(ctx, message, state)
    if state.get("action") == "awaiting_repost_campaign_targets_list":
        return await _handle_repost_campaign_targets_list_input(ctx, message, state, text)
    if state.get("state") == "waiting_vip_scheduled_post_target":
        rule_id = int(state.get("rule_id") or 0)
        scheduled_post_id = int(state.get("scheduled_post_id") or 0)
        parsed_target = _normalize_vip_scheduled_target_input(text)
        if not parsed_target:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось распознать канал/группу.\nОтправьте @channelname, t.me/channelname или -1001234567890")
            return True
        service = build_repost_campaign_scheduled_post_service(ctx)
        result = await ctx.run_db(
            service.add_manual_target,
            scheduled_post_id=scheduled_post_id,
            target_id=str(parsed_target["target_id"]),
            target_thread_id=parsed_target.get("target_thread_id"),
            target_title=parsed_target.get("target_title"),
            actor_id=message.from_user.id if message.from_user else None,
        )
        if not result.ok:
            await ctx.send_message_safe(chat_id=message.chat.id, text=f"❌ {result.error_text or 'Не удалось добавить канал/группу'}")
            return True
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, scheduled_post_id)
        targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, scheduled_post_id)
        ready = await ctx.run_db(service.build_readiness, scheduled_post_id=scheduled_post_id)
        t, k = build_vip_scheduled_post_wizard_targets_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=ready or {})
        await ctx.send_message_safe(chat_id=message.chat.id, text=t, reply_markup=k)
        return True
    if state.get("state") == "waiting_repost_campaign_scheduled_post_time":
        rule_id = int(state.get("rule_id") or 0)
        scheduled_post_id = int(state.get("scheduled_post_id") or 0)
        parsed = parse_campaign_schedule_input_to_utc(text)
        if parsed is None:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не понял дату и время.\nПример:\n10.05 18:00")
            return True
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.update_draft_scheduled_at, scheduled_post_id=scheduled_post_id, scheduled_at_utc=parsed, actor_id=message.from_user.id if message.from_user else None)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, scheduled_post_id)
        targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, scheduled_post_id)
        readiness = await ctx.run_db(service.build_readiness, scheduled_post_id=scheduled_post_id)
        text_preview, kb_preview = build_vip_scheduled_post_preview_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=readiness or {})
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await ctx.send_message_safe(chat_id=message.chat.id, text=text_preview, reply_markup=kb_preview)
        return True
    if state.get("state") != "repost_campaign_schedule_input":
        return False
    rule_id = int(state.get("rule_id") or 0)
    parsed = parse_campaign_schedule_input_to_utc(text)
    if parsed is None:
        await ctx.send_message_safe(chat_id=message.chat.id, text=
            "❌ Не удалось распознать дату и время.\n\nВведите в формате:\n09.05 18:00\n\nЧасовой пояс: UTC+3",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к выбору времени", callback_data=f"rule_repost_campaign_schedule_step4:{rule_id}")]]
            ),
        )
        return True
    runtime = build_repost_campaign_runtime(ctx)
    readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
    if int(readiness.get("show_seconds") or 0) <= 0:
        text_step3, kb_step3 = build_repost_campaign_schedule_wizard_step3_view(rule_id=rule_id, readiness=readiness)
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await ctx.send_message_safe(chat_id=message.chat.id, text=text_step3, reply_markup=kb_step3)
        return True
    text_preview, kb_preview = build_repost_campaign_schedule_preview_view(rule_id=rule_id, readiness=readiness, scheduled_at_utc=parsed)
    ctx.reset_user_state(message.from_user.id if message.from_user else None)
    await ctx.send_message_safe(chat_id=message.chat.id, text=text_preview, reply_markup=kb_preview)
    return True


def register_repost_campaign_message_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    @dp.message(
        lambda m: (
            m.chat.type == "private"
            and m.from_user
            and is_waiting_vip_scheduled_post_material(ctx, m.from_user.id)
        )
    )
    async def handle_vip_scheduled_post_material_registered(message):
        ctx.logger.info(
            "VIP_SCHEDULED_POST_MATERIAL_HANDLER_HIT | user_id=%s | content_type=%s | media_group_id=%s",
            message.from_user.id if message.from_user else None,
            getattr(message, "content_type", None),
            getattr(message, "media_group_id", None),
        )
        handled = await handle_vip_scheduled_post_material_message(ctx, message)
        if not handled:
            ctx.logger.warning(
                "VIP_SCHEDULED_POST_MATERIAL_HANDLER_NOT_HANDLED | user_id=%s",
                message.from_user.id if message.from_user else None,
            )
