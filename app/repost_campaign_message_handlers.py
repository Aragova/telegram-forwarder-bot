from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext
from app.repost_campaign_context import build_repost_campaign_runtime
from app.repost_campaign_context import build_repost_campaign_scheduled_post_service
from app.repost_campaign_schedule_service import parse_campaign_schedule_input_to_utc
from app.repost_campaign_ui import (
    build_repost_campaign_schedule_preview_view,
    build_repost_campaign_schedule_wizard_step3_view,
    build_vip_scheduled_post_preview_view,
    build_vip_scheduled_post_wizard_targets_view,
)
from app.saved_posts_service import build_saved_post_album_content_from_aiogram_messages, build_saved_post_content_from_aiogram_message
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
    await message.answer(f"{prefix_text}\n\n{text_step2}", reply_markup=kb_step2)


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
        await message.answer("❌ Не удалось определить черновик запланированного поста")
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
            await message.answer("📎 Получаю альбом… Подождите пару секунд.")
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
        await message.answer("❌ Не удалось сохранить рекламный пост")
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
    if state.get("state") == "waiting_vip_scheduled_post_target":
        rule_id = int(state.get("rule_id") or 0)
        scheduled_post_id = int(state.get("scheduled_post_id") or 0)
        parsed_target = _normalize_vip_scheduled_target_input(text)
        if not parsed_target:
            await message.answer("❌ Не удалось распознать канал/группу.\nОтправьте @channelname, t.me/channelname или -1001234567890")
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
            await message.answer(f"❌ {result.error_text or 'Не удалось добавить канал/группу'}")
            return True
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, scheduled_post_id)
        targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, scheduled_post_id)
        ready = await ctx.run_db(service.build_readiness, scheduled_post_id=scheduled_post_id)
        t, k = build_vip_scheduled_post_wizard_targets_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=ready or {})
        await message.answer(t, reply_markup=k)
        return True
    if state.get("state") == "waiting_repost_campaign_scheduled_post_time":
        rule_id = int(state.get("rule_id") or 0)
        scheduled_post_id = int(state.get("scheduled_post_id") or 0)
        parsed = parse_campaign_schedule_input_to_utc(text)
        if parsed is None:
            await message.answer("❌ Не понял дату и время.\nПример:\n10.05 18:00")
            return True
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.update_draft_scheduled_at, scheduled_post_id=scheduled_post_id, scheduled_at_utc=parsed, actor_id=message.from_user.id if message.from_user else None)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, scheduled_post_id)
        targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, scheduled_post_id)
        readiness = await ctx.run_db(service.build_readiness, scheduled_post_id=scheduled_post_id)
        text_preview, kb_preview = build_vip_scheduled_post_preview_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=readiness or {})
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await message.answer(text_preview, reply_markup=kb_preview)
        return True
    if state.get("state") != "repost_campaign_schedule_input":
        return False
    rule_id = int(state.get("rule_id") or 0)
    parsed = parse_campaign_schedule_input_to_utc(text)
    if parsed is None:
        await message.answer(
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
        await message.answer(text_step3, reply_markup=kb_step3)
        return True
    text_preview, kb_preview = build_repost_campaign_schedule_preview_view(rule_id=rule_id, readiness=readiness, scheduled_at_utc=parsed)
    ctx.reset_user_state(message.from_user.id if message.from_user else None)
    await message.answer(text_preview, reply_markup=kb_preview)
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
