from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_scheduled_post_service
from app.repost_campaign_ui import (
    build_vip_scheduled_post_detail_view,
    build_vip_scheduled_post_preview_view,
    build_vip_scheduled_post_wizard_post_view,
    build_vip_scheduled_post_wizard_show_view,
    build_vip_scheduled_post_wizard_targets_view,
    build_vip_scheduled_post_wizard_time_view,
    build_vip_scheduled_posts_list_view,
    build_vip_scheduled_posts_screen_view,
)


def register_repost_campaign_scheduled_post_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    async def _open_vip_step_post(callback: CallbackQuery, rule_id: int, sid: int):
        service = build_repost_campaign_scheduled_post_service(ctx)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, sid)
        saved = await ctx.run_db(ctx.db.list_saved_posts, rule_id=rule_id, limit=10)
        ready = await ctx.run_db(service.build_readiness, scheduled_post_id=sid)
        t, k = build_vip_scheduled_post_wizard_post_view(rule_id=rule_id, scheduled_post=row or {}, saved_posts=saved or [], readiness=ready or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    async def _open_vip_scheduled_post_step_targets_callback(*, callback: CallbackQuery, rule_id: int, scheduled_post_id: int) -> None:
        service = build_repost_campaign_scheduled_post_service(ctx)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, scheduled_post_id)
        readiness = await ctx.run_db(service.build_readiness, scheduled_post_id=scheduled_post_id)
        targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, scheduled_post_id, active_only=True)
        text, kb = build_vip_scheduled_post_wizard_targets_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=readiness or {})
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    async def _open_vip_scheduled_posts_list_callback(*, callback: CallbackQuery, rule_id: int, page: int = 0) -> None:
        posts = await ctx.run_db(ctx.db.list_campaign_scheduled_posts, rule_id=rule_id, statuses=["scheduled", "processing", "launched", "failed", "cancelled", "expired"], limit=100)
        text, kb = build_vip_scheduled_posts_list_view(rule_id=rule_id, posts=posts or [], page=page)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_posts:"))
    async def handle_rule_repost_campaign_scheduled_posts(callback: CallbackQuery):
        rule_id = int((callback.data or '').split(':')[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        active_placement = await ctx.run_db(service.build_active_scheduled_post_placement, rule_id=rule_id)
        text, kb = build_vip_scheduled_posts_screen_view(rule_id=rule_id, posts=[], active_placement=active_placement)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_posts_list:"))
    async def handle_rule_repost_campaign_scheduled_posts_list(callback: CallbackQuery):
        parts = (callback.data or "").split(":")
        rule_id = int(parts[1])
        page = 0
        if len(parts) >= 3:
            if parts[2].isdigit():
                page = int(parts[2])
            elif len(parts) >= 4 and parts[3].isdigit():
                page = int(parts[3])
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        await _open_vip_scheduled_posts_list_callback(callback=callback, rule_id=rule_id, page=page)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_new:"))
    async def handle_rule_repost_campaign_scheduled_post_new(callback: CallbackQuery):
        rule_id = int((callback.data or '').split(':')[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        created = await ctx.run_db(service.create_draft, rule_id=rule_id, created_by=callback.from_user.id if callback.from_user else None)
        if not created.ok:
            await ctx.answer_callback_safe(callback, created.error_text or 'Ошибка', show_alert=True)
            return
        scheduled_post_id = int((created.extra or {}).get("scheduled_post_id") or 0)
        ctx.user_states[callback.from_user.id] = {"state": "waiting_vip_scheduled_post_material", "rule_id": rule_id, "scheduled_post_id": scheduled_post_id}
        t = (
            "📝 Новый запланированный пост\n\n"
            "Отправьте сюда рекламный пост, который нужно запланировать.\n\n"
            "Можно:\n"
            "• переслать готовый пост из канала;\n"
            "• отправить текст;\n"
            "• отправить фото/видео с подписью;\n"
            "• отправить альбом.\n\n"
            "После сохранения поста ViMi перейдёт к шагу 2 — выбору каналов/групп."
        )
        k = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отменить", callback_data=f"rule_repost_campaign_scheduled_post_cancel_confirm:{rule_id}:{scheduled_post_id}")]])
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_post:"))
    async def handle_step_post(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        await _open_vip_step_post(callback, rule_id, int(sid))

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_pick_post:"))
    async def handle_pick_post(callback: CallbackQuery):
        _, rid, sid, pid = (callback.data or "").split(":", 3)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.update_draft_saved_post, scheduled_post_id=int(sid), saved_post_id=int(pid), actor_id=callback.from_user.id if callback.from_user else None)
        await _open_vip_step_post(callback, rule_id, int(sid))

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_targets:"))
    async def handle_step_targets(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        await _open_vip_scheduled_post_step_targets_callback(callback=callback, rule_id=rule_id, scheduled_post_id=int(sid))

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_show:"))
    async def handle_step_show(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid))
        ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
        t, k = build_vip_scheduled_post_wizard_show_view(rule_id=rule_id, scheduled_post=row or {}, readiness=ready or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_time:"))
    async def handle_step_time(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid))
        ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
        t, k = build_vip_scheduled_post_wizard_time_view(rule_id=rule_id, scheduled_post=row or {}, readiness=ready or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_preview:"))
    async def handle_preview(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid))
        targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, int(sid))
        ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
        t, k = build_vip_scheduled_post_preview_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=ready or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_confirm:"))
    async def handle_confirm(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.schedule_post, scheduled_post_id=int(sid), actor_id=callback.from_user.id if callback.from_user else None)
        await _open_vip_scheduled_posts_list_callback(callback=callback, rule_id=rule_id, page=0)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_detail:"))
    async def handle_detail(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        details = await ctx.run_db(service.get_post_details, scheduled_post_id=int(sid))
        text, kb = build_vip_scheduled_post_detail_view(rule_id=rule_id, details=details or {})
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
