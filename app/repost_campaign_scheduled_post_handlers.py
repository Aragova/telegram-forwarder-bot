from __future__ import annotations

from datetime import timedelta, timezone
from typing import Any

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_scheduled_post_service
from app.repost_campaign_schedule_service import campaign_schedule_now_utc
from app.repost_campaign_ui import (
    build_vip_scheduled_post_add_target_view,
    build_vip_scheduled_post_detail_view,
    build_vip_scheduled_post_pick_targets_view,
    build_vip_scheduled_post_preview_view,
    build_vip_scheduled_post_wizard_post_view,
    build_vip_scheduled_post_wizard_show_view,
    build_vip_scheduled_post_wizard_targets_view,
    build_vip_scheduled_post_wizard_time_view,
    build_vip_scheduled_posts_list_view,
    build_vip_scheduled_posts_screen_view,
)


def _rule_value(rule, name: str, default=None):
    if rule is None:
        return default
    if isinstance(rule, dict):
        return rule.get(name, default)
    return getattr(rule, name, default)


def _target_key(target: dict) -> str:
    return f"{str(target.get('target_id') or '')}|{str(target.get('target_thread_id') or '')}"


async def _build_vip_scheduled_known_targets(ctx: RepostCampaignHandlersContext, rule_id: int) -> list[dict]:
    known: list[dict[str, Any]] = []
    rule = await ctx.run_db(ctx.db.get_rule, rule_id)
    if rule and _rule_value(rule, "target_id"):
        known.append({"target_id": str(_rule_value(rule, "target_id")), "target_thread_id": _rule_value(rule, "target_thread_id"), "target_title": _rule_value(rule, "target_title") or str(_rule_value(rule, "target_id")), "source": "rule_main"})
    manual_targets = await ctx.run_db(ctx.db.list_rule_repost_campaign_targets, rule_id, active_only=True) or []
    for target in manual_targets:
        known.append({"target_id": str(target.get("target_id") or ""), "target_thread_id": target.get("target_thread_id"), "target_title": target.get("title") or str(target.get("target_id") or ""), "source": "manual_campaign"})
    posts = await ctx.run_db(ctx.db.list_campaign_scheduled_posts, rule_id=rule_id, limit=50) or []
    for post in posts:
        post_id = int(post.get("id") or 0)
        if post_id <= 0:
            continue
        scheduled_targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, post_id, active_only=True) or []
        for target in scheduled_targets:
            known.append({"target_id": str(target.get("target_id") or ""), "target_thread_id": target.get("target_thread_id"), "target_title": target.get("target_title") or target.get("title") or str(target.get("target_id") or ""), "source": "scheduled_history"})
    dedup: dict[str, dict] = {}
    for target in known:
        key = _target_key(target)
        if key not in dedup:
            dedup[key] = target
    return list(dedup.values())


def register_repost_campaign_scheduled_post_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_posts:"))
    async def handle_rule_repost_campaign_scheduled_posts(callback: CallbackQuery):
        rule_id = int((callback.data or '').split(':')[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        service = build_repost_campaign_scheduled_post_service(ctx)
        active_placement = await ctx.run_db(service.build_active_scheduled_post_placement, rule_id=rule_id)
        text, kb = build_vip_scheduled_posts_screen_view(rule_id=rule_id, posts=[], active_placement=active_placement)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_posts_list:"))
    async def handle_rule_repost_campaign_scheduled_posts_list(callback: CallbackQuery):
        parts = (callback.data or "").split(":")
        rule_id = int(parts[1]); page = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 0
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        posts = await ctx.run_db(ctx.db.list_campaign_scheduled_posts, rule_id=rule_id, statuses=["scheduled", "processing", "launched", "failed", "cancelled", "expired"], limit=100)
        text, kb = build_vip_scheduled_posts_list_view(rule_id=rule_id, posts=posts or [], page=page)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_new:"))
    async def handle_rule_repost_campaign_scheduled_post_new(callback: CallbackQuery):
        rule_id = int((callback.data or '').split(':')[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        service = build_repost_campaign_scheduled_post_service(ctx)
        created = await ctx.run_db(service.create_draft, rule_id=rule_id, created_by=callback.from_user.id if callback.from_user else None)
        if not created.ok:
            await ctx.answer_callback_safe(callback, created.error_text or 'Ошибка', show_alert=True)
            return
        scheduled_post_id = int((created.extra or {}).get("scheduled_post_id") or 0)
        ctx.user_states[callback.from_user.id] = {"state": "waiting_vip_scheduled_post_material", "rule_id": rule_id, "scheduled_post_id": scheduled_post_id}
        t = "📝 Новый запланированный пост\n\nОтправьте сюда рекламный пост, который нужно запланировать.\n\nМожно:\n• переслать готовый пост из канала;\n• отправить текст;\n• отправить фото/видео с подписью;\n• отправить альбом.\n\nПосле сохранения поста ViMi перейдёт к шагу 2 — выбору каналов/групп."
        k = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отменить", callback_data=f"rule_repost_campaign_scheduled_post_cancel_confirm:{rule_id}:{scheduled_post_id}")]])
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_step_"))
    async def handle_step_callbacks(callback: CallbackQuery):
        data = callback.data or ""
        if data.startswith("rule_repost_campaign_scheduled_post_step_post:"):
            _, rid, sid = data.split(":", 2)
            rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id):
                return
            service = build_repost_campaign_scheduled_post_service(ctx)
            row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid)); saved = await ctx.run_db(ctx.db.list_saved_posts, rule_id=rule_id, limit=10); ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
            t, k = build_vip_scheduled_post_wizard_post_view(rule_id=rule_id, scheduled_post=row or {}, saved_posts=saved or [], readiness=ready or {})
            await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)
        elif data.startswith("rule_repost_campaign_scheduled_post_step_targets:"):
            _, rid, sid = data.split(":", 2)
            rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id):
                return
            service = build_repost_campaign_scheduled_post_service(ctx)
            row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid)); readiness = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid)); targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, int(sid), active_only=True)
            t, k = build_vip_scheduled_post_wizard_targets_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=readiness or {})
            await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)
        elif data.startswith("rule_repost_campaign_scheduled_post_step_show:"):
            _, rid, sid = data.split(":", 2)
            rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id):
                return
            service = build_repost_campaign_scheduled_post_service(ctx)
            row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid)); ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
            t, k = build_vip_scheduled_post_wizard_show_view(rule_id=rule_id, scheduled_post=row or {}, readiness=ready or {})
            await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)
        elif data.startswith("rule_repost_campaign_scheduled_post_step_time:"):
            _, rid, sid = data.split(":", 2)
            rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id):
                return
            service = build_repost_campaign_scheduled_post_service(ctx)
            row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid)); ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
            t, k = build_vip_scheduled_post_wizard_time_view(rule_id=rule_id, scheduled_post=row or {}, readiness=ready or {})
            await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_pick_"))
    async def handle_pick_callbacks(callback: CallbackQuery):
        data = callback.data or ""
        service = build_repost_campaign_scheduled_post_service(ctx)
        if data.startswith("rule_repost_campaign_scheduled_post_pick_post:"):
            _, rid, sid, pid = data.split(":", 3)
            rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id): return
            await ctx.run_db(service.update_draft_saved_post, scheduled_post_id=int(sid), saved_post_id=int(pid), actor_id=callback.from_user.id if callback.from_user else None)
        elif data.startswith("rule_repost_campaign_scheduled_post_pick_targets:"):
            parts = data.split(":"); _, rid, sid = parts[:3]; page = int(parts[3]) if len(parts) > 3 else 0; rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id): return
            known_targets = await _build_vip_scheduled_known_targets(ctx, rule_id)
            selected_targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, int(sid), active_only=True) or []
            t, k = build_vip_scheduled_post_pick_targets_view(rule_id=rule_id, scheduled_post_id=int(sid), known_targets=known_targets, selected_targets=selected_targets, page=page, page_size=10)
            await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)
            return
        elif data.startswith("rule_repost_campaign_scheduled_post_pick_show:"):
            _, rid, sid, show_seconds_text = data.split(":", 3); rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id): return
            await ctx.run_db(service.update_draft_show_seconds, scheduled_post_id=int(sid), show_seconds=int(show_seconds_text), actor_id=callback.from_user.id if callback.from_user else None)
        else:
            return
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_add_target:"))
    async def handle_add_target(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        if callback.from_user:
            ctx.user_states[callback.from_user.id] = {"state": "waiting_vip_scheduled_post_target", "rule_id": rule_id, "scheduled_post_id": int(sid)}
        t, k = build_vip_scheduled_post_add_target_view(rule_id=rule_id, scheduled_post_id=int(sid))
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_time_"))
    async def handle_time_callbacks(callback: CallbackQuery):
        data = callback.data or ""
        if data.startswith("rule_repost_campaign_scheduled_post_input_time:"):
            _, rid, sid = data.split(":", 2); rule_id = int(rid)
            if not await ctx.ensure_rule_callback_access(callback, rule_id): return
            ctx.user_states[callback.from_user.id] = {"state": "waiting_repost_campaign_scheduled_post_time", "rule_id": rule_id, "scheduled_post_id": int(sid)}
            await ctx.edit_message_text_safe(message=callback.message, text="Введите дату и время\nПример:\n10.05 18:00", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_scheduled_post_step_time:{rid}:{sid}")]]))

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_quick_time:"))
    async def handle_quick_time(callback: CallbackQuery):
        _, rid, sid, preset = (callback.data or "").split(":", 3); rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        now = campaign_schedule_now_utc(); local = now + timedelta(hours=3)
        if preset == 'today_20': pick = local.replace(hour=20, minute=0, second=0, microsecond=0); pick = pick + timedelta(days=1) if pick <= local else pick
        elif preset == 'tomorrow_12': pick = (local + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        else: pick = (local + timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
        dt = (pick - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.update_draft_scheduled_at, scheduled_post_id=int(sid), scheduled_at_utc=dt, actor_id=callback.from_user.id if callback.from_user else None)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_preview:"))
    async def handle_preview(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2); rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid)); targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, int(sid)); ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
        t, k = build_vip_scheduled_post_preview_view(rule_id=rule_id, scheduled_post=row or {}, targets=targets or [], readiness=ready or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_confirm:"))
    async def handle_confirm(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2); rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.schedule_post, scheduled_post_id=int(sid), actor_id=callback.from_user.id if callback.from_user else None)
        posts = await ctx.run_db(ctx.db.list_campaign_scheduled_posts, rule_id=rule_id, statuses=["scheduled", "processing", "launched", "failed", "cancelled", "expired"], limit=100)
        text, kb = build_vip_scheduled_posts_list_view(rule_id=rule_id, posts=posts or [], page=0)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_detail:"))
    async def handle_detail(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2); rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        details = await ctx.run_db(service.get_post_details, scheduled_post_id=int(sid))
        text, kb = build_vip_scheduled_post_detail_view(rule_id=rule_id, details=details or {})
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
