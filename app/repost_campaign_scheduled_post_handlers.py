from __future__ import annotations

from datetime import timedelta, timezone

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime, build_repost_campaign_scheduled_post_service
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
    build_vip_scheduled_post_send_now_confirm_view,
    build_vip_scheduled_post_cancel_confirm_view,
)


def _rule_value(rule, name: str, default=None):
    if rule is None:
        return default
    if isinstance(rule, dict):
        return rule.get(name, default)
    return getattr(rule, name, default)


def _target_key(target: dict) -> str:
    return f"{str(target.get('target_id') or '')}|{str(target.get('target_thread_id') or '')}"


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

    async def _build_vip_scheduled_known_targets(rule_id: int, scheduled_post_id: int) -> list[dict]:
        known: list[dict] = []
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

    async def _open_vip_scheduled_post_pick_targets(*, callback: CallbackQuery, rule_id: int, scheduled_post_id: int, page: int = 0):
        known_targets = await _build_vip_scheduled_known_targets(rule_id, scheduled_post_id)
        selected_targets = await ctx.run_db(ctx.db.list_campaign_scheduled_post_targets, scheduled_post_id, active_only=True) or []
        t, k = build_vip_scheduled_post_pick_targets_view(rule_id=rule_id, scheduled_post_id=scheduled_post_id, known_targets=known_targets, selected_targets=selected_targets, page=page, page_size=10)
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

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



    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_vip_delete_active:"))
    async def handle_rule_repost_campaign_vip_delete_active(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        active_placement = await ctx.run_db(service.build_active_scheduled_post_placement, rule_id=rule_id)
        if active_placement is None:
            text, kb = build_vip_scheduled_posts_screen_view(rule_id=rule_id, posts=[], active_placement=None)
            await ctx.edit_message_text_safe(message=callback.message, text=f"Активных VIP-запланированных размещений нет.\n\n{text}", reply_markup=kb)
            return
        await ctx.edit_message_text_safe(message=callback.message, text="Удаляю активный рекламный пост…")
        runtime = build_repost_campaign_runtime(ctx)
        active_run_id = int(active_placement.get("active_run_id") or 0)
        if active_run_id <= 0:
            text, kb = build_vip_scheduled_posts_screen_view(rule_id=rule_id, posts=[], active_placement=None)
            await ctx.edit_message_text_safe(message=callback.message, text=f"Активных VIP-запланированных размещений нет.\n\n{text}", reply_markup=kb)
            return
        result = await runtime.delete_campaign_run_now(rule_id=rule_id, run_id=active_run_id, admin_id=callback.from_user.id if callback.from_user else None)
        updated = await ctx.run_db(service.build_active_scheduled_post_placement, rule_id=rule_id)
        text, kb = build_vip_scheduled_posts_screen_view(rule_id=rule_id, posts=[], active_placement=updated)
        extra = result.extra or {}
        if result.ok:
            prefix = "✅ Активный рекламный пост удалён.\n\nТеперь запланированные посты смогут стартовать."
        elif int(extra.get("deleted") or 0) > 0:
            prefix = f"⚠️ Удаление выполнено частично.\n\nУдалено: {int(extra.get('deleted') or 0)}\nОшибки: {int(extra.get('failed') or 0)}\nViMi повторит удаление автоматически."
        else:
            prefix = "Активных рекламных постов нет."
        await ctx.edit_message_text_safe(message=callback.message, text=f"{prefix}\n\n{text}", reply_markup=kb)

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

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_snapshot_targets:"))
    async def handle_snapshot_targets(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.update_draft_targets_from_current_campaign, scheduled_post_id=int(sid), actor_id=callback.from_user.id if callback.from_user else None)
        await _open_vip_scheduled_post_step_targets_callback(callback=callback, rule_id=rule_id, scheduled_post_id=int(sid))

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_add_target:"))
    async def handle_vip_scheduled_post_add_target(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        scheduled_post_id = int(sid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        if callback.from_user:
            ctx.user_states[callback.from_user.id] = {"state": "waiting_vip_scheduled_post_target", "rule_id": rule_id, "scheduled_post_id": scheduled_post_id}
        t, k = build_vip_scheduled_post_add_target_view(rule_id=rule_id, scheduled_post_id=scheduled_post_id)
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_pick_targets:"))
    async def handle_vip_scheduled_post_pick_targets(callback: CallbackQuery):
        parts = (callback.data or "").split(":")
        _, rid, sid = parts[:3]
        page = int(parts[3]) if len(parts) > 3 else 0
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        await _open_vip_scheduled_post_pick_targets(callback=callback, rule_id=rule_id, scheduled_post_id=int(sid), page=page)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_add_known_target:"))
    async def handle_vip_scheduled_post_add_known_target(callback: CallbackQuery):
        _, rid, sid, target_index_text, page_text = (callback.data or "").split(":", 4)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        scheduled_post_id = int(sid)
        target_index = int(target_index_text)
        page = int(page_text)
        known_targets = await _build_vip_scheduled_known_targets(rule_id, scheduled_post_id)
        if 0 <= target_index < len(known_targets):
            target = known_targets[target_index]
            service = build_repost_campaign_scheduled_post_service(ctx)
            result = await ctx.run_db(service.add_manual_target, scheduled_post_id=scheduled_post_id, target_id=str(target.get("target_id") or ""), target_thread_id=target.get("target_thread_id"), target_title=target.get("target_title"), actor_id=callback.from_user.id if callback.from_user else None)
            extra = result.extra if hasattr(result, "extra") else (result.get("extra") if isinstance(result, dict) else {})
            if extra and extra.get("already_exists"):
                await ctx.answer_callback_safe(callback, "Уже добавлено")
            else:
                await ctx.answer_callback_safe(callback, "✅ Канал/группа добавлены.")
        await _open_vip_scheduled_post_pick_targets(callback=callback, rule_id=rule_id, scheduled_post_id=scheduled_post_id, page=page)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_add_known_all:"))
    async def handle_vip_scheduled_post_add_known_all(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        scheduled_post_id = int(sid)
        known_targets = await _build_vip_scheduled_known_targets(rule_id, scheduled_post_id)
        service = build_repost_campaign_scheduled_post_service(ctx)
        for target in known_targets:
            await ctx.run_db(service.add_manual_target, scheduled_post_id=scheduled_post_id, target_id=str(target.get("target_id") or ""), target_thread_id=target.get("target_thread_id"), target_title=target.get("target_title"), actor_id=callback.from_user.id if callback.from_user else None)
        await ctx.answer_callback_safe(callback, "✅ Каналы/группы добавлены.")
        await _open_vip_scheduled_post_step_targets_callback(callback=callback, rule_id=rule_id, scheduled_post_id=scheduled_post_id)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_add_known_page:"))
    async def handle_vip_scheduled_post_add_known_page(callback: CallbackQuery):
        _, rid, sid, page_text = (callback.data or "").split(":", 3)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        scheduled_post_id = int(sid)
        page = int(page_text)
        known_targets = await _build_vip_scheduled_known_targets(rule_id, scheduled_post_id)
        start = max(0, page) * 10
        end = start + 10
        service = build_repost_campaign_scheduled_post_service(ctx)
        has_new_additions = False
        has_already_added = False
        for target in known_targets[start:end]:
            result = await ctx.run_db(service.add_manual_target, scheduled_post_id=scheduled_post_id, target_id=str(target.get("target_id") or ""), target_thread_id=target.get("target_thread_id"), target_title=target.get("target_title"), actor_id=callback.from_user.id if callback.from_user else None)
            extra = result.extra if hasattr(result, "extra") else (result.get("extra") if isinstance(result, dict) else {})
            if extra and extra.get("already_exists"):
                has_already_added = True
            else:
                has_new_additions = True
        if has_new_additions:
            await ctx.answer_callback_safe(callback, "✅ Каналы/группы добавлены.")
        elif has_already_added:
            await ctx.answer_callback_safe(callback, "Уже добавлено")
        await _open_vip_scheduled_post_pick_targets(callback=callback, rule_id=rule_id, scheduled_post_id=scheduled_post_id, page=page)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_edit:"))
    async def handle_vip_scheduled_post_edit(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid)) or {}
        st = str(row.get("status") or "")
        if st in {"draft", "ready"}:
            await _open_vip_step_post(callback, rule_id, int(sid))
            return
        if st == "scheduled":
            await ctx.answer_callback_safe(callback, "Запланированный пост уже подтверждён. Чтобы изменить его, отмените и создайте заново или используйте Дублировать.", show_alert=True)
            return
        await ctx.answer_callback_safe(callback, "Редактирование недоступно для текущего статуса.", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_pick_show:"))
    async def handle_pick_show(callback: CallbackQuery):
        _, rid, sid, show_seconds_text = (callback.data or "").split(":", 3)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.update_draft_show_seconds, scheduled_post_id=int(sid), show_seconds=int(show_seconds_text), actor_id=callback.from_user.id if callback.from_user else None)
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid))
        ready = await ctx.run_db(service.build_readiness, scheduled_post_id=int(sid))
        t, k = build_vip_scheduled_post_wizard_show_view(rule_id=rule_id, scheduled_post=row or {}, readiness=ready or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_quick_time:"))
    async def handle_quick_time(callback: CallbackQuery):
        _, rid, sid, preset = (callback.data or "").split(":", 3)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        now = campaign_schedule_now_utc(); local = now + timedelta(hours=3)
        if preset == 'today_20': pick = local.replace(hour=20, minute=0, second=0, microsecond=0); pick = pick + timedelta(days=1) if pick <= local else pick
        elif preset == 'tomorrow_12': pick = (local + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        else: pick = (local + timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
        dt = (pick - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.update_draft_scheduled_at, scheduled_post_id=int(sid), scheduled_at_utc=dt, actor_id=callback.from_user.id if callback.from_user else None)
        await handle_step_time(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_input_time:"))
    async def handle_input_time(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        ctx.user_states[callback.from_user.id] = {"state": "waiting_repost_campaign_scheduled_post_time", "rule_id": rule_id, "scheduled_post_id": int(sid)}
        await ctx.edit_message_text_safe(message=callback.message, text="Введите дату и время\nПример:\n10.05 18:00", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_scheduled_post_step_time:{rid}:{sid}")]]))

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_check_rights:"))
    async def handle_check_rights(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        await service.check_targets(scheduled_post_id=int(sid), actor_id=callback.from_user.id if callback.from_user else None)
        await ctx.answer_callback_safe_once(callback, "Проверка завершена")
        await handle_detail(callback)

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


    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_send_now_confirm:"))
    async def handle_vip_scheduled_post_send_now_confirm(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid))
        t, k = build_vip_scheduled_post_send_now_confirm_view(rule_id=rule_id, scheduled_post=row or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_send_now:"))
    async def handle_vip_scheduled_post_send_now(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        result = await service.send_now(scheduled_post_id=int(sid), actor_id=callback.from_user.id if callback.from_user else None)
        if not result.ok and result.error_text:
            await ctx.answer_callback_safe(callback, result.error_text, show_alert=True)
        await handle_detail(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_duplicate:"))
    async def handle_vip_scheduled_post_duplicate(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        result = await ctx.run_db(service.duplicate_post, scheduled_post_id=int(sid), actor_id=callback.from_user.id if callback.from_user else None)
        if not result.ok:
            await ctx.answer_callback_safe(callback, result.error_text or "Ошибка дублирования", show_alert=True)
            return
        new_id = int((result.extra or {}).get("scheduled_post_id") or 0)
        await ctx.answer_callback_safe_once(callback, "✅ Пост скопирован.\n\nОткройте копию и выберите время запуска.")
        service = build_repost_campaign_scheduled_post_service(ctx)
        details = await ctx.run_db(service.get_post_details, scheduled_post_id=new_id)
        text, kb = build_vip_scheduled_post_detail_view(rule_id=rule_id, details=details or {})
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_cancel_confirm:"))
    async def handle_cancel_confirm(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_post, int(sid))
        t, k = build_vip_scheduled_post_cancel_confirm_view(rule_id=rule_id, scheduled_post=row or {})
        await ctx.edit_message_text_safe(message=callback.message, text=t, reply_markup=k)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_scheduled_post_cancel:"))
    async def handle_cancel(callback: CallbackQuery):
        _, rid, sid = (callback.data or "").split(":", 2)
        rule_id = int(rid)
        if not await ctx.ensure_rule_callback_access(callback, rule_id): return
        service = build_repost_campaign_scheduled_post_service(ctx)
        await ctx.run_db(service.cancel_post, scheduled_post_id=int(sid), actor_id=callback.from_user.id if callback.from_user else None, reason="cancelled_from_ui")
        await _open_vip_scheduled_posts_list_callback(callback=callback, rule_id=rule_id, page=0)
