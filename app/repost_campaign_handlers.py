from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime
from app.repost_campaign_service import format_campaign_show_seconds_ru
from app.repost_campaign_ui import (
    build_repost_campaign_menu_view,
    build_repost_campaign_vip_coming_soon_view,
    build_repost_campaign_vip_features_view,
)
from app.saved_posts_service import get_saved_post_short_description


async def _render_repost_campaign_menu(callback: CallbackQuery, rule_id: int, ctx: RepostCampaignHandlersContext) -> bool:
    rule = await ctx.run_db(ctx.db.get_rule, rule_id)
    if not rule:
        await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return False
    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "repost":
        await ctx.answer_callback_safe(callback, "Рекламная кампания доступна только для режима репоста", show_alert=True)
        return False
    try:
        summary = await ctx.run_db(ctx.db.get_rule_repost_campaign_summary, rule_id)
    except Exception:
        ctx.logger.exception("Не удалось открыть меню рекламной кампании, rule_id=%s", rule_id)
        summary = {}
    show_seconds_ru = format_campaign_show_seconds_ru(int(getattr(rule, "repost_campaign_show_seconds", 0) or 0))
    targets_active = int((summary or {}).get("targets_active") or 0)
    targets_ready = int((summary or {}).get("targets_ready") or 0)
    saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
    if saved_post_id:
        try:
            saved_post = await ctx.run_db(ctx.db.get_saved_post, int(saved_post_id))
        except Exception as exc:
            ctx.logger.warning("Не удалось получить рекламный пост кампании rule_id=%s saved_post_id=%s: %s", rule_id, saved_post_id, exc, exc_info=True)
            saved_post = None

        if saved_post:
            try:
                content = saved_post.get("content_json") or saved_post.get("content") or {}
                saved_post_description = get_saved_post_short_description(content)
            except Exception:
                saved_post_description = "пост"
            saved_post_line = f"📝 Рекламный пост: #{saved_post_id} · {saved_post_description}\n"
        else:
            saved_post_line = "📝 Рекламный пост: не найден\n"
    else:
        saved_post_line = "📝 Рекламный пост: не выбран\n"

    runtime = build_repost_campaign_runtime(ctx)
    readiness = None
    try:
        readiness = await ctx.run_db(lambda: runtime.get_campaign_readiness(rule_id=rule_id))
        ctx.logger.info("REPOST_CAMPAIGN_READINESS_BUILT | rule_id=%s | ready=%s | warnings=%s", rule_id, readiness.get("ready"), len(readiness.get("warnings") or []))
    except Exception as exc:
        readiness = None
        ctx.logger.warning("REPOST_CAMPAIGN_READINESS_FAILED | rule_id=%s | error=%s", rule_id, exc)
    control_center = None
    try:
        control_center = await ctx.run_db(lambda: runtime.get_campaign_control_center(rule_id=rule_id))
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_CONTROL_CENTER_UI_FAILED | rule_id=%s | error=%s", rule_id, exc, exc_info=True)

    text, keyboard = build_repost_campaign_menu_view(
        rule_id=rule_id,
        summary={
            "show_seconds_text": show_seconds_ru,
            "show_seconds": int(getattr(rule, "repost_campaign_show_seconds", 0) or 0),
            "targets_active": targets_active,
            "targets_ready": targets_ready,
            "saved_post_id": saved_post_id,
        },
        saved_post_line=saved_post_line,
        readiness=readiness,
        control_center=control_center,
    )
    await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
    return True


def register_repost_campaign_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_menu:"))
    async def handle_rule_repost_campaign_menu(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await _render_repost_campaign_menu(callback, rule_id, ctx):
            return
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_vip_features:"))
    async def handle_rule_repost_campaign_vip_features(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        text, kb = build_repost_campaign_vip_features_view(rule_id=rule_id)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_vip_coming_soon:"))
    async def handle_rule_repost_campaign_vip_coming_soon(callback: CallbackQuery):
        _, rule_id_text, feature = (callback.data or "").split(":", 2)
        rule_id = int(rule_id_text)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        text, kb = build_repost_campaign_vip_coming_soon_view(rule_id=rule_id, feature=feature)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
