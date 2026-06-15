from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime
from app.repost_campaign_launch_job_service import RepostCampaignLaunchJobService
from app.repost_campaign_placement_service import RepostCampaignPlacementService
from app.repost_campaign_top_time_view_service import RepostCampaignTopTimeViewService
from app.repost_campaign_service import format_campaign_show_seconds_ru, normalize_campaign_show_seconds
from app.repost_campaign_ui import (
    ACTIVE_PLACEMENTS_PAGE_SIZE,
    build_repost_campaign_active_campaign_view,
    build_repost_campaign_active_placements_view,
    build_repost_campaign_clean_channel_settings_view,
    build_repost_campaign_launch_clean_channel_blocked_view,
    build_repost_campaign_launch_clean_channel_warning_view,
    build_repost_campaign_launch_mode_view,
    build_repost_campaign_launch_wizard_view,
    build_repost_campaign_wizard_clean_channel_step_view,
    build_repost_campaign_wizard_post_step_view,
    build_repost_campaign_wizard_review_step_view,
    build_repost_campaign_wizard_show_time_step_view,
    build_repost_campaign_wizard_targets_step_view,
    build_repost_campaign_wizard_top_time_step_view,
    build_repost_campaign_launch_job_status_view,
    build_repost_campaign_launch_queued_view,
    build_repost_campaign_launch_readiness_view,
    build_repost_campaign_menu_view,
    build_repost_campaign_show_menu_view,
    build_repost_campaign_post_menu_view,
    build_repost_campaign_target_action_result_view,
    build_repost_campaign_target_card_view,
    build_repost_campaign_target_check_result_view,
    build_repost_campaign_target_delete_confirm_view,
    build_repost_campaign_preview_delete_result_view,
    build_repost_campaign_target_preview_result_view,
    build_repost_campaign_targets_check_loading_view,
    build_repost_campaign_targets_check_result_view,
    build_repost_campaign_targets_list_view,
    build_repost_campaign_targets_menu_view,
    build_repost_campaign_top_time_active_pauses_view,
    build_repost_campaign_top_time_pause_cancel_confirm_view,
    build_repost_campaign_top_time_pause_detail_view,
    build_repost_campaign_top_time_presets_view,
    build_repost_campaign_top_time_settings_view,
    build_repost_campaign_vip_coming_soon_view,
    build_repost_campaign_vip_features_view,
    DEFAULT_REPOST_CAMPAIGN_TOP_TIME_SECONDS,
    REPOST_CAMPAIGN_TOP_TIME_PRESETS,
)
from app.saved_posts_service import get_saved_post_short_description


async def _get_repost_campaign_saved_post_line(rule, rule_id: int, ctx: RepostCampaignHandlersContext) -> str:
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
            return f"📝 Рекламный пост: #{saved_post_id} · {saved_post_description}\n"
        return "📝 Рекламный пост: не найден\n"
    return "📝 Рекламный пост: не выбран\n"


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
    saved_post_line = await _get_repost_campaign_saved_post_line(rule, rule_id, ctx)

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
        active_campaign = await _get_repost_campaign_active_campaign(rule_id, ctx)
        if active_campaign:
            control_center = dict(control_center or {})
            control_center["active_campaign"] = active_campaign
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
            "top_time_enabled": bool(getattr(rule, "repost_campaign_top_time_enabled", False)),
            "top_time_seconds": int(getattr(rule, "repost_campaign_top_time_seconds", 0) or 0),
        },
        saved_post_line=saved_post_line,
        readiness=readiness,
        control_center=control_center,
    )
    await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
    return True


WIZARD_STEPS = {"post", "targets", "show_time", "clean_channel", "top_time", "review"}


def _select_repost_campaign_wizard_step(*, summary: dict, readiness: dict | None) -> str:
    payload = summary or {}
    readiness = readiness or {}
    if not payload.get("saved_post_id") and not readiness.get("saved_post_id"):
        return "post"
    targets_count = int(payload.get("targets_active") or 0)
    if targets_count <= 0:
        targets_count = int(readiness.get("will_send_total") or 0) + int(readiness.get("will_skip_total") or 0)
    if targets_count <= 0:
        return "targets"
    show_seconds = int(payload.get("show_seconds") or readiness.get("show_seconds") or 0)
    if show_seconds <= 0:
        return "show_time"
    return "review"


def _build_repost_campaign_wizard_step_view(*, step: str, rule_id: int, summary: dict, saved_post_line: str, readiness: dict | None, control_center: dict | None):
    if step == "post":
        return build_repost_campaign_wizard_post_step_view(rule_id=rule_id, summary=summary, saved_post_line=saved_post_line, readiness=readiness, control_center=control_center)
    if step == "targets":
        return build_repost_campaign_wizard_targets_step_view(rule_id=rule_id, summary=summary, saved_post_line=saved_post_line, readiness=readiness, control_center=control_center)
    if step == "show_time":
        return build_repost_campaign_wizard_show_time_step_view(rule_id=rule_id, summary=summary, saved_post_line=saved_post_line, readiness=readiness, control_center=control_center)
    if step == "clean_channel":
        return build_repost_campaign_wizard_clean_channel_step_view(rule_id=rule_id, summary=summary, saved_post_line=saved_post_line, readiness=readiness, control_center=control_center)
    if step == "top_time":
        return build_repost_campaign_wizard_top_time_step_view(rule_id=rule_id, summary=summary, saved_post_line=saved_post_line, readiness=readiness, control_center=control_center)
    return build_repost_campaign_wizard_review_step_view(rule_id=rule_id, summary=summary, saved_post_line=saved_post_line, readiness=readiness, control_center=control_center)


async def _render_repost_campaign_launch_wizard(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
    *,
    requested_step: str | None = None,
) -> bool:
    rule = await ctx.run_db(ctx.db.get_rule, rule_id)
    if not rule:
        await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return False
    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "repost":
        await ctx.answer_callback_safe(callback, "Рекламная кампания доступна только для режима репоста", show_alert=True)
        return False
    if not ctx.settings.repost_campaign_admin_test_enabled:
        await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
        return False
    try:
        summary = await ctx.run_db(ctx.db.get_rule_repost_campaign_summary, rule_id)
    except Exception:
        ctx.logger.exception("Не удалось открыть мастер запуска рекламной кампании, rule_id=%s", rule_id)
        summary = {}

    saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
    saved_post_line = await _get_repost_campaign_saved_post_line(rule, rule_id, ctx)
    runtime = build_repost_campaign_runtime(ctx)
    readiness = None
    try:
        readiness = await ctx.run_db(lambda: runtime.get_campaign_readiness(rule_id=rule_id))
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_LAUNCH_WIZARD_READINESS_FAILED | rule_id=%s | error=%s", rule_id, exc)
    control_center = None
    try:
        control_center = await ctx.run_db(lambda: runtime.get_campaign_control_center(rule_id=rule_id))
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_LAUNCH_WIZARD_CONTROL_CENTER_FAILED | rule_id=%s | error=%s", rule_id, exc, exc_info=True)

    summary_payload = dict(summary or {})
    summary_payload.update({
        "show_seconds": int(getattr(rule, "repost_campaign_show_seconds", 0) or summary_payload.get("show_seconds") or 0),
        "saved_post_id": saved_post_id,
        "top_time_enabled": bool(getattr(rule, "repost_campaign_top_time_enabled", False)),
        "top_time_seconds": int(getattr(rule, "repost_campaign_top_time_seconds", 0) or 0),
    })
    step = requested_step if requested_step in WIZARD_STEPS else _select_repost_campaign_wizard_step(summary=summary_payload, readiness=readiness)
    text, keyboard = _build_repost_campaign_wizard_step_view(
        step=step,
        rule_id=rule_id,
        summary=summary_payload,
        saved_post_line=saved_post_line,
        readiness=readiness,
        control_center=control_center,
    )
    if ctx.should_answer_new_message_for_callback(callback):
        await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, reply_markup=keyboard)
    else:
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
    readiness_payload = readiness or {}
    targets_count = int(summary_payload.get("targets_active") or readiness_payload.get("will_send_total") or 0)
    ctx.logger.info(
        "REPOST_CAMPAIGN_LAUNCH_WIZARD_OPENED | rule_id=%s | step=%s | ready=%s | can_launch=%s | targets_count=%s",
        rule_id,
        step,
        readiness_payload.get("ready"),
        readiness_payload.get("can_launch"),
        targets_count,
    )
    return True


async def _render_repost_campaign_active_placements(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
    *,
    page: int = 0,
) -> bool:
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return False
    normalized_page = max(0, int(page or 0))
    try:
        service = RepostCampaignPlacementService(ctx.db, logger=ctx.logger)
        # Repository API for this stage supports limit without offset.
        # Request the loaded prefix and let the UI builder slice the requested page.
        limit = (normalized_page + 1) * ACTIVE_PLACEMENTS_PAGE_SIZE
        state = await ctx.run_db(
            lambda: service.build_clean_channel_state(
                rule_id=rule_id,
                basic_only=True,
                limit=limit,
            )
        )
        state = dict(state or {})
        state["page"] = normalized_page
        state["page_size"] = ACTIVE_PLACEMENTS_PAGE_SIZE
        text, keyboard = build_repost_campaign_active_placements_view(rule_id=rule_id, state=state)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        ctx.logger.info(
            "REPOST_CAMPAIGN_ACTIVE_PLACEMENTS_OPENED | rule_id=%s | page=%s | state=%s | placements_total=%s",
            rule_id,
            normalized_page,
            state.get("state"),
            state.get("placements_total"),
        )
        return True
    except Exception as exc:
        ctx.logger.exception(
            "REPOST_CAMPAIGN_ACTIVE_PLACEMENTS_OPEN_FAILED | rule_id=%s | page=%s | error=%s",
            rule_id,
            normalized_page,
            exc,
        )
        await ctx.answer_callback_safe(callback, "Не удалось открыть активные размещения", show_alert=True)
        return False




async def _render_repost_campaign_top_time_active_pauses(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
) -> bool:
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return False
    if not ctx.settings.repost_campaign_admin_test_enabled:
        await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
        return False
    try:
        service = RepostCampaignTopTimeViewService(ctx.db, logger=ctx.logger)
        state = await ctx.run_db(service.build_active_pauses_for_rule, rule_id)
        text, keyboard = build_repost_campaign_top_time_active_pauses_view(rule_id=rule_id, state=state)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        ctx.logger.info(
            "REPOST_CAMPAIGN_TOP_TIME_ACTIVE_PAUSES_OPENED | rule_id=%s | total=%s",
            rule_id,
            (state or {}).get("total"),
        )
        return True
    except Exception as exc:
        ctx.logger.exception("REPOST_CAMPAIGN_TOP_TIME_ACTIVE_PAUSES_FAILED | rule_id=%s | error=%s", rule_id, exc)
        await ctx.answer_callback_safe(callback, "Не удалось открыть активные паузы", show_alert=True)
        return False


async def _render_repost_campaign_top_time_pause_detail(
    callback: CallbackQuery,
    rule_id: int,
    pause_id: int,
    ctx: RepostCampaignHandlersContext,
) -> dict | None:
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return None
    if not ctx.settings.repost_campaign_admin_test_enabled:
        await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
        return None
    service = RepostCampaignTopTimeViewService(ctx.db, logger=ctx.logger)
    state = await ctx.run_db(service.build_pause_detail, pause_id)
    pause = (state or {}).get("pause") or {}
    if not (state or {}).get("ok") or int(pause.get("rule_id") or 0) != int(rule_id):
        state = {"ok": False, "error_text": "Пауза не найдена"}
    text, keyboard = build_repost_campaign_top_time_pause_detail_view(rule_id=rule_id, pause_id=pause_id, state=state)
    await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
    return state

async def _render_repost_campaign_top_time_settings(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
) -> bool:
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return False
    if not ctx.settings.repost_campaign_admin_test_enabled:
        await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
        return False
    try:
        settings = await ctx.run_db(ctx.db.get_rule_repost_campaign_top_time_settings, rule_id)
        text, keyboard = build_repost_campaign_top_time_settings_view(rule_id=rule_id, settings=settings or {})
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        ctx.logger.info(
            "REPOST_CAMPAIGN_TOP_TIME_SETTINGS_OPENED | rule_id=%s | enabled=%s | seconds=%s",
            rule_id,
            (settings or {}).get("enabled"),
            (settings or {}).get("seconds"),
        )
        return True
    except Exception as exc:
        ctx.logger.exception(
            "REPOST_CAMPAIGN_TOP_TIME_SETTINGS_OPEN_FAILED | rule_id=%s | error=%s",
            rule_id,
            exc,
        )
        await ctx.answer_callback_safe(callback, "Не удалось открыть время в топе", show_alert=True)
        return False


async def _render_repost_campaign_top_time_presets(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
) -> bool:
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return False
    if not ctx.settings.repost_campaign_admin_test_enabled:
        await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
        return False
    try:
        settings = await ctx.run_db(ctx.db.get_rule_repost_campaign_top_time_settings, rule_id)
        text, keyboard = build_repost_campaign_top_time_presets_view(rule_id=rule_id, settings=settings or {})
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        return True
    except Exception as exc:
        ctx.logger.exception("REPOST_CAMPAIGN_TOP_TIME_PRESETS_OPEN_FAILED | rule_id=%s | error=%s", rule_id, exc)
        await ctx.answer_callback_safe(callback, "Не удалось открыть выбор времени", show_alert=True)
        return False

async def _get_repost_campaign_active_campaign(rule_id: int, ctx: RepostCampaignHandlersContext) -> dict | None:
    service = RepostCampaignPlacementService(ctx.db, logger=ctx.logger)
    state = await ctx.run_db(lambda: service.build_active_placements(rule_id=rule_id, basic_only=True, limit=1))
    if not state.get("ok") or not state.get("has_active"):
        return None
    placements = state.get("placements") or []
    return dict(placements[0]) if placements else None


async def _render_repost_campaign_active_campaign(callback: CallbackQuery, rule_id: int, ctx: RepostCampaignHandlersContext) -> bool:
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return False
    rule = await ctx.run_db(ctx.db.get_rule, rule_id)
    if not rule:
        await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
        return False
    if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "repost":
        await ctx.answer_callback_safe(callback, "Рекламная кампания доступна только для режима репоста", show_alert=True)
        return False
    try:
        runtime = build_repost_campaign_runtime(ctx)
        await ctx.run_db(lambda: runtime.get_campaign_control_center(rule_id=rule_id))
        active_campaign = await _get_repost_campaign_active_campaign(rule_id, ctx)
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_ACTIVE_CAMPAIGN_UI_FAILED | rule_id=%s | error=%s", rule_id, exc, exc_info=True)
        active_campaign = None
    if not active_campaign:
        await _render_repost_campaign_menu(callback, rule_id, ctx)
        await ctx.answer_callback_safe(callback, "Активная кампания уже завершена")
        return False
    text, keyboard = build_repost_campaign_active_campaign_view(rule_id=rule_id, active_campaign=active_campaign)
    await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
    return True


async def _render_repost_campaign_clean_channel_settings(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
) -> bool:
    if not await ctx.ensure_rule_callback_access(callback, rule_id):
        return False
    try:
        settings = await ctx.run_db(ctx.db.get_rule_repost_campaign_clean_channel_settings, rule_id)
        service = RepostCampaignPlacementService(ctx.db, logger=ctx.logger)
        state = await ctx.run_db(
            lambda: service.build_clean_channel_state(
                rule_id=rule_id,
                basic_only=True,
                limit=ACTIVE_PLACEMENTS_PAGE_SIZE,
            )
        )
        text, keyboard = build_repost_campaign_clean_channel_settings_view(
            rule_id=rule_id,
            settings=settings or {},
            state=state,
        )
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        ctx.logger.info(
            "REPOST_CAMPAIGN_CLEAN_CHANNEL_SETTINGS_OPENED | rule_id=%s | enabled=%s | state=%s",
            rule_id,
            (settings or {}).get("enabled"),
            (state or {}).get("state"),
        )
        return True
    except Exception as exc:
        ctx.logger.exception(
            "REPOST_CAMPAIGN_CLEAN_CHANNEL_SETTINGS_OPEN_FAILED | rule_id=%s | error=%s",
            rule_id,
            exc,
        )
        await ctx.answer_callback_safe(callback, "Не удалось открыть Чистый канал", show_alert=True)
        return False


async def _render_repost_campaign_post_menu(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
    *,
    force_new_message: bool = False,
) -> bool:
    try:
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return False
        saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
        status = "не выбран"
        if saved_post_id:
            saved_post = await ctx.run_db(ctx.db.get_saved_post, int(saved_post_id))
            if saved_post:
                status = f"#{saved_post_id} · {get_saved_post_short_description(saved_post.get('content_json') or {})}"
        ctx.logger.info("REPOST_CAMPAIGN_SAVED_POST_MENU_OPENED | rule_id=%s | saved_post_id=%s", rule_id, saved_post_id)
        text, keyboard = build_repost_campaign_post_menu_view(
            rule_id=rule_id,
            saved_post_id=int(saved_post_id) if saved_post_id else None,
            saved_post_description=status.split(" · ", 1)[1] if " · " in status else None,
        )
        if force_new_message:
            ctx.logger.info(
                "REPOST_CAMPAIGN_POST_MENU_RENDER_NEW_MESSAGE | rule_id=%s | reason=callback_from_media_message",
                rule_id,
            )
            await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, parse_mode="HTML", reply_markup=keyboard)
        else:
            ctx.logger.info("REPOST_CAMPAIGN_POST_MENU_RENDER_EDIT | rule_id=%s", rule_id)
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        return True
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_SAVED_POST_MENU_FAILED | rule_id=%s | error=%s", rule_id, exc)
        await ctx.answer_callback_safe(callback, "Не удалось открыть меню рекламного поста", show_alert=True)
        return False


async def _handle_repost_campaign_target_action(
    callback: CallbackQuery,
    ctx: RepostCampaignHandlersContext,
    *,
    action: str,
    is_active: bool | None = None,
) -> None:
    if not await ctx.is_admin_callback(callback):
        return
    if not ctx.settings.repost_campaign_admin_test_enabled:
        await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
        return
    parts = callback.data.split(":")
    _, rule_id_raw, row_id_raw = parts[:3]
    page = int(parts[3]) if len(parts) > 3 else 0
    rule_id = int(rule_id_raw)
    row_id = int(row_id_raw)
    runtime = build_repost_campaign_runtime(ctx)
    if action == "remove":
        result = await ctx.run_db(lambda: runtime.remove_campaign_target(rule_id=rule_id, target_row_id=row_id, admin_id=callback.from_user.id if callback.from_user else None))
    else:
        result = await ctx.run_db(lambda: runtime.set_campaign_target_active(rule_id=rule_id, target_row_id=row_id, is_active=bool(is_active), admin_id=callback.from_user.id if callback.from_user else None))
    text, keyboard = build_repost_campaign_target_action_result_view(rule_id=rule_id, result=result, action=action, page=page)
    await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
    ctx.logger.info("REPOST_CAMPAIGN_TARGET_ACTION_UI_DONE | rule_id=%s | row_id=%s | action=%s | ok=%s", rule_id, row_id, action, result.get("ok"))
    await ctx.answer_callback_safe_once(callback)


async def _send_repost_campaign_policy_message(
    callback: CallbackQuery,
    ctx: RepostCampaignHandlersContext,
    *,
    text: str,
    keyboard: InlineKeyboardMarkup,
) -> None:
    if ctx.should_answer_new_message_for_callback(callback):
        await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, reply_markup=keyboard)
    else:
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)


async def _render_repost_campaign_manual_launch_policy_state(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
    *,
    policy_state: dict,
) -> dict:
    action = policy_state.get("action")
    if action == "base_block":
        readiness = policy_state.get("base_readiness") or {}
        text, keyboard = build_repost_campaign_launch_readiness_view(rule_id=rule_id, readiness=readiness)
    elif action == "block" or policy_state.get("ok") is False:
        text, keyboard = build_repost_campaign_launch_clean_channel_blocked_view(rule_id=rule_id, policy_state=policy_state)
        ctx.logger.info(
            "REPOST_CAMPAIGN_MANUAL_LAUNCH_CLEAN_CHANNEL_BLOCKED_UI | rule_id=%s | action=%s",
            rule_id,
            action,
        )
    elif action == "confirm_required":
        text, keyboard = build_repost_campaign_launch_clean_channel_warning_view(rule_id=rule_id, policy_state=policy_state)
        ctx.logger.info(
            "REPOST_CAMPAIGN_MANUAL_LAUNCH_CLEAN_CHANNEL_WARNING_UI | rule_id=%s | action=%s",
            rule_id,
            action,
        )
    elif action in {"allow", "allow_forced"}:
        readiness = policy_state.get("base_readiness") or {}
        text, keyboard = build_repost_campaign_launch_readiness_view(rule_id=rule_id, readiness=readiness)
    else:
        text, keyboard = build_repost_campaign_launch_clean_channel_blocked_view(rule_id=rule_id, policy_state=policy_state)
        ctx.logger.warning(
            "REPOST_CAMPAIGN_MANUAL_LAUNCH_POLICY_UNKNOWN_ACTION | rule_id=%s | action=%s",
            rule_id,
            action,
        )
    await _send_repost_campaign_policy_message(callback, ctx, text=text, keyboard=keyboard)
    return policy_state


async def _render_repost_campaign_manual_launch_policy(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
    *,
    force_ignore_clean_channel: bool = False,
) -> dict | None:
    try:
        runtime = build_repost_campaign_runtime(ctx)
        policy_state = await ctx.run_db(
            lambda: runtime.build_manual_launch_policy_state(
                rule_id=rule_id,
                force_ignore_clean_channel=force_ignore_clean_channel,
            )
        )
        return await _render_repost_campaign_manual_launch_policy_state(
            callback,
            rule_id,
            ctx,
            policy_state=policy_state,
        )
    except Exception as exc:
        ctx.logger.warning(
            "REPOST_CAMPAIGN_MANUAL_LAUNCH_POLICY_UI_FAILED | rule_id=%s | force_clean_channel=%s | error=%s",
            rule_id,
            bool(force_ignore_clean_channel),
            exc,
            exc_info=True,
        )
        await ctx.answer_callback_safe(callback, "Не удалось проверить запуск кампании", show_alert=True)
        return None


async def _enqueue_repost_campaign_manual_launch_from_callback(
    callback: CallbackQuery,
    rule_id: int,
    ctx: RepostCampaignHandlersContext,
    *,
    force_ignore_clean_channel: bool = False,
    answer_text: str = "🚀 Запуск кампании поставлен в очередь",
) -> bool:
    await ctx.answer_callback_safe_once(callback, answer_text)

    progress_chat_id = getattr(getattr(callback, "message", None), "chat", None)
    progress_chat_id = getattr(progress_chat_id, "id", None)
    progress_message_id = getattr(getattr(callback, "message", None), "message_id", None)
    text, keyboard = build_repost_campaign_launch_queued_view(rule_id=rule_id, job_id=None)
    progress_message = callback.message
    try:
        if ctx.should_answer_new_message_for_callback(callback):
            sent_message = await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, reply_markup=keyboard)
            if sent_message is not None:
                progress_message = sent_message
                progress_chat_id = getattr(getattr(sent_message, "chat", None), "id", progress_chat_id)
                progress_message_id = getattr(sent_message, "message_id", progress_message_id)
        else:
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_LAUNCH_QUEUE_UI_FAILED | rule_id=%s | error=%s", rule_id, exc)

    runtime = build_repost_campaign_runtime(ctx)
    service = RepostCampaignLaunchJobService(repo=ctx.db, campaign_runtime=runtime, bot=ctx.get_bot(), logger_=ctx.logger)
    try:
        enqueue_result = await ctx.run_db(
            lambda: service.enqueue_manual_launch(
                rule_id=rule_id,
                admin_id=callback.from_user.id if callback.from_user else None,
                progress_chat_id=progress_chat_id,
                progress_message_id=progress_message_id,
                force_ignore_clean_channel=force_ignore_clean_channel,
            )
        )
        job = enqueue_result.job
        if enqueue_result.created:
            text, keyboard = build_repost_campaign_launch_queued_view(rule_id=rule_id, job_id=int(job.get("id") or 0))
        else:
            text, keyboard = build_repost_campaign_launch_job_status_view(rule_id=rule_id, job=job)
            if str(job.get("status") or "") in {"pending", "processing"}:
                text = "🚀 Кампания уже запускается\n\n" + "\n".join(text.splitlines()[1:]).lstrip()
        await ctx.edit_message_text_safe(message=progress_message, text=text, reply_markup=keyboard)
        ctx.logger.info(
            "REPOST_CAMPAIGN_LAUNCH_JOB_UI_ENQUEUED | rule_id=%s | job_id=%s | created=%s | force_clean_channel=%s",
            rule_id,
            job.get("id"),
            enqueue_result.created,
            bool(force_ignore_clean_channel),
        )
        return True
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_LAUNCH_JOB_ENQUEUE_UI_FAILED | rule_id=%s | error=%s", rule_id, exc)
        await ctx.edit_message_text_safe(
            message=progress_message,
            text="❌ Не удалось поставить кампанию в очередь\n\nПроверьте настройки кампании и попробуйте ещё раз.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")]]),
        )
        return False


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

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_show_menu:"))
    async def handle_rule_repost_campaign_show_menu(callback: CallbackQuery):
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
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        show_seconds_ru = format_campaign_show_seconds_ru(int(getattr(rule, "repost_campaign_show_seconds", 0) or 0))
        text, kb = build_repost_campaign_show_menu_view(rule_id=rule_id, current_show_seconds_text=show_seconds_ru)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_show_set:"))
    async def handle_rule_repost_campaign_show_set(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            _, rule_id_raw, seconds_raw = callback.data.split(":")
            rule_id = int(rule_id_raw)
            seconds = normalize_campaign_show_seconds(int(seconds_raw))
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.run_db(ctx.db.update_rule_repost_campaign_settings, rule_id, enabled=True, show_seconds=seconds)
        ctx.invalidate_rule_card_cache(rule_id)
        try:
            await ctx.run_db(
                ctx.db.log_rule_change,
                event_type="repost_campaign_show_seconds_changed",
                rule_id=rule_id,
                admin_id=callback.from_user.id if callback.from_user else ctx.settings.admin_id,
                old_value=None,
                new_value={
                    "repost_campaign_enabled": True,
                    "repost_campaign_show_seconds": seconds,
                },
                extra={
                    "source": "admin_ui",
                },
            )
        except Exception as exc:
            ctx.logger.warning(
                "Не удалось записать аудит изменения кампании rule_id=%s event_type=%s: %s",
                rule_id,
                "repost_campaign_show_seconds_changed",
                exc,
                exc_info=True,
            )
        if not await _render_repost_campaign_menu(callback, rule_id, ctx):
            return
        await ctx.answer_callback_safe_once(callback, "Срок показа обновлён")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_menu:"))
    async def handle_rule_repost_campaign_post_menu(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        force_new = ctx.should_answer_new_message_for_callback(callback)
        if not await _render_repost_campaign_post_menu(callback, rule_id, ctx, force_new_message=force_new):
            return
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_edit_stub:"))
    async def handle_rule_repost_campaign_post_edit_stub(callback: CallbackQuery):
        await ctx.answer_callback_safe_once(callback, "Редактирование текста будет добавлено следующим шагом")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_add:"))
    async def handle_rule_repost_campaign_post_add(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        ctx.user_states[callback.from_user.id] = {"state": "awaiting_repost_campaign_saved_post", "rule_id": rule_id}
        text = (
            "🔁 Замена рекламного поста\n\nОтправьте или перешлите новый рекламный пост.\n\n"
            "Можно отправить:\n• текст\n• фото с подписью\n• видео с подписью\n• документ\n\n"
            "Форматирование, ссылки и premium emoji будут сохранены.\n"
            "Старый пост останется в библиотеке, но кампания будет привязана к новому посту."
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад к рекламному посту", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")]
            ]
        )
        if ctx.should_answer_new_message_for_callback(callback):
            await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, reply_markup=keyboard)
        else:
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback)


    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_active_campaign:"))
    async def handle_rule_repost_campaign_active_campaign(callback: CallbackQuery):
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
        rendered = await _render_repost_campaign_active_campaign(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_wizard_step:"))
    async def handle_rule_repost_campaign_wizard_step(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            _, rule_id_raw, step = (callback.data or "").split(":", 2)
            rule_id = int(rule_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if step not in WIZARD_STEPS:
            await ctx.answer_callback_safe(callback, "Ошибка шага", show_alert=True)
            return
        if not await _render_repost_campaign_launch_wizard(callback, rule_id, ctx, requested_step=step):
            return
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_wizard:"))
    async def handle_rule_repost_campaign_launch_wizard(callback: CallbackQuery):
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
        if not await _render_repost_campaign_launch_wizard(callback, rule_id, ctx):
            return
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch:") and not c.data.startswith("rule_repost_campaign_launch_wizard:"))
    async def handle_rule_repost_campaign_launch(callback: CallbackQuery):
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
        try:
            runtime = build_repost_campaign_runtime(ctx)
            readiness = await ctx.run_db(lambda: runtime.build_campaign_launch_readiness(rule_id=rule_id))
            text, keyboard = build_repost_campaign_launch_mode_view(rule_id=rule_id, readiness=readiness)
            if ctx.should_answer_new_message_for_callback(callback):
                await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, reply_markup=keyboard)
            else:
                await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
            ctx.logger.info(
                "REPOST_CAMPAIGN_LAUNCH_PREFLIGHT_UI | rule_id=%s | can_launch=%s | will_send_total=%s",
                rule_id,
                bool(readiness.get("can_launch")),
                int(readiness.get("will_send_total") or 0),
            )
        except Exception as exc:
            ctx.logger.warning("REPOST_CAMPAIGN_LAUNCH_UI_FAILED | rule_id=%s | error=%s", rule_id, exc)
            await ctx.answer_callback_safe(callback, "Не удалось запустить кампанию", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_now_preview:"))
    async def handle_rule_repost_campaign_launch_now_preview(callback: CallbackQuery):
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
        policy_state = await _render_repost_campaign_manual_launch_policy(
            callback,
            rule_id,
            ctx,
            force_ignore_clean_channel=False,
        )
        if policy_state is None:
            return
        ctx.logger.info(
            "REPOST_CAMPAIGN_LAUNCH_NOW_PREVIEW_POLICY_UI | rule_id=%s | action=%s | can_launch=%s",
            rule_id,
            policy_state.get("action"),
            bool(policy_state.get("can_launch")),
        )
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_job_status:"))
    async def handle_rule_repost_campaign_launch_job_status(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            _, rule_id_raw, job_id_raw = callback.data.split(":", 2)
            rule_id = int(rule_id_raw)
            job_id = int(job_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        job = await ctx.run_db(ctx.db.get_repost_campaign_launch_job, job_id)
        if not job or int(job.get("rule_id") or 0) != rule_id:
            await ctx.answer_callback_safe(callback, "Задача запуска не найдена", show_alert=True)
            return
        text, keyboard = build_repost_campaign_launch_job_status_view(rule_id=rule_id, job=job)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback, "Статус обновлён")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_confirm_force:"))
    async def handle_rule_repost_campaign_launch_confirm_force(callback: CallbackQuery):
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
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        try:
            runtime = build_repost_campaign_runtime(ctx)
            policy_state = await ctx.run_db(
                lambda: runtime.build_manual_launch_policy_state(
                    rule_id=rule_id,
                    force_ignore_clean_channel=True,
                )
            )
        except Exception as exc:
            ctx.logger.warning(
                "REPOST_CAMPAIGN_MANUAL_LAUNCH_FORCE_POLICY_FAILED | rule_id=%s | error=%s",
                rule_id,
                exc,
                exc_info=True,
            )
            await ctx.answer_callback_safe(callback, "Не удалось проверить запуск кампании", show_alert=True)
            return

        action = policy_state.get("action")
        ctx.logger.info(
            "REPOST_CAMPAIGN_MANUAL_LAUNCH_CLEAN_CHANNEL_FORCED | rule_id=%s | action=%s",
            rule_id,
            action,
        )
        if action in {"allow", "allow_forced"} and policy_state.get("can_launch") is True:
            await _enqueue_repost_campaign_manual_launch_from_callback(
                callback,
                rule_id,
                ctx,
                force_ignore_clean_channel=True,
                answer_text="🚀 Запуск поверх активной рекламы поставлен в очередь",
            )
            return

        if action == "base_block":
            readiness = policy_state.get("base_readiness") or {}
            text, keyboard = build_repost_campaign_launch_readiness_view(rule_id=rule_id, readiness=readiness)
            await _send_repost_campaign_policy_message(callback, ctx, text=text, keyboard=keyboard)
        else:
            text, keyboard = build_repost_campaign_launch_clean_channel_blocked_view(rule_id=rule_id, policy_state=policy_state)
            await _send_repost_campaign_policy_message(callback, ctx, text=text, keyboard=keyboard)
            ctx.logger.info(
                "REPOST_CAMPAIGN_MANUAL_LAUNCH_CLEAN_CHANNEL_BLOCKED_UI | rule_id=%s | action=%s",
                rule_id,
                action,
            )
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_launch_confirm:"))
    async def handle_rule_repost_campaign_launch_confirm(callback: CallbackQuery):
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
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return

        try:
            runtime = build_repost_campaign_runtime(ctx)
            policy_state = await ctx.run_db(
                lambda: runtime.build_manual_launch_policy_state(
                    rule_id=rule_id,
                    force_ignore_clean_channel=False,
                )
            )
        except Exception as exc:
            ctx.logger.warning(
                "REPOST_CAMPAIGN_MANUAL_LAUNCH_CONFIRM_POLICY_FAILED | rule_id=%s | error=%s",
                rule_id,
                exc,
                exc_info=True,
            )
            await ctx.answer_callback_safe(callback, "Не удалось проверить запуск кампании", show_alert=True)
            return

        action = policy_state.get("action")
        if action == "allow_forced":
            ctx.logger.warning(
                "REPOST_CAMPAIGN_MANUAL_LAUNCH_CONFIRM_UNEXPECTED_FORCED_ACTION | rule_id=%s | action=%s",
                rule_id,
                action,
            )
        if action in {"allow", "allow_forced"} and policy_state.get("can_launch") is not False:
            await _enqueue_repost_campaign_manual_launch_from_callback(
                callback,
                rule_id,
                ctx,
                force_ignore_clean_channel=False,
            )
            return

        await _render_repost_campaign_manual_launch_policy_state(
            callback,
            rule_id,
            ctx,
            policy_state=policy_state,
        )
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_preview:"))
    async def handle_rule_repost_campaign_post_preview(callback: CallbackQuery):
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

        runtime = build_repost_campaign_runtime(ctx)
        result = await runtime.preview_saved_post_in_main_target(rule_id=rule_id, admin_chat_id=callback.from_user.id)
        if result.ok:
            ctx.user_states.setdefault(callback.from_user.id, {})["last_repost_campaign_preview"] = {
                "rule_id": rule_id,
                "target_id": result.extra.get("target_id") if result.extra else None,
                "message_id": result.extra.get("message_id") if result.extra else None,
                "message_ids": result.extra.get("message_ids") if result.extra else None,
                "render_mode": result.extra.get("method") if result.extra else None,
            }
            text, keyboard = build_repost_campaign_target_preview_result_view(rule_id=rule_id, result=result.to_dict())
            ctx.logger.info(
                "REPOST_CAMPAIGN_TARGET_PREVIEW_UI_DONE | rule_id=%s | target_id=%s | message_ids=%s | method=%s",
                rule_id,
                result.extra.get("target_id") if result.extra else None,
                result.extra.get("message_ids") if result.extra else None,
                result.extra.get("method") if result.extra else None,
            )
            await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, reply_markup=keyboard)
        else:
            ctx.logger.warning("REPOST_CAMPAIGN_TARGET_PREVIEW_UI_FAILED | rule_id=%s | error=%s", rule_id, result.error_text)
            error_text = "❌ Не удалось показать рекламный пост\n\n" f"{result.error_text or 'Неизвестная ошибка'}"
            await ctx.send_message_safe(chat_id=callback.message.chat.id, text=error_text)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_preview_delete:"))
    async def handle_rule_repost_campaign_preview_delete(callback: CallbackQuery):
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
        preview = ctx.user_states.get(callback.from_user.id, {}).get("last_repost_campaign_preview")
        if not preview or int(preview.get("rule_id") or 0) != rule_id:
            await ctx.send_message_safe(chat_id=callback.message.chat.id, text="Предпросмотр уже не найден. Отправьте его заново.")
            await ctx.answer_callback_safe_once(callback)
            return
        runtime = build_repost_campaign_runtime(ctx)
        result = await runtime.delete_preview_messages(
            target_id=preview.get("target_id"),
            message_id=preview.get("message_id"),
            message_ids=preview.get("message_ids"),
            render_mode=preview.get("render_mode"),
        )
        if result.ok:
            ctx.user_states.get(callback.from_user.id, {}).pop("last_repost_campaign_preview", None)
            ctx.logger.info(
                "REPOST_CAMPAIGN_TARGET_PREVIEW_DELETE_UI_DONE | rule_id=%s | target_id=%s | message_ids=%s",
                rule_id, preview.get("target_id"), preview.get("message_ids")
            )
        else:
            ctx.logger.warning("REPOST_CAMPAIGN_TARGET_PREVIEW_DELETE_UI_FAILED | rule_id=%s | error=%s", rule_id, result.error_text)
        text, keyboard = build_repost_campaign_preview_delete_result_view(rule_id=rule_id, result=result.to_dict())
        await ctx.send_message_safe(chat_id=callback.message.chat.id, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_unlink:"))
    async def handle_rule_repost_campaign_post_unlink(callback: CallbackQuery):
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
        try:
            rule = await ctx.run_db(ctx.db.get_rule, rule_id)
            if not rule:
                await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
                return
            old_saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
            await ctx.run_db(ctx.db.set_rule_repost_campaign_saved_post, rule_id, None)
            ctx.invalidate_rule_card_cache(rule_id)
            ctx.logger.info("REPOST_CAMPAIGN_SAVED_POST_UNLINKED | rule_id=%s | old_saved_post_id=%s", rule_id, old_saved_post_id)
            await _render_repost_campaign_post_menu(
                callback,
                rule_id,
                ctx,
                force_new_message=ctx.should_answer_new_message_for_callback(callback),
            )
            await ctx.answer_callback_safe_once(callback, "Рекламный пост убран из кампании")
        except Exception as exc:
            ctx.logger.warning("REPOST_CAMPAIGN_SAVED_POST_UNLINK_FAILED | rule_id=%s | error=%s", rule_id, exc)
            await ctx.answer_callback_safe(callback, "Не удалось убрать пост из кампании", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_test_send:"))
    async def handle_rule_repost_campaign_test_send(callback: CallbackQuery):
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

        runtime = build_repost_campaign_runtime(ctx)
        result = await runtime.test_send_saved_post_to_main_target(
            rule_id=rule_id,
            admin_id=callback.from_user.id if callback.from_user else None,
        )
        if not result.ok:
            try:
                await ctx.run_db(
                    ctx.db.log_rule_change,
                    event_type="repost_campaign_test_send_failed",
                    rule_id=rule_id,
                    admin_id=callback.from_user.id if callback.from_user else ctx.settings.admin_id,
                    old_value=None,
                    new_value=None,
                    extra={"source": "admin_ui", "saved_post_id": result.saved_post_id, "target_id": result.target_id, "error": str(result.error_text)},
                )
            except Exception as audit_exc:
                ctx.logger.warning("Не удалось записать аудит ошибки тестового запуска rule_id=%s: %s", rule_id, audit_exc, exc_info=True)
            error_text = "❌ Не удалось отправить рекламный пост\n\n" f"{result.error_text or 'Неизвестная ошибка'}"
            if result.premium_required:
                error_text += "\n\nPremium-оформление требует Telethon-отправки. Проверьте права аккаунта-парсера в канале."
            await ctx.send_message_safe(chat_id=callback.message.chat.id, text=error_text)
            await ctx.answer_callback_safe_once(callback)
            return

        try:
            await ctx.run_db(
                ctx.db.log_rule_change,
                event_type="repost_campaign_test_send_done",
                rule_id=rule_id,
                admin_id=callback.from_user.id if callback.from_user else ctx.settings.admin_id,
                old_value=None,
                new_value={"saved_post_id": result.saved_post_id, "target_id": result.target_id, "message_id": result.message_id},
                extra={"source": "admin_ui"},
            )
        except Exception as audit_exc:
            ctx.logger.warning("Не удалось записать аудит тестового запуска rule_id=%s: %s", rule_id, audit_exc, exc_info=True)

        await ctx.send_message_safe(
            chat_id=callback.message.chat.id,
            text=(
                "✅ Тестовый запуск выполнен\n\nРекламный пост отправлен в основной канал правила.\n"
                f"Message ID: {result.message_id}\n"
                f"Метод: {result.method}"
            ),
        )
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_targets:"))
    async def handle_rule_repost_campaign_targets(callback: CallbackQuery):
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
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "repost":
            await ctx.answer_callback_safe(callback, "Рекламная кампания доступна только для режима репоста", show_alert=True)
            return
        summary = await ctx.run_db(ctx.db.get_rule_repost_campaign_summary, rule_id)
        text, keyboard = build_repost_campaign_targets_menu_view(rule_id=rule_id, summary=summary or {})
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_add_list:"))
    async def handle_rule_repost_campaign_add_list(callback: CallbackQuery):
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
        rule = await ctx.run_db(ctx.db.get_rule, rule_id)
        if not rule:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        ctx.user_states[callback.from_user.id] = {"action": "awaiting_repost_campaign_targets_list", "rule_id": rule_id}
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=(
                "📥 Добавление каналов кампании\n\n"
                "Отправьте список каналов, каждый с новой строки.\n\n"
                "Можно использовать:\n"
                "@channel_username\n"
                "-1001234567890\n\n"
                "Пример:\n"
                "@my_channel\n"
                "-1001234567890"
            ),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_targets:{rule_id}")]]),
        )
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_targets_list:"))
    async def handle_rule_repost_campaign_targets_list(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        parts = callback.data.split(":")
        try:
            rule_id = int(parts[1])
            page = int(parts[2]) if len(parts) > 2 else 0
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        targets = await ctx.run_db(ctx.db.list_rule_repost_campaign_targets, rule_id, active_only=False)
        text, keyboard = build_repost_campaign_targets_list_view(rule_id=rule_id, targets=targets or [], page=page)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_target_pause:"))
    async def handle_repost_campaign_target_pause(callback: CallbackQuery):
        await _handle_repost_campaign_target_action(callback, ctx, action="pause", is_active=False)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_target_resume:"))
    async def handle_repost_campaign_target_resume(callback: CallbackQuery):
        await _handle_repost_campaign_target_action(callback, ctx, action="resume", is_active=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_target_delete_confirm:"))
    async def handle_repost_campaign_target_delete_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        parts = callback.data.split(":")
        _, rule_id_raw, row_id_raw = parts[:3]
        page = int(parts[3]) if len(parts) > 3 else 0
        rule_id = int(rule_id_raw)
        row_id = int(row_id_raw)
        runtime = build_repost_campaign_runtime(ctx)
        target = await ctx.run_db(lambda: runtime.get_campaign_target(rule_id=rule_id, target_row_id=row_id))
        text, keyboard = build_repost_campaign_target_delete_confirm_view(rule_id=rule_id, target=target, page=page)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_target_card:"))
    async def handle_rule_repost_campaign_target_card(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        parts = callback.data.split(":")
        try:
            rule_id = int(parts[1])
            target_row_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 0
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        runtime = build_repost_campaign_runtime(ctx)
        target = await ctx.run_db(runtime.get_campaign_target, rule_id=rule_id, target_row_id=target_row_id)
        text, keyboard = build_repost_campaign_target_card_view(rule_id=rule_id, target=target, page=page)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_target_delete:"))
    async def handle_repost_campaign_target_delete(callback: CallbackQuery):
        await _handle_repost_campaign_target_action(callback, ctx, action="remove")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_check:"))
    async def handle_rule_repost_campaign_check(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            rule_id = int(callback.data.split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        try:
            runtime = build_repost_campaign_runtime(ctx)
            targets = await ctx.run_db(ctx.db.list_rule_repost_campaign_targets, rule_id, active_only=False)
            loading_text, loading_keyboard = build_repost_campaign_targets_check_loading_view(rule_id=rule_id, targets_count=len(targets or []))
            await ctx.edit_message_text_safe(message=callback.message, text=loading_text, reply_markup=loading_keyboard)
            result = await runtime.check_campaign_targets(rule_id=rule_id, active_only=False, admin_id=callback.from_user.id if callback.from_user else None)
            text, keyboard = build_repost_campaign_targets_check_result_view(rule_id=rule_id, result=result)
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_TARGET_CHECK_BATCH_UI_FAILED | rule_id=%s | error=%s", rule_id, exc)
            await ctx.answer_callback_safe(callback, "Не удалось выполнить проверку прав каналов/групп", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_target_check:"))
    async def handle_rule_repost_campaign_target_check(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            parts = callback.data.split(":")
            rule_id = int(parts[1])
            row_id = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 0
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        try:
            runtime = build_repost_campaign_runtime(ctx)
            result = await runtime.check_campaign_target(rule_id=rule_id, target_row_id=row_id, admin_id=callback.from_user.id if callback.from_user else None)
            text, keyboard = build_repost_campaign_target_check_result_view(rule_id=rule_id, result=result, page=page)
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_TARGET_CHECK_UI_FAILED | rule_id=%s | row_id=%s | error=%s", rule_id, row_id, exc)
            await ctx.answer_callback_safe(callback, "Не удалось выполнить проверку канала/группы", show_alert=True)

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



    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time_pause_cancel_now:"))
    async def handle_rule_repost_campaign_top_time_pause_cancel_now(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, rule_id_raw, pause_id_raw = (callback.data or "").split(":")
            rule_id = int(rule_id_raw)
            pause_id = int(pause_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        state = await _render_repost_campaign_top_time_pause_detail(callback, rule_id, pause_id, ctx)
        if not state or not state.get("ok"):
            await ctx.answer_callback_safe(callback, "Пауза не найдена", show_alert=True)
            return
        pause = state.get("pause") or {}
        if pause.get("status") != "active":
            ctx.logger.info("REPOST_CAMPAIGN_TOP_TIME_PAUSE_CANCEL_SKIPPED | pause_id=%s | status=%s", pause_id, pause.get("status"))
            await ctx.answer_callback_safe(callback, "Пауза уже не активна", show_alert=True)
            return
        cancelled = await ctx.run_db(
            ctx.db.cancel_campaign_top_time_pause,
            pause_id,
            cancel_reason="manual_admin_cancel",
            actor_id=callback.from_user.id if callback.from_user else None,
        )
        if not cancelled:
            ctx.logger.info("REPOST_CAMPAIGN_TOP_TIME_PAUSE_CANCEL_SKIPPED | pause_id=%s | status=%s", pause_id, pause.get("status"))
            await _render_repost_campaign_top_time_pause_detail(callback, rule_id, pause_id, ctx)
            await ctx.answer_callback_safe(callback, "Пауза уже не активна", show_alert=True)
            return
        try:
            await ctx.run_db(
                ctx.db.log_rule_change,
                event_type="campaign_top_time_pause_cancelled",
                rule_id=rule_id,
                admin_id=callback.from_user.id if callback.from_user else ctx.settings.admin_id,
                old_value=None,
                new_value={"status": "cancelled"},
                extra={
                    "pause_id": pause_id,
                    "campaign_run_id": pause.get("campaign_run_id"),
                    "campaign_run_message_id": pause.get("campaign_run_message_id"),
                    "target_id": pause.get("target_id"),
                    "target_thread_id": pause.get("target_thread_id"),
                    "cancel_reason": "manual_admin_cancel",
                },
            )
        except Exception as exc:
            ctx.logger.warning("Не удалось записать аудит завершения паузы rule_id=%s pause_id=%s: %s", rule_id, pause_id, exc, exc_info=True)
        ctx.logger.info(
            "REPOST_CAMPAIGN_TOP_TIME_PAUSE_CANCELLED | rule_id=%s | pause_id=%s | campaign_run_id=%s | actor_id=%s",
            rule_id, pause_id, pause.get("campaign_run_id"), callback.from_user.id if callback.from_user else None,
        )
        await _render_repost_campaign_top_time_pause_detail(callback, rule_id, pause_id, ctx)
        await ctx.answer_callback_safe(callback, "Пауза завершена")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time_pause_cancel_confirm:"))
    async def handle_rule_repost_campaign_top_time_pause_cancel_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, rule_id_raw, pause_id_raw = (callback.data or "").split(":")
            rule_id = int(rule_id_raw)
            pause_id = int(pause_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        state = await _render_repost_campaign_top_time_pause_detail(callback, rule_id, pause_id, ctx)
        if not state or not state.get("ok"):
            await ctx.answer_callback_safe(callback, "Пауза не найдена", show_alert=True)
            return
        if ((state.get("pause") or {}).get("status") != "active"):
            await ctx.answer_callback_safe(callback, "Пауза уже не активна", show_alert=True)
            return
        text, keyboard = build_repost_campaign_top_time_pause_cancel_confirm_view(rule_id=rule_id, pause_id=pause_id, state=state)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time_pause:"))
    async def handle_rule_repost_campaign_top_time_pause(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, rule_id_raw, pause_id_raw = (callback.data or "").split(":")
            rule_id = int(rule_id_raw)
            pause_id = int(pause_id_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        state = await _render_repost_campaign_top_time_pause_detail(callback, rule_id, pause_id, ctx)
        if state and state.get("ok"):
            await ctx.answer_callback_safe_once(callback)
        else:
            await ctx.answer_callback_safe(callback, "Пауза не найдена", show_alert=True)


    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time_active_pauses:"))
    async def handle_rule_repost_campaign_top_time_active_pauses(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            rule_id = int((callback.data or "").split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        rendered = await _render_repost_campaign_top_time_active_pauses(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback)


    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time:"))
    async def handle_rule_repost_campaign_top_time(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            rule_id = int((callback.data or "").split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        rendered = await _render_repost_campaign_top_time_settings(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time_toggle:"))
    async def handle_rule_repost_campaign_top_time_toggle(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            parts = (callback.data or "").split(":")
            rule_id = int(parts[1])
            action = str(parts[2] or "").strip().lower()
            if action not in {"on", "off"}:
                raise ValueError("unknown top time action")
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        current = await ctx.run_db(ctx.db.get_rule_repost_campaign_top_time_settings, rule_id)
        enabled = action == "on"
        seconds = int((current or {}).get("seconds") or 0) if enabled else 0
        if enabled and seconds <= 0:
            seconds = DEFAULT_REPOST_CAMPAIGN_TOP_TIME_SECONDS
        actor_id = callback.from_user.id if callback.from_user else ctx.settings.admin_id
        try:
            updated = await ctx.run_db(
                ctx.db.set_rule_repost_campaign_top_time_settings,
                rule_id,
                enabled=enabled,
                seconds=seconds,
                actor_id=actor_id,
            )
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_TOP_TIME_TOGGLE_FAILED | rule_id=%s | enabled=%s | error=%s", rule_id, enabled, exc)
            await ctx.answer_callback_safe(callback, "Не удалось сохранить настройку", show_alert=True)
            return
        if not updated:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        ctx.invalidate_rule_card_cache(rule_id)
        try:
            await ctx.run_db(
                ctx.db.log_rule_change,
                event_type="repost_campaign_top_time_settings_changed",
                rule_id=rule_id,
                admin_id=actor_id,
                old_value=current,
                new_value={"enabled": enabled, "seconds": seconds},
                extra={"source": "admin_ui"},
            )
        except Exception as exc:
            ctx.logger.warning("Не удалось записать аудит времени в топе rule_id=%s: %s", rule_id, exc, exc_info=True)
        rendered = await _render_repost_campaign_top_time_settings(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback, "Время в топе включено" if enabled else "Время в топе выключено")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time_presets:"))
    async def handle_rule_repost_campaign_top_time_presets(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            rule_id = int((callback.data or "").split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        rendered = await _render_repost_campaign_top_time_presets(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_top_time_set:"))
    async def handle_rule_repost_campaign_top_time_set(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            parts = (callback.data or "").split(":")
            rule_id = int(parts[1])
            seconds = int(parts[2])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        allowed_seconds = {value for value, _ in REPOST_CAMPAIGN_TOP_TIME_PRESETS}
        if seconds not in allowed_seconds:
            await ctx.answer_callback_safe(callback, "Ошибка времени", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        actor_id = callback.from_user.id if callback.from_user else ctx.settings.admin_id
        updated = await ctx.run_db(
            ctx.db.set_rule_repost_campaign_top_time_settings,
            rule_id,
            enabled=True,
            seconds=seconds,
            actor_id=actor_id,
        )
        if not updated:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        ctx.invalidate_rule_card_cache(rule_id)
        try:
            await ctx.run_db(
                ctx.db.log_rule_change,
                event_type="repost_campaign_top_time_settings_changed",
                rule_id=rule_id,
                admin_id=actor_id,
                old_value=None,
                new_value={"enabled": True, "seconds": seconds},
                extra={"source": "admin_ui"},
            )
        except Exception as exc:
            ctx.logger.warning("Не удалось записать аудит времени в топе rule_id=%s: %s", rule_id, exc, exc_info=True)
        rendered = await _render_repost_campaign_top_time_settings(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback, "Время в топе обновлено")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_clean_channel:"))
    async def handle_rule_repost_campaign_clean_channel(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            rule_id = int((callback.data or "").split(":")[1])
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        rendered = await _render_repost_campaign_clean_channel_settings(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_clean_channel_toggle:"))
    async def handle_rule_repost_campaign_clean_channel_toggle(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            parts = (callback.data or "").split(":")
            rule_id = int(parts[1])
            action = str(parts[2] or "").strip().lower()
            if action not in {"on", "off"}:
                raise ValueError("unknown clean channel action")
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        enabled = action == "on"
        actor_id = callback.from_user.id if callback.from_user else ctx.settings.admin_id
        try:
            updated = await ctx.run_db(
                ctx.db.set_rule_repost_campaign_clean_channel_enabled,
                rule_id,
                enabled,
                actor_id,
            )
        except Exception as exc:
            ctx.logger.exception(
                "REPOST_CAMPAIGN_CLEAN_CHANNEL_TOGGLE_FAILED | rule_id=%s | enabled=%s | error=%s",
                rule_id,
                enabled,
                exc,
            )
            await ctx.answer_callback_safe(callback, "Не удалось сохранить настройку", show_alert=True)
            return
        if not updated:
            await ctx.answer_callback_safe(callback, "Правило не найдено", show_alert=True)
            return
        ctx.invalidate_rule_card_cache(rule_id)
        rendered = await _render_repost_campaign_clean_channel_settings(callback, rule_id, ctx)
        if rendered:
            await ctx.answer_callback_safe_once(callback, "Чистый канал включён" if enabled else "Чистый канал выключен")

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_active_placements:"))
    async def handle_rule_repost_campaign_active_placements(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            parts = (callback.data or "").split(":")
            rule_id = int(parts[1])
            page = int(parts[2]) if len(parts) > 2 else 0
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        rendered = await _render_repost_campaign_active_placements(callback, rule_id, ctx, page=page)
        if rendered:
            await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_vip_coming_soon:"))
    async def handle_rule_repost_campaign_vip_coming_soon(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            _, rule_id_text, feature = (callback.data or "").split(":", 2)
            rule_id = int(rule_id_text)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if feature == "clean_channel":
            rendered = await _render_repost_campaign_clean_channel_settings(callback, rule_id, ctx)
            if rendered:
                await ctx.answer_callback_safe_once(callback)
            return
        if feature == "top_time":
            rendered = await _render_repost_campaign_top_time_settings(callback, rule_id, ctx)
            if rendered:
                await ctx.answer_callback_safe_once(callback)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        text, kb = build_repost_campaign_vip_coming_soon_view(rule_id=rule_id, feature=feature)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
        await ctx.answer_callback_safe_once(callback)
