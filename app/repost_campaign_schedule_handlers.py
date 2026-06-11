from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime
from app.repost_campaign_schedule_service import RepostCampaignScheduleService, campaign_schedule_now_utc
from app.repost_campaign_service import normalize_campaign_show_seconds
from app.repost_campaign_ui import (
    build_repost_campaign_schedule_current_view,
    build_repost_campaign_schedule_preview_view,
    build_repost_campaign_schedule_result_view,
    build_repost_campaign_schedule_wizard_step1_view,
    build_repost_campaign_schedule_wizard_step2_view,
    build_repost_campaign_schedule_wizard_step3_view,
    build_repost_campaign_schedule_wizard_step4_view,
    build_repost_campaign_scheduled_launch_cancel_confirm_view,
    build_repost_campaign_scheduled_launch_cancel_result_view,
    build_repost_campaign_scheduled_launch_detail_view,
)
from datetime import datetime, timedelta, timezone


REPOST_CAMPAIGN_SCHEDULED_DETAIL_PREFIX = "rule_repost_campaign_" + "scheduled_detail:"
REPOST_CAMPAIGN_SCHEDULED_CANCEL_CONFIRM_PREFIX = "rule_repost_campaign_" + "scheduled_cancel_confirm:"
REPOST_CAMPAIGN_SCHEDULED_CANCEL_PREFIX = "rule_repost_campaign_" + "scheduled_cancel:"


def _build_repost_campaign_schedule_service(ctx: RepostCampaignHandlersContext) -> RepostCampaignScheduleService:
    return RepostCampaignScheduleService(
        repo=ctx.db,
        campaign_runtime=build_repost_campaign_runtime(ctx),
        logger_=ctx.logger,
    )


def _schedule_policy_epoch(scheduled_at_utc: datetime) -> int:
    return int(scheduled_at_utc.timestamp())


def _schedule_policy_ack_key(rule_id: int, epoch: int) -> str:
    return f"repost_campaign_schedule_policy_ack:{rule_id}:{epoch}"


def _set_schedule_policy_ack(ctx: RepostCampaignHandlersContext, callback: CallbackQuery, *, rule_id: int, scheduled_at_utc: datetime, action: str) -> None:
    if action != "schedule_with_overlap_warning":
        return
    user_id = callback.from_user.id if callback.from_user else None
    if user_id is None:
        return
    epoch = _schedule_policy_epoch(scheduled_at_utc)
    ctx.user_states.setdefault(user_id, {})["schedule_policy_ack"] = {
        "key": _schedule_policy_ack_key(rule_id, epoch),
        "action": action,
    }


def _has_schedule_policy_ack(ctx: RepostCampaignHandlersContext, callback: CallbackQuery, *, rule_id: int, scheduled_at_utc: datetime, action: str) -> bool:
    if action != "schedule_with_overlap_warning":
        return False
    user_id = callback.from_user.id if callback.from_user else None
    if user_id is None:
        return False
    epoch = _schedule_policy_epoch(scheduled_at_utc)
    ack = ctx.user_states.get(user_id, {}).get("schedule_policy_ack") or {}
    return ack.get("key") == _schedule_policy_ack_key(rule_id, epoch) and ack.get("action") == action


def _clear_schedule_policy_ack(ctx: RepostCampaignHandlersContext, callback: CallbackQuery) -> None:
    user_id = callback.from_user.id if callback.from_user else None
    if user_id is None:
        return
    ctx.user_states.get(user_id, {}).pop("schedule_policy_ack", None)


async def _build_repost_campaign_schedule_policy_preview(
    *,
    ctx: RepostCampaignHandlersContext,
    rule_id: int,
    scheduled_at_utc: datetime,
) -> tuple[str, InlineKeyboardMarkup, dict]:
    service = _build_repost_campaign_schedule_service(ctx)
    try:
        policy_state = await ctx.run_db(
            lambda: service.build_scheduled_launch_policy_state(
                rule_id=rule_id,
                scheduled_at_utc=scheduled_at_utc,
            )
        )
    except Exception:
        ctx.logger.warning(
            "REPOST_CAMPAIGN_SCHEDULE_PREVIEW_POLICY_FAILED | rule_id=%s",
            rule_id,
            exc_info=True,
        )
        policy_state = {
            "ok": False,
            "action": "policy_error",
            "can_schedule": False,
            "scheduled_at": scheduled_at_utc.isoformat(),
            "blocking_text": "Не удалось проверить Чистый канал",
        }
    readiness = policy_state.get("base_readiness") or {}
    text, kb = build_repost_campaign_schedule_preview_view(
        rule_id=rule_id,
        readiness=readiness,
        scheduled_at_utc=scheduled_at_utc,
        scheduled_policy=policy_state,
    )
    ctx.logger.info(
        "REPOST_CAMPAIGN_SCHEDULE_PREVIEW_POLICY_UI | rule_id=%s | action=%s | can_schedule=%s",
        rule_id,
        policy_state.get("action"),
        policy_state.get("can_schedule") is True,
    )
    return text, kb, policy_state


def register_repost_campaign_schedule_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data.startswith(REPOST_CAMPAIGN_SCHEDULED_DETAIL_PREFIX))
    async def handle_rule_repost_campaign_scheduled_detail(callback: CallbackQuery):
        try:
            _, rule_id_text, launch_id_text = (callback.data or "").split(":", 2)
            rule_id = int(rule_id_text)
            launch_id = int(launch_id_text)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        row = await ctx.run_db(ctx.db.get_campaign_scheduled_launch, launch_id)
        if not row:
            await ctx.answer_callback_safe(callback, "Запланированный запуск не найден", show_alert=True)
            return
        text, kb = build_repost_campaign_scheduled_launch_detail_view(rule_id=rule_id, scheduled_launch=row)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith(REPOST_CAMPAIGN_SCHEDULED_CANCEL_CONFIRM_PREFIX))
    async def handle_rule_repost_campaign_scheduled_cancel_confirm(callback: CallbackQuery):
        try:
            _, rule_id_text, launch_id_text = (callback.data or "").split(":", 2)
            rule_id = int(rule_id_text)
            launch_id = int(launch_id_text)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        text, kb = build_repost_campaign_scheduled_launch_cancel_confirm_view(rule_id=rule_id, scheduled_launch_id=launch_id)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith(REPOST_CAMPAIGN_SCHEDULED_CANCEL_PREFIX))
    async def handle_rule_repost_campaign_scheduled_cancel(callback: CallbackQuery):
        try:
            _, rule_id_text, launch_id_text = (callback.data or "").split(":", 2)
            rule_id = int(rule_id_text)
            launch_id = int(launch_id_text)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        runtime = _build_repost_campaign_schedule_service(ctx)
        result = runtime.cancel_scheduled_launch(scheduled_launch_id=launch_id, cancelled_by=callback.from_user.id if callback.from_user else None)
        text, kb = build_repost_campaign_scheduled_launch_cancel_result_view(rule_id=rule_id, ok=result.ok)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_menu:"))
    async def handle_rule_repost_campaign_schedule_menu(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        runtime = build_repost_campaign_runtime(ctx)
        readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
        text, kb = build_repost_campaign_schedule_current_view(rule_id=rule_id, readiness=readiness)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_current:"))
    async def handle_rule_repost_campaign_schedule_current(callback: CallbackQuery):
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
            readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
            text, keyboard = build_repost_campaign_schedule_current_view(rule_id=rule_id, readiness=readiness)
            if ctx.should_answer_new_message_for_callback(callback):
                await ctx.send_message_safe(chat_id=callback.from_user.id, text=text, reply_markup=keyboard)
            else:
                await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        except Exception as exc:
            ctx.logger.warning("REPOST_CAMPAIGN_SCHEDULE_CURRENT_UI_FAILED | rule_id=%s | error=%s", rule_id, exc)
            await ctx.answer_callback_safe(callback, "Не удалось открыть планирование запуска", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_step1:"))
    async def handle_rule_repost_campaign_schedule_step1(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        runtime = build_repost_campaign_runtime(ctx)
        readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
        text, kb = build_repost_campaign_schedule_wizard_step1_view(rule_id=rule_id, readiness=readiness)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_step2:"))
    async def handle_rule_repost_campaign_schedule_step2(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        runtime = build_repost_campaign_runtime(ctx)
        readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
        if not (bool(readiness.get("saved_post_id")) and readiness.get("saved_post_exists") is not False):
            text, kb = build_repost_campaign_schedule_wizard_step1_view(rule_id=rule_id, readiness=readiness)
        else:
            text, kb = build_repost_campaign_schedule_wizard_step2_view(rule_id=rule_id, readiness=readiness)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_step3:"))
    async def handle_rule_repost_campaign_schedule_step3(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        runtime = build_repost_campaign_runtime(ctx)
        readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
        text, kb = build_repost_campaign_schedule_wizard_step3_view(rule_id=rule_id, readiness=readiness)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_show_pick:"))
    async def handle_rule_repost_campaign_schedule_show_pick(callback: CallbackQuery):
        _, rule_id_text, seconds_text = (callback.data or "").split(":", 2)
        rule_id = int(rule_id_text)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        seconds = normalize_campaign_show_seconds(int(seconds_text))
        await ctx.run_db(ctx.db.update_rule_repost_campaign_settings, rule_id, enabled=True, show_seconds=seconds)
        text, kb = build_repost_campaign_schedule_wizard_step4_view(rule_id=rule_id)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_step4:"))
    async def handle_rule_repost_campaign_schedule_step4(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        runtime = build_repost_campaign_runtime(ctx)
        readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
        if int(readiness.get("show_seconds") or 0) <= 0:
            text, kb = build_repost_campaign_schedule_wizard_step3_view(rule_id=rule_id, readiness=readiness)
        else:
            text, kb = build_repost_campaign_schedule_wizard_step4_view(rule_id=rule_id)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_quick:"))
    async def handle_rule_repost_campaign_schedule_quick(callback: CallbackQuery):
        _, rule_id_text, preset = (callback.data or "").split(":", 2)
        rule_id = int(rule_id_text)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        now = campaign_schedule_now_utc()
        local = now + timedelta(hours=3)
        if preset == "today_20":
            pick_local = local.replace(hour=20, minute=0, second=0, microsecond=0)
            if pick_local <= local:
                pick_local = pick_local + timedelta(days=1)
        elif preset == "tomorrow_12":
            pick_local = (local + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        else:
            pick_local = (local + timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
        scheduled_at_utc = (pick_local - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        runtime = build_repost_campaign_runtime(ctx)
        readiness = runtime.build_campaign_launch_readiness(rule_id=rule_id)
        if int(readiness.get("show_seconds") or 0) <= 0:
            text, kb = build_repost_campaign_schedule_wizard_step3_view(rule_id=rule_id, readiness=readiness)
        else:
            text, kb, policy_state = await _build_repost_campaign_schedule_policy_preview(
                ctx=ctx,
                rule_id=rule_id,
                scheduled_at_utc=scheduled_at_utc,
            )
            _clear_schedule_policy_ack(ctx, callback)
            _set_schedule_policy_ack(
                ctx,
                callback,
                rule_id=rule_id,
                scheduled_at_utc=scheduled_at_utc,
                action=str(policy_state.get("action") or ""),
            )
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_input:"))
    async def handle_rule_repost_campaign_schedule_input(callback: CallbackQuery):
        rule_id = int((callback.data or "").split(":")[1])
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        ctx.user_states[callback.from_user.id] = {"state": "repost_campaign_schedule_input", "rule_id": rule_id}
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="🕒 Запланировать запуск\n\nВведите дату и время запуска.\n\nФормат:\n09.05 18:00\n\nЧасовой пояс: UTC+3",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к выбору времени", callback_data=f"rule_repost_campaign_schedule_step4:{rule_id}")]]
            ),
        )

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_schedule_confirm:"))
    async def handle_rule_repost_campaign_schedule_confirm(callback: CallbackQuery):
        try:
            _, rule_id_text, epoch_text = (callback.data or "").split(":", 2)
            rule_id = int(rule_id_text)
            epoch = int(epoch_text)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        ctx.logger.info("REPOST_CAMPAIGN_SCHEDULE_CREATE_STARTED | rule_id=%s | admin_id=%s", rule_id, callback.from_user.id if callback.from_user else None)
        scheduled_at_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
        service = _build_repost_campaign_schedule_service(ctx)
        try:
            policy_state = await ctx.run_db(
                lambda: service.build_scheduled_launch_policy_state(rule_id=rule_id, scheduled_at_utc=scheduled_at_utc)
            )
        except Exception as exc:
            ctx.logger.warning(
                "REPOST_CAMPAIGN_SCHEDULE_CONFIRM_POLICY_FAILED | rule_id=%s | error=%s",
                rule_id,
                exc,
                exc_info=True,
            )
            policy_state = {
                "ok": False,
                "action": "policy_error",
                "can_schedule": False,
                "scheduled_at": scheduled_at_utc.isoformat(),
                "blocking_text": "Не удалось проверить Чистый канал",
            }
            text, kb = build_repost_campaign_schedule_preview_view(
                rule_id=rule_id,
                readiness={},
                scheduled_at_utc=scheduled_at_utc,
                scheduled_policy=policy_state,
            )
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
            await ctx.answer_callback_safe_once(callback)
            return
        action = str(policy_state.get("action") or "")
        can_schedule = policy_state.get("can_schedule") is True
        ctx.logger.info(
            "REPOST_CAMPAIGN_SCHEDULE_CONFIRM_POLICY_UI | rule_id=%s | action=%s | can_schedule=%s",
            rule_id,
            action,
            can_schedule,
        )
        if policy_state.get("ok") is False or not can_schedule:
            text, kb = build_repost_campaign_schedule_preview_view(
                rule_id=rule_id,
                readiness=policy_state.get("base_readiness") or {},
                scheduled_at_utc=scheduled_at_utc,
                scheduled_policy=policy_state,
            )
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
            await ctx.answer_callback_safe_once(callback)
            return
        allowed_policy_actions = {"allow", "schedule_with_clean_channel_wait", "schedule_with_overlap_warning"}
        if action not in allowed_policy_actions:
            text, kb = build_repost_campaign_schedule_preview_view(
                rule_id=rule_id,
                readiness=policy_state.get("base_readiness") or {},
                scheduled_at_utc=scheduled_at_utc,
                scheduled_policy=policy_state,
            )
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
            await ctx.answer_callback_safe_once(callback)
            return
        if action == "schedule_with_overlap_warning":
            if not _has_schedule_policy_ack(ctx, callback, rule_id=rule_id, scheduled_at_utc=scheduled_at_utc, action=action):
                _set_schedule_policy_ack(ctx, callback, rule_id=rule_id, scheduled_at_utc=scheduled_at_utc, action=action)
                ctx.logger.info("REPOST_CAMPAIGN_SCHEDULE_OVERLAP_WARNING_ACK_REQUIRED | rule_id=%s | epoch=%s", rule_id, epoch)
                text, kb = build_repost_campaign_schedule_preview_view(
                    rule_id=rule_id,
                    readiness=policy_state.get("base_readiness") or {},
                    scheduled_at_utc=scheduled_at_utc,
                    scheduled_policy=policy_state,
                )
                await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
                await ctx.answer_callback_safe_once(callback)
                return
            ctx.logger.info("REPOST_CAMPAIGN_SCHEDULE_OVERLAP_WARNING_ACK_ACCEPTED | rule_id=%s | epoch=%s", rule_id, epoch)
        result = service.schedule_campaign_launch(
            rule_id=rule_id,
            scheduled_at_utc=scheduled_at_utc,
            created_by=callback.from_user.id if callback.from_user else None,
            scheduled_policy=policy_state,
        )
        if result.ok:
            _clear_schedule_policy_ack(ctx, callback)
            row = await ctx.run_db(ctx.db.get_campaign_scheduled_launch, int((result.extra or {}).get("scheduled_launch_id")))
            text, kb = build_repost_campaign_schedule_result_view(rule_id=rule_id, scheduled_launch=row or {})
            ctx.logger.info("REPOST_CAMPAIGN_SCHEDULE_CREATE_DONE | rule_id=%s | action=%s", rule_id, action)
        else:
            readiness = (result.extra or {}).get("launch_readiness") or policy_state.get("base_readiness") or {}
            text, kb = build_repost_campaign_schedule_preview_view(rule_id=rule_id, readiness=readiness, scheduled_at_utc=scheduled_at_utc, scheduled_policy=policy_state)
            ctx.logger.warning("REPOST_CAMPAIGN_SCHEDULE_CREATE_FAILED | rule_id=%s | action=%s | error=%s", rule_id, action, result.error_text)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=kb)
        await ctx.answer_callback_safe_once(callback)
