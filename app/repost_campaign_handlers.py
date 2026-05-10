from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime
from app.repost_campaign_service import format_campaign_show_seconds_ru
from app.repost_campaign_ui import (
    build_repost_campaign_menu_view,
    build_repost_campaign_post_menu_view,
    build_repost_campaign_preview_delete_result_view,
    build_repost_campaign_target_preview_result_view,
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
            await callback.message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        else:
            ctx.logger.info("REPOST_CAMPAIGN_POST_MENU_RENDER_EDIT | rule_id=%s", rule_id)
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        return True
    except Exception as exc:
        ctx.logger.warning("REPOST_CAMPAIGN_SAVED_POST_MENU_FAILED | rule_id=%s | error=%s", rule_id, exc)
        await ctx.answer_callback_safe(callback, "Не удалось открыть меню рекламного поста", show_alert=True)
        return False


def _build_repost_campaign_post_preview_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Это рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        [InlineKeyboardButton(text="🔁 Заменить пост", callback_data=f"rule_repost_campaign_post_add:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к рекламному посту", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
    ])


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
            await callback.message.answer(text, reply_markup=keyboard)
        else:
            ctx.logger.warning("REPOST_CAMPAIGN_TARGET_PREVIEW_UI_FAILED | rule_id=%s | error=%s", rule_id, result.error_text)
            error_text = "❌ Не удалось показать рекламный пост\n\n" f"{result.error_text or 'Неизвестная ошибка'}"
            await callback.message.answer(error_text)
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
            await callback.message.answer("Предпросмотр уже не найден. Отправьте его заново.")
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
        await callback.message.answer(text, reply_markup=keyboard)
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
            await callback.message.answer(error_text)
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

        await callback.message.answer(
            "✅ Тестовый запуск выполнен\n\nРекламный пост отправлен в основной канал правила.\n"
            f"Message ID: {result.message_id}\n"
            f"Метод: {result.method}"
        )
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
