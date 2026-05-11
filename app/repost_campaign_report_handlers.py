from __future__ import annotations

import asyncio

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime
from app.repost_campaign_ui import (
    build_repost_campaign_post_channels_stats_view,
    build_repost_campaign_post_stats_loading_view,
    build_repost_campaign_post_stats_view,
    build_repost_campaign_posts_library_view,
    build_repost_campaign_run_details_view,
    build_repost_campaign_views_report_error_view,
    build_repost_campaign_views_report_loading_view,
    build_repost_campaign_views_report_view,
)


def register_repost_campaign_report_handlers(dp: Dispatcher, ctx: RepostCampaignHandlersContext) -> None:
    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_history:"))
    async def handle_rule_repost_campaign_history(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            rule_id = int(callback.data.split(":")[1])
            ctx.logger.info("REPOST_CAMPAIGN_POST_LIBRARY_UI_OPENED_FAST | rule_id=%s", rule_id)
            runtime = build_repost_campaign_runtime(ctx)
            library = await runtime.build_campaign_posts_library(rule_id=rule_id)
            text, keyboard = build_repost_campaign_posts_library_view(rule_id=rule_id, library=library)
            await ctx.answer_callback_safe_once(callback)
            if ctx.should_answer_new_message_for_callback(callback):
                await ctx.send_message_safe(chat_id=callback.from_user.id, text=text, reply_markup=keyboard)
            else:
                await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        except Exception:
            ctx.logger.exception("REPOST_CAMPAIGN_POST_LIBRARY_UI_FAILED | rule_id=%s | error=%s", callback.data, callback.data)
            await ctx.answer_callback_safe(callback, "Не удалось открыть библиотеку постов", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_history_detail:"))
    async def handle_rule_repost_campaign_history_detail(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
            rule_id = int(rule_id_raw)
            run_id = int(run_id_raw)
            ctx.logger.info("REPOST_CAMPAIGN_RUN_DETAILS_UI_OPENED | rule_id=%s | run_id=%s", rule_id, run_id)
            runtime = build_repost_campaign_runtime(ctx)
            details = await ctx.run_db(lambda: runtime.get_campaign_run_details(rule_id=rule_id, run_id=run_id))
            text, keyboard = build_repost_campaign_run_details_view(rule_id=rule_id, details=details)
            await ctx.answer_callback_safe_once(callback)
            if ctx.should_answer_new_message_for_callback(callback):
                await ctx.send_message_safe(chat_id=callback.from_user.id, text=text, reply_markup=keyboard)
            else:
                await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        except Exception:
            ctx.logger.exception(
                "REPOST_CAMPAIGN_RUN_DETAILS_UI_FAILED | rule_id=%s | run_id=%s | error=%s",
                callback.data,
                callback.data,
                callback.data,
            )
            await ctx.answer_callback_safe(callback, "Не удалось открыть детали запуска", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_views_report:"))
    async def handle_rule_repost_campaign_views_report(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        rule_id = 0
        run_id = 0
        try:
            _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
            rule_id = int(rule_id_raw)
            run_id = int(run_id_raw)
            started = asyncio.get_event_loop().time()
            ctx.logger.info("REPOST_CAMPAIGN_VIEWS_REPORT_UI_OPENED | rule_id=%s | run_id=%s", rule_id, run_id)
            await ctx.answer_callback_safe_once(callback)
            loading_text, loading_kb = build_repost_campaign_views_report_loading_view(rule_id=rule_id, run_id=run_id)
            await ctx.edit_message_text_safe(message=callback.message, text=loading_text, reply_markup=loading_kb)
            ctx.logger.info("REPOST_CAMPAIGN_VIEWS_REPORT_LOADING_SHOWN | rule_id=%s | run_id=%s", rule_id, run_id)
            runtime = build_repost_campaign_runtime(ctx)
            report = await runtime.build_campaign_views_report(rule_id=rule_id, run_id=run_id)
            text, keyboard = build_repost_campaign_views_report_view(rule_id=rule_id, run_id=run_id, report=report)
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
            duration_ms = int((asyncio.get_event_loop().time() - started) * 1000)
            ctx.logger.info("REPOST_CAMPAIGN_VIEWS_REPORT_UI_BUILT | rule_id=%s | run_id=%s | duration_ms=%s", rule_id, run_id, duration_ms)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_VIEWS_REPORT_UI_FAILED | rule_id=%s | run_id=%s | error=%s", rule_id, run_id, exc)
            text, keyboard = build_repost_campaign_views_report_error_view(rule_id=rule_id, run_id=run_id)
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_stats:"))
    async def handle_rule_repost_campaign_post_stats(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        _, rule_id_raw, saved_post_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        saved_post_id = int(saved_post_id_raw)
        started = asyncio.get_event_loop().time()
        await ctx.answer_callback_safe_once(callback)
        loading_text, loading_kb = build_repost_campaign_post_stats_loading_view(rule_id=rule_id, saved_post_id=saved_post_id)
        await ctx.edit_message_text_safe(message=callback.message, text=loading_text, reply_markup=loading_kb)
        ctx.logger.info("REPOST_CAMPAIGN_POST_STATS_LOADING_SHOWN | rule_id=%s | saved_post_id=%s", rule_id, saved_post_id)
        try:
            runtime = build_repost_campaign_runtime(ctx)
            stats = await runtime.build_campaign_post_stats(rule_id=rule_id, saved_post_id=saved_post_id, include_live_views=True)
            text, keyboard = build_repost_campaign_post_stats_view(rule_id=rule_id, saved_post_id=saved_post_id, stats=stats)
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
            duration_ms = int((asyncio.get_event_loop().time() - started) * 1000)
            ctx.logger.info("REPOST_CAMPAIGN_POST_STATS_UI_BUILT | rule_id=%s | saved_post_id=%s | duration_ms=%s", rule_id, saved_post_id, duration_ms)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_POST_STATS_UI_FAILED | rule_id=%s | saved_post_id=%s | error=%s", rule_id, saved_post_id, exc)
            text = (
                "📄 Рекламный пост\n\n"
                "⚠️ Статистику просмотров сейчас получить не удалось.\n\n"
                "Пост и история размещений доступны.\n"
                "Попробуйте обновить статистику позже."
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить статистику", callback_data=f"rule_repost_campaign_post_stats:{rule_id}:{saved_post_id}")],
                [InlineKeyboardButton(text="📚 К библиотеке", callback_data=f"rule_repost_campaign_history:{rule_id}")],
                [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
            ])
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_channels_stats:"))
    async def handle_rule_repost_campaign_post_channels_stats(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        try:
            _, rule_id_raw, saved_post_id_raw, offset_raw = callback.data.split(":", 3)
            rule_id = int(rule_id_raw)
            saved_post_id = int(saved_post_id_raw)
            offset = int(offset_raw)
        except Exception:
            await ctx.answer_callback_safe(callback, "Ошибка данных", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        try:
            runtime = build_repost_campaign_runtime(ctx)
            stats = await runtime.build_campaign_post_stats(rule_id=rule_id, saved_post_id=saved_post_id, include_live_views=True)
            text, keyboard = build_repost_campaign_post_channels_stats_view(
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                stats=stats,
                offset=offset,
                page_size=10,
            )
            await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_POST_CHANNELS_STATS_UI_FAILED | rule_id=%s | saved_post_id=%s | offset=%s | error=%s", rule_id, saved_post_id, offset, exc)
            await ctx.answer_callback_safe(callback, "Не удалось открыть статистику по каналам", show_alert=True)
