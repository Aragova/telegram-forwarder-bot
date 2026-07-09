from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_context import RepostCampaignHandlersContext, build_repost_campaign_runtime
from app.repost_campaign_top_time_view_service import RepostCampaignTopTimeViewService
from app.repost_campaign_export_service import (
    build_campaign_post_stats_csv,
    build_campaign_post_stats_txt,
    build_campaign_post_stats_xlsx,
    build_campaign_run_report_csv,
    build_campaign_run_report_txt,
    build_campaign_run_report_xlsx,
)
from app.repost_campaign_ui import (
    build_repost_campaign_delete_result_view,
    build_repost_campaign_post_channels_stats_view,
    build_repost_campaign_post_stats_loading_view,
    build_repost_campaign_post_stats_view,
    build_repost_campaign_posts_library_view,
    build_repost_campaign_run_details_view,
    build_repost_campaign_run_delete_confirm_view,
    build_repost_campaign_run_delete_failures_resolve_confirm_view,
    build_repost_campaign_run_delete_failures_resolve_result_view,
    build_repost_campaign_run_delete_loading_view,
    build_repost_campaign_run_delete_result_view,
    build_repost_campaign_views_report_error_view,
    build_repost_campaign_views_report_loading_view,
    build_repost_campaign_views_report_view,
)

async def _send_export_document(ctx: RepostCampaignHandlersContext, callback: CallbackQuery, *, filename: str, content: bytes) -> None:
    tmp_path: Path | None = None
    try:
        suffix = Path(filename).suffix or ".txt"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        if ctx.send_document_safe is not None:
            await ctx.send_document_safe(
                chat_id=callback.message.chat.id,
                document=FSInputFile(str(tmp_path), filename=filename),
            )
    finally:
        if tmp_path:
            tmp_path.unlink(missing_ok=True)


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
            if (details or {}).get("ok"):
                run = (details or {}).get("run") or {}
                service = RepostCampaignTopTimeViewService(ctx.db, logger=ctx.logger)
                details = dict(details or {})
                details["top_time_summary"] = await ctx.run_db(
                    lambda: service.build_run_top_time_summary(
                        run_id,
                        top_time_enabled_snapshot=bool(run.get("top_time_enabled_snapshot")),
                        top_time_seconds_snapshot=int(run.get("top_time_seconds_snapshot") or 0),
                    )
                )
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



    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_run_delete_confirm:"))
    async def handle_rule_repost_campaign_run_delete_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        run_id = int(run_id_raw)
        runtime = build_repost_campaign_runtime(ctx)
        details = await ctx.run_db(lambda: runtime.get_campaign_run_details(rule_id=rule_id, run_id=run_id))
        text, keyboard = build_repost_campaign_run_delete_confirm_view(rule_id=rule_id, run_id=run_id, details=details)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)


    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_run_delete_now:"))
    async def handle_rule_repost_campaign_run_delete_now(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        run_id = int(run_id_raw)
        await ctx.answer_callback_safe_once(callback)
        loading_text, loading_kb = build_repost_campaign_run_delete_loading_view(rule_id=rule_id, run_id=run_id)
        await ctx.edit_message_text_safe(message=callback.message, text=loading_text, reply_markup=loading_kb)
        runtime = build_repost_campaign_runtime(ctx)
        result = await runtime.delete_campaign_run_now(
            rule_id=rule_id,
            run_id=run_id,
            admin_id=callback.from_user.id if callback.from_user else None,
        )
        result_text, result_kb = build_repost_campaign_run_delete_result_view(rule_id=rule_id, run_id=run_id, result=result)
        await ctx.edit_message_text_safe(message=callback.message, text=result_text, reply_markup=result_kb)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_run_delete_failures_resolve_confirm:"))
    async def handle_rule_repost_campaign_run_delete_failures_resolve_confirm(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        run_id = int(run_id_raw)
        runtime = build_repost_campaign_runtime(ctx)
        details = await ctx.run_db(lambda: runtime.get_campaign_run_details(rule_id=rule_id, run_id=run_id))
        text, keyboard = build_repost_campaign_run_delete_failures_resolve_confirm_view(rule_id=rule_id, run_id=run_id, details=details)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)


    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_run_delete_failures_resolve_apply:"))
    async def handle_rule_repost_campaign_run_delete_failures_resolve_apply(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        run_id = int(run_id_raw)
        runtime = build_repost_campaign_runtime(ctx)
        result = await runtime.resolve_campaign_run_delete_failures(
            rule_id=rule_id,
            run_id=run_id,
            admin_id=callback.from_user.id if callback.from_user else None,
        )
        text, keyboard = build_repost_campaign_run_delete_failures_resolve_result_view(rule_id=rule_id, run_id=run_id, result=result)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)


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

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_use:"))
    async def handle_rule_repost_campaign_post_use(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        _, rule_id_raw, saved_post_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        saved_post_id = int(saved_post_id_raw)
        runtime = build_repost_campaign_runtime(ctx)
        result = await ctx.run_db(
            lambda: runtime.select_campaign_saved_post_from_library(
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                admin_id=callback.from_user.id if callback.from_user else None,
            )
        )
        if result.get("ok"):
            text = (
                "✅ Пост выбран\n\n"
                "Этот рекламный пост теперь используется в кампании.\n\n"
                "Перед запуском ViMi ещё раз проверит:\n"
                "• каналы/группы;\n"
                "• время показа;\n"
                "• права публикации.\n\n"
                "Запуск не выполнен автоматически."
            )
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🚀 Проверить и запустить", callback_data=f"rule_repost_campaign_launch:{rule_id}")],
                    [InlineKeyboardButton(text="📚 К библиотеке", callback_data=f"rule_repost_campaign_history:{rule_id}")],
                    [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
                ]
            )
        else:
            text = f"❌ {result.get('error_text') or 'Не удалось выбрать пост'}"
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="📚 К библиотеке", callback_data=f"rule_repost_campaign_history:{rule_id}")]]
            )
        ctx.logger.info("REPOST_CAMPAIGN_POST_USE_FROM_LIBRARY | rule_id=%s | saved_post_id=%s | ok=%s", rule_id, saved_post_id, bool(result.get("ok")))
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_delete_message:"))
    async def handle_rule_repost_campaign_delete_message(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        if not ctx.settings.repost_campaign_admin_test_enabled:
            await ctx.answer_callback_safe(callback, "Функция пока выключена", show_alert=True)
            return
        try:
            _, rule_id_raw, run_id_raw, run_message_id_raw = callback.data.split(":", 3)
            rule_id = int(rule_id_raw)
            run_id = int(run_id_raw)
            run_message_id = int(run_message_id_raw)
            runtime = build_repost_campaign_runtime(ctx)
            result = await runtime.delete_campaign_run_message_now(
                rule_id=rule_id,
                run_id=run_id,
                run_message_id=run_message_id,
                admin_id=callback.from_user.id if callback.from_user else None,
            )
            text, keyboard = build_repost_campaign_delete_result_view(rule_id=rule_id, result=result)
            await ctx.answer_callback_safe_once(callback)
            if ctx.should_answer_new_message_for_callback(callback):
                await ctx.send_message_safe(chat_id=callback.from_user.id, text=text, reply_markup=keyboard)
            else:
                await ctx.edit_message_text_safe(message=callback.message, text=text, reply_markup=keyboard)
            ctx.logger.info(
                "REPOST_CAMPAIGN_MANUAL_DELETE_UI_DONE | rule_id=%s | run_id=%s | run_message_id=%s | ok=%s",
                rule_id,
                run_id,
                run_message_id,
                result.ok,
            )
        except Exception as exc:
            ctx.logger.exception(
                "REPOST_CAMPAIGN_MANUAL_DELETE_UI_FAILED | rule_id=%s | run_id=%s | run_message_id=%s | error=%s",
                callback.data,
                callback.data,
                callback.data,
                exc,
            )
            await ctx.answer_callback_safe(callback, "Не удалось выполнить удаление публикации", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_views_export_csv:"))
    async def handle_rule_repost_campaign_views_export_csv(callback: CallbackQuery):
        _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        run_id = int(run_id_raw)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        try:
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_STARTED | rule_id=%s | run_id=%s | format=csv", rule_id, run_id)
            runtime = build_repost_campaign_runtime(ctx)
            report = await runtime.build_campaign_views_report(rule_id=rule_id, run_id=run_id)
            if not report or not report.get("ok"):
                await ctx.answer_callback_safe(callback, "❌ Отчёт не найден. Обновите экран и попробуйте ещё раз.", show_alert=True)
                return
            payload = build_campaign_run_report_csv(report)
            await _send_export_document(ctx, callback, filename=f"campaign_run_{run_id}_report.csv", content=payload)
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_DONE | rule_id=%s | run_id=%s | format=csv | size_bytes=%s", rule_id, run_id, len(payload))
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_EXPORT_FAILED | rule_id=%s | run_id=%s | format=csv | error=%s", rule_id, run_id, exc)
            await ctx.answer_callback_safe(callback, "❌ Не удалось отправить файл отчёта.", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_views_export_xlsx:"))
    async def handle_rule_repost_campaign_views_export_xlsx(callback: CallbackQuery):
        _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        run_id = int(run_id_raw)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        try:
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_STARTED | rule_id=%s | run_id=%s | format=xlsx", rule_id, run_id)
            runtime = build_repost_campaign_runtime(ctx)
            report = await runtime.build_campaign_views_report(rule_id=rule_id, run_id=run_id)
            if not report or not report.get("ok"):
                await ctx.answer_callback_safe(callback, "❌ Отчёт не найден. Обновите экран и попробуйте ещё раз.", show_alert=True)
                return
            payload = build_campaign_run_report_xlsx(report)
            await _send_export_document(ctx, callback, filename=f"campaign_run_{run_id}_report.xlsx", content=payload)
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_DONE | rule_id=%s | run_id=%s | format=xlsx | size_bytes=%s", rule_id, run_id, len(payload))
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_EXPORT_FAILED | rule_id=%s | run_id=%s | format=xlsx | error=%s", rule_id, run_id, exc)
            await ctx.answer_callback_safe(callback, "❌ Не удалось отправить файл отчёта.", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_views_export_txt:"))
    async def handle_rule_repost_campaign_views_export_txt(callback: CallbackQuery):
        _, rule_id_raw, run_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        run_id = int(run_id_raw)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        try:
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_STARTED | rule_id=%s | run_id=%s | format=txt", rule_id, run_id)
            runtime = build_repost_campaign_runtime(ctx)
            report = await runtime.build_campaign_views_report(rule_id=rule_id, run_id=run_id)
            if not report or not report.get("ok"):
                await ctx.answer_callback_safe(callback, "❌ Отчёт не найден. Обновите экран и попробуйте ещё раз.", show_alert=True)
                return
            payload = build_campaign_run_report_txt(report).encode("utf-8")
            await _send_export_document(ctx, callback, filename=f"campaign_run_{run_id}_report.txt", content=payload)
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_DONE | rule_id=%s | run_id=%s | format=txt | size_bytes=%s", rule_id, run_id, len(payload))
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_EXPORT_FAILED | rule_id=%s | run_id=%s | format=txt | error=%s", rule_id, run_id, exc)
            await ctx.answer_callback_safe(callback, "❌ Не удалось отправить файл отчёта.", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_export_csv:"))
    async def handle_rule_repost_campaign_post_export_csv(callback: CallbackQuery):
        _, rule_id_raw, saved_post_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        saved_post_id = int(saved_post_id_raw)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        try:
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_STARTED | rule_id=%s | saved_post_id=%s | format=csv", rule_id, saved_post_id)
            runtime = build_repost_campaign_runtime(ctx)
            stats = await runtime.build_campaign_post_stats(rule_id=rule_id, saved_post_id=saved_post_id, include_live_views=True)
            if not stats or not stats.get("ok"):
                await ctx.answer_callback_safe(callback, "❌ Отчёт не найден. Обновите экран и попробуйте ещё раз.", show_alert=True)
                return
            payload = build_campaign_post_stats_csv(stats)
            await _send_export_document(ctx, callback, filename=f"campaign_post_{saved_post_id}_stats.csv", content=payload)
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_DONE | rule_id=%s | saved_post_id=%s | format=csv | size_bytes=%s", rule_id, saved_post_id, len(payload))
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_EXPORT_FAILED | rule_id=%s | saved_post_id=%s | format=csv | error=%s", rule_id, saved_post_id, exc)
            await ctx.answer_callback_safe(callback, "❌ Не удалось отправить файл отчёта.", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_export_xlsx:"))
    async def handle_rule_repost_campaign_post_export_xlsx(callback: CallbackQuery):
        _, rule_id_raw, saved_post_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        saved_post_id = int(saved_post_id_raw)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        try:
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_STARTED | rule_id=%s | saved_post_id=%s | format=xlsx", rule_id, saved_post_id)
            runtime = build_repost_campaign_runtime(ctx)
            stats = await runtime.build_campaign_post_stats(rule_id=rule_id, saved_post_id=saved_post_id, include_live_views=True)
            if not stats or not stats.get("ok"):
                await ctx.answer_callback_safe(callback, "❌ Отчёт не найден. Обновите экран и попробуйте ещё раз.", show_alert=True)
                return
            payload = build_campaign_post_stats_xlsx(stats)
            await _send_export_document(ctx, callback, filename=f"campaign_post_{saved_post_id}_stats.xlsx", content=payload)
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_DONE | rule_id=%s | saved_post_id=%s | format=xlsx | size_bytes=%s", rule_id, saved_post_id, len(payload))
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_EXPORT_FAILED | rule_id=%s | saved_post_id=%s | format=xlsx | error=%s", rule_id, saved_post_id, exc)
            await ctx.answer_callback_safe(callback, "❌ Не удалось отправить файл отчёта.", show_alert=True)

    @dp.callback_query(lambda c: c.data.startswith("rule_repost_campaign_post_export_txt:"))
    async def handle_rule_repost_campaign_post_export_txt(callback: CallbackQuery):
        _, rule_id_raw, saved_post_id_raw = callback.data.split(":", 2)
        rule_id = int(rule_id_raw)
        saved_post_id = int(saved_post_id_raw)
        if not await ctx.ensure_rule_callback_access(callback, rule_id):
            return
        try:
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_STARTED | rule_id=%s | saved_post_id=%s | format=txt", rule_id, saved_post_id)
            runtime = build_repost_campaign_runtime(ctx)
            stats = await runtime.build_campaign_post_stats(rule_id=rule_id, saved_post_id=saved_post_id, include_live_views=True)
            if not stats or not stats.get("ok"):
                await ctx.answer_callback_safe(callback, "❌ Отчёт не найден. Обновите экран и попробуйте ещё раз.", show_alert=True)
                return
            payload = build_campaign_post_stats_txt(stats).encode("utf-8")
            await _send_export_document(ctx, callback, filename=f"campaign_post_{saved_post_id}_stats.txt", content=payload)
            ctx.logger.info("REPOST_CAMPAIGN_EXPORT_DONE | rule_id=%s | saved_post_id=%s | format=txt | size_bytes=%s", rule_id, saved_post_id, len(payload))
            await ctx.answer_callback_safe_once(callback)
        except Exception as exc:
            ctx.logger.exception("REPOST_CAMPAIGN_EXPORT_FAILED | rule_id=%s | saved_post_id=%s | format=txt | error=%s", rule_id, saved_post_id, exc)
            await ctx.answer_callback_safe(callback, "❌ Не удалось отправить файл отчёта.", show_alert=True)
