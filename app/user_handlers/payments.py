from __future__ import annotations

import logging
from typing import Any

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app import user_ui
from app.config import settings
from app.payments import LavaTopAPIError, PaymentService as LavaPaymentService
from .context import UserHandlersContext


RECOVERY_CTA_EVENT_TYPE = "recovery_cta_shown_after_payment"
LAVA_PAYMENT_LOGGER = logging.getLogger("forwarder.payments.lava")


def _admin_manual_payment_keyboard(payment_intent_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_manual_payment:{int(payment_intent_id)}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_manual_payment:{int(payment_intent_id)}"),
            ]
        ]
    )


def _is_recovery_cta_already_shown(events: list[dict[str, Any]], payment_intent_id: int) -> bool:
    for event in events:
        if str(event.get("event_type") or "") != RECOVERY_CTA_EVENT_TYPE:
            continue
        metadata = event.get("metadata_json") if isinstance(event.get("metadata_json"), dict) else {}
        if int(metadata.get("payment_intent_id") or 0) != int(payment_intent_id):
            continue
        if bool(metadata.get("already_shown")):
            return True
    return False


def register_user_payment_handlers(dp: Dispatcher, ctx: UserHandlersContext) -> None:

    @dp.callback_query(lambda c: c.data == "user_subscription")
    async def handle_user_subscription_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_lava_subscription_text(),
            reply_markup=user_ui.build_lava_subscription_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_tariff_basic")
    async def handle_user_tariff_basic_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_lava_subscription_text(),
            reply_markup=user_ui.build_lava_subscription_keyboard(),
        )

    @dp.callback_query(lambda c: c.data == "user_pay_lava_basic")
    async def handle_user_pay_lava_basic_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback, "⏳ Создаю ссылку на оплату…")

        lava_service = LavaPaymentService()
        username = callback.from_user.username if callback.from_user else None
        LAVA_PAYMENT_LOGGER.info(
            "Старт создания invoice Lava.top user_id=%s tariff_code=basic provider=lava_top",
            user_id,
        )
        try:
            invoice_view = await lava_service.create_lava_basic_invoice(
                user_id=user_id,
                username=username,
            )
            if not invoice_view.payment_url:
                raise LavaTopAPIError("Lava.top вернул пустую ссылку оплаты")
        except Exception as exc:
            status_code = exc.status_code if isinstance(exc, LavaTopAPIError) else None
            LAVA_PAYMENT_LOGGER.exception(
                "Ошибка создания invoice Lava.top user_id=%s tariff_code=basic status_code=%s",
                user_id,
                status_code,
            )
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=(
                    "Не удалось создать ссылку оплаты. "
                    "Попробуйте позже или напишите в поддержку."
                ),
                reply_markup=user_ui.build_lava_subscription_keyboard(),
            )
            return

        LAVA_PAYMENT_LOGGER.info(
            "Invoice Lava.top создан user_id=%s tariff_code=basic invoice_id=%s amount=%s currency=%s payment_url=%s",
            user_id,
            invoice_view.invoice_id,
            invoice_view.amount,
            invoice_view.currency,
            bool(invoice_view.payment_url),
        )
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_lava_invoice_created_text(
                invoice_id=0,
                tariff_title="BASIC",
                amount=invoice_view.amount,
                currency=invoice_view.currency,
            ),
            reply_markup=user_ui.build_lava_invoice_keyboard(invoice_id=0, payment_url=invoice_view.payment_url),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_select_plan:"))
    async def handle_user_select_plan_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        plan_name = str((callback.data or "").split(":", 1)[1] if ":" in (callback.data or "") else "").upper()
        ctx.logger.info("пользователь выбрал тариф plan_name=%s user_id=%s tenant_id=%s", plan_name, user_id, tenant_id)
        if plan_name == "OWNER":
            await ctx.answer_callback_safe(callback, "Тариф OWNER недоступен", show_alert=True)
            return
        if plan_name == "FREE":
            await ctx.answer_callback_safe_once(callback)
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="Тариф FREE не требует создания счёта.",
                reply_markup=ctx.public_plans_keyboard(),
            )
            return
        if plan_name not in {"BASIC", "PRO"}:
            await ctx.answer_callback_safe(callback, "Разрешены только тарифы BASIC и PRO", show_alert=True)
            return
        current_sub = await ctx.run_db(ctx.subscription_service.get_active_subscription, tenant_id) or {}
        current_plan = str(current_sub.get("plan_name") or "FREE").upper()
        if current_plan == plan_name:
            await ctx.answer_callback_safe_once(callback, "Этот тариф уже подключён")
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="✅ Этот тариф уже активен.\n\nВыберите другой план или вернитесь назад.",
                reply_markup=ctx.public_plans_keyboard(current_plan),
            )
            return
        plan = ctx.get_plan_info(plan_name, "ru")
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_plan_confirmation_text(plan),
            reply_markup=user_ui.build_user_plan_confirmation_keyboard(plan_name),
        )
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_confirm_plan:"))
    async def handle_user_confirm_plan_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        plan_name = str((callback.data or "").split(":", 1)[1] if ":" in (callback.data or "") else "").upper()
        if plan_name not in {"BASIC", "PRO"}:
            await ctx.answer_callback_safe(callback, "Разрешены только тарифы BASIC и PRO", show_alert=True)
            return

        invoices = await ctx.run_db(ctx.get_user_invoices_payload, tenant_id, 10)
        for invoice in invoices:
            status = str(invoice.get("status") or "")
            if status not in {"open", "draft"}:
                continue
            if ctx.invoice_plan_name(invoice, invoice.get("items") or []) == plan_name:
                await ctx.answer_callback_safe_once(callback)
                await ctx.edit_message_text_safe(
                    message=callback.message,
                    text=user_ui.build_user_invoice_text(invoice, invoice.get("items") or []),
                    reply_markup=ctx.public_invoice_keyboard(int(invoice.get("id") or 0)),
                )
                return

        sub = await ctx.run_db(ctx.subscription_service.get_active_subscription, tenant_id)
        if not sub:
            await ctx.answer_callback_safe(callback, "Не удалось найти активную подписку", show_alert=True)
            return
        sub = await ctx.run_db(ctx.billing_service.ensure_billing_period, sub)
        plan = ctx.get_plan_info(plan_name, "ru")
        invoice_id = await ctx.run_db(
            ctx.invoice_service.create_draft_invoice,
            int(tenant_id),
            int(sub.get("id") or 0),
            str(sub.get("current_period_start")),
            str(sub.get("current_period_end")),
            currency="USD",
        )
        if not invoice_id:
            await ctx.answer_callback_safe(callback, "Не удалось создать счёт", show_alert=True)
            return
        await ctx.run_db(
            ctx.invoice_service.add_invoice_item,
            int(invoice_id),
            item_type="base_plan",
            description=plan_name,
            quantity=1,
            unit_price=float(plan.get("price") or 0),
            metadata={"plan_name": plan_name},
        )
        await ctx.run_db(ctx.invoice_service.finalize_invoice, int(invoice_id))
        invoice = await ctx.run_db(ctx.db.get_invoice, int(invoice_id)) if hasattr(ctx.db, "get_invoice") else {"id": int(invoice_id), "status": "open", "total": float(plan.get("price") or 0), "currency": "USD"}
        items = await ctx.run_db(ctx.db.list_invoice_items, int(invoice_id)) if hasattr(ctx.db, "list_invoice_items") else []
        ctx.logger.info("создан счёт invoice_id=%s tenant_id=%s plan=%s", invoice_id, tenant_id, plan_name)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_invoice_text(invoice, items),
            reply_markup=ctx.public_invoice_keyboard(int(invoice_id)),
        )

    @dp.callback_query(lambda c: c.data == "user_invoices")
    async def handle_user_invoices_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        user_id = callback.from_user.id if callback.from_user else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoices = await ctx.run_db(ctx.get_user_invoices_payload, tenant_id, 10)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_invoices_text(invoices),
            reply_markup=user_ui.build_user_invoices_keyboard(invoices),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice:"))
    async def handle_user_invoice_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        invoice_id = int((callback.data or "0").split(":", 1)[1] or 0)
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        items = await ctx.run_db(ctx.db.list_invoice_items, invoice_id) if hasattr(ctx.db, "list_invoice_items") else []
        if not invoice:
            await ctx.answer_callback_safe(callback, "Счёт не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка открыть чужой счёт user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_invoice_text(invoice, items),
            reply_markup=ctx.public_invoice_keyboard(invoice_id),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice_pay:"))
    async def handle_user_invoice_pay_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        invoice_id = int((callback.data or "0").split(":", 1)[1] or 0)
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        if not invoice:
            await ctx.answer_callback_safe(callback, "Счёт не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка открыть оплату чужого счёта user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
        methods = await ctx.run_db(ctx.payment_service.list_available_methods, invoice)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_payment_methods_text(invoice, methods),
            reply_markup=user_ui.build_user_payment_methods_keyboard(invoice_id, methods),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_pay_provider:"))
    async def handle_user_pay_provider_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        parts = (callback.data or "").split(":")
        if len(parts) < 3:
            await ctx.answer_callback_safe(callback, "Некорректные данные оплаты", show_alert=True)
            return
        invoice_id = int(parts[1] or 0)
        provider = str(parts[2] or "")
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        if not invoice:
            await ctx.answer_callback_safe(callback, "Счёт не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка оплаты чужого счёта user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
        payment_result = await ctx.run_db(ctx.payment_service.start_payment, invoice, provider, user_id=user_id)
        await ctx.answer_callback_safe_once(callback)
        methods = await ctx.run_db(ctx.payment_service.list_available_methods, invoice)
        if not payment_result:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=user_ui.build_user_payment_methods_text(invoice, methods),
                reply_markup=user_ui.build_user_payment_methods_keyboard(invoice_id, methods),
            )
            return
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_payment_result_text(invoice, payment_result),
            reply_markup=user_ui.build_user_payment_result_keyboard(invoice_id, payment_result),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice_pay_lava:"))
    async def handle_user_invoice_pay_lava_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        raw = (callback.data or "").split(":", 1)
        invoice_id = int(raw[1] or 0) if len(raw) > 1 else 0
        if invoice_id <= 0:
            await ctx.answer_callback_safe(callback, "Некорректный счёт", show_alert=True)
            return
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        if not invoice:
            await ctx.answer_callback_safe(callback, "Счёт не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка Lava оплаты чужого счёта user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
        if str(invoice.get("status") or "").lower() not in {"open", "pending"}:
            await ctx.answer_callback_safe(callback, "Этот счёт нельзя оплатить через Lava.top", show_alert=True)
            return
        if not settings.lava_top_enabled:
            await ctx.answer_callback_safe(callback, "Оплата через Lava.top сейчас выключена", show_alert=True)
            return
        items = await ctx.run_db(ctx.db.list_invoice_items, invoice_id) if hasattr(ctx.db, "list_invoice_items") else []
        plan_name = (ctx.invoice_plan_name(invoice, items) if callable(ctx.invoice_plan_name) else "BASIC") or "BASIC"
        tariff_code = str(plan_name).strip().lower()
        amount = float(invoice.get("total") or 0)
        currency = str(invoice.get("currency") or "USD").upper()

        await ctx.answer_callback_safe_once(callback, "⏳ Создаю ссылку на оплату…")
        lava_service = LavaPaymentService()
        username = callback.from_user.username if callback.from_user else None
        try:
            invoice_view = await lava_service.create_lava_invoice_for_user_invoice(
                user_id=user_id,
                invoice_id=invoice_id,
                tariff_code=tariff_code,
                amount=amount,
                currency=currency,
                username=username,
            )
            if not invoice_view.payment_url:
                raise LavaTopAPIError("Lava.top вернул пустую ссылку оплаты")
        except Exception as exc:
            status_code = exc.status_code if isinstance(exc, LavaTopAPIError) else None
            LAVA_PAYMENT_LOGGER.warning(
                "Ошибка создания invoice Lava.top user_id=%s internal_invoice_id=%s tariff_code=%s provider=lava_top status_code=%s",
                user_id,
                invoice_id,
                tariff_code,
                status_code,
            )
            methods = await ctx.run_db(ctx.payment_service.list_available_methods, invoice)
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="Не удалось создать ссылку оплаты Lava.top. Попробуйте позже или выберите другой способ оплаты.",
                reply_markup=user_ui.build_user_payment_methods_keyboard(invoice_id, methods),
            )
            return

        LAVA_PAYMENT_LOGGER.info(
            "Invoice Lava.top создан user_id=%s internal_invoice_id=%s tariff_code=%s provider=lava_top lava_invoice_id=%s amount=%s currency=%s payment_url_present=%s",
            user_id,
            invoice_id,
            tariff_code,
            invoice_view.invoice_id,
            invoice_view.amount,
            invoice_view.currency,
            bool(invoice_view.payment_url),
        )
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_lava_invoice_created_text(
                invoice_id=invoice_id,
                tariff_title=str(plan_name).upper(),
                amount=invoice_view.amount,
                currency=invoice_view.currency,
            ),
            reply_markup=user_ui.build_lava_invoice_keyboard(invoice_id=invoice_id, payment_url=invoice_view.payment_url),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_upload_receipt:"))
    async def handle_user_upload_receipt_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        invoice_id = int((callback.data or "0").split(":", 1)[1] or 0)
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        if not invoice:
            await ctx.answer_callback_safe(callback, "Счёт не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка загрузить чек по чужому счёту user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
        intent = await ctx.find_active_manual_payment_intent_for_invoice(int(invoice_id))
        if not intent:
            await ctx.answer_callback_safe(callback, "❌ Активная ручная оплата не найдена", show_alert=True)
            return
        payment_intent_id = int(intent.get("id") or 0)
        ctx.user_states[user_id] = {
            "action": "awaiting_payment_receipt",
            "invoice_id": int(invoice_id),
            "payment_intent_id": payment_intent_id,
        }
        ctx.logger.info("пользователь начал загрузку чека invoice_id=%s intent_id=%s", invoice_id, payment_intent_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_manual_receipt_request_text(invoice, intent),
            reply_markup=user_ui.build_user_manual_receipt_keyboard(invoice_id),
            parse_mode="HTML",
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_payment_status:"))
    async def handle_user_payment_status_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        invoice_id = int((callback.data or "0").split(":", 1)[1] or 0)
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        if not invoice:
            await ctx.answer_callback_safe(callback, "Счёт не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка просмотра статуса чужой оплаты user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
        payment_intent = await ctx.find_latest_payment_intent_for_invoice(invoice_id, tenant_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_payment_status_text(invoice, payment_intent),
            reply_markup=user_ui.build_user_payment_status_keyboard(invoice_id, payment_intent),
        )

    @dp.callback_query(lambda c: c.data and (c.data.startswith("user_manual_paid:") or c.data.startswith("user_manual_paid_stub:")))
    async def handle_user_manual_paid_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        raw = (callback.data or "").split(":", 1)
        invoice_id = int(raw[1] or 0) if len(raw) > 1 else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        if not invoice:
            await ctx.answer_callback_safe(callback, "Счёт не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка чужой заявки user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому счёту", show_alert=True)
            return
        if str(invoice.get("status") or "") == "paid":
            await ctx.answer_callback_safe(callback, "✅ Этот счёт уже оплачен.", show_alert=True)
            return
        intent = await ctx.find_active_manual_payment_intent_for_invoice(int(invoice_id))
        if not intent:
            await ctx.answer_callback_safe(callback, "❌ Не найдена активная ручная оплата по этому счёту.\nВернитесь к счёту и выберите способ оплаты.", show_alert=True)
            return
        payment_intent_id = int(intent.get("id") or 0)
        payload = dict(intent.get("confirmation_payload_json") or {})
        if not bool(payload.get("receipt_uploaded")):
            ctx.logger.info("пользователь попытался отправить заявку без чека invoice_id=%s intent_id=%s", invoice_id, payment_intent_id)
            await ctx.answer_callback_safe(callback, "❌ Сначала прикрепите чек оплаты", show_alert=True)
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=user_ui.build_user_manual_receipt_request_text(invoice, intent),
                reply_markup=user_ui.build_user_manual_receipt_keyboard(invoice_id),
                parse_mode="HTML",
            )
            return
        payload.update(
            {
                "user_id": int(user_id),
                "tenant_id": int(tenant_id),
                "invoice_id": int(invoice_id),
                "payment_intent_id": payment_intent_id,
                "provider": str(intent.get("provider") or ""),
                "amount": float(intent.get("amount") or 0),
                "currency": str(intent.get("currency") or "USD").upper(),
                "submitted_at": ctx.utc_now_iso(),
                "status": "submitted_by_user",
            }
        )
        await ctx.run_db(ctx.payment_service.save_manual_confirmation_payload, payment_intent_id, payload)
        ctx.logger.info("пользователь отправил заявку ручной оплаты user_id=%s invoice_id=%s intent_id=%s", user_id, invoice_id, payment_intent_id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=(
                "📨 Чек отправлен администратору\n\n"
                "Администратор проверит платёж и подтвердит тариф.\n"
                "Обычно это занимает некоторое время."
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📊 Статус оплаты", callback_data=f"user_payment_status:{int(invoice_id)}")],
                    [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
                ]
            ),
        )
        if ctx.bot:
            provider_title = user_ui.payment_provider_title(str(intent.get("provider") or ""))
            try:
                await ctx.bot.send_message(
                    ctx.settings.admin_id,
                    (
                        "💳 Новая ручная оплата\n\n"
                        f"Пользователь: {user_id}\n"
                        f"Аккаунт: #{tenant_id}\n"
                        f"Счёт: #{invoice_id}\n"
                        f"Payment intent: #{payment_intent_id}\n"
                        f"Способ: {provider_title}\n"
                        f"Сумма: {payload['amount']} {payload['currency']}\n"
                        "Статус: submitted_by_user"
                    ),
                )
                if str(payload.get("receipt_kind") or "") == "photo":
                    await ctx.bot.send_photo(
                        ctx.settings.admin_id,
                        str(payload.get("receipt_file_id") or ""),
                        caption=f"Чек по заявке #{payment_intent_id}",
                        reply_markup=_admin_manual_payment_keyboard(payment_intent_id),
                    )
                else:
                    await ctx.bot.send_document(
                        ctx.settings.admin_id,
                        str(payload.get("receipt_file_id") or ""),
                        caption=f"Чек по заявке #{payment_intent_id}",
                        reply_markup=_admin_manual_payment_keyboard(payment_intent_id),
                    )
                ctx.logger.info("заявка с чеком отправлена админу intent_id=%s", payment_intent_id)
            except Exception as exc:
                ctx.logger.warning("не удалось отправить уведомление администратору intent_id=%s: %s", payment_intent_id, exc)

    @dp.message(lambda m: m.chat.type == "private" and m.from_user is not None and ctx.user_states.get(m.from_user.id, {}).get("action") == "awaiting_payment_receipt")
    async def handle_user_payment_receipt_message(message: Message):
        user_id = message.from_user.id if message.from_user else 0
        if ctx.is_admin_user(user_id):
            ctx.user_states.pop(user_id, None)
            return
        state = ctx.user_states.get(user_id) or {}
        invoice_id = int(state.get("invoice_id") or 0)
        payment_intent_id = int(state.get("payment_intent_id") or 0)
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        intent = await ctx.run_db(ctx.db.get_payment_intent, payment_intent_id) if hasattr(ctx.db, "get_payment_intent") else None
        if not invoice or not intent:
            ctx.user_states.pop(user_id, None)
            await message.answer("❌ Счёт или оплата не найдены. Откройте счёт и начните заново.")
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id) or int(intent.get("invoice_id") or 0) != int(invoice_id):
            ctx.logger.warning("попытка загрузки чека в чужую оплату user_id=%s invoice_id=%s intent_id=%s", user_id, invoice_id, payment_intent_id)
            ctx.user_states.pop(user_id, None)
            await message.answer("⛔ Нет доступа к этому счёту.")
            return
        if str(intent.get("provider") or "") not in ctx.manual_payment_providers:
            ctx.user_states.pop(user_id, None)
            await message.answer("❌ Этот способ оплаты не поддерживает ручную загрузку чека.")
            return
        if str(intent.get("status") or "") not in ctx.manual_payment_active_statuses:
            ctx.user_states.pop(user_id, None)
            await message.answer("❌ Эта заявка на оплату уже закрыта.\n\nОткройте счёт и создайте новую попытку оплаты, затем прикрепите чек.")
            return
        receipt_kind = ""
        receipt_file_id = ""
        receipt_file_unique_id = ""
        receipt_file_name = ""
        receipt_mime_type = ""
        if message.photo:
            photo = message.photo[-1]
            receipt_kind = "photo"
            receipt_file_id = str(photo.file_id or "")
            receipt_file_unique_id = str(photo.file_unique_id or "")
            receipt_mime_type = "image/jpeg"
        elif message.document:
            if not ctx.is_supported_receipt_document(message.document):
                await message.answer("❌ Неподдерживаемый формат чека\n\nПрикрепите PDF, JPG, PNG или WEBP.")
                return
            document = message.document
            receipt_kind = "document"
            receipt_file_id = str(document.file_id or "")
            receipt_file_unique_id = str(document.file_unique_id or "")
            receipt_file_name = str(document.file_name or "")
            receipt_mime_type = str(document.mime_type or "")
        else:
            await message.answer("❌ Чек не найден\n\nПрикрепите чек оплаты файлом или фотографией.\nПоддерживаются: PDF, JPG, PNG, WEBP.")
            return
        payload = dict(intent.get("confirmation_payload_json") or {})
        payload.update(
            {
                "receipt_uploaded": True,
                "receipt_kind": receipt_kind,
                "receipt_file_id": receipt_file_id,
                "receipt_file_unique_id": receipt_file_unique_id,
                "receipt_file_name": receipt_file_name,
                "receipt_mime_type": receipt_mime_type,
                "receipt_uploaded_at": ctx.utc_now_iso(),
                "user_id": int(user_id),
                "tenant_id": int(tenant_id),
                "invoice_id": int(invoice_id),
                "payment_intent_id": int(payment_intent_id),
            }
        )
        await ctx.run_db(ctx.payment_service.save_manual_confirmation_payload, payment_intent_id, payload)
        ctx.user_states.pop(user_id, None)
        ctx.logger.info("пользователь прикрепил чек invoice_id=%s intent_id=%s file_id=%s", invoice_id, payment_intent_id, receipt_file_id)
        await message.answer(
            user_ui.build_user_manual_receipt_uploaded_text(invoice, intent),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"user_manual_paid:{int(invoice_id)}")],
                    [InlineKeyboardButton(text="📊 Статус оплаты", callback_data=f"user_payment_status:{int(invoice_id)}")],
                    [InlineKeyboardButton(text="🧾 Вернуться к счёту", callback_data=f"user_invoice:{int(invoice_id)}")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"user_invoice_pay:{int(invoice_id)}")],
                ]
            ),
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_confirm_manual_payment:"))
    async def handle_admin_confirm_manual_payment_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        admin_id = callback.from_user.id if callback.from_user else ctx.settings.admin_id
        payment_intent_id = int((callback.data or "0").split(":", 1)[1] or 0)
        intent = await ctx.run_db(ctx.db.get_payment_intent, payment_intent_id) if hasattr(ctx.db, "get_payment_intent") else None
        if not intent:
            await ctx.answer_callback_safe(callback, "❌ Платёж не найден", show_alert=True)
            return
        if str(intent.get("status") or "") == "paid":
            await ctx.answer_callback_safe(callback, "✅ Уже оплачено", show_alert=True)
            return
        if str(intent.get("provider") or "") not in ctx.manual_payment_providers:
            await ctx.answer_callback_safe(callback, "❌ Это не ручная оплата", show_alert=True)
            return
        payload = dict(intent.get("confirmation_payload_json") or {})
        if not bool(payload.get("receipt_uploaded")):
            ctx.logger.warning("админ попытался подтвердить оплату без чека intent_id=%s", payment_intent_id)
            await ctx.answer_callback_safe(callback, "❌ Нельзя подтвердить оплату без чека", show_alert=True)
            return
        ok = await ctx.run_db(ctx.payment_service.confirm_manual_payment, payment_intent_id, admin_id, "manual_payment_confirmed_by_admin")
        await ctx.answer_callback_safe_once(callback)
        if not ok:
            await ctx.answer_callback_safe(callback, "❌ Не удалось подтвердить оплату", show_alert=True)
            return
        ctx.logger.info("админ подтвердил оплату с чеком intent_id=%s", payment_intent_id)
        await ctx.edit_message_text_safe(message=callback.message, text=f"✅ Оплата #{payment_intent_id} подтверждена.", reply_markup=None)
        confirmation_payload = intent.get("confirmation_payload_json") if isinstance(intent.get("confirmation_payload_json"), dict) else {}
        user_id = int(confirmation_payload.get("user_id") or 0)
        tenant_id = int(intent.get("tenant_id") or 0)
        if tenant_id:
            recent_events = await ctx.run_db(ctx.billing_service.get_recent_billing_events, tenant_id, 50)
            if _is_recovery_cta_already_shown(recent_events, payment_intent_id):
                ctx.logger.info("recovery CTA уже был показан ранее intent_id=%s tenant_id=%s", payment_intent_id, tenant_id)
                return
        if user_id and ctx.bot:
            try:
                await ctx.bot.send_message(
                    user_id,
                    (
                        "✅ Оплата подтверждена\n\n"
                        "Ваш тариф активирован.\n"
                        "Если публикации или видео были остановлены из-за лимитов, восстановите работу одним нажатием."
                    ),
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Восстановить работу", callback_data="user_recovery")]]),
                )
                if hasattr(ctx.db, "create_billing_event"):
                    await ctx.run_db(
                        ctx.db.create_billing_event,
                        int(tenant_id),
                        RECOVERY_CTA_EVENT_TYPE,
                        event_source="user_payments",
                        metadata={
                            "payment_intent_id": int(payment_intent_id),
                            "invoice_id": int(intent.get("invoice_id") or 0),
                            "user_id": int(user_id),
                            "already_shown": True,
                        },
                    )
            except Exception as exc:
                ctx.logger.warning("не удалось уведомить пользователя user_id=%s intent_id=%s: %s", user_id, payment_intent_id, exc)
        elif not user_id:
            ctx.logger.warning("не удалось уведомить пользователя user_id=%s intent_id=%s", user_id, payment_intent_id)

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_reject_manual_payment:"))
    async def handle_admin_reject_manual_payment_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        admin_id = callback.from_user.id if callback.from_user else ctx.settings.admin_id
        payment_intent_id = int((callback.data or "0").split(":", 1)[1] or 0)
        intent = await ctx.run_db(ctx.db.get_payment_intent, payment_intent_id) if hasattr(ctx.db, "get_payment_intent") else None
        if not intent:
            await ctx.answer_callback_safe(callback, "❌ Платёж не найден", show_alert=True)
            return
        if str(intent.get("status") or "") == "paid":
            await ctx.answer_callback_safe(callback, "❌ Оплата уже подтверждена, отклонение недоступно", show_alert=True)
            return
        if str(intent.get("provider") or "") not in ctx.manual_payment_providers:
            await ctx.answer_callback_safe(callback, "❌ Это не ручная оплата", show_alert=True)
            return
        payload = dict(intent.get("confirmation_payload_json") or {})
        payload.update({"status": "rejected_by_admin", "rejected_by": int(admin_id), "rejected_at": ctx.utc_now_iso(), "reason": "manual_payment_rejected_by_admin"})
        ok = await ctx.run_db(ctx.db.mark_payment_failed, payment_intent_id, "manual_payment_rejected_by_admin", payload=payload)
        await ctx.answer_callback_safe_once(callback)
        if not ok:
            await ctx.answer_callback_safe(callback, "❌ Не удалось отклонить оплату", show_alert=True)
            return
        ctx.logger.info("админ отклонил оплату intent_id=%s", payment_intent_id)
        await ctx.edit_message_text_safe(message=callback.message, text=f"❌ Оплата #{payment_intent_id} отклонена.", reply_markup=None)
        user_id = int(payload.get("user_id") or 0)
        if user_id and ctx.bot:
            try:
                await ctx.bot.send_message(user_id, "❌ Оплата не подтверждена\nПлатёж не найден или данные не совпали.\n\nПроверьте чек и отправьте заявку повторно.")
            except Exception as exc:
                ctx.logger.warning("не удалось уведомить пользователя об отклонении user_id=%s intent_id=%s: %s", user_id, payment_intent_id, exc)
