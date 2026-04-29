from __future__ import annotations

import logging
import secrets
import time
from typing import Any

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app import user_ui
from app.billing_catalog import format_price
from app.payments.payment_matrix import methods_for_currency, method_by_code
from app.payments.payment_router import PaymentRouter
from app.config import settings
from app.payments import LavaTopAPIError, PaymentService as LavaPaymentService
from .context import UserHandlersContext


RECOVERY_CTA_EVENT_TYPE = "recovery_cta_shown_after_payment"
LAVA_PAYMENT_LOGGER = logging.getLogger("forwarder.payments.lava")
BILLING_LOGGER = logging.getLogger("forwarder.billing")
PAY_ACTION_TTL_SECONDS = 30 * 60


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
    router = PaymentRouter(
        ensure_user_tenant=ctx.ensure_user_tenant,
        subscription_service=ctx.subscription_service,
        billing_service=ctx.billing_service,
        invoice_service=ctx.invoice_service,
        payment_service=ctx.payment_service,
    )

    async def _show_legacy_flow_disabled(callback: CallbackQuery) -> None:
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="Этот раздел больше не используется.\n\nПерейдите в раздел подписки.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="💎 Подписка", callback_data="user_subscription")]]
            ),
        )

    def _save_pay_action(user_id: int, action: dict[str, Any]) -> str:
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        actions = state.get("subscription_pay_actions") if isinstance(state.get("subscription_pay_actions"), dict) else {}
        short_id = secrets.token_urlsafe(6)[:8]
        actions[short_id] = {**action, "user_id": int(user_id), "created_at": int(time.time())}
        ctx.user_states[user_id] = {**state, "subscription_pay_actions": actions}
        return short_id

    async def _render_purchase(callback: CallbackQuery, tariff_code: str, currency: str, period: int | None):
        user_id = callback.from_user.id if callback.from_user else 0
        prices = {p: format_price(tariff_code, p, currency) for p in (1, 3, 6, 12)}
        if period is None:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=user_ui.build_user_tariff_period_select_text(tariff_code),
                reply_markup=user_ui.build_user_tariff_period_select_keyboard(tariff_code=tariff_code, currency=currency, prices=prices),
            )
            return
        methods = methods_for_currency(currency)
        pay_buttons: list[tuple[str, str]] = []
        for method in methods:
            short_id = _save_pay_action(user_id, {"tariff_code": tariff_code, "currency": currency, "period_months": period, "method_code": method.get("code")})
            pay_buttons.append((str(method.get("title") or method.get("code")), short_id))
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_tariff_payment_select_text(tariff_code, currency, period, prices),
            reply_markup=user_ui.build_user_tariff_payment_select_keyboard(tariff_code=tariff_code, currency=currency, pay_buttons=pay_buttons),
        )

    @dp.callback_query(lambda c: c.data == "user_subscription")
    async def handle_user_subscription_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_user_subscription_status_text(await ctx.run_db(ctx.subscription_service.get_active_subscription, await ctx.run_db(ctx.ensure_user_tenant, callback.from_user.id if callback.from_user else 0))),
            reply_markup=user_ui.build_user_subscription_status_keyboard(await ctx.run_db(ctx.subscription_service.get_active_subscription, await ctx.run_db(ctx.ensure_user_tenant, callback.from_user.id if callback.from_user else 0))),
        )

    @dp.callback_query(lambda c: c.data == "user_subscription_plans" or c.data == "user_billing_shop")
    async def handle_user_subscription_plans_callback(callback: CallbackQuery):
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=user_ui.build_user_subscription_plans_text(), reply_markup=user_ui.build_user_subscription_plans_keyboard())

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_subscription_buy:"))
    async def handle_user_subscription_buy_callback(callback: CallbackQuery):
        _, tariff_code = (callback.data or "").split(":", 1)
        await ctx.answer_callback_safe_once(callback)
        user_id = callback.from_user.id if callback.from_user else 0
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        ctx.user_states[user_id] = {**state, "subscription_purchase": {"tariff_code": str(tariff_code).lower(), "currency": "USD", "period_months": None}}
        await _render_purchase(callback, str(tariff_code).lower(), "USD", None)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_subscription_currency:"))
    async def handle_user_subscription_currency_callback(callback: CallbackQuery):
        _, tariff_code, currency = (callback.data or "").split(":", 2)
        user_id = callback.from_user.id if callback.from_user else 0
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        purchase = state.get("subscription_purchase") if isinstance(state.get("subscription_purchase"), dict) else {}
        period = purchase.get("period_months")
        ctx.user_states[user_id] = {**state, "subscription_purchase": {"tariff_code": tariff_code, "currency": currency.upper(), "period_months": period}}
        await ctx.answer_callback_safe_once(callback)
        await _render_purchase(callback, tariff_code, currency.upper(), int(period) if period is not None else None)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_subscription_period:"))
    async def handle_user_subscription_period_callback(callback: CallbackQuery):
        _, tariff_code, currency, period_raw = (callback.data or "").split(":", 3)
        period = int(period_raw or 1)
        user_id = callback.from_user.id if callback.from_user else 0
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        ctx.user_states[user_id] = {**state, "subscription_purchase": {"tariff_code": tariff_code, "currency": currency.upper(), "period_months": period}}
        await ctx.answer_callback_safe_once(callback)
        await _render_purchase(callback, tariff_code, currency.upper(), period)


    @dp.callback_query(lambda c: c.data and c.data.startswith("user_subscription_back_to_periods:"))
    async def handle_user_subscription_back_to_periods_callback(callback: CallbackQuery):
        _, tariff_code, currency = (callback.data or "").split(":", 2)
        user_id = callback.from_user.id if callback.from_user else 0
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        ctx.user_states[user_id] = {**state, "subscription_purchase": {"tariff_code": tariff_code, "currency": currency.upper(), "period_months": None}}
        await ctx.answer_callback_safe_once(callback)
        await _render_purchase(callback, tariff_code, currency.upper(), None)

    @dp.callback_query(lambda c: c.data == "user_tariff_basic")
    async def handle_user_tariff_basic_callback(callback: CallbackQuery):
        if ctx.is_admin_user(callback.from_user.id if callback.from_user else None):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=user_ui.build_billing_subscription_text(await ctx.run_db(ctx.subscription_service.get_active_subscription, await ctx.run_db(ctx.ensure_user_tenant, callback.from_user.id if callback.from_user else 0))),
            reply_markup=user_ui.build_billing_subscription_keyboard(),
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
        await _show_legacy_flow_disabled(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_confirm_plan:"))
    async def handle_user_confirm_plan_callback(callback: CallbackQuery):
        await _show_legacy_flow_disabled(callback)

    @dp.callback_query(lambda c: c.data == "user_invoices")
    async def handle_user_invoices_callback(callback: CallbackQuery):
        await _show_legacy_flow_disabled(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice:"))
    async def handle_user_invoice_callback(callback: CallbackQuery):
        await _show_legacy_flow_disabled(callback)



    @dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice_check_payment:"))
    async def handle_user_invoice_check_payment_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        if ctx.is_admin_user(user_id):
            await ctx.answer_callback_safe(callback, "Раздел только для пользователей", show_alert=True)
            return
        raw = (callback.data or "").split(":", 1)
        invoice_id = int(raw[1] or 0) if len(raw) > 1 else 0
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        items = await ctx.run_db(ctx.db.list_invoice_items, invoice_id) if hasattr(ctx.db, "list_invoice_items") else []
        if not invoice:
            await ctx.answer_callback_safe(callback, "Платёж не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому платежу", show_alert=True)
            return

        status = str(invoice.get("status") or "").lower()
        await ctx.answer_callback_safe_once(callback)
        if status == "paid":
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="Платёж подтверждён\n\nПодписка активирована",
                reply_markup=user_ui.build_user_invoice_keyboard(invoice_id),
            )
            return
        if status in {"open", "draft", "pending"}:
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="Платёж ещё не подтверждён",
                reply_markup=user_ui.build_user_invoice_keyboard(invoice_id),
            )
            return
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="Платёж ещё не подтверждён",
            reply_markup=user_ui.build_user_invoice_keyboard(invoice_id),
        )
    @dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice_pay:"))
    async def handle_user_invoice_pay_callback(callback: CallbackQuery):
        await _show_legacy_flow_disabled(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_pay_provider:"))
    async def handle_user_pay_provider_callback(callback: CallbackQuery):
        await _show_legacy_flow_disabled(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_invoice_pay_lava:"))
    async def handle_user_invoice_pay_lava_callback(callback: CallbackQuery):
        await _show_legacy_flow_disabled(callback)

    
    @dp.callback_query(lambda c: c.data and c.data.startswith("user_billing_pick:"))
    async def handle_user_billing_pick_callback(callback: CallbackQuery):
        _, tariff, period, currency = (callback.data or "").split(":", 3)
        methods = methods_for_currency(currency)
        rows = [[InlineKeyboardButton(text=str(m.get("title")), callback_data=f"user_billing_pay:{tariff}:{period}:{currency}:{m.get('code')}")] for m in methods]
        rows.append([InlineKeyboardButton(text="⬅️ К покупке", callback_data="user_billing_shop")])
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=f"💳 Выберите способ оплаты\n\nТариф: {tariff.upper()}\nПериод: {period} мес\nСумма: {format_price(tariff, int(period), currency)}", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_billing_pay:"))
    async def handle_user_billing_pay_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        username = callback.from_user.username if callback.from_user else None
        _, tariff, period, currency, method_code = (callback.data or "").split(":", 4)
        method = method_by_code(currency, method_code) or {}
        if not bool(method.get("enabled", True)):
            await ctx.answer_callback_safe_once(callback)
            await ctx.edit_message_text_safe(message=callback.message, text="Этот способ оплаты скоро будет доступен.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к способам оплаты", callback_data=f"user_billing_pick:{tariff}:{period}:{currency}")]]))
            return
        await ctx.answer_callback_safe_once(callback, "⏳ Создаю платёж…")
        result = await router.start_payment(
            user_id=user_id, tariff_code=tariff, period_months=int(period), currency=currency, method_code=method_code, username=username
        )
        BILLING_LOGGER.info("start_payment user_id=%s tariff_code=%s period_months=%s currency=%s method_code=%s provider=%s internal_invoice_created=%s", user_id, tariff, period, currency, method_code, result.provider, True)
        if result.payment_url:
            await ctx.edit_message_text_safe(message=callback.message, text=f"✅ Ссылка на оплату создана\n\nТариф: {tariff.upper()}\nПериод: {period} месяц\nСумма: {result.amount_text}\nСпособ: {result.method_title}\n\nПосле оплаты доступ активируется автоматически.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Перейти к оплате", url=result.payment_url)],[InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"user_invoice_check_payment:{int(result.invoice_id)}")],[InlineKeyboardButton(text="⬅️ Выбрать другой способ", callback_data=f"user_billing_pick:{tariff}:{period}:{currency}")]]))
            return
        await ctx.edit_message_text_safe(message=callback.message, text=("✅ Платёж создан\n\n" f"Тариф: {tariff.upper()}\nПериод: {period} месяц\nСумма: {result.amount_text}\n" f"Способ: {result.method_title}\n\nПосле оплаты прикрепите чек сюда в чат."), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🧾 Отправить чек", callback_data=f"user_upload_receipt:{int(result.invoice_id)}")],[InlineKeyboardButton(text="⬅️ Выбрать другой способ", callback_data=f"user_billing_pick:{tariff}:{period}:{currency}")]]))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_subscription_pay:"))
    async def handle_user_subscription_pay_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        short_id = (callback.data or "").split(":", 1)[1] if ":" in (callback.data or "") else ""
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        actions = state.get("subscription_pay_actions") if isinstance(state.get("subscription_pay_actions"), dict) else {}
        action = actions.get(short_id) if isinstance(actions.get(short_id), dict) else None
        if not action or int(action.get("user_id") or 0) != int(user_id):
            await ctx.answer_callback_safe(callback, "⚠️ Данные устарели. Откройте оплату заново.", show_alert=True)
            return
        if int(time.time()) - int(action.get("created_at") or 0) > PAY_ACTION_TTL_SECONDS:
            actions.pop(short_id, None)
            ctx.user_states[user_id] = {**state, "subscription_pay_actions": actions}
            await ctx.answer_callback_safe(callback, "⚠️ Данные устарели. Откройте оплату заново.", show_alert=True)
            return
        tariff = str(action.get("tariff_code") or "basic")
        period = int(action.get("period_months") or 1)
        currency = str(action.get("currency") or "USD").upper()
        method_code = str(action.get("method_code") or "")
        callback.data = f"user_billing_pay:{tariff}:{period}:{currency}:{method_code}"
        await handle_user_billing_pay_callback(callback)

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
                    [InlineKeyboardButton(text="💳 Вернуться к платежу", callback_data=f"user_invoice:{int(invoice_id)}")],
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
            await message.answer("❌ Платёж не найден. Откройте раздел оплаты и начните заново.")
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id) or int(intent.get("invoice_id") or 0) != int(invoice_id):
            ctx.logger.warning("попытка загрузки чека в чужую оплату user_id=%s invoice_id=%s intent_id=%s", user_id, invoice_id, payment_intent_id)
            ctx.user_states.pop(user_id, None)
            await message.answer("⛔ Нет доступа к этому платежу.")
            return
        if str(intent.get("provider") or "") not in ctx.manual_payment_providers:
            ctx.user_states.pop(user_id, None)
            await message.answer("❌ Этот способ оплаты не поддерживает ручную загрузку чека.")
            return
        if str(intent.get("status") or "") not in ctx.manual_payment_active_statuses:
            ctx.user_states.pop(user_id, None)
            await message.answer("❌ Эта заявка на оплату уже закрыта.\n\nОткройте раздел оплаты и создайте новую попытку, затем прикрепите чек.")
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
                    [InlineKeyboardButton(text="💳 Вернуться к платежу", callback_data=f"user_invoice:{int(invoice_id)}")],
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
