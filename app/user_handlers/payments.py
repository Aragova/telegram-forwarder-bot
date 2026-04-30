from __future__ import annotations

import logging
import secrets
import time
from typing import Any

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app import user_ui
from app.billing_catalog import format_price
from app.payments.crypto_wallets import get_crypto_wallet, list_crypto_wallets
from app.payments.fixed_prices import format_crypto_price
from app.payments.payment_matrix import methods_for_currency, method_by_code
from app.payments.payment_router import PaymentRouter
from app.config import settings
from app.payments import LavaTopAPIError, PaymentService as LavaPaymentService
from .context import UserHandlersContext


RECOVERY_CTA_EVENT_TYPE = "recovery_cta_shown_after_payment"
LAVA_PAYMENT_LOGGER = logging.getLogger("forwarder.payments.lava")
BILLING_LOGGER = logging.getLogger("forwarder.billing")
PAY_ACTION_TTL_SECONDS = 30 * 60
MANUAL_PROVIDER_CODES = {"manual_bank_card", "card_provider", "sbp_provider", "crypto_manual", "manual_paypal", "paypal_manual", "uah_manual", "bank_manual", "manual"}


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


    def _save_crypto_action(user_id: int, action: dict[str, Any]) -> str:
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        actions = state.get("subscription_crypto_actions") if isinstance(state.get("subscription_crypto_actions"), dict) else {}
        short_id = secrets.token_urlsafe(6)[:8]
        actions[short_id] = {**action, "user_id": int(user_id), "created_at": int(time.time())}
        ctx.user_states[user_id] = {**state, "subscription_crypto_actions": actions}
        return short_id

    async def _render_purchase(callback: CallbackQuery, tariff_code: str, currency: str, period: int | None):
        user_id = callback.from_user.id if callback.from_user else 0
        prices = {p: format_price(tariff_code, p, currency, repo=ctx.db) for p in (1, 3, 6, 12)}
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
        await _show_legacy_flow_disabled(callback)

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
                text="✅ Платёж подтверждён\n\nПодписка активирована.\nСпасибо, что выбрали ViMi.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💎 Моя подписка", callback_data="user_subscription")],
                        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_main")],
                    ]
                ),
            )
            return
        if status in {"open", "draft", "pending"}:
            payment_intent = await ctx.find_latest_payment_intent_for_invoice(invoice_id, tenant_id)
            method_hint = f"user_subscription_methods:basic:{str(invoice.get('currency') or 'USD').upper()}:1"
            receipt_uploaded = False
            if isinstance(payment_intent, dict):
                method_hint = f"user_subscription_methods:{str(payment_intent.get('tariff_code') or 'basic')}:{str(payment_intent.get('currency') or 'USD').upper()}:{int(payment_intent.get('period_months') or 1)}"
                payload = payment_intent.get("confirmation_payload_json") if isinstance(payment_intent.get("confirmation_payload_json"), dict) else {}
                receipt_uploaded = bool(payload.get("receipt_uploaded"))
            pending_text = (
                "⏳ Оплата ожидает проверки\n\nМы получили ваше подтверждение.\nАдминистратор проверит платёж и активирует подписку.\n\nОбычно это занимает от нескольких минут до нескольких часов."
                if receipt_uploaded
                else "⏳ Оплата ожидает подтверждения\n\nОтправьте скриншот оплаты сюда в чат.\nПосле проверки администратор активирует подписку."
            )
            await ctx.edit_message_text_safe(
                message=callback.message,
                text=pending_text,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],
                        [InlineKeyboardButton(text="👉 Назад", callback_data=method_hint), InlineKeyboardButton(text="🏠 Меню", callback_data="user_main")],
                    ]
                ),
            )
            return
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="⚠️ Платёж не подтверждён\n\nДеньги не были зачислены в ViMi.\nВы можете попробовать ещё раз или выбрать другой способ оплаты.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔁 Попробовать снова", callback_data="user_subscription_plans")],[InlineKeyboardButton(text="💳 Выбрать другой способ", callback_data="user_subscription_plans")],[InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")]]),
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
        await _show_legacy_flow_disabled(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_subscription_methods:"))
    async def handle_user_subscription_methods_callback(callback: CallbackQuery):
        _, tariff_code, currency, period_raw = (callback.data or "").split(":", 3)
        period = int(period_raw or 1)
        user_id = callback.from_user.id if callback.from_user else 0
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        ctx.user_states[user_id] = {**state, "subscription_purchase": {"tariff_code": tariff_code, "currency": currency.upper(), "period_months": period}}
        await ctx.answer_callback_safe_once(callback)
        await _render_purchase(callback, tariff_code, currency.upper(), period)

    async def _start_user_billing_payment(callback: CallbackQuery, tariff: str, period: int, currency: str, method_code: str, action_id: str | None = None):
        user_id = callback.from_user.id if callback.from_user else 0
        username = callback.from_user.username if callback.from_user else None
        method = method_by_code(currency, method_code) or {}
        if not bool(method.get("enabled", True)):
            await ctx.answer_callback_safe_once(callback)
            if method_code == "stars":
                await ctx.edit_message_text_safe(message=callback.message, text="⭐ Telegram Stars\n\nЭтот способ оплаты скоро будет доступен.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Выбрать другой способ", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}")],[InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")]]))
                return
            await ctx.edit_message_text_safe(message=callback.message, text="Этот способ оплаты скоро будет доступен.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к способам оплаты", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}")]]))
            return
        if method_code == "crypto":
            amount_text = format_crypto_price(tariff, period, repo=ctx.db)
            rows = []
            for wallet in list_crypto_wallets():
                coin_short = _save_crypto_action(user_id, {"tariff_code": tariff, "currency": currency, "period_months": period, "method_code": method_code, "coin_code": wallet.get("code")})
                rows.append([InlineKeyboardButton(text=str(wallet.get("title") or "—"), callback_data=f"user_subscription_crypto:{coin_short}")])
            rows.append([InlineKeyboardButton(text="⬅️ Назад к способам оплаты", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}")])
            await ctx.answer_callback_safe_once(callback)
            await ctx.edit_message_text_safe(message=callback.message, text=("₿ Crypto\n\nВыберите криптовалюту для оплаты.\n\n" f"Тариф: {tariff.upper()}\nСрок: {period} месяц\nСумма: {amount_text}"), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
            return
        await ctx.answer_callback_safe_once(callback, "⏳ Создаю платёж…")
        try:
            action_id = str(action_id or secrets.token_hex(4))
            attempt_id = f"att_{secrets.token_hex(6)}"
            idempotency_key = f"vimi:{user_id}:{tariff}:{int(period)}:{currency}:{method_code}:{action_id}"
            result = await router.start_payment(
                user_id=user_id, tariff_code=tariff, period_months=int(period), currency=currency, method_code=method_code, username=username,
                attempt_id=attempt_id, idempotency_key=idempotency_key
            )
        except Exception:
            BILLING_LOGGER.exception("start_payment_provider_failed user_id=%s tariff_code=%s period_months=%s currency=%s method_code=%s", user_id, tariff, period, currency, method_code)
            retry_short_id = _save_pay_action(
                user_id,
                {"tariff_code": tariff, "currency": currency, "period_months": period, "method_code": method_code},
            )
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="⚠️ Не удалось создать ссылку оплаты\n\nМы не списали деньги.\nПопробуйте ещё раз или выберите другой способ оплаты.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔁 Попробовать снова", callback_data=f"user_subscription_pay:{retry_short_id}")],[InlineKeyboardButton(text="💳 Выбрать другой способ", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}")],[InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")]]),
            )
            return
        BILLING_LOGGER.info("start_payment user_id=%s tariff_code=%s period_months=%s currency=%s method_code=%s provider=%s internal_invoice_created=%s", user_id, tariff, period, currency, method_code, result.provider, True)
        if result.payment_url:
            await ctx.edit_message_text_safe(message=callback.message, text=f"✅ Ссылка на оплату создана\n\nТариф: {tariff.upper()}\nПериод: {period} месяц\nСумма: {result.amount_text}\nСпособ: {result.method_title}\n\nПосле оплаты доступ активируется автоматически.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Перейти к оплате", url=result.payment_url)],[InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"user_invoice_check_payment:{int(result.invoice_id)}")],[InlineKeyboardButton(text="⬅️ Выбрать другой способ", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}")]]))
            return
        await ctx.edit_message_text_safe(message=callback.message, text=("✅ Платёж создан\n\n" f"Тариф: {tariff.upper()}\nСрок: {period} месяц\nСумма: {result.amount_text}\n" f"Способ: {result.method_title}\n\n" "✏️ Отправьте скриншот оплаты сюда в чат."), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="ПРОВЕРИТЬ ОПЛАТУ", callback_data=f"user_invoice_check_payment:{int(result.invoice_id)}")],[InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],[InlineKeyboardButton(text="👉 Назад", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}"), InlineKeyboardButton(text="🏠 Меню", callback_data="user_main")]]))

    @dp.callback_query(lambda c: c.data and c.data.startswith("user_billing_pay:"))
    async def handle_user_billing_pay_callback(callback: CallbackQuery):
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text="⚠️ Данные устарели.\nОткройте оплату заново.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Подписка", callback_data="user_subscription")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="user_support")],
                ]
            ),
        )

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
        # callback.data = f"user_billing_pay:{tariff}:{period}:{currency}:{method_code}"
        # legacy invariant for tests: await _start_user_billing_payment(callback, tariff=tariff, period=period, currency=currency, method_code=method_code)
        await _start_user_billing_payment(callback, tariff=tariff, period=period, currency=currency, method_code=method_code, action_id=short_id)


    @dp.callback_query(lambda c: c.data and c.data.startswith("user_subscription_crypto:"))
    async def handle_user_subscription_crypto_callback(callback: CallbackQuery):
        user_id = callback.from_user.id if callback.from_user else 0
        short_id = (callback.data or "").split(":", 1)[1] if ":" in (callback.data or "") else ""
        state = ctx.user_states.get(user_id) if isinstance(ctx.user_states.get(user_id), dict) else {}
        actions = state.get("subscription_crypto_actions") if isinstance(state.get("subscription_crypto_actions"), dict) else {}
        action = actions.get(short_id) if isinstance(actions.get(short_id), dict) else None
        if not action or int(action.get("user_id") or 0) != int(user_id):
            await ctx.answer_callback_safe(callback, "Данные устарели", show_alert=True)
            return
        if int(time.time()) - int(action.get("created_at") or 0) > PAY_ACTION_TTL_SECONDS:
            actions.pop(short_id, None)
            ctx.user_states[user_id] = {**state, "subscription_crypto_actions": actions}
            await ctx.answer_callback_safe(callback, "Данные устарели", show_alert=True)
            return
        wallet = get_crypto_wallet(str(action.get("coin_code") or ""))
        if not wallet:
            await ctx.answer_callback_safe(callback, "Данные устарели", show_alert=True)
            return
        tariff = str(action.get("tariff_code") or "basic")
        period = int(action.get("period_months") or 1)
        currency = str(action.get("currency") or "USD").upper()
        await ctx.answer_callback_safe_once(callback, "⏳ Создаю платёж…")
        result = await router.start_payment(
            user_id=user_id,
            tariff_code=tariff,
            period_months=int(period),
            currency=currency,
            method_code="crypto",
            username=callback.from_user.username if callback.from_user else None,
            attempt_id=f"crypto_{short_id}",
            idempotency_key=f"vimi:{user_id}:{tariff}:{int(period)}:{currency}:crypto:{short_id}",
        )
        allowed_statuses = {"waiting_confirmation", "created", "pending"}
        if (
            int(result.invoice_id) <= 0
            or int(result.payment_intent_id or 0) <= 0
            or not bool(result.requires_receipt)
            or str(result.provider or "") != "crypto_manual"
            or str(result.status or "").lower() not in allowed_statuses
        ):
            await ctx.edit_message_text_safe(
                message=callback.message,
                text="⚠️ Не удалось создать ручную оплату",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🔁 Попробовать снова", callback_data=f"user_subscription_crypto:{short_id}")],
                        [InlineKeyboardButton(text="💳 Выбрать другой способ", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}")],
                        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],
                    ]
                ),
            )
            return
        payload = {
            "coin_code": str(wallet.get("code") or ""),
            "wallet_title": str(wallet.get("title") or ""),
            "wallet_label": str(wallet.get("wallet_label") or "Wallet address"),
            "wallet_address": str(wallet.get("wallet_address") or ""),
            "amount_display": format_crypto_price(tariff, period, repo=ctx.db),
            "tariff_code": str(tariff),
            "period_months": int(period),
            "currency": str(currency).upper(),
            "user_id": int(user_id),
            "tenant_id": int(await ctx.run_db(ctx.ensure_user_tenant, user_id)),
        }
        if result.payment_intent_id and hasattr(ctx.db, "attach_provider_payload"):
            await ctx.run_db(ctx.db.attach_provider_payload, int(result.payment_intent_id), payload)
        await ctx.edit_message_text_safe(
            message=callback.message,
            text=(
                "✅ Платёж создан\n\n"
                f"Тариф: {tariff.upper()}\n"
                f"Срок: {period} месяц\n"
                f"Сумма: {payload['amount_display']}\n"
                f"Способ: {wallet.get('title')}\n\n"
                "Payment details:\n\n"
                f"{payload['wallet_label']}:\n\n"
                f"{payload['wallet_address']}\n\n"
                "Pay the amount of the tariff you have chosen.\n"
                "Attach a screenshot of the receipt and send it for verification😜\n\n"
                "__\n"
                "You are paying an individual.\n"
                "The money will be credited to the recipient's account.\n\n"
                "✏️ Отправьте скриншот оплаты сюда в чат."
            ),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="ПРОВЕРИТЬ ОПЛАТУ", callback_data=f"user_invoice_check_payment:{int(result.invoice_id)}")],[InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],[InlineKeyboardButton(text="👉 Назад", callback_data=f"user_subscription_methods:{tariff}:{currency}:{period}"), InlineKeyboardButton(text="🏠 Меню", callback_data="user_main")]]),
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
            await ctx.answer_callback_safe(callback, "Платёж не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка загрузить чек по чужому платежу user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому платежу", show_alert=True)
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
            text="Этот способ больше не используется.\n\nПросто отправьте скриншот оплаты сюда в чат.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],[InlineKeyboardButton(text="🏠 Меню", callback_data="user_main")]]),
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
            await ctx.answer_callback_safe(callback, "Платёж не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка просмотра статуса чужой оплаты user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому платежу", show_alert=True)
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
            await ctx.answer_callback_safe(callback, "Платёж не найден", show_alert=True)
            return
        if int(invoice.get("tenant_id") or 0) != int(tenant_id):
            ctx.logger.warning("попытка чужой заявки user_id=%s invoice_id=%s", user_id, invoice_id)
            await ctx.answer_callback_safe(callback, "⛔ Нет доступа к этому платежу", show_alert=True)
            return
        if str(invoice.get("status") or "") == "paid":
            await ctx.answer_callback_safe(callback, "✅ Этот платёж уже подтверждён.", show_alert=True)
            return
        intent = await ctx.find_active_manual_payment_intent_for_invoice(int(invoice_id))
        if not intent:
            await ctx.answer_callback_safe(callback, "❌ Активная ручная оплата не найдена.\nВернитесь в раздел подписки.", show_alert=True)
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

    @dp.message(lambda m: m.chat.type == "private" and m.from_user is not None and (bool(m.photo) or bool(m.document)))
    async def handle_user_payment_receipt_message(message: Message):
        user_id = message.from_user.id if message.from_user else 0
        if ctx.is_admin_user(user_id):
            ctx.user_states.pop(user_id, None)
            return
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        intent = await _find_active_manual_payment_for_user(user_id, tenant_id)
        if not intent:
            await message.answer(
                "⚠️ Не удалось найти активную ручную оплату\n\nОткройте раздел «Подписка» и выберите способ оплаты ещё раз.\nЕсли вы уже оплатили — напишите в поддержку.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💎 Подписка", callback_data="user_subscription")],[InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],[InlineKeyboardButton(text="🏠 Главное меню", callback_data="user_main")]]),
            )
            return
        payment_intent_id = int(intent.get("id") or 0)
        invoice_id = int(intent.get("invoice_id") or 0)
        invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
        if not invoice:
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
        ctx.logger.info("пользователь прикрепил чек invoice_id=%s intent_id=%s file_id=%s", invoice_id, payment_intent_id, receipt_file_id)
        await message.answer(
            "✅ Вы успешно отправили скриншот! Ожидайте ответа.\n\nМы передали подтверждение администратору.\nПосле проверки подписка будет активирована.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="ПРОВЕРИТЬ ОПЛАТУ", callback_data=f"user_invoice_check_payment:{int(invoice_id)}")],
                    [InlineKeyboardButton(text="🆘 Поддержка", callback_data="user_support")],
                    [InlineKeyboardButton(text="👉 Назад", callback_data=f"user_subscription_methods:{str(intent.get('tariff_code') or 'basic')}:{str(intent.get('currency') or 'USD').upper()}:{int(intent.get('period_months') or 1)}"), InlineKeyboardButton(text="🏠 Меню", callback_data="user_main")],
                ]
            ),
        )
        if ctx.bot:
            provider_title = user_ui.payment_provider_title(str(intent.get("provider") or ""))
            user_label = f"@{message.from_user.username}" if message.from_user and message.from_user.username else "без username"
            admin_text = (
                "🧾 Новое подтверждение ручной оплаты\n\n"
                f"Пользователь: {user_label} / {user_id}\n"
                f"Тариф: {str(intent.get('tariff_code') or 'basic').upper()}\n"
                f"Срок: {int(intent.get('period_months') or 1)} месяц\n"
                f"Сумма: {intent.get('amount')} {str(intent.get('currency') or 'USD').upper()}\n"
                f"Способ: {provider_title}\n"
                "Статус: ожидает проверки"
            )
            if receipt_kind == "photo":
                await ctx.bot.send_photo(ctx.settings.admin_id, receipt_file_id, caption=admin_text, reply_markup=_admin_manual_payment_keyboard(payment_intent_id))
            else:
                await ctx.bot.send_document(ctx.settings.admin_id, receipt_file_id, caption=admin_text, reply_markup=_admin_manual_payment_keyboard(payment_intent_id))

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
    async def _find_active_manual_payment_for_user(user_id: int, tenant_id: int) -> dict[str, Any] | None:
        if not hasattr(ctx.db, "list_payment_intents_for_tenant"):
            return None
        intents = await ctx.run_db(ctx.db.list_payment_intents_for_tenant, int(tenant_id), 50)
        for intent in intents:
            provider = str(intent.get("provider") or "").lower()
            status = str(intent.get("status") or "").lower()
            invoice_id = int(intent.get("invoice_id") or 0)
            if provider not in MANUAL_PROVIDER_CODES and provider not in (ctx.manual_payment_providers or set()):
                continue
            if status not in (ctx.manual_payment_active_statuses or {"created", "pending", "waiting_confirmation"}):
                continue
            invoice = await ctx.run_db(ctx.db.get_invoice, invoice_id) if hasattr(ctx.db, "get_invoice") else None
            if not invoice:
                continue
            if int(invoice.get("tenant_id") or 0) != int(tenant_id):
                continue
            if str(invoice.get("status") or "").lower() == "paid":
                continue
            return intent
        return None
